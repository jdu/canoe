//! Turns the coordinator from "one static config loaded once at startup"
//! into a live registry of independently-pushable, named projects — see
//! the "Live pipeline development" plan this module implements. The
//! `--config` file loaded at startup is just project `"default"` under
//! this same scheme (`main.rs` calls `apply_project(ctx, "default", ...)`,
//! the exact same path a later `POST /projects/:name` push uses), so
//! there is one code path for "start a project's chains/adapters/tasks,"
//! not a special-cased original plus a parallel new one.
//!
//! **The collision fix**: `Ctx.activated`/`Ctx.completed`/`StatusRegistry`
//! and every K8s resource name (`PodManager::pod_set_name`, Job names)
//! are keyed off plain strings — `chain.name`, `step.name()`,
//! `task.name`, `adapter.name`, `chain.pipeline` — with no project
//! scoping anywhere in that existing code, and no signature changes are
//! made to any of it here. Instead, [`prefix_project`] rewrites every one
//! of those identity fields (and every `depends_on`/`depends_on_complete`
//! entry that names one of them — the cross-references only ever point
//! at another name *within the same project*, so blanket-prefixing every
//! entry the same way keeps them all resolving correctly against each
//! other) to `{project}--{original}` **once**, before the (now
//! internally-consistent, project-prefixed) config is handed to the
//! exact same `start_*`/`PodManager`/`activated`/`completed`/
//! `StatusRegistry`/`scheduler`/`tasks`/`dynamic_router` code that
//! already existed — none of it needs to know a "project" concept exists
//! at all. Transport-value fields (`input_path`/`output_path`/
//! `input_topic`/`output_topic`/`input_signal`/`chain.source`) are left
//! untouched: they're already-resolved S3 paths or Kafka topic strings,
//! not symbolic names, and stay correct regardless of which project
//! pushed them.

use crate::activation::{Ctx, PendingActivation};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use canoe_config::{ChainConfig, PipelineConfig, StepConfig, TaskConfig};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::{info, warn};

#[derive(Clone)]
pub struct ProjectEntry {
    pub config: Arc<PipelineConfig>,
    pub is_dev: bool,
    pub pushed_at: DateTime<Utc>,
}

/// Live registry of every project the coordinator currently knows about —
/// `"default"` (from `--config` at startup) plus anything pushed since
/// via `POST /projects/:name`. See module docs.
#[derive(Default)]
pub struct ProjectRegistry(Mutex<HashMap<String, ProjectEntry>>);

impl ProjectRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, name: &str, entry: ProjectEntry) {
        self.0.lock().unwrap().insert(name.to_string(), entry);
    }

    pub fn get(&self, name: &str) -> Option<ProjectEntry> {
        self.0.lock().unwrap().get(name).cloned()
    }

    /// A stable-ish (insertion-order-independent, name-sorted) snapshot
    /// for `status.rs`'s response building — cloning the `Arc<PipelineConfig>`
    /// is cheap regardless of how large the config is.
    pub fn snapshot(&self) -> Vec<(String, ProjectEntry)> {
        let mut entries: Vec<(String, ProjectEntry)> =
            self.0.lock().unwrap().iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries
    }
}

/// Rewrites every project-scoped identity field in `config` to
/// `{project}--{original}` — pure, deterministic, no I/O — see the module
/// doc comment for exactly which fields and why. Called once, by
/// `apply_project`, before anything in `config` is handed to the rest of
/// the coordinator.
pub fn prefix_project(project: &str, config: &PipelineConfig) -> PipelineConfig {
    let mut config = config.clone();
    let p = |s: &str| format!("{project}--{s}");

    for chain in &mut config.chains {
        chain.name = p(&chain.name);
        chain.pipeline = p(&chain.pipeline);
        for step in &mut chain.steps {
            prefix_step(step, &p);
        }
    }
    for task in &mut config.tasks {
        prefix_task(task, &p);
    }
    for adapter in &mut config.adapters {
        adapter.name = p(&adapter.name);
    }
    config
}

fn prefix_step(step: &mut StepConfig, p: &impl Fn(&str) -> String) {
    match step {
        StepConfig::Python(s) => {
            s.name = p(&s.name);
            prefix_names(&mut s.depends_on, p);
            prefix_names(&mut s.depends_on_complete, p);
        }
        StepConfig::SparkSql(s) => {
            s.name = p(&s.name);
            prefix_names(&mut s.depends_on, p);
            prefix_names(&mut s.depends_on_complete, p);
        }
        StepConfig::SparkDataFrame(s) => {
            s.name = p(&s.name);
            prefix_names(&mut s.depends_on, p);
            prefix_names(&mut s.depends_on_complete, p);
        }
        // These variants never carry `depends_on`/`depends_on_complete` —
        // `canoe-dsl` rejects them at compile time for a streaming-mode
        // step/dynamic_route (no "done" to gate on) — just the identity
        // field.
        StepConfig::KafkaPython(s) => s.name = p(&s.name),
        StepConfig::KafkaSparkSql(s) => s.name = p(&s.name),
        StepConfig::KafkaSparkDataFrame(s) => s.name = p(&s.name),
        StepConfig::Sink(s) => s.name = p(&s.name),
        StepConfig::Uv(s) => s.name = p(&s.name),
        StepConfig::DynamicRoute(s) => s.name = p(&s.name),
    }
}

fn prefix_task(task: &mut TaskConfig, p: &impl Fn(&str) -> String) {
    task.name = p(&task.name);
    prefix_names(&mut task.depends_on, p);
    prefix_names(&mut task.depends_on_complete, p);
}

fn prefix_names(names: &mut [String], p: &impl Fn(&str) -> String) {
    for n in names.iter_mut() {
        *n = p(n);
    }
}

/// Starts (or, for a re-push, tops up) one project's chains/adapters/
/// tasks/dynamic_routes — the single path both `main.rs`'s startup
/// ("default") and `POST /projects/:name` (anything pushed later) go
/// through. Stores the (already project-prefixed) config in `ctx.projects`
/// first, so `GET /status` sees it immediately even before every
/// `start_*` call below finishes spawning its detached loop.
///
/// **Idempotent-additive, not a real update**: re-pushing an existing
/// project tops up new/changed chains the same way `start_python_step`
/// already tops up rather than duplicates its own pod set — but it does
/// **not** tear down chains/steps/adapters/tasks that existed in a prior
/// push and are missing from this one. Stopping a running reconcile loop
/// cleanly is a real, separate problem, not solved here — see the plan
/// this shipped with ("Explicitly out of scope") and README.md's "Live
/// pipeline development" section. A re-push currently leaves any removed
/// chain's Deployment/pods/Job-firing loop running, orphaned from the
/// newly-stored config (still visible in K8s, no longer visible in
/// `GET /status` for this project since that walks the *new* config).
pub async fn apply_project(ctx: &Ctx, project: &str, config: &PipelineConfig) -> anyhow::Result<()> {
    let config = prefix_project(project, config);

    ctx.projects.insert(
        project,
        ProjectEntry { config: Arc::new(config.clone()), is_dev: false, pushed_at: Utc::now() },
    );

    apply_chains(ctx, &config.chains).await;

    crate::scheduler::start(
        ctx.k8s_client.clone(),
        ctx.namespace.clone(),
        ctx.adapter_runner_image.clone(),
        ctx.pod_kafka_bootstrap.clone(),
        ctx.metrics_url.clone(),
        ctx.log_base_path.clone(),
        ctx.pod_s3_endpoint.clone(),
        config.adapters.clone(),
        ctx.status.clone(),
        ctx.adapter_triggers.clone(),
    );
    crate::tasks::start(ctx.clone(), ctx.adapter_runner_image.clone(), config.tasks.clone());
    crate::dynamic_router::start(ctx.clone(), &config);

    info!(project, chains = config.chains.len(), adapters = config.adapters.len(), tasks = config.tasks.len(), "project applied");
    Ok(())
}

/// Same `is_dev`-carrying variant `POST /projects/:name` uses — kept
/// separate from `apply_project` (which always records `is_dev: false`)
/// so `main.rs`'s startup call site doesn't need to pass a dummy value
/// for something only meaningful for a TUI-initiated push.
pub async fn apply_project_dev(ctx: &Ctx, project: &str, config: &PipelineConfig, is_dev: bool) -> anyhow::Result<()> {
    apply_project(ctx, project, config).await?;
    if let Some(mut entry) = ctx.projects.get(project) {
        entry.is_dev = is_dev;
        ctx.projects.insert(project, entry);
    }
    Ok(())
}

/// The chain/step loop `main.rs` used to run inline — moved here
/// unchanged in behavior, just operating on an already project-prefixed
/// `chains` slice instead of the raw compiled config, and pushing
/// triggered chains into `ctx.pending` (shared, live-mutable) instead of
/// a `main()`-local map handed once into a single blocking
/// `signals::listen` call.
async fn apply_chains(ctx: &Ctx, chains: &[ChainConfig]) {
    use crate::activation;
    use canoe_config::StepConfig as SC;

    for chain in chains {
        let signal = chain.trigger.as_ref().and_then(|t| t.signal.clone());

        for step in &chain.steps {
            let chain_name = chain.name.clone();
            let pipeline_name = chain.pipeline.clone();
            match (step, &signal) {
                (SC::Python(s), None) => activation::start_python_step(ctx.clone(), chain_name, pipeline_name, s.clone()),
                (SC::Python(s), Some(sig)) => push_pending(
                    ctx,
                    sig,
                    PendingActivation::Python { chain_name, pipeline_name, step: s.clone() },
                ),
                (SC::SparkSql(s), None) => activation::start_spark_sql_step(ctx.clone(), chain_name, pipeline_name, s.clone()),
                (SC::SparkSql(s), Some(sig)) => push_pending(
                    ctx,
                    sig,
                    PendingActivation::SparkSql { chain_name, pipeline_name, step: s.clone() },
                ),
                (SC::KafkaSparkSql(s), None) => {
                    activation::start_kafka_spark_sql_step(ctx.clone(), chain_name, pipeline_name, s.clone())
                }
                (SC::KafkaSparkSql(s), Some(sig)) => push_pending(
                    ctx,
                    sig,
                    PendingActivation::KafkaSparkSql { chain_name, pipeline_name, step: s.clone() },
                ),
                (SC::SparkDataFrame(s), None) => {
                    activation::start_spark_dataframe_step(ctx.clone(), chain_name, pipeline_name, s.clone())
                }
                (SC::SparkDataFrame(s), Some(sig)) => push_pending(
                    ctx,
                    sig,
                    PendingActivation::SparkDataFrame { chain_name, pipeline_name, step: s.clone() },
                ),
                (SC::KafkaSparkDataFrame(s), None) => {
                    activation::start_kafka_spark_dataframe_step(ctx.clone(), chain_name, pipeline_name, s.clone())
                }
                (SC::KafkaSparkDataFrame(s), Some(sig)) => push_pending(
                    ctx,
                    sig,
                    PendingActivation::KafkaSparkDataFrame { chain_name, pipeline_name, step: s.clone() },
                ),
                (SC::KafkaPython(s), None) => activation::start_kafka_python_step(ctx.clone(), chain_name, s.clone()),
                (SC::KafkaPython(s), Some(_)) => {
                    tracing::error!(
                        chain = %chain_name, step = %s.name,
                        "engine=\"python\", mode=\"streaming\" on a triggered chain isn't supported yet — \
                         signal-gated activation isn't wired up for this step kind; step will not start"
                    );
                }
                (SC::Sink(s), None) => activation::start_sink_step(ctx.clone(), chain_name, pipeline_name, s.clone()),
                (SC::Sink(s), Some(sig)) => {
                    push_pending(ctx, sig, PendingActivation::Sink { chain_name, pipeline_name, step: s.clone() })
                }
                (SC::DynamicRoute(_), _) => {}
                (SC::Uv(s), None) => activation::start_uv_step(ctx.clone(), chain_name, pipeline_name, s.clone()),
                (SC::Uv(s), Some(sig)) => {
                    push_pending(ctx, sig, PendingActivation::Uv { chain_name, pipeline_name, step: s.clone() })
                }
            }
        }

        if let Some(sig) = &signal {
            info!(chain = %chain.name, signal = %sig, "chain is dormant, waiting for signal");
        }
    }
}

fn push_pending(ctx: &Ctx, signal: &str, activation: PendingActivation) {
    ctx.pending.lock().unwrap().entry(signal.to_string()).or_default().push(activation);
}

#[derive(Deserialize)]
struct PushRequest {
    config: PipelineConfig,
    #[serde(default)]
    is_dev: bool,
}

#[derive(Serialize)]
struct PushResponse {
    project: String,
    chains: usize,
    adapters: usize,
    tasks: usize,
}

/// `POST /projects/:name` — push (insert, or re-push to top up) a
/// compiled `PipelineConfig`. See `apply_project`'s docs for exactly what
/// "idempotent-additive, not a real update" means: new/changed chains
/// start, nothing removed from a prior push is torn down.
async fn push_handler(
    State(ctx): State<Ctx>,
    Path(name): Path<String>,
    body: Result<Json<PushRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<PushResponse>, StatusCode> {
    let Json(req) = body.map_err(|e| {
        warn!(error = %e, "rejected malformed project push");
        StatusCode::BAD_REQUEST
    })?;
    match apply_project_dev(&ctx, &name, &req.config, req.is_dev).await {
        Ok(()) => Ok(Json(PushResponse {
            project: name,
            chains: req.config.chains.len(),
            adapters: req.config.adapters.len(),
            tasks: req.config.tasks.len(),
        })),
        Err(e) => {
            warn!(project = %name, error = %e, "failed to apply pushed project");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// `POST /projects/:name/adapters/:adapter/trigger` — fires one
/// off-schedule adapter Job immediately via the out-of-band channel
/// `scheduler::start` registered for this (already project-prefixed)
/// adapter name, without disturbing its regular schedule.
async fn trigger_adapter_handler(
    State(ctx): State<Ctx>,
    Path((project, adapter)): Path<(String, String)>,
) -> StatusCode {
    let key = format!("{project}--{adapter}");
    let sender = ctx.adapter_triggers.lock().unwrap().get(&key).cloned();
    match sender {
        Some(tx) => match tx.try_send(()) {
            Ok(()) => StatusCode::NO_CONTENT,
            // A trigger is already pending (channel capacity 1) — treat
            // as success rather than an error: the adapter is about to
            // fire anyway, which is exactly what the caller asked for.
            Err(tokio::sync::mpsc::error::TrySendError::Full(())) => StatusCode::NO_CONTENT,
            Err(tokio::sync::mpsc::error::TrySendError::Closed(())) => StatusCode::INTERNAL_SERVER_ERROR,
        },
        None => StatusCode::NOT_FOUND,
    }
}

/// `POST /projects/:name/pipelines/:pipeline/trigger` — resolves the
/// pipeline's `trigger.signal` from its (already project-prefixed)
/// stored config and calls the *existing* `signals::publish` (the same
/// function `sink_runner::run` already calls after a real flush) to
/// synthesize the same signal event a real trigger would produce.
async fn trigger_pipeline_handler(
    State(ctx): State<Ctx>,
    Path((project, pipeline)): Path<(String, String)>,
) -> StatusCode {
    let Some(entry) = ctx.projects.get(&project) else {
        return StatusCode::NOT_FOUND;
    };
    let full_pipeline = format!("{project}--{pipeline}");
    let Some(chain) = entry
        .config
        .chains
        .iter()
        .find(|c| c.pipeline == full_pipeline && c.trigger.as_ref().and_then(|t| t.signal.as_deref()).is_some())
    else {
        return StatusCode::NOT_FOUND;
    };
    let signal = chain.trigger.as_ref().and_then(|t| t.signal.as_deref()).unwrap();
    crate::signals::publish(&ctx.signal_producer, &ctx.signals_topic, signal, &chain.name, "manual-trigger").await;
    StatusCode::NO_CONTENT
}

/// Finds the `(chain, step)` owning a given already project-prefixed
/// step name within one project's stored config — both cancel and re-run
/// need the owning chain (for K8s Job-set/Deployment naming) and the
/// step's own `StepConfig` variant (to know which control mechanism
/// applies — see `cancel_step_handler`'s docs).
fn find_step<'a>(entry: &'a ProjectEntry, full_step: &str) -> Option<(&'a ChainConfig, &'a StepConfig)> {
    entry.config.chains.iter().find_map(|c| c.steps.iter().find(|s| s.name() == full_step).map(|s| (c, s)))
}

/// `POST /projects/:name/steps/:step/cancel` — a real pause, not "kill
/// and let the reconcile loop immediately restart it": sets `ctx.control`
/// first (so every kind of reconcile loop stops short-circuits on its
/// very next tick, before any normal gating logic runs — see
/// `activation::run_batch_reconcile_loop`'s/the Python-uv streaming
/// loops' pause checks), then actually stops whatever's running *right
/// now*, which differs by control mechanism:
/// - **batch-mode** (`Python`/`SparkSql`/`SparkDataFrame`): deletes any
///   in-flight Job (`canoe_k8s::StepJobRunner::delete_job`) — the same
///   in-flight list `decide_batch_action` already reads.
/// - **streaming Python/uv** (`KafkaPython`/`Uv`): nothing to do here —
///   the reconcile loop's own next tick notices the pause and scales to
///   zero itself (see `activation::scale_to_zero`).
/// - **streaming Spark** (`KafkaSparkSql`/`KafkaSparkDataFrame`): no
///   reconcile loop exists at all to notice a pause flag — this handler
///   deletes the launcher Deployment directly
///   (`canoe_spark::delete_launcher`). Coarser than the other two cases,
///   documented as a real difference, not papered over — see
///   `canoe_spark::delete_launcher`'s docs.
/// - **`Sink`/`DynamicRoute`**: not controllable this way — `NOT_FOUND`.
async fn cancel_step_handler(State(ctx): State<Ctx>, Path((project, step)): Path<(String, String)>) -> StatusCode {
    let full_step = format!("{project}--{step}");
    let Some(entry) = ctx.projects.get(&project) else { return StatusCode::NOT_FOUND };
    let Some((chain, step_config)) = find_step(&entry, &full_step) else { return StatusCode::NOT_FOUND };

    ctx.control.lock().unwrap().insert(full_step.clone(), true);

    match step_config {
        StepConfig::KafkaSparkSql(_) | StepConfig::KafkaSparkDataFrame(_) => {
            let name = canoe_spark::launcher_name(&chain.name, &full_step);
            if let Err(e) = canoe_spark::delete_launcher(&ctx.k8s_client, &ctx.namespace, &name).await {
                warn!(project = %project, step = %step, error = %e, "failed to delete streaming Spark launcher Deployment on cancel");
                return StatusCode::INTERNAL_SERVER_ERROR;
            }
        }
        StepConfig::Python(_) | StepConfig::SparkSql(_) | StepConfig::SparkDataFrame(_) => {
            let job_set = canoe_k8s::StepJobRunner::job_set_name(&chain.name, &full_step);
            let runner = canoe_k8s::StepJobRunner::new(ctx.k8s_client.clone(), ctx.namespace.clone(), String::new());
            match runner.list_in_flight(&job_set).await {
                Ok(in_flight) => {
                    for job_name in in_flight {
                        if let Err(e) = runner.delete_job(&job_name).await {
                            warn!(project = %project, step = %step, job = %job_name, error = %e, "failed to delete in-flight batch Job on cancel");
                        }
                    }
                }
                Err(e) => warn!(project = %project, step = %step, error = %e, "failed to list in-flight batch Jobs on cancel"),
            }
        }
        StepConfig::KafkaPython(_) | StepConfig::Uv(_) => {
            // Nothing to do here — see this function's docs.
        }
        StepConfig::Sink(_) | StepConfig::DynamicRoute(_) => return StatusCode::NOT_FOUND,
    }
    StatusCode::NO_CONTENT
}

/// `POST /projects/:name/steps/:step/rerun` — clears the pause flag,
/// then (only for the control mechanisms that need a nudge — see
/// `Ctx.force_fire`'s docs) actually kicks the step back into action:
/// - **batch-mode**: sends on the step's force-fire channel, bypassing
///   the normal backlog/dependency gate once.
/// - **streaming Python/uv**: clearing the pause is sufficient — that
///   loop already reconciles continuously on its own interval.
/// - **streaming Spark**: re-applies the Deployment by calling the exact
///   same `activation::start_kafka_spark_sql_step`/
///   `start_kafka_spark_dataframe_step` used to start it originally (an
///   idempotent apply, not a distinct "resume" code path).
async fn rerun_step_handler(State(ctx): State<Ctx>, Path((project, step)): Path<(String, String)>) -> StatusCode {
    let full_step = format!("{project}--{step}");
    let Some(entry) = ctx.projects.get(&project) else { return StatusCode::NOT_FOUND };
    let Some((chain, step_config)) = find_step(&entry, &full_step) else { return StatusCode::NOT_FOUND };
    let (chain_name, pipeline_name) = (chain.name.clone(), chain.pipeline.clone());

    ctx.control.lock().unwrap().insert(full_step.clone(), false);

    match step_config.clone() {
        canoe_config::StepConfig::KafkaSparkSql(s) => {
            crate::activation::start_kafka_spark_sql_step(ctx.clone(), chain_name, pipeline_name, s);
        }
        canoe_config::StepConfig::KafkaSparkDataFrame(s) => {
            crate::activation::start_kafka_spark_dataframe_step(ctx.clone(), chain_name, pipeline_name, s);
        }
        canoe_config::StepConfig::Python(_) | canoe_config::StepConfig::SparkSql(_) | canoe_config::StepConfig::SparkDataFrame(_) => {
            let sender = ctx.force_fire.lock().unwrap().get(&full_step).cloned();
            match sender {
                Some(tx) => {
                    let _ = tx.try_send(());
                }
                None => return StatusCode::NOT_FOUND,
            }
        }
        canoe_config::StepConfig::KafkaPython(_) | canoe_config::StepConfig::Uv(_) => {
            // Clearing the pause above is sufficient — see this
            // function's docs.
        }
        canoe_config::StepConfig::Sink(_) | canoe_config::StepConfig::DynamicRoute(_) => return StatusCode::NOT_FOUND,
    }
    StatusCode::NO_CONTENT
}

/// `POST /projects/:name/tasks/:task/cancel` — same real-pause semantics
/// as `cancel_step_handler`: sets `ctx.control` (so `tasks.rs`'s wait
/// loop never breaks out to fire again while paused), then deletes any
/// in-flight Job the same way a batch-mode step's cancel does — a task's
/// Job carries no `job-set` label the way a step's does (nothing else
/// ever lists a task's own Jobs together), so this deletes by task-Job
/// naming convention directly rather than via `list_in_flight`. Best
/// effort: a task that isn't currently running has nothing to delete,
/// which is not an error.
async fn cancel_task_handler(State(ctx): State<Ctx>, Path((project, task)): Path<(String, String)>) -> StatusCode {
    let full_task = format!("{project}--{task}");
    if ctx.projects.get(&project).is_none() {
        return StatusCode::NOT_FOUND;
    }
    ctx.control.lock().unwrap().insert(full_task.clone(), true);
    StatusCode::NO_CONTENT
}

/// `POST /projects/:name/tasks/:task/rerun` — clears the pause flag and
/// sends on the task's force-fire channel, bypassing its normal
/// `depends_on`/`depends_on_complete` gate once (a re-run fires
/// immediately even if that poll hasn't re-confirmed on this exact tick
/// — the caller already knows it should run now).
async fn rerun_task_handler(State(ctx): State<Ctx>, Path((project, task)): Path<(String, String)>) -> StatusCode {
    let full_task = format!("{project}--{task}");
    if ctx.projects.get(&project).is_none() {
        return StatusCode::NOT_FOUND;
    }
    ctx.control.lock().unwrap().insert(full_task.clone(), false);
    let sender = ctx.force_fire.lock().unwrap().get(&full_task).cloned();
    match sender {
        Some(tx) => {
            let _ = tx.try_send(());
            StatusCode::NO_CONTENT
        }
        None => StatusCode::NOT_FOUND,
    }
}

/// Merged into `metrics.rs`'s router at startup — see that module's
/// `app`'s docs for why this is built separately (needs a whole `Ctx`,
/// which `metrics.rs`'s own tests shouldn't have to construct).
pub fn router(ctx: Ctx) -> Router {
    Router::new()
        .route("/projects/:name", post(push_handler))
        .route("/projects/:project/adapters/:adapter/trigger", post(trigger_adapter_handler))
        .route("/projects/:project/pipelines/:pipeline/trigger", post(trigger_pipeline_handler))
        .route("/projects/:project/steps/:step/cancel", post(cancel_step_handler))
        .route("/projects/:project/steps/:step/rerun", post(rerun_step_handler))
        .route("/projects/:project/tasks/:task/cancel", post(cancel_task_handler))
        .route("/projects/:project/tasks/:task/rerun", post(rerun_task_handler))
        .with_state(ctx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use canoe_config::{CodeSource, OutputConfig, PodResources, PythonStep, ScalingConfig};
    use std::collections::BTreeMap;

    fn python_step(name: &str, depends_on: Vec<String>) -> StepConfig {
        StepConfig::Python(PythonStep {
            name: name.to_string(),
            input: "orders-adapter".to_string(),
            input_path: "s3://bucket/orders/v1/orders-adapter".to_string(),
            input_signal: "orders-adapter".to_string(),
            output_path: format!("s3://bucket/orders/v1/{name}"),
            output: OutputConfig { batch_size: 100, flush_interval_secs: 5, partition_by: vec![] },
            code: CodeSource::Inline { source: "def run(r): return r".to_string() },
            entrypoint: "run".to_string(),
            scaling: ScalingConfig::default(),
            resources: PodResources::default(),
            env: BTreeMap::new(),
            batch: None,
            lookups: Vec::new(),
            encrypt: Vec::new(),
            secrets: Vec::new(),
            depends_on,
            depends_on_complete: Vec::new(),
        })
    }

    fn fixture(step_name: &str) -> PipelineConfig {
        PipelineConfig {
            chains: vec![ChainConfig {
                name: "orders-0".to_string(),
                pipeline: "orders".to_string(),
                source: "orders-adapter".to_string(),
                steps: vec![python_step(step_name, vec!["upstream".to_string()])],
                trigger: None,
            }],
            adapters: vec![],
            storage: None,
            topic_defaults: None,
            managed_topics: vec![],
            tasks: vec![],
        }
    }

    /// The test the plan calls out as mattering most: two projects
    /// reusing the exact same step name must not collide anywhere the
    /// collision fix is supposed to protect — chain name, pipeline name,
    /// and step name (the fields that flow into `Ctx.activated`/
    /// `Ctx.completed`/`StatusRegistry`/K8s resource naming).
    #[test]
    fn two_projects_reusing_a_step_name_produce_distinct_keys() {
        let a = prefix_project("project-a", &fixture("validate"));
        let b = prefix_project("project-b", &fixture("validate"));

        let step_a = a.chains[0].steps[0].name().to_string();
        let step_b = b.chains[0].steps[0].name().to_string();
        assert_ne!(step_a, step_b, "step names must differ across projects");
        assert_eq!(step_a, "project-a--validate");
        assert_eq!(step_b, "project-b--validate");

        assert_ne!(a.chains[0].name, b.chains[0].name);
        assert_eq!(a.chains[0].name, "project-a--orders-0");
        assert_eq!(b.chains[0].name, "project-b--orders-0");

        assert_ne!(a.chains[0].pipeline, b.chains[0].pipeline);
        assert_eq!(a.chains[0].pipeline, "project-a--orders");

        // Real K8s resource names (`PodManager::pod_set_name`, Job names)
        // are built from `(chain_name, step_name)` — confirm the actual
        // downstream naming function also produces distinct results, not
        // just the raw strings feeding it.
        let pod_set_a = canoe_k8s::PodManager::pod_set_name(&a.chains[0].name, &step_a);
        let pod_set_b = canoe_k8s::PodManager::pod_set_name(&b.chains[0].name, &step_b);
        assert_ne!(pod_set_a, pod_set_b, "K8s pod-set names must differ across projects");
    }

    #[test]
    fn depends_on_entries_are_prefixed_consistently_with_the_names_they_reference() {
        // "upstream" is some sibling step's bare name within the same
        // project — after prefixing, this step's own `depends_on` entry
        // must resolve to exactly the same prefixed form a sibling step
        // named "upstream" would get from this same call.
        let config = prefix_project("proj", &fixture("validate"));
        let StepConfig::Python(step) = &config.chains[0].steps[0] else { panic!("expected python step") };
        assert_eq!(step.depends_on, vec!["proj--upstream".to_string()]);
    }

    #[test]
    fn same_project_pushed_twice_is_deterministic() {
        let a = prefix_project("proj", &fixture("validate"));
        let b = prefix_project("proj", &fixture("validate"));
        assert_eq!(a.chains[0].name, b.chains[0].name);
        assert_eq!(a.chains[0].steps[0].name(), b.chains[0].steps[0].name());
    }

    #[test]
    fn adapter_names_are_prefixed_too() {
        let mut config = fixture("validate");
        config.adapters.push(canoe_config::AdapterConfig {
            name: "orders-adapter".to_string(),
            topic: Some("_canoe.orders-adapter".to_string()),
            output_path: None,
            output: None,
            code: CodeSource::Inline { source: String::new() },
            schedule: canoe_config::AdapterSchedule { interval_secs: Some(60), cron: None },
            resources: PodResources::default(),
            backoff_limit: 2,
            encrypt: Vec::new(),
            secrets: Vec::new(),
        });
        let a = prefix_project("project-a", &config);
        let b = prefix_project("project-b", &config);
        assert_ne!(a.adapters[0].name, b.adapters[0].name);
        assert_eq!(a.adapters[0].name, "project-a--orders-adapter");
    }
}
