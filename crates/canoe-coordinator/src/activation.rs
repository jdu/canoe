//! Starting a chain's steps, either immediately at coordinator startup
//! (the common case) or later, once a signal a `schedule { signal = ... }`
//! block named actually fires — see `signals.rs` for how that arrives.
//!
//! "Starting" a step means: for Python steps, applying its Deployment and
//! kicking off its lag-driven autoscaler loop; for Spark steps, applying
//! its spark-submit launcher Deployment (a one-shot, idempotent apply —
//! the launcher itself runs forever once created); for a sink, spawning
//! its consume-buffer-flush loop. A chain with no `trigger` gets all of
//! this immediately; a triggered chain instead sits fully dormant (no
//! Deployment even created) until `signals::listen` calls `activate`.

use crate::status::{StatusRegistry, StepStatus};
use canoe_config::{
    EventLogConfig, EventLogOverride, GlobalConfig, KafkaPythonStep, KafkaSparkDataFrameStep, KafkaSparkSqlStep,
    PythonStep, SinkStep, SparkDataFrameStep, SparkSqlStep, UvStep,
};
use canoe_k8s::{Autoscaler, JobStatus, PodManager, S3BacklogInspector, StepJobRunner};
use canoe_kafka::LagInspector;
use canoe_spark::SparkLauncher;
use chrono::Utc;
use kube::Client;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{error, info, warn};

#[derive(Clone)]
pub struct Ctx {
    pub k8s_client: Client,
    pub namespace: String,
    pub worker_image: String,
    /// Image for `engine = "uv"` steps (bundles `uv`; see
    /// workers/uv-step-worker) — distinct from `worker_image` since a uv
    /// step's script is the whole program (its own consume/produce loop),
    /// not a `def run(record)` called from inside the regular worker.
    pub uv_worker_image: String,
    /// Pushed to every task as `CANOE_METRICS_URL` — see `metrics.rs`.
    pub metrics_url: String,
    /// The coordinator's *own* address for its direct Kafka use
    /// (`LagInspector`, the signal listener/publisher) — see
    /// `pod_kafka_bootstrap` for what gets baked into every worker
    /// pod/Job's `CANOE_KAFKA_BOOTSTRAP` instead (usually identical;
    /// only diverges under a split-network topology — see
    /// `main.rs`'s `Cli::pod_kafka_bootstrap` docs).
    pub kafka_bootstrap: String,
    pub pod_kafka_bootstrap: String,
    /// S3 endpoint override for every worker pod/Job's `AWS_ENDPOINT`
    /// env var — `None` forwards the coordinator's own `AWS_ENDPOINT`
    /// process env var unchanged (see `crates/canoe-k8s/src/s3_env.rs`).
    /// The coordinator's own direct S3 calls always just read
    /// `AWS_ENDPOINT` from its own environment via `object_store`,
    /// unaffected by this field.
    pub pod_s3_endpoint: Option<String>,
    pub k8s_api_server: String,
    pub spark_service_account: String,
    pub reconcile_interval: Duration,
    pub signals_topic: String,
    /// Pushed to every task as `CANOE_LOG_BASE_PATH` when set — see
    /// `main.rs`'s `Cli::log_base_path` docs.
    pub log_base_path: Option<String>,
    /// Every step, dynamic_route, and pipeline name the coordinator has
    /// applied (a Deployment/launcher successfully created, or a
    /// dynamic_route's discovery loop started) — checked by
    /// `crates/canoe-coordinator/src/tasks.rs` to resolve a `task`'s
    /// `depends_on` entries that name one of these. Never removed from:
    /// a name that's ever been activated stays "activated" for the rest
    /// of the coordinator's lifetime, matching the "applied, not ready"
    /// semantics documented on `canoe_config::TaskConfig::depends_on`.
    pub activated: Arc<Mutex<HashSet<String>>>,
    /// Every step/task name whose Job has *actually succeeded* at least
    /// once — unlike `activated` ("applied," never removed, true forever
    /// once set), this is the real completion signal a `depends_on_complete`
    /// entry (see `dependencies_satisfied`) polls. Only batch-mode steps
    /// (one-shot Jobs — `run_batch_reconcile_loop`) and tasks
    /// (`tasks.rs`) can ever populate this — a streaming-mode step has no
    /// "done" to report, and `canoe-dsl` rejects a `depends_on_complete`
    /// entry naming one at compile time.
    pub completed: Arc<Mutex<HashSet<String>>>,
    /// Latest-snapshot-per-step/adapter status, served by `crate::status`'s
    /// `GET /status` — see its module docs.
    pub status: Arc<StatusRegistry>,
    /// The coordinator's optional `--global-config` file, loaded once at
    /// startup — `Default`/empty when the flag is omitted, so every
    /// consumer of this (today: `resolve_event_log`) degrades to "off"
    /// cleanly rather than needing to special-case an absent file. See
    /// `main.rs`'s `Cli::global_config` docs.
    pub global: Arc<GlobalConfig>,
    /// Live registry of every project the coordinator knows about — see
    /// `crate::projects`'s module docs. `"default"` (from `--config` at
    /// startup) plus anything pushed later via `POST /projects/:name`.
    pub projects: Arc<crate::projects::ProjectRegistry>,
    /// Every triggered chain still waiting on its `trigger.signal`,
    /// shared and live-mutable (not a `main()`-local snapshot moved once
    /// into a single blocking call) so a chain pushed by a project
    /// *after* the coordinator has been running for a while is still
    /// picked up by the same long-running `signals::listen` loop — see
    /// that module's docs for the exact read/drain behavior.
    pub pending: Arc<Mutex<HashMap<String, Vec<PendingActivation>>>>,
    /// Per-adapter (already project-prefixed, `{project}--{adapter}`)
    /// out-of-band trigger channel — `POST /projects/:name/adapters/:adapter/trigger`
    /// sends on the matching sender; `scheduler::run_adapter_loop` races
    /// its receiver against its own ticker/sleep via `tokio::select!` to
    /// fire one off-schedule run without disturbing the regular schedule.
    /// Populated by `scheduler::start` on every `apply_project` call
    /// (including the startup one), never removed from.
    pub adapter_triggers: Arc<Mutex<HashMap<String, tokio::sync::mpsc::Sender<()>>>>,
    /// Container image used for every adapter's (and task's) one-shot
    /// Job — moved here from a `main()`-local variable since
    /// `apply_project` needs it on every push, not only at startup.
    pub adapter_runner_image: String,
    /// The coordinator's own producer for publishing to the signals
    /// topic directly — used by `POST /projects/:name/pipelines/:pipeline/trigger`
    /// to call `signals::publish` the same way `sink_runner::run` already
    /// does after a real flush. Constructed once at startup (same
    /// `kafka_bootstrap` every other direct Kafka use on `Ctx` shares).
    pub signal_producer: Arc<rdkafka::producer::FutureProducer>,
    /// "Is this (project-prefixed) name paused" — separate from
    /// `StatusRegistry`, deliberately: every reconcile loop only ever
    /// *reads* this (`is_paused`), and only the new cancel/re-run
    /// endpoints (`crate::projects`) ever write it. `StatusRegistry`
    /// entries are upserted wholesale by reconcile loops every tick
    /// (computed replica/backlog/etc.) — mixing a flag only a handler
    /// should set into that same map would create a read-modify-write
    /// race between "loop reports its latest observation" and "handler
    /// sets pause state." In-memory only, same as `activated`/`completed`/
    /// the project registry itself — not persisted across a restart.
    pub control: Arc<Mutex<HashMap<String, bool>>>,
    /// Per-step/task (already project-prefixed) force-fire channel — the
    /// exact same pattern `adapter_triggers` established: re-run sends on
    /// the matching sender to bypass a batch-mode step's/task's normal
    /// backlog-or-dependency gate once, without disturbing anything else
    /// about the loop. Streaming Python/uv steps don't need one (clearing
    /// `control` is enough — that loop already reconciles continuously on
    /// its own interval); Spark streaming doesn't need one either (no
    /// reconcile loop to interrupt — re-run just re-applies the
    /// Deployment directly).
    pub force_fire: Arc<Mutex<HashMap<String, tokio::sync::mpsc::Sender<()>>>>,
}

/// Whether `name` is currently paused — the one place every reconcile
/// loop reads `ctx.control`, so the "reconcile loops never write this"
/// invariant is easy to audit (this function never takes `&mut`).
pub fn is_paused(control: &Mutex<HashMap<String, bool>>, name: &str) -> bool {
    control.lock().unwrap().get(name).copied().unwrap_or(false)
}

/// Folds one reconcile pass's outcome into the status registry — shared by
/// `start_python_step`/`start_uv_step`/`start_kafka_python_step`, the three
/// step kinds with a live per-tick `Autoscaler::reconcile_*` loop. On
/// success, records the fresh `ReconcileReport` verbatim; on failure,
/// preserves whatever replica/backlog/lag numbers were last known (rather
/// than blanking them to `None`) so a single failed pass doesn't make a
/// healthy-looking step suddenly appear to have zero replicas.
fn record_reconcile(ctx: &Ctx, pipeline_name: &str, chain_name: &str, step_name: &str, kind: &str, result: anyhow::Result<canoe_k8s::ReconcileReport>) {
    match result {
        Ok(report) => ctx.status.update_step(StepStatus {
            pipeline: pipeline_name.to_string(),
            chain: chain_name.to_string(),
            name: step_name.to_string(),
            kind: kind.to_string(),
            desired_replicas: Some(report.desired_replicas),
            alive_replicas: Some(report.alive_replicas),
            circuit_open: report.circuit_open,
            backlog: report.backlog,
            lag: report.lag,
            last_reconciled: Utc::now(),
            last_error: None,
            paused: false,
        }),
        Err(e) => {
            warn!(step = %step_name, error = %e, "autoscale reconcile failed");
            let prior = ctx.status.step(step_name);
            ctx.status.update_step(StepStatus {
                pipeline: pipeline_name.to_string(),
                chain: chain_name.to_string(),
                name: step_name.to_string(),
                kind: kind.to_string(),
                desired_replicas: prior.as_ref().and_then(|p| p.desired_replicas),
                alive_replicas: prior.as_ref().and_then(|p| p.alive_replicas),
                circuit_open: prior.as_ref().map(|p| p.circuit_open).unwrap_or(false),
                backlog: prior.as_ref().and_then(|p| p.backlog),
                lag: prior.as_ref().and_then(|p| p.lag),
                last_reconciled: Utc::now(),
                last_error: Some(e.to_string()),
                paused: false,
            });
        }
    }
}

/// Marks `step_name` and `pipeline_name` as activated. `pipeline_name` is
/// `ChainConfig.pipeline` (a real field, threaded through every
/// activation call site below) — previously this recovered the pipeline
/// name by stripping chain_name's trailing `-<digits>` counter suffix
/// (`canoe_dsl::compiler::build_chains` always compiles a chain name as
/// `"{pipeline_name}-{counter}"`), a wart now that the compiler carries
/// the real name through directly.
pub fn mark_activated(ctx: &Ctx, pipeline_name: &str, step_name: &str) {
    let mut activated = ctx.activated.lock().unwrap();
    activated.insert(step_name.to_string());
    activated.insert(pipeline_name.to_string());
}

/// Marks `name` as *completed* — its Job has actually succeeded, at least
/// once. See `Ctx.completed`'s docs for how this differs from `activated`.
pub fn mark_completed(ctx: &Ctx, name: &str) {
    ctx.completed.lock().unwrap().insert(name.to_string());
}

/// Status bookkeeping for a batch-mode step's Job-firing reconcile loop —
/// the Job-based equivalent of `record_reconcile`/`applied_spark_status`.
/// `running`: whether a Job is currently tracked as in flight this tick
/// (there's no meaningful "desired replicas" for a one-shot Job the way a
/// persistent pod set has one, so `alive_replicas` is repurposed as a
/// simple 0/1 "is something running right now").
fn record_batch_status(
    ctx: &Ctx,
    pipeline_name: &str,
    chain_name: &str,
    step_name: &str,
    kind: &str,
    running: bool,
    backlog: Option<i64>,
    last_error: Option<String>,
) {
    ctx.status.update_step(StepStatus {
        pipeline: pipeline_name.to_string(),
        chain: chain_name.to_string(),
        name: step_name.to_string(),
        kind: kind.to_string(),
        desired_replicas: None,
        alive_replicas: Some(if running { 1 } else { 0 }),
        circuit_open: false,
        backlog,
        lag: None,
        last_reconciled: Utc::now(),
        last_error,
        paused: false,
    });
}

/// A step that belongs to a triggered chain, parked until its pipeline's
/// signal fires.
pub enum PendingActivation {
    Python { chain_name: String, pipeline_name: String, step: PythonStep },
    /// Batch-mode (`mode = "batch"`, the default) — see `start_spark_sql_step`.
    SparkSql { chain_name: String, pipeline_name: String, step: SparkSqlStep },
    /// Streaming-mode (`mode = "streaming"`) — see `start_kafka_spark_sql_step`.
    KafkaSparkSql { chain_name: String, pipeline_name: String, step: KafkaSparkSqlStep },
    /// Batch-mode — see `start_spark_dataframe_step`.
    SparkDataFrame { chain_name: String, pipeline_name: String, step: SparkDataFrameStep },
    /// Streaming-mode — see `start_kafka_spark_dataframe_step`.
    KafkaSparkDataFrame { chain_name: String, pipeline_name: String, step: KafkaSparkDataFrameStep },
    Sink { chain_name: String, pipeline_name: String, step: SinkStep },
    Uv { chain_name: String, pipeline_name: String, step: UvStep },
}

impl PendingActivation {
    pub fn describe(&self) -> (&str, &str) {
        match self {
            PendingActivation::Python { chain_name, step, .. } => (chain_name.as_str(), step.name.as_str()),
            PendingActivation::SparkSql { chain_name, step, .. } => (chain_name.as_str(), step.name.as_str()),
            PendingActivation::KafkaSparkSql { chain_name, step, .. } => (chain_name.as_str(), step.name.as_str()),
            PendingActivation::SparkDataFrame { chain_name, step, .. } => {
                (chain_name.as_str(), step.name.as_str())
            }
            PendingActivation::KafkaSparkDataFrame { chain_name, step, .. } => {
                (chain_name.as_str(), step.name.as_str())
            }
            PendingActivation::Sink { chain_name, step, .. } => (chain_name.as_str(), step.name.as_str()),
            PendingActivation::Uv { chain_name, step, .. } => (chain_name.as_str(), step.name.as_str()),
        }
    }

    pub fn activate(self, ctx: &Ctx) {
        match self {
            PendingActivation::Python { chain_name, pipeline_name, step } => {
                start_python_step(ctx.clone(), chain_name, pipeline_name, step)
            }
            PendingActivation::SparkSql { chain_name, pipeline_name, step } => {
                start_spark_sql_step(ctx.clone(), chain_name, pipeline_name, step)
            }
            PendingActivation::KafkaSparkSql { chain_name, pipeline_name, step } => {
                start_kafka_spark_sql_step(ctx.clone(), chain_name, pipeline_name, step)
            }
            PendingActivation::SparkDataFrame { chain_name, pipeline_name, step } => {
                start_spark_dataframe_step(ctx.clone(), chain_name, pipeline_name, step)
            }
            PendingActivation::KafkaSparkDataFrame { chain_name, pipeline_name, step } => {
                start_kafka_spark_dataframe_step(ctx.clone(), chain_name, pipeline_name, step)
            }
            PendingActivation::Sink { chain_name, pipeline_name, step } => {
                start_sink_step(ctx.clone(), chain_name, pipeline_name, step)
            }
            PendingActivation::Uv { chain_name, pipeline_name, step } => {
                start_uv_step(ctx.clone(), chain_name, pipeline_name, step)
            }
        }
    }
}

/// Batch-mode (`python { mode = "batch" }`, the default): fires one
/// one-shot K8s Job per execution — drains currently-available S3 backlog,
/// exits — instead of maintaining a persistent pod set. This is the
/// collapse of "micro-batch" (a Pod that loops forever, polling bounded
/// batches) into "batch" (a Job that runs once and stops): the reconcile
/// loop below still ticks on `ctx.reconcile_interval`, same cadence as
/// before, but each tick either polls the currently-tracked Job to
/// completion or, once nothing's in flight, checks `S3BacklogInspector`
/// and fires a new one if there's real work — never both in the same
/// tick, so this step's Jobs never overlap. Job success is a free,
/// native "done" signal (see `mark_completed`) — no `_SUCCESS`-marker
/// mechanism needed to get it. Detached: the caller doesn't await this,
/// it runs for the lifetime of the coordinator process.
pub fn start_python_step(ctx: Ctx, chain_name: String, pipeline_name: String, step: PythonStep) {
    tokio::spawn(async move {
        let runner = StepJobRunner::new(ctx.k8s_client.clone(), ctx.namespace.clone(), ctx.worker_image.clone());
        let fire: FireFn = {
            let runner = runner.clone();
            let chain_name = chain_name.clone();
            let step = step.clone();
            let bootstrap = ctx.pod_kafka_bootstrap.clone();
            let metrics_url = ctx.metrics_url.clone();
            let log_base_path = ctx.log_base_path.clone();
            let pod_s3_endpoint = ctx.pod_s3_endpoint.clone();
            Box::new(move || {
                let runner = runner.clone();
                let chain_name = chain_name.clone();
                let step = step.clone();
                let bootstrap = bootstrap.clone();
                let metrics_url = metrics_url.clone();
                let log_base_path = log_base_path.clone();
                let pod_s3_endpoint = pod_s3_endpoint.clone();
                Box::pin(async move {
                    runner
                        .run_python_step_once(&chain_name, &step, &bootstrap, &metrics_url, log_base_path.as_deref(), pod_s3_endpoint.as_deref())
                        .await
                })
            })
        };
        run_batch_reconcile_loop(
            ctx,
            chain_name,
            pipeline_name,
            step.name.clone(),
            step.input_path.clone(),
            step.output_path.clone(),
            step.depends_on.clone(),
            step.depends_on_complete.clone(),
            "python",
            fire,
        )
        .await;
    });
}

/// Batch-mode (`spark_sql { mode = "batch" }`, the default) — same
/// Job-firing reconcile loop `start_python_step` uses
/// (`run_batch_reconcile_loop`, shared rather than copy-pasted: the two
/// only differ in *how* a Job gets fired), just via
/// `SparkLauncher::ensure_sql_batch_step` instead of
/// `StepJobRunner::run_python_step_once`.
pub fn start_spark_sql_step(ctx: Ctx, chain_name: String, pipeline_name: String, step: SparkSqlStep) {
    tokio::spawn(async move {
        let launcher = SparkLauncher::new(
            ctx.k8s_client.clone(),
            ctx.namespace.clone(),
            ctx.k8s_api_server.clone(),
            ctx.spark_service_account.clone(),
        );
        let event_log_path = resolve_event_log(
            ctx.global.spark.as_ref().and_then(|s| s.event_log.as_ref()),
            step.spark.event_log_override.as_ref(),
        );
        let fire: FireFn = {
            let launcher = launcher.clone();
            let chain_name = chain_name.clone();
            let step = step.clone();
            let bootstrap = ctx.pod_kafka_bootstrap.clone();
            let event_log_path = event_log_path.clone();
            let pod_s3_endpoint = ctx.pod_s3_endpoint.clone();
            Box::new(move || {
                let launcher = launcher.clone();
                let chain_name = chain_name.clone();
                let step = step.clone();
                let bootstrap = bootstrap.clone();
                let event_log_path = event_log_path.clone();
                let pod_s3_endpoint = pod_s3_endpoint.clone();
                Box::pin(async move {
                    launcher
                        .ensure_sql_batch_step(&chain_name, &step, &bootstrap, event_log_path.as_deref(), pod_s3_endpoint.as_deref())
                        .await
                })
            })
        };
        run_batch_reconcile_loop(
            ctx,
            chain_name,
            pipeline_name,
            step.name.clone(),
            step.input_path.clone(),
            step.output_path.clone(),
            step.depends_on.clone(),
            step.depends_on_complete.clone(),
            "spark_sql",
            fire,
        )
        .await;
    });
}

/// Batch-mode DataFrame twin of `start_spark_sql_step` — see its docs.
pub fn start_spark_dataframe_step(ctx: Ctx, chain_name: String, pipeline_name: String, step: SparkDataFrameStep) {
    tokio::spawn(async move {
        let launcher = SparkLauncher::new(
            ctx.k8s_client.clone(),
            ctx.namespace.clone(),
            ctx.k8s_api_server.clone(),
            ctx.spark_service_account.clone(),
        );
        let event_log_path = resolve_event_log(
            ctx.global.spark.as_ref().and_then(|s| s.event_log.as_ref()),
            step.spark.event_log_override.as_ref(),
        );
        let fire: FireFn = {
            let launcher = launcher.clone();
            let chain_name = chain_name.clone();
            let step = step.clone();
            let bootstrap = ctx.pod_kafka_bootstrap.clone();
            let event_log_path = event_log_path.clone();
            let pod_s3_endpoint = ctx.pod_s3_endpoint.clone();
            Box::new(move || {
                let launcher = launcher.clone();
                let chain_name = chain_name.clone();
                let step = step.clone();
                let bootstrap = bootstrap.clone();
                let event_log_path = event_log_path.clone();
                let pod_s3_endpoint = pod_s3_endpoint.clone();
                Box::pin(async move {
                    launcher
                        .ensure_dataframe_batch_step(&chain_name, &step, &bootstrap, event_log_path.as_deref(), pod_s3_endpoint.as_deref())
                        .await
                })
            })
        };
        run_batch_reconcile_loop(
            ctx,
            chain_name,
            pipeline_name,
            step.name.clone(),
            step.input_path.clone(),
            step.output_path.clone(),
            step.depends_on.clone(),
            step.depends_on_complete.clone(),
            "spark_dataframe",
            fire,
        )
        .await;
    });
}

/// Type-erased "fire one Job, return its name" closure — `start_python_step`/
/// `start_spark_sql_step`/`start_spark_dataframe_step` each build one of
/// these (capturing their own step's config and the right
/// `StepJobRunner`/`SparkLauncher` call) and hand it to
/// `run_batch_reconcile_loop`, which doesn't know or care which engine
/// it's driving. Boxed rather than generic (`impl Fn() -> impl Future`)
/// since a plain `Fn` closure returning a borrowing future runs into
/// stable Rust's usual async-closure lifetime friction; every capture is
/// cloned fresh on each call instead; boxing sidesteps that cleanly at
/// the cost of one alloc per fire (a batch step fires rarely — at most
/// once per `reconcile_interval` tick — so this is not remotely hot).
type FireFn = Box<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<String>> + Send>> + Send>;

/// Shared reconcile-loop skeleton for every batch-mode step kind. Ticks
/// on `ctx.reconcile_interval`; each tick either polls a locally-tracked
/// Job to completion, or — nothing tracked — lists in-flight Jobs and
/// checks S3 backlog, hands both to `decide_batch_action` (pure, unit-
/// tested below), and executes whatever it decides. Also gates actually
/// firing a *new* Job on `depends_on`/`depends_on_complete` being
/// satisfied (checked every tick a fire is otherwise due, not just
/// once at startup, so a step whose dependency hasn't finished yet
/// simply waits rather than erroring) — see `dependencies_satisfied`.
#[allow(clippy::too_many_arguments)]
async fn run_batch_reconcile_loop(
    ctx: Ctx,
    chain_name: String,
    pipeline_name: String,
    step_name: String,
    input_path: String,
    output_path: String,
    depends_on: Vec<String>,
    depends_on_complete: Vec<String>,
    kind: &'static str,
    fire: FireFn,
) {
    let job_set = StepJobRunner::job_set_name(&chain_name, &step_name);
    // `worker_image` is irrelevant here — this runner instance is only
    // ever used for `list_in_flight`/`job_status`/`delete_job`, never
    // `create_job` (that's `fire`'s job, built by the caller with the
    // right runner).
    let runner = StepJobRunner::new(ctx.k8s_client.clone(), ctx.namespace.clone(), String::new());

    let backlog = match S3BacklogInspector::new(&input_path) {
        Ok(b) => b,
        Err(e) => {
            error!(step = %step_name, error = %e, "failed to start S3 backlog inspector, batch execution disabled for this step");
            return;
        }
    };
    let checkpoint_path = format!("{}/.checkpoint.json", output_path.trim_end_matches('/'));

    mark_activated(&ctx, &pipeline_name, &step_name);
    info!(chain = %chain_name, step = %step_name, kind, "batch-mode step reconcile loop started");

    // Re-run's force-fire channel — same pattern `scheduler.rs`'s adapter
    // trigger channel established. `POST /projects/:name/steps/:step/rerun`
    // sends on the matching sender (looked up by this already
    // project-prefixed `step_name`) to bypass the normal backlog/
    // dependency gate once.
    let (force_tx, mut force_rx) = tokio::sync::mpsc::channel::<()>(1);
    ctx.force_fire.lock().unwrap().insert(step_name.clone(), force_tx);

    let mut current_job: Option<String> = None;
    let mut ticker = tokio::time::interval(ctx.reconcile_interval);
    loop {
        let mut forced = false;
        tokio::select! {
            _ = ticker.tick() => {}
            _ = force_rx.recv() => {
                forced = true;
                info!(chain = %chain_name, step = %step_name, "force-fire trigger received");
            }
        }

        // Pause check comes first, before any other gating logic — a
        // paused step's tick skips entirely (no backlog check, no Job
        // status poll, no firing) until explicitly re-run. If something
        // was tracked as in-flight when the pause took effect, drop that
        // tracking too: `POST .../cancel` deletes the Job directly (see
        // `crate::projects`'s cancel handler), so polling its status here
        // would just find it gone — starting fresh once resumed is
        // simpler and correct either way (the very next non-paused tick
        // re-lists in-flight Jobs from K8s directly, same as a
        // coordinator restart mid-Job already handles).
        if is_paused(&ctx.control, &step_name) {
            if current_job.take().is_some() {
                info!(chain = %chain_name, step = %step_name, "paused with a job tracked as in-flight, dropping local tracking");
            }
            continue;
        }

        if forced && current_job.is_none() {
            match fire().await {
                Ok(job_name) => {
                    info!(chain = %chain_name, step = %step_name, job = %job_name, "force-fired batch job (re-run)");
                    current_job = Some(job_name);
                }
                Err(e) => error!(chain = %chain_name, step = %step_name, error = %e, "failed to force-fire batch job"),
            }
            continue;
        }

        if let Some(job_name) = current_job.clone() {
            match runner.job_status(&job_name).await {
                Ok(JobStatus::Succeeded) => {
                    info!(chain = %chain_name, step = %step_name, job = %job_name, "batch job succeeded");
                    mark_completed(&ctx, &step_name);
                    record_batch_status(&ctx, &pipeline_name, &chain_name, &step_name, kind, false, None, None);
                    current_job = None;
                }
                Ok(JobStatus::Failed) => {
                    warn!(chain = %chain_name, step = %step_name, job = %job_name, "batch job failed");
                    record_batch_status(
                        &ctx, &pipeline_name, &chain_name, &step_name, kind, false, None,
                        Some(format!("job {job_name} failed")),
                    );
                    current_job = None;
                }
                Ok(JobStatus::Running) => {
                    record_batch_status(&ctx, &pipeline_name, &chain_name, &step_name, kind, true, None, None);
                }
                Err(e) => warn!(chain = %chain_name, step = %step_name, error = %e, "failed to poll batch job status"),
            }
            continue;
        }

        // Nothing tracked locally — check K8s directly (covers a
        // coordinator restart mid-Job) and the current S3 backlog, then
        // let `decide_batch_action` decide what to do with what was just
        // observed.
        let in_flight = match runner.list_in_flight(&job_set).await {
            Ok(names) => names,
            Err(e) => {
                warn!(chain = %chain_name, step = %step_name, error = %e, "failed to list in-flight batch jobs");
                continue;
            }
        };
        let backlog_count = match backlog.backlog_for(&input_path, &checkpoint_path).await {
            Ok(b) => b.unprocessed_run_count,
            Err(e) => {
                warn!(chain = %chain_name, step = %step_name, error = %e, "failed to check S3 backlog");
                continue;
            }
        };
        record_batch_status(&ctx, &pipeline_name, &chain_name, &step_name, kind, false, Some(backlog_count), None);

        // `paused` is always `false` here — the pause check earlier in
        // this loop already `continue`s before this point is ever
        // reached while paused (and skips this backlog/in-flight lookup
        // entirely). `decide_batch_action` still accepts it so the
        // "paused means don't fire, even with real backlog" contract is
        // independently true and unit-testable, not just an artifact of
        // this one call site's control flow.
        match decide_batch_action(false, &in_flight, backlog_count) {
            BatchAction::WaitFor(name) => current_job = Some(name),
            BatchAction::Idle => {}
            BatchAction::Fire => {
                if !dependencies_satisfied(&ctx.activated, &ctx.completed, &depends_on, &depends_on_complete) {
                    info!(chain = %chain_name, step = %step_name, "backlog ready but depends_on/depends_on_complete not yet satisfied, waiting");
                    continue;
                }
                match fire().await {
                    Ok(job_name) => {
                        info!(chain = %chain_name, step = %step_name, job = %job_name, backlog = backlog_count, "fired batch job");
                        current_job = Some(job_name);
                    }
                    Err(e) => error!(chain = %chain_name, step = %step_name, error = %e, "failed to fire batch job"),
                }
            }
        }
    }
}

/// `depends_on` (applied — `activated`) and `depends_on_complete`
/// (actually finished — `completed`) both empty is trivially satisfied.
/// Reuses the same `Arc<Mutex<HashSet<String>>>` membership check
/// `tasks.rs`'s dependency polling already does — see its module docs —
/// rather than a new mechanism; the *polling cadence* here is
/// `run_batch_reconcile_loop`'s own `ctx.reconcile_interval` tick, not a
/// separate poll loop of its own, since a batch step already ticks on
/// exactly this cadence to check backlog anyway.
///
/// Takes the two registries directly rather than a whole `&Ctx` (which
/// also carries a real `kube::Client`, awkward to construct in a test)
/// so this stays unit-testable with plain in-memory fixtures — same
/// pure-function-extraction reasoning as `decide_batch_action` above,
/// applied here since a full reconcile-loop integration test would need
/// a K8s client this codebase has no precedent for mocking.
fn dependencies_satisfied(
    activated: &Mutex<HashSet<String>>,
    completed: &Mutex<HashSet<String>>,
    depends_on: &[String],
    depends_on_complete: &[String],
) -> bool {
    let activated = activated.lock().unwrap();
    if !depends_on.iter().all(|d| activated.contains(d)) {
        return false;
    }
    drop(activated);
    let completed = completed.lock().unwrap();
    depends_on_complete.iter().all(|d| completed.contains(d))
}

/// What `start_python_step`'s reconcile loop should do next, once nothing
/// is tracked *locally* as in flight this tick — pure decision logic
/// (no I/O, no `Ctx`), extracted specifically so the two rules that
/// actually matter here (never fire a second Job while one's in flight;
/// only fire when there's real backlog) are unit-testable without a K8s
/// client or S3 fixture — see `tests::decide_batch_action_tests` below.
/// The surrounding loop is responsible for the actual K8s/S3 calls that
/// produce `in_flight`/`backlog_count`, and for actually executing
/// whichever `BatchAction` this returns.
#[derive(Debug, Clone, PartialEq, Eq)]
enum BatchAction {
    /// K8s itself reports a Job for this step still running (most likely:
    /// the coordinator restarted mid-Job) — track and poll it, don't fire
    /// a new one.
    WaitFor(String),
    /// Nothing in flight, no backlog — do nothing this tick.
    Idle,
    /// Nothing in flight, real backlog — fire a new Job.
    Fire,
}

/// Merges the coordinator's global `spark.event_log` default
/// (`ctx.global.spark.event_log`, from the optional `--global-config`
/// file) with one Spark step's own `spark { event_log { ... } }`
/// override, returning the final `spark.eventLog.dir` value to pass to
/// `canoe-spark` — `None` means "don't enable event logging for this
/// step's launch." Pure and unit-tested (no K8s client, no I/O), same
/// pattern as `decide_batch_action`/`dependencies_satisfied` above.
///
/// Merge rule (per-field, step wins): `effective_enabled` is the step's
/// own `enabled` if it set one, else the global's `enabled` (`false` if
/// there's no global config at all — event logging is off by default,
/// same "omitting `--global-config` degrades cleanly to off" contract
/// `main.rs`'s `Cli::global_config` docs promise). If `effective_enabled`
/// is `false`, the result is `None` regardless of any `base_path`
/// anywhere (no point returning a path nothing will write to).
/// `effective_base_path` is the step's own `base_path` if it set one,
/// else the global's — if that ends up `None` too (a step turns event
/// logging on via its own `enabled = true` but neither it nor any global
/// config supplies a `base_path`), the result is also `None`: there's
/// nowhere to point `spark.eventLog.dir` at, so "enabled with no path"
/// can't actually fire. This is the judgment call the plan's "use your
/// judgment" note flags — documented here rather than left implicit.
fn resolve_event_log(global: Option<&EventLogConfig>, step_override: Option<&EventLogOverride>) -> Option<String> {
    let global_enabled = global.map(|g| g.enabled).unwrap_or(false);
    let effective_enabled = step_override.and_then(|o| o.enabled).unwrap_or(global_enabled);
    if !effective_enabled {
        return None;
    }
    let global_base_path = global.map(|g| g.base_path.as_str());
    step_override
        .and_then(|o| o.base_path.as_deref())
        .or(global_base_path)
        .map(str::to_string)
}

/// `paused` is checked first, before anything else — a paused step does
/// nothing this tick regardless of what `in_flight`/`backlog_count` say,
/// matching the real reconcile loop's pause check (which additionally
/// skips the backlog/in-flight *lookups themselves*, avoiding the I/O
/// entirely — this pure function can't demonstrate that half, since it
/// takes already-fetched values, but it does prove the decision itself
/// never fires while paused even when handed a real backlog).
fn decide_batch_action(paused: bool, in_flight: &[String], backlog_count: i64) -> BatchAction {
    if paused {
        return BatchAction::Idle;
    }
    if let Some(job_name) = in_flight.first() {
        return BatchAction::WaitFor(job_name.clone());
    }
    if backlog_count > 0 {
        BatchAction::Fire
    } else {
        BatchAction::Idle
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fires_when_nothing_in_flight_and_backlog_is_positive() {
        assert_eq!(decide_batch_action(false, &[], 3), BatchAction::Fire);
    }

    #[test]
    fn stays_idle_when_nothing_in_flight_and_no_backlog() {
        assert_eq!(decide_batch_action(false, &[], 0), BatchAction::Idle);
    }

    #[test]
    fn never_fires_a_second_job_while_one_is_already_in_flight_even_with_backlog() {
        let in_flight = vec!["canoe-batch-orders-validate-123".to_string()];
        assert_eq!(
            decide_batch_action(false, &in_flight, 10),
            BatchAction::WaitFor("canoe-batch-orders-validate-123".to_string())
        );
    }

    #[test]
    fn waits_for_the_in_flight_job_even_with_zero_backlog() {
        // Backlog can read 0 mid-flight (the in-flight Job just hasn't
        // advanced the checkpoint past everything yet) — must still wait
        // for it rather than treating this as idle.
        let in_flight = vec!["canoe-batch-orders-validate-123".to_string()];
        assert_eq!(
            decide_batch_action(false, &in_flight, 0),
            BatchAction::WaitFor("canoe-batch-orders-validate-123".to_string())
        );
    }

    /// The pause-check short-circuit the plan calls out as mattering
    /// most: a paused step does nothing even with real backlog present.
    #[test]
    fn paused_step_does_nothing_even_with_real_backlog_present() {
        assert_eq!(decide_batch_action(true, &[], 42), BatchAction::Idle);
    }

    /// Paused wins even over an in-flight Job — this shouldn't co-occur
    /// in practice (cancel deletes any in-flight Job before this could be
    /// reached), but the pure decision itself must still not report
    /// "wait for it" while paused.
    #[test]
    fn paused_wins_over_an_in_flight_job() {
        let in_flight = vec!["canoe-batch-orders-validate-123".to_string()];
        assert_eq!(decide_batch_action(true, &in_flight, 0), BatchAction::Idle);
    }

    #[test]
    fn is_paused_reads_the_control_registry_by_name() {
        let control: Mutex<HashMap<String, bool>> = Mutex::new(HashMap::new());
        assert!(!is_paused(&control, "proj--validate"), "unset name defaults to not paused");
        control.lock().unwrap().insert("proj--validate".to_string(), true);
        assert!(is_paused(&control, "proj--validate"));
        assert!(!is_paused(&control, "proj--other"), "a different name is unaffected");
    }

    #[test]
    fn negative_backlog_is_treated_as_idle_not_fired() {
        // `unprocessed_run_count` is a signed count (see
        // `canoe_k8s::Backlog`'s docs); shouldn't happen in practice, but
        // a fire decision should require a strictly positive count, not
        // just "not zero."
        assert_eq!(decide_batch_action(false, &[], -1), BatchAction::Idle);
    }

    fn registries(activated: &[&str], completed: &[&str]) -> (Mutex<HashSet<String>>, Mutex<HashSet<String>>) {
        (
            Mutex::new(activated.iter().map(|s| s.to_string()).collect()),
            Mutex::new(completed.iter().map(|s| s.to_string()).collect()),
        )
    }

    #[test]
    fn no_dependencies_is_trivially_satisfied() {
        let (activated, completed) = registries(&[], &[]);
        assert!(dependencies_satisfied(&activated, &completed, &[], &[]));
    }

    #[test]
    fn depends_on_waits_for_activated_not_completed() {
        let (activated, completed) = registries(&["upstream"], &[]);
        // "applied" is enough for plain depends_on, even though
        // "upstream" hasn't actually completed.
        assert!(dependencies_satisfied(&activated, &completed, &["upstream".to_string()], &[]));
    }

    #[test]
    fn depends_on_not_yet_activated_blocks() {
        let (activated, completed) = registries(&[], &[]);
        assert!(!dependencies_satisfied(&activated, &completed, &["upstream".to_string()], &[]));
    }

    #[test]
    fn depends_on_complete_requires_actual_completion_not_just_applied() {
        let (activated, completed) = registries(&["upstream"], &[]);
        // Applied but not yet completed — depends_on_complete must still block.
        assert!(!dependencies_satisfied(&activated, &completed, &[], &["upstream".to_string()]));
    }

    #[test]
    fn depends_on_complete_satisfied_once_completed() {
        let (activated, completed) = registries(&["upstream"], &["upstream"]);
        assert!(dependencies_satisfied(&activated, &completed, &[], &["upstream".to_string()]));
    }

    #[test]
    fn both_kinds_together_require_both_to_hold() {
        let (activated, completed) = registries(&["a", "b"], &["b"]);
        assert!(dependencies_satisfied(&activated, &completed, &["a".to_string()], &["b".to_string()]));
        // "a" is only applied, not completed — a depends_on_complete on
        // "a" specifically must still block even though "b" is fully
        // satisfied.
        assert!(!dependencies_satisfied(&activated, &completed, &["a".to_string()], &["a".to_string()]));
    }

    fn global_event_log(enabled: bool, base_path: &str) -> EventLogConfig {
        EventLogConfig { enabled, base_path: base_path.to_string() }
    }

    #[test]
    fn resolve_event_log_global_only() {
        let global = global_event_log(true, "s3a://canoe-lake/_spark-events");
        assert_eq!(
            resolve_event_log(Some(&global), None),
            Some("s3a://canoe-lake/_spark-events".to_string())
        );
    }

    #[test]
    fn resolve_event_log_override_only() {
        let step_override = EventLogOverride { enabled: Some(true), base_path: Some("s3a://other/_events".to_string()) };
        assert_eq!(resolve_event_log(None, Some(&step_override)), Some("s3a://other/_events".to_string()));
    }

    #[test]
    fn resolve_event_log_override_disables_an_enabled_global() {
        let global = global_event_log(true, "s3a://canoe-lake/_spark-events");
        let step_override = EventLogOverride { enabled: Some(false), base_path: None };
        assert_eq!(resolve_event_log(Some(&global), Some(&step_override)), None);
    }

    #[test]
    fn resolve_event_log_override_changes_only_base_path_inheriting_enabled() {
        let global = global_event_log(true, "s3a://canoe-lake/_spark-events");
        let step_override = EventLogOverride { enabled: None, base_path: Some("s3a://canoe-lake/_step-events".to_string()) };
        assert_eq!(
            resolve_event_log(Some(&global), Some(&step_override)),
            Some("s3a://canoe-lake/_step-events".to_string())
        );
    }

    #[test]
    fn resolve_event_log_neither_set() {
        assert_eq!(resolve_event_log(None, None), None);
    }

    #[test]
    fn resolve_event_log_override_enables_with_no_base_path_anywhere_is_a_no_op() {
        // A step turns event logging on via its own `enabled = true` but
        // neither it nor any global config supplies a `base_path` — see
        // `resolve_event_log`'s docs for why this can't actually fire.
        let step_override = EventLogOverride { enabled: Some(true), base_path: None };
        assert_eq!(resolve_event_log(None, Some(&step_override)), None);
    }
}

/// Same as `start_python_step`, for an `engine = "uv"` step — its pods
/// run a different image/command but are otherwise plain `Pod`s like any
/// other: lag-driven autoscaling, scale-to-zero, and the circuit breaker
/// all work identically (`Autoscaler::reconcile_uv_step`).
pub fn start_uv_step(ctx: Ctx, chain_name: String, pipeline_name: String, step: UvStep) {
    tokio::spawn(async move {
        let pods = PodManager::new(ctx.k8s_client.clone(), ctx.namespace.clone(), ctx.uv_worker_image.clone());
        let pod_set = PodManager::uv_pod_set_name(&chain_name, &step.name);
        let already_alive = match pods.list_pods(&pod_set).await {
            Ok(existing) => existing.iter().filter(|p| p.alive).count() as u32,
            Err(e) => {
                error!(chain = %chain_name, step = %step.name, error = %e, "failed to list existing pods, assuming none");
                0
            }
        };
        for _ in already_alive..step.scaling.min_replicas {
            if let Err(e) = pods
                .create_uv_pod(&chain_name, &step, &ctx.pod_kafka_bootstrap, &ctx.metrics_url, ctx.log_base_path.as_deref(), ctx.pod_s3_endpoint.as_deref())
                .await
            {
                error!(chain = %chain_name, step = %step.name, error = %e, "failed to create uv worker pod");
                return;
            }
        }
        info!(chain = %chain_name, step = %step.name, "uv worker pod set ensured");
        mark_activated(&ctx, &pipeline_name, &step.name);

        let lag = match LagInspector::new(&ctx.kafka_bootstrap) {
            Ok(l) => l,
            Err(e) => {
                error!(step = %step.name, error = %e, "failed to start lag inspector, autoscaling disabled for this step");
                return;
            }
        };
        let backlog = match S3BacklogInspector::new(&step.input_path) {
            Ok(b) => b,
            Err(e) => {
                error!(step = %step.name, error = %e, "failed to start S3 backlog inspector, autoscaling disabled for this step");
                return;
            }
        };
        let mut autoscaler = Autoscaler::new(pods, lag, Some(backlog));
        let mut ticker = tokio::time::interval(ctx.reconcile_interval);
        loop {
            ticker.tick().await;
            // Pause check first, before the normal reconcile call — a
            // paused step scales to 0 (deletes every pod in its set,
            // same `PodManager` calls the autoscaler already makes when
            // scaling down for real) and skips pod creation entirely.
            // Re-run just clears `ctx.control`; this loop is already
            // continuously reconciling on `reconcile_interval`, so the
            // very next tick recreates `min_replicas` pods on its own —
            // no force-fire channel needed here, unlike batch mode's
            // fire-once-then-idle shape.
            if is_paused(&ctx.control, &step.name) {
                if let Err(e) = scale_to_zero(&ctx, &PodManager::uv_pod_set_name(&chain_name, &step.name)).await {
                    warn!(chain = %chain_name, step = %step.name, error = %e, "failed to scale paused step to zero");
                }
                continue;
            }
            let result = autoscaler
                .reconcile_uv_step(&chain_name, &step, &ctx.pod_kafka_bootstrap, &ctx.metrics_url, ctx.log_base_path.as_deref(), ctx.pod_s3_endpoint.as_deref())
                .await;
            record_reconcile(&ctx, &pipeline_name, &chain_name, &step.name, "uv", result);
        }
    });
}

/// Deletes every pod in `pod_set` — the "cancel" half of Python/uv
/// streaming lifecycle control. A fresh, short-lived `PodManager` (the
/// `worker_image` it carries is irrelevant here, same reasoning as
/// `run_batch_reconcile_loop`'s Job-status-only `StepJobRunner`) rather
/// than threading the caller's own through, since this is called from
/// two different reconcile loops with two different `PodManager`
/// instances already captured by value inside their own closures.
async fn scale_to_zero(ctx: &Ctx, pod_set: &str) -> anyhow::Result<()> {
    let pods = PodManager::new(ctx.k8s_client.clone(), ctx.namespace.clone(), String::new());
    for pod in pods.list_pods(pod_set).await? {
        pods.delete_pod(&pod.name).await?;
    }
    Ok(())
}

/// `dynamic_route`'s own per-discovered-value pods (see
/// `KafkaPythonStep`'s docs) — same pod top-up + circuit-breaker +
/// Kafka-lag-driven autoscaling shape as `start_python_step`, just against
/// the old `CANOE_INPUT_TOPIC`/`CANOE_OUTPUT_TOPIC`/`CANOE_CONSUMER_GROUP`
/// env var contract instead of the S3 + signal one.
pub fn start_kafka_python_step(ctx: Ctx, chain_name: String, step: KafkaPythonStep) {
    tokio::spawn(async move {
        let pods = PodManager::new(ctx.k8s_client.clone(), ctx.namespace.clone(), ctx.worker_image.clone());
        let pod_set = PodManager::pod_set_name(&chain_name, &step.name);
        let already_alive = match pods.list_pods(&pod_set).await {
            Ok(existing) => existing.iter().filter(|p| p.alive).count() as u32,
            Err(e) => {
                error!(chain = %chain_name, step = %step.name, error = %e, "failed to list existing pods, assuming none");
                0
            }
        };
        for _ in already_alive..step.scaling.min_replicas {
            if let Err(e) = pods
                .create_kafka_python_pod(&chain_name, &step, &ctx.pod_kafka_bootstrap, &ctx.metrics_url, ctx.log_base_path.as_deref(), ctx.pod_s3_endpoint.as_deref())
                .await
            {
                error!(chain = %chain_name, step = %step.name, error = %e, "failed to create python worker pod");
                return;
            }
        }
        info!(chain = %chain_name, step = %step.name, "python worker pod set ensured");
        // No real `ChainConfig`/pipeline here — `dynamic_router.rs` builds
        // `chain_name` itself as a synthetic `"dyn-<route>"` string, not
        // from any compiled `ChainConfig.pipeline`. Using it as-is for
        // "pipeline_name" matches what the old `pipeline_name_of` hack
        // would've produced for a non-`-<digits>`-suffixed name like this
        // anyway (returned unchanged).
        mark_activated(&ctx, &chain_name, &step.name);

        let lag = match LagInspector::new(&ctx.kafka_bootstrap) {
            Ok(l) => l,
            Err(e) => {
                error!(step = %step.name, error = %e, "failed to start lag inspector, autoscaling disabled for this step");
                return;
            }
        };
        let mut autoscaler = Autoscaler::new(pods, lag, None);
        let mut ticker = tokio::time::interval(ctx.reconcile_interval);
        loop {
            ticker.tick().await;
            // See `start_uv_step`'s identical pause check for why this
            // comes first and what "scale to zero" means here.
            if is_paused(&ctx.control, &step.name) {
                if let Err(e) = scale_to_zero(&ctx, &PodManager::pod_set_name(&chain_name, &step.name)).await {
                    warn!(chain = %chain_name, step = %step.name, error = %e, "failed to scale paused step to zero");
                }
                continue;
            }
            let result = autoscaler
                .reconcile_kafka_python_step(&chain_name, &step, &ctx.pod_kafka_bootstrap, &ctx.metrics_url, ctx.log_base_path.as_deref(), ctx.pod_s3_endpoint.as_deref())
                .await;
            record_reconcile(&ctx, &chain_name, &chain_name, &step.name, "dynamic_route", result);
        }
    });
}

/// Streaming-mode (`spark_sql { mode = "streaming" }`, also `spark_sql`'s
/// default before `mode` existed) — unchanged behavior from before this
/// migration: applies a one-shot, idempotent spark-submit launcher
/// Deployment (runs forever once created; no live reconcile loop here,
/// unlike `start_python_step`).
pub fn start_kafka_spark_sql_step(ctx: Ctx, chain_name: String, pipeline_name: String, step: KafkaSparkSqlStep) {
    tokio::spawn(async move {
        let launcher = SparkLauncher::new(
            ctx.k8s_client.clone(),
            ctx.namespace.clone(),
            ctx.k8s_api_server.clone(),
            ctx.spark_service_account.clone(),
        );
        let event_log_path = resolve_event_log(
            ctx.global.spark.as_ref().and_then(|s| s.event_log.as_ref()),
            step.spark.event_log_override.as_ref(),
        );
        match launcher
            .ensure_sql_step(&chain_name, &step, &ctx.kafka_bootstrap, event_log_path.as_deref(), ctx.pod_s3_endpoint.as_deref())
            .await
        {
            Ok(()) => {
                info!(chain = %chain_name, step = %step.name, "spark SQL launcher ensured");
                mark_activated(&ctx, &pipeline_name, &step.name);
                ctx.status.update_step(applied_spark_status(&pipeline_name, &chain_name, &step.name, "kafka_spark_sql", None));
            }
            Err(e) => {
                error!(chain = %chain_name, step = %step.name, error = %e, "failed to apply spark SQL launcher");
                ctx.status.update_step(applied_spark_status(&pipeline_name, &chain_name, &step.name, "kafka_spark_sql", Some(e.to_string())));
            }
        }
    });
}

/// Streaming-mode DataFrame twin of `start_kafka_spark_sql_step` — see its docs.
pub fn start_kafka_spark_dataframe_step(ctx: Ctx, chain_name: String, pipeline_name: String, step: KafkaSparkDataFrameStep) {
    tokio::spawn(async move {
        let launcher = SparkLauncher::new(
            ctx.k8s_client.clone(),
            ctx.namespace.clone(),
            ctx.k8s_api_server.clone(),
            ctx.spark_service_account.clone(),
        );
        let event_log_path = resolve_event_log(
            ctx.global.spark.as_ref().and_then(|s| s.event_log.as_ref()),
            step.spark.event_log_override.as_ref(),
        );
        match launcher
            .ensure_dataframe_step(&chain_name, &step, &ctx.kafka_bootstrap, event_log_path.as_deref(), ctx.pod_s3_endpoint.as_deref())
            .await
        {
            Ok(()) => {
                info!(chain = %chain_name, step = %step.name, "spark DataFrame launcher ensured");
                mark_activated(&ctx, &pipeline_name, &step.name);
                ctx.status.update_step(applied_spark_status(&pipeline_name, &chain_name, &step.name, "kafka_spark_dataframe", None));
            }
            Err(e) => {
                error!(chain = %chain_name, step = %step.name, error = %e, "failed to apply spark DataFrame launcher");
                ctx.status.update_step(applied_spark_status(&pipeline_name, &chain_name, &step.name, "kafka_spark_dataframe", Some(e.to_string())));
            }
        }
    });
}

/// A streaming-mode Spark step has no live reconcile loop (a one-shot
/// `ensure_*` apply,
/// not a ticking `Autoscaler`) — this just records "applied" vs. "failed
/// to apply", with every replica/backlog/lag field `None` since there's
/// nothing to report.
fn applied_spark_status(pipeline_name: &str, chain_name: &str, step_name: &str, kind: &str, last_error: Option<String>) -> StepStatus {
    StepStatus {
        pipeline: pipeline_name.to_string(),
        chain: chain_name.to_string(),
        name: step_name.to_string(),
        kind: kind.to_string(),
        desired_replicas: None,
        alive_replicas: None,
        circuit_open: false,
        backlog: None,
        lag: None,
        last_reconciled: Utc::now(),
        last_error,
        paused: false,
    }
}

pub fn start_sink_step(ctx: Ctx, chain_name: String, pipeline_name: String, step: SinkStep) {
    tokio::spawn(async move {
        if let Err(e) = crate::sink_runner::run(
            step,
            chain_name,
            pipeline_name,
            ctx.kafka_bootstrap.clone(),
            ctx.signals_topic.clone(),
            ctx.status.clone(),
        )
        .await
        {
            error!(error = %e, "sink runner exited");
        }
    });
}
