//! `GET /status`: an introspection snapshot of every pipeline/chain/step
//! and adapter the coordinator knows about, for `canoe-tui` (or any other
//! read-only client) to poll. Served on the same axum router/port as
//! `metrics.rs`'s `/metrics` endpoints — no separate CLI flag or port.
//!
//! Live per-step/adapter state (pod counts, circuit-breaker status,
//! backlog/lag, last adapter run) is computed fresh every reconcile tick
//! inside each step's own detached loop (`activation.rs`) or adapter
//! schedule loop (`scheduler.rs`) and would otherwise be thrown away —
//! `StatusRegistry` is the one place that retains the *latest* snapshot
//! of it, in memory, for as long as the coordinator runs. It holds no
//! history: only ever the most recent update per step/adapter name.
//!
//! The response is built by walking the *static* `PipelineConfig`
//! (always complete, even for a step that hasn't reconciled even once
//! yet — e.g. one dormant behind a `trigger.signal`) and left-joining in
//! whatever `StatusRegistry` has for each name; a step/adapter with no
//! registry entry yet reports nulls for the live fields rather than
//! being omitted, so a client can still show it as "not yet activated."

use crate::projects::ProjectRegistry;
use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use canoe_config::{AdapterConfig, PipelineConfig, StepConfig};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Serialize)]
pub struct StepStatus {
    pub pipeline: String,
    pub chain: String,
    pub name: String,
    /// "python" | "uv" | "spark_sql" | "spark_dataframe" | "sink" | "dynamic_route".
    pub kind: String,
    pub desired_replicas: Option<u32>,
    pub alive_replicas: Option<u32>,
    pub circuit_open: bool,
    pub backlog: Option<i64>,
    pub lag: Option<i64>,
    pub last_reconciled: DateTime<Utc>,
    pub last_error: Option<String>,
    /// Computed fresh from `Ctx.control` at response-build time
    /// (`build_pipeline_response`/`not_yet_reconciled`), not stored here
    /// by whoever calls `update_step` — see `Ctx.control`'s docs for why
    /// this stays the one source of truth rather than being duplicated
    /// into every per-tick snapshot. Reconcile loops constructing a
    /// `StepStatus` always set this `false`; it's overwritten before the
    /// response is ever returned.
    pub paused: bool,
}

/// One task's latest-known lifecycle state — surfaced for the first time
/// by this feature (`tasks.rs` already knew success/failure internally,
/// it just never reported it before). See `state`'s docs for what each
/// variant means.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    /// Declared, not yet fired — still waiting on `depends_on`/
    /// `depends_on_complete`, or paused before ever having run.
    Pending,
    /// Job fired, not yet finished.
    Running,
    Succeeded,
    Failed,
    /// Explicitly paused via `POST .../tasks/:task/cancel` — distinct
    /// from `Pending` so the TUI can show *why* it isn't running.
    Paused,
}

#[derive(Debug, Clone, Serialize)]
pub struct TaskStatus {
    pub name: String,
    pub state: TaskState,
    pub last_run_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AdapterStatus {
    pub name: String,
    pub last_run_at: Option<DateTime<Utc>>,
    /// `None` — never run yet; `Some(true)` — last run submitted fine;
    /// `Some(false)` — failed to submit (see `last_error`). Kept as a
    /// plain bool + separate error string, rather than `Option<Result<(),
    /// String>>`, since `serde` has no blanket `Serialize` for
    /// `std::result::Result` and this shape is simpler to consume as JSON
    /// anyway (mirrors `StepStatus::last_error`'s convention).
    pub last_success: Option<bool>,
    pub last_error: Option<String>,
    pub next_run_at: Option<DateTime<Utc>>,
}

/// In-memory latest-snapshot-per-name store — see module docs. Cheap to
/// clone (`Arc`-backed via callers holding it behind their own `Arc`, same
/// convention as `metrics::Store`).
#[derive(Default)]
pub struct StatusRegistry {
    steps: Mutex<HashMap<String, StepStatus>>,
    adapters: Mutex<HashMap<String, AdapterStatus>>,
    tasks: Mutex<HashMap<String, TaskStatus>>,
}

impl StatusRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update_step(&self, status: StepStatus) {
        self.steps.lock().unwrap().insert(status.name.clone(), status);
    }

    pub fn update_task(&self, status: TaskStatus) {
        self.tasks.lock().unwrap().insert(status.name.clone(), status);
    }

    /// Not read anywhere yet — kept for parity with `step()` (used by a
    /// reconcile loop to preserve prior state on a failed pass); a future
    /// task-status caller may want the same "read back what I last
    /// wrote" ability.
    #[allow(dead_code)]
    pub fn task(&self, name: &str) -> Option<TaskStatus> {
        self.tasks.lock().unwrap().get(name).cloned()
    }

    pub fn update_adapter(&self, status: AdapterStatus) {
        self.adapters.lock().unwrap().insert(status.name.clone(), status);
    }

    /// The last-known status for a step, if any — used by a reconcile
    /// loop that just failed to preserve the prior replica/backlog/lag
    /// numbers instead of blanking them out on a single failed pass.
    pub fn step(&self, name: &str) -> Option<StepStatus> {
        self.steps.lock().unwrap().get(name).cloned()
    }

    fn snapshot(&self) -> (HashMap<String, StepStatus>, HashMap<String, AdapterStatus>, HashMap<String, TaskStatus>) {
        (self.steps.lock().unwrap().clone(), self.adapters.lock().unwrap().clone(), self.tasks.lock().unwrap().clone())
    }
}

#[derive(Debug, Serialize)]
pub struct ChainStatus {
    pub chain: String,
    pub source: String,
    pub steps: Vec<StepStatus>,
}

#[derive(Debug, Serialize)]
pub struct PipelineStatus {
    pub pipeline: String,
    pub chains: Vec<ChainStatus>,
}

#[derive(Debug, Serialize)]
pub struct StatusResponse {
    pub projects: Vec<ProjectStatus>,
}

/// One project's slice of the response — `adapters` is scoped per-project
/// here (not top-level-flat, as it was before the live project registry
/// existed) for the same collision-avoidance reason chain/step names got
/// project-prefixed in `crate::projects::prefix_project`: two projects
/// could otherwise declare an adapter with the same name and have one
/// silently shadow the other in a flat list.
#[derive(Debug, Serialize)]
pub struct ProjectStatus {
    pub project: String,
    pub is_dev: bool,
    pub pushed_at: DateTime<Utc>,
    pub pipelines: Vec<PipelineStatus>,
    pub adapters: Vec<AdapterStatus>,
    pub tasks: Vec<TaskStatus>,
}

fn step_kind(step: &StepConfig) -> &'static str {
    match step {
        StepConfig::Python(_) => "python",
        StepConfig::KafkaPython(_) => "kafka_python",
        StepConfig::Uv(_) => "uv",
        StepConfig::SparkSql(_) => "spark_sql",
        StepConfig::KafkaSparkSql(_) => "kafka_spark_sql",
        StepConfig::SparkDataFrame(_) => "spark_dataframe",
        StepConfig::KafkaSparkDataFrame(_) => "kafka_spark_dataframe",
        StepConfig::Sink(_) => "sink",
        StepConfig::DynamicRoute(_) => "dynamic_route",
    }
}

/// A step present in the static config but with no registry entry yet —
/// dormant behind a `trigger.signal`, or simply hasn't reconciled once
/// yet — reports nulls for every live field.
fn not_yet_reconciled(pipeline: &str, chain: &str, step: &StepConfig, now: DateTime<Utc>, paused: bool) -> StepStatus {
    StepStatus {
        pipeline: pipeline.to_string(),
        chain: chain.to_string(),
        name: step.name().to_string(),
        kind: step_kind(step).to_string(),
        desired_replicas: None,
        alive_replicas: None,
        circuit_open: false,
        backlog: None,
        lag: None,
        last_reconciled: now,
        last_error: None,
        paused,
    }
}

/// `control` is read once per step here — see `Ctx.control`'s docs for
/// why `paused` is computed fresh at response-build time rather than
/// trusted from whatever a reconcile loop last stored in `steps`.
fn build_pipeline_response(
    config: &PipelineConfig,
    steps: &HashMap<String, StepStatus>,
    control: &Mutex<HashMap<String, bool>>,
) -> Vec<PipelineStatus> {
    let now = Utc::now();
    let mut by_pipeline: Vec<(String, Vec<ChainStatus>)> = Vec::new();
    for chain in &config.chains {
        let chain_status = ChainStatus {
            chain: chain.name.clone(),
            source: chain.source.clone(),
            steps: chain
                .steps
                .iter()
                .map(|s| {
                    let paused = crate::activation::is_paused(control, s.name());
                    let mut status = steps
                        .get(s.name())
                        .cloned()
                        .unwrap_or_else(|| not_yet_reconciled(&chain.pipeline, &chain.name, s, now, paused));
                    status.paused = paused;
                    status
                })
                .collect(),
        };
        match by_pipeline.iter_mut().find(|(name, _)| name == &chain.pipeline) {
            Some((_, chains)) => chains.push(chain_status),
            None => by_pipeline.push((chain.pipeline.clone(), vec![chain_status])),
        }
    }
    by_pipeline.into_iter().map(|(pipeline, chains)| PipelineStatus { pipeline, chains }).collect()
}

fn build_task_response(config: &PipelineConfig, tasks: &HashMap<String, TaskStatus>, control: &Mutex<HashMap<String, bool>>) -> Vec<TaskStatus> {
    config
        .tasks
        .iter()
        .map(|t| {
            let paused = crate::activation::is_paused(control, &t.name);
            let mut status = tasks.get(&t.name).cloned().unwrap_or_else(|| TaskStatus {
                name: t.name.clone(),
                state: TaskState::Pending,
                last_run_at: None,
                last_error: None,
            });
            if paused {
                status.state = TaskState::Paused;
            }
            status
        })
        .collect()
}

fn build_adapter_response(config: &PipelineConfig, adapters: &HashMap<String, AdapterStatus>) -> Vec<AdapterStatus> {
    config
        .adapters
        .iter()
        .map(|a: &AdapterConfig| {
            adapters.get(&a.name).cloned().unwrap_or_else(|| AdapterStatus {
                name: a.name.clone(),
                last_run_at: None,
                last_success: None,
                last_error: None,
                next_run_at: None,
            })
        })
        .collect()
}

/// Walks every project currently in the registry, left-joining in
/// `StatusRegistry`'s live snapshot the same way the single-config
/// version always did — now once per project instead of once overall.
/// Every project's config already has its chain/step/adapter names
/// project-prefixed (`crate::projects::prefix_project`, applied once at
/// push time), so the same flat `StatusRegistry` lookup by name still
/// resolves correctly per project without this function needing to know
/// anything about that prefixing itself.
fn build_response(registry: &ProjectRegistry, status: &StatusRegistry, control: &Mutex<HashMap<String, bool>>) -> StatusResponse {
    let (steps, adapters, tasks) = status.snapshot();
    let projects = registry
        .snapshot()
        .into_iter()
        .map(|(name, entry)| ProjectStatus {
            project: name,
            is_dev: entry.is_dev,
            pushed_at: entry.pushed_at,
            pipelines: build_pipeline_response(&entry.config, &steps, control),
            adapters: build_adapter_response(&entry.config, &adapters),
            tasks: build_task_response(&entry.config, &tasks, control),
        })
        .collect();
    StatusResponse { projects }
}

#[derive(Clone)]
struct AppState {
    projects: Arc<ProjectRegistry>,
    registry: Arc<StatusRegistry>,
    control: Arc<Mutex<HashMap<String, bool>>>,
}

async fn status_handler(State(state): State<AppState>) -> Json<StatusResponse> {
    Json(build_response(&state.projects, &state.registry, &state.control))
}

/// Standalone router for `/status`, merged into `metrics.rs`'s router at
/// startup (each has its own state type, so each is resolved to
/// `Router<()>` via `with_state` before `Router::merge` combines them).
pub fn router(projects: Arc<ProjectRegistry>, registry: Arc<StatusRegistry>, control: Arc<Mutex<HashMap<String, bool>>>) -> Router {
    Router::new().route("/status", get(status_handler)).with_state(AppState { projects, registry, control })
}

#[cfg(test)]
mod tests {
    use super::*;
    use canoe_config::{AdapterSchedule, ChainConfig, CodeSource, OutputConfig, PodResources, PythonStep, ScalingConfig};
    use std::collections::BTreeMap;

    fn python_step(name: &str) -> StepConfig {
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
            depends_on: Vec::new(),
            depends_on_complete: Vec::new(),
        })
    }

    fn fixture_config() -> PipelineConfig {
        PipelineConfig {
            chains: vec![ChainConfig {
                name: "orders-0".to_string(),
                pipeline: "orders".to_string(),
                source: "orders-adapter".to_string(),
                steps: vec![python_step("enrich")],
                trigger: None,
            }],
            adapters: vec![AdapterConfig {
                name: "orders-adapter".to_string(),
                topic: Some("_canoe.orders-adapter".to_string()),
                output_path: None,
                output: None,
                code: CodeSource::Inline { source: "".to_string() },
                schedule: AdapterSchedule { interval_secs: Some(60), cron: None },
                resources: PodResources::default(),
                backoff_limit: 2,
                encrypt: Vec::new(),
                secrets: Vec::new(),
            }],
            storage: None,
            topic_defaults: None,
            managed_topics: vec![],
            tasks: vec![canoe_config::TaskConfig {
                name: "notify".to_string(),
                code: CodeSource::Inline { source: "print('hi')".to_string() },
                depends_on: Vec::new(),
                depends_on_complete: Vec::new(),
                resources: PodResources::default(),
                backoff_limit: 2,
                secrets: Vec::new(),
            }],
        }
    }

    fn fixture_registry(project: &str) -> ProjectRegistry {
        let registry = ProjectRegistry::new();
        registry.insert(
            project,
            crate::projects::ProjectEntry { config: Arc::new(fixture_config()), is_dev: false, pushed_at: Utc::now() },
        );
        registry
    }

    #[test]
    fn a_step_with_no_registry_entry_reports_nulls() {
        let projects = fixture_registry("default");
        let status = StatusRegistry::new();
        let control: Mutex<HashMap<String, bool>> = Mutex::new(HashMap::new());
        let resp = build_response(&projects, &status, &control);
        assert_eq!(resp.projects.len(), 1);
        assert_eq!(resp.projects[0].project, "default");
        let step = &resp.projects[0].pipelines[0].chains[0].steps[0];
        assert_eq!(step.name, "enrich");
        assert_eq!(step.kind, "python");
        assert!(step.alive_replicas.is_none());
        assert!(!step.circuit_open);

        let adapter = &resp.projects[0].adapters[0];
        assert_eq!(adapter.name, "orders-adapter");
        assert!(adapter.last_run_at.is_none());
    }

    #[test]
    fn a_registered_step_and_adapter_report_their_live_values() {
        let projects = fixture_registry("default");
        let status = StatusRegistry::new();
        let now = Utc::now();
        status.update_step(StepStatus {
            pipeline: "orders".to_string(),
            chain: "orders-0".to_string(),
            name: "enrich".to_string(),
            kind: "python".to_string(),
            desired_replicas: Some(2),
            alive_replicas: Some(2),
            circuit_open: false,
            backlog: Some(5),
            lag: None,
            last_reconciled: now,
            last_error: None,
            paused: false,
        });
        status.update_adapter(AdapterStatus {
            name: "orders-adapter".to_string(),
            last_run_at: Some(now),
            last_success: Some(true),
            last_error: None,
            next_run_at: Some(now),
        });

        let control: Mutex<HashMap<String, bool>> = Mutex::new(HashMap::new());
        let resp = build_response(&projects, &status, &control);
        let step = &resp.projects[0].pipelines[0].chains[0].steps[0];
        assert_eq!(step.alive_replicas, Some(2));
        assert_eq!(step.backlog, Some(5));

        let adapter = &resp.projects[0].adapters[0];
        assert!(adapter.last_run_at.is_some());
        assert_eq!(adapter.last_success, Some(true));
    }

    /// A task with no registry entry yet reports `Pending`; one that's
    /// been updated (surfacing what `tasks.rs` already tracks internally
    /// — this feature's whole point, since it reported nothing before)
    /// round-trips through `build_response` faithfully.
    #[test]
    fn task_status_round_trips_through_the_response() {
        let projects = fixture_registry("default");
        let status = StatusRegistry::new();
        let control: Mutex<HashMap<String, bool>> = Mutex::new(HashMap::new());

        let resp = build_response(&projects, &status, &control);
        let task = &resp.projects[0].tasks[0];
        assert_eq!(task.name, "notify");
        assert_eq!(task.state, TaskState::Pending);
        assert!(task.last_run_at.is_none());

        let now = Utc::now();
        status.update_task(TaskStatus { name: "notify".to_string(), state: TaskState::Succeeded, last_run_at: Some(now), last_error: None });
        let resp = build_response(&projects, &status, &control);
        let task = &resp.projects[0].tasks[0];
        assert_eq!(task.state, TaskState::Succeeded);
        assert_eq!(task.last_run_at, Some(now));
    }

    /// A paused task reports `Paused` regardless of whatever state
    /// `tasks.rs` last recorded for it — `ctx.control` overrides the
    /// stored snapshot at response-build time, same discipline as
    /// `StepStatus::paused`.
    #[test]
    fn a_paused_task_reports_paused_state_even_if_last_recorded_as_running() {
        let projects = fixture_registry("default");
        let status = StatusRegistry::new();
        status.update_task(TaskStatus { name: "notify".to_string(), state: TaskState::Running, last_run_at: Some(Utc::now()), last_error: None });

        let control: Mutex<HashMap<String, bool>> = Mutex::new(HashMap::new());
        control.lock().unwrap().insert("notify".to_string(), true);

        let resp = build_response(&projects, &status, &control);
        assert_eq!(resp.projects[0].tasks[0].state, TaskState::Paused);
    }

    /// Two projects, both containing an adapter/step named the same as
    /// the fixture's — since `build_response` walks the registry (which
    /// only ever holds already project-prefixed configs in the real
    /// system, via `apply_project`), each project's slice of the
    /// response stays independent even with identical bare names, and
    /// the flat `StatusRegistry` lookups inside each still resolve
    /// correctly per project.
    #[test]
    fn two_projects_with_identically_named_pieces_stay_independent_in_the_response() {
        let projects = ProjectRegistry::new();
        projects.insert(
            "proj-a",
            crate::projects::ProjectEntry { config: Arc::new(fixture_config()), is_dev: false, pushed_at: Utc::now() },
        );
        projects.insert(
            "proj-b",
            crate::projects::ProjectEntry { config: Arc::new(fixture_config()), is_dev: true, pushed_at: Utc::now() },
        );
        let status = StatusRegistry::new();
        let control: Mutex<HashMap<String, bool>> = Mutex::new(HashMap::new());
        let resp = build_response(&projects, &status, &control);
        assert_eq!(resp.projects.len(), 2);
        let names: Vec<&str> = resp.projects.iter().map(|p| p.project.as_str()).collect();
        assert_eq!(names, vec!["proj-a", "proj-b"]);
        assert!(!resp.projects.iter().find(|p| p.project == "proj-a").unwrap().is_dev);
        assert!(resp.projects.iter().find(|p| p.project == "proj-b").unwrap().is_dev);
    }

    /// End-to-end through the real HTTP handler, same `oneshot` pattern
    /// as `metrics.rs`'s tests.
    #[tokio::test]
    async fn status_endpoint_round_trips_through_http() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use http_body_util::BodyExt;
        use tower::ServiceExt;

        let projects = Arc::new(fixture_registry("default"));
        let registry = Arc::new(StatusRegistry::new());
        registry.update_adapter(AdapterStatus {
            name: "orders-adapter".to_string(),
            last_run_at: Some(Utc::now()),
            last_success: Some(true),
            last_error: None,
            next_run_at: None,
        });

        let control = Arc::new(Mutex::new(HashMap::new()));
        let router = super::router(projects, registry, control);
        let req = Request::builder().method("GET").uri("/status").body(Body::empty()).unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed["projects"][0]["project"], "default");
        assert_eq!(parsed["projects"][0]["pipelines"][0]["pipeline"], "orders");
        assert_eq!(parsed["projects"][0]["pipelines"][0]["chains"][0]["steps"][0]["name"], "enrich");
        assert_eq!(parsed["projects"][0]["adapters"][0]["name"], "orders-adapter");
    }
}
