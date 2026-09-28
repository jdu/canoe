//! DTOs mirroring `canoe-coordinator`'s `GET /status` JSON shape
//! (`crates/canoe-coordinator/src/status.rs`), duplicated here rather than
//! shared via a crate dependency — `canoe-coordinator` is bin-only (no
//! `lib.rs`), and this is the same per-runtime duplication precedent
//! already used elsewhere in this codebase (e.g. `MetricsReporter`/
//! `LogShipper` between the Rust coordinator and the Python workers).

use chrono::{DateTime, Utc};
use serde::Deserialize;

// A few fields below aren't read by `ui.rs` today (it derives chain/step
// identity from the containing `ChainStatus`/`PipelineStatus` instead) but
// are kept to mirror the coordinator's full response shape faithfully.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct StepStatusDto {
    pub pipeline: String,
    pub chain: String,
    pub name: String,
    pub kind: String,
    pub desired_replicas: Option<u32>,
    pub alive_replicas: Option<u32>,
    pub circuit_open: bool,
    pub backlog: Option<i64>,
    pub lag: Option<i64>,
    pub last_reconciled: DateTime<Utc>,
    pub last_error: Option<String>,
    pub paused: bool,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Pending,
    Running,
    Succeeded,
    Failed,
    Paused,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TaskStatusDto {
    pub name: String,
    pub state: TaskState,
    pub last_run_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct ChainStatus {
    pub chain: String,
    pub source: String,
    pub steps: Vec<StepStatusDto>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PipelineStatus {
    pub pipeline: String,
    pub chains: Vec<ChainStatus>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AdapterStatusDto {
    pub name: String,
    pub last_run_at: Option<DateTime<Utc>>,
    pub last_success: Option<bool>,
    pub last_error: Option<String>,
    pub next_run_at: Option<DateTime<Utc>>,
}

/// Flattened across every project the coordinator reports — the
/// List/PipelineDetail screens predate the live project registry and
/// show one combined view regardless of which project a pipeline/adapter
/// came from (their names are already project-prefixed by the
/// coordinator, e.g. `"default--orders"`, so they stay visually
/// distinguishable even flattened). `Screen::Projects` is the one place
/// that cares about project boundaries as such — see `ProjectStatusDto`.
#[derive(Debug, Clone, Deserialize)]
pub struct StatusResponse {
    pub pipelines: Vec<PipelineStatus>,
    pub adapters: Vec<AdapterStatusDto>,
    pub tasks: Vec<TaskStatusDto>,
}

/// One project's slice, as `canoe-coordinator`'s real `/status` response
/// nests it (`crates/canoe-coordinator/src/status.rs::ProjectStatus`).
/// `project`/`is_dev`/`pushed_at` aren't read yet (`flatten` only uses
/// `pipelines`/`adapters`/`tasks`) — kept to mirror the coordinator's
/// full response shape faithfully, same precedent as `StepStatusDto`'s
/// `#[allow(dead_code)]` above.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct ProjectStatusDto {
    pub project: String,
    pub is_dev: bool,
    pub pushed_at: DateTime<Utc>,
    pub pipelines: Vec<PipelineStatus>,
    pub adapters: Vec<AdapterStatusDto>,
    pub tasks: Vec<TaskStatusDto>,
}

#[derive(Debug, Clone, Deserialize)]
struct RawStatusResponse {
    projects: Vec<ProjectStatusDto>,
}

pub async fn fetch_status(client: &reqwest::Client, base_url: &str) -> anyhow::Result<StatusResponse> {
    let url = format!("{}/status", base_url.trim_end_matches('/'));
    let resp = client.get(&url).send().await?.error_for_status()?;
    let raw = resp.json::<RawStatusResponse>().await?;
    Ok(flatten(raw))
}

/// Combines every project's pipelines/adapters into the one flat shape
/// `Screen::List`/`Screen::PipelineDetail` have always rendered — see
/// `StatusResponse`'s docs for why flattening (rather than threading a
/// project concept through those two pre-existing screens) is fine here.
fn flatten(raw: RawStatusResponse) -> StatusResponse {
    let mut pipelines = Vec::new();
    let mut adapters = Vec::new();
    let mut tasks = Vec::new();
    for project in raw.projects {
        pipelines.extend(project.pipelines);
        adapters.extend(project.adapters);
        tasks.extend(project.tasks);
    }
    StatusResponse { pipelines, adapters, tasks }
}

/// Splits an already project-prefixed name (`"{project}--{name}"`, per
/// `canoe-coordinator`'s `projects::prefix_project`) back into its two
/// parts, for building a `/projects/:name/...` URL from a name the
/// flattened `StatusResponse` reports. Relies on project names
/// themselves never containing `--` — the same assumption the prefixing
/// scheme itself already makes (nothing splits it back apart on the
/// coordinator side either).
pub fn split_project_prefix(full_name: &str) -> Option<(&str, &str)> {
    full_name.split_once("--")
}

/// `POST /projects/:name` — compiles `hcl_path` locally via
/// `canoe_dsl::compile_file` (the coordinator never parses HCL itself —
/// see the crate-level module docs) and pushes the resulting
/// `PipelineConfig`. Returns the coordinator's `PushResponse` shape as a
/// raw `serde_json::Value` (small enough not to need its own DTO here).
pub async fn push_project(
    client: &reqwest::Client,
    base_url: &str,
    name: &str,
    hcl_path: &std::path::Path,
    is_dev: bool,
) -> anyhow::Result<serde_json::Value> {
    let config = canoe_dsl::compile_file(hcl_path)?;
    let url = format!("{}/projects/{}", base_url.trim_end_matches('/'), name);
    let body = serde_json::json!({ "config": config, "is_dev": is_dev });
    let resp = client.post(&url).json(&body).send().await?.error_for_status()?;
    Ok(resp.json::<serde_json::Value>().await?)
}

/// `POST /projects/:name/adapters/:adapter/trigger` — fires one
/// off-schedule adapter run.
pub async fn trigger_adapter(client: &reqwest::Client, base_url: &str, project: &str, adapter: &str) -> anyhow::Result<()> {
    let url = format!("{}/projects/{project}/adapters/{adapter}/trigger", base_url.trim_end_matches('/'));
    client.post(&url).send().await?.error_for_status()?;
    Ok(())
}

/// `POST /projects/:name/pipelines/:pipeline/trigger` — synthesizes that
/// pipeline's `trigger.signal` right now.
pub async fn trigger_pipeline(client: &reqwest::Client, base_url: &str, project: &str, pipeline: &str) -> anyhow::Result<()> {
    let url = format!("{}/projects/{project}/pipelines/{pipeline}/trigger", base_url.trim_end_matches('/'));
    client.post(&url).send().await?.error_for_status()?;
    Ok(())
}

/// `POST /projects/:name/steps/:step/cancel` — a real pause, not a
/// kill-and-immediately-restart; see `canoe-coordinator`'s
/// `projects::cancel_step_handler` docs for exactly what "cancel" stops
/// for each step kind.
pub async fn cancel_step(client: &reqwest::Client, base_url: &str, project: &str, step: &str) -> anyhow::Result<()> {
    let url = format!("{}/projects/{project}/steps/{step}/cancel", base_url.trim_end_matches('/'));
    client.post(&url).send().await?.error_for_status()?;
    Ok(())
}

pub async fn rerun_step(client: &reqwest::Client, base_url: &str, project: &str, step: &str) -> anyhow::Result<()> {
    let url = format!("{}/projects/{project}/steps/{step}/rerun", base_url.trim_end_matches('/'));
    client.post(&url).send().await?.error_for_status()?;
    Ok(())
}

pub async fn cancel_task(client: &reqwest::Client, base_url: &str, project: &str, task: &str) -> anyhow::Result<()> {
    let url = format!("{}/projects/{project}/tasks/{task}/cancel", base_url.trim_end_matches('/'));
    client.post(&url).send().await?.error_for_status()?;
    Ok(())
}

pub async fn rerun_task(client: &reqwest::Client, base_url: &str, project: &str, task: &str) -> anyhow::Result<()> {
    let url = format!("{}/projects/{project}/tasks/{task}/rerun", base_url.trim_end_matches('/'));
    client.post(&url).send().await?.error_for_status()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flatten_combines_pipelines_and_adapters_across_every_project() {
        let raw = RawStatusResponse {
            projects: vec![
                ProjectStatusDto {
                    project: "default".to_string(),
                    is_dev: false,
                    pushed_at: Utc::now(),
                    pipelines: vec![PipelineStatus { pipeline: "default--orders".to_string(), chains: vec![] }],
                    adapters: vec![AdapterStatusDto {
                        name: "default--orders-adapter".to_string(),
                        last_run_at: None,
                        last_success: None,
                        last_error: None,
                        next_run_at: None,
                    }],
                    tasks: vec![TaskStatusDto {
                        name: "default--notify".to_string(),
                        state: TaskState::Pending,
                        last_run_at: None,
                        last_error: None,
                    }],
                },
                ProjectStatusDto {
                    project: "my-pipeline".to_string(),
                    is_dev: true,
                    pushed_at: Utc::now(),
                    pipelines: vec![PipelineStatus { pipeline: "my-pipeline--orders".to_string(), chains: vec![] }],
                    adapters: vec![],
                    tasks: vec![],
                },
            ],
        };
        let flat = flatten(raw);
        assert_eq!(flat.pipelines.len(), 2);
        assert_eq!(flat.adapters.len(), 1);
        assert_eq!(flat.tasks.len(), 1);
        assert_eq!(flat.pipelines[0].pipeline, "default--orders");
        assert_eq!(flat.pipelines[1].pipeline, "my-pipeline--orders");
        assert_eq!(flat.tasks[0].name, "default--notify");
    }

    #[test]
    fn split_project_prefix_splits_on_the_first_double_dash() {
        assert_eq!(split_project_prefix("my-pipeline--orders_adapter"), Some(("my-pipeline", "orders_adapter")));
        assert_eq!(split_project_prefix("no-separator-here"), None);
    }

    /// Confirms `push_project` actually compiles locally via
    /// `canoe_dsl::compile_file` (not something the coordinator does —
    /// see the crate-level docs) and POSTs the expected
    /// `{config, is_dev}` shape to `/projects/:name`. No mocking library
    /// in this workspace's dependencies, so this spins up a tiny local
    /// TCP listener that reads one raw HTTP request and inspects it
    /// directly, same "no mocking needed, just a real minimal fixture"
    /// precedent `test_lookup.py`/`test_run_once.py` already establish
    /// on the Python side of this codebase.
    #[tokio::test]
    async fn push_project_compiles_locally_and_posts_the_expected_shape() {
        use std::io::Write as _;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let dir = std::env::temp_dir().join(format!("canoe-tui-push-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let hcl_path = dir.join("adapter.canoe.hcl");
        let mut f = std::fs::File::create(&hcl_path).unwrap();
        write!(
            f,
            r#"
adapter "orders_adapter" {{
  topic = "orders.raw"
  schedule {{ interval = "5m" }}
  script {{ code = "print('hi')" }}
}}
"#
        )
        .unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 65536];
            let mut total = 0;
            // Read headers first.
            let header_end;
            loop {
                let n = socket.read(&mut buf[total..]).await.unwrap();
                total += n;
                if let Some(pos) = find_subslice(&buf[..total], b"\r\n\r\n") {
                    header_end = pos + 4;
                    break;
                }
            }
            let header_text = String::from_utf8_lossy(&buf[..header_end]).to_string();
            let content_length: usize = header_text
                .lines()
                .find_map(|l| l.to_lowercase().starts_with("content-length:").then(|| l.split(':').nth(1).unwrap().trim().parse().unwrap()))
                .unwrap_or(0);
            while total < header_end + content_length {
                let n = socket.read(&mut buf[total..]).await.unwrap();
                total += n;
            }
            let body = String::from_utf8_lossy(&buf[header_end..header_end + content_length]).to_string();

            let request_line = header_text.lines().next().unwrap().to_string();
            let response = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}";
            socket.write_all(response).await.unwrap();
            (request_line, body)
        });

        let client = reqwest::Client::new();
        let result = push_project(&client, &format!("http://{addr}"), "my-pipeline", &hcl_path, true).await;
        std::fs::remove_dir_all(&dir).ok();

        result.expect("push should succeed against the local test server");
        let (request_line, body) = server.await.unwrap();
        assert!(request_line.starts_with("POST /projects/my-pipeline "), "unexpected request line: {request_line}");

        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["is_dev"], true);
        // `config` is the real compiled `PipelineConfig` — confirms
        // `compile_file` actually ran, not a stub/placeholder.
        assert_eq!(parsed["config"]["adapters"][0]["name"], "orders_adapter");
    }

    fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack.windows(needle.len()).position(|w| w == needle)
    }
}
