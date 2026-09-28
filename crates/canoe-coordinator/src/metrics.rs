//! A minimal, built-in Prometheus Pushgateway: every task canoe runs
//! (Python/uv step worker pods, adapter Jobs) is either short-lived (an
//! adapter's Job may finish before Prometheus's next scrape ever reaches
//! it) or scales up and down on its own schedule (a step's worker
//! Deployment) — in both cases, having Prometheus discover and scrape
//! each pod directly means fighting service discovery for a constantly
//! changing set of targets. Instead, every task pushes its own metrics
//! here over plain HTTP, the coordinator holds the latest value per
//! metric+label combination in memory, and Prometheus only ever needs
//! one static scrape target: the coordinator itself.
//!
//! This deliberately isn't a full Pushgateway (no job/instance grouping
//! semantics, no metric deletion API) — see the module docs on `Store`
//! for what "holds the latest value" actually means for counters vs.
//! gauges, and the crate-level README section "Metrics" for the wire
//! protocol every task pushes.

use crate::projects::ProjectRegistry;
use crate::status::StatusRegistry;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tracing::{info, warn};

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MetricType {
    Counter,
    Gauge,
}

impl MetricType {
    fn as_prometheus_str(self) -> &'static str {
        match self {
            MetricType::Counter => "counter",
            MetricType::Gauge => "gauge",
        }
    }
}

#[derive(Deserialize)]
pub struct ReportRequest {
    metrics: Vec<ReportedMetric>,
}

#[derive(Deserialize)]
struct ReportedMetric {
    name: String,
    #[serde(rename = "type")]
    metric_type: MetricType,
    value: f64,
    #[serde(default)]
    labels: HashMap<String, String>,
}

/// `(metric name, sorted label pairs)` — two pushes with the same name
/// and the same label set overwrite the same entry.
type Key = (String, Vec<(String, String)>);

struct Entry {
    metric_type: MetricType,
    value: f64,
}

/// Holds the *last pushed value* per metric+label combination — a cache,
/// not an aggregator: pushing a counter value of `5` and then `3` stores
/// `3`, it does not sum them. This matches real Pushgateway semantics,
/// and is why task-side reporters must track their own cumulative totals
/// and push the current total each time (see the wire-protocol docs),
/// not push a delta and expect the coordinator to add it up.
///
/// A consequence worth knowing: for a genuinely one-shot task (an
/// adapter's Job), every separate run pushes under the same grouping key
/// and overwrites the last run's value — so a `counter` from a one-shot
/// task doesn't behave like a real Prometheus counter (`rate()` over it
/// would see it reset to whatever the new run's value is, every run, not
/// accumulate). The convention documented for adapters is to push
/// `gauge`s instead (`..._last_run_success`, `..._last_run_duration_seconds`,
/// `..._last_run_timestamp_seconds`) — see workers/adapter-runner/run.py.
/// `counter` is the right choice for a long-lived task (a step's worker
/// pod) that keeps the same grouping key and keeps incrementing the same
/// running total for as long as it's alive.
#[derive(Clone)]
pub struct Store(Arc<Mutex<HashMap<Key, Entry>>>);

impl Store {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(HashMap::new())))
    }

    fn report(&self, req: ReportRequest) {
        let mut store = self.0.lock().unwrap();
        for m in req.metrics {
            let mut labels: Vec<(String, String)> = m.labels.into_iter().collect();
            labels.sort();
            store.insert((m.name, labels), Entry { metric_type: m.metric_type, value: m.value });
        }
    }

    /// Renders every stored metric as Prometheus text exposition format
    /// (`# TYPE ...` line per distinct metric name, then one sample line
    /// per label combination).
    fn render(&self) -> String {
        let store = self.0.lock().unwrap();

        // Group by metric name so each name gets exactly one `# TYPE`
        // line, regardless of how many label combinations it has.
        let mut by_name: HashMap<&str, (MetricType, Vec<(&[(String, String)], f64)>)> = HashMap::new();
        for ((name, labels), entry) in store.iter() {
            by_name
                .entry(name.as_str())
                .or_insert_with(|| (entry.metric_type, Vec::new()))
                .1
                .push((labels.as_slice(), entry.value));
        }

        let mut names: Vec<&str> = by_name.keys().copied().collect();
        names.sort();

        let mut out = String::new();
        for name in names {
            let (metric_type, samples) = &by_name[name];
            let _ = writeln!(out, "# TYPE {name} {}", metric_type.as_prometheus_str());
            for (labels, value) in samples {
                if labels.is_empty() {
                    let _ = writeln!(out, "{name} {value}");
                } else {
                    let rendered = labels
                        .iter()
                        .map(|(k, v)| format!("{k}=\"{}\"", escape_label_value(v)))
                        .collect::<Vec<_>>()
                        .join(",");
                    let _ = writeln!(out, "{name}{{{rendered}}} {value}");
                }
            }
        }
        out
    }
}

fn escape_label_value(v: &str) -> String {
    v.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n")
}

async fn report_handler(State(store): State<Store>, body: Result<Json<ReportRequest>, axum::extract::rejection::JsonRejection>) -> StatusCode {
    match body {
        Ok(Json(req)) => {
            store.report(req);
            StatusCode::NO_CONTENT
        }
        Err(e) => {
            warn!(error = %e, "rejected malformed metrics report");
            StatusCode::BAD_REQUEST
        }
    }
}

async fn metrics_handler(State(store): State<Store>) -> String {
    store.render()
}

/// Binds and serves the metrics + status + project-management endpoints
/// forever. Routes: `POST /metrics/report` (every task pushes here),
/// `GET /metrics` (Prometheus scrapes here), `GET /status` (`crate::status`
/// — introspection for `canoe-tui`/other read-only clients), and,
/// merged in by the caller via `projects_router` (built by
/// `crate::projects::router`, which needs a whole `Ctx` — deliberately
/// kept out of this module's own signature so its existing tests don't
/// need to construct a `kube::Client`): `POST /projects/:name` push,
/// `.../adapters/:adapter/trigger`, `.../pipelines/:pipeline/trigger`.
/// All merged onto this same router/port rather than separate servers
/// since it's cheap to share one bound address. Detached: the caller
/// doesn't await this — same fire-and-forget convention as
/// `scheduler::start`/`archiver::start`.
fn app(
    store: Store,
    projects: Arc<ProjectRegistry>,
    status: Arc<StatusRegistry>,
    control: Arc<Mutex<HashMap<String, bool>>>,
    projects_router: Router,
) -> Router {
    let metrics_router = Router::new()
        .route("/metrics/report", post(report_handler))
        .route("/metrics", get(metrics_handler))
        .with_state(store);
    metrics_router.merge(crate::status::router(projects, status, control)).merge(projects_router)
}

pub fn start(
    store: Store,
    projects: Arc<ProjectRegistry>,
    status: Arc<StatusRegistry>,
    control: Arc<Mutex<HashMap<String, bool>>>,
    projects_router: Router,
    addr: SocketAddr,
) {
    tokio::spawn(async move {
        let app = app(store, projects, status, control, projects_router);

        let listener = match tokio::net::TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(%addr, error = %e, "failed to bind metrics server");
                return;
            }
        };
        info!(%addr, "metrics server listening (GET /metrics for Prometheus, POST /metrics/report for tasks, GET /status for introspection)");
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!(error = %e, "metrics server exited");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    fn empty_projects() -> Arc<ProjectRegistry> {
        Arc::new(ProjectRegistry::new())
    }

    #[test]
    fn later_push_overwrites_same_key() {
        let store = Store::new();
        store.report(ReportRequest {
            metrics: vec![ReportedMetric {
                name: "canoe_test_total".to_string(),
                metric_type: MetricType::Counter,
                value: 5.0,
                labels: labels(&[("step", "validate")]),
            }],
        });
        store.report(ReportRequest {
            metrics: vec![ReportedMetric {
                name: "canoe_test_total".to_string(),
                metric_type: MetricType::Counter,
                value: 12.0,
                labels: labels(&[("step", "validate")]),
            }],
        });
        let rendered = store.render();
        assert!(rendered.contains("canoe_test_total{step=\"validate\"} 12"));
        assert!(!rendered.contains(" 5"));
    }

    #[test]
    fn distinct_label_sets_are_independent_entries() {
        let store = Store::new();
        store.report(ReportRequest {
            metrics: vec![
                ReportedMetric {
                    name: "canoe_test_total".to_string(),
                    metric_type: MetricType::Counter,
                    value: 1.0,
                    labels: labels(&[("step", "a")]),
                },
                ReportedMetric {
                    name: "canoe_test_total".to_string(),
                    metric_type: MetricType::Counter,
                    value: 2.0,
                    labels: labels(&[("step", "b")]),
                },
            ],
        });
        let rendered = store.render();
        assert!(rendered.contains("canoe_test_total{step=\"a\"} 1"));
        assert!(rendered.contains("canoe_test_total{step=\"b\"} 2"));
        assert_eq!(rendered.matches("# TYPE canoe_test_total").count(), 1);
    }

    #[test]
    fn renders_valid_type_line_and_escapes_label_values() {
        let store = Store::new();
        store.report(ReportRequest {
            metrics: vec![ReportedMetric {
                name: "canoe_test_gauge".to_string(),
                metric_type: MetricType::Gauge,
                value: 3.5,
                labels: labels(&[("msg", "quote\"and\\backslash")]),
            }],
        });
        let rendered = store.render();
        assert!(rendered.contains("# TYPE canoe_test_gauge gauge"));
        assert!(rendered.contains(r#"msg="quote\"and\\backslash""#));
    }

    /// End-to-end through the real HTTP handlers (no network — `oneshot`
    /// drives the `Router` in-process) — the same request shape a task's
    /// `urllib.request.urlopen` POST or Prometheus's own GET would send.
    #[tokio::test]
    async fn report_then_scrape_round_trips_through_http() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use http_body_util::BodyExt;
        use tower::ServiceExt;

        let store = Store::new();
        let router = app(store, empty_projects(), Arc::new(StatusRegistry::new()), Arc::new(Mutex::new(HashMap::new())), Router::new());

        let report_body = serde_json::json!({
            "metrics": [
                {"name": "canoe_step_messages_total", "type": "counter", "value": 7, "labels": {"step": "validate"}}
            ]
        });
        let report_req = Request::builder()
            .method("POST")
            .uri("/metrics/report")
            .header("content-type", "application/json")
            .body(Body::from(report_body.to_string()))
            .unwrap();
        let report_resp = router.clone().oneshot(report_req).await.unwrap();
        assert_eq!(report_resp.status(), StatusCode::NO_CONTENT);

        let scrape_req = Request::builder().method("GET").uri("/metrics").body(Body::empty()).unwrap();
        let scrape_resp = router.oneshot(scrape_req).await.unwrap();
        assert_eq!(scrape_resp.status(), StatusCode::OK);
        let bytes = scrape_resp.into_body().collect().await.unwrap().to_bytes();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(text.contains("# TYPE canoe_step_messages_total counter"));
        assert!(text.contains(r#"canoe_step_messages_total{step="validate"} 7"#));
    }

    #[tokio::test]
    async fn malformed_report_body_is_rejected_not_panicked_on() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let store = Store::new();
        let router = app(store, empty_projects(), Arc::new(StatusRegistry::new()), Arc::new(Mutex::new(HashMap::new())), Router::new());
        let req = Request::builder()
            .method("POST")
            .uri("/metrics/report")
            .header("content-type", "application/json")
            .body(Body::from("not json"))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }
}
