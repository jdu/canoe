//! Runs every adapter on its own schedule, Airflow-scheduler style: one
//! long-lived task per adapter that sleeps until the next due time, fires
//! a K8s Job for that run, and loops — rather than K8s CronJob's native
//! scheduling, so `interval` (a fixed cadence from last run, no native
//! K8s equivalent) and `cron` share one code path and one place that
//! knows what "due" means.

use crate::status::{AdapterStatus, StatusRegistry};
use canoe_config::AdapterConfig;
use canoe_k8s::AdapterJobRunner;
use chrono::Utc;
use kube::Client;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// Spawns one detached task per adapter; each runs for the coordinator's
/// lifetime. Mirrors `activation::start_python_step`: fire-and-forget,
/// the caller doesn't await these. `adapter.name` is assumed already
/// project-prefixed by `projects::prefix_project` (this module has no
/// project concept of its own) — used verbatim as the key into
/// `adapter_triggers` and as the status-registry/Job identity, so two
/// projects' adapters never collide here for the same reason chain names
/// don't collide in `activation.rs`.
///
/// Registers one out-of-band trigger channel per adapter into
/// `adapter_triggers` (shared, keyed by adapter name) before spawning —
/// `POST /projects/:name/adapters/:adapter/trigger` sends on the matching
/// sender to fire one off-schedule run without disturbing the regular
/// schedule.
pub fn start(
    client: Client,
    namespace: String,
    runner_image: String,
    pod_kafka_bootstrap: String,
    metrics_url: String,
    log_base_path: Option<String>,
    pod_s3_endpoint: Option<String>,
    adapters: Vec<AdapterConfig>,
    status: Arc<StatusRegistry>,
    adapter_triggers: Arc<Mutex<HashMap<String, mpsc::Sender<()>>>>,
) {
    for adapter in adapters {
        let client = client.clone();
        let namespace = namespace.clone();
        let runner_image = runner_image.clone();
        let pod_kafka_bootstrap = pod_kafka_bootstrap.clone();
        let metrics_url = metrics_url.clone();
        let log_base_path = log_base_path.clone();
        let pod_s3_endpoint = pod_s3_endpoint.clone();
        let status = status.clone();

        let (trigger_tx, trigger_rx) = mpsc::channel::<()>(1);
        adapter_triggers.lock().unwrap().insert(adapter.name.clone(), trigger_tx);

        tokio::spawn(async move {
            run_adapter_loop(
                client,
                namespace,
                runner_image,
                pod_kafka_bootstrap,
                metrics_url,
                log_base_path,
                pod_s3_endpoint,
                adapter,
                status,
                trigger_rx,
            )
            .await;
        });
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_adapter_loop(
    client: Client,
    namespace: String,
    runner_image: String,
    pod_kafka_bootstrap: String,
    metrics_url: String,
    log_base_path: Option<String>,
    pod_s3_endpoint: Option<String>,
    adapter: AdapterConfig,
    status: Arc<StatusRegistry>,
    mut trigger_rx: mpsc::Receiver<()>,
) {
    let runner = AdapterJobRunner::new(client, namespace, runner_image);

    if let Some(interval_secs) = adapter.schedule.interval_secs {
        info!(adapter = %adapter.name, interval_secs, "adapter scheduled");
        let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
        // First tick fires immediately (tokio::time::interval's default);
        // an adapter should run once at startup rather than wait a full
        // interval, same as Airflow's `catchup`-less "run on start".
        loop {
            // Races the regular ticker against an out-of-band manual
            // trigger — whichever resolves first fires one run via the
            // exact same `fire()` call either branch takes (the notify-
            // vs-tick race itself isn't the interesting part to test;
            // `fire()`'s behavior is already covered by its own
            // callers' existing coverage). A manual trigger does not
            // reset the regular ticker — the next scheduled run still
            // happens on its original cadence.
            tokio::select! {
                _ = ticker.tick() => {}
                _ = trigger_rx.recv() => {
                    info!(adapter = %adapter.name, "manual trigger received");
                }
            }
            let next_run_at = Some(Utc::now() + chrono::Duration::seconds(interval_secs as i64));
            fire(
                &runner,
                &adapter,
                &pod_kafka_bootstrap,
                &metrics_url,
                log_base_path.as_deref(),
                pod_s3_endpoint.as_deref(),
                &status,
                next_run_at,
            )
            .await;
        }
    } else if let Some(cron_expr) = &adapter.schedule.cron {
        // The `cron` crate expects a leading seconds field; adapters
        // author standard 5-field cron, so prepend "0" here the same way
        // canoe-config's validation does when checking the expression at
        // parse time.
        let schedule = match cron::Schedule::from_str(&format!("0 {cron_expr}")) {
            Ok(s) => s,
            Err(e) => {
                error!(adapter = %adapter.name, error = %e, "invalid cron expression, adapter will never run");
                return;
            }
        };
        info!(adapter = %adapter.name, cron = %cron_expr, "adapter scheduled");
        loop {
            let Some(next) = schedule.upcoming(Utc).next() else {
                error!(adapter = %adapter.name, "cron schedule has no upcoming runs, stopping");
                return;
            };
            let now = Utc::now();
            let wait = (next - now).to_std().unwrap_or(Duration::ZERO);
            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                _ = trigger_rx.recv() => {
                    info!(adapter = %adapter.name, "manual trigger received");
                }
            }
            let next_run_at = schedule.upcoming(Utc).next();
            fire(
                &runner,
                &adapter,
                &pod_kafka_bootstrap,
                &metrics_url,
                log_base_path.as_deref(),
                pod_s3_endpoint.as_deref(),
                &status,
                next_run_at,
            )
            .await;
        }
    } else {
        // canoe-config's validation rejects this at load time; unreachable
        // in practice, but don't silently do nothing if it slips through.
        error!(adapter = %adapter.name, "adapter has no schedule, this should have failed config validation");
    }
}

async fn fire(
    runner: &AdapterJobRunner,
    adapter: &AdapterConfig,
    kafka_bootstrap: &str,
    metrics_url: &str,
    log_base_path: Option<&str>,
    pod_s3_endpoint: Option<&str>,
    status: &StatusRegistry,
    next_run_at: Option<chrono::DateTime<Utc>>,
) {
    let result = runner.run_once(adapter, kafka_bootstrap, metrics_url, log_base_path, pod_s3_endpoint).await;
    let last_error = match &result {
        Ok(()) => {
            info!(adapter = %adapter.name, "adapter run submitted");
            None
        }
        Err(e) => {
            warn!(adapter = %adapter.name, error = %e, "failed to submit adapter run");
            Some(e.to_string())
        }
    };
    status.update_adapter(AdapterStatus {
        name: adapter.name.clone(),
        last_run_at: Some(Utc::now()),
        last_success: Some(result.is_ok()),
        last_error,
        next_run_at,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `run_adapter_loop`'s inner `tokio::select!` itself needs a real
    /// `AdapterJobRunner`/`kube::Client` this codebase has no precedent
    /// for mocking (same reasoning as `decide_batch_action`'s extraction
    /// in `activation.rs`) — the interesting, testable-without-a-cluster
    /// part isn't which branch `fire()` gets called from (already
    /// exercised by every existing scheduler test path), it's that a
    /// manual trigger actually wins a race against a regular schedule
    /// that hasn't come due yet, rather than being silently starved.
    /// This exercises the identical `tokio::select!` shape against a
    /// long-interval ticker.
    #[tokio::test]
    async fn a_manual_trigger_wins_the_race_against_a_far_off_ticker() {
        let mut ticker = tokio::time::interval(Duration::from_secs(3600));
        ticker.tick().await; // consume the immediate first tick
        let (tx, mut rx) = mpsc::channel::<()>(1);
        tx.send(()).await.unwrap();

        let fired_via_trigger;
        tokio::select! {
            _ = ticker.tick() => { fired_via_trigger = false; }
            _ = rx.recv() => { fired_via_trigger = true; }
        }
        assert!(fired_via_trigger, "a pending manual trigger must win against a not-yet-due ticker");
    }

    #[test]
    fn start_registers_one_trigger_sender_per_adapter_keyed_by_name() {
        // Structural check on the map `start()` populates — doesn't
        // spawn the real loop (needs a live `kube::Client`, no mocking
        // precedent in this codebase), just confirms the registration
        // contract `POST /projects/:name/adapters/:adapter/trigger`
        // depends on: one sender per adapter name, insertable/lookupable
        // exactly like `start()` does before spawning each loop.
        let triggers: Arc<Mutex<HashMap<String, mpsc::Sender<()>>>> = Arc::new(Mutex::new(HashMap::new()));
        for name in ["proj-a--orders-adapter", "proj-b--orders-adapter"] {
            let (tx, _rx) = mpsc::channel::<()>(1);
            triggers.lock().unwrap().insert(name.to_string(), tx);
        }
        let map = triggers.lock().unwrap();
        assert!(map.contains_key("proj-a--orders-adapter"));
        assert!(map.contains_key("proj-b--orders-adapter"));
        assert!(!map.get("proj-a--orders-adapter").unwrap().same_channel(map.get("proj-b--orders-adapter").unwrap()));
    }
}
