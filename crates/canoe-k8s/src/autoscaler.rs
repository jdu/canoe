//! Drives a step's pod count based on observed Kafka consumer lag, in
//! place of a generic HPA — the coordinator already computes lag to
//! decide routing/backpressure, so it reuses that signal here directly.
//! Manages plain `Pod`s (`pod_manager.rs`), not a Deployment — see that
//! module's docs for why — which means this reconcile loop also owns
//! cleaning up dead pods and a circuit breaker that stops replacing ones
//! that keep failing fast, instead of relying on kubelet's own
//! crash-loop backoff (which doesn't exist here: `restartPolicy: Never`
//! means a crashed pod never restarts on its own at all).

use crate::pod_manager::PodManager;
use crate::s3_backlog::S3BacklogInspector;
use canoe_config::{KafkaPythonStep, ScalingConfig, UvStep};
use canoe_kafka::LagInspector;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

/// A pod younger than this when it fails counts as a "fast" failure for
/// the circuit breaker — old enough to have plausibly done real work
/// first doesn't tighten it. Deliberately shorter than the default
/// `reconcile_interval_secs` (20s) so a pod that fails before ever really
/// starting is unambiguously "fast," not an artifact of how often this
/// loop happens to run.
const FAST_FAILURE_WINDOW: Duration = Duration::from_secs(30);

/// How long the circuit breaker waits, once open, before allowing a
/// single retry pod — fixed, not exponential-backoff, by design: this is
/// a "did the underlying problem go away" probe on a slow, bounded
/// cadence, not a race to reconnect as fast as possible the way a
/// network client's backoff would be. See README.md's "Step pods, not
/// Deployments" for the tradeoff.
const CIRCUIT_COOLDOWN: Duration = Duration::from_secs(300);

#[derive(Default)]
struct CircuitState {
    consecutive_fast_failures: u32,
    open_until: Option<Instant>,
}

/// One pod being cleaned up this reconcile pass, as far as the circuit
/// breaker cares.
struct FailedPod {
    fast: bool,
}

/// What one reconcile pass observed, for a caller (`canoe-coordinator`'s
/// `StatusRegistry`) that wants to surface it somewhere other than logs —
/// see `canoe_config::StepConfig`'s docs for why replica/backlog/lag are
/// all optional at the `StepStatus` level even though this report always
/// fills them in for a step type that has them.
#[derive(Debug, Clone, Copy)]
pub struct ReconcileReport {
    pub desired_replicas: u32,
    pub alive_replicas: u32,
    pub circuit_open: bool,
    /// S3 unprocessed run count — `Some` for `PythonStep`/`UvStep`
    /// (S3 + signal transport), `None` for `KafkaPythonStep`.
    pub backlog: Option<i64>,
    /// Kafka consumer lag — `Some` for `KafkaPythonStep`, `None` for
    /// `PythonStep`/`UvStep`.
    pub lag: Option<i64>,
}

/// What `reconcile`'s backlog check reads from — a Kafka consumer group's
/// lag (`KafkaPythonStep`, `dynamic_route`'s per-value pods, still
/// Kafka-topic transport) or an S3 backlog of unprocessed `run_id=*`
/// directories (`PythonStep`/`UvStep`, S3 + signal transport). See
/// `canoe_config::StepConfig::input_ref`'s docs for the transport split.
enum Work<'a> {
    Kafka { topic: &'a str },
    S3 { input_path: &'a str, checkpoint_path: &'a str },
}

pub struct Autoscaler {
    pods: PodManager,
    lag: LagInspector,
    /// `None` for an `Autoscaler` that only ever reconciles Kafka-topic
    /// transport steps (`dynamic_route`'s `KafkaPythonStep` pods) — no S3
    /// backlog to read for those, so there's no point requiring a caller
    /// to construct one just to satisfy this field.
    backlog: Option<S3BacklogInspector>,
    last_scaled: HashMap<String, Instant>,
    circuits: HashMap<String, CircuitState>,
}

impl Autoscaler {
    pub fn new(pods: PodManager, lag: LagInspector, backlog: Option<S3BacklogInspector>) -> Self {
        Self {
            pods,
            lag,
            backlog,
            last_scaled: HashMap::new(),
            circuits: HashMap::new(),
        }
    }

    /// Same as this module's now-removed `reconcile_step` (batch-mode
    /// Python steps moved to Job-based execution — see
    /// `crates/canoe-k8s/src/step_job.rs` and
    /// `crates/canoe-coordinator/src/activation.rs::start_python_step` —
    /// so there's no longer a persistent pod set to reconcile for them),
    /// for a `uv`-engine step's pods. Identical scaling logic — a uv
    /// step's pods are plain `Pod`s like any other, just running a
    /// different container image/command.
    pub async fn reconcile_uv_step(
        &mut self,
        chain_name: &str,
        step: &UvStep,
        bootstrap_servers: &str,
        metrics_url: &str,
        log_base_path: Option<&str>,
        pod_s3_endpoint: Option<&str>,
    ) -> anyhow::Result<ReconcileReport> {
        let pod_set = PodManager::uv_pod_set_name(chain_name, &step.name);
        let checkpoint_path = format!("{}/.checkpoint.json", step.output_path.trim_end_matches('/'));
        let work = Work::S3 { input_path: &step.input_path, checkpoint_path: &checkpoint_path };
        let (to_create, report) = self.reconcile(&pod_set, &step.name, work, &step.scaling).await?;
        for _ in 0..to_create {
            self.pods.create_uv_pod(chain_name, step, bootstrap_servers, metrics_url, log_base_path, pod_s3_endpoint).await?;
        }
        Ok(report)
    }

    /// Same as `reconcile_uv_step`, for one of `dynamic_route`'s
    /// per-discovered-value pods — these stay Kafka-topic transport (see
    /// `canoe_config::KafkaPythonStep`'s docs), so this reads Kafka
    /// consumer lag same as before this migration.
    pub async fn reconcile_kafka_python_step(
        &mut self,
        chain_name: &str,
        step: &KafkaPythonStep,
        bootstrap_servers: &str,
        metrics_url: &str,
        log_base_path: Option<&str>,
        pod_s3_endpoint: Option<&str>,
    ) -> anyhow::Result<ReconcileReport> {
        let pod_set = PodManager::pod_set_name(chain_name, &step.name);
        let work = Work::Kafka { topic: &step.input_topic };
        let (to_create, report) = self.reconcile(&pod_set, &step.name, work, &step.scaling).await?;
        for _ in 0..to_create {
            self.pods.create_kafka_python_pod(chain_name, step, bootstrap_servers, metrics_url, log_base_path, pod_s3_endpoint).await?;
        }
        Ok(report)
    }

    /// Shared reconcile logic: lists/cleans up pods, updates the circuit
    /// breaker, and decides how many pods to scale by. Returns how many
    /// *new* pods the caller should create (0 if scaling down, holding
    /// steady, on cooldown, or circuit-open) alongside a `ReconcileReport`
    /// snapshot of what this pass observed — creation itself stays in the
    /// caller since building the right pod spec needs the caller's own
    /// step type. The report is still filled in (with `backlog`/`lag`
    /// `None` where a read failed) on every early return below, so a
    /// caller updating a status registry never has to special-case "we
    /// bailed early this pass."
    async fn reconcile(
        &mut self,
        pod_set: &str,
        step_name: &str,
        work: Work<'_>,
        scaling: &ScalingConfig,
    ) -> anyhow::Result<(u32, ReconcileReport)> {
        let now = Instant::now();

        // 1. See what's actually running, clean up anything dead, and
        // fold dead pods into the circuit breaker before making any
        // scaling decision this pass.
        let pod_states = self.pods.list_pods(pod_set).await?;
        let mut alive = 0u32;
        let mut failed = Vec::new();
        for pod in pod_states {
            if pod.alive {
                alive += 1;
                continue;
            }
            failed.push(FailedPod { fast: pod.age < FAST_FAILURE_WINDOW });
            if let Err(e) = self.pods.delete_pod(&pod.name).await {
                warn!(step = %step_name, pod = %pod.name, error = %e, "failed to clean up dead pod, will retry next reconcile");
            }
        }

        let circuit = self.circuits.entry(pod_set.to_string()).or_default();
        update_circuit(circuit, &failed, now, scaling.backoff_limit);
        if !failed.is_empty() {
            info!(step = %step_name, dead_pods = failed.len(), alive, "cleaned up dead pods");
        }
        let circuit_open = is_circuit_open(circuit, now);
        let report_with = |desired_replicas: u32, backlog: Option<i64>, lag: Option<i64>| ReconcileReport {
            desired_replicas,
            alive_replicas: alive,
            circuit_open,
            backlog,
            lag,
        };

        // 2. Normal cooldown gate, same as before.
        if let Some(last) = self.last_scaled.get(pod_set) {
            if last.elapsed() < Duration::from_secs(scaling.cooldown_secs) {
                return Ok((0, report_with(alive, None, None)));
            }
        }

        let (total_lag, backlog_for_report, lag_for_report) = match work {
            Work::Kafka { topic } => match self.lag.lag_for(topic, pod_set) {
                Ok(lag) => (lag.total_lag, None, Some(lag.total_lag)),
                Err(e) => {
                    warn!(step = %step_name, error = %e, "failed to read consumer lag, skipping scale decision");
                    return Ok((0, report_with(alive, None, None)));
                }
            },
            Work::S3 { input_path, checkpoint_path } => {
                let Some(backlog) = &self.backlog else {
                    warn!(step = %step_name, "autoscaler has no S3 backlog inspector configured, skipping scale decision");
                    return Ok((0, report_with(alive, None, None)));
                };
                match backlog.backlog_for(input_path, checkpoint_path).await {
                    Ok(backlog) => (backlog.unprocessed_run_count, Some(backlog.unprocessed_run_count), None),
                    Err(e) => {
                        warn!(step = %step_name, error = %e, "failed to read S3 backlog, skipping scale decision");
                        return Ok((0, report_with(alive, None, None)));
                    }
                }
            }
        };

        let desired = desired_replicas(total_lag, alive, scaling);
        let report = report_with(desired, backlog_for_report, lag_for_report);

        if desired == alive {
            return Ok((0, report));
        }

        if desired > alive {
            if !circuit_allows_replacement(circuit, now) {
                error!(
                    step = %step_name,
                    consecutive_fast_failures = circuit.consecutive_fast_failures,
                    "circuit open: not creating replacement pods for this step (it keeps failing fast) — fix the step and it will retry automatically after the cooldown"
                );
                return Ok((0, report));
            }
            let to_create = desired - alive;
            info!(step = %step_name, backlog = total_lag, alive, desired, "scaling up pod set");
            self.last_scaled.insert(pod_set.to_string(), now);
            Ok((to_create, report))
        } else {
            info!(step = %step_name, backlog = total_lag, alive, desired, "scaling down pod set");
            let pod_states = self.pods.list_pods(pod_set).await?;
            let to_remove = alive.saturating_sub(desired);
            for pod in pod_states.into_iter().filter(|p| p.alive).take(to_remove as usize) {
                self.pods.delete_pod(&pod.name).await?;
            }
            self.last_scaled.insert(pod_set.to_string(), now);
            Ok((0, report))
        }
    }
}

/// Read-only "is the circuit currently open" check for reporting —
/// distinct from `circuit_allows_replacement`, which mutates state (it
/// clears `open_until` once the cooldown has elapsed, to permit exactly
/// one retry). This never mutates, so it's safe to call every reconcile
/// pass purely to populate a status snapshot.
fn is_circuit_open(state: &CircuitState, now: Instant) -> bool {
    matches!(state.open_until, Some(until) if now < until)
}

/// Rolls this pass's dead pods into the circuit breaker's state: a fast
/// failure increments the streak; a slow one (the pod ran long enough to
/// plausibly have done real work) resets it — a step that's been healthy
/// for a while shouldn't have one unrelated crash count toward tripping
/// the breaker. Opens (sets `open_until`) the moment the streak reaches
/// `backoff_limit`, if it isn't open already.
fn update_circuit(state: &mut CircuitState, failed: &[FailedPod], now: Instant, backoff_limit: u32) {
    for pod in failed {
        if pod.fast {
            state.consecutive_fast_failures += 1;
        } else {
            state.consecutive_fast_failures = 0;
            state.open_until = None;
        }
    }
    if state.consecutive_fast_failures >= backoff_limit && state.open_until.is_none() {
        state.open_until = Some(now + CIRCUIT_COOLDOWN);
    }
}

/// Whether the reconcile loop may create a replacement pod right now.
/// Half-open behavior: once the cooldown has elapsed, allow exactly one
/// attempt (tentatively clearing `open_until`) — if that pod also fails
/// fast, `update_circuit` reopens it on the next pass with the same
/// cooldown, rather than escalating; if it survives, `update_circuit`
/// resets the streak to 0 on its own.
fn circuit_allows_replacement(state: &mut CircuitState, now: Instant) -> bool {
    match state.open_until {
        None => true,
        Some(until) if now >= until => {
            state.open_until = None;
            true
        }
        Some(_) => false,
    }
}

/// Target replicas so that lag-per-replica trends toward
/// `target_lag_per_replica`, clamped to [min_replicas, max_replicas].
fn desired_replicas(total_lag: i64, current: u32, scaling: &ScalingConfig) -> u32 {
    if total_lag <= 0 {
        return scaling.min_replicas;
    }
    let target = scaling.target_lag_per_replica.max(1);
    let raw = (total_lag as f64 / target as f64).ceil() as i64;
    let raw = raw.max(scaling.min_replicas as i64).min(scaling.max_replicas as i64);
    let _ = current; // reserved for future smoothing (e.g. step-size limiting)
    raw as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scaling() -> ScalingConfig {
        ScalingConfig {
            min_replicas: 1,
            max_replicas: 10,
            target_lag_per_replica: 5_000,
            cooldown_secs: 60,
            backoff_limit: 3,
        }
    }

    #[test]
    fn scales_up_with_lag() {
        assert_eq!(desired_replicas(0, 1, &scaling()), 1);
        assert_eq!(desired_replicas(4_999, 1, &scaling()), 1);
        assert_eq!(desired_replicas(5_001, 1, &scaling()), 2);
        assert_eq!(desired_replicas(100_000, 1, &scaling()), 10); // clamped to max
    }

    #[test]
    fn scales_to_and_from_zero_when_min_replicas_is_zero() {
        let scale_to_zero = ScalingConfig {
            min_replicas: 0,
            ..scaling()
        };
        assert_eq!(desired_replicas(0, 1, &scale_to_zero), 0, "no lag, no reason to keep any replica running");
        assert_eq!(desired_replicas(0, 0, &scale_to_zero), 0, "already at zero, stays there");
        assert_eq!(desired_replicas(5_001, 0, &scale_to_zero), 2, "new lag wakes it back up from zero");
    }

    #[test]
    fn circuit_stays_closed_below_the_backoff_limit() {
        let mut state = CircuitState::default();
        let now = Instant::now();
        update_circuit(&mut state, &[FailedPod { fast: true }, FailedPod { fast: true }], now, 3);
        assert!(circuit_allows_replacement(&mut state, now));
    }

    #[test]
    fn circuit_opens_at_the_backoff_limit() {
        let mut state = CircuitState::default();
        let now = Instant::now();
        update_circuit(
            &mut state,
            &[FailedPod { fast: true }, FailedPod { fast: true }, FailedPod { fast: true }],
            now,
            3,
        );
        assert!(!circuit_allows_replacement(&mut state, now), "3 fast failures at backoff_limit=3 must open the circuit");
    }

    #[test]
    fn a_slow_failure_resets_the_streak() {
        let mut state = CircuitState::default();
        let now = Instant::now();
        update_circuit(&mut state, &[FailedPod { fast: true }, FailedPod { fast: true }], now, 3);
        update_circuit(&mut state, &[FailedPod { fast: false }], now, 3);
        assert_eq!(state.consecutive_fast_failures, 0);
        assert!(circuit_allows_replacement(&mut state, now));
    }

    #[test]
    fn circuit_allows_exactly_one_retry_after_cooldown_then_reopens_on_another_fast_failure() {
        let mut state = CircuitState::default();
        let t0 = Instant::now();
        update_circuit(&mut state, &[FailedPod { fast: true }, FailedPod { fast: true }, FailedPod { fast: true }], t0, 3);
        assert!(!circuit_allows_replacement(&mut state, t0), "still within cooldown");

        let after_cooldown = t0 + CIRCUIT_COOLDOWN + Duration::from_secs(1);
        assert!(circuit_allows_replacement(&mut state, after_cooldown), "cooldown elapsed, one retry allowed");

        // The retry pod fails fast too.
        update_circuit(&mut state, &[FailedPod { fast: true }], after_cooldown, 3);
        assert!(
            !circuit_allows_replacement(&mut state, after_cooldown),
            "a single fast failure right after the retry must reopen the circuit, not wait for 3 more"
        );
    }
}
