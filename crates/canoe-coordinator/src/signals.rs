//! The coordinator's internal signal bus: a plain Kafka topic
//! (`_canoe.signals` by default) that sink steps publish to on a
//! successful flush (see `sink_runner::run`) and that triggered chains
//! (`ChainConfig.trigger.signal` — compiled from a DSL `schedule { signal
//! = ... }` block) wait on before starting.
//!
//! A signal only ever activates each pending step once: after the first
//! match it's removed from the pending set, so re-firing the same signal
//! later (another batch lands, say) doesn't respawn already-running
//! consumers/launchers. "Started" is a one-way transition, not a re-run
//! trigger — see the DSL/README docs for the scale-back-to-zero follow-up
//! this doesn't attempt yet.

use crate::activation::{Ctx, PendingActivation};
use canoe_kafka::{make_consumer, KafkaSettings};
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;
use tracing::{error, info, warn};

/// Two conceptually distinct uses share this one wire event: (1) the
/// existing opt-in, user-named cross-pipeline trigger (`TriggerConfig.signal`
/// / `SinkStep.signal`) — `signal` is whatever name the author chose, and
/// `path`/`run_id`/`key`/`row_count` are `None`/empty since a trigger fire
/// doesn't describe any one write; (2) the new automatic, always-on
/// wake-up every S3-writing node (`PythonStep`/`UvStep`/`SinkStep`) fires
/// after each flush, keyed by its own name — here `path`/`run_id` always
/// describe exactly the run that was just written, which is what a
/// consumer's self-heal listing needs to know where to look.
#[derive(Serialize)]
struct SignalEvent<'a> {
    signal: &'a str,
    chain: &'a str,
    sink: &'a str,
    emitted_at: chrono::DateTime<chrono::Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    run_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    key: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    row_count: Option<u64>,
}

/// Publishes one signal event after a sink's successful flush (the
/// existing opt-in, user-named trigger — see `SignalEvent`'s docs). Best
/// effort: a failed publish is logged, not propagated — losing a signal
/// shouldn't take down the sink runner that just committed a write.
pub async fn publish(producer: &FutureProducer, topic: &str, signal: &str, chain: &str, sink: &str) {
    publish_run(producer, topic, signal, chain, sink, None, None, None, None).await;
}

/// Publishes an automatic, always-on wake-up signal after an S3-transport
/// node's flush — `signal` is the producer's own name (what a consumer's
/// `input_signal` filters on), `path`/`run_id` describe the run just
/// written. Also used by `publish` above for the named-trigger case (with
/// every S3-specific field `None`).
#[allow(clippy::too_many_arguments)]
pub async fn publish_run(
    producer: &FutureProducer,
    topic: &str,
    signal: &str,
    chain: &str,
    sink: &str,
    path: Option<&str>,
    run_id: Option<&str>,
    key: Option<&str>,
    row_count: Option<u64>,
) {
    let event = SignalEvent {
        signal,
        chain,
        sink,
        emitted_at: chrono::Utc::now(),
        path,
        run_id,
        key,
        row_count,
    };
    let payload = match serde_json::to_vec(&event) {
        Ok(p) => p,
        Err(e) => {
            error!(signal, error = %e, "failed to encode signal event");
            return;
        }
    };
    let record = FutureRecord::to(topic).payload(&payload).key(signal);
    if let Err((e, _)) = producer.send(record, Duration::from_secs(5)).await {
        error!(signal, chain, sink, error = %e, "failed to publish signal");
    } else {
        info!(signal, chain, sink, "published signal");
    }
}

/// Consumes the signals topic forever, activating every step parked on a
/// signal the first time it's seen. Runs as a coordinator singleton (a
/// fixed consumer group), same as the rest of the coordinator's control
/// loop — never scaled, unlike the step workers it starts.
///
/// Reads/drains `ctx.pending` (shared, live-mutable — see `Ctx.pending`'s
/// docs) on each incoming signal, rather than owning a local snapshot
/// handed in once at startup: a chain pushed by a project *after* this
/// loop already started (via `POST /projects/:name`, `projects::apply_project`
/// inserting straight into `ctx.pending`) is picked up by this exact same
/// already-running consumer, not a separate listener. Unlike the old
/// local-map version, this no longer idles forever just because nothing
/// is pending *yet* at startup — a later push can always add entries, so
/// the consumer always subscribes and runs the real loop.
///
/// Never returns under normal operation — `main` awaits this to keep the
/// process (and every detached task `activation::start_*` spawned) alive.
/// That holds even with no triggered chains at all: this still blocks
/// forever rather than let a Kafka client error tear down the coordinator
/// out from under its already-running steps.
pub async fn listen(ctx: Ctx) {
    info!("signal listener starting (pending set is shared and live — grows as projects are pushed)");

    let consumer = match make_consumer(&KafkaSettings {
        bootstrap_servers: ctx.kafka_bootstrap.clone(),
        group_id: "canoe-signal-listener".to_string(),
    }) {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "failed to start signal listener consumer; triggered chains will never start");
            std::future::pending::<()>().await;
            return;
        }
    };
    if let Err(e) = consumer.subscribe(&[ctx.signals_topic.as_str()]) {
        error!(error = %e, "failed to subscribe to signals topic; triggered chains will never start");
        std::future::pending::<()>().await;
        return;
    }

    loop {
        let msg = match consumer.recv().await {
            Ok(m) => m,
            Err(e) => {
                error!(error = %e, "signal listener receive error");
                continue;
            }
        };
        consumer.commit_message(&msg, CommitMode::Async).ok();

        let Some(payload) = msg.payload() else { continue };
        let signal = match serde_json::from_slice::<serde_json::Value>(payload) {
            Ok(v) => v.get("signal").and_then(|s| s.as_str()).map(str::to_string),
            Err(e) => {
                warn!(error = %e, "dropping unparseable signal event");
                None
            }
        };
        let Some(signal) = signal else { continue };

        let Some(waiting) = drain_pending(&ctx.pending, &signal) else {
            continue; // no chain is waiting on this signal (or it already fired once)
        };
        info!(signal = %signal, steps = waiting.len(), "signal fired, activating pending steps");
        for activation in waiting {
            let (chain, step) = activation.describe();
            info!(signal = %signal, chain, step, "activating");
            activation.activate(&ctx);
        }
    }
}

/// Removes and returns whatever's parked on `signal` in the shared
/// pending map — pulled out as its own tiny function specifically so the
/// "shared, live-mutable, not a one-time snapshot" behavior is
/// unit-testable without a real Kafka broker (the consumer loop itself
/// needs one; this doesn't).
fn drain_pending(
    pending: &Mutex<HashMap<String, Vec<PendingActivation>>>,
    signal: &str,
) -> Option<Vec<PendingActivation>> {
    pending.lock().unwrap().remove(signal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use canoe_config::{CodeSource, OutputConfig, PodResources, PythonStep, ScalingConfig};
    use std::collections::BTreeMap;

    fn pending_python(chain_name: &str) -> PendingActivation {
        PendingActivation::Python {
            chain_name: chain_name.to_string(),
            pipeline_name: "orders".to_string(),
            step: PythonStep {
                name: "validate".to_string(),
                input: "orders-adapter".to_string(),
                input_path: "s3://bucket/orders/v1/orders-adapter".to_string(),
                input_signal: "orders-adapter".to_string(),
                output_path: "s3://bucket/orders/v1/validate".to_string(),
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
            },
        }
    }

    /// Simulates the exact scenario the shared-map change exists for: a
    /// project pushed (inserting into `pending`) *after* the listener
    /// loop has conceptually already started — the same `Arc<Mutex<...>>`
    /// must still see and drain that later insert, not just whatever was
    /// present at construction time.
    #[test]
    fn a_signal_pushed_after_the_map_was_created_is_still_drainable() {
        let pending: Mutex<HashMap<String, Vec<PendingActivation>>> = Mutex::new(HashMap::new());

        // "startup": project "default" pushes one chain waiting on "a".
        pending.lock().unwrap().insert("a".to_string(), vec![pending_python("default--orders-0")]);

        // The listener "loop" drains "a" — gone afterwards.
        assert!(drain_pending(&pending, "a").is_some());
        assert!(drain_pending(&pending, "a").is_none(), "a signal only ever activates its pending chains once");

        // A *second* project is pushed later, inserting into the very
        // same map the (still-running, in the real system) listener
        // reads from — must be drainable exactly like the first.
        pending.lock().unwrap().insert("b".to_string(), vec![pending_python("my-pipeline--orders-0")]);
        let drained = drain_pending(&pending, "b");
        assert!(drained.is_some());
        assert_eq!(drained.unwrap()[0].describe().0, "my-pipeline--orders-0");
    }

    #[test]
    fn draining_an_unknown_signal_is_a_no_op() {
        let pending: Mutex<HashMap<String, Vec<PendingActivation>>> = Mutex::new(HashMap::new());
        assert!(drain_pending(&pending, "nothing-waiting-on-this").is_none());
    }
}
