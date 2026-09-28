//! Reads a sink step's producer's `run_id=*` history (S3 + signal
//! transport — see README.md's "S3 + signal-driven pipeline transport"),
//! buffers rows, and flushes to the configured datalake sink on whichever
//! comes first: `batch_size` rows or `flush_interval_secs` elapsed.
//!
//! Wakes up fast on the coordinator's internal signals topic (filtered to
//! `step.input_signal`), but never *trusts* the signal alone: every poll
//! timeout also re-lists what's actually new under `step.input_path`
//! against this sink's own reader checkpoint — the signal is a latency
//! optimization, self-heal listing is what guarantees a run is never
//! permanently missed.

use crate::status::{StatusRegistry, StepStatus};
use bytes::Bytes;
use canoe_config::{SinkConfig, SinkStep};
use canoe_kafka::{make_consumer, make_producer, KafkaSettings};
use canoe_sink::{list_run_ids, read_rows_under_prefix, Checkpointer, ScdView};
use chrono::Utc;
use object_store::path::Path as StorePath;
use object_store::{parse_url, ObjectStore};
use rdkafka::consumer::{CommitMode, Consumer};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};
use url::Url;

pub async fn run(
    step: SinkStep,
    chain_name: String,
    pipeline_name: String,
    bootstrap_servers: String,
    signals_topic: String,
    status: Arc<StatusRegistry>,
) -> anyhow::Result<()> {
    let (batch_size, flush_interval_secs) = match &step.sink {
        SinkConfig::Parquet {
            batch_size,
            flush_interval_secs,
            ..
        } => (*batch_size, *flush_interval_secs),
        _ => (1_000, 30),
    };

    let group_id = format!("canoe-sink-{chain_name}-{}", step.name);
    let consumer = make_consumer(&KafkaSettings {
        bootstrap_servers: bootstrap_servers.clone(),
        group_id,
    })?;
    consumer.subscribe(&[signals_topic.as_str()])?;

    let signal_producer = make_producer(&KafkaSettings {
        bootstrap_servers,
        group_id: String::new(), // producers ignore group_id
    })?;

    let sink = canoe_sink::build_sink(&step.sink, &step.encrypt, &step.column_metadata)?;
    info!(step = %step.name, sink = sink.name(), "sink runner started");

    // Where this sink tracks how far it's read into its *producer's* own
    // run history — private to this reader, distinct from
    // `S3BacklogInspector`'s autoscaling-only checkpoint read (a Python/Uv
    // step's own `{output_path}/.checkpoint.json`) and from `checkpointer`
    // below (which compacts *this sink's own* run history, not its
    // producer's).
    let reader_checkpoint_uri = format!("{}/.source_checkpoint.json", sink_dataset_uri(&step)?.trim_end_matches('/'));
    let mut through_run_id = match read_checkpoint(&reader_checkpoint_uri).await {
        Ok(v) => v,
        Err(e) => {
            warn!(step = %step.name, error = %e, "failed to read sink's source checkpoint, starting from the beginning");
            None
        }
    };

    // Built once, reused for every checkpoint pass this sink ever
    // triggers — `None` if this sink has no `checkpoint` block (the
    // common case) or its format isn't `parquet` (checkpointing only
    // makes sense against ParquetSink's run_id=* layout; canoe-dsl
    // already rejects the combination for any other format at compile
    // time, but a hand-written YAML config could still reach here).
    let checkpointer = build_checkpointer(&step);
    // Guards against two checkpoint passes for this sink overlapping —
    // if a pass is still running when the next flush completes, that
    // flush's new run just gets picked up by the *next* pass instead of
    // triggering a concurrent one (see checkpoint_after_flush's docs).
    let checkpoint_in_progress = Arc::new(AtomicBool::new(false));

    // Same shape as the checkpointer, entirely independent of it — a
    // sink can have both a `checkpoint` and an `scd` block at once, each
    // reading `run_id=*` and writing its own `checkpoint=*`/`scd=*`
    // prefix without interfering with the other.
    let scd_view = build_scd_view(&step);
    let scd_in_progress = Arc::new(AtomicBool::new(false));

    let mut buffer: Vec<serde_json::Value> = Vec::with_capacity(batch_size);
    let mut last_flush = tokio::time::Instant::now();
    let flush_interval = Duration::from_secs(flush_interval_secs);
    // Self-heal cadence: even with zero signal traffic, this sink still
    // notices new runs on this schedule.
    let poll_timeout = Duration::from_secs(5);

    loop {
        match tokio::time::timeout(poll_timeout, consumer.recv()).await {
            Ok(Ok(msg)) => {
                consumer.commit_message(&msg, CommitMode::Async).ok();
                // Only used to wake up promptly — which signal (if any)
                // fired doesn't change what happens next: either way we
                // go list what's actually new under input_path below.
            }
            Ok(Err(e)) => error!(step = %step.name, error = %e, "signal listener receive error"),
            Err(_) => { /* poll timeout, self-heal check anyway */ }
        }

        match fetch_new_rows(&step.input_path, through_run_id.as_deref()).await {
            Ok((rows, latest)) => {
                if !rows.is_empty() {
                    buffer.extend(rows);
                }
                through_run_id = latest.or(through_run_id);
            }
            Err(e) => warn!(step = %step.name, error = %e, "failed to list new runs from producer, will retry"),
        }

        let due = buffer.len() >= batch_size || (!buffer.is_empty() && last_flush.elapsed() >= flush_interval);
        if due {
            let rows = std::mem::take(&mut buffer);
            let n = rows.len();
            if let Err(e) = sink.write_batch(&rows).await {
                error!(step = %step.name, error = %e, rows = n, "sink flush failed, rows dropped");
                status.update_step(StepStatus {
                    pipeline: pipeline_name.clone(),
                    chain: chain_name.clone(),
                    name: step.name.clone(),
                    kind: "sink".to_string(),
                    desired_replicas: None,
                    alive_replicas: None,
                    circuit_open: false,
                    backlog: None,
                    lag: None,
                    last_reconciled: Utc::now(),
                    last_error: Some(e.to_string()),
                    paused: false,
                });
            } else {
                info!(step = %step.name, rows = n, "flushed batch to sink");
                if let Some(through) = &through_run_id {
                    if let Err(e) = write_checkpoint(&reader_checkpoint_uri, through).await {
                        warn!(step = %step.name, error = %e, "failed to persist sink's source checkpoint");
                    }
                }
                // The existing opt-in, user-named cross-pipeline trigger.
                if let Some(signal) = &step.signal {
                    crate::signals::publish(&signal_producer, &signals_topic, signal, &chain_name, &step.name).await;
                }
                // The new automatic, always-on wake-up — keyed by this
                // sink's own name, same as every other S3-writing node.
                crate::signals::publish_run(
                    &signal_producer,
                    &signals_topic,
                    &step.input_signal,
                    &chain_name,
                    &step.name,
                    None,
                    through_run_id.as_deref(),
                    None,
                    Some(n as u64),
                )
                .await;
                if let Some(checkpointer) = &checkpointer {
                    checkpoint_after_flush(checkpointer.clone(), checkpoint_in_progress.clone(), step.name.clone());
                }
                if let Some(scd_view) = &scd_view {
                    scd_after_flush(scd_view.clone(), scd_in_progress.clone(), step.name.clone());
                }
                status.update_step(StepStatus {
                    pipeline: pipeline_name.clone(),
                    chain: chain_name.clone(),
                    name: step.name.clone(),
                    kind: "sink".to_string(),
                    desired_replicas: None,
                    alive_replicas: None,
                    circuit_open: false,
                    backlog: None,
                    lag: None,
                    last_reconciled: Utc::now(),
                    last_error: None,
                    paused: false,
                });
            }
            last_flush = tokio::time::Instant::now();
        }
    }
}

fn split_uri(uri: &str) -> anyhow::Result<(Box<dyn ObjectStore>, StorePath)> {
    let url = Url::parse(uri).map_err(|e| anyhow::anyhow!("invalid object store URI '{uri}': {e}"))?;
    Ok(parse_url(&url)?)
}

/// The sink's own dataset root — `<base_path>/dataset_id=<id>/` — used
/// only to place this reader's own source checkpoint alongside the data
/// it produces (never mixed into the producer's own directory).
fn sink_dataset_uri(step: &SinkStep) -> anyhow::Result<String> {
    match &step.sink {
        SinkConfig::Parquet { base_path, dataset_id, .. } => {
            Ok(format!("{}/dataset_id={}", base_path.trim_end_matches('/'), dataset_id))
        }
        other => anyhow::bail!("sink \"{}\": checkpointed reading only supports format=parquet, got {other:?}", step.name),
    }
}

async fn read_checkpoint(uri: &str) -> anyhow::Result<Option<String>> {
    let (store, path) = split_uri(uri)?;
    match store.get(&path).await {
        Ok(result) => {
            let bytes = result.bytes().await?;
            let v: serde_json::Value = serde_json::from_slice(&bytes)?;
            Ok(v.get("through_run_id").and_then(|s| s.as_str()).map(str::to_string))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

async fn write_checkpoint(uri: &str, through_run_id: &str) -> anyhow::Result<()> {
    let (store, path) = split_uri(uri)?;
    let payload = serde_json::json!({ "through_run_id": through_run_id });
    store.put(&path, Bytes::from(serde_json::to_vec(&payload)?).into()).await?;
    Ok(())
}

/// Lists and reads every `run_id=*` under `input_path` newer than
/// `through`, oldest to newest, folding all their rows together — mirrors
/// `crate::checkpoint::Checkpointer`'s own listing algorithm. Returns the
/// newest run_id actually seen (`None` if nothing was new) so the caller
/// can advance its checkpoint.
async fn fetch_new_rows(input_path: &str, through: Option<&str>) -> anyhow::Result<(Vec<serde_json::Value>, Option<String>)> {
    let (store, dataset_path) = split_uri(input_path)?;
    let run_ids = list_run_ids(store.as_ref(), &dataset_path).await?;
    let new_ids: Vec<&String> = match through {
        Some(t) => run_ids.iter().filter(|id| id.as_str() > t).collect(),
        None => run_ids.iter().collect(),
    };

    let mut rows = Vec::new();
    let mut latest = None;
    for id in new_ids {
        let run_path = dataset_path.child(format!("run_id={id}").as_str());
        rows.extend(read_rows_under_prefix(store.as_ref(), &run_path).await?);
        latest = Some(id.clone());
    }
    Ok((rows, latest))
}

fn build_checkpointer(step: &SinkStep) -> Option<Arc<Checkpointer>> {
    let checkpoint = step.checkpoint.as_ref()?;
    let SinkConfig::Parquet {
        base_path,
        dataset_id,
        partition_by,
        ..
    } = &step.sink
    else {
        warn!(step = %step.name, "checkpoint block set on a non-parquet sink, ignoring (canoe-dsl should have rejected this)");
        return None;
    };
    match Checkpointer::new(base_path, dataset_id, checkpoint.keys.clone(), partition_by.clone()) {
        Ok(c) => Some(Arc::new(c)),
        Err(e) => {
            error!(step = %step.name, error = %e, "failed to construct checkpointer, checkpointing disabled for this sink");
            None
        }
    }
}

/// Spawns one checkpoint pass, non-blocking — the sink's own flush loop
/// moves on to the next message immediately rather than waiting on a
/// pass that reads back potentially many run directories. Skips
/// spawning entirely (rather than queuing) if a pass for this sink is
/// already running: that in-flight pass hasn't necessarily seen the run
/// that just landed yet, but the *next* successful flush will try again,
/// so a run is never permanently missed, just picked up one flush later
/// than the absolute earliest possible.
fn checkpoint_after_flush(checkpointer: Arc<Checkpointer>, in_progress: Arc<AtomicBool>, step_name: String) {
    if in_progress.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).is_err() {
        info!(step = %step_name, "checkpoint pass already running, this run will be picked up next time");
        return;
    }
    tokio::spawn(async move {
        match checkpointer.run().await {
            Ok(Some(result)) => info!(
                step = %step_name,
                checkpoint_id = %result.checkpoint_id,
                through_run_id = %result.through_run_id,
                row_count = result.row_count,
                "checkpoint pass completed"
            ),
            Ok(None) => { /* no new runs since the last checkpoint — nothing to log */ }
            Err(e) => error!(step = %step_name, error = %e, "checkpoint pass failed"),
        }
        in_progress.store(false, Ordering::Release);
    });
}

fn build_scd_view(step: &SinkStep) -> Option<Arc<ScdView>> {
    let scd = step.scd.as_ref()?;
    let SinkConfig::Parquet {
        base_path,
        dataset_id,
        partition_by,
        ..
    } = &step.sink
    else {
        warn!(step = %step.name, "scd block set on a non-parquet sink, ignoring (canoe-dsl should have rejected this)");
        return None;
    };
    match ScdView::new(
        base_path,
        dataset_id,
        scd.keys.clone(),
        scd.track_fields.clone(),
        scd.effective_field.clone(),
        partition_by.clone(),
    ) {
        Ok(v) => Some(Arc::new(v)),
        Err(e) => {
            error!(step = %step.name, error = %e, "failed to construct scd view, scd rollup disabled for this sink");
            None
        }
    }
}

/// Same non-blocking spawn-with-a-guard shape as `checkpoint_after_flush`
/// — see its docs.
fn scd_after_flush(scd_view: Arc<ScdView>, in_progress: Arc<AtomicBool>, step_name: String) {
    if in_progress.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).is_err() {
        info!(step = %step_name, "scd pass already running, this run will be picked up next time");
        return;
    }
    tokio::spawn(async move {
        match scd_view.run().await {
            Ok(Some(result)) => info!(
                step = %step_name,
                scd_id = %result.scd_id,
                through_run_id = %result.through_run_id,
                row_count = result.row_count,
                current_row_count = result.current_row_count,
                "scd pass completed"
            ),
            Ok(None) => { /* no new runs since the last scd pass — nothing to log */ }
            Err(e) => error!(step = %step_name, error = %e, "scd pass failed"),
        }
        in_progress.store(false, Ordering::Release);
    });
}

