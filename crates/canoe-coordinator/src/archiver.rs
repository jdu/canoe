//! Runs one background archive loop per managed topic when
//! `topic_defaults.archive` (a DSL `topics { archive { ... } }` block) is
//! set: a dedicated consumer group buffers raw messages and periodically
//! flushes them to S3 as Parquet via `canoe_sink::TopicArchiver`. Entirely
//! separate from a chain's own step/sink consumers — its own consumer
//! group, its own buffer, its own flush timer — so a slow or failing
//! archive flush can never back-pressure the actual pipeline. This is
//! durability/debugging tooling, secondary to the pipeline it watches.

use canoe_config::{ArchiveConfig, ManagedTopic};
use canoe_kafka::{make_consumer, KafkaSettings};
use canoe_sink::TopicArchiver;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::Message;
use std::time::Duration;
use tracing::{error, info, warn};

/// Spawns one detached task per managed topic; each runs for the
/// coordinator's lifetime. Mirrors `scheduler::start`/`activation::start_*`:
/// fire-and-forget, the caller doesn't await these.
pub fn start(kafka_bootstrap: String, archive: ArchiveConfig, managed_topics: Vec<ManagedTopic>) {
    for mt in managed_topics {
        let bootstrap = kafka_bootstrap.clone();
        let archive = archive.clone();
        tokio::spawn(async move {
            run_archive_loop(bootstrap, archive, mt).await;
        });
    }
}

async fn run_archive_loop(kafka_bootstrap: String, archive: ArchiveConfig, mt: ManagedTopic) {
    let archiver = match TopicArchiver::new(&archive.base_path, &mt.pipeline, &mt.task) {
        Ok(a) => a,
        Err(e) => {
            error!(pipeline = %mt.pipeline, task = %mt.task, error = %e, "failed to start topic archiver");
            return;
        }
    };

    let group_id = format!("canoe-archive-{}-{}", mt.pipeline, mt.task);
    let consumer = match make_consumer(&KafkaSettings {
        bootstrap_servers: kafka_bootstrap,
        group_id,
    }) {
        Ok(c) => c,
        Err(e) => {
            error!(pipeline = %mt.pipeline, task = %mt.task, error = %e, "failed to start archive consumer");
            return;
        }
    };
    if let Err(e) = consumer.subscribe(&[mt.topic.as_str()]) {
        error!(pipeline = %mt.pipeline, task = %mt.task, topic = %mt.topic, error = %e, "failed to subscribe archive consumer");
        return;
    }

    info!(
        pipeline = %mt.pipeline,
        task = %mt.task,
        topic = %mt.topic,
        interval_secs = archive.interval_secs,
        "topic archiver started"
    );

    let mut buffer: Vec<serde_json::Value> = Vec::new();
    let mut last_flush = tokio::time::Instant::now();
    let flush_interval = Duration::from_secs(archive.interval_secs);

    loop {
        let poll = tokio::time::timeout(Duration::from_secs(1), consumer.recv()).await;
        match poll {
            Ok(Ok(msg)) => {
                if let Some(payload) = msg.payload() {
                    match serde_json::from_slice::<serde_json::Value>(payload) {
                        Ok(v) => buffer.push(v),
                        Err(e) => {
                            warn!(pipeline = %mt.pipeline, task = %mt.task, error = %e, "dropping unparseable message from archive buffer")
                        }
                    }
                }
                consumer.commit_message(&msg, CommitMode::Async).ok();
            }
            Ok(Err(e)) => error!(pipeline = %mt.pipeline, task = %mt.task, error = %e, "archive consumer receive error"),
            Err(_) => { /* poll timeout, fall through to flush check */ }
        }

        if last_flush.elapsed() >= flush_interval {
            if !buffer.is_empty() {
                let rows = std::mem::take(&mut buffer);
                let n = rows.len();
                if let Err(e) = archiver.write_batch(&rows).await {
                    error!(pipeline = %mt.pipeline, task = %mt.task, error = %e, rows = n, "archive flush failed, rows dropped");
                } else {
                    info!(pipeline = %mt.pipeline, task = %mt.task, rows = n, "archived batch");
                }
            }
            last_flush = tokio::time::Instant::now();
        }
    }
}
