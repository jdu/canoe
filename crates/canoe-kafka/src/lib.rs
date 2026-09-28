//! Thin Kafka wrappers used by the coordinator: a message consumer for
//! chain trigger/sink topics, a producer for routing, and lag inspection
//! used to drive worker autoscaling.

mod admin;
mod lag;

pub use admin::ensure_topic_retention;
pub use lag::{ConsumerLag, LagInspector};

use anyhow::Context;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct KafkaSettings {
    pub bootstrap_servers: String,
    pub group_id: String,
}

pub fn make_consumer(settings: &KafkaSettings) -> anyhow::Result<StreamConsumer> {
    ClientConfig::new()
        .set("bootstrap.servers", &settings.bootstrap_servers)
        .set("group.id", &settings.group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .context("failed to build kafka consumer")
}

pub fn make_producer(settings: &KafkaSettings) -> anyhow::Result<FutureProducer> {
    ClientConfig::new()
        .set("bootstrap.servers", &settings.bootstrap_servers)
        .set("message.timeout.ms", "30000")
        .create()
        .context("failed to build kafka producer")
}

/// Read one message and, on success, produce it unchanged to `topic` — used
/// by the coordinator to route between step topics without touching payload
/// bytes when a step's worker pods handle transform + republish themselves,
/// and by tests validating connectivity.
pub async fn forward_once(
    consumer: &StreamConsumer,
    producer: &FutureProducer,
    topic: &str,
    timeout: Duration,
) -> anyhow::Result<bool> {
    let msg = match tokio::time::timeout(timeout, consumer.recv()).await {
        Ok(Ok(m)) => m,
        Ok(Err(e)) => return Err(e).context("kafka receive error"),
        Err(_) => return Ok(false), // timed out, nothing to forward
    };
    let payload = msg.payload().unwrap_or_default();
    let key = msg.key().unwrap_or_default();
    producer
        .send(
            FutureRecord::to(topic).payload(payload).key(key),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)
        .context("kafka produce error")?;
    consumer.commit_message(&msg, rdkafka::consumer::CommitMode::Async)?;
    Ok(true)
}
