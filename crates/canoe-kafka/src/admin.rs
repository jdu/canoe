//! Applies retention config to every topic canoe manages internally
//! (a chain's step-to-step wiring topics — see `ManagedTopic` in
//! canoe-config). Called once per topic at coordinator startup so a
//! chain's durability guarantees are declared in the same HCL that
//! defines the chain, rather than provisioned out-of-band.

use anyhow::{bail, Context};
use rdkafka::admin::{AdminClient, AdminOptions, AlterConfig, NewTopic, ResourceSpecifier, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::error::RDKafkaErrorCode;
use std::time::Duration;

/// Default partition/replication for a topic canoe creates itself. Not
/// configurable from the DSL today — a documented simplification; a
/// deployment that needs different topology should pre-create the topic
/// (with whatever partitions/replication it wants) before the
/// coordinator starts, in which case this only touches `retention.ms`.
const DEFAULT_PARTITIONS: i32 = 6;
const DEFAULT_REPLICATION: i32 = 3;

/// Ensures `topic` exists with `retention.ms` set to `retention_ms`.
/// If the topic doesn't exist yet, creates it with that retention baked
/// in from the start; if it already exists (very likely, since producers
/// commonly auto-create topics on first send), leaves partitions/
/// replication alone and applies retention via a config alter instead.
pub async fn ensure_topic_retention(bootstrap_servers: &str, topic: &str, retention_ms: i64) -> anyhow::Result<()> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()
        .context("failed to build admin client")?;
    let opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(10)));
    let retention_str = retention_ms.to_string();

    let new_topic = NewTopic::new(topic, DEFAULT_PARTITIONS, TopicReplication::Fixed(DEFAULT_REPLICATION))
        .set("retention.ms", &retention_str);
    let create_results = admin
        .create_topics(&[new_topic], &opts)
        .await
        .with_context(|| format!("failed to create topic '{topic}'"))?;

    let already_existed = match create_results.into_iter().next() {
        Some(Ok(_)) => false,
        Some(Err((_, RDKafkaErrorCode::TopicAlreadyExists))) => true,
        Some(Err((name, code))) => bail!("failed to create topic '{name}': {code:?}"),
        None => bail!("create_topics returned no result for '{topic}'"),
    };

    if already_existed {
        let alter = AlterConfig::new(ResourceSpecifier::Topic(topic)).set("retention.ms", &retention_str);
        let alter_results = admin
            .alter_configs(&[alter], &opts)
            .await
            .with_context(|| format!("failed to alter retention.ms on existing topic '{topic}'"))?;
        if let Some(Err((_, code))) = alter_results.into_iter().next() {
            bail!("failed to set retention.ms on topic '{topic}': {code:?}");
        }
    }

    Ok(())
}
