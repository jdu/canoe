//! Applies `topic_defaults.retention_ms` (a DSL `topics { retention {
//! duration = ... } }` block) to every managed topic at coordinator
//! startup — see `canoe_kafka::ensure_topic_retention`. Runs once,
//! synchronously, before any chain starts consuming/producing, so a
//! topic's durability guarantee is in place from the first message
//! rather than raced against whichever comes first.

use canoe_config::ManagedTopic;
use tracing::{error, info};

pub async fn apply(kafka_bootstrap: &str, retention_ms: i64, managed_topics: &[ManagedTopic]) {
    for mt in managed_topics {
        match canoe_kafka::ensure_topic_retention(kafka_bootstrap, &mt.topic, retention_ms).await {
            Ok(()) => info!(
                pipeline = %mt.pipeline,
                task = %mt.task,
                topic = %mt.topic,
                retention_ms,
                "topic retention ensured"
            ),
            Err(e) => error!(
                pipeline = %mt.pipeline,
                task = %mt.task,
                topic = %mt.topic,
                error = %e,
                "failed to ensure topic retention"
            ),
        }
    }
}
