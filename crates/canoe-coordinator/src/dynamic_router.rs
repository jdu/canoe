//! Airflow-`.expand()`-style dynamic fan-out — see `canoe_config::DynamicRouteStep`
//! for the full contract. Unlike a static `route` (fully expanded into N
//! filter-only steps at DSL-compile time), a `dynamic_route`'s branches
//! don't exist until this loop discovers them: it watches `input_topic`,
//! and the first time it sees a new value of `field` it provisions a
//! dedicated topic + Deployment (`task`, from `activation::start_*`) for
//! that value alone, capped at `max_branches` — past that, new values
//! fall back to `overflow` (provisioned once, up front) or are dropped.

use crate::activation::{self, Ctx};
use canoe_config::{
    DynamicRouteStep, DynamicTaskTemplate, KafkaPythonStep, KafkaSparkDataFrameStep, KafkaSparkSqlStep,
    PipelineConfig, StepConfig,
};
use canoe_kafka::{make_consumer, make_producer, KafkaSettings};
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::producer::FutureRecord;
use rdkafka::Message;
use std::collections::HashSet;
use std::time::Duration;
use tracing::{error, info, warn};

/// Spawns one detached task per distinct `dynamic_route` (deduplicated —
/// the same route can appear in multiple `ChainConfig`s, same as any
/// step reachable from more than one branch). Mirrors
/// `scheduler::start`/`archiver::start`: fire-and-forget.
pub fn start(ctx: Ctx, config: &PipelineConfig) {
    let mut started: HashSet<String> = HashSet::new();
    for chain in &config.chains {
        for step in &chain.steps {
            if let StepConfig::DynamicRoute(dr) = step {
                if started.insert(dr.output_topic.clone()) {
                    let ctx = ctx.clone();
                    let dr = dr.clone();
                    tokio::spawn(async move {
                        run(ctx, dr).await;
                    });
                }
            }
        }
    }
}

async fn run(ctx: Ctx, dr: DynamicRouteStep) {
    // A task's `depends_on` can name a dynamic_route directly (see
    // `crates/canoe-coordinator/src/tasks.rs`) — satisfied as soon as
    // this discovery loop actually starts, same "applied, not ready"
    // semantics `activation::mark_activated` uses for regular steps
    // (there's no equivalent of "every branch discovered" to wait for,
    // since branches are discovered indefinitely for as long as this
    // loop runs).
    ctx.activated.lock().unwrap().insert(dr.name.clone());

    // dr.output_topic is always "_canoe.<name>.merged" (see
    // compiler::default_dynamic_route_topic) — reused as the namespace
    // for everything this route provisions, so no separate name field is
    // needed here.
    let base_topic = dr.output_topic.trim_end_matches(".merged").to_string();
    let route_key = base_topic.trim_start_matches("_canoe.").replace('.', "-");
    let chain_name = format!("dyn-{route_key}");

    if let Some(overflow) = &dr.overflow {
        let overflow_topic = format!("{base_topic}.overflow");
        info!(route = %dr.name, topic = %overflow_topic, "provisioning dynamic_route overflow task");
        start_task(&ctx, overflow, chain_name.clone(), "overflow".to_string(), overflow_topic, dr.output_topic.clone());
    }

    let consumer = match make_consumer(&KafkaSettings {
        bootstrap_servers: ctx.kafka_bootstrap.clone(),
        group_id: format!("canoe-dynroute-{route_key}"),
    }) {
        Ok(c) => c,
        Err(e) => {
            error!(route = %dr.name, error = %e, "failed to start dynamic_route consumer");
            return;
        }
    };
    let producer = match make_producer(&KafkaSettings {
        bootstrap_servers: ctx.kafka_bootstrap.clone(),
        group_id: String::new(),
    }) {
        Ok(p) => p,
        Err(e) => {
            error!(route = %dr.name, error = %e, "failed to start dynamic_route producer");
            return;
        }
    };
    if let Err(e) = consumer.subscribe(&[dr.input_topic.as_str()]) {
        error!(route = %dr.name, topic = %dr.input_topic, error = %e, "failed to subscribe dynamic_route consumer");
        return;
    }

    info!(
        route = %dr.name,
        input_topic = %dr.input_topic,
        field = %dr.field,
        max_branches = dr.max_branches,
        "dynamic_route started"
    );

    let mut provisioned: HashSet<String> = HashSet::new();

    loop {
        let msg = match consumer.recv().await {
            Ok(m) => m,
            Err(e) => {
                error!(route = %dr.name, error = %e, "dynamic_route receive error");
                continue;
            }
        };

        let Some(payload) = msg.payload() else {
            consumer.commit_message(&msg, CommitMode::Async).ok();
            continue;
        };

        let value = match serde_json::from_slice::<serde_json::Value>(payload) {
            Ok(record) => field_value_as_string(&record, &dr.field),
            Err(e) => {
                warn!(route = %dr.name, error = %e, "dropping unparseable message");
                consumer.commit_message(&msg, CommitMode::Async).ok();
                continue;
            }
        };

        let target_topic = match value {
            Some(v) => {
                let slug = sanitize_topic_segment(&v);
                if !provisioned.contains(&slug) {
                    if provisioned.len() as u32 >= dr.max_branches {
                        warn!(route = %dr.name, value = %v, max_branches = dr.max_branches, "max_branches reached, routing to overflow");
                        overflow_topic_if_configured(&dr, &base_topic)
                    } else {
                        let task_topic = format!("{base_topic}.value.{slug}");
                        info!(route = %dr.name, value = %v, slug = %slug, "provisioning dynamic_route branch");
                        start_task(&ctx, &dr.task, chain_name.clone(), slug.clone(), task_topic.clone(), dr.output_topic.clone());
                        provisioned.insert(slug);
                        Some(task_topic)
                    }
                } else {
                    Some(format!("{base_topic}.value.{slug}"))
                }
            }
            None => {
                warn!(route = %dr.name, "message missing/null split field, routing to overflow");
                overflow_topic_if_configured(&dr, &base_topic)
            }
        };

        if let Some(topic) = target_topic {
            let key = msg.key().unwrap_or_default();
            let record = FutureRecord::to(&topic).payload(payload).key(key);
            if let Err((e, _)) = producer.send(record, Duration::from_secs(5)).await {
                error!(route = %dr.name, topic = %topic, error = %e, "failed to route message");
            }
        } else {
            warn!(route = %dr.name, "no overflow configured, dropping message");
        }

        consumer.commit_message(&msg, CommitMode::Async).ok();
    }
}

fn overflow_topic_if_configured(dr: &DynamicRouteStep, base_topic: &str) -> Option<String> {
    dr.overflow.as_ref().map(|_| format!("{base_topic}.overflow"))
}

fn field_value_as_string(record: &serde_json::Value, field: &str) -> Option<String> {
    match record.get(field) {
        Some(serde_json::Value::String(s)) => Some(s.clone()),
        Some(serde_json::Value::Null) | None => None,
        Some(other) => Some(other.to_string()),
    }
}

/// Kafka topic names are limited to 249 chars and (by strong convention)
/// `[a-zA-Z0-9._-]`; a discovered value could be anything (unicode,
/// slashes, megabytes of text from a malformed field). Lowercase
/// alphanumerics/`-`/`_` pass through, everything else becomes `_`; long
/// values get truncated with a hash suffix so two different long values
/// that share a 60-char prefix still land in different branches.
fn sanitize_topic_segment(value: &str) -> String {
    let cleaned: String = value
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c.to_ascii_lowercase() } else { '_' })
        .collect();
    if cleaned.is_empty() {
        return "_empty".to_string();
    }
    if cleaned.len() <= 60 {
        return cleaned;
    }
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    format!("{}-{:x}", &cleaned[..60], hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn sanitize_lowercases_and_replaces_unsafe_chars() {
        assert_eq!(sanitize_topic_segment("US"), "us");
        assert_eq!(sanitize_topic_segment("North America"), "north_america");
        assert_eq!(sanitize_topic_segment("a/b/c"), "a_b_c");
        assert_eq!(sanitize_topic_segment("already-safe_123"), "already-safe_123");
    }

    #[test]
    fn sanitize_handles_empty_and_unicode() {
        assert_eq!(sanitize_topic_segment(""), "_empty");
        assert_eq!(sanitize_topic_segment("日本"), "__"); // two chars, both replaced
    }

    #[test]
    fn sanitize_truncates_long_values_with_a_hash_suffix() {
        let long = "x".repeat(200);
        let slug = sanitize_topic_segment(&long);
        assert!(slug.len() < long.len());
        assert!(slug.starts_with(&"x".repeat(60)));

        // Two long values sharing a 60-char prefix must still diverge.
        let long2 = format!("{}{}", "x".repeat(60), "y".repeat(200));
        let slug2 = sanitize_topic_segment(&long2);
        assert_ne!(slug, slug2);
    }

    #[test]
    fn field_value_as_string_coerces_json_types() {
        assert_eq!(field_value_as_string(&json!({"region": "US"}), "region"), Some("US".to_string()));
        assert_eq!(field_value_as_string(&json!({"region": 42}), "region"), Some("42".to_string()));
        assert_eq!(field_value_as_string(&json!({"region": true}), "region"), Some("true".to_string()));
        assert_eq!(field_value_as_string(&json!({"region": null}), "region"), None);
        assert_eq!(field_value_as_string(&json!({}), "region"), None);
    }
}

fn start_task(
    ctx: &Ctx,
    template: &DynamicTaskTemplate,
    chain_name: String,
    task_name: String,
    input_topic: String,
    output_topic: String,
) {
    match template {
        DynamicTaskTemplate::Python { code, entrypoint, scaling, resources } => {
            activation::start_kafka_python_step(
                ctx.clone(),
                chain_name,
                KafkaPythonStep {
                    name: task_name,
                    input_topic,
                    output_topic,
                    code: code.clone(),
                    entrypoint: entrypoint.clone(),
                    scaling: scaling.clone(),
                    resources: resources.clone(),
                    env: Default::default(),
                    batch: None,
                },
            );
        }
        DynamicTaskTemplate::SparkSql { sql, spark } => {
            // No real `ChainConfig`/pipeline here — this is one of
            // dynamic_route's synthetic per-value tasks (see
            // `start_kafka_python_step`'s docs for the same note).
            // dynamic_route stays Kafka-topic transport on principle, so
            // this always builds the streaming-mode struct, never the new
            // batch-mode one.
            let pipeline_name = chain_name.clone();
            activation::start_kafka_spark_sql_step(
                ctx.clone(),
                chain_name,
                pipeline_name,
                KafkaSparkSqlStep {
                    name: task_name,
                    input_topic,
                    output_topic,
                    sql: sql.clone(),
                    spark: spark.clone(),
                    lookups: Vec::new(), // dynamic_route stays Kafka-topic transport; lookups aren't supported here
                    encrypt: Vec::new(), // dynamic_route stays Kafka-topic transport; encrypt isn't supported here
                    secrets: Vec::new(), // dynamic_route stays Kafka-topic transport; secrets aren't supported here
                },
            );
        }
        DynamicTaskTemplate::SparkDataFrame { code, spark } => {
            let pipeline_name = chain_name.clone();
            activation::start_kafka_spark_dataframe_step(
                ctx.clone(),
                chain_name,
                pipeline_name,
                KafkaSparkDataFrameStep {
                    name: task_name,
                    input_topic,
                    output_topic,
                    code: code.clone(),
                    spark: spark.clone(),
                    lookups: Vec::new(), // dynamic_route stays Kafka-topic transport; lookups aren't supported here
                    encrypt: Vec::new(), // dynamic_route stays Kafka-topic transport; encrypt isn't supported here
                    secrets: Vec::new(), // dynamic_route stays Kafka-topic transport; secrets aren't supported here
                },
            );
        }
    }
}
