#[test]
fn topics_block_compiles_retention_and_archive_defaults() {
    let src = r#"
storage {
  base_path = "s3://bucket"
}

topics {
  retention {
    duration = "7d"
  }
  archive {
    base_path = "s3://canoe-archive"
    interval  = "10m"
  }
}

pipeline "orders" {
  source        = "orders.raw"
}

step "validate" {
  engine = "python"
  input  = "orders.raw"
  filter { predicate = "amount > 0" }
}

step "enrich" {
  engine = "spark_sql"
  input  = "validate"
  with_field {
    name       = "flag"
    expression = "true"
  }
}

sink "land" {
  input      = "enrich"
  format     = "parquet"
  base_path  = "s3://bucket"
  dataset_id = "orders"
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");

    let defaults = config.topic_defaults.clone().expect("topic_defaults should be set");
    assert_eq!(defaults.retention_ms, Some(7 * 24 * 60 * 60 * 1000));
    let archive = defaults.archive.expect("archive should be set");
    assert_eq!(archive.base_path, "s3://canoe-archive");
    assert_eq!(archive.interval_secs, 600);

    // Only Kafka-transport nodes get a managed topic now: "enrich"
    // (spark_sql, still Kafka-topic-based) but not "validate" (python,
    // S3 + signal transport — no Kafka topic of its own any more), the
    // sink (no output at all), or the external source.
    assert_eq!(config.managed_topics.len(), 1);
    let names: std::collections::HashSet<&str> =
        config.managed_topics.iter().map(|t| t.task.as_str()).collect();
    assert_eq!(names, std::collections::HashSet::from(["enrich"]));
    for mt in &config.managed_topics {
        assert_eq!(mt.pipeline, "orders");
        assert!(mt.topic.starts_with("_canoe."), "topic should be internal: {}", mt.topic);
    }

    let yaml = serde_yaml::to_string(&config).unwrap();
    canoe_config::PipelineConfig::parse(&yaml).expect("compiled config must validate");
}

#[test]
fn topics_block_is_optional() {
    let src = r#"
storage {
  base_path = "s3://bucket"
}

pipeline "orders" {
  source        = "orders.raw"
}
step "validate" {
  engine = "python"
  input  = "orders.raw"
  filter { predicate = "amount > 0" }
}
sink "land" {
  input      = "validate"
  format     = "parquet"
  base_path  = "s3://bucket"
  dataset_id = "orders"
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile without a topics block");
    assert!(config.topic_defaults.is_none());
    // "validate" is a python (S3-transport) step — no Kafka topic of its
    // own, so nothing lands in managed_topics here at all.
    assert_eq!(config.managed_topics.len(), 0);
}

#[test]
fn rejects_duplicate_topics_blocks() {
    let src = r#"
storage {
  base_path = "s3://bucket"
}

topics {
  retention { duration = "1d" }
}
topics {
  retention { duration = "2d" }
}
pipeline "orders" {
  source        = "orders.raw"
}
step "validate" {
  engine = "python"
  input  = "orders.raw"
  filter { predicate = "amount > 0" }
}
sink "land" {
  input      = "validate"
  format     = "parquet"
  base_path  = "s3://bucket"
  dataset_id = "orders"
}
"#;
    assert!(canoe_dsl::compile_str(src).is_err());
}

#[test]
fn rejects_invalid_retention_duration() {
    let src = r#"
storage {
  base_path = "s3://bucket"
}

topics {
  retention { duration = "not a duration" }
}
pipeline "orders" {
  source        = "orders.raw"
}
step "validate" {
  engine = "python"
  input  = "orders.raw"
  filter { predicate = "amount > 0" }
}
sink "land" {
  input      = "validate"
  format     = "parquet"
  base_path  = "s3://bucket"
  dataset_id = "orders"
}
"#;
    assert!(canoe_dsl::compile_str(src).is_err());
}
