//! Arbitrary Spark `--conf` passthrough: `spark { conf = { "spark.sql..."
//! = "..." } }` — an object literal (not a nested block) since Spark's
//! own setting names contain dots, which HCL body-attribute identifiers
//! can't. Keys are used verbatim, deliberately not given canoe-invented
//! aliases (e.g. no `partitions` shorthand for `spark.sql.shuffle.partitions`).

use canoe_config::StepConfig;

#[test]
fn spark_conf_passes_through_dotted_keys_verbatim() {
    let src = r#"
storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders" {
  source = "orders_adapter"
}

step "enrich" {
  engine = "spark_sql"
  input  = "orders_adapter"

  spark {
    executor_instances = 5
    conf = {
      "spark.sql.shuffle.partitions"        = "200"
      "spark.dynamicAllocation.enabled"     = "true"
      "spark.sql.adaptive.coalescePartitions.enabled" = "false"
    }
  }

  spark_sql {
    sql = "SELECT * FROM input"
  }
}

sink "land" {
  input      = "enrich"
  format     = "parquet"
  base_path  = "s3://canoe-lake-sink"
  dataset_id = "orders"
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    let chain = &config.chains[0];
    let StepConfig::KafkaSparkSql(step) = &chain.steps[0] else {
        panic!("expected enrich to compile to a (streaming, default) spark_sql step");
    };
    assert_eq!(step.spark.executor_instances, 5);

    let conf: std::collections::BTreeMap<_, _> = step.spark.extra_conf.iter().cloned().collect();
    assert_eq!(conf.get("spark.sql.shuffle.partitions").map(String::as_str), Some("200"));
    assert_eq!(conf.get("spark.dynamicAllocation.enabled").map(String::as_str), Some("true"));
    assert_eq!(
        conf.get("spark.sql.adaptive.coalescePartitions.enabled").map(String::as_str),
        Some("false")
    );
}

#[test]
fn spark_conf_defaults_to_empty() {
    let src = r#"
storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders" {
  source = "orders_adapter"
}

step "enrich" {
  engine = "spark_sql"
  input  = "orders_adapter"

  spark_sql {
    sql = "SELECT * FROM input"
  }
}

sink "land" {
  input      = "enrich"
  format     = "parquet"
  base_path  = "s3://canoe-lake-sink"
  dataset_id = "orders"
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    let chain = &config.chains[0];
    let StepConfig::KafkaSparkSql(step) = &chain.steps[0] else {
        panic!("expected enrich to compile to a spark_sql step");
    };
    assert!(step.spark.extra_conf.is_empty());
}

#[test]
fn non_object_conf_is_a_clear_compile_error() {
    let src = r#"
storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders" {
  source = "orders_adapter"
}

step "enrich" {
  engine = "spark_sql"
  input  = "orders_adapter"

  spark {
    conf = ["not", "an", "object"]
  }

  spark_sql {
    sql = "SELECT * FROM input"
  }
}
"#;
    let err = canoe_dsl::compile_str(src).expect_err("a list `conf` should fail to compile");
    let message = format!("{err:#}");
    assert!(message.contains("object"), "error should explain the expected shape: {message}");
}

/// `spark { event_log { ... } }` — a per-step override of the
/// coordinator's global `spark.event_log` default (see
/// `canoe_config::EventLogOverride`'s docs). The compiler only copies
/// this through verbatim; the actual merge against any global default
/// happens later, at Spark-launch time in `canoe-coordinator`.
#[test]
fn event_log_override_compiles_to_the_right_shape() {
    let src = r#"
storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders" {
  source = "orders_adapter"
}

step "enrich" {
  engine = "spark_sql"
  input  = "orders_adapter"

  spark {
    event_log {
      enabled = false
    }
  }

  spark_sql {
    sql = "SELECT * FROM input"
  }
}

sink "land" {
  input      = "enrich"
  format     = "parquet"
  base_path  = "s3://canoe-lake-sink"
  dataset_id = "orders"
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    let chain = &config.chains[0];
    let StepConfig::KafkaSparkSql(step) = &chain.steps[0] else {
        panic!("expected enrich to compile to a (streaming, default) spark_sql step");
    };
    let event_log = step.spark.event_log_override.as_ref().expect("event_log_override should be Some");
    assert_eq!(event_log.enabled, Some(false));
    assert!(event_log.base_path.is_none());
}

/// A step declaring `base_path` but not `enabled` must preserve that
/// distinction (`enabled: None`, not `enabled: Some(false)`) — this is
/// exactly what lets `resolve_event_log`'s merge logic inherit `enabled`
/// from the global default while still overriding `base_path`.
#[test]
fn event_log_override_partial_fields_stay_distinguishable_from_unset() {
    let src = r#"
storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders" {
  source = "orders_adapter"
}

step "enrich" {
  engine = "spark_sql"
  input  = "orders_adapter"

  spark {
    event_log {
      base_path = "s3a://canoe-lake/_step-events"
    }
  }

  spark_sql {
    sql = "SELECT * FROM input"
  }
}

sink "land" {
  input      = "enrich"
  format     = "parquet"
  base_path  = "s3://canoe-lake-sink"
  dataset_id = "orders"
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    let chain = &config.chains[0];
    let StepConfig::KafkaSparkSql(step) = &chain.steps[0] else {
        panic!("expected enrich to compile to a (streaming, default) spark_sql step");
    };
    let event_log = step.spark.event_log_override.as_ref().expect("event_log_override should be Some");
    assert!(event_log.enabled.is_none());
    assert_eq!(event_log.base_path.as_deref(), Some("s3a://canoe-lake/_step-events"));
}

/// Omitting `event_log { ... }` entirely must resolve to `None` —
/// backward compatible, every existing example unaffected.
#[test]
fn event_log_override_omitted_resolves_to_none() {
    let src = r#"
storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders" {
  source = "orders_adapter"
}

step "enrich" {
  engine = "spark_sql"
  input  = "orders_adapter"

  spark_sql {
    sql = "SELECT * FROM input"
  }
}

sink "land" {
  input      = "enrich"
  format     = "parquet"
  base_path  = "s3://canoe-lake-sink"
  dataset_id = "orders"
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    let chain = &config.chains[0];
    let StepConfig::KafkaSparkSql(step) = &chain.steps[0] else {
        panic!("expected enrich to compile to a (streaming, default) spark_sql step");
    };
    assert!(step.spark.event_log_override.is_none());
}
