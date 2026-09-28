use canoe_config::{CodeSource, StepConfig};

#[test]
fn map_values_with_s3_source_compiles_for_python_engine() {
    let src = r#"
storage {
  base_path = "s3://bucket"
}

pipeline "orders" {
  source        = "orders.raw"
}

step "normalize" {
  engine = "python"
  input  = "orders.raw"

  map_values {
    field = "region_code"
    to    = "region_name"
    mapping {
      bucket = "canoe-reference-data"
      key    = "region_map.json"
    }
  }
}

sink "land" {
  input      = "normalize"
  format     = "parquet"
  base_path  = "s3://bucket"
  dataset_id = "orders"
}
"#;
    // No network access needed: an S3 mapping source is only ever
    // resolved at runtime by the worker pod, never at DSL-compile time.
    let config = canoe_dsl::compile_str(src).expect("should compile without touching S3");
    let StepConfig::Python(step) = &config.chains[0].steps[0] else {
        panic!("expected python step");
    };
    let CodeSource::Inline { source } = &step.code else {
        panic!("expected inline generated source");
    };
    assert!(source.contains("from canoe_worker.mapping import load_json_mapping"));
    assert!(source.contains("canoe-reference-data"));
    assert!(source.contains("region_map.json"));
}

#[test]
fn map_values_with_s3_source_is_rejected_for_spark_sql_engine() {
    let src = r#"
pipeline "orders" {
  source        = "orders.raw"
}

step "normalize" {
  engine = "spark_sql"
  input  = "orders.raw"

  map_values {
    field = "region_code"
    mapping {
      bucket = "canoe-reference-data"
      key    = "region_map.json"
    }
  }
}

sink "land" {
  input      = "normalize"
  format     = "parquet"
  base_path  = "s3://bucket"
  dataset_id = "orders"
}
"#;
    let err = canoe_dsl::compile_str(src).expect_err("S3 mapping should be rejected for spark_sql");
    let message = format!("{err:#}");
    assert!(message.contains("spark_sql"), "error should explain the spark_sql restriction: {message}");
}

#[test]
fn map_values_with_inline_json_compiles_to_case_when_for_spark_sql() {
    let src = r#"
pipeline "orders" {
  source        = "orders.raw"
}

step "normalize" {
  engine = "spark_sql"
  input  = "orders.raw"

  map_values {
    field = "region_code"
    to    = "region_name"
    mapping {
      code = "{\"US\": \"United States\", \"CA\": \"Canada\"}"
    }
  }
}

sink "land" {
  input      = "normalize"
  format     = "parquet"
  base_path  = "s3://bucket"
  dataset_id = "orders"
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    // No `mode` declared, so this defaults to mode="streaming" (spark_sql's
    // pre-existing-behavior default) — KafkaSparkSql, not the new
    // batch-mode SparkSql.
    let StepConfig::KafkaSparkSql(step) = &config.chains[0].steps[0] else {
        panic!("expected streaming-mode (kafka_spark_sql) step");
    };
    assert!(step.sql.contains("WHEN CAST(region_code AS STRING) = 'US' THEN 'United States'"));
    assert!(step.sql.contains("WHEN CAST(region_code AS STRING) = 'CA' THEN 'Canada'"));
    assert!(step.sql.contains("AS region_name"));

    let yaml = serde_yaml::to_string(&config).unwrap();
    canoe_config::PipelineConfig::parse(&yaml).expect("compiled config must validate");
}

#[test]
fn map_values_rejects_non_object_mapping() {
    let src = r#"
storage {
  base_path = "s3://bucket"
}

pipeline "orders" {
  source        = "orders.raw"
}

step "normalize" {
  engine = "python"
  input  = "orders.raw"

  map_values {
    field = "region_code"
    mapping {
      code = "[1, 2, 3]"
    }
  }
}

sink "land" {
  input      = "normalize"
  format     = "parquet"
  base_path  = "s3://bucket"
  dataset_id = "orders"
}
"#;
    assert!(canoe_dsl::compile_str(src).is_err());
}
