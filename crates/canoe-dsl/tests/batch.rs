use canoe_config::StepConfig;

#[test]
fn batch_block_compiles_onto_an_escape_hatch_python_step() {
    let src = r#"
storage {
  base_path = "s3://bucket"
}

pipeline "orders" {
  source        = "orders.raw"
}

step "aggregate" {
  engine = "python"
  input  = "orders.raw"

  python {
    code = "def run(records):\n    return records"
  }

  batch {
    size                 = 500
    flush_interval_secs  = 10
  }
}

sink "land" {
  input      = "aggregate"
  format     = "parquet"
  base_path  = "s3://bucket"
  dataset_id = "orders"
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    let StepConfig::Python(step) = &config.chains[0].steps[0] else {
        panic!("expected aggregate to compile to a python step");
    };
    let batch = step.batch.as_ref().expect("batch should be set");
    assert_eq!(batch.size, 500);
    assert_eq!(batch.flush_interval_secs, 10);

    let yaml = serde_yaml::to_string(&config).unwrap();
    canoe_config::PipelineConfig::parse(&yaml).expect("compiled config must validate");
}

#[test]
fn batch_flush_interval_secs_defaults_when_omitted() {
    let src = r#"
storage {
  base_path = "s3://bucket"
}

pipeline "orders" {
  source        = "orders.raw"
}
step "aggregate" {
  engine = "python"
  input  = "orders.raw"
  python {
    code = "def run(records):\n    return records"
  }
  batch {
    size = 100
  }
}
sink "land" {
  input      = "aggregate"
  format     = "parquet"
  base_path  = "s3://bucket"
  dataset_id = "orders"
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    let StepConfig::Python(step) = &config.chains[0].steps[0] else {
        panic!("expected python step");
    };
    assert_eq!(step.batch.as_ref().unwrap().flush_interval_secs, 5);
}

#[test]
fn batch_is_optional() {
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
    let config = canoe_dsl::compile_str(src).expect("should compile");
    let StepConfig::Python(step) = &config.chains[0].steps[0] else {
        panic!("expected python step");
    };
    assert!(step.batch.is_none());
}

#[test]
fn rejects_batch_without_size() {
    let src = r#"
storage {
  base_path = "s3://bucket"
}

pipeline "orders" {
  source        = "orders.raw"
}
step "aggregate" {
  engine = "python"
  input  = "orders.raw"
  python {
    code = "def run(records):\n    return records"
  }
  batch {}
}
sink "land" {
  input      = "aggregate"
  format     = "parquet"
  base_path  = "s3://bucket"
  dataset_id = "orders"
}
"#;
    assert!(canoe_dsl::compile_str(src).is_err());
}

#[test]
fn rejects_batch_on_declarative_ops() {
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
  batch {
    size = 100
  }
}
sink "land" {
  input      = "validate"
  format     = "parquet"
  base_path  = "s3://bucket"
  dataset_id = "orders"
}
"#;
    let err = canoe_dsl::compile_str(src).expect_err("declarative ops must reject batch");
    let msg = format!("{err:#}");
    assert!(msg.contains("escape hatch"), "unexpected error: {msg}");
}

#[test]
fn rejects_batch_on_uv_engine() {
    let src = r#"
storage {
  base_path = "s3://bucket"
}

pipeline "orders" {
  source        = "orders.raw"
}
step "geocode" {
  engine = "uv"
  input  = "orders.raw"
  script {
    code = "print('hi')"
  }
  batch {
    size = 100
  }
}
sink "land" {
  input      = "geocode"
  format     = "parquet"
  base_path  = "s3://bucket"
  dataset_id = "orders"
}
"#;
    let err = canoe_dsl::compile_str(src).expect_err("uv engine must reject batch");
    let msg = format!("{err:#}");
    assert!(msg.contains("batch"), "unexpected error: {msg}");
}
