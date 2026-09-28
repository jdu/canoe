use canoe_config::StepConfig;

#[test]
fn checkpoint_block_compiles_onto_the_sink_step() {
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

  checkpoint {
    keys = ["customer_id"]
  }
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    let StepConfig::Sink(sink) = &config.chains[0].steps[1] else {
        panic!("expected land to compile to a sink step");
    };
    let checkpoint = sink.checkpoint.as_ref().expect("checkpoint should be set");
    assert_eq!(checkpoint.keys, vec!["customer_id".to_string()]);

    let yaml = serde_yaml::to_string(&config).unwrap();
    canoe_config::PipelineConfig::parse(&yaml).expect("compiled config must validate");
}

#[test]
fn checkpoint_supports_composite_keys() {
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
  checkpoint {
    keys = ["order_id", "line_no"]
  }
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    let StepConfig::Sink(sink) = &config.chains[0].steps[1] else {
        panic!("expected sink step");
    };
    assert_eq!(sink.checkpoint.as_ref().unwrap().keys, vec!["order_id".to_string(), "line_no".to_string()]);
}

#[test]
fn checkpoint_is_optional() {
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
    let StepConfig::Sink(sink) = &config.chains[0].steps[1] else {
        panic!("expected sink step");
    };
    assert!(sink.checkpoint.is_none());
}

#[test]
fn rejects_checkpoint_without_keys() {
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
  checkpoint {}
}
"#;
    assert!(canoe_dsl::compile_str(src).is_err());
}
