use canoe_config::StepConfig;

#[test]
fn scd_block_compiles_onto_the_sink_step() {
    let src = r#"
storage {
  base_path = "s3://bucket"
}

pipeline "customers" {
  source        = "customers.raw"
}

step "validate" {
  engine = "python"
  input  = "customers.raw"
  filter { predicate = "customer_id != ''" }
}

sink "land" {
  input      = "validate"
  format     = "parquet"
  base_path  = "s3://bucket"
  dataset_id = "customers"

  scd {
    keys = ["customer_id"]
  }
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    let StepConfig::Sink(sink) = &config.chains[0].steps[1] else {
        panic!("expected land to compile to a sink step");
    };
    let scd = sink.scd.as_ref().expect("scd should be set");
    assert_eq!(scd.keys, vec!["customer_id".to_string()]);
    assert_eq!(scd.scd_type, 2);
    assert!(scd.track_fields.is_empty());
    assert!(scd.effective_field.is_none());

    let yaml = serde_yaml::to_string(&config).unwrap();
    canoe_config::PipelineConfig::parse(&yaml).expect("compiled config must validate");
}

#[test]
fn scd_supports_track_fields_and_effective_field() {
    let src = r#"
storage {
  base_path = "s3://bucket"
}

pipeline "customers" {
  source        = "customers.raw"
}
step "validate" {
  engine = "python"
  input  = "customers.raw"
  filter { predicate = "customer_id != ''" }
}
sink "land" {
  input      = "validate"
  format     = "parquet"
  base_path  = "s3://bucket"
  dataset_id = "customers"
  scd {
    keys            = ["customer_id"]
    track_fields    = ["plan", "region"]
    effective_field = "updated_at"
  }
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    let StepConfig::Sink(sink) = &config.chains[0].steps[1] else {
        panic!("expected sink step");
    };
    let scd = sink.scd.as_ref().unwrap();
    assert_eq!(scd.track_fields, vec!["plan".to_string(), "region".to_string()]);
    assert_eq!(scd.effective_field.as_deref(), Some("updated_at"));
}

#[test]
fn scd_is_optional() {
    let src = r#"
storage {
  base_path = "s3://bucket"
}

pipeline "customers" {
  source        = "customers.raw"
}
step "validate" {
  engine = "python"
  input  = "customers.raw"
  filter { predicate = "customer_id != ''" }
}
sink "land" {
  input      = "validate"
  format     = "parquet"
  base_path  = "s3://bucket"
  dataset_id = "customers"
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    let StepConfig::Sink(sink) = &config.chains[0].steps[1] else {
        panic!("expected sink step");
    };
    assert!(sink.scd.is_none());
}

#[test]
fn checkpoint_and_scd_can_both_be_set_on_the_same_sink() {
    let src = r#"
storage {
  base_path = "s3://bucket"
}

pipeline "customers" {
  source        = "customers.raw"
}
step "validate" {
  engine = "python"
  input  = "customers.raw"
  filter { predicate = "customer_id != ''" }
}
sink "land" {
  input      = "validate"
  format     = "parquet"
  base_path  = "s3://bucket"
  dataset_id = "customers"
  checkpoint {
    keys = ["customer_id"]
  }
  scd {
    keys = ["customer_id"]
  }
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    let StepConfig::Sink(sink) = &config.chains[0].steps[1] else {
        panic!("expected sink step");
    };
    assert!(sink.checkpoint.is_some());
    assert!(sink.scd.is_some());
}

#[test]
fn rejects_scd_without_keys() {
    let src = r#"
storage {
  base_path = "s3://bucket"
}

pipeline "customers" {
  source        = "customers.raw"
}
step "validate" {
  engine = "python"
  input  = "customers.raw"
  filter { predicate = "customer_id != ''" }
}
sink "land" {
  input      = "validate"
  format     = "parquet"
  base_path  = "s3://bucket"
  dataset_id = "customers"
  scd {}
}
"#;
    assert!(canoe_dsl::compile_str(src).is_err());
}

#[test]
fn rejects_unsupported_scd_type() {
    let src = r#"
storage {
  base_path = "s3://bucket"
}

pipeline "customers" {
  source        = "customers.raw"
}
step "validate" {
  engine = "python"
  input  = "customers.raw"
  filter { predicate = "customer_id != ''" }
}
sink "land" {
  input      = "validate"
  format     = "parquet"
  base_path  = "s3://bucket"
  dataset_id = "customers"
  scd {
    keys = ["customer_id"]
    type = 1
  }
}
"#;
    let err = canoe_dsl::compile_str(src).expect_err("type 1 should be rejected");
    let msg = format!("{err:#}");
    assert!(msg.contains("Type 2") || msg.contains("not supported"), "unexpected error: {msg}");
}
