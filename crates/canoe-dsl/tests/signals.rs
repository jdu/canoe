use canoe_config::StepConfig;

#[test]
fn sink_signal_and_schedule_compile_to_config() {
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
  base_path  = "s3://canoe-lake"
  dataset_id = "orders"
  signal     = "orders_landed"
}

pipeline "orders_rollup" {
  schedule {
    signal = "orders_landed"
  }

  source        = "orders.enriched"
}

step "totals" {
  engine = "spark_sql"
  input  = "orders.enriched"
  spark_sql { sql = "SELECT count(*) FROM input" }
}

sink "land_rollup" {
  input      = "totals"
  format     = "parquet"
  base_path  = "s3://canoe-lake"
  dataset_id = "orders_rollup"
}
"#;

    let config = canoe_dsl::compile_str(src).expect("should compile");
    assert_eq!(config.chains.len(), 2);

    let orders = config
        .chains
        .iter()
        .find(|c| c.name.starts_with("orders-"))
        .expect("orders chain");
    assert!(orders.trigger.is_none(), "untriggered pipeline must have no trigger");
    let StepConfig::Sink(sink) = orders.steps.last().unwrap() else {
        panic!("expected sink step");
    };
    assert_eq!(sink.signal.as_deref(), Some("orders_landed"));

    let rollup = config
        .chains
        .iter()
        .find(|c| c.name.starts_with("orders_rollup-"))
        .expect("rollup chain");
    let trigger = rollup.trigger.as_ref().expect("rollup chain must be triggered");
    assert_eq!(trigger.signal.as_deref(), Some("orders_landed"));
    assert!(trigger.cron.is_none());

    // Round-trip through canoe-config's own loader, same as canoe-dslc's output would be.
    let yaml = serde_yaml::to_string(&config).unwrap();
    canoe_config::PipelineConfig::parse(&yaml).expect("compiled config must validate");
}
