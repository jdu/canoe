#[test]
fn adapter_with_interval_schedule_compiles() {
    let src = r#"
adapter "stripe_charges" {
  topic = "stripe.charges.raw"

  schedule {
    interval = "5m"
  }

  script {
    code = <<-PY
      # /// script
      # dependencies = ["stripe"]
      # ///
      print("hello")
    PY
  }
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    assert_eq!(config.adapters.len(), 1);
    let adapter = &config.adapters[0];
    assert_eq!(adapter.name, "stripe_charges");
    assert_eq!(adapter.topic.as_deref(), Some("stripe.charges.raw"));
    assert!(adapter.output_path.is_none());
    assert!(adapter.output.is_none());
    assert_eq!(adapter.schedule.interval_secs, Some(300));
    assert!(adapter.schedule.cron.is_none());

    let canoe_config::CodeSource::Inline { source } = &adapter.code else {
        panic!("expected inline script source");
    };
    assert!(source.contains("# /// script"));
    assert!(source.contains("print(\"hello\")"));

    let yaml = serde_yaml::to_string(&config).unwrap();
    canoe_config::PipelineConfig::parse(&yaml).expect("compiled config must validate");
}

#[test]
fn adapter_with_cron_schedule_compiles() {
    let src = r#"
adapter "nightly_export" {
  topic = "exports.nightly.raw"
  schedule {
    cron = "0 2 * * *"
  }
  script {
    code = "print('hi')"
  }
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    assert_eq!(config.adapters[0].schedule.cron.as_deref(), Some("0 2 * * *"));
    assert!(config.adapters[0].schedule.interval_secs.is_none());
}

#[test]
fn adapter_rejects_invalid_interval() {
    let src = r#"
adapter "bad" {
  topic = "x.raw"
  schedule {
    interval = "not a duration"
  }
  script {
    code = "print('hi')"
  }
}
"#;
    assert!(canoe_dsl::compile_str(src).is_err());
}

#[test]
fn adapter_rejects_missing_schedule() {
    let src = r#"
adapter "bad" {
  topic = "x.raw"
  script {
    code = "print('hi')"
  }
}
"#;
    assert!(canoe_dsl::compile_str(src).is_err());
}

#[test]
fn batch_adapter_resolves_output_path_and_output() {
    let src = r#"
storage {
  base_path = "s3://bucket/canoe"
}

adapter "orders_adapter" {
  mode = "batch"

  schedule {
    interval = "1m"
  }

  output {
    batch_size          = 500
    flush_interval_secs = 30
    partition_by        = ["region"]
  }

  script {
    code = "print('hi')"
  }
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    let adapter = &config.adapters[0];
    assert!(adapter.topic.is_none());
    assert_eq!(adapter.output_path.as_deref(), Some("s3://bucket/canoe/v1/orders_adapter"));
    let output = adapter.output.as_ref().expect("output config");
    assert_eq!(output.batch_size, 500);
    assert_eq!(output.flush_interval_secs, 30);
    assert_eq!(output.partition_by, vec!["region".to_string()]);

    let yaml = serde_yaml::to_string(&config).unwrap();
    canoe_config::PipelineConfig::parse(&yaml).expect("compiled config must validate");
}

#[test]
fn batch_adapter_rejects_topic() {
    let src = r#"
storage {
  base_path = "s3://bucket/canoe"
}

adapter "bad" {
  mode = "batch"
  topic = "x.raw"

  schedule {
    interval = "1m"
  }

  output {}

  script {
    code = "print('hi')"
  }
}
"#;
    let err = format!("{:#}", canoe_dsl::compile_str(src).unwrap_err());
    assert!(err.contains("topic"), "unexpected error: {err}");
}

#[test]
fn streaming_adapter_rejects_output_block() {
    let src = r#"
adapter "bad" {
  topic = "x.raw"

  schedule {
    interval = "1m"
  }

  output {}

  script {
    code = "print('hi')"
  }
}
"#;
    let err = format!("{:#}", canoe_dsl::compile_str(src).unwrap_err());
    assert!(err.contains("output"), "unexpected error: {err}");
}

#[test]
fn batch_adapter_requires_storage_block() {
    let src = r#"
adapter "bad" {
  mode = "batch"

  schedule {
    interval = "1m"
  }

  output {}

  script {
    code = "print('hi')"
  }
}
"#;
    assert!(canoe_dsl::compile_str(src).is_err());
}

/// A downstream batch step can reference a batch-mode adapter by name and
/// get the adapter's real resolved S3 output path as its `input_path` —
/// not the raw name treated as a literal path (see `compile_graph`'s
/// batch-adapter seeding of `name_to_ref`/`name_to_signal`).
#[test]
fn downstream_step_resolves_batch_adapter_by_name() {
    let src = r#"
storage {
  base_path = "s3://bucket/canoe"
}

pipeline "orders" {
  source = "orders_adapter"
}

adapter "orders_adapter" {
  mode = "batch"

  schedule {
    interval = "1m"
  }

  output {}

  script {
    code = "print('hi')"
  }
}

step "validate" {
  engine = "python"
  input  = "orders_adapter"
  code   = "pass"
}

sink "validate_sink" {
  input     = "validate"
  format    = "parquet"
  base_path = "s3://bucket/canoe/sinks/validate"
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    let chain = &config.chains[0];
    let step = &chain.steps[0];
    let canoe_config::StepConfig::Python(python_step) = step else {
        panic!("expected python step (batch mode)");
    };
    assert_eq!(python_step.input_path, "s3://bucket/canoe/v1/orders_adapter");
}
