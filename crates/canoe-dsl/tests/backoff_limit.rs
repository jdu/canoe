use canoe_config::StepConfig;

#[test]
fn step_backoff_limit_defaults_to_two_and_is_overridable() {
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
    assert_eq!(step.scaling.backoff_limit, 2);
}

#[test]
fn step_backoff_limit_can_be_set_explicitly() {
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
  scaling {
    backoff_limit = 5
  }
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
    assert_eq!(step.scaling.backoff_limit, 5);
}

#[test]
fn adapter_backoff_limit_defaults_to_two_and_is_overridable() {
    let default_src = r#"
adapter "a" {
  topic = "a.raw"
  schedule { interval = "5m" }
  script { code = "print('hi')" }
}
"#;
    let config = canoe_dsl::compile_str(default_src).expect("should compile");
    assert_eq!(config.adapters[0].backoff_limit, 2);

    let overridden_src = r#"
adapter "a" {
  topic = "a.raw"
  schedule { interval = "5m" }
  script { code = "print('hi')" }
  backoff_limit = 7
}
"#;
    let config = canoe_dsl::compile_str(overridden_src).expect("should compile");
    assert_eq!(config.adapters[0].backoff_limit, 7);
}

#[test]
fn task_backoff_limit_defaults_to_two_and_is_overridable() {
    let default_src = r#"
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
task "notify" {
  depends_on = ["validate"]
  script { code = "print('hi')" }
}
"#;
    let config = canoe_dsl::compile_str(default_src).expect("should compile");
    assert_eq!(config.tasks[0].backoff_limit, 2);

    let overridden_src = default_src.replace(
        "task \"notify\" {\n  depends_on = [\"validate\"]",
        "task \"notify\" {\n  backoff_limit = 4\n  depends_on = [\"validate\"]",
    );
    let config = canoe_dsl::compile_str(&overridden_src).expect("should compile");
    assert_eq!(config.tasks[0].backoff_limit, 4);
}
