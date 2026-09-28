//! `secrets = ["name", ...]` — AWS Secrets Manager entries a step/adapter/
//! task's pod or Job should have mounted as files, via the Secrets Store
//! CSI Driver (see `crates/canoe-k8s/src/secrets.rs`). Copied straight
//! through to `canoe_config`, no path resolution needed. Compile-time
//! validation is limited to rejecting an empty or duplicate name — canoe
//! has no way to check a declared secret actually exists in Secrets
//! Manager.

use canoe_config::StepConfig;

#[test]
fn step_can_declare_secrets() {
    let src = r#"
storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders" {
  source = "orders_adapter"
}

step "charge_customer" {
  engine  = "python"
  input   = "orders_adapter"
  secrets = ["stripe_api_key", "db_password"]

  filter { predicate = "amount > 0" }
}

sink "land" {
  input      = "charge_customer"
  format     = "parquet"
  base_path  = "s3://canoe-lake-sink"
  dataset_id = "orders"
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    let StepConfig::Python(step) = &config.chains[0].steps[0] else {
        panic!("expected charge_customer to compile to a python step");
    };
    assert_eq!(step.secrets, vec!["stripe_api_key".to_string(), "db_password".to_string()]);
}

#[test]
fn secrets_defaults_to_empty_when_omitted() {
    let src = r#"
storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders" {
  source = "orders_adapter"
}

step "validate" {
  engine = "python"
  input  = "orders_adapter"
  filter { predicate = "amount > 0" }
}

sink "land" {
  input      = "validate"
  format     = "parquet"
  base_path  = "s3://canoe-lake-sink"
  dataset_id = "orders"
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    let StepConfig::Python(step) = &config.chains[0].steps[0] else {
        panic!("expected validate to compile to a python step");
    };
    assert!(step.secrets.is_empty());
}

#[test]
fn adapter_can_declare_secrets() {
    let src = r#"
adapter "stripe" {
  topic   = "stripe.charges"
  secrets = ["stripe_api_key"]

  schedule {
    interval = "5m"
  }

  script {
    code = <<-PY
      # /// script
      # dependencies = []
      # ///
      pass
    PY
  }
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    assert_eq!(config.adapters[0].secrets, vec!["stripe_api_key".to_string()]);
}

#[test]
fn task_can_declare_secrets() {
    let src = r#"
storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders" {
  source = "orders_adapter"
}

step "validate" {
  engine = "python"
  input  = "orders_adapter"
  filter { predicate = "amount > 0" }
}

sink "land" {
  input      = "validate"
  format     = "parquet"
  base_path  = "s3://canoe-lake-sink"
  dataset_id = "orders"
}

task "notify_ops" {
  depends_on = ["validate"]
  secrets    = ["ops_webhook_token"]

  script {
    code = <<-PY
      # /// script
      # dependencies = []
      # ///
      pass
    PY
  }
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    assert_eq!(config.tasks[0].secrets, vec!["ops_webhook_token".to_string()]);
}

#[test]
fn rejects_an_empty_secret_name() {
    let src = r#"
storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders" {
  source = "orders_adapter"
}

step "validate" {
  engine  = "python"
  input   = "orders_adapter"
  secrets = [""]
  filter { predicate = "amount > 0" }
}
"#;
    let err = canoe_dsl::compile_str(src).expect_err("empty secret name should be rejected");
    let message = format!("{err:#}");
    assert!(message.contains("empty"), "error should mention the empty name: {message}");
}

#[test]
fn rejects_a_duplicate_secret_name() {
    let src = r#"
storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders" {
  source = "orders_adapter"
}

step "validate" {
  engine  = "python"
  input   = "orders_adapter"
  secrets = ["stripe_api_key", "stripe_api_key"]
  filter { predicate = "amount > 0" }
}
"#;
    let err = canoe_dsl::compile_str(src).expect_err("duplicate secret name should be rejected");
    let message = format!("{err:#}");
    assert!(message.contains("stripe_api_key"), "error should name the duplicate: {message}");
}

#[test]
fn dynamic_route_task_rejects_secrets() {
    let src = r#"
storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders" {
  source = "orders_adapter"
}

dynamic_route "by_region" {
  input = "orders_adapter"
  field = "region"

  task {
    engine  = "python"
    secrets = ["stripe_api_key"]

    filter { predicate = "true" }
  }
}
"#;
    let err = canoe_dsl::compile_str(src).expect_err("dynamic_route task with secrets should be rejected");
    let message = format!("{err:#}");
    assert!(message.contains("secrets"), "error should mention secrets: {message}");
}
