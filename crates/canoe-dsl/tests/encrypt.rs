//! Column-level field encryption: an `encrypt { fields = [...]; key = "..." }`
//! block on `step`/`sink`/`adapter`, copied straight through to
//! `canoe_config::EncryptConfig` with no path resolution (unlike
//! `lookup`, nothing here references another node) — plus the compiler's
//! one real piece of validation: rejecting a field that's both encrypted
//! and used as a `checkpoint.keys`/`scd.keys`/`scd.track_fields` entry on
//! the same sink.

use canoe_config::StepConfig;

#[test]
fn step_can_declare_multiple_encrypt_blocks() {
    let src = r#"
storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders" {
  source = "orders_adapter"
}

step "mask_pii" {
  engine = "python"
  input  = "orders_adapter"

  encrypt {
    fields = ["ssn", "email"]
    key    = "pii_v1"
  }

  encrypt {
    fields = ["card_number"]
    key    = "payments_v1"
  }

  filter { predicate = "amount > 0" }
}

sink "land" {
  input      = "mask_pii"
  format     = "parquet"
  base_path  = "s3://canoe-lake-sink"
  dataset_id = "orders"
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    let chain = &config.chains[0];
    let StepConfig::Python(step) = &chain.steps[0] else {
        panic!("expected mask_pii to compile to a python step");
    };
    assert_eq!(step.encrypt.len(), 2);
    let pii = step.encrypt.iter().find(|e| e.key == "pii_v1").expect("pii_v1 block");
    assert_eq!(pii.fields, vec!["ssn".to_string(), "email".to_string()]);
    let payments = step.encrypt.iter().find(|e| e.key == "payments_v1").expect("payments_v1 block");
    assert_eq!(payments.fields, vec!["card_number".to_string()]);
}

#[test]
fn sink_can_declare_encrypt() {
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

  encrypt {
    fields = ["ssn"]
    key    = "pii_v1"
  }
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    let StepConfig::Sink(sink) = &config.chains[0].steps[1] else {
        panic!("expected land to compile to a sink step");
    };
    assert_eq!(sink.encrypt.len(), 1);
    assert_eq!(sink.encrypt[0].fields, vec!["ssn".to_string()]);
    assert_eq!(sink.encrypt[0].key, "pii_v1");
}

#[test]
fn adapter_can_declare_encrypt() {
    let src = r#"
adapter "orders_adapter" {
  topic = "orders.raw"

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

  encrypt {
    fields = ["card_last4"]
    key    = "pii_v1"
  }
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    assert_eq!(config.adapters[0].encrypt.len(), 1);
    assert_eq!(config.adapters[0].encrypt[0].fields, vec!["card_last4".to_string()]);
}

#[test]
fn rejects_a_field_both_encrypted_and_used_as_a_checkpoint_key() {
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

  encrypt {
    fields = ["customer_id"]
    key    = "pii_v1"
  }

  checkpoint {
    keys = ["customer_id"]
  }
}
"#;
    let err = canoe_dsl::compile_str(src).expect_err("encrypted checkpoint key should be rejected");
    let message = format!("{err:#}");
    assert!(message.contains("customer_id"), "error should name the conflicting field: {message}");
    assert!(message.contains("checkpoint"), "error should mention checkpoint: {message}");
}

#[test]
fn rejects_a_field_both_encrypted_and_used_as_an_scd_track_field() {
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

  encrypt {
    fields = ["payment_method"]
    key    = "pii_v1"
  }

  scd {
    keys         = ["customer_id"]
    track_fields = ["payment_method"]
  }
}
"#;
    let err = canoe_dsl::compile_str(src).expect_err("encrypted scd track_field should be rejected");
    let message = format!("{err:#}");
    assert!(message.contains("payment_method"), "error should name the conflicting field: {message}");
    assert!(message.contains("scd"), "error should mention scd: {message}");
}

#[test]
fn dynamic_route_task_rejects_an_encrypt_block() {
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
    engine = "python"

    encrypt {
      fields = ["ssn"]
      key    = "pii_v1"
    }

    filter { predicate = "true" }
  }
}
"#;
    let err = canoe_dsl::compile_str(src).expect_err("dynamic_route task with an encrypt block should be rejected");
    let message = format!("{err:#}");
    assert!(message.contains("encrypt"), "error should mention encrypt: {message}");
}
