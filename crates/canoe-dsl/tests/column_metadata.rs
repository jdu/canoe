//! Column metadata at sink points: a `metadata = "column-metadata.yaml"`
//! attribute on `sink { ... }`, resolved at compile time (same "read a
//! `file = ...` once, embed the result" precedent as `map_values`'s
//! `mapping { file = ... }`) into `SinkStep.column_metadata`.

use std::io::Write;

fn write_temp(dir: &std::path::Path, name: &str, contents: &str) {
    let mut f = std::fs::File::create(dir.join(name)).unwrap();
    f.write_all(contents.as_bytes()).unwrap();
}

#[test]
fn sink_metadata_file_resolves_into_column_metadata() {
    let dir = std::env::temp_dir().join(format!("canoe-dsl-test-metadata-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    write_temp(
        &dir,
        "column-metadata.yaml",
        r#"
customer_id:
  label: "Customer ID"
  description: "Unique identifier for the customer"
  sensitivity: "confidential"
  classification: ["pii"]
email:
  label: "Email Address"
  sensitivity: "restricted"
"#,
    );

    let src = r#"
storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders" {
  source = "orders_adapter"
}

sink "land" {
  input      = "orders_adapter"
  format     = "parquet"
  base_path  = "s3://canoe-lake"
  dataset_id = "orders"
  metadata   = "column-metadata.yaml"
}
"#;
    let config = canoe_dsl::compile_str_with_base(src, &dir).expect("should compile");
    let canoe_config::StepConfig::Sink(sink) = &config.chains[0].steps[0] else {
        panic!("expected sink step");
    };
    assert_eq!(sink.column_metadata.len(), 2);
    let customer_id = &sink.column_metadata["customer_id"];
    assert_eq!(customer_id["label"], "Customer ID");
    assert_eq!(customer_id["sensitivity"], "confidential");
    assert_eq!(customer_id["classification"], serde_json::json!(["pii"]));

    let email = &sink.column_metadata["email"];
    assert_eq!(email["label"], "Email Address");
    assert!(email.get("classification").is_none());

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn sink_without_metadata_attribute_compiles_with_an_empty_map() {
    let src = r#"
storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders" {
  source = "orders_adapter"
}

sink "land" {
  input      = "orders_adapter"
  format     = "parquet"
  base_path  = "s3://canoe-lake"
  dataset_id = "orders"
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    let canoe_config::StepConfig::Sink(sink) = &config.chains[0].steps[0] else {
        panic!("expected sink step");
    };
    assert!(sink.column_metadata.is_empty());
}

#[test]
fn missing_metadata_file_produces_a_clear_error() {
    let src = r#"
storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders" {
  source = "orders_adapter"
}

sink "land" {
  input      = "orders_adapter"
  format     = "parquet"
  base_path  = "s3://canoe-lake"
  dataset_id = "orders"
  metadata   = "does_not_exist.yaml"
}
"#;
    let err = canoe_dsl::compile_str(src).expect_err("missing metadata file must fail to compile");
    let message = format!("{err:#}");
    assert!(message.contains("does_not_exist.yaml"), "error should name the missing file: {message}");
}

#[test]
fn non_mapping_metadata_yaml_produces_a_clear_error() {
    let dir = std::env::temp_dir().join(format!("canoe-dsl-test-metadata-bad-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    write_temp(&dir, "bad.yaml", "- not\n- a\n- mapping\n");

    let src = r#"
storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders" {
  source = "orders_adapter"
}

sink "land" {
  input      = "orders_adapter"
  format     = "parquet"
  base_path  = "s3://canoe-lake"
  dataset_id = "orders"
  metadata   = "bad.yaml"
}
"#;
    let err = canoe_dsl::compile_str_with_base(src, &dir).expect_err("non-mapping YAML must fail to compile");
    let message = format!("{err:#}");
    assert!(message.contains("mapping"), "error should explain the expected shape: {message}");

    std::fs::remove_dir_all(&dir).ok();
}
