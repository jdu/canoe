//! Phase 1 verification: a `source` -> `python` step -> `sink` pipeline
//! with a `storage {}` block resolves `input`/`input_path`/`input_signal`/
//! `output_path` exactly as README.md's "S3 + signal-driven pipeline
//! transport" describes.

use canoe_config::StepConfig;

fn compile() -> canoe_config::PipelineConfig {
    let src = r#"
storage {
  base_path = "s3://canoe-lake"
  version   = "v2"
}

pipeline "orders" {
  source = "orders_adapter"
}

step "validate" {
  engine = "python"
  input  = "orders_adapter"
  filter { predicate = "amount > 0" }

  output {
    batch_size = 500
    flush_interval_secs = 10
    partition_by = ["dt"]
  }
}

sink "land" {
  input      = "validate"
  format     = "parquet"
  base_path  = "s3://canoe-lake-sink"
  dataset_id = "orders"
}
"#;
    canoe_dsl::compile_str(src).expect("should compile")
}

#[test]
fn resolves_input_and_output_paths_under_storage_base_path_and_version() {
    let config = compile();
    let chain = &config.chains[0];
    let StepConfig::Python(validate) = &chain.steps[0] else {
        panic!("expected validate to compile to a python step");
    };

    // `input` stays the unresolved producer name (not a path) — the
    // pipeline's `source` in this case, since nothing upstream produces it.
    assert_eq!(validate.input, "orders_adapter");
    // The source is external (not a known step/route/dynamic_route name in
    // this file), so it falls through as a literal string — this is the
    // documented Phase 1 stand-in for an adapter that hasn't migrated to
    // S3 + signal transport yet (see the plan's Phase 2 notes).
    assert_eq!(validate.input_path, "orders_adapter");
    assert_eq!(validate.input_signal, "orders_adapter");

    assert_eq!(validate.output_path, "s3://canoe-lake/orders/v2/validate");
    assert_eq!(validate.output.batch_size, 500);
    assert_eq!(validate.output.flush_interval_secs, 10);
    assert_eq!(validate.output.partition_by, vec!["dt".to_string()]);
}

#[test]
fn sink_resolves_input_to_the_producing_steps_own_output_path() {
    let config = compile();
    let chain = &config.chains[0];
    let StepConfig::Sink(sink) = &chain.steps[1] else {
        panic!("expected land to compile to a sink step");
    };
    assert_eq!(sink.input, "validate");
    assert_eq!(sink.input_path, "s3://canoe-lake/orders/v2/validate");
    assert_eq!(sink.input_signal, "validate");
}

#[test]
fn missing_storage_block_is_a_clear_compile_error_for_an_s3_transport_step() {
    let src = r#"
pipeline "p" {
  source = "raw"
}
step "s" {
  engine = "python"
  input  = "raw"
  filter { predicate = "true" }
}
sink "land" {
  input      = "s"
  format     = "parquet"
  base_path  = "s3://bucket"
  dataset_id = "d"
}
"#;
    let err = canoe_dsl::compile_str(src).expect_err("python step without a storage block should fail");
    let message = format!("{err:#}");
    assert!(message.contains("storage"), "error should mention the missing storage block: {message}");
}
