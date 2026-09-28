use canoe_config::{CodeSource, StepConfig};
use std::path::PathBuf;

fn load() -> canoe_config::PipelineConfig {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/orders-uv-step.canoe.hcl");
    canoe_dsl::compile_file(&path).expect("orders-uv-step.canoe.hcl should compile")
}

#[test]
fn compiles_file_and_inline_uv_steps_into_one_chain() {
    let config = load();
    assert_eq!(config.chains.len(), 1);
    let chain = &config.chains[0];
    assert_eq!(chain.steps.len(), 4);
    assert_eq!(chain.steps[0].name(), "validate");
    assert_eq!(chain.steps[1].name(), "geocode");
    assert_eq!(chain.steps[2].name(), "dedupe");
    assert_eq!(chain.steps[3].name(), "land");

    let yaml = serde_yaml::to_string(&config).unwrap();
    canoe_config::PipelineConfig::parse(&yaml).expect("compiled config must validate");
}

#[test]
fn uv_step_from_file_resolves_the_external_script() {
    let config = load();
    let StepConfig::Uv(step) = &config.chains[0].steps[1] else {
        panic!("expected geocode to compile to a uv step");
    };
    let CodeSource::Inline { source } = &step.code else {
        panic!("expected inline (resolved-from-file) source");
    };
    assert!(source.contains("# /// script"));
    assert!(source.contains("requests>=2.31"));
    assert!(source.contains("def geocode(city: str)"));
    assert_eq!(step.scaling.max_replicas, 5);
    assert_eq!(step.resources.mem_request, "384Mi");
    assert_eq!(step.env.get("GEOCODE_API_URL").unwrap(), "https://geocode.example.com/v1/search");
    assert_eq!(step.env.get("GEOCODE_TIMEOUT_SECS").unwrap(), "10");
}

#[test]
fn uv_step_from_inline_code_is_used_verbatim() {
    let config = load();
    let StepConfig::Uv(step) = &config.chains[0].steps[2] else {
        panic!("expected dedupe to compile to a uv step");
    };
    let CodeSource::Inline { source } = &step.code else {
        panic!("expected inline source");
    };
    assert!(source.contains("# /// script"));
    assert!(source.contains("SEEN_MAX = 100_000"));
}

#[test]
fn uv_step_topics_chain_correctly() {
    let config = load();
    let chain = &config.chains[0];
    let StepConfig::Python(validate) = &chain.steps[0] else { panic!() };
    let StepConfig::Uv(geocode) = &chain.steps[1] else { panic!() };
    let StepConfig::Uv(dedupe) = &chain.steps[2] else { panic!() };
    assert_eq!(geocode.input_path, validate.output_path);
    assert_eq!(dedupe.input_path, geocode.output_path);
}

#[test]
fn rejects_uv_step_without_script_block() {
    let src = r#"
storage {
  base_path = "s3://bucket"
}
pipeline "p" {
  source = "raw"
}
step "s" {
  engine = "uv"
  input  = "raw"
}
sink "land" {
  input      = "s"
  format     = "parquet"
  base_path  = "s3://bucket"
  dataset_id = "d"
}
"#;
    let err = canoe_dsl::compile_str(src).expect_err("uv step without script should fail");
    let message = format!("{err:#}");
    assert!(message.contains("script"), "error should mention the missing script block: {message}");
}

#[test]
fn rejects_uv_step_combined_with_declarative_ops() {
    let src = r#"
storage {
  base_path = "s3://bucket"
}
pipeline "p" {
  source = "raw"
}
step "s" {
  engine = "uv"
  input  = "raw"
  filter { predicate = "true" }
  script { code = "print('hi')" }
}
sink "land" {
  input      = "s"
  format     = "parquet"
  base_path  = "s3://bucket"
  dataset_id = "d"
}
"#;
    assert!(canoe_dsl::compile_str(src).is_err());
}

#[test]
fn rejects_uv_engine_for_dynamic_route_task() {
    let src = r#"
storage {
  base_path = "s3://bucket"
}
pipeline "p" {
  source = "raw"
}
step "s" {
  engine = "python"
  input  = "raw"
  filter { predicate = "true" }
}
dynamic_route "r" {
  input = "s"
  field = "region"
  task {
    engine = "uv"
    script { code = "print('hi')" }
  }
}
sink "land" {
  input      = "r"
  format     = "parquet"
  base_path  = "s3://bucket"
  dataset_id = "d"
}
"#;
    assert!(canoe_dsl::compile_str(src).is_err());
}
