use canoe_config::{DynamicTaskTemplate, StepConfig};
use std::path::PathBuf;

fn load() -> canoe_config::PipelineConfig {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/orders-dynamic.canoe.hcl");
    canoe_dsl::compile_file(&path).expect("orders-dynamic.canoe.hcl should compile")
}

#[test]
fn compiles_to_a_single_chain_with_a_dynamic_route_step() {
    let config = load();
    assert_eq!(config.chains.len(), 1, "no static branches to duplicate across");
    let chain = &config.chains[0];
    assert_eq!(chain.steps.len(), 3);
    assert_eq!(chain.steps[0].name(), "validate");
    assert_eq!(chain.steps[1].name(), "split_by_region");
    assert_eq!(chain.steps[2].name(), "land");

    // canoe-config's own adjacency validation is the real proof this is
    // wired correctly end to end.
    let yaml = serde_yaml::to_string(&config).unwrap();
    canoe_config::PipelineConfig::parse(&yaml).expect("compiled config must validate");
}

#[test]
fn dynamic_route_step_carries_field_cap_and_templates() {
    let config = load();
    let StepConfig::DynamicRoute(dr) = &config.chains[0].steps[1] else {
        panic!("expected split_by_region to compile to a dynamic_route step");
    };
    assert_eq!(dr.field, "region");
    assert_eq!(dr.max_branches, 50);
    // "validate" is a `python` (S3-transport) step, so its resolved
    // output is an S3 path, not a Kafka topic — see the module doc on
    // `crates/canoe-config/src/lib.rs`'s `StepConfig::input_ref` for why
    // this doesn't fail compilation even though `dynamic_route` itself
    // stays Kafka-topic-based.
    assert_eq!(dr.input_topic, "s3://canoe-lake/orders_dynamic/v1/validate");
    assert_eq!(dr.output_topic, "_canoe.split_by_region.merged");

    let DynamicTaskTemplate::SparkSql { sql, .. } = &dr.task else {
        panic!("expected task to compile to a spark_sql template");
    };
    assert!(sql.contains("region AS region_tag"));

    let overflow = dr.overflow.as_ref().expect("overflow should be set");
    let DynamicTaskTemplate::Python { code: canoe_config::CodeSource::Inline { source }, .. } = overflow else {
        panic!("expected overflow to compile to an inline python template");
    };
    assert!(source.contains("region_tag"));
    assert!(source.contains("unclassified"));
}

#[test]
fn sink_reads_directly_from_the_dynamic_route_by_name() {
    let config = load();
    let StepConfig::Sink(sink) = &config.chains[0].steps[2] else {
        panic!("expected land to compile to a sink step");
    };
    let StepConfig::DynamicRoute(dr) = &config.chains[0].steps[1] else {
        panic!("expected dynamic_route step");
    };
    assert_eq!(sink.input_path, dr.output_topic, "sink's `input = \"split_by_region\"` must resolve to the dynamic_route's merged output topic");
}

#[test]
fn defaults_max_branches_when_omitted() {
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
    engine = "python"
    filter { predicate = "true" }
  }
}
sink "land" {
  input      = "r"
  format     = "parquet"
  base_path  = "s3://bucket"
  dataset_id = "d"
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    let StepConfig::DynamicRoute(dr) = &config.chains[0].steps[1] else {
        panic!("expected dynamic_route step");
    };
    assert_eq!(dr.max_branches, 1024);
    assert!(dr.overflow.is_none());
}

#[test]
fn rejects_dynamic_route_without_task_block() {
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
