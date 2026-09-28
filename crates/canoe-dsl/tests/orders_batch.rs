use canoe_config::{CodeSource, StepConfig};
use std::path::PathBuf;

#[test]
fn compiles_the_batch_example_and_validates() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/orders-batch.canoe.hcl");
    let config = canoe_dsl::compile_file(&path).expect("orders-batch.canoe.hcl should compile");

    assert_eq!(config.chains.len(), 1);
    let chain = &config.chains[0];
    let StepConfig::Python(step) = &chain.steps[0] else {
        panic!("expected enrich_with_fx_rate to compile to a python step");
    };
    let batch = step.batch.as_ref().expect("batch should be set");
    assert_eq!(batch.size, 500);
    assert_eq!(batch.flush_interval_secs, 5);

    let CodeSource::Inline { source } = &step.code else {
        panic!("expected inline source");
    };
    assert!(source.contains("def run(records: list[dict])"));

    let yaml = serde_yaml::to_string(&config).unwrap();
    canoe_config::PipelineConfig::parse(&yaml).expect("compiled config must validate");
}
