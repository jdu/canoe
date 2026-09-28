use canoe_config::{SinkConfig, StepConfig};
use std::path::PathBuf;

fn load() -> canoe_config::PipelineConfig {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples/orders.canoe.hcl");
    canoe_dsl::compile_file(&path).expect("orders.canoe.hcl should compile")
}

#[test]
fn compiled_output_passes_canoe_config_validation() {
    // canoe-config's own loader re-validates topic adjacency end to end;
    // round-tripping through YAML is the same path canoe-dslc's output
    // takes before a human ever hands it to the coordinator.
    let config = load();
    let yaml = serde_yaml::to_string(&config).unwrap();
    canoe_config::PipelineConfig::parse(&yaml).expect("compiled config must pass canoe-config validation");
}

#[test]
fn compiles_branching_pipeline_into_three_chains() {
    let config = load();

    // One path per region branch: source -> validate -> branch filter ->
    // regional enrich -> sink. All three converge on the same sink input
    // topic because of the `merge` block.
    assert_eq!(config.chains.len(), 3);
    for chain in &config.chains {
        assert_eq!(chain.source, "orders.raw");
        assert_eq!(chain.steps.len(), 4);
        assert_eq!(chain.steps[0].name(), "validate");
        assert!(chain.steps[1].name().starts_with("split_by_region__"));
        assert!(matches!(chain.steps[2], StepConfig::KafkaSparkSql(_)));
        assert_eq!(chain.steps[3].name(), "land");
    }

    let merge_topics: std::collections::HashSet<&str> = config
        .chains
        .iter()
        .map(|c| c.steps[3].input_ref())
        .collect();
    assert_eq!(merge_topics.len(), 1, "all branches must merge into one sink input topic");

    let branch_names: std::collections::HashSet<&str> =
        config.chains.iter().map(|c| c.steps[1].name()).collect();
    assert_eq!(
        branch_names,
        std::collections::HashSet::from([
            "split_by_region__us",
            "split_by_region__eu",
            "split_by_region__default",
        ])
    );
}

#[test]
fn validate_step_compiles_declarative_ops_to_python() {
    let config = load();
    let chain = &config.chains[0];
    let StepConfig::Python(step) = &chain.steps[0] else {
        panic!("expected validate to compile to a python step");
    };
    let canoe_config::CodeSource::Inline { source } = &step.code else {
        panic!("expected inline generated source");
    };
    assert!(source.contains("def run(record):"));
    assert!(source.contains("eval(\"amount > 0\""));
    assert!(source.contains("record[\"customer_id\"] = record.pop(\"cust_id\")"));
    assert!(source.contains("record.pop(\"internal_debug_flag\", None)"));
    assert_eq!(step.scaling.min_replicas, 2);
    assert_eq!(step.scaling.max_replicas, 20);
}

#[test]
fn validate_step_resolves_local_map_values_mapping_file() {
    let config = load();
    let StepConfig::Python(step) = &config.chains[0].steps[0] else {
        panic!("expected validate to compile to a python step");
    };
    let canoe_config::CodeSource::Inline { source } = &step.code else {
        panic!("expected inline generated source");
    };
    // examples/mappings/payment_method.json read at compile time and
    // embedded as a literal dict, not left as a runtime file reference.
    assert!(source.contains("import json"));
    assert!(source.contains("credit_card"));
    assert!(source.contains("bank_transfer"));
    assert!(source.contains(
        "record[\"payment_method\"] = _MAP_0.get(record.get(\"payment_method_code\"), \"other\")"
    ));
}

#[test]
fn route_branches_compile_to_filter_only_python_steps() {
    let config = load();
    let us_chain = config
        .chains
        .iter()
        .find(|c| c.steps[1].name() == "split_by_region__us")
        .expect("a chain routed through the us branch");

    let StepConfig::Python(branch) = &us_chain.steps[1] else {
        panic!("expected route branch to compile to a python step");
    };
    let canoe_config::CodeSource::Inline { source } = &branch.code else {
        panic!("expected inline generated source");
    };
    assert!(source.contains("region == 'US'"));

    let default_chain = config
        .chains
        .iter()
        .find(|c| c.steps[1].name() == "split_by_region__default")
        .expect("a chain routed through the default branch");
    let StepConfig::Python(default_branch) = &default_chain.steps[1] else {
        panic!("expected default route branch to compile to a python step");
    };
    let canoe_config::CodeSource::Inline { source } = &default_branch.code else {
        panic!("expected inline generated source");
    };
    assert!(source.contains("not (region == 'US' or region == 'EU')"));
}

#[test]
fn enrich_steps_compile_to_spark_sql_with_field_ctes() {
    let config = load();
    let us_chain = config
        .chains
        .iter()
        .find(|c| c.steps[1].name() == "split_by_region__us")
        .unwrap();
    // No `mode` declared in the fixture, so this defaults to
    // mode="streaming" (spark_sql's pre-existing-behavior default) —
    // KafkaSparkSql, not the new batch-mode SparkSql.
    let StepConfig::KafkaSparkSql(enrich) = &us_chain.steps[2] else {
        panic!("expected enrich_us to compile to a streaming-mode (kafka_spark_sql) step");
    };
    assert_eq!(enrich.name, "enrich_us");
    assert!(enrich.sql.contains("0.07 AS region_tax_rate"));
    assert!(enrich.sql.contains("date_format(current_timestamp(), 'yyyy-MM-dd') AS processed_date"));

    // Regression: merge aliasing must update the *step's own* output_topic
    // field, not just internal graph-traversal state, or the emitted YAML
    // fails canoe-config's adjacency validation (output_topic of one step
    // must equal input_path of the next).
    let StepConfig::Sink(sink) = &us_chain.steps[3] else {
        panic!("expected sink step");
    };
    assert_eq!(enrich.output_topic, sink.input_path);
}

#[test]
fn sink_resolves_to_parquet_with_configured_path() {
    let config = load();
    let StepConfig::Sink(sink) = &config.chains[0].steps[3] else {
        panic!("expected land to compile to a sink step");
    };
    match &sink.sink {
        SinkConfig::Parquet { base_path, dataset_id, partition_by, .. } => {
            assert_eq!(base_path, "s3://canoe-lake");
            assert_eq!(dataset_id, "orders");
            assert_eq!(partition_by, &vec!["processed_date".to_string()]);
        }
        other => panic!("expected parquet sink, got {other:?}"),
    }
}
