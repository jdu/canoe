//! Cross-pipeline `lookup` joins: a `lookup { ... }` block resolves to a
//! literal path via plain string formatting (mirroring `default_path`'s
//! convention), never through this file's own name->path graph — the
//! whole point being that the referenced pipeline/node lives in a
//! *different* `.hcl` file this compiler invocation never parses, so
//! resolution can't (and doesn't need to) depend on it existing at all.

use canoe_config::StepConfig;

#[test]
fn cross_file_lookup_resolves_by_string_formatting_without_the_target_file_ever_being_parsed() {
    // Note there is no second file here at all, let alone one declaring a
    // `customer_pipeline`/`customer_snapshot` node — this compiles cleanly
    // anyway, which is exactly the point: a `lookup` target isn't part of
    // this file's compiled graph, so its existence is never checked here.
    let src = r#"
storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders" {
  source = "orders_adapter"
}

step "join_customer_snapshot" {
  engine = "spark_sql"
  mode   = "batch"
  input  = "orders_adapter"

  lookup "customers" {
    pipeline = "customer_pipeline"
    node     = "customer_snapshot"
  }

  spark_sql {
    sql = "SELECT * FROM input LEFT JOIN customers ON input.customer_id = customers.id"
  }
}

sink "land" {
  input      = "join_customer_snapshot"
  format     = "parquet"
  base_path  = "s3://canoe-lake-sink"
  dataset_id = "orders"
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile even though customer_pipeline is undefined");
    let chain = &config.chains[0];
    let StepConfig::SparkSql(step) = &chain.steps[0] else {
        panic!("expected join_customer_snapshot to compile to a batch-mode spark_sql step");
    };
    assert_eq!(step.lookups.len(), 1);
    assert_eq!(step.lookups[0].name, "customers");
    // <base_path>/<pipeline>/<version>/<node> — the *lookup's own*
    // pipeline/node, defaulted base_path/version from this file's
    // `storage {}` block, exactly `default_path`'s convention.
    assert_eq!(step.lookups[0].path, "s3://canoe-lake/customer_pipeline/v1/customer_snapshot");
}

#[test]
fn multiple_lookups_on_one_step_resolve_independently() {
    let src = r#"
storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders" {
  source = "orders_adapter"
}

step "join_everything" {
  engine = "spark_sql"
  mode   = "batch"
  input  = "orders_adapter"

  lookup "customers" {
    pipeline = "customer_pipeline"
    node     = "customer_snapshot"
  }

  lookup "products" {
    pipeline  = "catalog_pipeline"
    node      = "product_snapshot"
    base_path = "s3://other-team-lake"
    version   = "v3"
  }

  spark_sql {
    sql = "SELECT * FROM input LEFT JOIN customers ON true LEFT JOIN products ON true"
  }
}

sink "land" {
  input      = "join_everything"
  format     = "parquet"
  base_path  = "s3://canoe-lake-sink"
  dataset_id = "orders"
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    let chain = &config.chains[0];
    let StepConfig::SparkSql(step) = &chain.steps[0] else {
        panic!("expected join_everything to compile to a batch-mode spark_sql step");
    };
    assert_eq!(step.lookups.len(), 2);

    let customers = step.lookups.iter().find(|l| l.name == "customers").expect("customers lookup");
    assert_eq!(customers.path, "s3://canoe-lake/customer_pipeline/v1/customer_snapshot");

    // Explicit base_path/version override this file's own storage {} block.
    let products = step.lookups.iter().find(|l| l.name == "products").expect("products lookup");
    assert_eq!(products.path, "s3://other-team-lake/catalog_pipeline/v3/product_snapshot");
}

#[test]
fn lookup_without_base_path_or_a_storage_block_is_a_clear_compile_error() {
    let src = r#"
pipeline "orders" {
  source = "orders_adapter"
}

step "join_customer_snapshot" {
  engine = "spark_sql"
  input  = "orders_adapter"

  lookup "customers" {
    pipeline = "customer_pipeline"
    node     = "customer_snapshot"
  }

  spark_sql {
    sql = "SELECT * FROM input"
  }
}
"#;
    let err = canoe_dsl::compile_str(src).expect_err("lookup with no base_path and no storage block should fail");
    let message = format!("{err:#}");
    assert!(message.contains("base_path"), "error should mention the missing base_path: {message}");
}

#[test]
fn python_step_can_declare_lookups_too() {
    let src = r#"
storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders" {
  source = "orders_adapter"
}

step "enrich" {
  engine = "python"
  input  = "orders_adapter"

  lookup "customers" {
    pipeline = "customer_pipeline"
    node     = "customer_snapshot"
  }

  filter { predicate = "amount > 0" }
}

sink "land" {
  input      = "enrich"
  format     = "parquet"
  base_path  = "s3://canoe-lake-sink"
  dataset_id = "orders"
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    let chain = &config.chains[0];
    let StepConfig::Python(step) = &chain.steps[0] else {
        panic!("expected enrich to compile to a python step");
    };
    assert_eq!(step.lookups.len(), 1);
    assert_eq!(step.lookups[0].path, "s3://canoe-lake/customer_pipeline/v1/customer_snapshot");
}

#[test]
fn dynamic_route_task_rejects_a_lookup_block() {
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

    lookup "customers" {
      pipeline = "customer_pipeline"
      node     = "customer_snapshot"
    }

    filter { predicate = "true" }
  }
}
"#;
    let err = canoe_dsl::compile_str(src).expect_err("dynamic_route task with a lookup block should be rejected");
    let message = format!("{err:#}");
    assert!(message.contains("lookup"), "error should mention lookup: {message}");
}
