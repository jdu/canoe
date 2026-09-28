use canoe_config::StepConfig;

#[test]
fn env_block_compiles_onto_a_python_step() {
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

  env {
    REGION      = "us-east-1"
    RETRY_LIMIT = 3
    DEBUG       = true
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
        panic!("expected validate to compile to a python step");
    };
    assert_eq!(step.env.get("REGION").unwrap(), "us-east-1");
    assert_eq!(step.env.get("RETRY_LIMIT").unwrap(), "3");
    assert_eq!(step.env.get("DEBUG").unwrap(), "true");

    let yaml = serde_yaml::to_string(&config).unwrap();
    canoe_config::PipelineConfig::parse(&yaml).expect("compiled config must validate");
}

#[test]
fn env_block_compiles_onto_a_uv_step() {
    let src = r##"
storage {
  base_path = "s3://bucket"
}
pipeline "orders" {
  source        = "orders.raw"
}

step "geocode" {
  engine = "uv"
  input  = "orders.raw"

  script {
    code = "# /// script\n# dependencies = []\n# ///\nprint('hi')"
  }

  env {
    API_KEY = "secret123"
  }
}

sink "land" {
  input      = "geocode"
  format     = "parquet"
  base_path  = "s3://bucket"
  dataset_id = "orders"
}
"##;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    let StepConfig::Uv(step) = &config.chains[0].steps[0] else {
        panic!("expected geocode to compile to a uv step");
    };
    assert_eq!(step.env.get("API_KEY").unwrap(), "secret123");
}

#[test]
fn env_is_optional_and_defaults_to_empty() {
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
    assert!(step.env.is_empty());
}

#[test]
fn rejects_env_key_using_the_reserved_canoe_prefix() {
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
  env {
    CANOE_INPUT_TOPIC = "hijack"
  }
}
sink "land" {
  input      = "validate"
  format     = "parquet"
  base_path  = "s3://bucket"
  dataset_id = "orders"
}
"#;
    let err = canoe_dsl::compile_str(src).expect_err("should reject reserved CANOE_ prefix");
    assert!(err.to_string().contains("reserved") || format!("{err:#}").contains("reserved"));
}

#[test]
fn rejects_env_on_a_spark_sql_step() {
    let src = r#"
pipeline "orders" {
  source        = "orders.raw"
}
step "enrich" {
  engine = "spark_sql"
  input  = "orders.raw"
  with_field {
    name       = "processed"
    expression = "true"
  }
  env {
    REGION = "us-east-1"
  }
}
sink "land" {
  input      = "enrich"
  format     = "parquet"
  base_path  = "s3://bucket"
  dataset_id = "orders"
}
"#;
    let err = canoe_dsl::compile_str(src).expect_err("should reject env on spark_sql");
    let msg = format!("{err:#}");
    assert!(msg.contains("env"), "unexpected error: {msg}");
}
