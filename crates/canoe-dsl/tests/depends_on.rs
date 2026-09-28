//! `depends_on`/`depends_on_complete` on `step`/`task` — ordering-only
//! dependencies with two semantics: `depends_on` (applied — reuses
//! `ctx.activated`, `task` already had this, extended to `step` here) and
//! `depends_on_complete` (real completion — reuses `ctx.completed`, only
//! meaningful against a batch-mode step or a task, never a streaming-mode
//! step or `dynamic_route`, which `canoe-dsl` rejects at compile time).

use canoe_config::StepConfig;

#[test]
fn batch_mode_step_can_declare_both_depends_on_and_depends_on_complete() {
    let src = r#"
storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders" {
  source = "orders_adapter"
}

step "warm_cache" {
  engine = "python"
  input  = "orders_adapter"
  depends_on = ["validate"]

  filter { predicate = "true" }
}

step "validate" {
  engine = "python"
  input  = "orders_adapter"
  depends_on_complete = ["warm_cache"]

  filter { predicate = "amount > 0" }
}

sink "land" {
  input      = "validate"
  format     = "parquet"
  base_path  = "s3://canoe-lake-sink"
  dataset_id = "orders"
}

sink "land_warm_cache" {
  input      = "warm_cache"
  format     = "parquet"
  base_path  = "s3://canoe-lake-sink"
  dataset_id = "warm_cache"
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    let chain = config.chains.iter().find(|c| c.steps[0].name() == "warm_cache").unwrap();
    let StepConfig::Python(warm_cache) = &chain.steps[0] else {
        panic!("expected warm_cache to compile to a python step");
    };
    assert_eq!(warm_cache.depends_on, vec!["validate".to_string()]);
    assert!(warm_cache.depends_on_complete.is_empty());

    let chain2 = config.chains.iter().find(|c| c.steps[0].name() == "validate").unwrap();
    let StepConfig::Python(validate) = &chain2.steps[0] else {
        panic!("expected validate to compile to a python step");
    };
    assert!(validate.depends_on.is_empty());
    assert_eq!(validate.depends_on_complete, vec!["warm_cache".to_string()]);
}

#[test]
fn task_can_declare_depends_on_complete_alongside_depends_on() {
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
  filter { predicate = "true" }
}

sink "land" {
  input      = "validate"
  format     = "parquet"
  base_path  = "s3://canoe-lake-sink"
  dataset_id = "orders"
}

task "notify" {
  depends_on          = ["validate"]
  depends_on_complete = ["validate"]

  script {
    code = <<-PY
      # /// script
      # dependencies = []
      # ///
      print("done")
    PY
  }
}
"#;
    let config = canoe_dsl::compile_str(src).expect("should compile");
    assert_eq!(config.tasks.len(), 1);
    assert_eq!(config.tasks[0].depends_on, vec!["validate".to_string()]);
    assert_eq!(config.tasks[0].depends_on_complete, vec!["validate".to_string()]);
}

#[test]
fn depends_on_complete_rejects_a_streaming_mode_target() {
    let src = r#"
storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders" {
  source = "orders_adapter"
}

step "enrich" {
  engine = "spark_sql"
  mode   = "streaming"
  input  = "orders_adapter"

  spark_sql {
    sql = "SELECT * FROM input"
  }
}

step "validate" {
  engine = "python"
  input  = "orders_adapter"
  depends_on_complete = ["enrich"]

  filter { predicate = "true" }
}
"#;
    let err = canoe_dsl::compile_str(src).expect_err("depends_on_complete on a streaming-mode step must fail");
    let message = format!("{err:#}");
    assert!(
        message.contains("enrich") && message.contains("streaming-mode"),
        "error should name the offending step and explain why: {message}"
    );
}

#[test]
fn depends_on_complete_rejects_a_dynamic_route_target() {
    let src = r#"
storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders" {
  source = "orders_adapter"
}

dynamic_route "by_region" {
  input        = "orders_adapter"
  field        = "region"
  max_branches = 4

  task {
    engine = "python"
    entrypoint = "steps.x.run"
    python { code = "def run(r): return r" }
  }
}

step "validate" {
  engine = "python"
  input  = "orders_adapter"
  depends_on_complete = ["by_region"]

  filter { predicate = "true" }
}
"#;
    let err = canoe_dsl::compile_str(src).expect_err("depends_on_complete on a dynamic_route must fail");
    let message = format!("{err:#}");
    assert!(message.contains("by_region"), "error should name the offending dynamic_route: {message}");
}

#[test]
fn depends_on_is_rejected_on_a_streaming_mode_step() {
    let src = r#"
storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders" {
  source = "orders_adapter"
}

step "enrich" {
  engine     = "spark_sql"
  mode       = "streaming"
  input      = "orders_adapter"
  depends_on = ["orders_adapter"]

  spark_sql {
    sql = "SELECT * FROM input"
  }
}
"#;
    let err = canoe_dsl::compile_str(src).expect_err("depends_on on a streaming-mode step must fail");
    let message = format!("{err:#}");
    assert!(message.contains("mode=\"batch\""), "error should explain the batch-mode restriction: {message}");
}

#[test]
fn depends_on_is_rejected_on_a_uv_step() {
    let src = r#"
storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders" {
  source = "orders_adapter"
}

step "script_step" {
  engine     = "uv"
  input      = "orders_adapter"
  depends_on = ["orders_adapter"]

  script {
    code = <<-PY
      # /// script
      # dependencies = []
      # ///
      print("hi")
    PY
  }
}
"#;
    let err = canoe_dsl::compile_str(src).expect_err("depends_on on a uv step must fail");
    let message = format!("{err:#}");
    assert!(message.contains("mode=\"batch\""), "error should explain the batch-mode restriction: {message}");
}

#[test]
fn depends_on_rejects_an_unknown_name() {
    let src = r#"
storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders" {
  source = "orders_adapter"
}

step "validate" {
  engine     = "python"
  input      = "orders_adapter"
  depends_on = ["does_not_exist"]

  filter { predicate = "true" }
}
"#;
    let err = canoe_dsl::compile_str(src).expect_err("depends_on naming an unknown target must fail");
    let message = format!("{err:#}");
    assert!(message.contains("does_not_exist"), "error should name the unknown target: {message}");
}

#[test]
fn depends_on_rejects_the_steps_own_name() {
    let src = r#"
storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders" {
  source = "orders_adapter"
}

step "validate" {
  engine     = "python"
  input      = "orders_adapter"
  depends_on = ["validate"]

  filter { predicate = "true" }
}
"#;
    let err = canoe_dsl::compile_str(src).expect_err("depends_on naming the step's own name must fail");
    let message = format!("{err:#}");
    assert!(message.contains("own name"), "error should explain the self-reference restriction: {message}");
}
