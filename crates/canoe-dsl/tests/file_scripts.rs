use canoe_config::CodeSource;
use std::io::Write;

/// Every case here needs an actual file on disk, resolved relative to a
/// known `base_dir` — `compile_str_with_base` is the entry point that
/// takes one explicitly, so tests don't depend on the process's CWD.
fn write_temp(dir: &std::path::Path, name: &str, contents: &str) {
    let mut f = std::fs::File::create(dir.join(name)).unwrap();
    f.write_all(contents.as_bytes()).unwrap();
}

#[test]
fn adapter_script_can_reference_a_separate_file() {
    let dir = std::env::temp_dir().join(format!("canoe-dsl-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    write_temp(&dir, "charges.py", "# /// script\n# dependencies = []\n# ///\nprint('hi')\n");

    let src = r#"
adapter "stripe_charges" {
  topic = "stripe.charges.raw"
  schedule {
    interval = "5m"
  }
  script {
    file = "charges.py"
  }
}
"#;
    let config = canoe_dsl::compile_str_with_base(src, &dir).expect("should compile");
    let CodeSource::Inline { source } = &config.adapters[0].code else {
        panic!("expected inline (resolved-from-file) source");
    };
    assert!(source.contains("print('hi')"));

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn python_step_can_reference_a_separate_file() {
    let dir = std::env::temp_dir().join(format!("canoe-dsl-test-step-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    write_temp(&dir, "validate.py", "def run(record):\n    return record\n");

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
  python {
    file = "validate.py"
  }
}

sink "land" {
  input      = "validate"
  format     = "parquet"
  base_path  = "s3://bucket"
  dataset_id = "x"
}
"#;
    let config = canoe_dsl::compile_str_with_base(src, &dir).expect("should compile");
    let canoe_config::StepConfig::Python(step) = &config.chains[0].steps[0] else {
        panic!("expected python step");
    };
    let CodeSource::Inline { source } = &step.code else {
        panic!("expected inline (resolved-from-file) source");
    };
    assert_eq!(source, "def run(record):\n    return record\n");

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn missing_file_produces_a_clear_error() {
    let src = r#"
adapter "bad" {
  topic = "x.raw"
  schedule { interval = "5m" }
  script {
    file = "does_not_exist.py"
  }
}
"#;
    let err = canoe_dsl::compile_str(src).expect_err("missing file must fail to compile");
    let message = format!("{err:#}");
    assert!(message.contains("does_not_exist.py"), "error should name the missing file: {message}");
}

#[test]
fn rejects_both_code_and_file_on_same_block() {
    let src = r#"
adapter "bad" {
  topic = "x.raw"
  schedule { interval = "5m" }
  script {
    code = "print('hi')"
    file = "whatever.py"
  }
}
"#;
    assert!(canoe_dsl::compile_str(src).is_err());
}
