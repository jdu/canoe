//! canoe-dsl: an HCL-flavored declarative language for canoe pipelines,
//! compiling down to the `canoe_config::PipelineConfig` YAML the
//! coordinator already runs. See `examples/orders.canoe.hcl` for a full
//! pipeline and the crate-level docs in `ast.rs`/`compiler.rs` for how
//! parsing and DAG-to-chain compilation work.
//!
//! ```ignore
//! let src = std::fs::read_to_string("pipeline.canoe.hcl").unwrap();
//! let config = canoe_dsl::compile_str(&src).unwrap();
//! let yaml = serde_yaml::to_string(&config).unwrap(); // any serde_yaml in scope
//! std::fs::write("pipeline.yaml", yaml).unwrap();
//! ```

mod ast;
mod codegen;
mod compiler;
mod expr;

use anyhow::Context;
use canoe_config::PipelineConfig;
use std::path::Path;

/// Compiles DSL source with no known file location, so any `file = ...`
/// escape hatch (a step's `python`/`spark_sql`/`spark_dataframe` block, or
/// an adapter's `script`) resolves relative to the current working
/// directory. Prefer `compile_file` when compiling an actual `.hcl` file
/// on disk — it resolves `file = ...` relative to that file instead,
/// which is almost always what's wanted.
pub fn compile_str(src: &str) -> anyhow::Result<PipelineConfig> {
    let base_dir = std::env::current_dir().unwrap_or_else(|_| Path::new(".").to_path_buf());
    compile_str_with_base(src, &base_dir)
}

/// Same as `compile_str`, but `file = ...` escape hatches resolve
/// relative to `base_dir` instead of the current working directory.
pub fn compile_str_with_base(src: &str, base_dir: &Path) -> anyhow::Result<PipelineConfig> {
    let raw = ast::parse(src)?;
    compiler::compile(&raw, base_dir)
}

pub fn compile_file(path: impl AsRef<Path>) -> anyhow::Result<PipelineConfig> {
    let path = path.as_ref();
    let src = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read DSL file {}", path.display()))?;
    let base_dir = path.parent().unwrap_or_else(|| Path::new("."));
    compile_str_with_base(&src, base_dir)
        .with_context(|| format!("failed to compile DSL file {}", path.display()))
}
