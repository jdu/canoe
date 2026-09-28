//! canoe-dslc: compiles a canoe DSL pipeline (`.canoe.hcl`) into the
//! `PipelineConfig` YAML `canoe-coordinator` runs. Purely a compile-time
//! tool — the coordinator never parses the DSL itself, only its YAML
//! output (see crates/canoe-dsl for why).

use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "canoe-dslc", about = "Compile a canoe DSL pipeline to YAML")]
struct Cli {
    /// Path to a `.canoe.hcl` pipeline file.
    input: PathBuf,

    /// Where to write the compiled YAML. Defaults to stdout.
    #[arg(short, long)]
    output: Option<PathBuf>,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = canoe_dsl::compile_file(&cli.input)?;
    let yaml = serde_yaml::to_string(&config)?;

    match cli.output {
        Some(path) => std::fs::write(&path, yaml)?,
        None => print!("{yaml}"),
    }
    Ok(())
}
