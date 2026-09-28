//! Parses the canoe pipeline DSL (an HCL dialect) into a raw, order-preserving
//! tree. This layer only understands HCL syntax and this DSL's block/attribute
//! names — it does no topic resolution or codegen; see `compiler.rs` for that.

use anyhow::{bail, Context};
use hcl::{Block, Body, Structure, Value};
use std::collections::BTreeMap;
use std::path::Path;

/// Every block in this DSL is now a discrete, root-level declaration —
/// `pipeline`, `step`, `route`, `dynamic_route`, `merge`, `sink`, `task`,
/// `adapter` — that references others *by name* (`input = "..."`,
/// `depends_on = [...]`), Terraform-resource style, rather than being
/// nested inside a `pipeline` block. `compiler.rs` discovers which
/// step/route/merge/sink belong to which pipeline by tracing every
/// `input` reference reachable from that pipeline's `source_topic` —
/// there's no explicit membership list anywhere.
pub struct RawFile {
    pub pipelines: Vec<RawPipeline>,
    pub steps: Vec<RawStep>,
    pub routes: Vec<RawRoute>,
    pub dynamic_routes: Vec<RawDynamicRoute>,
    pub merges: Vec<RawMerge>,
    pub sinks: Vec<RawSink>,
    /// One-shot, run-after-a-dependency tasks — see `RawTask`.
    pub tasks: Vec<RawTask>,
    pub adapters: Vec<RawAdapter>,
    /// At most one top-level `topics { ... }` block — global to the
    /// whole file, not per-pipeline. See `RawTopics`.
    pub topics: Option<RawTopics>,
    /// At most one top-level `storage { ... }` block — global to the
    /// whole file, not per-pipeline. See `RawStorage`.
    pub storage: Option<RawStorage>,
}

/// A top-level `storage { base_path = "..."; version = "..." }` block: the
/// S3 root every S3-transport node (a `python`/`uv` step, or a sink's own
/// producer reference) resolves its path beneath. File-level rather than
/// per-`pipeline` deliberately: `compiler::compile_graph` builds every
/// node's path once, globally, before it knows which pipeline(s) can
/// reach that node, so a per-pipeline setting would be ambiguous for a
/// node reachable from more than one pipeline. Required only if the file
/// declares at least one S3-transport step; a Spark/dynamic_route-only
/// file needs no `storage` block at all.
pub struct RawStorage {
    pub base_path: String,
    pub version: Option<String>,
}

/// A top-level `topics { ... }` block: retention and archival policy for
/// every internal chain-wiring topic across every `pipeline` in this
/// file (a step's output topic — never a pipeline's external
/// `source_topic`, which canoe doesn't consider its own to manage). See
/// `compiler::compile_pipeline`'s `managed_topics` collection for how
/// "every internal topic" gets enumerated, and
/// `crates/canoe-coordinator/src/retention.rs`/`archiver.rs` for what
/// the coordinator does with this at run time.
pub struct RawTopics {
    pub retention: Option<RawRetention>,
    pub archive: Option<RawArchive>,
}

/// `duration` is a human duration like `"7d"`/`"168h"`, parsed via
/// `humantime` at compile time into milliseconds (matching
/// `AdapterSchedule.interval_secs`'s "author human units, compile down to
/// a plain number" convention).
pub struct RawRetention {
    pub duration: String,
}

pub struct RawArchive {
    /// Object store URI every managed topic's archive dumps are rooted
    /// under, e.g. `s3://bucket/prefix`.
    pub base_path: String,
    /// How often each managed topic's archiver flushes. Human duration,
    /// same convention as `RawRetention::duration`.
    pub interval: String,
}

/// Source for any escape-hatch/script block (`python`, `spark_sql`,
/// `spark_dataframe`, an adapter's `script`): either literal inline text,
/// or a path to a file the compiler reads at compile time. `File` paths
/// are resolved relative to the `.hcl` source file's own directory (see
/// `compiler::compile`'s `base_dir`) — there's no runtime file resolution
/// involved; by the time this reaches `PipelineConfig` it's always
/// `CodeSource::Inline`, same as every other DSL-generated step.
pub enum RawCode {
    Inline(String),
    File(String),
}

impl RawCode {
    /// Reads the file (relative to `base_dir`) if this is `File`, or
    /// returns the inline text as-is.
    pub fn resolve(&self, base_dir: &Path) -> anyhow::Result<String> {
        match self {
            RawCode::Inline(s) => Ok(s.clone()),
            RawCode::File(rel_path) => {
                let full_path = base_dir.join(rel_path);
                std::fs::read_to_string(&full_path).with_context(|| {
                    format!("failed to read file \"{rel_path}\" (resolved to {})", full_path.display())
                })
            }
        }
    }
}

/// Parses a `code`/`sql`/`file`-style block: exactly one of the inline
/// attribute (named `inline_attr`, e.g. `"code"` or `"sql"`) or `file`
/// must be set.
fn parse_code_block(body: &Body, ctx: &str, inline_attr: &str) -> anyhow::Result<RawCode> {
    match (attr_str(body, inline_attr), attr_str(body, "file")) {
        (Some(inline), None) => Ok(RawCode::Inline(inline)),
        (None, Some(file)) => Ok(RawCode::File(file)),
        (Some(_), Some(_)) => bail!("{ctx}: set exactly one of `{inline_attr}` or `file`, got both"),
        (None, None) => bail!("{ctx}: missing required `{inline_attr}` or `file` attribute"),
    }
}

/// Where a `map_values` op's lookup table comes from: literal JSON text,
/// a local file (adjacent to the `.hcl` script, resolved by
/// `canoe-dslc` at compile time — same as `RawCode::File`), or an S3
/// object (resolved at *run time* by the Python worker pod instead,
/// since a compiling machine may have neither network access nor the
/// right IAM role to fetch it, and a mapping file may be updated
/// independently of the pipeline that reads it). See
/// `compiler::resolve_mapping_json` for how `Inline`/`File` get turned
/// into an actual `serde_json::Map` at compile time, and
/// `codegen::python`'s `MapValues` arm for how `S3` instead compiles to
/// a runtime fetch via `canoe_worker.mapping.load_json_mapping`.
pub enum RawMappingSource {
    Inline(String),
    File(String),
    S3 { bucket: String, key: String },
}

fn parse_mapping_block(body: &Body, ctx: &str) -> anyhow::Result<RawMappingSource> {
    let code = attr_str(body, "code");
    let file = attr_str(body, "file");
    let bucket = attr_str(body, "bucket");
    let key = attr_str(body, "key");
    match (code, file, bucket, key) {
        (Some(c), None, None, None) => Ok(RawMappingSource::Inline(c)),
        (None, Some(f), None, None) => Ok(RawMappingSource::File(f)),
        (None, None, Some(bucket), Some(key)) => Ok(RawMappingSource::S3 { bucket, key }),
        (None, None, Some(_), None) | (None, None, None, Some(_)) => {
            bail!("{ctx}: an S3 mapping source needs both `bucket` and `key`")
        }
        (None, None, None, None) => {
            bail!("{ctx}: `mapping` block needs one of `code`, `file`, or `bucket`+`key`")
        }
        _ => bail!("{ctx}: `mapping` block must set exactly one source (`code`, `file`, or `bucket`+`key`)"),
    }
}

/// An `adapter "name" { ... }` block: a scheduled, standalone `uv run`
/// Python script that publishes to `topic`. Not part of any pipeline —
/// its output topic just happens to be what some pipeline's
/// `source_topic` reads.
pub struct RawAdapter {
    pub name: String,
    /// `mode = "streaming"` (default) or `"batch"` — same vocabulary as
    /// `RawStep::mode`. Streaming requires `topic`, rejects `output`;
    /// batch rejects `topic`, requires `output`. See `compiler.rs`'s
    /// `compile_adapter`.
    pub mode: Option<String>,
    /// Required for `mode = "streaming"` (the default), rejected for
    /// `mode = "batch"` — see `mode`'s docs.
    pub topic: Option<String>,
    /// Only valid for `mode = "batch"` — batch_size/flush_interval_secs/
    /// partition_by for the adapter's S3 writes, same shape as a step's
    /// `output {}` block. See `RawOutput`.
    pub output: Option<RawOutput>,
    /// PEP 723 script source (`# /// script` header + body), inline or a
    /// file; the coordinator runs it as `uv run <file>`, letting `uv`
    /// resolve the header's own `dependencies` into an ephemeral venv per
    /// run.
    pub script: RawCode,
    pub schedule: RawAdapterSchedule,
    pub resources: Option<RawResources>,
    pub backoff_limit: Option<u64>,
    /// Zero or more `encrypt { fields = [...]; key = "..." }` blocks — see
    /// `RawEncrypt`. Not automatic for an adapter (script owns its whole
    /// loop) — canoe only resolves the named key(s) into env vars; see
    /// `canoe_config::AdapterConfig::encrypt`'s docs.
    pub encrypt: Vec<RawEncrypt>,
    /// `secrets = ["name", ...]` — AWS Secrets Manager entries to mount
    /// as files in this adapter's Job, via the Secrets Store CSI Driver.
    /// Unlike `encrypt`, this *is* fully automatic even for an adapter's
    /// self-owned script — see `canoe_config::AdapterConfig::secrets`'s docs.
    pub secrets: Vec<String>,
}

/// Exactly one of `interval`/`cron` is required — enforced in
/// `compiler.rs` (`interval` here is a human duration like `"5m"`/`"1h30m"`,
/// parsed via `humantime` into the `interval_secs` the coordinator uses;
/// `cron` is a standard 5-field expression evaluated in UTC).
#[derive(Default)]
pub struct RawAdapterSchedule {
    pub interval: Option<String>,
    pub cron: Option<String>,
}

/// Just the entry point and activation policy — `compiler.rs` discovers
/// this pipeline's steps/routes/merges/sinks by tracing `input`
/// references reachable from `source_topic` through the file's root-level
/// blocks (see `RawFile`'s docs).
pub struct RawPipeline {
    pub name: String,
    pub source: String,
    /// How this pipeline starts, if not just continuously from coordinator
    /// startup — see `RawSchedule`.
    pub schedule: Option<RawSchedule>,
}

/// A `task "name" { ... }` block: a one-shot side-effecting script (call a
/// service, send a notification) that runs once its `depends_on` names
/// are all satisfied, rather than on a `schedule` (like an adapter) or by
/// consuming a Kafka topic (like a step). Not part of any pipeline's data
/// flow — it has no `input`/output topic of its own.
///
/// `depends_on` may name a step, a pipeline, or another task:
/// - a **step** or **pipeline** dependency is satisfied once the
///   coordinator has applied that step's Deployment (or, for a pipeline,
///   every step in it) — not once it's "done," since a step's Deployment
///   runs forever. See `crates/canoe-coordinator/src/tasks.rs`.
/// - a **task** dependency is satisfied once that task's one-shot Job has
///   actually completed successfully — a real completion dependency,
///   forming a task DAG.
pub struct RawTask {
    pub name: String,
    pub script: RawCode,
    pub depends_on: Vec<String>,
    /// Real completion gate — see `canoe_config::TaskConfig::depends_on_complete`.
    pub depends_on_complete: Vec<String>,
    pub resources: Option<RawResources>,
    pub backoff_limit: Option<u64>,
    /// `secrets = ["name", ...]` — see `RawAdapter::secrets`'s docs (same
    /// mechanism).
    pub secrets: Vec<String>,
}

/// A `dynamic_route "name" { ... }` block — see
/// `canoe_config::DynamicRouteStep` for the full runtime semantics this
/// compiles to. Unlike `route`, there's no static `branch` list: `task`
/// is a template applied to every value the coordinator discovers at run
/// time, and `overflow` (if set) is a second template provisioned once,
/// up front, for anything past `max_branches` distinct values.
pub struct RawDynamicRoute {
    pub name: String,
    pub input: String,
    pub field: String,
    pub max_branches: Option<u32>,
    pub task: RawDynamicTask,
    pub overflow: Option<RawDynamicTask>,
}

/// Same shape as a `step` block's body (`engine` + declarative ops/escape
/// hatch + `scaling`/`resources`/`spark`), just without its own `name`/
/// `input` — those come from the `dynamic_route` (`overflow` shares
/// `task`'s topic-naming, just always-on instead of discovered).
pub struct RawDynamicTask {
    pub engine: String,
    pub ops: Vec<RawOp>,
    pub scaling: Option<RawScaling>,
    pub resources: Option<RawResources>,
    pub spark: Option<RawSparkConf>,
    /// Parsed the same as `RawStep::env`, but not supported for
    /// dynamic_route tasks/overflow yet — `compiler.rs` rejects a
    /// non-empty one, same as `engine = "uv"` there.
    pub env: BTreeMap<String, String>,
    /// Parsed the same as `RawStep::batch`, but not supported for
    /// dynamic_route tasks/overflow yet — `compiler.rs` rejects a `Some`
    /// one, same reasoning as `env` above.
    pub batch: Option<RawBatch>,
    /// Parsed the same as `RawStep::output`, but not supported for
    /// dynamic_route tasks/overflow — `dynamic_route` stays Kafka-topic
    /// transport, so there's no S3 output to configure; `compiler.rs`
    /// rejects a `Some` one, same reasoning as `env`/`batch` above.
    pub output: Option<RawOutput>,
}

/// A pipeline-level `schedule { ... }` block.
pub struct RawSchedule {
    /// Activate this pipeline when a sink elsewhere names this signal —
    /// see `RawSink::signal`.
    pub signal: Option<String>,
    /// Reserved for future cron-based scheduling; parsed but not yet
    /// acted on by the coordinator.
    pub cron: Option<String>,
}

pub struct RawStep {
    pub name: String,
    /// "python" | "spark_sql" | "spark_dataframe"
    pub engine: String,
    /// "batch" | "streaming" — orthogonal to `engine`. `batch`: S3+signal
    /// transport, one-shot K8s Job (drains currently-available backlog,
    /// exits — Job completion is a free, native "done" signal). `streaming`:
    /// Kafka-topic transport, a continuous process (a persistent Pod for
    /// `python`, `spark.readStream` for Spark). Defaults to `"batch"` for
    /// `python`/`uv` (already S3-transport before this attribute existed)
    /// and `"streaming"` for `spark_sql`/`spark_dataframe` (already
    /// Kafka-transport, and Spark's only option before this attribute
    /// existed) — see `compiler.rs::resolve_mode`. `uv` has no streaming
    /// option (out of scope — it already owns its entire loop).
    pub mode: Option<String>,
    /// Name of another step/route-branch/merge, or a literal Kafka topic.
    pub input: String,
    /// Declarative ops in declaration order.
    pub ops: Vec<RawOp>,
    pub scaling: Option<RawScaling>,
    pub resources: Option<RawResources>,
    pub spark: Option<RawSparkConf>,
    /// An `env { KEY = "value", ... }` block — user config passed through
    /// to the step's pod as plain env vars. Only meaningful for
    /// `engine = "python"`/`"uv"` today; `compiler.rs` rejects a non-empty
    /// one on any other engine rather than silently dropping it.
    pub env: BTreeMap<String, String>,
    /// A `batch { size = ..., flush_interval_secs = ... }` block — see
    /// `canoe_config::BatchConfig`. Only meaningful for a `python { code =
    /// ... }` escape-hatch step; `compiler.rs` rejects it on declarative
    /// ops or any other engine.
    pub batch: Option<RawBatch>,
    /// An `output { batch_size = ...; flush_interval_secs = ...;
    /// partition_by = [...] }` block — see `canoe_config::OutputConfig`.
    /// Only meaningful for `engine = "python"`/`"uv"` steps (the
    /// S3-transport engines); `compiler.rs` rejects it on any other
    /// engine.
    pub output: Option<RawOutput>,
    /// Zero or more `lookup "name" { ... }` blocks — a snapshot join
    /// against a node declared in a *different* `.hcl` file (possibly a
    /// different deployment entirely), read via that node's `latest.json`
    /// pointer at run time rather than tracked incrementally. See
    /// `RawLookup`/`canoe_config::LookupConfig`. Step-only: `dynamic_route`
    /// task/overflow bodies reject a `lookup` block outright (see
    /// `parse_dynamic_task`) since those stay on the Kafka-topic transport.
    pub lookups: Vec<RawLookup>,
    /// Zero or more `encrypt { fields = [...]; key = "..." }` blocks (no
    /// label, unlike `lookup` — multiple are just distinguished by their
    /// own `fields`/`key`) — see `RawEncrypt`. Rejected on `dynamic_route`
    /// task/overflow bodies, same as `lookup` (see `parse_dynamic_task`).
    pub encrypt: Vec<RawEncrypt>,
    /// `secrets = ["name", ...]` — see `RawAdapter::secrets`'s docs (same
    /// mechanism). Rejected on `dynamic_route` task/overflow bodies, same
    /// as `lookup`/`encrypt` (see `parse_dynamic_task`).
    pub secrets: Vec<String>,
    /// `depends_on = ["name", ...]` — ordering-only dependency (no data
    /// consumption), same mechanism `RawTask::depends_on` already has,
    /// extended here from `task`-only to any step. Only meaningful for a
    /// batch-mode step today (`compiler.rs` rejects a non-empty value on
    /// `uv` or a streaming-mode step).
    pub depends_on: Vec<String>,
    /// `depends_on_complete = ["name", ...]` — same as `depends_on`, but
    /// a real *completion* gate (see `canoe_config::TaskConfig::depends_on_complete`).
    /// `compiler.rs` additionally rejects an entry naming a streaming-mode
    /// step or `dynamic_route` — neither ever "completes."
    pub depends_on_complete: Vec<String>,
}

/// `lookup "name" { pipeline = "..."; node = "..."; base_path = "...";
/// version = "..." }` — `base_path`/`version` are optional and default to
/// the file's own top-level `storage {}` block when omitted (the common
/// case: same lake, different pipeline); set them explicitly to reach a
/// genuinely different deployment/bucket. Resolved to a literal path by
/// `compiler.rs` via plain string formatting (the same convention
/// `default_path()` uses for same-file nodes) — never through the
/// file-local name→path graph, since the target isn't in it.
pub struct RawLookup {
    pub name: String,
    pub pipeline: String,
    pub node: String,
    pub base_path: Option<String>,
    pub version: Option<String>,
}

/// `encrypt { fields = [...]; key = "..." }` — see
/// `canoe_config::EncryptConfig`. Copied straight through by `compiler.rs`
/// with no path resolution (unlike `lookup`, nothing here references
/// another node); the compiler's only real work is rejecting a field that
/// also appears in the same sink's `checkpoint.keys`/`scd.keys`/
/// `scd.track_fields`.
pub struct RawEncrypt {
    pub fields: Vec<String>,
    pub key: String,
}

pub struct RawBatch {
    pub size: u64,
    pub flush_interval_secs: Option<u64>,
}

/// See `RawStep::output`/`canoe_config::OutputConfig`.
pub struct RawOutput {
    pub batch_size: Option<u64>,
    pub flush_interval_secs: Option<u64>,
    pub partition_by: Vec<String>,
}

pub enum RawOp {
    Rename { from: String, to: String },
    Drop { fields: Vec<String> },
    Select { fields: Vec<String> },
    Cast { field: String, to: String },
    WithField { name: String, expression: String },
    Filter { predicate: String },
    /// Look `field` up in a JSON object mapping (`mapping { ... }`,
    /// e.g. `{"US": "United States"}`) and write the result to `to`
    /// (defaults to `field` itself — an in-place replace). Values not
    /// found in the mapping pass through unchanged unless `default` is
    /// set. See `RawMappingSource`.
    MapValues {
        field: String,
        to: Option<String>,
        mapping: RawMappingSource,
        default: Option<String>,
    },
    /// Escape hatch: raw Python source defining `def run(record): ...`,
    /// used verbatim instead of any declarative ops in the same step.
    /// Inline or a file (`python { file = "steps/validate.py" }`).
    PythonCode { code: RawCode },
    /// Escape hatch: raw Spark SQL (queried against the `input` view),
    /// used verbatim instead of any declarative ops in the same step.
    /// Inline or a file.
    SparkSqlCode { sql: RawCode },
    /// Escape hatch: raw PySpark source defining
    /// `def transform(df: DataFrame) -> DataFrame`. `spark_dataframe`
    /// steps only support this escape hatch in this first pass — no
    /// declarative op compiles to DataFrame code yet. Inline or a file.
    SparkDataFrameCode { code: RawCode },
    /// `engine = "uv"` steps only support this: a full standalone `uv
    /// run` script (own PEP 723 header, own Kafka consume/produce loop —
    /// see `canoe_config::UvStep`). Inline or a file
    /// (`script { file = "steps/enrich.py" }`).
    UvScript { code: RawCode },
}

pub struct RawRoute {
    pub name: String,
    pub input: String,
    pub branches: Vec<RawBranch>,
    /// Whether a `default { }` block was declared — downstream steps
    /// reference it as `input = "<route_name>.default"`, matching every
    /// non-default-matching predicate.
    pub has_default: bool,
}

pub struct RawBranch {
    pub name: String,
    pub when: String,
}

pub struct RawMerge {
    pub name: String,
    pub inputs: Vec<String>,
}

pub struct RawSink {
    pub name: String,
    pub input: String,
    pub format: String,
    /// Object store URI every write under this sink is rooted at, e.g.
    /// `s3://bucket/prefix`. Never written to directly — each flush lands
    /// under `<base_path>/dataset_id=<dataset_id>/run_id=<run_id>/`.
    pub base_path: String,
    /// Defaults to the sink's own `name` if omitted.
    pub dataset_id: Option<String>,
    pub partition_by: Vec<String>,
    pub batch_size: Option<usize>,
    pub flush_interval_secs: Option<u64>,
    /// Published to the coordinator's signals topic every time this sink
    /// flushes a batch — another pipeline's `schedule { signal = ... }`
    /// names this to start when this write happens.
    pub signal: Option<String>,
    /// Materializes a deduplicated, compacted view of this sink's write
    /// history after every flush — see `RawCheckpoint`. Only valid when
    /// `format = "parquet"`; the compiler rejects it otherwise.
    pub checkpoint: Option<RawCheckpoint>,
    /// Materializes a Type-2 slowly-changing-dimension view of this
    /// sink's write history after every flush — see `RawScd`. Same
    /// `format = "parquet"`-only restriction as `checkpoint`.
    pub scd: Option<RawScd>,
    /// Zero or more `encrypt { fields = [...]; key = "..." }` blocks — see
    /// `RawEncrypt`.
    pub encrypt: Vec<RawEncrypt>,
    /// Path (relative to this `.hcl` file) to a YAML file of per-column
    /// metadata (labels, descriptions, sensitivity/classification tags,
    /// or any other free-form keys) — resolved and embedded at compile
    /// time by `canoe-dslc`, same "`file = ...` read once at compile
    /// time" precedent as `RawMappingSource::File`. See
    /// `codegen::resolve_column_metadata`.
    pub metadata: Option<String>,
}

pub struct RawCheckpoint {
    pub keys: Vec<String>,
}

pub struct RawScd {
    pub keys: Vec<String>,
    pub track_fields: Vec<String>,
    pub effective_field: Option<String>,
    /// SCD Type — parsed as-is (only `2` compiles successfully;
    /// `compiler.rs` rejects anything else with a pointer to `checkpoint`
    /// for Type-1-style overwrite semantics).
    pub scd_type: Option<u64>,
}

#[derive(Default)]
pub struct RawScaling {
    pub min_replicas: Option<u32>,
    pub max_replicas: Option<u32>,
    pub target_lag_per_replica: Option<i64>,
    pub cooldown_secs: Option<u64>,
    /// How many consecutive fast pod failures before the coordinator's
    /// circuit breaker stops replacing this step's crashed pods — see
    /// `canoe_config::ScalingConfig::backoff_limit`.
    pub backoff_limit: Option<u64>,
}

#[derive(Default)]
pub struct RawResources {
    pub cpu_request: Option<String>,
    pub mem_request: Option<String>,
    pub cpu_limit: Option<String>,
    pub mem_limit: Option<String>,
}

#[derive(Default)]
pub struct RawSparkConf {
    pub image: Option<String>,
    pub executor_instances: Option<u32>,
    pub executor_cores: Option<u32>,
    pub executor_memory: Option<String>,
    pub driver_memory: Option<String>,
    /// Arbitrary extra `spark-submit --conf` entries, keyed by Spark's own
    /// dotted setting names (e.g. `"spark.sql.shuffle.partitions"`) —
    /// deliberately not given canoe-invented aliases like `partitions`, so
    /// this stays a plain passthrough to whatever Spark itself calls a
    /// setting, not a second vocabulary to keep in sync with Spark's own
    /// docs. Passed straight through as `--conf <key>=<value>` by
    /// `canoe-spark`'s `spark_submit_args` — see `SparkJobConfig::extra_conf`.
    pub conf: BTreeMap<String, String>,
    /// This step's own override of the coordinator's global
    /// `spark.event_log` default — see `canoe_config::EventLogOverride`'s
    /// docs. `None` (no `event_log { ... }` block at all) is the common
    /// case: most steps just inherit whatever `--global-config` sets.
    pub event_log: Option<RawEventLog>,
}

/// See `RawSparkConf::event_log`/`canoe_config::EventLogOverride`. Both
/// fields optional so "this step said nothing" is distinguishable from
/// "this step explicitly said false/empty" once `canoe-coordinator`
/// merges this against the global default at Spark-launch time — the
/// compiler itself does no merging, just copies whatever was written.
pub struct RawEventLog {
    pub enabled: Option<bool>,
    pub base_path: Option<String>,
}

pub fn parse(src: &str) -> anyhow::Result<RawFile> {
    let body = hcl::parse(src).context("failed to parse DSL as HCL")?;

    let mut pipelines = Vec::new();
    for block in blocks(&body, "pipeline") {
        pipelines.push(parse_pipeline(block)?);
    }
    let mut steps = Vec::new();
    for block in blocks(&body, "step") {
        steps.push(parse_step(block)?);
    }
    let mut routes = Vec::new();
    for block in blocks(&body, "route") {
        routes.push(parse_route(block)?);
    }
    let mut dynamic_routes = Vec::new();
    for block in blocks(&body, "dynamic_route") {
        dynamic_routes.push(parse_dynamic_route(block)?);
    }
    let mut merges = Vec::new();
    for block in blocks(&body, "merge") {
        merges.push(parse_merge(block)?);
    }
    let mut sinks = Vec::new();
    for block in blocks(&body, "sink") {
        sinks.push(parse_sink(block)?);
    }
    let mut tasks = Vec::new();
    for block in blocks(&body, "task") {
        tasks.push(parse_task(block)?);
    }
    let mut adapters = Vec::new();
    for block in blocks(&body, "adapter") {
        adapters.push(parse_adapter(block)?);
    }
    if pipelines.is_empty() && adapters.is_empty() {
        bail!("no `pipeline` or `adapter` block found in DSL source");
    }

    let topics_blocks: Vec<&Block> = blocks(&body, "topics").collect();
    if topics_blocks.len() > 1 {
        bail!("only one top-level `topics` block is allowed per DSL file, found {}", topics_blocks.len());
    }
    let topics = topics_blocks.into_iter().next().map(parse_topics).transpose()?;

    let storage_blocks: Vec<&Block> = blocks(&body, "storage").collect();
    if storage_blocks.len() > 1 {
        bail!("only one top-level `storage` block is allowed per DSL file, found {}", storage_blocks.len());
    }
    let storage = storage_blocks.into_iter().next().map(parse_storage).transpose()?;

    Ok(RawFile {
        pipelines,
        steps,
        routes,
        dynamic_routes,
        merges,
        sinks,
        tasks,
        adapters,
        topics,
        storage,
    })
}

fn parse_storage(block: &Block) -> anyhow::Result<RawStorage> {
    let body = &block.body;
    Ok(RawStorage {
        base_path: require_attr_str(body, "base_path", "storage")?,
        version: attr_str(body, "version"),
    })
}

fn parse_topics(block: &Block) -> anyhow::Result<RawTopics> {
    let body = &block.body;
    let retention = blocks(body, "retention")
        .next()
        .map(|b| -> anyhow::Result<RawRetention> {
            Ok(RawRetention {
                duration: require_attr_str(&b.body, "duration", "topics.retention")?,
            })
        })
        .transpose()?;
    let archive = blocks(body, "archive")
        .next()
        .map(|b| -> anyhow::Result<RawArchive> {
            Ok(RawArchive {
                base_path: require_attr_str(&b.body, "base_path", "topics.archive")?,
                interval: require_attr_str(&b.body, "interval", "topics.archive")?,
            })
        })
        .transpose()?;
    if retention.is_none() && archive.is_none() {
        bail!("topics block declares neither `retention` nor `archive` — remove it if unused");
    }
    Ok(RawTopics { retention, archive })
}

fn parse_adapter(block: &Block) -> anyhow::Result<RawAdapter> {
    let name = label(block, 0, "adapter")?;
    let ctx = format!("adapter \"{name}\"");
    let body = &block.body;
    let mode = attr_str(body, "mode");
    let topic = attr_str(body, "topic");
    let output = blocks(body, "output").next().map(|b| parse_output_block(&b.body)).transpose()?;

    let script_block = blocks(body, "script")
        .next()
        .with_context(|| format!("{ctx}: missing required `script {{ code = ... }}` (or `file = ...`) block"))?;
    let script = parse_code_block(&script_block.body, &ctx, "code")?;

    let schedule_block = blocks(body, "schedule")
        .next()
        .with_context(|| format!("{ctx}: missing required `schedule {{ ... }}` block"))?;
    let schedule = RawAdapterSchedule {
        interval: attr_str(&schedule_block.body, "interval"),
        cron: attr_str(&schedule_block.body, "cron"),
    };

    let resources = blocks(body, "resources").next().map(parse_resources).transpose()?;
    let backoff_limit = attr_u64(body, "backoff_limit")?;
    let encrypt = blocks(body, "encrypt")
        .map(|b| parse_encrypt(b, &ctx))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let secrets = attr_str_list(body, "secrets").unwrap_or_default();

    Ok(RawAdapter {
        name,
        mode,
        topic,
        output,
        script,
        schedule,
        resources,
        backoff_limit,
        encrypt,
        secrets,
    })
}

fn parse_pipeline(block: &Block) -> anyhow::Result<RawPipeline> {
    let name = label(block, 0, "pipeline")?;
    let body = &block.body;
    let source = require_attr_str(body, "source", &format!("pipeline \"{name}\""))?;
    let schedule = blocks(body, "schedule").next().map(parse_schedule).transpose()?;

    Ok(RawPipeline { name, source, schedule })
}

fn parse_task(block: &Block) -> anyhow::Result<RawTask> {
    let name = label(block, 0, "task")?;
    let ctx = format!("task \"{name}\"");
    let body = &block.body;

    let script_block = blocks(body, "script")
        .next()
        .with_context(|| format!("{ctx}: missing required `script {{ code = ... }}` (or `file = ...`) block"))?;
    let script = parse_code_block(&script_block.body, &ctx, "code")?;

    let depends_on = require_attr_str_list(body, "depends_on", &ctx)?;
    if depends_on.is_empty() {
        bail!("{ctx}: `depends_on` must name at least one step, pipeline, or task");
    }
    let depends_on_complete = attr_str_list(body, "depends_on_complete").unwrap_or_default();

    let resources = blocks(body, "resources").next().map(parse_resources).transpose()?;
    let backoff_limit = attr_u64(body, "backoff_limit")?;
    let secrets = attr_str_list(body, "secrets").unwrap_or_default();

    Ok(RawTask {
        name,
        script,
        depends_on,
        depends_on_complete,
        resources,
        backoff_limit,
        secrets,
    })
}

fn parse_schedule(block: &Block) -> anyhow::Result<RawSchedule> {
    let body = &block.body;
    Ok(RawSchedule {
        signal: attr_str(body, "signal"),
        cron: attr_str(body, "cron"),
    })
}

fn parse_step(block: &Block) -> anyhow::Result<RawStep> {
    let name = label(block, 0, "step")?;
    let ctx = format!("step \"{name}\"");
    let body = &block.body;
    let engine = require_attr_str(body, "engine", &ctx)?;
    let mode = attr_str(body, "mode");
    let input = require_attr_str(body, "input", &ctx)?;
    let (ops, scaling, resources, spark, env, batch, output) = parse_ops_and_config(body, &ctx)?;
    let lookups = blocks(body, "lookup")
        .map(|b| parse_lookup(b, &ctx))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let encrypt = blocks(body, "encrypt")
        .map(|b| parse_encrypt(b, &ctx))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let secrets = attr_str_list(body, "secrets").unwrap_or_default();
    let depends_on = attr_str_list(body, "depends_on").unwrap_or_default();
    let depends_on_complete = attr_str_list(body, "depends_on_complete").unwrap_or_default();

    Ok(RawStep {
        name,
        engine,
        mode,
        input,
        ops,
        scaling,
        resources,
        spark,
        env,
        batch,
        output,
        lookups,
        encrypt,
        secrets,
        depends_on,
        depends_on_complete,
    })
}

/// See `RawLookup`. `base_path`/`version` stay `None` here (unresolved) —
/// `compiler.rs` fills in the file's `storage {}` defaults for whichever
/// one is omitted.
fn parse_lookup(block: &Block, step_ctx: &str) -> anyhow::Result<RawLookup> {
    let name = label(block, 0, "lookup")?;
    let ctx = format!("{step_ctx} lookup \"{name}\"");
    let body = &block.body;
    Ok(RawLookup {
        name,
        pipeline: require_attr_str(body, "pipeline", &ctx)?,
        node: require_attr_str(body, "node", &ctx)?,
        base_path: attr_str(body, "base_path"),
        version: attr_str(body, "version"),
    })
}

/// See `RawEncrypt`. No label (unlike `lookup`) — an `encrypt { ... }`
/// block is just distinguished from any others on the same node by its
/// own `fields`/`key`.
fn parse_encrypt(block: &Block, ctx: &str) -> anyhow::Result<RawEncrypt> {
    let body = &block.body;
    let fields = require_attr_str_list(body, "fields", ctx)?;
    if fields.is_empty() {
        bail!("{ctx}: `encrypt` block requires at least one field in `fields`");
    }
    Ok(RawEncrypt {
        fields,
        key: require_attr_str(body, "key", ctx)?,
    })
}

/// Shared by `step` and `dynamic_route`'s `task`/`overflow` blocks: every
/// declarative op / escape hatch, plus the `scaling`/`resources`/`spark`
/// config blocks a body may carry alongside them.
#[allow(clippy::type_complexity)]
fn parse_ops_and_config(
    body: &Body,
    ctx: &str,
) -> anyhow::Result<(
    Vec<RawOp>,
    Option<RawScaling>,
    Option<RawResources>,
    Option<RawSparkConf>,
    BTreeMap<String, String>,
    Option<RawBatch>,
    Option<RawOutput>,
)> {
    let mut ops = Vec::new();
    for structure in body.iter() {
        let Structure::Block(b) = structure else {
            continue;
        };
        let op = match b.identifier.as_str() {
            "rename" => RawOp::Rename {
                from: require_attr_str(&b.body, "from", ctx)?,
                to: require_attr_str(&b.body, "to", ctx)?,
            },
            "drop" => RawOp::Drop {
                fields: require_attr_str_list(&b.body, "fields", ctx)?,
            },
            "select" => RawOp::Select {
                fields: require_attr_str_list(&b.body, "fields", ctx)?,
            },
            "cast" => RawOp::Cast {
                field: require_attr_str(&b.body, "field", ctx)?,
                to: require_attr_str(&b.body, "to", ctx)?,
            },
            "with_field" => RawOp::WithField {
                name: require_attr_str(&b.body, "name", ctx)?,
                expression: require_attr_str(&b.body, "expression", ctx)?,
            },
            "filter" => RawOp::Filter {
                predicate: require_attr_str(&b.body, "predicate", ctx)?,
            },
            "map_values" => {
                let mapping_block = blocks(&b.body, "mapping")
                    .next()
                    .with_context(|| format!("{ctx}: `map_values` requires a `mapping {{ ... }}` block"))?;
                RawOp::MapValues {
                    field: require_attr_str(&b.body, "field", ctx)?,
                    to: attr_str(&b.body, "to"),
                    mapping: parse_mapping_block(&mapping_block.body, ctx)?,
                    default: attr_str(&b.body, "default"),
                }
            }
            "python" => RawOp::PythonCode {
                code: parse_code_block(&b.body, ctx, "code")?,
            },
            "spark_sql" => RawOp::SparkSqlCode {
                sql: parse_code_block(&b.body, ctx, "sql")?,
            },
            "spark_dataframe" => RawOp::SparkDataFrameCode {
                code: parse_code_block(&b.body, ctx, "code")?,
            },
            "script" => RawOp::UvScript {
                code: parse_code_block(&b.body, ctx, "code")?,
            },
            "scaling" | "resources" | "spark" | "env" | "batch" | "output" | "lookup" | "encrypt" => continue, // handled below (or, for `lookup`/`encrypt` on a non-`step` caller, rejected by that caller)
            other => bail!("{ctx}: unknown block `{other}`"),
        };
        ops.push(op);
    }

    let scaling = blocks(body, "scaling").next().map(parse_scaling).transpose()?;
    let resources = blocks(body, "resources").next().map(parse_resources).transpose()?;
    let spark = blocks(body, "spark").next().map(parse_spark_conf).transpose()?;
    let env = blocks(body, "env")
        .next()
        .map(|b| parse_env_block(&b.body, ctx))
        .transpose()?
        .unwrap_or_default();
    let batch = blocks(body, "batch").next().map(|b| parse_batch_block(&b.body, ctx)).transpose()?;
    let output = blocks(body, "output").next().map(|b| parse_output_block(&b.body)).transpose()?;

    Ok((ops, scaling, resources, spark, env, batch, output))
}

/// An `output { batch_size = ...; flush_interval_secs = ...; partition_by
/// = [...] }` block — see `RawStep::output`/`canoe_config::OutputConfig`.
/// Every attribute is optional; an empty `output {}` block just means
/// "use the S3-transport defaults."
fn parse_output_block(body: &Body) -> anyhow::Result<RawOutput> {
    Ok(RawOutput {
        batch_size: attr_u64(body, "batch_size")?,
        flush_interval_secs: attr_u64(body, "flush_interval_secs")?,
        partition_by: attr_str_list(body, "partition_by").unwrap_or_default(),
    })
}

/// An `env { KEY = "value", ... }` block: every attribute becomes an env
/// var name/value pair, in whatever order HCL happens to iterate them
/// (unlike `ops`, order doesn't matter for env vars) — coerced to a
/// string the same way any other DSL attribute is (`value_to_string`), so
/// `env { RETRIES = 3 }` works without quoting.
fn parse_env_block(body: &Body, ctx: &str) -> anyhow::Result<BTreeMap<String, String>> {
    let mut vars = BTreeMap::new();
    for attr in body.attributes() {
        let key = attr.key.as_str().to_string();
        let value = value_to_string(&Value::from(attr.expr.clone()))
            .with_context(|| format!("{ctx}: env var `{key}` must be a string, number, or bool"))?;
        vars.insert(key, value);
    }
    Ok(vars)
}

/// A `batch { size = ..., flush_interval_secs = ... }` block — `size` is
/// required (there's no sensible default for "how many records to hold
/// before calling the batch function"), `flush_interval_secs` is
/// optional (defaults applied by `canoe_config::BatchConfig`'s own serde
/// default).
fn parse_batch_block(body: &Body, ctx: &str) -> anyhow::Result<RawBatch> {
    Ok(RawBatch {
        size: attr_u64(body, "size")?.with_context(|| format!("{ctx}: `batch` block requires a `size` attribute"))?,
        flush_interval_secs: attr_u64(body, "flush_interval_secs")?,
    })
}

fn parse_dynamic_route(block: &Block) -> anyhow::Result<RawDynamicRoute> {
    let name = label(block, 0, "dynamic_route")?;
    let ctx = format!("dynamic_route \"{name}\"");
    let body = &block.body;
    let input = require_attr_str(body, "input", &ctx)?;
    let field = require_attr_str(body, "field", &ctx)?;
    let max_branches = attr_u64(body, "max_branches")?.map(|n| n as u32);

    let task_block = blocks(body, "task")
        .next()
        .with_context(|| format!("{ctx}: missing required `task {{ ... }}` block"))?;
    let task = parse_dynamic_task(task_block, &format!("{ctx} task"))?;

    let overflow = blocks(body, "overflow")
        .next()
        .map(|b| parse_dynamic_task(b, &format!("{ctx} overflow")))
        .transpose()?;

    Ok(RawDynamicRoute {
        name,
        input,
        field,
        max_branches,
        task,
        overflow,
    })
}

fn parse_dynamic_task(block: &Block, ctx: &str) -> anyhow::Result<RawDynamicTask> {
    let body = &block.body;
    let engine = require_attr_str(body, "engine", ctx)?;
    let (ops, scaling, resources, spark, env, batch, output) = parse_ops_and_config(body, ctx)?;
    if blocks(body, "lookup").next().is_some() {
        bail!("{ctx}: `lookup` blocks are not supported here — dynamic_route stays on the Kafka-topic transport");
    }
    if blocks(body, "encrypt").next().is_some() {
        bail!("{ctx}: `encrypt` blocks are not supported here — dynamic_route/KafkaPythonStep is out of scope for field encryption");
    }
    if attr_str_list(body, "secrets").is_some() {
        bail!("{ctx}: `secrets` is not supported here — dynamic_route/KafkaPythonStep is out of scope for Secrets Manager mounting");
    }
    Ok(RawDynamicTask {
        engine,
        ops,
        scaling,
        resources,
        spark,
        env,
        batch,
        output,
    })
}

fn parse_route(block: &Block) -> anyhow::Result<RawRoute> {
    let name = label(block, 0, "route")?;
    let ctx = format!("route \"{name}\"");
    let body = &block.body;
    let input = require_attr_str(body, "input", &ctx)?;

    let mut branches = Vec::new();
    for b in blocks(body, "branch") {
        let branch_name = label(b, 0, "branch")?;
        branches.push(RawBranch {
            when: require_attr_str(&b.body, "when", &ctx)?,
            name: branch_name,
        });
    }
    if branches.is_empty() {
        bail!("{ctx}: declares no branches");
    }
    let has_default = blocks(body, "default").next().is_some();

    Ok(RawRoute {
        name,
        input,
        branches,
        has_default,
    })
}

fn parse_merge(block: &Block) -> anyhow::Result<RawMerge> {
    let name = label(block, 0, "merge")?;
    let ctx = format!("merge \"{name}\"");
    let inputs = require_attr_str_list(&block.body, "inputs", &ctx)?;
    if inputs.len() < 2 {
        bail!("{ctx}: `inputs` must list at least two upstream names to merge");
    }
    Ok(RawMerge { name, inputs })
}

fn parse_sink(block: &Block) -> anyhow::Result<RawSink> {
    let name = label(block, 0, "sink")?;
    let ctx = format!("sink \"{name}\"");
    let body = &block.body;
    let checkpoint = blocks(body, "checkpoint")
        .next()
        .map(|b| -> anyhow::Result<RawCheckpoint> {
            Ok(RawCheckpoint {
                keys: require_attr_str_list(&b.body, "keys", &ctx)?,
            })
        })
        .transpose()?;
    let scd = blocks(body, "scd")
        .next()
        .map(|b| -> anyhow::Result<RawScd> {
            Ok(RawScd {
                keys: require_attr_str_list(&b.body, "keys", &ctx)?,
                track_fields: attr_str_list(&b.body, "track_fields").unwrap_or_default(),
                effective_field: attr_str(&b.body, "effective_field"),
                scd_type: attr_u64(&b.body, "type")?,
            })
        })
        .transpose()?;
    let encrypt = blocks(body, "encrypt")
        .map(|b| parse_encrypt(b, &ctx))
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(RawSink {
        input: require_attr_str(body, "input", &ctx)?,
        format: require_attr_str(body, "format", &ctx)?,
        base_path: require_attr_str(body, "base_path", &ctx)?,
        dataset_id: attr_str(body, "dataset_id"),
        partition_by: attr_str_list(body, "partition_by").unwrap_or_default(),
        batch_size: attr_u64(body, "batch_size")?.map(|n| n as usize),
        flush_interval_secs: attr_u64(body, "flush_interval_secs")?,
        signal: attr_str(body, "signal"),
        checkpoint,
        scd,
        encrypt,
        metadata: attr_str(body, "metadata"),
        name,
    })
}

fn parse_scaling(block: &Block) -> anyhow::Result<RawScaling> {
    let body = &block.body;
    Ok(RawScaling {
        min_replicas: attr_u64(body, "min_replicas")?.map(|n| n as u32),
        max_replicas: attr_u64(body, "max_replicas")?.map(|n| n as u32),
        target_lag_per_replica: attr_u64(body, "target_lag_per_replica")?.map(|n| n as i64),
        cooldown_secs: attr_u64(body, "cooldown_secs")?,
        backoff_limit: attr_u64(body, "backoff_limit")?,
    })
}

fn parse_resources(block: &Block) -> anyhow::Result<RawResources> {
    let body = &block.body;
    Ok(RawResources {
        cpu_request: attr_str(body, "cpu_request"),
        mem_request: attr_str(body, "mem_request"),
        cpu_limit: attr_str(body, "cpu_limit"),
        mem_limit: attr_str(body, "mem_limit"),
    })
}

fn parse_spark_conf(block: &Block) -> anyhow::Result<RawSparkConf> {
    let body = &block.body;
    Ok(RawSparkConf {
        image: attr_str(body, "image"),
        executor_instances: attr_u64(body, "executor_instances")?.map(|n| n as u32),
        executor_cores: attr_u64(body, "executor_cores")?.map(|n| n as u32),
        executor_memory: attr_str(body, "executor_memory"),
        driver_memory: attr_str(body, "driver_memory"),
        conf: attr_str_map(body, "conf", "spark")?,
        event_log: blocks(body, "event_log")
            .next()
            .map(|b| -> anyhow::Result<RawEventLog> {
                Ok(RawEventLog {
                    enabled: attr_bool(&b.body, "enabled")?,
                    base_path: attr_str(&b.body, "base_path"),
                })
            })
            .transpose()?,
    })
}

// --- small helpers over hcl::Body/Block ---

fn blocks<'a>(body: &'a Body, identifier: &'a str) -> impl Iterator<Item = &'a Block> {
    body.blocks().filter(move |b| b.identifier.as_str() == identifier)
}

fn label(block: &Block, index: usize, kind: &str) -> anyhow::Result<String> {
    block
        .labels
        .get(index)
        .map(|l| l.clone().into_inner())
        .with_context(|| format!("`{kind}` block is missing its name label"))
}

fn value_to_string(value: &Value) -> anyhow::Result<String> {
    match value {
        Value::String(s) => Ok(s.clone()),
        Value::Number(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        other => bail!("expected a string, got {other:?}"),
    }
}

fn attr_str(body: &Body, key: &str) -> Option<String> {
    body.attributes()
        .find(|a| a.key.as_str() == key)
        .and_then(|a| value_to_string(&Value::from(a.expr.clone())).ok())
}

fn require_attr_str(body: &Body, key: &str, ctx: &str) -> anyhow::Result<String> {
    attr_str(body, key).with_context(|| format!("{ctx}: missing required attribute `{key}`"))
}

fn attr_str_list(body: &Body, key: &str) -> Option<Vec<String>> {
    let attr = body.attributes().find(|a| a.key.as_str() == key)?;
    let value = Value::from(attr.expr.clone());
    match value {
        Value::Array(items) => Some(
            items
                .iter()
                .filter_map(|v| value_to_string(v).ok())
                .collect(),
        ),
        single => value_to_string(&single).ok().map(|s| vec![s]),
    }
}

fn require_attr_str_list(body: &Body, key: &str, ctx: &str) -> anyhow::Result<Vec<String>> {
    attr_str_list(body, key).with_context(|| format!("{ctx}: missing required attribute `{key}`"))
}

/// Parses `key = { "a.b.c" = "value", ... }` — an object-literal attribute
/// with string keys — into a `BTreeMap`. Object-literal syntax (not a
/// nested `{ a.b.c = "value" }` *block*, the way `env { ... }` works) is
/// required here specifically because HCL body-attribute identifiers
/// can't contain dots, but Spark's own setting names always do
/// (`spark.sql.shuffle.partitions`) — see `RawSparkConf::conf`.
fn attr_str_map(body: &Body, key: &str, ctx: &str) -> anyhow::Result<BTreeMap<String, String>> {
    let Some(attr) = body.attributes().find(|a| a.key.as_str() == key) else {
        return Ok(BTreeMap::new());
    };
    let value = Value::from(attr.expr.clone());
    let Value::Object(map) = value else {
        bail!("{ctx}: `{key}` must be an object, e.g. `{key} = {{ \"spark.sql.shuffle.partitions\" = \"200\" }}`");
    };
    map.into_iter()
        .map(|(k, v)| {
            let v = value_to_string(&v)
                .with_context(|| format!("{ctx}: `{key}.{k}` must be a string, number, or bool"))?;
            Ok((k, v))
        })
        .collect()
}

fn attr_bool(body: &Body, key: &str) -> anyhow::Result<Option<bool>> {
    let Some(attr) = body.attributes().find(|a| a.key.as_str() == key) else {
        return Ok(None);
    };
    match Value::from(attr.expr.clone()) {
        Value::Bool(b) => Ok(Some(b)),
        other => bail!("attribute `{key}` must be a bool, got {other:?}"),
    }
}

fn attr_u64(body: &Body, key: &str) -> anyhow::Result<Option<u64>> {
    let Some(attr) = body.attributes().find(|a| a.key.as_str() == key) else {
        return Ok(None);
    };
    match Value::from(attr.expr.clone()) {
        Value::Number(n) => n
            .as_u64()
            .map(Some)
            .with_context(|| format!("attribute `{key}` must be a non-negative integer")),
        other => bail!("attribute `{key}` must be a number, got {other:?}"),
    }
}
