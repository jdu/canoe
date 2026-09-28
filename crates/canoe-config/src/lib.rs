//! Config schema for canoe pipelines: a pipeline is a set of named chains,
//! each rooted at a named producer (`source`) and made of steps that pass
//! data from one node to the next, ending at a sink. A step's execution
//! model is `mode = "batch" | "streaming"`, orthogonal to its `engine`
//! (python/spark_sql/spark_dataframe/uv) — see `StepConfig`'s variants.
//! **batch** (the default for `python`/`uv`/`spark_sql`/`spark_dataframe`
//! alike) is S3 + signal transport (see `StorageConfig`, `OutputConfig`,
//! and `StepConfig::input_ref`/`output_ref`'s docs): row data moves as
//! Parquet in an object store, Kafka carries only a tiny wake-up signal,
//! and the step runs as a one-shot K8s Job that drains backlog and exits
//! (Job completion is a free "done" signal). **streaming** (`Kafka*Step`
//! variants — `KafkaPythonStep`/`KafkaSparkSqlStep`/`KafkaSparkDataFrameStep`,
//! plus `DynamicRouteStep`, always streaming) moves row data through a
//! Kafka topic directly, a genuinely continuous process with no natural
//! "done" — canoe's original design, still `spark_sql`/`spark_dataframe`'s
//! *default* mode (preserves every pre-existing pipeline's behavior).

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    pub chains: Vec<ChainConfig>,
    /// Scheduled source adapters — see `AdapterConfig`. Not part of any
    /// chain: an adapter's only job is publishing to its `topic`, which a
    /// chain's `source` then picks up like any other producer.
    #[serde(default)]
    pub adapters: Vec<AdapterConfig>,
    /// File-level `storage { base_path = "..."; version = "..." }` block —
    /// the S3 root and version segment every S3-transport node (`Python`/
    /// `Uv`/`Sink` steps) resolves its own `<base_path>/<pipeline>/<version>/<name>`
    /// path beneath. Deliberately file-level, not per-pipeline: the DSL
    /// compiler builds every node's path once, globally, before it knows
    /// which pipeline(s) can reach that node, so a per-pipeline setting
    /// would be ambiguous for a node reachable from more than one
    /// pipeline. `None` is only valid for a file with no S3-transport
    /// steps at all (e.g. Spark/dynamic_route-only pipelines).
    #[serde(default)]
    pub storage: Option<StorageConfig>,
    /// Retention and archival policy applied to every topic in
    /// `managed_topics` — a DSL top-level `topics { ... }` block, global
    /// to the whole file. `None` means canoe leaves every topic's
    /// retention at the cluster default and archives nothing.
    #[serde(default)]
    pub topic_defaults: Option<TopicDefaults>,
    /// Every internal step-to-step topic across every pipeline in this
    /// file — one entry per step's output topic, deduplicated by the DSL
    /// compiler before chain-path enumeration would otherwise duplicate
    /// a step (and its topic) across multiple branches. Does *not*
    /// include a pipeline's external `source_topic`: canoe only manages
    /// retention/archival for topics it itself produces to, not ones
    /// another system might also own. See `crates/canoe-coordinator/src/retention.rs`
    /// and `archiver.rs`.
    #[serde(default)]
    pub managed_topics: Vec<ManagedTopic>,
    /// One-shot, run-after-a-dependency tasks — a DSL root-level `task
    /// { ... }` block. Not part of any chain (no input/output topic of
    /// its own); see `TaskConfig`.
    #[serde(default)]
    pub tasks: Vec<TaskConfig>,
}

/// File-level S3 root for every S3-transport node — see `PipelineConfig::storage`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Object store URI, e.g. `s3://bucket/prefix`. Every node resolves to
    /// `<base_path>/<pipeline>/<version>/<node_name>/`.
    pub base_path: String,
    #[serde(default = "default_storage_version")]
    pub version: String,
}

fn default_storage_version() -> String {
    "v1".to_string()
}

/// A one-shot side-effecting script that runs once every name in
/// `depends_on` is satisfied, rather than on a schedule (an adapter) or
/// by consuming a topic (a step) — see `crates/canoe-coordinator/src/tasks.rs`
/// for how dependencies are resolved and the Job actually fired.
/// Mechanically identical to an `AdapterConfig`'s Job (same
/// `workers/adapter-runner` image, same `uv run <script>`), just
/// triggered differently.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskConfig {
    pub name: String,
    /// Where the Job fetches the script from at run time.
    pub code: CodeSource,
    /// Names of steps, dynamic_routes, pipelines, or other tasks this
    /// task must wait on — see `crates/canoe-dsl`'s `compile_task` for
    /// the compile-time validation of what a name may refer to, and
    /// `crates/canoe-coordinator/src/tasks.rs` for what "wait on" means
    /// for each kind (a continuous Deployment vs. another one-shot task).
    pub depends_on: Vec<String>,
    /// Same ordering purpose as `depends_on`, but a *real completion*
    /// gate rather than an "applied" one — names another task or a
    /// batch-mode step, satisfied only once that Job has actually
    /// succeeded (`crates/canoe-coordinator/src/activation.rs`'s
    /// `Ctx.completed`, populated by `run_batch_reconcile_loop`/this
    /// task's own success — see `tasks.rs`'s module docs). A
    /// streaming-mode step or `dynamic_route` has no "done" to depend on
    /// this way; `crates/canoe-dsl` rejects one at compile time.
    #[serde(default)]
    pub depends_on_complete: Vec<String>,
    #[serde(default)]
    pub resources: PodResources,
    /// How many times the Job's own pod may fail before K8s gives up on
    /// this run — passed straight through as the Job's `spec.backoffLimit`.
    /// See `default_backoff_limit`'s docs for why this and the step-level
    /// circuit breaker's `backoff_limit` share a name despite meaning
    /// mechanically different things.
    #[serde(default = "default_backoff_limit")]
    pub backoff_limit: u32,
    /// Zero or more AWS Secrets Manager entries this task's Job should
    /// have mounted as files, via the Secrets Store CSI Driver + AWS
    /// provider (ASCP) — see `crates/canoe-k8s/src/secrets.rs`. Each named
    /// secret shows up as its own file under `CANOE_SECRETS_PATH` (fixed
    /// at `/mnt/secrets-store`). Fully automatic — canoe provisions the
    /// `SecretProviderClass` and the CSI volume/mount itself, unlike
    /// `EncryptConfig`, which an adapter/uv script has to call a helper
    /// for explicitly: mounting a file needs no crypto logic on the
    /// script's part.
    #[serde(default)]
    pub secrets: Vec<String>,
}

/// Shared default for every block's `backoff_limit` — `2` matches the
/// value `adapter`/`task` Jobs already hardcoded before this became
/// configurable. Deliberately the same name and default across
/// `AdapterConfig`/`TaskConfig` (a K8s Job's native `spec.backoffLimit`:
/// kubelet retries the *same* Job a bounded number of times) and
/// `PythonStep`/`UvStep` (a coordinator-side circuit breaker counting
/// consecutive *fast* pod failures before it stops creating replacement
/// pods for that step — see `crates/canoe-k8s/src/pod_manager.rs`) even
/// though the underlying mechanisms differ: both answer the same
/// author-facing question, "how many failures before canoe gives up and
/// makes noise instead of retrying forever."
pub fn default_backoff_limit() -> u32 {
    2
}

/// One internal chain-wiring topic canoe knows about: `task` is the step
/// that produces `topic` (its output), `pipeline` is the DSL pipeline
/// that step belongs to. Used to apply `topic_defaults.retention_ms` and
/// to name an archive dump's `<pipeline>/<task>/` folder.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagedTopic {
    pub pipeline: String,
    pub task: String,
    pub topic: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicDefaults {
    /// Applied to every topic in `managed_topics` via the Kafka admin
    /// API at coordinator startup (creating the topic with this
    /// retention if it doesn't exist yet, or altering it in place if it
    /// does) — see `crates/canoe-kafka/src/admin.rs`. `None` leaves
    /// retention at the cluster/topic default.
    #[serde(default)]
    pub retention_ms: Option<i64>,
    /// Periodically dumps every managed topic's raw messages to S3 as
    /// Parquet, for durability/debugging independent of the pipeline
    /// sinks — see `ArchiveConfig`. `None` disables archival entirely.
    #[serde(default)]
    pub archive: Option<ArchiveConfig>,
}

/// Runs as its own consumer group per topic (`crates/canoe-coordinator/src/archiver.rs`),
/// entirely separate from a chain's own step/sink consumers, so a slow
/// or failing archive flush can never back-pressure the actual pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveConfig {
    /// Object store URI archived files are rooted under, e.g.
    /// `s3://bucket/prefix`. Each flush writes one file to
    /// `<base_path>/<pipeline>/<task>/<ulid>.parquet` — see
    /// `crates/canoe-sink/src/archive.rs`.
    pub base_path: String,
    /// How often each managed topic's archiver flushes whatever it's
    /// buffered since the last flush. Purely time-based — there's no
    /// batch-size trigger the way a sink has one; an idle topic just
    /// skips the flush (no empty files).
    pub interval_secs: u64,
}

/// A scheduled job that pulls data from an external source system and
/// publishes it to `topic`. Runs as a standalone `uv run` Python script
/// (PEP 723 inline dependency metadata in the script header — no
/// pre-built environment or requirements.txt) inside a one-shot K8s Job,
/// on a schedule the coordinator itself tracks and dispatches — see
/// `crates/canoe-coordinator/src/scheduler.rs`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdapterConfig {
    pub name: String,
    /// Kafka topic the script publishes to; the coordinator sets this on
    /// `CANOE_OUTPUT_TOPIC` for the script to read, same convention as
    /// `PythonStep`. Set only when this adapter is Kafka-transport (no
    /// `mode` attribute, or `mode = "streaming"` — the default, preserving
    /// every pre-existing adapter's compiled shape); `None` for a
    /// `mode = "batch"` adapter, which writes to `output_path` instead.
    /// There's no `mode` field here (unlike `PythonStep`/`SparkSqlStep`):
    /// an adapter isn't part of a `StepConfig`-style sum type keyed by
    /// mode, so which transport it uses is just which of
    /// `topic`/`output_path` is set — the same "one struct, mutually
    /// exclusive optional fields" shape `AdapterSchedule` already uses for
    /// `interval_secs`/`cron`.
    pub topic: Option<String>,
    /// S3 path the script writes Parquet runs to, via the
    /// `canoe_adapter_output` helper baked into the adapter-runner image —
    /// set only for `mode = "batch"`. Unlike a step's `output_path`, this
    /// isn't scoped under a pipeline (an adapter has no owning pipeline):
    /// it resolves to `<base_path>/<version>/<name>/`, not
    /// `<base_path>/<pipeline>/<version>/<name>/` — see
    /// `compiler.rs`'s `adapter_output_path`.
    #[serde(default)]
    pub output_path: Option<String>,
    /// Only set for `mode = "batch"` — see `OutputConfig`.
    #[serde(default)]
    pub output: Option<OutputConfig>,
    /// Where the Job fetches the script from at run time.
    pub code: CodeSource,
    pub schedule: AdapterSchedule,
    #[serde(default)]
    pub resources: PodResources,
    /// Passed straight through as the Job's `spec.backoffLimit` — see
    /// `default_backoff_limit`'s docs.
    #[serde(default = "default_backoff_limit")]
    pub backoff_limit: u32,
    /// Zero or more `encrypt { fields = [...]; key = "..." }` blocks — see
    /// `EncryptConfig`. **Not automatic**, unlike a `python`/`spark_sql`
    /// step or a sink: an adapter's script owns its entire loop (same
    /// reason `env {}` exists for user config rather than canoe injecting
    /// it) — there is no canoe-owned interception point around what it
    /// produces. canoe only resolves the named key(s) into
    /// `CANOE_ENCRYPT_KEY_<NAME>` env vars on the Job; the script author
    /// must call the documented encrypt helper explicitly on these
    /// `fields` before producing, using the same wire format so its
    /// output is still transparently decryptable downstream.
    #[serde(default)]
    pub encrypt: Vec<EncryptConfig>,
    /// Zero or more AWS Secrets Manager entries this adapter's Job should
    /// have mounted as files — see `TaskConfig::secrets`'s docs (same
    /// mechanism, same `CANOE_SECRETS_PATH`). Unlike `encrypt` above, this
    /// *is* fully automatic even for an adapter's self-owned script: it's
    /// just a file mount canoe sets up, no crypto logic required on the
    /// script's part.
    #[serde(default)]
    pub secrets: Vec<String>,
}

/// Exactly one of `interval_secs`/`cron` must be set — validated in
/// `PipelineConfig::validate`. `interval_secs` is a fixed cadence from
/// the adapter's last run (what a DSL `schedule { interval = "5m" }`
/// compiles to); `cron` is a standard 5-field cron expression evaluated
/// in UTC, same as `schedule { cron = "*/5 * * * *" }`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdapterSchedule {
    #[serde(default)]
    pub interval_secs: Option<u64>,
    #[serde(default)]
    pub cron: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainConfig {
    pub name: String,
    /// The DSL `pipeline` block this chain belongs to. A real field
    /// (rather than parsed back out of `name`, as it used to be — see
    /// `crates/canoe-coordinator/src/activation.rs::mark_activated`) since
    /// a node's resolved S3 path needs its pipeline name at compile time,
    /// and recovering it by string-manipulating `name` was already a
    /// known wart.
    pub pipeline: String,
    /// Name of the producer (an adapter, or another chain's terminal
    /// producer) the coordinator listens on to trigger/track this chain.
    /// For an S3-transport first step this is a producer *name*, resolved
    /// by the compiler into that step's `input_path`/`input_signal`; for a
    /// still-Kafka-topic-based first step (Spark/dynamic_route) it *is*
    /// the topic name directly, same as before this migration.
    pub source: String,
    pub steps: Vec<StepConfig>,
    /// When set, this chain's steps stay dormant (deployments held at 0
    /// replicas, Spark launchers not submitted) until activated — see
    /// `TriggerConfig`. `None` means the chain runs continuously from
    /// startup, as if every step were always fed by live topic traffic.
    #[serde(default)]
    pub trigger: Option<TriggerConfig>,
}

/// How a chain gets started, for chains that don't just run continuously
/// from coordinator startup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerConfig {
    /// Activate this chain the first time a message carrying this name
    /// arrives on the coordinator's internal signals topic (see
    /// `SinkStep::signal` for how a signal gets emitted in the first
    /// place). Implemented by `canoe-coordinator`'s signal listener.
    #[serde(default)]
    pub signal: Option<String>,
    /// Reserved for future cron-based scheduling; accepted so pipelines
    /// can declare it now, but the coordinator does not act on it yet.
    #[serde(default)]
    pub cron: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StepConfig {
    Python(PythonStep),
    /// `python { mode = "streaming" }` — see `KafkaPythonStep`'s docs. Was
    /// `dynamic_route`-only before `mode` existed as a first-class DSL
    /// attribute; now reachable from any regular step too.
    KafkaPython(KafkaPythonStep),
    SparkSql(SparkSqlStep),
    /// `spark_sql { mode = "streaming" }` (also today's *only* behavior,
    /// before `mode` existed) — see `KafkaSparkSqlStep`'s docs.
    KafkaSparkSql(KafkaSparkSqlStep),
    SparkDataFrame(SparkDataFrameStep),
    /// `spark_dataframe { mode = "streaming" }` — see `KafkaSparkDataFrameStep`'s docs.
    KafkaSparkDataFrame(KafkaSparkDataFrameStep),
    Sink(SinkStep),
    DynamicRoute(DynamicRouteStep),
    Uv(UvStep),
}

impl StepConfig {
    pub fn name(&self) -> &str {
        match self {
            StepConfig::Python(s) => &s.name,
            StepConfig::KafkaPython(s) => &s.name,
            StepConfig::SparkSql(s) => &s.name,
            StepConfig::KafkaSparkSql(s) => &s.name,
            StepConfig::SparkDataFrame(s) => &s.name,
            StepConfig::KafkaSparkDataFrame(s) => &s.name,
            StepConfig::Sink(s) => &s.name,
            StepConfig::DynamicRoute(s) => &s.name,
            StepConfig::Uv(s) => &s.name,
        }
    }

    /// The *logical name* of the producer this step reads from — a step
    /// name or a chain's `source` — used to check a chain's declared
    /// `source` against its first step, and (by the DSL compiler) as the
    /// key into `name_to_topic`/`name_to_path` resolution maps. For every
    /// still-Kafka-topic-transport variant this happens to be the same
    /// string as `input_ref()` (the topic name serves as both the logical
    /// name and the physical transport value); for the S3-transport
    /// variants it's different from `input_ref()` (a name vs. its resolved
    /// S3 path).
    pub fn input_name(&self) -> &str {
        match self {
            StepConfig::Python(s) => &s.input,
            StepConfig::Uv(s) => &s.input,
            StepConfig::Sink(s) => &s.input,
            StepConfig::SparkSql(s) => &s.input,
            StepConfig::SparkDataFrame(s) => &s.input,
            StepConfig::KafkaPython(s) => &s.input_topic,
            StepConfig::KafkaSparkSql(s) => &s.input_topic,
            StepConfig::KafkaSparkDataFrame(s) => &s.input_topic,
            StepConfig::DynamicRoute(s) => &s.input_topic,
        }
    }

    /// The value this step reads from — an S3 path (batch-mode variants,
    /// the S3+signal transport) or a Kafka topic name (streaming-mode
    /// variants, `DynamicRoute`; see README.md's "S3 + signal-driven
    /// pipeline transport"). Transport-agnostic on purpose: adjacency
    /// validation (`PipelineConfig::validate`) and the DSL compiler's graph
    /// resolution both just compare these strings for equality. Note this
    /// does *not* by itself catch a step wired directly into a step on the
    /// other transport — the DSL compiler forwards whatever value a
    /// producer resolved to into whatever field the consumer's transport
    /// uses, so a mismatched pair can end up with equal strings on both
    /// sides and pass this check anyway. Real mixed-transport rejection is
    /// expected in a later phase once `DynamicRoute` also migrates.
    pub fn input_ref(&self) -> &str {
        match self {
            StepConfig::Python(s) => &s.input_path,
            StepConfig::Uv(s) => &s.input_path,
            StepConfig::Sink(s) => &s.input_path,
            StepConfig::SparkSql(s) => &s.input_path,
            StepConfig::SparkDataFrame(s) => &s.input_path,
            StepConfig::KafkaPython(s) => &s.input_topic,
            StepConfig::KafkaSparkSql(s) => &s.input_topic,
            StepConfig::KafkaSparkDataFrame(s) => &s.input_topic,
            StepConfig::DynamicRoute(s) => &s.input_topic,
        }
    }

    /// The value this step produces to, if any (a sink is terminal — `None`).
    pub fn output_ref(&self) -> Option<&str> {
        match self {
            StepConfig::Python(s) => Some(&s.output_path),
            StepConfig::Uv(s) => Some(&s.output_path),
            StepConfig::Sink(_) => None,
            StepConfig::SparkSql(s) => Some(&s.output_path),
            StepConfig::SparkDataFrame(s) => Some(&s.output_path),
            StepConfig::KafkaPython(s) => Some(&s.output_topic),
            StepConfig::KafkaSparkSql(s) => Some(&s.output_topic),
            StepConfig::KafkaSparkDataFrame(s) => Some(&s.output_topic),
            StepConfig::DynamicRoute(s) => Some(&s.output_topic),
        }
    }
}

/// A step whose entire behavior is a standalone `uv run` script — its own
/// `# /// script` PEP 723 header declares whatever third-party
/// dependencies it needs, resolved into an ephemeral venv per pod start,
/// same convention as `AdapterConfig`. Unlike a `PythonStep` (whose code
/// is a `def run(record): ...` function called once per message inside
/// the long-lived, shared-environment `python-worker` process — so it's
/// limited to whatever's pre-installed in that image), a `UvStep`'s
/// script *is* the whole program: it owns its own Kafka consume/produce
/// loop against `CANOE_INPUT_TOPIC`/`CANOE_OUTPUT_TOPIC`/
/// `CANOE_KAFKA_BOOTSTRAP`, the same way an adapter's script owns
/// producing to `CANOE_OUTPUT_TOPIC`. Reach for this when a step
/// genuinely needs a dependency `workers/python-worker`'s image doesn't
/// carry, and installing it there for every step isn't the right
/// tradeoff. Runs continuously in a K8s Deployment (unlike an adapter's
/// one-shot Job — a step processes an ongoing stream, not a scheduled
/// pull) via `workers/uv-step-worker`, which execs `uv run` directly
/// (process replacement, not a subprocess wait) so K8s's restart-on-exit
/// and signal delivery pass straight through to the script.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UvStep {
    pub name: String,
    /// Name of the producer this step reads from (see `StepConfig::input_name`).
    pub input: String,
    /// Compiler-resolved S3 path of that producer's output.
    pub input_path: String,
    /// The producer's own name — the signal filter this step's worker
    /// subscribes to for wake-up (see `PythonStep::input_signal`'s docs).
    pub input_signal: String,
    /// Compiler-resolved S3 path this step writes its own output under.
    pub output_path: String,
    #[serde(default)]
    pub output: OutputConfig,
    pub code: CodeSource,
    #[serde(default)]
    pub scaling: ScalingConfig,
    #[serde(default)]
    pub resources: PodResources,
    /// User-declared env vars (a DSL `env { ... }` block) injected into the
    /// script's pod alongside canoe's own `CANOE_*` ones — see README.md's
    /// "Custom scripts in pipelines" / "env vars" for why: config a script
    /// needs (an API key, a region, a feature flag) belongs in the HCL
    /// that already owns every other knob for this step, not hardcoded in
    /// the script itself.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Zero or more snapshot joins against a node in a *different* `.hcl`
    /// file — see `LookupConfig`.
    #[serde(default)]
    pub lookups: Vec<LookupConfig>,
    /// Zero or more `encrypt { fields = [...]; key = "..." }` blocks — see
    /// `EncryptConfig`. Applied transparently: `canoe_worker`'s S3+signal
    /// loop decrypts any `canoe:enc:v1:`-prefixed value it reads before
    /// `run()` sees it, and encrypts these declared output fields after
    /// `run()` returns, before writing.
    #[serde(default)]
    pub encrypt: Vec<EncryptConfig>,
    /// Zero or more AWS Secrets Manager entries this step's pod should
    /// have mounted as files — see `TaskConfig::secrets`'s docs. Fully
    /// automatic (a file mount, not crypto), unlike `encrypt`.
    #[serde(default)]
    pub secrets: Vec<String>,
}

/// A resolved `lookup "name" { pipeline = ...; node = ... }` block: a
/// snapshot join against a node declared in a wholly separate `.hcl`
/// file (possibly a different deployment/bucket), read via that node's
/// `latest.json` pointer at run time rather than tracked incrementally
/// like `input`/`input_path` — there is no checkpoint for a lookup, only
/// "whatever's current right now." `path` is resolved by
/// `crates/canoe-dsl/src/compiler.rs::resolve_lookup` via plain string
/// formatting (`<base_path>/<pipeline>/<version>/<node>`), never through
/// the file-local name→path graph that resolves `input`/`input_path` —
/// the target isn't a node this compiler invocation ever parses, so
/// there's no existence check possible at compile time; a bad
/// `pipeline`/`node` only surfaces at run time as a missing/empty
/// `latest.json` fetch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LookupConfig {
    /// The name this lookup is addressed by at read time — a Spark SQL
    /// temp view name, or a key in the `lookups` dict a Python step's
    /// `run()` receives.
    pub name: String,
    pub path: String,
}

/// Airflow-`.expand()`-style dynamic fan-out: unlike a `route` block
/// (whose branches — and their predicates — are all fixed at DSL-compile
/// time), a `dynamic_route` doesn't know `field`'s distinct values ahead
/// of time. The coordinator itself discovers them at run time, by
/// watching `input_topic` and provisioning one dedicated Kafka topic + K8s
/// Deployment (running `task`) per *newly seen* value — see
/// `crates/canoe-coordinator/src/dynamic_router.rs`. Every dynamically
/// provisioned task, whatever value it was created for, writes to the
/// same shared `output_topic`: there's no way to `merge { inputs = [...]
/// }` branches whose names aren't known statically, so a dynamic_route
/// merges its own output implicitly.
///
/// `max_branches` bounds the number of distinct values it will ever
/// provision a Deployment for — required, not optional, because an
/// unbounded version of this risks runaway K8s object creation on a
/// high-cardinality or adversarial field (the same reason Airflow's
/// `.expand()` has `max_map_length`). Once the cap is hit, a *new* value
/// routes to `overflow` if configured (provisioned once, up front, not
/// dynamically), or is dropped with a logged warning if not.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamicRouteStep {
    pub name: String,
    pub input_topic: String,
    /// The shared topic every dynamically provisioned `task` instance
    /// (and `overflow`, if set) writes to.
    pub output_topic: String,
    /// JSON field whose distinct values determine the fan-out. Coerced
    /// to a string (numbers/bools via their JSON text, objects/arrays
    /// rejected) to build both the per-value topic name and the
    /// Deployment name.
    pub field: String,
    pub max_branches: u32,
    pub task: DynamicTaskTemplate,
    #[serde(default)]
    pub overflow: Option<DynamicTaskTemplate>,
}

/// Same declarative-ops-or-escape-hatch compilation a normal step's body
/// goes through (`crates/canoe-dsl/src/codegen.rs`), just without a fixed
/// `input_topic`/`output_topic` of its own — the coordinator fills those
/// in per discovered value when it instantiates a real `PythonStep`/
/// `SparkSqlStep`/`SparkDataFrameStep` from this template.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "engine", rename_all = "snake_case")]
pub enum DynamicTaskTemplate {
    Python {
        code: CodeSource,
        entrypoint: String,
        #[serde(default)]
        scaling: ScalingConfig,
        #[serde(default)]
        resources: PodResources,
    },
    SparkSql {
        sql: String,
        #[serde(default)]
        spark: SparkJobConfig,
    },
    SparkDataFrame {
        code: CodeSource,
        #[serde(default)]
        spark: SparkJobConfig,
    },
}

/// A step whose transform is a Python function. Executed by generic worker
/// pods (shared container image) which pull the actual step code at
/// startup per `code`. S3 + signal transport: reads new `run_id=` folders
/// under `input_path` (waking up on `input_signal`, self-healing via its
/// own checkpoint if a signal is missed — see README.md's "S3 + signal-
/// driven pipeline transport"), writes its own runs under `output_path`
/// per `output`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PythonStep {
    pub name: String,
    /// Name of the producer this step reads from — a step name or a
    /// chain's `source` (see `StepConfig::input_name`).
    pub input: String,
    /// Compiler-resolved S3 path of that producer's output
    /// (`<base_path>/<pipeline>/<version>/<input>/`).
    pub input_path: String,
    /// The producer's own name — the signal filter this step's worker
    /// subscribes to for wake-up on the coordinator's internal signals
    /// topic. Always equal to `input` today (every producer's automatic
    /// wake-up signal is keyed by its own name); kept as its own field
    /// rather than reusing `input` because a future block (e.g. `split`)
    /// may key its signal differently than its bare producer name.
    pub input_signal: String,
    /// Compiler-resolved S3 path this step writes its own output under
    /// (`<base_path>/<pipeline>/<version>/<name>/`).
    pub output_path: String,
    #[serde(default)]
    pub output: OutputConfig,
    /// Where the pod fetches the function body from at startup.
    pub code: CodeSource,
    /// Dotted path to the callable inside the loaded module, e.g. "steps.enrich.run".
    pub entrypoint: String,
    #[serde(default)]
    pub scaling: ScalingConfig,
    #[serde(default)]
    pub resources: PodResources,
    /// User-declared env vars (a DSL `env { ... }` block) injected into the
    /// worker pod alongside canoe's own `CANOE_*` ones — same purpose as
    /// `UvStep::env`, see its docs.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// When set, `canoe_worker/main.py` buffers up to `size` records (or
    /// `flush_interval_secs`, whichever comes first) and calls the step's
    /// function once per batch with the whole list, instead of once per
    /// message — see `BatchConfig`. Only valid for a `python { code =
    /// ... }` escape-hatch step; the DSL compiler rejects it on
    /// declarative ops (which always compile to a fixed per-record
    /// function) or any non-Python engine.
    #[serde(default)]
    pub batch: Option<BatchConfig>,
    /// Zero or more snapshot joins against a node in a *different* `.hcl`
    /// file — see `LookupConfig`. Only present when the step declared at
    /// least one `lookup {}` block; when non-empty, `canoe_worker/main.py`
    /// calls `run()` with an extra `lookups: dict[str, list[dict]]`
    /// argument instead of today's single-arg call.
    #[serde(default)]
    pub lookups: Vec<LookupConfig>,
    /// Zero or more `encrypt { fields = [...]; key = "..." }` blocks — see
    /// `EncryptConfig`.
    #[serde(default)]
    pub encrypt: Vec<EncryptConfig>,
    /// Zero or more AWS Secrets Manager entries this step's pod should
    /// have mounted as files — see `TaskConfig::secrets`'s docs. Fully
    /// automatic (a file mount, not crypto), unlike `encrypt`.
    #[serde(default)]
    pub secrets: Vec<String>,
    /// Ordering-only dependency (no data consumption) on another
    /// batch-mode step/task/adapter or pipeline name — this step's Job
    /// won't fire until every name here has been *applied*
    /// (`Ctx.activated`). See `TaskConfig::depends_on`'s docs (same
    /// mechanism, extended from `task`-only to `step` here).
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// Same as `depends_on`, but a real *completion* gate — see
    /// `TaskConfig::depends_on_complete`'s docs.
    #[serde(default)]
    pub depends_on_complete: Vec<String>,
}

/// A resolved `encrypt { fields = [...]; key = "..." }` block: `fields`
/// (by name, on this node's own output row) get replaced with a
/// self-describing ciphertext string — `canoe:enc:v1:<key>:<base64(nonce
/// || ciphertext+tag)>` (AES-256-GCM, AAD bound to the field name,
/// plaintext = the JSON encoding of the original value so its type
/// round-trips through decrypt) — before the row is written. `key` names
/// an env var, `CANOE_ENCRYPT_KEY_<KEY UPPERCASED>`, holding a
/// base64-encoded 32-byte key; resolved independently by whichever
/// process is encrypting or decrypting (the coordinator's own env for its
/// sink-writing path, each pod's own env for worker runtimes) — never
/// carried in this config itself. See `crates/canoe-sink/src/crypto.rs`
/// (encrypt only — the coordinator never needs to decrypt) and
/// `canoe_worker/crypto.py` (encrypt + decrypt; duplicated verbatim into
/// `workers/spark-runner`).
///
/// Because the value is self-describing (not schema.avsc metadata), it
/// travels correctly through every transport and boundary the rest of
/// this migration created without any extra plumbing: Kafka-topic
/// payloads (Spark steps' primary I/O, adapters), `Checkpointer`/`ScdView`
/// compaction (which never inspect field types, so ciphertext just folds
/// through as an opaque string), and cross-file `lookup` reads (no shared
/// compile-time knowledge needed to know a looked-up field is encrypted —
/// the value announces it itself).
///
/// Deliberately **not** inherited across steps: a step that reads a
/// decrypted field and writes it back out is plaintext on output unless
/// it declares its own `encrypt` block for that field — canoe never tries
/// to infer "this was encrypted upstream, keep it encrypted," since a
/// step's own code may rename/derive/drop fields in ways canoe can't
/// trace. Also: never encrypt a field also used as a
/// `checkpoint.keys`/`scd.keys`/`scd.track_fields` entry on the same
/// sink — AES-GCM's random nonce means the same plaintext encrypts to
/// different ciphertext every time, silently breaking dedup/change
/// equality checks on that field; `canoe-dsl` rejects this combination at
/// compile time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptConfig {
    pub fields: Vec<String>,
    pub key: String,
}

/// Configures how an S3-transport node (`PythonStep`/`UvStep`) batches and
/// partitions its own output — the same knobs `SinkConfig::Parquet` has
/// always offered a sink, generalized to every node now that every node
/// writes runs the same way (`crate::run_writer` server-side /
/// `output_writer.py` worker-side). Reuses `SinkConfig::Parquet`'s
/// existing `default_batch_size`/`default_flush_interval_secs` functions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputConfig {
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_flush_interval_secs")]
    pub flush_interval_secs: u64,
    #[serde(default)]
    pub partition_by: Vec<String>,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            flush_interval_secs: default_flush_interval_secs(),
            partition_by: Vec::new(),
        }
    }
}

/// Today's pre-migration `PythonStep` shape, verbatim: Kafka-topic
/// transport, a genuinely continuous process (a persistent Pod, no
/// backlog-draining Job) with no natural "done." Used for two things: (1)
/// `dynamic_route`'s per-discovered-value pods, which stay Kafka-topic-based
/// on principle — each gets its own dedicated topic the coordinator
/// provisions at discovery time, which doesn't fit the "producer name
/// resolves to one shared S3 path" model, and (2), now that `mode` is a
/// first-class DSL attribute, a regular `step { engine = "python"; mode =
/// "streaming" }` — see `StepConfig::KafkaPython`. `dynamic_router.rs`
/// still constructs these directly at runtime rather than through the DSL
/// compiler (a dynamic_route's tasks aren't known until discovery), and
/// hands them straight to `activation::start_kafka_python_step`, same as
/// before; a regular streaming-mode step reaches the same function via the
/// `StepConfig::KafkaPython` variant instead.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaPythonStep {
    pub name: String,
    pub input_topic: String,
    pub output_topic: String,
    pub code: CodeSource,
    pub entrypoint: String,
    #[serde(default)]
    pub scaling: ScalingConfig,
    #[serde(default)]
    pub resources: PodResources,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub batch: Option<BatchConfig>,
}

/// `def run(records: list[dict]) -> list[dict] | dict | None` instead of
/// the default `def run(record: dict) -> ...` — the whole point of
/// batching is amortizing per-call overhead (a network round trip inside
/// the function, a vectorized computation) across many records at once,
/// which a one-record-at-a-time contract can't express.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchConfig {
    pub size: usize,
    #[serde(default = "default_batch_flush_interval_secs")]
    pub flush_interval_secs: u64,
}

fn default_batch_flush_interval_secs() -> u64 {
    5
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CodeSource {
    /// Inline source code, injected into the pod via a mounted ConfigMap/Secret.
    Inline { source: String },
    /// Fetched by the worker at startup.
    S3 { bucket: String, key: String },
    Git { repo: String, r#ref: String, path: String },
}

/// `spark_sql { mode = "batch" }` (the default) — S3+signal transport, one-
/// shot: fires as a K8s Job, `spark.read.parquet(...)` reads unprocessed
/// `run_id=` backlog under `input_path`, writes its own output under
/// `output_path` through the *same* `run_id=`/`schema.avsc`/`latest.json`
/// convention every other node uses (a new Python-side writer in
/// `workers/spark-runner`, mirroring `canoe_worker/output_writer.py`), then
/// exits — Job completion is what makes this batch, not a pod that loops
/// forever. See `KafkaSparkSqlStep` for `mode = "streaming"`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SparkSqlStep {
    pub name: String,
    /// Name of the producer this step reads from (see `StepConfig::input_name`).
    pub input: String,
    /// Compiler-resolved S3 path of that producer's output.
    pub input_path: String,
    /// The producer's own name — the signal filter this step's Job
    /// subscribes to for wake-up (see `PythonStep::input_signal`'s docs).
    pub input_signal: String,
    /// Compiler-resolved S3 path this step writes its own output under.
    pub output_path: String,
    #[serde(default)]
    pub output: OutputConfig,
    pub sql: String,
    #[serde(default)]
    pub spark: SparkJobConfig,
    /// Zero or more snapshot joins against a node in a *different* `.hcl`
    /// file — see `LookupConfig`. Each is registered as its own temp view
    /// (by `lookup.name`) alongside the main input's fixed `"input"` view,
    /// so the step's `sql` can join across them with ordinary `SELECT ...
    /// FROM input JOIN <lookup_name> ON ...`.
    #[serde(default)]
    pub lookups: Vec<LookupConfig>,
    /// Zero or more `encrypt { fields = [...]; key = "..." }` blocks — see
    /// `EncryptConfig`.
    #[serde(default)]
    pub encrypt: Vec<EncryptConfig>,
    /// Zero or more AWS Secrets Manager entries this step's driver pod
    /// should have mounted as files — see `TaskConfig::secrets`'s docs.
    /// `canoe-spark` wires the CSI mount in via spark-submit `--conf`
    /// flags rather than a pod spec it authors directly — see
    /// `SparkLauncher::spark_submit_args`'s docs for why this, unlike
    /// python/uv steps, keeps using `canoe-spark`'s own ServiceAccount
    /// instead of switching to a dedicated one.
    #[serde(default)]
    pub secrets: Vec<String>,
    /// Ordering-only dependency — see `PythonStep::depends_on`'s docs.
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// Real completion gate — see `PythonStep::depends_on_complete`'s docs.
    #[serde(default)]
    pub depends_on_complete: Vec<String>,
}

/// `spark_sql { mode = "streaming" }` — today's `SparkSqlStep` shape,
/// verbatim, kept under its own name: Kafka-topic transport, a genuinely
/// continuous `spark.readStream` query with no natural "done." Mirrors why
/// `KafkaPythonStep` exists — see its docs. Deliberately not the default
/// for new pipelines' `spark_sql` steps' underlying *struct* (batch is),
/// but is still `spark_sql`'s default *mode* (preserves every existing
/// pipeline's behavior unchanged).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSparkSqlStep {
    pub name: String,
    pub input_topic: String,
    pub output_topic: String,
    pub sql: String,
    #[serde(default)]
    pub spark: SparkJobConfig,
    #[serde(default)]
    pub lookups: Vec<LookupConfig>,
    #[serde(default)]
    pub encrypt: Vec<EncryptConfig>,
    #[serde(default)]
    pub secrets: Vec<String>,
}

/// `spark_dataframe { mode = "batch" }` (the default) — see `SparkSqlStep`'s
/// docs; identical batch semantics, PySpark DataFrame code instead of SQL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SparkDataFrameStep {
    pub name: String,
    pub input: String,
    pub input_path: String,
    pub input_signal: String,
    pub output_path: String,
    #[serde(default)]
    pub output: OutputConfig,
    /// PySpark source defining `def transform(df: DataFrame) -> DataFrame`.
    pub code: CodeSource,
    #[serde(default)]
    pub spark: SparkJobConfig,
    /// Zero or more snapshot joins against a node in a *different* `.hcl`
    /// file — see `LookupConfig` and `SparkSqlStep::lookups`. Passed as
    /// extra DataFrame arguments to `transform()`.
    #[serde(default)]
    pub lookups: Vec<LookupConfig>,
    /// Zero or more `encrypt { fields = [...]; key = "..." }` blocks — see
    /// `EncryptConfig`.
    #[serde(default)]
    pub encrypt: Vec<EncryptConfig>,
    /// Zero or more AWS Secrets Manager entries this step's driver pod
    /// should have mounted as files — see `SparkSqlStep::secrets`'s docs.
    #[serde(default)]
    pub secrets: Vec<String>,
    /// Ordering-only dependency — see `PythonStep::depends_on`'s docs.
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// Real completion gate — see `PythonStep::depends_on_complete`'s docs.
    #[serde(default)]
    pub depends_on_complete: Vec<String>,
}

/// `spark_dataframe { mode = "streaming" }` — today's `SparkDataFrameStep`
/// shape, verbatim. See `KafkaSparkSqlStep`'s docs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSparkDataFrameStep {
    pub name: String,
    pub input_topic: String,
    pub output_topic: String,
    pub code: CodeSource,
    #[serde(default)]
    pub spark: SparkJobConfig,
    #[serde(default)]
    pub lookups: Vec<LookupConfig>,
    #[serde(default)]
    pub encrypt: Vec<EncryptConfig>,
    #[serde(default)]
    pub secrets: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SparkJobConfig {
    /// Container image used for both the driver launcher pod and executors.
    #[serde(default = "default_spark_image")]
    pub image: String,
    #[serde(default = "default_executor_instances")]
    pub executor_instances: u32,
    #[serde(default = "default_executor_cores")]
    pub executor_cores: u32,
    #[serde(default = "default_executor_memory")]
    pub executor_memory: String,
    #[serde(default = "default_driver_memory")]
    pub driver_memory: String,
    /// Extra `--conf key=value` pairs passed to spark-submit.
    #[serde(default)]
    pub extra_conf: Vec<(String, String)>,
    /// This step's own `spark { event_log { ... } }` override, if any —
    /// carried through **verbatim** from the DSL, not resolved against
    /// `GlobalConfig::spark`'s default (`canoe-dsl`'s compiler has no
    /// access to the coordinator's `--global-config` file at compile
    /// time — pipelines compile independently of any running
    /// coordinator). The actual merge happens at Spark-launch time, in
    /// `canoe-coordinator`'s `activation::resolve_event_log`. `None`
    /// means "this step declared no `event_log { ... }` block at all,"
    /// not "explicitly disabled" — see `EventLogOverride`'s docs for why
    /// that distinction matters.
    #[serde(default)]
    pub event_log_override: Option<EventLogOverride>,
}

/// A step's own `spark { event_log { ... } }` block — always optional
/// per-field (unlike `EventLogConfig`, the global default, where both
/// fields are required) specifically so "this step said nothing about
/// `enabled`/`base_path`" is distinguishable from "this step explicitly
/// set it to `false`/some value" during the global+override merge in
/// `canoe-coordinator::activation::resolve_event_log`. A step overriding
/// only `enabled = false` (leaving `base_path` unset) must still inherit
/// nothing for `base_path` — the merge just doesn't need it once the
/// effective `enabled` is `false`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct EventLogOverride {
    pub enabled: Option<bool>,
    pub base_path: Option<String>,
}

/// The coordinator's optional `--global-config` YAML file — see
/// `crates/canoe-coordinator/src/main.rs`'s `Cli::global_config` docs.
/// Operator/deployment-scoped settings that need to stay consistent
/// across every pipeline one coordinator runs, distinct from the
/// per-pipeline `PipelineConfig` YAML `--config` loads. Deliberately a
/// small, narrow struct today (just `spark`) but shaped to grow
/// additional top-level sections later without a breaking change —
/// `#[serde(default)]` throughout, so an older/partial file (or an empty
/// one) still parses.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GlobalConfig {
    #[serde(default)]
    pub spark: Option<SparkDefaults>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SparkDefaults {
    #[serde(default)]
    pub event_log: Option<EventLogConfig>,
}

/// The operator-scoped default for Spark event logging — unlike
/// `EventLogOverride`, both fields are required here: this is the actual
/// base value a step's override merges against, not itself a sparse
/// override, so there's no "unset vs. explicit" ambiguity to preserve.
/// See the module-level plan this shipped with (Spark event logging to
/// S3 + a coordinator-level global config file) for the full design:
/// canoe only *configures* `spark.eventLog.*` + the S3A credentials
/// Spark's own connector needs — running/pointing a Spark History Server
/// at `base_path` is left entirely to the user, not a canoe-managed
/// service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventLogConfig {
    pub enabled: bool,
    /// Recommend an `s3a://` URI specifically (not canoe's usual `s3://`
    /// convention elsewhere in this config, e.g. `StorageConfig::base_path`)
    /// — Spark's bundled Hadoop S3 connector registers the `s3a` scheme,
    /// not `s3`; pointing `spark.eventLog.dir` at a bare `s3://` URI
    /// would need an extra `spark.hadoop.fs.s3.impl` scheme-alias conf
    /// this feature does not add. See `canoe-spark`'s `spark_submit_args`
    /// docs for the exact `spark.hadoop.fs.s3a.*` confs this resolves to.
    pub base_path: String,
}

impl GlobalConfig {
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)?;
        Self::parse(&raw)
    }

    pub fn parse(raw: &str) -> anyhow::Result<Self> {
        Ok(serde_yaml::from_str(raw)?)
    }
}

fn default_spark_image() -> String {
    "canoe/spark-runner:latest".to_string()
}
fn default_executor_instances() -> u32 {
    2
}
fn default_executor_cores() -> u32 {
    1
}
fn default_executor_memory() -> String {
    "2g".to_string()
}
fn default_driver_memory() -> String {
    "1g".to_string()
}

impl Default for SparkJobConfig {
    fn default() -> Self {
        Self {
            image: default_spark_image(),
            executor_instances: default_executor_instances(),
            executor_cores: default_executor_cores(),
            executor_memory: default_executor_memory(),
            driver_memory: default_driver_memory(),
            extra_conf: Vec::new(),
            event_log_override: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkStep {
    pub name: String,
    /// Name of the producer this sink reads from (see `StepConfig::input_name`).
    pub input: String,
    /// Compiler-resolved S3 path of that producer's output.
    pub input_path: String,
    /// The producer's own name — the signal filter the sink-runner
    /// subscribes to for wake-up (see `PythonStep::input_signal`'s docs).
    pub input_signal: String,
    pub sink: SinkConfig,
    /// Published to the coordinator's internal signals topic every time
    /// this sink successfully flushes a batch. Other chains can name this
    /// signal in their `trigger.signal` to start when this write happens
    /// — see `TriggerConfig`.
    #[serde(default)]
    pub signal: Option<String>,
    /// Materializes a deduplicated, compacted view of this sink's
    /// `run_id=*` history after every successful flush — see
    /// `CheckpointConfig` and `crates/canoe-sink/src/checkpoint.rs`. Only
    /// meaningful for `SinkConfig::Parquet`; the DSL compiler rejects it
    /// on any other format.
    #[serde(default)]
    pub checkpoint: Option<CheckpointConfig>,
    /// Materializes a slowly-changing-dimension (Type 2) view of this
    /// sink's `run_id=*` history after every successful flush — see
    /// `ScdViewConfig` and `crates/canoe-sink/src/scd_view.rs`. Same
    /// `SinkConfig::Parquet`-only restriction as `checkpoint`; not to be
    /// confused with the unrelated `SinkConfig::Scd` sink *format*.
    #[serde(default)]
    pub scd: Option<ScdViewConfig>,
    /// Zero or more `encrypt { fields = [...]; key = "..." }` blocks — see
    /// `EncryptConfig`. Applied to this sink's own output fields by
    /// `crates/canoe-sink/src/run_writer.rs::write_run` before the schema
    /// is inferred / the Parquet is written.
    #[serde(default)]
    pub encrypt: Vec<EncryptConfig>,
    /// Per-column metadata (labels, descriptions, sensitivity/
    /// classification tags, or any other free-form keys), resolved at
    /// compile time from a `metadata = "column-metadata.yaml"` file
    /// adjacent to the `.hcl` file — see `canoe-dsl`'s
    /// `codegen::resolve_column_metadata`. Embedded as a `canoe.metadata`
    /// custom property per matching field in every run's `schema.avsc` by
    /// `crates/canoe-sink/src/avro.rs::arrow_schema_to_avsc` — the
    /// already-existing per-run schema file is "persistent and
    /// accessible near the written data" for free, so no separate
    /// sidecar file is written for this. Keyed by column name; each
    /// value is an open map, not a fixed struct, since canoe doesn't need
    /// to know every metadata key a YAML file might declare.
    #[serde(default)]
    pub column_metadata: BTreeMap<String, BTreeMap<String, serde_json::Value>>,
}

/// Compacts a `SinkConfig::Parquet` dataset's `run_id=*` runs into one
/// deduplicated `checkpoint=<ulid>/` directory, keyed by `keys`: runs are
/// folded in oldest to newest, and for rows sharing the same `keys`
/// values, whichever run processed last wins — see the module docs on
/// `crates/canoe-sink/src/checkpoint.rs::Checkpointer` for the full
/// algorithm, including how an existing checkpoint lets a later pass
/// start from there instead of replaying every run from the beginning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointConfig {
    pub keys: Vec<String>,
}

/// Rolls a `SinkConfig::Parquet` dataset's `run_id=*` runs up into a
/// Type-2 slowly-changing-dimension view under `scd=<ulid>/`, keyed by
/// `keys`: each key's row history is preserved (`_scd_is_current`,
/// `_scd_valid_from`/`_scd_valid_to`, `_scd_version` columns added to
/// every row) rather than collapsed to the latest value the way
/// `checkpoint` is — see `crates/canoe-sink/src/scd_view.rs::ScdView` for
/// the full algorithm, which otherwise mirrors `Checkpointer`'s
/// incremental-from-`through_run_id` behavior exactly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScdViewConfig {
    pub keys: Vec<String>,
    /// Fields to watch for changes when deciding whether to version a
    /// key's row. Left empty (the default): every field on the row
    /// except `keys` and the `_scd_*` bookkeeping columns is watched.
    #[serde(default)]
    pub track_fields: Vec<String>,
    /// A field on the incoming row holding its own "as of" timestamp
    /// (e.g. an upstream `updated_at`), used for `_scd_valid_from`/
    /// `_scd_valid_to` instead of wall-clock processing time. Unset by
    /// default — versions are then timestamped at the moment this pass
    /// runs, not when the underlying change actually happened upstream.
    #[serde(default)]
    pub effective_field: Option<String>,
    /// SCD Type — only Type 2 (full version history) is implemented;
    /// canoe-dsl rejects any other value at compile time (a Type 1
    /// "overwrite in place" table is exactly what `checkpoint` already
    /// gives you).
    #[serde(default = "default_scd_view_type")]
    pub scd_type: u8,
}

fn default_scd_view_type() -> u8 {
    2
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "format", rename_all = "snake_case")]
pub enum SinkConfig {
    Parquet {
        /// Object store URI under which every dataset this sink ever
        /// writes is rooted, e.g. `s3://bucket/prefix` — usually shared
        /// across many sinks pointed at the same datalake. Each write
        /// lands under `<base_path>/dataset_id=<dataset_id>/run_id=<run_id>/`,
        /// never directly in `base_path` itself; see `ParquetSink` in
        /// `crates/canoe-sink`.
        base_path: String,
        /// Identifies this sink's dataset within `base_path`. Distinct
        /// sinks that intentionally write the same logical dataset (e.g.
        /// two chains landing the same table) should share a
        /// `dataset_id` so their runs interleave under one `latest.json`;
        /// otherwise give each sink its own.
        dataset_id: String,
        #[serde(default)]
        partition_by: Vec<String>,
        #[serde(default = "default_batch_size")]
        batch_size: usize,
        #[serde(default = "default_flush_interval_secs")]
        flush_interval_secs: u64,
    },
    Iceberg {
        catalog: String,
        table: String,
        #[serde(default)]
        partition_by: Vec<String>,
    },
    Scd {
        catalog: String,
        table: String,
        keys: Vec<String>,
        /// SCD Type (currently only Type 2 is supported by the sink).
        #[serde(default = "default_scd_type")]
        scd_type: u8,
    },
}

fn default_batch_size() -> usize {
    10_000
}
fn default_flush_interval_secs() -> u64 {
    30
}
fn default_scd_type() -> u8 {
    2
}

/// Replica scaling policy for a step's worker pod set (see
/// `crates/canoe-k8s/src/pod_manager.rs` — plain coordinator-managed
/// `Pod`s, not a Deployment; see that module's docs for why). The
/// coordinator watches consumer-group lag on `input_topic` and scales
/// replicas within [min_replicas, max_replicas] to keep lag-per-replica
/// near `target_lag_per_replica`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalingConfig {
    #[serde(default = "default_min_replicas")]
    pub min_replicas: u32,
    #[serde(default = "default_max_replicas")]
    pub max_replicas: u32,
    #[serde(default = "default_target_lag")]
    pub target_lag_per_replica: i64,
    #[serde(default = "default_cooldown_secs")]
    pub cooldown_secs: u64,
    /// How many times in a row a replacement pod may fail *fast* (within
    /// `crates/canoe-k8s/src/pod_manager.rs`'s fast-failure window of its
    /// own start) before the coordinator's circuit breaker stops creating
    /// new ones for this step and waits for a cooldown instead — see
    /// README.md's "Step pods, not Deployments". A pod with
    /// `restartPolicy: Never` never restarts in place on its own (that's
    /// the whole point — see the README section this links to), so
    /// without this, a deterministic bug would otherwise mean the
    /// coordinator recreates a doomed pod forever, just on a slower
    /// cadence than kubelet's own crash-loop backoff would have used.
    #[serde(default = "default_backoff_limit")]
    pub backoff_limit: u32,
}

fn default_min_replicas() -> u32 {
    1
}
fn default_max_replicas() -> u32 {
    10
}
fn default_target_lag() -> i64 {
    5_000
}
fn default_cooldown_secs() -> u64 {
    60
}

impl Default for ScalingConfig {
    fn default() -> Self {
        Self {
            min_replicas: default_min_replicas(),
            max_replicas: default_max_replicas(),
            target_lag_per_replica: default_target_lag(),
            cooldown_secs: default_cooldown_secs(),
            backoff_limit: default_backoff_limit(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PodResources {
    #[serde(default = "default_cpu_request")]
    pub cpu_request: String,
    #[serde(default = "default_mem_request")]
    pub mem_request: String,
    #[serde(default = "default_cpu_limit")]
    pub cpu_limit: String,
    #[serde(default = "default_mem_limit")]
    pub mem_limit: String,
}

fn default_cpu_request() -> String {
    "250m".to_string()
}
fn default_mem_request() -> String {
    "256Mi".to_string()
}
fn default_cpu_limit() -> String {
    "1".to_string()
}
fn default_mem_limit() -> String {
    "512Mi".to_string()
}

impl Default for PodResources {
    fn default() -> Self {
        Self {
            cpu_request: default_cpu_request(),
            mem_request: default_mem_request(),
            cpu_limit: default_cpu_limit(),
            mem_limit: default_mem_limit(),
        }
    }
}

impl PipelineConfig {
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)?;
        Self::parse(&raw)
    }

    pub fn parse(raw: &str) -> anyhow::Result<Self> {
        let cfg: PipelineConfig = serde_yaml::from_str(raw)?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> anyhow::Result<()> {
        for chain in &self.chains {
            if chain.steps.is_empty() {
                anyhow::bail!("chain '{}' has no steps", chain.name);
            }
            let first = &chain.steps[0];
            if first.input_name() != chain.source {
                anyhow::bail!(
                    "chain '{}': first step '{}' must read from source '{}', got '{}'",
                    chain.name,
                    first.name(),
                    chain.source,
                    first.input_name()
                );
            }
            for pair in chain.steps.windows(2) {
                let (prev, next) = (&pair[0], &pair[1]);
                match prev.output_ref() {
                    Some(out) if out == next.input_ref() => {}
                    Some(out) => anyhow::bail!(
                        "chain '{}': step '{}' outputs to '{}' but next step '{}' reads from '{}' \
                         (mismatched output/input, or a transport mismatch — S3-transport steps can't \
                         wire directly into a still-Kafka-topic-based step or vice versa)",
                        chain.name,
                        prev.name(),
                        out,
                        next.name(),
                        next.input_ref()
                    ),
                    None => anyhow::bail!(
                        "chain '{}': step '{}' is a sink and cannot be followed by '{}'",
                        chain.name,
                        prev.name(),
                        next.name()
                    ),
                }
            }
        }
        for adapter in &self.adapters {
            match (&adapter.schedule.interval_secs, &adapter.schedule.cron) {
                (Some(_), Some(_)) => anyhow::bail!(
                    "adapter '{}': schedule must set exactly one of interval_secs or cron, got both",
                    adapter.name
                ),
                (None, None) => anyhow::bail!(
                    "adapter '{}': schedule must set exactly one of interval_secs or cron",
                    adapter.name
                ),
                (Some(secs), None) if *secs == 0 => anyhow::bail!(
                    "adapter '{}': schedule.interval_secs must be greater than 0",
                    adapter.name
                ),
                (None, Some(expr)) => {
                    // cron crate expects a leading seconds field; adapters
                    // author standard 5-field cron (minute granularity),
                    // so prepend "0" the same way the DSL/coordinator do.
                    let six_field = format!("0 {expr}");
                    six_field.parse::<cron::Schedule>().map_err(|e| {
                        anyhow::anyhow!("adapter '{}': invalid cron expression '{expr}': {e}", adapter.name)
                    })?;
                }
                (Some(_), None) => {}
            }
        }
        for task in &self.tasks {
            if task.depends_on.is_empty() {
                anyhow::bail!("task '{}': depends_on must name at least one step, pipeline, or task", task.name);
            }
            if task.depends_on.contains(&task.name) {
                anyhow::bail!("task '{}': depends_on cannot name the task's own name", task.name);
            }
        }
        Ok(())
    }

    /// Flattened list of all (chain_name, step) pairs across the pipeline.
    pub fn all_steps(&self) -> impl Iterator<Item = (&str, &StepConfig)> {
        self.chains
            .iter()
            .flat_map(|c| c.steps.iter().map(move |s| (c.name.as_str(), s)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_example_pipeline() {
        let yaml = r#"
storage:
  base_path: s3://canoe-lake
  version: v1
chains:
  - name: orders
    pipeline: orders
    source: orders_adapter
    steps:
      - type: python
        name: validate
        input: orders_adapter
        input_path: s3://canoe-lake/orders/v1/orders_adapter
        input_signal: orders_adapter
        output_path: s3://canoe-lake/orders/v1/validate
        code:
          kind: s3
          bucket: canoe-steps
          key: orders/validate.py
        entrypoint: steps.validate.run
      - type: sink
        name: land
        input: validate
        input_path: s3://canoe-lake/orders/v1/validate
        input_signal: validate
        sink:
          format: parquet
          base_path: s3://canoe-lake
          dataset_id: orders
          partition_by: ["processed_date"]
"#;
        let cfg = PipelineConfig::parse(yaml).expect("valid config");
        assert_eq!(cfg.chains.len(), 1);
        assert_eq!(cfg.chains[0].steps.len(), 2);
    }

    #[test]
    fn rejects_broken_chain() {
        let yaml = r#"
storage:
  base_path: s3://canoe-lake
  version: v1
chains:
  - name: broken
    pipeline: broken
    source: a
    steps:
      - type: sink
        name: land
        input: b
        input_path: s3://canoe-lake/broken/v1/b
        input_signal: b
        sink:
          format: parquet
          base_path: s3://bucket
          dataset_id: x
"#;
        assert!(PipelineConfig::parse(yaml).is_err());
    }

    #[test]
    fn parses_adapter_with_interval_schedule() {
        let yaml = r#"
chains: []
adapters:
  - name: stripe_charges
    topic: stripe.charges.raw
    code:
      kind: inline
      source: "print('hi')"
    schedule:
      interval_secs: 300
"#;
        let cfg = PipelineConfig::parse(yaml).expect("valid config");
        assert_eq!(cfg.adapters.len(), 1);
        assert_eq!(cfg.adapters[0].schedule.interval_secs, Some(300));
    }

    #[test]
    fn rejects_adapter_with_no_schedule() {
        let yaml = r#"
chains: []
adapters:
  - name: stripe_charges
    topic: stripe.charges.raw
    code:
      kind: inline
      source: "print('hi')"
    schedule: {}
"#;
        assert!(PipelineConfig::parse(yaml).is_err());
    }

    #[test]
    fn rejects_adapter_with_invalid_cron() {
        let yaml = r#"
chains: []
adapters:
  - name: stripe_charges
    topic: stripe.charges.raw
    code:
      kind: inline
      source: "print('hi')"
    schedule:
      cron: "not a cron expression"
"#;
        assert!(PipelineConfig::parse(yaml).is_err());
    }

    #[test]
    fn accepts_adapter_with_valid_cron() {
        let yaml = r#"
chains: []
adapters:
  - name: stripe_charges
    topic: stripe.charges.raw
    code:
      kind: inline
      source: "print('hi')"
    schedule:
      cron: "*/5 * * * *"
"#;
        assert!(PipelineConfig::parse(yaml).is_ok());
    }

    #[test]
    fn parses_topic_defaults_and_managed_topics() {
        let yaml = r#"
chains: []
topic_defaults:
  retention_ms: 604800000
  archive:
    base_path: s3://canoe-archive
    interval_secs: 600
managed_topics:
  - pipeline: orders
    task: validate
    topic: _canoe.orders.validate
"#;
        let cfg = PipelineConfig::parse(yaml).expect("valid config");
        let defaults = cfg.topic_defaults.expect("topic_defaults should be present");
        assert_eq!(defaults.retention_ms, Some(604_800_000));
        let archive = defaults.archive.expect("archive should be present");
        assert_eq!(archive.base_path, "s3://canoe-archive");
        assert_eq!(archive.interval_secs, 600);
        assert_eq!(cfg.managed_topics.len(), 1);
        assert_eq!(cfg.managed_topics[0].task, "validate");
    }

    #[test]
    fn topic_defaults_and_managed_topics_default_when_absent() {
        // Pre-existing configs without either field must still parse.
        let yaml = r#"
chains: []
"#;
        let cfg = PipelineConfig::parse(yaml).expect("valid config");
        assert!(cfg.topic_defaults.is_none());
        assert!(cfg.managed_topics.is_empty());
    }

    #[test]
    fn global_config_round_trips_through_serde_yaml() {
        let yaml = r#"
spark:
  event_log:
    enabled: true
    base_path: s3a://canoe-lake/_spark-events
"#;
        let cfg = GlobalConfig::parse(yaml).expect("valid global config");
        let event_log = cfg.spark.clone().expect("spark section").event_log.expect("event_log section");
        assert!(event_log.enabled);
        assert_eq!(event_log.base_path, "s3a://canoe-lake/_spark-events");

        let round_tripped: GlobalConfig = serde_yaml::from_str(&serde_yaml::to_string(&cfg).unwrap()).unwrap();
        assert_eq!(round_tripped.spark.unwrap().event_log.unwrap().base_path, "s3a://canoe-lake/_spark-events");
    }

    #[test]
    fn global_config_missing_sections_default_cleanly() {
        assert!(GlobalConfig::parse("").unwrap().spark.is_none());
        assert!(GlobalConfig::parse("spark: {}").unwrap().spark.unwrap().event_log.is_none());
    }
}
