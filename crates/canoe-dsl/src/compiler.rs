//! Resolves a parsed DSL file into the existing `canoe_config::PipelineConfig`
//! (the format `canoe-coordinator` already consumes).
//!
//! Every `step`/`route`/`dynamic_route`/`merge`/`sink`/`task` is a
//! discrete, root-level declaration (see `ast::RawFile`'s docs) — this
//! compiler builds *one* global graph of them, keyed by name, then for
//! each `pipeline` traces every `input` reference reachable from its
//! `source_topic` to discover which of those root-level blocks belong to
//! it. Concretely:
//!
//! 1. Naming an internal Kafka topic for every step/route-branch/merge's
//!    output, so authors reference each other **by name** instead of
//!    wiring topic strings by hand.
//! 2. Expanding each `route` block into one filter-only step per branch
//!    (plus a `default` branch, if declared, filtering out everything the
//!    named branches matched) — routing is just N independent consumer
//!    groups on the same topic, each keeping only the rows its predicate
//!    matches.
//! 3. Resolving `merge` blocks by pointing every merged producer's output
//!    at one shared topic — no runtime step is needed for a merge, it's
//!    pure topic aliasing.
//! 4. For each pipeline, enumerating every simple path from its
//!    `source_topic` to a sink as one `ChainConfig`. A step reachable from
//!    multiple branches (downstream of a merge) is duplicated across
//!    those chains; this is harmless because applying its K8s Deployment
//!    is idempotent, but it does mean chain count can grow with branch
//!    count — fine for the hand-authored pipelines this DSL targets, not
//!    meant to scale to machine-generated DAGs with hundreds of branches.
//! 5. Every step/route/dynamic_route/sink declared in the file must be
//!    reachable from at least one pipeline's `source_topic` — an orphaned
//!    block (a typo in an `input`, a block nobody wired up) is a compile
//!    error, not silently dropped.
//!
//! `task` blocks are compiled separately (`compile_task`): they're not
//! part of any pipeline's data flow at all, just validated so their
//! `depends_on` names resolve to something real.

use crate::ast::{
    RawAdapter, RawDynamicTask, RawEncrypt, RawFile, RawLookup, RawOp, RawOutput, RawResources, RawScaling,
    RawSparkConf, RawStorage, RawTask, RawTopics,
};
use crate::codegen;
use anyhow::{bail, Context};
use canoe_config::{
    AdapterConfig, AdapterSchedule, ArchiveConfig, BatchConfig, ChainConfig, CheckpointConfig, CodeSource,
    DynamicRouteStep, DynamicTaskTemplate, EncryptConfig, KafkaPythonStep, KafkaSparkDataFrameStep, KafkaSparkSqlStep,
    LookupConfig, ManagedTopic, OutputConfig, PipelineConfig, PodResources, PythonStep, ScalingConfig, ScdViewConfig,
    SinkConfig, SinkStep, SparkDataFrameStep, SparkJobConfig, SparkSqlStep, StepConfig, TaskConfig, TopicDefaults,
    TriggerConfig, UvStep,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

/// `base_dir` resolves every `file = ...` escape hatch (a step's
/// `python`/`spark_sql`/`spark_dataframe` block, an adapter's `script`, or
/// a task's `script`) relative to the `.hcl` source file's own directory —
/// `lib.rs`'s `compile_file` passes the file's parent; `compile_str`
/// defaults to the current directory.
pub fn compile(raw: &RawFile, base_dir: &Path) -> anyhow::Result<PipelineConfig> {
    validate_unique_names(raw)?;

    let (nodes, sinks) = compile_graph(raw, base_dir)?;
    validate_step_dependencies(&nodes, &depends_on_targets(raw))?;

    let mut chains = Vec::new();
    let mut managed_topics = Vec::new();
    let mut reached_steps: HashSet<String> = HashSet::new();
    let mut reached_sinks: HashSet<String> = HashSet::new();

    for pipeline in &raw.pipelines {
        let trigger = pipeline.schedule.as_ref().map(|s| TriggerConfig {
            signal: s.signal.clone(),
            cron: s.cron.clone(),
        });
        let resolved_source = resolve_pipeline_source(raw, &pipeline.source)?;
        let (pipeline_chains, pipeline_reached_steps, pipeline_reached_sinks) =
            build_chains(&pipeline.name, &resolved_source, &nodes, &sinks, trigger)
                .with_context(|| format!("compiling pipeline \"{}\"", pipeline.name))?;

        // Only Kafka-transport nodes have an internal topic worth
        // retention/archival management — S3-transport nodes (Python/Uv)
        // have no topic of their own any more, just an S3 path.
        for node in nodes
            .iter()
            .filter(|n| pipeline_reached_steps.contains(&n.name) && transport_of(&n.step) == Transport::Kafka)
        {
            managed_topics.push(ManagedTopic {
                pipeline: pipeline.name.clone(),
                task: node.name.clone(),
                topic: node.output_ref.clone(),
            });
        }

        reached_steps.extend(pipeline_reached_steps);
        reached_sinks.extend(pipeline_reached_sinks);
        chains.extend(pipeline_chains);
    }

    if let Some(orphan) = nodes.iter().find(|n| !reached_steps.contains(&n.name)) {
        bail!(
            "\"{}\" is declared but not reachable from any pipeline's source — nothing's `input` traces back \
             to it (check for a typo, or remove it if it's unused)",
            orphan.name
        );
    }
    if let Some(orphan) = sinks.iter().find(|s| !reached_sinks.contains(&s.name)) {
        bail!(
            "sink \"{}\" is declared but not reachable from any pipeline's source",
            orphan.name
        );
    }

    let mut adapters = Vec::new();
    for adapter in &raw.adapters {
        adapters.push(
            compile_adapter(adapter, base_dir, raw.storage.as_ref())
                .with_context(|| format!("compiling adapter \"{}\"", adapter.name))?,
        );
    }

    let depends_on_targets = depends_on_targets(raw);
    let mut tasks = Vec::new();
    for task in &raw.tasks {
        tasks.push(
            compile_task(task, base_dir, &depends_on_targets)
                .with_context(|| format!("compiling task \"{}\"", task.name))?,
        );
    }

    let topic_defaults = raw.topics.as_ref().map(compile_topics).transpose().context("compiling topics block")?;
    let storage = raw
        .storage
        .as_ref()
        .map(|s| canoe_config::StorageConfig {
            base_path: s.base_path.clone(),
            version: s.version.clone().unwrap_or_else(|| "v1".to_string()),
        });

    Ok(PipelineConfig {
        chains,
        adapters,
        tasks,
        topic_defaults,
        managed_topics,
        storage,
    })
}

/// Every step/route/dynamic_route/merge/sink/task/pipeline/adapter name
/// must be unique across the *whole file* (not just within a pipeline,
/// since none of them are nested inside one any more) — `input`/
/// `depends_on` reference these by bare name with no block-kind prefix to
/// disambiguate, so a collision would be genuinely ambiguous, not just a
/// style nit.
fn validate_unique_names(raw: &RawFile) -> anyhow::Result<()> {
    let mut seen: HashMap<String, &'static str> = HashMap::new();
    fn check(name: &str, kind: &'static str, seen: &mut HashMap<String, &'static str>) -> anyhow::Result<()> {
        if let Some(existing_kind) = seen.insert(name.to_string(), kind) {
            bail!(
                "name \"{name}\" is used by both a {existing_kind} and a {kind} — every step/route/dynamic_route/\
                 merge/sink/task/pipeline/adapter name must be unique across the whole file, since `input`/\
                 `depends_on` reference them by bare name"
            );
        }
        Ok(())
    }
    for s in &raw.steps {
        check(&s.name, "step", &mut seen)?;
    }
    for r in &raw.routes {
        check(&r.name, "route", &mut seen)?;
    }
    for dr in &raw.dynamic_routes {
        check(&dr.name, "dynamic_route", &mut seen)?;
    }
    for m in &raw.merges {
        check(&m.name, "merge", &mut seen)?;
    }
    for sk in &raw.sinks {
        check(&sk.name, "sink", &mut seen)?;
    }
    for t in &raw.tasks {
        check(&t.name, "task", &mut seen)?;
    }
    for p in &raw.pipelines {
        check(&p.name, "pipeline", &mut seen)?;
    }
    for a in &raw.adapters {
        check(&a.name, "adapter", &mut seen)?;
    }
    Ok(())
}

/// Names a `task`'s `depends_on` may validly reference — steps and
/// dynamic_routes (continuous Deployments, satisfied once applied),
/// pipelines (satisfied once every step in it is applied), and other
/// tasks (satisfied once that task's one-shot Job actually completes).
/// `route`/`merge`/`sink` are deliberately not included: a route branch
/// only exists as `route.branch`, not a single Deployment `depends_on`
/// could sensibly point at, and merges/sinks aren't independently
/// startable things either — see README.md's "Status / what's stubbed".
fn depends_on_targets(raw: &RawFile) -> HashSet<String> {
    let mut names = HashSet::new();
    for s in &raw.steps {
        names.insert(s.name.clone());
    }
    for dr in &raw.dynamic_routes {
        names.insert(dr.name.clone());
    }
    for p in &raw.pipelines {
        names.insert(p.name.clone());
    }
    for t in &raw.tasks {
        names.insert(t.name.clone());
    }
    names
}

/// Validates every step's `depends_on`/`depends_on_complete` (only
/// present on batch-mode `Python`/`SparkSql`/`SparkDataFrame` nodes —
/// every other variant's `reject_depends_on_outside_batch` already
/// guarantees these are empty for it, so this just skips them): each
/// name must be a known step/dynamic_route/pipeline/task and not the
/// step's own name (same rules `compile_task` already enforces for
/// `task.depends_on`), and a `depends_on_complete` entry specifically
/// must not name a streaming-mode step or `dynamic_route` — neither ever
/// completes, so canoe-dsl catches that authoring mistake here rather
/// than letting a step wait forever on a dependency that can never be
/// satisfied.
fn validate_step_dependencies(nodes: &[Node], known: &HashSet<String>) -> anyhow::Result<()> {
    let streaming_names: HashSet<&str> = nodes
        .iter()
        .filter(|n| {
            matches!(
                n.step,
                StepConfig::KafkaPython(_) | StepConfig::KafkaSparkSql(_) | StepConfig::KafkaSparkDataFrame(_) | StepConfig::DynamicRoute(_)
            )
        })
        .map(|n| n.name.as_str())
        .collect();

    for node in nodes {
        let (depends_on, depends_on_complete): (&[String], &[String]) = match &node.step {
            StepConfig::Python(s) => (&s.depends_on, &s.depends_on_complete),
            StepConfig::SparkSql(s) => (&s.depends_on, &s.depends_on_complete),
            StepConfig::SparkDataFrame(s) => (&s.depends_on, &s.depends_on_complete),
            _ => continue,
        };
        for dep in depends_on.iter().chain(depends_on_complete) {
            if !known.contains(dep) {
                bail!(
                    "step \"{}\": depends_on/depends_on_complete \"{dep}\" is not a known step, dynamic_route, \
                     pipeline, or task name (route/merge/sink names aren't valid depends_on targets)",
                    node.name
                );
            }
            if dep == &node.name {
                bail!("step \"{}\": depends_on/depends_on_complete cannot name the step's own name (\"{dep}\")", node.name);
            }
        }
        for dep in depends_on_complete {
            if streaming_names.contains(dep.as_str()) {
                bail!(
                    "step \"{}\": depends_on_complete names \"{dep}\", which is a streaming-mode step (or \
                     dynamic_route) and can never complete — only a batch-mode step or task has a \"done\" to \
                     depend on",
                    node.name
                );
            }
        }
    }
    Ok(())
}

fn compile_task(t: &RawTask, base_dir: &Path, known: &HashSet<String>) -> anyhow::Result<TaskConfig> {
    validate_secrets(&t.secrets, &format!("task \"{}\"", t.name))?;
    for dep in t.depends_on.iter().chain(&t.depends_on_complete) {
        if !known.contains(dep) {
            bail!(
                "depends_on/depends_on_complete \"{dep}\" is not a known step, dynamic_route, pipeline, or task \
                 name (route/merge/sink names aren't valid depends_on targets — see README.md's \"Tasks\")"
            );
        }
        if *dep == t.name {
            bail!("depends_on/depends_on_complete cannot name the task's own name (\"{dep}\")");
        }
    }
    Ok(TaskConfig {
        name: t.name.clone(),
        code: CodeSource::Inline {
            source: t.script.resolve(base_dir)?,
        },
        depends_on: t.depends_on.clone(),
        depends_on_complete: t.depends_on_complete.clone(),
        resources: t.resources.as_ref().map(to_config_resources).unwrap_or_default(),
        backoff_limit: t.backoff_limit.map(|n| n as u32).unwrap_or_else(canoe_config::default_backoff_limit),
        secrets: t.secrets.clone(),
    })
}

fn compile_topics(t: &RawTopics) -> anyhow::Result<TopicDefaults> {
    let retention_ms = t
        .retention
        .as_ref()
        .map(|r| -> anyhow::Result<i64> {
            let duration = humantime::parse_duration(&r.duration).with_context(|| {
                format!("topics.retention.duration \"{}\" is not a valid duration (try e.g. \"7d\", \"168h\")", r.duration)
            })?;
            Ok(duration.as_millis() as i64)
        })
        .transpose()?;

    let archive = t
        .archive
        .as_ref()
        .map(|a| -> anyhow::Result<ArchiveConfig> {
            let duration = humantime::parse_duration(&a.interval).with_context(|| {
                format!("topics.archive.interval \"{}\" is not a valid duration (try e.g. \"10m\", \"1h\")", a.interval)
            })?;
            Ok(ArchiveConfig {
                base_path: a.base_path.clone(),
                interval_secs: duration.as_secs(),
            })
        })
        .transpose()?;

    Ok(TopicDefaults { retention_ms, archive })
}

fn compile_adapter(a: &RawAdapter, base_dir: &Path, storage: Option<&RawStorage>) -> anyhow::Result<AdapterConfig> {
    let ctx = format!("adapter \"{}\"", a.name);
    validate_secrets(&a.secrets, &ctx)?;
    let mode = resolve_adapter_mode(a.mode.as_deref(), &ctx)?;
    let (topic, output_path, output) = match mode {
        Mode::Streaming => {
            if a.output.is_some() {
                bail!("{ctx}: `output {{ ... }}` is only valid for mode = \"batch\", not streaming");
            }
            let topic = a
                .topic
                .clone()
                .with_context(|| format!("{ctx}: mode = \"streaming\" requires `topic = \"...\"`"))?;
            (Some(topic), None, None)
        }
        Mode::Batch => {
            if a.topic.is_some() {
                bail!("{ctx}: `topic` is only valid for mode = \"streaming\" — a batch adapter writes to S3, not a Kafka topic");
            }
            (None, Some(adapter_output_path(storage, &a.name)?), Some(to_config_output(a.output.as_ref())))
        }
    };
    let schedule = match (&a.schedule.interval, &a.schedule.cron) {
        (Some(_), Some(_)) => {
            bail!("schedule must set exactly one of interval or cron, got both")
        }
        (None, None) => bail!("schedule must set exactly one of interval or cron"),
        (Some(interval), None) => {
            let duration = humantime::parse_duration(interval)
                .with_context(|| format!("schedule.interval \"{interval}\" is not a valid duration (try e.g. \"5m\", \"1h30m\", \"30s\")"))?;
            AdapterSchedule {
                interval_secs: Some(duration.as_secs()),
                cron: None,
            }
        }
        (None, Some(cron)) => AdapterSchedule {
            interval_secs: None,
            cron: Some(cron.clone()),
        },
    };

    Ok(AdapterConfig {
        name: a.name.clone(),
        topic,
        output_path,
        output,
        code: CodeSource::Inline {
            source: a.script.resolve(base_dir)?,
        },
        schedule,
        resources: a.resources.as_ref().map(to_config_resources).unwrap_or_default(),
        backoff_limit: a.backoff_limit.map(|n| n as u32).unwrap_or_else(canoe_config::default_backoff_limit),
        encrypt: to_config_encrypts(&a.encrypt),
        secrets: a.secrets.clone(),
    })
}

enum Engine {
    Python,
    SparkSql,
    SparkDataFrame,
    Uv,
}

/// Orthogonal to `Engine` — see `RawStep::mode`'s docs and the module doc
/// comment on `canoe_config`. `Batch`: S3+signal transport, one-shot Job.
/// `Streaming`: Kafka-topic transport, a continuous process.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Batch,
    Streaming,
}

/// Resolves a step's `mode` attribute against its `engine`'s default —
/// `"batch"` for `python`/`uv` (already S3-transport before `mode` existed
/// as a DSL attribute), `"streaming"` for `spark_sql`/`spark_dataframe`
/// (already Kafka-transport, and Spark's only option before `mode`
/// existed) — so every pre-existing pipeline compiles to the identical
/// resolved shape it did before this attribute was added. `uv` has no
/// streaming option: it already owns its entire consume/produce loop, so
/// there's nothing a `mode = "streaming"` would change about it.
fn resolve_mode(engine: &str, mode: Option<&str>, ctx: &str) -> anyhow::Result<Mode> {
    let default = match engine {
        "spark_sql" | "spark_dataframe" => Mode::Streaming,
        _ => Mode::Batch,
    };
    match mode {
        None => Ok(default),
        Some("batch") => Ok(Mode::Batch),
        Some("streaming") => {
            if engine == "uv" {
                bail!(
                    "{ctx}: engine=\"uv\" does not support mode=\"streaming\" — a uv step already owns its \
                     entire consume/produce loop, so only mode=\"batch\" is supported"
                );
            }
            Ok(Mode::Streaming)
        }
        Some(other) => bail!("{ctx}: unknown mode \"{other}\" (expected \"batch\" or \"streaming\")"),
    }
}

/// An adapter's `mode` resolution — separate from `resolve_mode` because
/// an adapter has no `engine` to default off of: it defaults to
/// `Streaming` unconditionally (every adapter was Kafka-topic-only before
/// `mode` existed, so this is the only default that keeps every
/// pre-existing adapter compiling to the identical resolved shape).
fn resolve_adapter_mode(mode: Option<&str>, ctx: &str) -> anyhow::Result<Mode> {
    match mode {
        None => Ok(Mode::Streaming),
        Some("batch") => Ok(Mode::Batch),
        Some("streaming") => Ok(Mode::Streaming),
        Some(other) => bail!("{ctx}: unknown mode \"{other}\" (expected \"batch\" or \"streaming\")"),
    }
}

/// Rejects an `env { ... }` key that would collide with canoe's own
/// reserved env vars (`CANOE_INPUT_TOPIC` etc. — see
/// `crates/canoe-k8s/src/deployment.rs`'s `build_manifest`) rather than
/// silently letting a user's value clobber one the runtime depends on.
fn validate_env_keys(env: &BTreeMap<String, String>, ctx: &str) -> anyhow::Result<()> {
    for key in env.keys() {
        if key.is_empty() {
            bail!("{ctx}: `env` has an empty key");
        }
        if key.to_uppercase().starts_with("CANOE_") {
            bail!("{ctx}: env var `{key}` uses canoe's reserved `CANOE_` prefix — pick a different name");
        }
    }
    Ok(())
}

fn parse_engine(s: &str) -> anyhow::Result<Engine> {
    match s {
        "python" => Ok(Engine::Python),
        "spark_sql" => Ok(Engine::SparkSql),
        "spark_dataframe" => Ok(Engine::SparkDataFrame),
        "uv" => Ok(Engine::Uv),
        other => bail!("unknown engine \"{other}\" (expected python, spark_sql, spark_dataframe, or uv)"),
    }
}

/// A node's transport — whether row data moves through S3 (`Python`/`Uv`
/// steps, `route` branches) or a Kafka topic (`SparkSql`/`SparkDataFrame`/
/// `DynamicRoute`, unmigrated pending a later phase). See
/// `canoe_config::StepConfig::input_ref`/`output_ref`'s docs for why a
/// step on one transport can never validly wire directly into a step on
/// the other.
#[derive(PartialEq, Eq, Clone, Copy)]
enum Transport {
    S3,
    Kafka,
}

fn transport_of(step: &StepConfig) -> Transport {
    match step {
        StepConfig::Python(_) | StepConfig::Uv(_) | StepConfig::Sink(_) | StepConfig::SparkSql(_) | StepConfig::SparkDataFrame(_) => {
            Transport::S3
        }
        StepConfig::KafkaPython(_)
        | StepConfig::KafkaSparkSql(_)
        | StepConfig::KafkaSparkDataFrame(_)
        | StepConfig::DynamicRoute(_) => Transport::Kafka,
    }
}

/// One runtime unit: a user-authored step, or a route branch expanded into
/// a filter-only step. Always ends up as exactly one `StepConfig`.
struct Node {
    name: String,
    /// The raw, unresolved name this node's `input = "..."` referenced —
    /// resolved against `name_to_ref`/`name_to_signal` once the whole
    /// graph (including merges) is known.
    input_ref: String,
    /// This node's own resolved output value: an S3 path (S3 transport)
    /// or a Kafka topic name (Kafka transport) — see `Transport`.
    output_ref: String,
    step: StepConfig,
}

/// Resolves a node's S3 output path: `<base_path>/<pipeline>/<version>/<name>/`.
/// `pipeline` comes from `compute_pipeline_ownership` — see its docs for
/// how a node's owning pipeline is determined *before* `compile_graph`'s
/// node-construction loop needs it (a pure name-level graph walk over the
/// raw AST, run once up front, rather than needing per-pipeline
/// reachability tracing to have already happened).
fn default_path(storage: Option<&RawStorage>, pipeline: &str, name: &str) -> anyhow::Result<String> {
    let storage = storage.with_context(|| {
        format!(
            "\"{name}\" uses S3 + signal transport (engine \"python\"/\"uv\", or a `route` branch) but no \
             top-level `storage {{ base_path = \"...\" }}` block is declared — add one"
        )
    })?;
    let version = storage.version.as_deref().unwrap_or("v1");
    Ok(format!("{}/{}/{}/{}", storage.base_path.trim_end_matches('/'), pipeline, version, name))
}

/// Resolves a `mode = "batch"` adapter's S3 output path:
/// `<base_path>/<version>/<name>/` — deliberately **without** a pipeline
/// segment, unlike `default_path`. Adapters aren't part of any pipeline's
/// data flow (`compute_pipeline_ownership` only walks
/// steps/routes/dynamic_routes reachable from a `pipeline.source`, and
/// never assigns an adapter an owning pipeline), so there's no `pipeline`
/// value to put in that segment — the adapter's own name is the only
/// thing distinguishing its path from another adapter's.
fn adapter_output_path(storage: Option<&RawStorage>, name: &str) -> anyhow::Result<String> {
    let storage = storage.with_context(|| {
        format!(
            "adapter \"{name}\" uses mode = \"batch\" (S3 + signal transport) but no top-level \
             `storage {{ base_path = \"...\" }}` block is declared — add one"
        )
    })?;
    let version = storage.version.as_deref().unwrap_or("v1");
    Ok(format!("{}/{}/{}", storage.base_path.trim_end_matches('/'), version, name))
}

/// A `pipeline { source = "..." }` value must equal the literal value
/// `build_chains` will find at the head of the trace — i.e. whatever the
/// first step's *resolved* `input_ref()` ends up being (see
/// `StepConfig::input_ref`), not the raw string a step's own `input`
/// attribute might have named. For a topic-sourced pipeline this was
/// always already true without any resolution step: a topic string passes
/// through `compile_graph`'s name-resolution fallback unchanged (it's
/// never a key in `name_to_ref`), so `source = "orders.raw"` already
/// equals the literal topic value. A `mode = "batch"` adapter has no
/// topic to name literally this way, so `source = "<adapter_name>"` needs
/// the same by-name resolution a downstream step's `input` gets — this
/// mirrors that resolution for the one other place a raw name can appear
/// unresolved. Streaming adapters are untouched: `source = <topic>` still
/// matches literally, same as today.
fn resolve_pipeline_source(raw: &RawFile, source: &str) -> anyhow::Result<String> {
    for adapter in &raw.adapters {
        if adapter.name != source {
            continue;
        }
        let ctx = format!("adapter \"{}\"", adapter.name);
        if resolve_adapter_mode(adapter.mode.as_deref(), &ctx)? == Mode::Batch {
            return adapter_output_path(raw.storage.as_ref(), &adapter.name);
        }
    }
    Ok(source.to_string())
}

/// Resolves a step's `lookup` blocks to literal paths — deliberately
/// **not** via `name_to_ref`/`compute_pipeline_ownership`: a lookup names
/// a node in a *different* `.hcl` file, one this compiler invocation
/// never parses, so there's no graph to walk and no existence check is
/// possible (a bad `pipeline`/`node` only surfaces at run time, as a
/// missing/empty `latest.json` fetch). Reuses `default_path`'s exact
/// `<base_path>/<pipeline>/<version>/<node>` format string — the DSL's
/// own `pipeline`/`node` (never this file's `compute_pipeline_ownership`)
/// provide the middle two segments, and `base_path`/`version` fall back to
/// this file's own top-level `storage {}` block when the `lookup` block
/// doesn't override them.
fn resolve_lookups(storage: Option<&RawStorage>, lookups: &[RawLookup], ctx: &str) -> anyhow::Result<Vec<LookupConfig>> {
    lookups.iter().map(|l| resolve_lookup(storage, l, ctx)).collect()
}

/// `RawEncrypt` -> `EncryptConfig` is a straight copy — unlike `lookup`,
/// nothing here references another node, so there's no path resolution to
/// do. See `EncryptConfig`'s docs for the wire format and why encryption
/// is never inherited across steps.
fn to_config_encrypts(encrypt: &[RawEncrypt]) -> Vec<EncryptConfig> {
    encrypt
        .iter()
        .map(|e| EncryptConfig {
            fields: e.fields.clone(),
            key: e.key.clone(),
        })
        .collect()
}

/// Rejects an empty secret name or a name declared more than once on the
/// same node — cheap, compile-time-only validation; canoe has no way to
/// check a declared secret actually exists in AWS Secrets Manager (see
/// `crates/canoe-k8s/src/secrets.rs`'s docs), same accepted limitation as
/// `lookup`'s unchecked cross-file references.
fn validate_secrets(secrets: &[String], ctx: &str) -> anyhow::Result<()> {
    let mut seen = HashSet::new();
    for name in secrets {
        if name.trim().is_empty() {
            bail!("{ctx}: `secrets` contains an empty name");
        }
        if !seen.insert(name.as_str()) {
            bail!("{ctx}: `secrets` names \"{name}\" more than once");
        }
    }
    Ok(())
}

/// AES-GCM's random nonce means the same plaintext encrypts to different
/// ciphertext every time — so a field that's both encrypted and used as a
/// dedup/change-detection key on the same sink would silently break
/// `Checkpointer`/`ScdView`'s equality checks (every run's ciphertext for
/// that field looks "different" even when the underlying value hasn't
/// changed). Reject the combination at compile time rather than let it
/// fail silently at run time.
fn validate_sink_encrypt_key_conflicts(sink: &crate::ast::RawSink, ctx: &str) -> anyhow::Result<()> {
    let encrypted: HashSet<&str> = sink.encrypt.iter().flat_map(|e| e.fields.iter().map(String::as_str)).collect();
    if encrypted.is_empty() {
        return Ok(());
    }
    if let Some(checkpoint) = &sink.checkpoint {
        for key in &checkpoint.keys {
            if encrypted.contains(key.as_str()) {
                bail!(
                    "{ctx}: field \"{key}\" is both `encrypt`ed and used as a `checkpoint.keys` entry — AES-GCM's \
                     random nonce means identical plaintexts encrypt differently each time, which would silently \
                     break checkpoint dedup on this field. Encrypt a different field, or drop it from `checkpoint.keys`"
                );
            }
        }
    }
    if let Some(scd) = &sink.scd {
        for key in scd.keys.iter().chain(scd.track_fields.iter()) {
            if encrypted.contains(key.as_str()) {
                bail!(
                    "{ctx}: field \"{key}\" is both `encrypt`ed and used as an `scd.keys`/`scd.track_fields` entry \
                     — AES-GCM's random nonce means identical plaintexts encrypt differently each time, which would \
                     silently break SCD change-detection on this field. Encrypt a different field, or drop it from \
                     `scd.keys`/`scd.track_fields`"
                );
            }
        }
    }
    Ok(())
}

fn resolve_lookup(storage: Option<&RawStorage>, lookup: &RawLookup, ctx: &str) -> anyhow::Result<LookupConfig> {
    let base_path = match &lookup.base_path {
        Some(b) => b.clone(),
        None => storage
            .map(|s| s.base_path.clone())
            .with_context(|| {
                format!(
                    "{ctx}: lookup \"{}\" has no `base_path` and no top-level `storage {{ base_path = \"...\" }}` \
                     block is declared to default from — set one or the other",
                    lookup.name
                )
            })?,
    };
    let version = lookup
        .version
        .clone()
        .or_else(|| storage.and_then(|s| s.version.clone()))
        .unwrap_or_else(|| "v1".to_string());
    let path = format!("{}/{}/{}/{}", base_path.trim_end_matches('/'), lookup.pipeline, version, lookup.node);
    Ok(LookupConfig { name: lookup.name.clone(), path })
}

/// For every S3-transport physical node (a `python`/`uv` step, or a
/// `route` branch's filter-only step) and every `merge`, the first (in
/// declaration order) `pipeline` whose `source` transitively reaches it —
/// used only to pick the `<pipeline>` path segment in `default_path`.
///
/// Deliberately a standalone pass over the *raw* AST (step/route/
/// dynamic_route/merge `input` references, by name), not something
/// computed from `compile_graph`'s own `Node`s — `compile_graph` needs a
/// node's owning pipeline *before* it builds that node's `StepConfig`
/// (to resolve its `output_path`), which is before any per-pipeline
/// reachability tracing (`build_chains`) has run. Running this graph walk
/// first, independent of `StepConfig`/topic/path resolution entirely,
/// avoids restructuring `compile_graph`'s existing single-pass shape.
///
/// A node reachable from more than one pipeline (possible via a `merge`
/// combining producers whose own lineages trace back to different
/// pipelines) is attributed to whichever pipeline was declared first —
/// the codebase doesn't otherwise assume strict single-pipeline
/// ownership (`compile()`'s `managed_topics` collection, for instance,
/// happily attributes one entry per *every* reaching pipeline, since a
/// `ManagedTopic` is just an annotation, not a physical location), but an
/// S3 path is one physical location a node can only have one of, so
/// "first declared pipeline wins" is the simplest deterministic policy
/// rather than inventing new ambiguity-handling machinery for it.
fn compute_pipeline_ownership(raw: &RawFile) -> HashMap<String, String> {
    let mut ownership: HashMap<String, String> = HashMap::new();

    for pipeline in &raw.pipelines {
        let mut frontier: std::collections::VecDeque<String> = std::collections::VecDeque::new();
        frontier.push_back(pipeline.source.clone());
        let mut visited: HashSet<String> = HashSet::new();

        while let Some(ident) = frontier.pop_front() {
            if !visited.insert(ident.clone()) {
                continue;
            }
            for step in &raw.steps {
                if step.input == ident {
                    ownership.entry(step.name.clone()).or_insert_with(|| pipeline.name.clone());
                    frontier.push_back(step.name.clone());
                }
            }
            for route in &raw.routes {
                if route.input == ident {
                    for branch in &route.branches {
                        let physical = format!("{}__{}", route.name, branch.name);
                        let logical = format!("{}.{}", route.name, branch.name);
                        ownership.entry(physical).or_insert_with(|| pipeline.name.clone());
                        frontier.push_back(logical);
                    }
                    if route.has_default {
                        let physical = format!("{}__default", route.name);
                        let logical = format!("{}.default", route.name);
                        ownership.entry(physical).or_insert_with(|| pipeline.name.clone());
                        frontier.push_back(logical);
                    }
                }
            }
            for dr in &raw.dynamic_routes {
                // dynamic_route stays Kafka-topic transport (no S3 path
                // of its own to own), but reachability still flows
                // through it to whatever's downstream.
                if dr.input == ident {
                    frontier.push_back(dr.name.clone());
                }
            }
            for merge in &raw.merges {
                if merge.inputs.iter().any(|i| *i == ident) {
                    ownership.entry(merge.name.clone()).or_insert_with(|| pipeline.name.clone());
                    frontier.push_back(merge.name.clone());
                }
            }
        }
    }

    ownership
}

/// Looks up `name`'s owning pipeline (see `compute_pipeline_ownership`),
/// falling back to a clearly-fake segment for a node `compute_pipeline_ownership`
/// didn't reach at all — that only happens for an orphaned node (nothing
/// traces back to any pipeline's `source`), which `compile()`'s own
/// orphan check rejects right after `compile_graph` returns, so this
/// fallback value is never actually written anywhere a user would see it.
fn owning_pipeline<'a>(ownership: &'a HashMap<String, String>, name: &str) -> &'a str {
    ownership.get(name).map(String::as_str).unwrap_or("_unreachable")
}

/// Sets a node's resolved input onto whichever fields its transport uses:
/// `input`/`input_path`/`input_signal` for S3-transport steps (`name` is
/// the *unresolved* producer name, kept as `input` verbatim — see
/// `PythonStep::input`'s docs), or `input_topic` for Kafka-transport steps
/// (where the resolved value already doubles as the logical name, same as
/// before this migration).
fn set_input(step: &mut StepConfig, name: &str, resolved_ref: String, resolved_signal: String) {
    match step {
        StepConfig::Python(s) => {
            s.input = name.to_string();
            s.input_path = resolved_ref;
            s.input_signal = resolved_signal;
        }
        StepConfig::Uv(s) => {
            s.input = name.to_string();
            s.input_path = resolved_ref;
            s.input_signal = resolved_signal;
        }
        StepConfig::Sink(s) => {
            s.input = name.to_string();
            s.input_path = resolved_ref;
            s.input_signal = resolved_signal;
        }
        StepConfig::SparkSql(s) => {
            s.input = name.to_string();
            s.input_path = resolved_ref;
            s.input_signal = resolved_signal;
        }
        StepConfig::SparkDataFrame(s) => {
            s.input = name.to_string();
            s.input_path = resolved_ref;
            s.input_signal = resolved_signal;
        }
        StepConfig::KafkaPython(s) => s.input_topic = resolved_ref,
        StepConfig::KafkaSparkSql(s) => s.input_topic = resolved_ref,
        StepConfig::KafkaSparkDataFrame(s) => s.input_topic = resolved_ref,
        StepConfig::DynamicRoute(s) => s.input_topic = resolved_ref,
    }
}

/// Builds the *whole file's* node/sink graph once — every root-level
/// `step`, `route` (expanded into per-branch filter steps), `dynamic_route`,
/// and `sink`, with `merge` blocks resolved as pure topic aliasing across
/// all of them. Each pipeline then traces its own subgraph out of this via
/// `build_chains` — see the module docs.
fn compile_graph(raw: &RawFile, base_dir: &Path) -> anyhow::Result<(Vec<Node>, Vec<Node>)> {
    let storage = raw.storage.as_ref();
    let ownership = compute_pipeline_ownership(raw);
    let mut nodes: Vec<Node> = Vec::new();
    // logical name (step name, dynamic_route name, or "route.branch") -> index into `nodes`
    let mut name_to_index: HashMap<String, usize> = HashMap::new();

    for step in &raw.steps {
        let ctx = format!("step \"{}\"", step.name);
        let mode = resolve_mode(&step.engine, step.mode.as_deref(), &ctx)?;
        let is_s3_step = mode == Mode::Batch;
        let output_ref = if is_s3_step {
            default_path(storage, owning_pipeline(&ownership, &step.name), &step.name)?
        } else {
            default_topic(&step.name)
        };
        let step_config = build_step_config(step, &output_ref, base_dir, storage)?;
        name_to_index.insert(step.name.clone(), nodes.len());
        nodes.push(Node {
            name: step.name.clone(),
            input_ref: step.input.clone(),
            output_ref,
            step: step_config,
        });
    }

    for route in &raw.routes {
        let mut all_predicates = Vec::new();
        for branch in &route.branches {
            let logical_name = format!("{}.{}", route.name, branch.name);
            let node_name = format!("{}__{}", route.name, branch.name);
            let output_ref = default_path(storage, owning_pipeline(&ownership, &node_name), &node_name)?;
            let ops = vec![RawOp::Filter {
                predicate: branch.when.clone(),
            }];
            let step_config = StepConfig::Python(PythonStep {
                name: node_name.clone(),
                input: String::new(),      // filled in during input resolution below
                input_path: String::new(), // filled in during input resolution below
                input_signal: String::new(),
                output_path: output_ref.clone(),
                output: OutputConfig::default(),
                code: CodeSource::Inline {
                    source: codegen::python(&ops, base_dir)?,
                },
                entrypoint: "steps.generated.run".to_string(),
                scaling: ScalingConfig::default(),
                resources: PodResources::default(),
                env: BTreeMap::new(),
                batch: None,
                lookups: Vec::new(),
                encrypt: Vec::new(),
                secrets: Vec::new(),
                depends_on: Vec::new(),
                depends_on_complete: Vec::new(),
            });
            name_to_index.insert(logical_name, nodes.len());
            nodes.push(Node {
                name: node_name,
                input_ref: route.input.clone(),
                output_ref,
                step: step_config,
            });
            all_predicates.push(branch.when.clone());
        }
        if route.has_default {
            let logical_name = format!("{}.default", route.name);
            let node_name = format!("{}__default", route.name);
            let output_ref = default_path(storage, owning_pipeline(&ownership, &node_name), &node_name)?;
            let predicate = format!("NOT ({})", all_predicates.join(" OR "));
            let ops = vec![RawOp::Filter { predicate }];
            let step_config = StepConfig::Python(PythonStep {
                name: node_name.clone(),
                input: String::new(),
                input_path: String::new(),
                input_signal: String::new(),
                output_path: output_ref.clone(),
                output: OutputConfig::default(),
                code: CodeSource::Inline {
                    source: codegen::python(&ops, base_dir)?,
                },
                entrypoint: "steps.generated.run".to_string(),
                scaling: ScalingConfig::default(),
                resources: PodResources::default(),
                env: BTreeMap::new(),
                batch: None,
                lookups: Vec::new(),
                encrypt: Vec::new(),
                secrets: Vec::new(),
                depends_on: Vec::new(),
                depends_on_complete: Vec::new(),
            });
            name_to_index.insert(logical_name, nodes.len());
            nodes.push(Node {
                name: node_name,
                input_ref: route.input.clone(),
                output_ref,
                step: step_config,
            });
        }
    }

    for dr in &raw.dynamic_routes {
        let ctx = format!("dynamic_route \"{}\"", dr.name);
        let output_ref = default_dynamic_route_topic(&dr.name);
        let task = compile_dynamic_task(&dr.task, base_dir).with_context(|| format!("{ctx} task"))?;
        let overflow = dr
            .overflow
            .as_ref()
            .map(|o| compile_dynamic_task(o, base_dir))
            .transpose()
            .with_context(|| format!("{ctx} overflow"))?;
        let step_config = StepConfig::DynamicRoute(DynamicRouteStep {
            name: dr.name.clone(),
            input_topic: String::new(), // filled in during input resolution below
            output_topic: output_ref.clone(),
            field: dr.field.clone(),
            max_branches: dr.max_branches.unwrap_or(DEFAULT_MAX_BRANCHES),
            task,
            overflow,
        });
        name_to_index.insert(dr.name.clone(), nodes.len());
        nodes.push(Node {
            name: dr.name.clone(),
            input_ref: dr.input.clone(),
            output_ref,
            step: step_config,
        });
    }

    // Resolve merges: point every merged producer's output at one shared
    // value (S3 path or Kafka topic, per the merged producers' shared
    // transport) instead of its own default one. `name_to_ref` tracks the
    // resolved physical value per logical name; `name_to_signal` tracks
    // which producer's *own* automatic wake-up signal a consumer of that
    // name should listen for.
    let mut name_to_ref: HashMap<String, String> =
        name_to_index.iter().map(|(name, &idx)| (name.clone(), nodes[idx].output_ref.clone())).collect();
    let mut name_to_signal: HashMap<String, String> =
        name_to_index.iter().map(|(name, &idx)| (name.clone(), nodes[idx].name.clone())).collect();

    // Seed batch-mode adapters into the same name-resolution maps: a
    // downstream batch step's `input = "adapter_name"` must resolve to
    // the adapter's actual S3 output path, not fall through to the
    // "unknown name -> use it literally" default in the resolution loop
    // below (that fallback exists for *streaming* adapters, whose
    // `input = "topic.string"` already IS the literal Kafka topic value —
    // adapters were never keys in this map before batch mode existed, so
    // a first step consuming a streaming adapter matched by topic
    // string, not by name; that behavior is unchanged here). Only batch
    // adapters get a name entry: a streaming adapter has no single
    // resolved value that isn't already its topic string.
    for adapter in &raw.adapters {
        if resolve_adapter_mode(adapter.mode.as_deref(), &format!("adapter \"{}\"", adapter.name))? == Mode::Batch {
            let output_path = adapter_output_path(storage, &adapter.name)?;
            name_to_ref.insert(adapter.name.clone(), output_path);
            name_to_signal.insert(adapter.name.clone(), adapter.name.clone());
        }
    }

    for merge in &raw.merges {
        let mut indices = Vec::with_capacity(merge.inputs.len());
        for input_name in &merge.inputs {
            let idx = *name_to_index.get(input_name).with_context(|| {
                format!(
                    "merge \"{}\": input \"{input_name}\" is not a known step, route branch, or dynamic_route",
                    merge.name
                )
            })?;
            indices.push(idx);
        }
        let transport = transport_of(&nodes[indices[0]].step);
        for &idx in &indices {
            if transport_of(&nodes[idx].step) != transport {
                bail!(
                    "merge \"{}\": inputs mix S3 transport (python/uv steps, route branches) with Kafka-topic \
                     transport (spark_sql/spark_dataframe/dynamic_route) — a merge can't alias two different \
                     kinds of value into one",
                    merge.name
                );
            }
        }
        let shared_ref = match transport {
            Transport::S3 => default_path(
                storage,
                owning_pipeline(&ownership, &merge.name),
                &format!("merge__{}", merge.name),
            )?,
            Transport::Kafka => default_merge_topic(&merge.name),
        };
        for (input_name, &idx) in merge.inputs.iter().zip(&indices) {
            nodes[idx].output_ref = shared_ref.clone();
            set_output_ref(&mut nodes[idx].step, shared_ref.clone());
            name_to_ref.insert(nodes[idx].name.clone(), shared_ref.clone());
            name_to_ref.insert(input_name.clone(), shared_ref.clone());
            // Each merge participant still fires its own automatic
            // wake-up signal under its own name (there's no single
            // producer identity a merge's shared output belongs to) — a
            // downstream consumer only gets a fast wake-up from whichever
            // participant happens to flush under this key; self-heal via
            // periodic checkpoint/backlog listing (see README.md's "S3 +
            // signal-driven pipeline transport") is what guarantees
            // correctness regardless of which signal actually arrives.
            name_to_signal.insert(input_name.clone(), nodes[idx].name.clone());
        }
        name_to_signal.insert(merge.name.clone(), merge.name.clone());
        name_to_ref.insert(merge.name.clone(), shared_ref);
    }

    // Resolve each node's input reference to its final (post-merge) value.
    for node in &mut nodes {
        let resolved_ref = name_to_ref.get(&node.input_ref).cloned().unwrap_or_else(|| node.input_ref.clone());
        let resolved_signal =
            name_to_signal.get(&node.input_ref).cloned().unwrap_or_else(|| node.input_ref.clone());
        set_input(&mut node.step, &node.input_ref, resolved_ref, resolved_signal);
    }

    // Resolve sinks the same way.
    let mut sinks = Vec::new();
    for sink in &raw.sinks {
        let resolved_ref = name_to_ref.get(&sink.input).cloned().unwrap_or_else(|| sink.input.clone());
        let resolved_signal = name_to_signal.get(&sink.input).cloned().unwrap_or_else(|| sink.input.clone());
        if sink.format != "parquet" {
            bail!(
                "sink \"{}\": format \"{}\" is not yet implemented by canoe-sink (only \"parquet\" is); see IcebergSink/ScdSink in crates/canoe-sink",
                sink.name,
                sink.format
            );
        }
        // Only ever reached for format="parquet" today (the bail! above
        // rejects every other format already) — if a second format
        // lands, this needs its own explicit "checkpoint/scd only valid
        // for parquet" check rather than inheriting this one for free.
        let scd = sink
            .scd
            .as_ref()
            .map(|s| -> anyhow::Result<ScdViewConfig> {
                let scd_type = s.scd_type.unwrap_or(2);
                if scd_type != 2 {
                    bail!(
                        "sink \"{}\": scd `type = {scd_type}` is not supported — only Type 2 is implemented \
                         (use `checkpoint` instead for Type-1-style overwrite-in-place semantics)",
                        sink.name
                    );
                }
                Ok(ScdViewConfig {
                    keys: s.keys.clone(),
                    track_fields: s.track_fields.clone(),
                    effective_field: s.effective_field.clone(),
                    scd_type: scd_type as u8,
                })
            })
            .transpose()?;

        validate_sink_encrypt_key_conflicts(sink, &format!("sink \"{}\"", sink.name))?;
        let column_metadata = codegen::resolve_column_metadata(sink.metadata.as_deref(), base_dir)
            .with_context(|| format!("sink \"{}\"", sink.name))?;

        sinks.push(Node {
            name: sink.name.clone(),
            input_ref: sink.input.clone(),
            output_ref: String::new(),
            step: StepConfig::Sink(SinkStep {
                name: sink.name.clone(),
                input: sink.input.clone(),
                input_path: resolved_ref,
                input_signal: resolved_signal,
                sink: SinkConfig::Parquet {
                    base_path: sink.base_path.clone(),
                    dataset_id: sink.dataset_id.clone().unwrap_or_else(|| sink.name.clone()),
                    partition_by: sink.partition_by.clone(),
                    batch_size: sink.batch_size.unwrap_or(10_000),
                    flush_interval_secs: sink.flush_interval_secs.unwrap_or(30),
                },
                signal: sink.signal.clone(),
                checkpoint: sink.checkpoint.as_ref().map(|c| CheckpointConfig { keys: c.keys.clone() }),
                scd,
                encrypt: to_config_encrypts(&sink.encrypt),
                column_metadata,
            }),
        });
    }

    Ok((nodes, sinks))
}

/// `depends_on`/`depends_on_complete` are only wired into
/// `crates/canoe-coordinator/src/activation.rs`'s batch-mode reconcile
/// loop (`run_batch_reconcile_loop`) — a streaming-mode step (or `uv`,
/// which has no batch/streaming split to begin with, just its own
/// persistent-pod loop) has nothing that would ever check them, so
/// declaring either there is a real authoring mistake worth catching at
/// compile time rather than silently doing nothing.
fn reject_depends_on_outside_batch(step: &crate::ast::RawStep, ctx: &str) -> anyhow::Result<()> {
    if !step.depends_on.is_empty() || !step.depends_on_complete.is_empty() {
        bail!(
            "{ctx}: `depends_on`/`depends_on_complete` are only supported for mode=\"batch\" steps today — \
             only a batch-mode step's Job-firing reconcile loop can gate on them"
        );
    }
    Ok(())
}

fn build_step_config(
    step: &crate::ast::RawStep,
    output_ref: &str,
    base_dir: &Path,
    storage: Option<&RawStorage>,
) -> anyhow::Result<StepConfig> {
    let ctx = format!("step \"{}\"", step.name);
    let mode = resolve_mode(&step.engine, step.mode.as_deref(), &ctx)?;
    let lookups = resolve_lookups(storage, &step.lookups, &ctx)?;
    let encrypt = to_config_encrypts(&step.encrypt);
    validate_secrets(&step.secrets, &ctx)?;
    let secrets = step.secrets.clone();
    match (parse_engine(&step.engine).with_context(|| ctx.clone())?, mode) {
        (Engine::Python, Mode::Batch) => {
            for op in &step.ops {
                if matches!(op, RawOp::SparkSqlCode { .. } | RawOp::SparkDataFrameCode { .. } | RawOp::UvScript { .. }) {
                    bail!("{ctx}: engine=\"python\" steps cannot use spark_sql/spark_dataframe/script blocks");
                }
            }
            validate_env_keys(&step.env, &ctx)?;
            let batch = step
                .batch
                .as_ref()
                .map(|b| -> anyhow::Result<BatchConfig> {
                    let is_escape_hatch = matches!(step.ops.as_slice(), [RawOp::PythonCode { .. }]);
                    if !is_escape_hatch {
                        bail!(
                            "{ctx}: `batch` requires a `python {{ code = ... }}` escape hatch — declarative ops always \
                             compile to a fixed `def run(record): ...` contract, one record at a time"
                        );
                    }
                    Ok(BatchConfig {
                        size: b.size as usize,
                        flush_interval_secs: b.flush_interval_secs.unwrap_or(5),
                    })
                })
                .transpose()?;
            Ok(StepConfig::Python(PythonStep {
                name: step.name.clone(),
                input: String::new(),      // filled in during input resolution
                input_path: String::new(), // filled in during input resolution
                input_signal: String::new(),
                output_path: output_ref.to_string(),
                output: to_config_output(step.output.as_ref()),
                code: CodeSource::Inline {
                    source: codegen::python(&step.ops, base_dir).with_context(|| ctx.clone())?,
                },
                entrypoint: "steps.generated.run".to_string(),
                scaling: step
                    .scaling
                    .as_ref()
                    .map(to_config_scaling)
                    .unwrap_or_default(),
                resources: step
                    .resources
                    .as_ref()
                    .map(to_config_resources)
                    .unwrap_or_default(),
                env: step.env.clone(),
                batch,
                lookups: lookups.clone(),
                encrypt: encrypt.clone(),
                secrets: secrets.clone(),
                depends_on: step.depends_on.clone(),
                depends_on_complete: step.depends_on_complete.clone(),
            }))
        }
        (Engine::Python, Mode::Streaming) => {
            reject_depends_on_outside_batch(step, &ctx)?;
            for op in &step.ops {
                if matches!(op, RawOp::SparkSqlCode { .. } | RawOp::SparkDataFrameCode { .. } | RawOp::UvScript { .. }) {
                    bail!("{ctx}: engine=\"python\" steps cannot use spark_sql/spark_dataframe/script blocks");
                }
            }
            validate_env_keys(&step.env, &ctx)?;
            if !lookups.is_empty() {
                bail!("{ctx}: `lookup` is only supported for mode=\"batch\" steps today (streaming has no S3+signal transport to run lookups against)");
            }
            if !encrypt.is_empty() {
                bail!("{ctx}: `encrypt` is only supported for mode=\"batch\" steps today");
            }
            let batch = step
                .batch
                .as_ref()
                .map(|b| -> anyhow::Result<BatchConfig> {
                    let is_escape_hatch = matches!(step.ops.as_slice(), [RawOp::PythonCode { .. }]);
                    if !is_escape_hatch {
                        bail!(
                            "{ctx}: `batch` requires a `python {{ code = ... }}` escape hatch — declarative ops always \
                             compile to a fixed `def run(record): ...` contract, one record at a time"
                        );
                    }
                    Ok(BatchConfig {
                        size: b.size as usize,
                        flush_interval_secs: b.flush_interval_secs.unwrap_or(5),
                    })
                })
                .transpose()?;
            Ok(StepConfig::KafkaPython(KafkaPythonStep {
                name: step.name.clone(),
                input_topic: String::new(),
                output_topic: output_ref.to_string(),
                code: CodeSource::Inline {
                    source: codegen::python(&step.ops, base_dir).with_context(|| ctx.clone())?,
                },
                entrypoint: "steps.generated.run".to_string(),
                scaling: step.scaling.as_ref().map(to_config_scaling).unwrap_or_default(),
                resources: step.resources.as_ref().map(to_config_resources).unwrap_or_default(),
                env: step.env.clone(),
                batch,
            }))
        }
        (Engine::SparkSql, Mode::Batch) => {
            for op in &step.ops {
                if matches!(op, RawOp::PythonCode { .. } | RawOp::SparkDataFrameCode { .. } | RawOp::UvScript { .. }) {
                    bail!("{ctx}: engine=\"spark_sql\" steps cannot use python/spark_dataframe/script blocks");
                }
            }
            if !step.env.is_empty() {
                bail!("{ctx}: `env` is only supported for engine=\"python\"/\"uv\" steps today");
            }
            if step.batch.is_some() {
                bail!("{ctx}: `batch` is only supported for engine=\"python\" steps today");
            }
            Ok(StepConfig::SparkSql(SparkSqlStep {
                name: step.name.clone(),
                input: String::new(),
                input_path: String::new(),
                input_signal: String::new(),
                output_path: output_ref.to_string(),
                output: to_config_output(step.output.as_ref()),
                sql: codegen::spark_sql(&step.ops, base_dir).with_context(|| ctx.clone())?,
                spark: step.spark.as_ref().map(to_config_spark).unwrap_or_default(),
                lookups: lookups.clone(),
                encrypt: encrypt.clone(),
                secrets: secrets.clone(),
                depends_on: step.depends_on.clone(),
                depends_on_complete: step.depends_on_complete.clone(),
            }))
        }
        (Engine::SparkSql, Mode::Streaming) => {
            reject_depends_on_outside_batch(step, &ctx)?;
            for op in &step.ops {
                if matches!(op, RawOp::PythonCode { .. } | RawOp::SparkDataFrameCode { .. } | RawOp::UvScript { .. }) {
                    bail!("{ctx}: engine=\"spark_sql\" steps cannot use python/spark_dataframe/script blocks");
                }
            }
            if !step.env.is_empty() {
                bail!("{ctx}: `env` is only supported for engine=\"python\"/\"uv\" steps today");
            }
            if step.batch.is_some() {
                bail!("{ctx}: `batch` is only supported for engine=\"python\" steps today");
            }
            if step.output.is_some() {
                bail!("{ctx}: `output` is only supported for mode=\"batch\" steps (the S3-transport mode) today");
            }
            Ok(StepConfig::KafkaSparkSql(KafkaSparkSqlStep {
                name: step.name.clone(),
                input_topic: String::new(),
                output_topic: output_ref.to_string(),
                sql: codegen::spark_sql(&step.ops, base_dir).with_context(|| ctx.clone())?,
                spark: step.spark.as_ref().map(to_config_spark).unwrap_or_default(),
                lookups: lookups.clone(),
                encrypt: encrypt.clone(),
                secrets: secrets.clone(),
            }))
        }
        (Engine::SparkDataFrame, mode @ (Mode::Batch | Mode::Streaming)) => {
            if mode == Mode::Streaming {
                reject_depends_on_outside_batch(step, &ctx)?;
            }
            if !step.env.is_empty() {
                bail!("{ctx}: `env` is only supported for engine=\"python\"/\"uv\" steps today");
            }
            if step.batch.is_some() {
                bail!("{ctx}: `batch` is only supported for engine=\"python\" steps today");
            }
            if mode == Mode::Streaming && step.output.is_some() {
                bail!("{ctx}: `output` is only supported for mode=\"batch\" steps (the S3-transport mode) today");
            }
            let code = step
                .ops
                .iter()
                .find_map(|op| match op {
                    RawOp::SparkDataFrameCode { code } => Some(code),
                    _ => None,
                })
                .with_context(|| {
                    format!("{ctx}: engine=\"spark_dataframe\" steps require a `spark_dataframe {{ code = ... }}` block (no declarative ops compile to DataFrame code yet)")
                })?
                .resolve(base_dir)?;
            if step.ops.len() > 1 {
                bail!("{ctx}: `spark_dataframe {{ code = ... }}` must be the step's only block");
            }
            if mode == Mode::Batch {
                Ok(StepConfig::SparkDataFrame(SparkDataFrameStep {
                    name: step.name.clone(),
                    input: String::new(),
                    input_path: String::new(),
                    input_signal: String::new(),
                    output_path: output_ref.to_string(),
                    output: to_config_output(step.output.as_ref()),
                    code: CodeSource::Inline { source: code },
                    spark: step.spark.as_ref().map(to_config_spark).unwrap_or_default(),
                    lookups: lookups.clone(),
                    encrypt: encrypt.clone(),
                    secrets: secrets.clone(),
                    depends_on: step.depends_on.clone(),
                    depends_on_complete: step.depends_on_complete.clone(),
                }))
            } else {
                Ok(StepConfig::KafkaSparkDataFrame(KafkaSparkDataFrameStep {
                    name: step.name.clone(),
                    input_topic: String::new(),
                    output_topic: output_ref.to_string(),
                    code: CodeSource::Inline { source: code },
                    spark: step.spark.as_ref().map(to_config_spark).unwrap_or_default(),
                    lookups: lookups.clone(),
                    encrypt: encrypt.clone(),
                    secrets: secrets.clone(),
                }))
            }
        }
        (Engine::Uv, _) => {
            reject_depends_on_outside_batch(step, &ctx)?;
            let code = step
                .ops
                .iter()
                .find_map(|op| match op {
                    RawOp::UvScript { code } => Some(code),
                    _ => None,
                })
                .with_context(|| {
                    format!("{ctx}: engine=\"uv\" steps require a `script {{ code = ... }}` (or `file = ...`) block")
                })?
                .resolve(base_dir)?;
            if step.ops.len() > 1 {
                bail!("{ctx}: `script {{ ... }}` must be the step's only block");
            }
            if step.batch.is_some() {
                bail!("{ctx}: `batch` is only supported for engine=\"python\" steps today — a `uv` step already owns its whole consume loop, so batch it there directly");
            }
            validate_env_keys(&step.env, &ctx)?;
            Ok(StepConfig::Uv(UvStep {
                name: step.name.clone(),
                input: String::new(),
                input_path: String::new(),
                input_signal: String::new(),
                output_path: output_ref.to_string(),
                output: to_config_output(step.output.as_ref()),
                code: CodeSource::Inline { source: code },
                scaling: step.scaling.as_ref().map(to_config_scaling).unwrap_or_default(),
                resources: step.resources.as_ref().map(to_config_resources).unwrap_or_default(),
                env: step.env.clone(),
                lookups: lookups.clone(),
                encrypt: encrypt.clone(),
                secrets: secrets.clone(),
            }))
        }
    }
}

fn set_output_ref(step: &mut StepConfig, value: String) {
    match step {
        StepConfig::Python(s) => s.output_path = value,
        StepConfig::SparkSql(s) => s.output_path = value,
        StepConfig::SparkDataFrame(s) => s.output_path = value,
        StepConfig::KafkaPython(s) => s.output_topic = value,
        StepConfig::KafkaSparkSql(s) => s.output_topic = value,
        StepConfig::KafkaSparkDataFrame(s) => s.output_topic = value,
        StepConfig::DynamicRoute(s) => s.output_topic = value,
        StepConfig::Uv(s) => s.output_path = value,
        StepConfig::Sink(_) => {
            unreachable!("sinks have no output and are never merge inputs")
        }
    }
}

fn to_config_output(raw: Option<&RawOutput>) -> OutputConfig {
    let d = OutputConfig::default();
    match raw {
        None => d,
        Some(r) => OutputConfig {
            batch_size: r.batch_size.map(|n| n as usize).unwrap_or(d.batch_size),
            flush_interval_secs: r.flush_interval_secs.unwrap_or(d.flush_interval_secs),
            partition_by: r.partition_by.clone(),
        },
    }
}

/// Traces every simple path from `source` to a sink through the
/// *whole file's* node/sink graph — same DFS as before this DSL's blocks
/// moved to the root level, just walking a shared graph instead of a
/// per-pipeline one. Returns the resulting chains plus the set of step
/// and sink names actually reached, which `compile()` uses both for
/// per-pipeline `managed_topics` attribution and to catch orphaned
/// (declared-but-unreachable) blocks.
#[allow(clippy::type_complexity)]
fn build_chains(
    pipeline_name: &str,
    source: &str,
    nodes: &[Node],
    sinks: &[Node],
    trigger: Option<TriggerConfig>,
) -> anyhow::Result<(Vec<ChainConfig>, HashSet<String>, HashSet<String>)> {
    let mut by_input: HashMap<&str, Vec<usize>> = HashMap::new();
    for (i, node) in nodes.iter().enumerate() {
        by_input.entry(node.step.input_ref()).or_default().push(i);
    }
    let mut sink_by_input: HashMap<&str, &Node> = HashMap::new();
    for sink in sinks {
        sink_by_input.insert(sink.step.input_ref(), sink);
    }

    let mut chains = Vec::new();
    let mut reached_steps: HashSet<String> = HashSet::new();
    let mut reached_sinks: HashSet<String> = HashSet::new();
    let mut chain_counter = 0usize;
    let mut stack: Vec<(String, Vec<StepConfig>, HashSet<String>)> =
        vec![(source.to_string(), Vec::new(), HashSet::new())];

    while let Some((value, path, visited)) = stack.pop() {
        let mut extended = false;

        if let Some(indices) = by_input.get(value.as_str()) {
            for &idx in indices {
                let node = &nodes[idx];
                if visited.contains(&node.name) {
                    bail!(
                        "pipeline \"{pipeline_name}\": cycle detected — step \"{}\" is downstream of itself",
                        node.name
                    );
                }
                let mut next_path = path.clone();
                next_path.push(node.step.clone());
                let mut next_visited = visited.clone();
                next_visited.insert(node.name.clone());
                stack.push((node.output_ref.clone(), next_path, next_visited));
                reached_steps.insert(node.name.clone());
                extended = true;
            }
        }

        if let Some(sink) = sink_by_input.get(value.as_str()) {
            let mut final_path = path.clone();
            final_path.push(sink.step.clone());
            chain_counter += 1;
            chains.push(ChainConfig {
                name: format!("{pipeline_name}-{chain_counter}"),
                pipeline: pipeline_name.to_string(),
                source: source.to_string(),
                steps: final_path,
                trigger: trigger.clone(),
            });
            reached_sinks.insert(sink.name.clone());
            extended = true;
        }

        if !extended {
            if path.is_empty() {
                bail!("pipeline \"{pipeline_name}\": source \"{source}\" is not consumed by any step or route");
            }
            let last = path.last().unwrap();
            bail!(
                "pipeline \"{pipeline_name}\": step \"{}\" output \"{value}\" is not consumed by anything downstream (add a step, route, merge, or sink reading it)",
                last.name()
            );
        }
    }

    Ok((chains, reached_steps, reached_sinks))
}

fn default_topic(step: &str) -> String {
    format!("_canoe.{step}")
}

fn default_merge_topic(merge: &str) -> String {
    format!("_canoe.merge.{merge}")
}

fn default_dynamic_route_topic(dynamic_route: &str) -> String {
    format!("_canoe.{dynamic_route}.merged")
}

/// Matches Airflow's default `max_map_length` order of magnitude —
/// unbounded dynamic fan-out risks runaway K8s object creation on a
/// high-cardinality field, so `dynamic_route` always has a cap even when
/// the DSL author doesn't set one.
const DEFAULT_MAX_BRANCHES: u32 = 1024;

fn compile_dynamic_task(t: &RawDynamicTask, base_dir: &Path) -> anyhow::Result<DynamicTaskTemplate> {
    if !t.env.is_empty() {
        bail!("`env` is not supported for dynamic_route tasks/overflow yet — DynamicTaskTemplate has nowhere to carry it (see the `Engine::Uv` arm below for the same limitation on a whole engine)");
    }
    if t.batch.is_some() {
        bail!("`batch` is not supported for dynamic_route tasks/overflow yet — DynamicTaskTemplate has nowhere to carry it, same as `env` above");
    }
    if t.output.is_some() {
        bail!("`output` is not supported for dynamic_route tasks/overflow — dynamic_route stays Kafka-topic transport, so there's no S3 output to configure");
    }
    match parse_engine(&t.engine)? {
        Engine::Python => {
            for op in &t.ops {
                if matches!(op, RawOp::SparkSqlCode { .. } | RawOp::SparkDataFrameCode { .. }) {
                    bail!("engine=\"python\" tasks cannot use spark_sql/spark_dataframe blocks");
                }
            }
            Ok(DynamicTaskTemplate::Python {
                code: CodeSource::Inline {
                    source: codegen::python(&t.ops, base_dir)?,
                },
                entrypoint: "steps.generated.run".to_string(),
                scaling: t.scaling.as_ref().map(to_config_scaling).unwrap_or_default(),
                resources: t.resources.as_ref().map(to_config_resources).unwrap_or_default(),
            })
        }
        Engine::SparkSql => {
            for op in &t.ops {
                if matches!(op, RawOp::PythonCode { .. } | RawOp::SparkDataFrameCode { .. }) {
                    bail!("engine=\"spark_sql\" tasks cannot use python/spark_dataframe blocks");
                }
            }
            Ok(DynamicTaskTemplate::SparkSql {
                sql: codegen::spark_sql(&t.ops, base_dir)?,
                spark: t.spark.as_ref().map(to_config_spark).unwrap_or_default(),
            })
        }
        Engine::SparkDataFrame => {
            let code = t
                .ops
                .iter()
                .find_map(|op| match op {
                    RawOp::SparkDataFrameCode { code } => Some(code),
                    _ => None,
                })
                .context(
                    "engine=\"spark_dataframe\" tasks require a `spark_dataframe { code = ... }` block \
                     (no declarative ops compile to DataFrame code yet)",
                )?
                .resolve(base_dir)?;
            if t.ops.len() > 1 {
                bail!("`spark_dataframe {{ code = ... }}` must be the task's only block");
            }
            Ok(DynamicTaskTemplate::SparkDataFrame {
                code: CodeSource::Inline { source: code },
                spark: t.spark.as_ref().map(to_config_spark).unwrap_or_default(),
            })
        }
        // Not supported for dynamic_route tasks yet — DynamicTaskTemplate
        // has no Uv variant, so there's nowhere to put a uv step's own
        // scaling/resources (a real UvStep carries both; a template
        // shared across every discovered value would need its own copy).
        // Reach for a regular `engine = "uv"` step instead.
        Engine::Uv => bail!("engine=\"uv\" is not supported for dynamic_route tasks/overflow yet"),
    }
}

fn to_config_scaling(raw: &RawScaling) -> ScalingConfig {
    let d = ScalingConfig::default();
    ScalingConfig {
        min_replicas: raw.min_replicas.unwrap_or(d.min_replicas),
        max_replicas: raw.max_replicas.unwrap_or(d.max_replicas),
        target_lag_per_replica: raw.target_lag_per_replica.unwrap_or(d.target_lag_per_replica),
        cooldown_secs: raw.cooldown_secs.unwrap_or(d.cooldown_secs),
        backoff_limit: raw.backoff_limit.map(|n| n as u32).unwrap_or(d.backoff_limit),
    }
}

fn to_config_resources(raw: &RawResources) -> PodResources {
    let d = PodResources::default();
    PodResources {
        cpu_request: raw.cpu_request.clone().unwrap_or(d.cpu_request),
        mem_request: raw.mem_request.clone().unwrap_or(d.mem_request),
        cpu_limit: raw.cpu_limit.clone().unwrap_or(d.cpu_limit),
        mem_limit: raw.mem_limit.clone().unwrap_or(d.mem_limit),
    }
}

fn to_config_spark(raw: &RawSparkConf) -> SparkJobConfig {
    let d = SparkJobConfig::default();
    SparkJobConfig {
        image: raw.image.clone().unwrap_or(d.image),
        executor_instances: raw.executor_instances.unwrap_or(d.executor_instances),
        executor_cores: raw.executor_cores.unwrap_or(d.executor_cores),
        executor_memory: raw.executor_memory.clone().unwrap_or(d.executor_memory),
        driver_memory: raw.driver_memory.clone().unwrap_or(d.driver_memory),
        extra_conf: raw.conf.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        event_log_override: raw.event_log.as_ref().map(|e| canoe_config::EventLogOverride {
            enabled: e.enabled,
            base_path: e.base_path.clone(),
        }),
    }
}
