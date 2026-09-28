mod activation;
mod archiver;
mod dynamic_router;
mod metrics;
mod projects;
mod retention;
mod scheduler;
mod signals;
mod sink_runner;
mod status;
mod tasks;

use activation::Ctx;
use canoe_config::{GlobalConfig, PipelineConfig};
use clap::Parser;
use kube::Client;
use status::StatusRegistry;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::info;

/// canoe-coordinator: routes data between chain steps via Kafka and
/// manages the Fargate worker quorum for Python/Spark steps in EKS.
#[derive(Parser, Debug)]
#[command(name = "canoe-coordinator")]
struct Cli {
    /// Path to the pipeline YAML config.
    #[arg(long)]
    config: String,

    /// Kafka bootstrap servers, e.g. "b-1.mycluster.kafka.us-east-1.amazonaws.com:9092".
    /// The coordinator's *own* address for its direct Kafka use
    /// (`LagInspector`, the signal listener/publisher, topic
    /// retention/archival) — see `pod_kafka_bootstrap` for the (usually
    /// identical) value baked into every worker pod/Job instead.
    #[arg(long, env = "CANOE_KAFKA_BOOTSTRAP")]
    kafka_bootstrap: String,

    /// Kafka bootstrap servers baked into every worker pod/Job's
    /// `CANOE_KAFKA_BOOTSTRAP` env var. Defaults to `--kafka-bootstrap`
    /// when unset — correct for any deployment where the coordinator and
    /// every pod it creates share one network (the common case, e.g.
    /// real EKS). Only needs to differ when the coordinator runs
    /// somewhere pods can't reach by the same address — e.g. a host-run
    /// coordinator against a local `kind` cluster, where pods need an
    /// in-cluster DNS name instead. See README.md's "Local dev stack".
    #[arg(long, env = "CANOE_POD_KAFKA_BOOTSTRAP")]
    pod_kafka_bootstrap: Option<String>,

    /// S3 endpoint override baked into every worker pod/Job's
    /// `AWS_ENDPOINT` env var (`crates/canoe-k8s/src/s3_env.rs`).
    /// Defaults to the coordinator's own `AWS_ENDPOINT` process env var
    /// when unset (the same value `object_store`'s `AmazonS3Builder::from_env()`
    /// already reads for the coordinator's own direct S3 calls —
    /// `S3BacklogInspector`, `Checkpointer`). Same split as
    /// `pod_kafka_bootstrap` above, for the same reason.
    #[arg(long, env = "CANOE_POD_S3_ENDPOINT")]
    pod_s3_endpoint: Option<String>,

    /// K8s namespace to run worker/launcher pods in (must be matched by an
    /// EKS Fargate profile).
    #[arg(long, env = "CANOE_NAMESPACE", default_value = "canoe-workers")]
    namespace: String,

    /// Shared container image used by every Python step's worker pods.
    #[arg(long, env = "CANOE_WORKER_IMAGE")]
    worker_image: String,

    /// Container image used by every `engine = "uv"` step's worker pods
    /// (bundles `uv`; see workers/uv-step-worker).
    #[arg(long, env = "CANOE_UV_WORKER_IMAGE")]
    uv_worker_image: String,

    /// API server URL used in spark-submit's `k8s://` master, e.g.
    /// "https://ABCDEF.gr7.us-east-1.eks.amazonaws.com".
    #[arg(long, env = "CANOE_K8S_API_SERVER")]
    k8s_api_server: String,

    /// Service account Spark driver/executor pods run as.
    #[arg(long, env = "CANOE_SPARK_SERVICE_ACCOUNT", default_value = "canoe-spark")]
    spark_service_account: String,

    /// Seconds between autoscaler reconcile passes per Python step.
    #[arg(long, default_value_t = 20)]
    reconcile_interval_secs: u64,

    /// Kafka topic sinks publish signals to, and triggered chains
    /// (`schedule { signal = ... }`) wait on. See `signals.rs`.
    #[arg(long, env = "CANOE_SIGNALS_TOPIC", default_value = "_canoe.signals")]
    signals_topic: String,

    /// Container image used for every adapter's one-shot Job (bundles
    /// `uv`; see workers/adapter-runner).
    #[arg(long, env = "CANOE_ADAPTER_RUNNER_IMAGE")]
    adapter_runner_image: String,

    /// Address the coordinator's built-in metrics endpoint binds to —
    /// `GET /metrics` for Prometheus's scrape, `POST /metrics/report`
    /// for every task to push to. See `metrics.rs`.
    #[arg(long, env = "CANOE_METRICS_ADDR", default_value = "0.0.0.0:9095")]
    metrics_addr: SocketAddr,

    /// URL every task is told (via `CANOE_METRICS_URL`) to push its
    /// metrics to — the coordinator's own address as reachable *from
    /// inside the cluster* (a K8s Service in front of `metrics_addr`,
    /// typically), not necessarily the same as `metrics_addr` itself
    /// (which is what the coordinator's own process binds to).
    #[arg(long, env = "CANOE_METRICS_URL")]
    metrics_url: String,

    /// Global root S3 location every task/worker (Python/uv step pods,
    /// adapter Jobs) ships its own logs to, e.g. "s3://canoe-logs". Passed
    /// through unchanged as `CANOE_LOG_BASE_PATH`; each task derives its
    /// own object key under it, `<pipeline|adapter>/<task>/<year>/<month>/
    /// <run-date>_<ulid>.log` — see README.md's "Log collection". Omit to
    /// leave log shipping off entirely (tasks still log to stdout, just
    /// not durably collected anywhere).
    #[arg(long, env = "CANOE_LOG_BASE_PATH")]
    log_base_path: Option<String>,

    /// Path to an optional coordinator-level global config YAML — distinct
    /// from `--config` (the compiled *pipeline* YAML): operator/
    /// deployment-scoped settings that need to stay consistent across
    /// every pipeline this coordinator runs, starting with
    /// `spark.event_log` (see `canoe_config::GlobalConfig`). Omit to
    /// leave every setting it could provide at its "off" default — same
    /// optional-flag precedent as `--log-base-path` above, not an error
    /// to not pass this.
    #[arg(long, env = "CANOE_GLOBAL_CONFIG")]
    global_config: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    let global_config = match &cli.global_config {
        Some(path) => GlobalConfig::load(path)?,
        None => GlobalConfig::default(),
    };
    let global_config = Arc::new(global_config);
    let config = PipelineConfig::load(&cli.config)?;
    info!(
        chains = config.chains.len(),
        adapters = config.adapters.len(),
        "loaded pipeline config"
    );

    let status_registry = Arc::new(StatusRegistry::new());

    // See `Cli::pod_kafka_bootstrap`/`Cli::pod_s3_endpoint`'s docs: these
    // default to the coordinator's own address/endpoint, which is
    // correct whenever the coordinator and its pods share a network, and
    // only need to be given explicitly when they don't (e.g. this
    // repo's local `kind` dev stack — see `scripts/run-coordinator.sh`).
    let pod_kafka_bootstrap = cli.pod_kafka_bootstrap.clone().unwrap_or_else(|| cli.kafka_bootstrap.clone());
    let pod_s3_endpoint = cli.pod_s3_endpoint.clone().or_else(|| std::env::var("AWS_ENDPOINT").ok());

    let k8s_client = Client::try_default().await?;
    let signal_producer = canoe_kafka::make_producer(&canoe_kafka::KafkaSettings {
        bootstrap_servers: cli.kafka_bootstrap.clone(),
        group_id: String::new(), // producers don't use a group id
    })?;
    let ctx = Ctx {
        k8s_client: k8s_client.clone(),
        namespace: cli.namespace.clone(),
        worker_image: cli.worker_image,
        uv_worker_image: cli.uv_worker_image,
        metrics_url: cli.metrics_url.clone(),
        kafka_bootstrap: cli.kafka_bootstrap.clone(),
        pod_kafka_bootstrap: pod_kafka_bootstrap.clone(),
        pod_s3_endpoint: pod_s3_endpoint.clone(),
        k8s_api_server: cli.k8s_api_server,
        spark_service_account: cli.spark_service_account,
        reconcile_interval: Duration::from_secs(cli.reconcile_interval_secs),
        signals_topic: cli.signals_topic,
        log_base_path: cli.log_base_path.clone(),
        activated: Arc::new(Mutex::new(HashSet::new())),
        completed: Arc::new(Mutex::new(HashSet::new())),
        status: status_registry.clone(),
        global: global_config,
        projects: Arc::new(projects::ProjectRegistry::new()),
        pending: Arc::new(Mutex::new(HashMap::new())),
        adapter_triggers: Arc::new(Mutex::new(HashMap::new())),
        adapter_runner_image: cli.adapter_runner_image,
        signal_producer: Arc::new(signal_producer),
        control: Arc::new(Mutex::new(HashMap::new())),
        force_fire: Arc::new(Mutex::new(HashMap::new())),
    };

    // Every task (Python/uv step worker pods, adapter Jobs) pushes its
    // own metrics here rather than being scraped directly — see
    // `metrics.rs` for why. Started before anything else so no task's
    // very first push ever races an endpoint that isn't listening yet.
    // `GET /status` (introspection for `canoe-tui`/other read-only
    // clients) and `POST /projects/...` (push/trigger — `projects.rs`)
    // are served on this same address/port.
    let metrics_store = metrics::Store::new();
    metrics::start(
        metrics_store,
        ctx.projects.clone(),
        status_registry.clone(),
        ctx.control.clone(),
        projects::router(ctx.clone()),
        cli.metrics_addr,
    );

    // Durability/debugging for the chain's own internal wiring topics —
    // a DSL top-level `topics { ... }` block, global to the file. Applied
    // before anything starts producing/consuming: retention synchronously
    // (so it's in place from the first message), archiving as detached
    // background loops (secondary to, and independent of, the pipeline).
    // Only ever driven by `--config`'s own `topics {}` block today — a
    // pushed project's own `topics {}` block (if any) is not applied by
    // `apply_project` (out of scope for this first pass, same "additive,
    // not a full merge" limitation as everything else about a push).
    if let Some(defaults) = &config.topic_defaults {
        if let Some(retention_ms) = defaults.retention_ms {
            retention::apply(&cli.kafka_bootstrap, retention_ms, &config.managed_topics).await;
        }
        if let Some(archive) = &defaults.archive {
            archiver::start(cli.kafka_bootstrap.clone(), archive.clone(), config.managed_topics.clone());
        }
    }

    // The already-loaded `--config` becomes project `"default"` — the
    // exact same `apply_project` path a later `POST /projects/:name`
    // push uses, not a special-cased original plus a parallel new one.
    projects::apply_project(&ctx, "default", &config).await?;

    info!("coordinator up, signal listener starting (pending set is shared and grows as projects are pushed)");
    signals::listen(ctx).await;
    Ok(())
}
