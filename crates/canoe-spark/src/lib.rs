//! Launches Spark steps on the same EKS cluster the coordinator runs in.
//!
//! Each Spark step gets one long-lived "launcher" pod (modeled as a
//! single-replica Deployment) that runs `spark-submit --master
//! k8s://<apiserver> --deploy-mode cluster`. In cluster mode, spark-submit
//! itself creates the actual driver pod, which in turn requests executor
//! pods from the API server as needed — the launcher pod just supervises
//! that submission and restarts it if it dies. Driver and launcher pods
//! land on Fargate via the same namespace-matched Fargate profile used for
//! Python workers; executor pod scaling is handled by Spark's own dynamic
//! allocation, not the canoe autoscaler.

use canoe_config::{KafkaSparkDataFrameStep, KafkaSparkSqlStep, PodResources, SparkDataFrameStep, SparkJobConfig, SparkSqlStep};
use k8s_openapi::api::apps::v1::{Deployment, DeploymentSpec};
use k8s_openapi::api::core::v1::{Container, EnvVar, PodSpec, PodTemplateSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use kube::api::{Patch, PatchParams};
use kube::{Api, Client};
use std::collections::BTreeMap;

#[derive(Clone)]
pub struct SparkLauncher {
    client: Client,
    namespace: String,
    k8s_api_server: String,
    service_account: String,
}

impl SparkLauncher {
    pub fn new(
        client: Client,
        namespace: impl Into<String>,
        k8s_api_server: impl Into<String>,
        service_account: impl Into<String>,
    ) -> Self {
        Self {
            client,
            namespace: namespace.into(),
            k8s_api_server: k8s_api_server.into(),
            service_account: service_account.into(),
        }
    }

    pub async fn ensure_sql_step(
        &self,
        chain_name: &str,
        step: &KafkaSparkSqlStep,
        bootstrap_servers: &str,
        event_log_path: Option<&str>,
        pod_s3_endpoint: Option<&str>,
    ) -> anyhow::Result<()> {
        let name = launcher_name(chain_name, &step.name);
        let mut env = vec![
            ("CANOE_SQL".to_string(), step.sql.clone()),
            ("CANOE_INPUT_TOPIC".to_string(), step.input_topic.clone()),
            ("CANOE_OUTPUT_TOPIC".to_string(), step.output_topic.clone()),
            ("CANOE_KAFKA_BOOTSTRAP".to_string(), bootstrap_servers.to_string()),
            ("CANOE_MODE".to_string(), "sql".to_string()),
        ];
        if !step.lookups.is_empty() {
            env.push(("CANOE_LOOKUPS".to_string(), serde_json::to_string(&step.lookups)?));
        }
        if !step.encrypt.is_empty() {
            env.push(("CANOE_ENCRYPT_FIELDS".to_string(), serde_json::to_string(&step.encrypt)?));
        }
        let spc_name = self.ensure_secrets(&name, &step.secrets).await?;
        if spc_name.is_some() {
            env.push(("CANOE_SECRETS_PATH".to_string(), canoe_k8s::SECRETS_MOUNT_PATH.to_string()));
        }
        let args = self.spark_submit_args(&name, &step.spark, &env, spc_name.as_deref(), event_log_path, pod_s3_endpoint);
        self.apply_launcher(&name, chain_name, &step.name, &step.spark, args).await
    }

    pub async fn ensure_dataframe_step(
        &self,
        chain_name: &str,
        step: &KafkaSparkDataFrameStep,
        bootstrap_servers: &str,
        event_log_path: Option<&str>,
        pod_s3_endpoint: Option<&str>,
    ) -> anyhow::Result<()> {
        let name = launcher_name(chain_name, &step.name);
        let mut env = vec![
            ("CANOE_CODE_SOURCE".to_string(), serde_json::to_string(&step.code)?),
            ("CANOE_INPUT_TOPIC".to_string(), step.input_topic.clone()),
            ("CANOE_OUTPUT_TOPIC".to_string(), step.output_topic.clone()),
            ("CANOE_KAFKA_BOOTSTRAP".to_string(), bootstrap_servers.to_string()),
            ("CANOE_MODE".to_string(), "dataframe".to_string()),
        ];
        if !step.lookups.is_empty() {
            env.push(("CANOE_LOOKUPS".to_string(), serde_json::to_string(&step.lookups)?));
        }
        if !step.encrypt.is_empty() {
            env.push(("CANOE_ENCRYPT_FIELDS".to_string(), serde_json::to_string(&step.encrypt)?));
        }
        let spc_name = self.ensure_secrets(&name, &step.secrets).await?;
        if spc_name.is_some() {
            env.push(("CANOE_SECRETS_PATH".to_string(), canoe_k8s::SECRETS_MOUNT_PATH.to_string()));
        }
        let args = self.spark_submit_args(&name, &step.spark, &env, spc_name.as_deref(), event_log_path, pod_s3_endpoint);
        self.apply_launcher(&name, chain_name, &step.name, &step.spark, args).await
    }

    /// Batch-mode (`spark_sql { mode = "batch" }`, the default) — fires a
    /// one-shot K8s Job (via `canoe_k8s::StepJobRunner::create_job`,
    /// reused rather than reinvented — see its docs) running the exact
    /// same `spark-submit` command this launcher's streaming path already
    /// uses, just wrapped in Job semantics (`restartPolicy: Never`,
    /// `backoffLimit`, exits on completion) instead of an always-on
    /// Deployment. Returns the created Job's name so the caller's
    /// reconcile loop can poll `StepJobRunner::job_status` for
    /// completion, same pattern `start_python_step` already established.
    ///
    /// Adds `CANOE_INPUT_PATH`/`CANOE_OUTPUT_PATH`/`CANOE_INPUT_SIGNAL`/
    /// output-config env vars (the S3+signal contract, mirroring
    /// `pod_manager.rs`'s equivalent for Python) via the same
    /// `spark.kubernetes.driverEnv.*` mechanism `spark_submit_args`
    /// already uses for `CANOE_LOOKUPS`/`CANOE_ENCRYPT_FIELDS` — these
    /// land on the *driver* pod spark-submit itself creates, same as
    /// every other Spark env var, not on this wrapper Job's own pod.
    /// `CANOE_TRANSPORT=batch` (orthogonal to the existing
    /// `CANOE_MODE=sql`) tells `canoe_spark_runner.py` to take the batch
    /// read/write path instead of `spark.readStream`.
    pub async fn ensure_sql_batch_step(
        &self,
        chain_name: &str,
        step: &SparkSqlStep,
        bootstrap_servers: &str,
        event_log_path: Option<&str>,
        pod_s3_endpoint: Option<&str>,
    ) -> anyhow::Result<String> {
        let name = launcher_name(chain_name, &step.name);
        let mut env = vec![
            ("CANOE_SQL".to_string(), step.sql.clone()),
            ("CANOE_INPUT_PATH".to_string(), step.input_path.clone()),
            ("CANOE_OUTPUT_PATH".to_string(), step.output_path.clone()),
            ("CANOE_INPUT_SIGNAL".to_string(), step.input_signal.clone()),
            ("CANOE_CHECKPOINT_PATH".to_string(), format!("{}/.checkpoint.json", step.output_path.trim_end_matches('/'))),
            ("CANOE_OUTPUT_PARTITION_BY".to_string(), step.output.partition_by.join(",")),
            ("CANOE_CHAIN_NAME".to_string(), chain_name.to_string()),
            ("CANOE_STEP_NAME".to_string(), step.name.clone()),
            ("CANOE_KAFKA_BOOTSTRAP".to_string(), bootstrap_servers.to_string()),
            ("CANOE_MODE".to_string(), "sql".to_string()),
            ("CANOE_TRANSPORT".to_string(), "batch".to_string()),
        ];
        if !step.lookups.is_empty() {
            env.push(("CANOE_LOOKUPS".to_string(), serde_json::to_string(&step.lookups)?));
        }
        if !step.encrypt.is_empty() {
            env.push(("CANOE_ENCRYPT_FIELDS".to_string(), serde_json::to_string(&step.encrypt)?));
        }
        let spc_name = self.ensure_secrets(&name, &step.secrets).await?;
        if spc_name.is_some() {
            env.push(("CANOE_SECRETS_PATH".to_string(), canoe_k8s::SECRETS_MOUNT_PATH.to_string()));
        }
        let args = self.spark_submit_args(&name, &step.spark, &env, spc_name.as_deref(), event_log_path, pod_s3_endpoint);
        self.submit_batch_job(chain_name, &step.name, &step.spark, args).await
    }

    /// DataFrame twin of `ensure_sql_batch_step` — see its docs.
    pub async fn ensure_dataframe_batch_step(
        &self,
        chain_name: &str,
        step: &SparkDataFrameStep,
        bootstrap_servers: &str,
        event_log_path: Option<&str>,
        pod_s3_endpoint: Option<&str>,
    ) -> anyhow::Result<String> {
        let name = launcher_name(chain_name, &step.name);
        let mut env = vec![
            ("CANOE_CODE_SOURCE".to_string(), serde_json::to_string(&step.code)?),
            ("CANOE_INPUT_PATH".to_string(), step.input_path.clone()),
            ("CANOE_OUTPUT_PATH".to_string(), step.output_path.clone()),
            ("CANOE_INPUT_SIGNAL".to_string(), step.input_signal.clone()),
            ("CANOE_CHECKPOINT_PATH".to_string(), format!("{}/.checkpoint.json", step.output_path.trim_end_matches('/'))),
            ("CANOE_OUTPUT_PARTITION_BY".to_string(), step.output.partition_by.join(",")),
            ("CANOE_CHAIN_NAME".to_string(), chain_name.to_string()),
            ("CANOE_STEP_NAME".to_string(), step.name.clone()),
            ("CANOE_KAFKA_BOOTSTRAP".to_string(), bootstrap_servers.to_string()),
            ("CANOE_MODE".to_string(), "dataframe".to_string()),
            ("CANOE_TRANSPORT".to_string(), "batch".to_string()),
        ];
        if !step.lookups.is_empty() {
            env.push(("CANOE_LOOKUPS".to_string(), serde_json::to_string(&step.lookups)?));
        }
        if !step.encrypt.is_empty() {
            env.push(("CANOE_ENCRYPT_FIELDS".to_string(), serde_json::to_string(&step.encrypt)?));
        }
        let spc_name = self.ensure_secrets(&name, &step.secrets).await?;
        if spc_name.is_some() {
            env.push(("CANOE_SECRETS_PATH".to_string(), canoe_k8s::SECRETS_MOUNT_PATH.to_string()));
        }
        let args = self.spark_submit_args(&name, &step.spark, &env, spc_name.as_deref(), event_log_path, pod_s3_endpoint);
        self.submit_batch_job(chain_name, &step.name, &step.spark, args).await
    }

    /// Wraps `spark-submit <args>` as a one-shot Job via
    /// `canoe_k8s::StepJobRunner::create_job` — the wrapper Job's own pod
    /// (which just runs spark-submit and exits once the driver
    /// finishes) keeps `self.service_account` (`canoe-spark`), not the
    /// dedicated `canoe-secrets` ServiceAccount `create_job` would
    /// otherwise apply for a non-empty `secrets` list: this pod still
    /// needs `canoe-spark`'s own pod-management permissions to create the
    /// driver/executor pods spark-submit requests, and any of this
    /// step's own `secrets` are already mounted on the *driver* pod via
    /// `spark_submit_args`'s CSI-volume confs above, not on this
    /// wrapper — so `create_job` is called with an empty `secrets`
    /// equivalent (no volumes/mounts, explicit service account) rather
    /// than letting it infer either from a step type it doesn't see here.
    async fn submit_batch_job(
        &self,
        chain_name: &str,
        step_name: &str,
        spark: &SparkJobConfig,
        args: Vec<String>,
    ) -> anyhow::Result<String> {
        let runner = canoe_k8s::StepJobRunner::new(self.client.clone(), self.namespace.clone(), spark.image.clone());
        let env = vec![EnvVar {
            name: "CANOE_LAUNCHER_NAME".to_string(),
            value: Some(launcher_name(chain_name, step_name)),
            ..Default::default()
        }];
        runner
            .create_job(
                chain_name,
                step_name,
                "spark-submit",
                &spark.image,
                Some(vec!["/opt/spark/bin/spark-submit".to_string()]),
                Some(args),
                env,
                &PodResources::default(),
                canoe_config::default_backoff_limit(),
                Vec::new(),
                Vec::new(),
                Some(self.service_account.clone()),
            )
            .await
    }

    /// Idempotently provisions a `SecretProviderClass` (named after this
    /// step's launcher, deterministic across re-applies) when `secrets`
    /// is non-empty, returning its name for `spark_submit_args` to
    /// reference via a CSI volume `--conf`. `None` when `secrets` is empty
    /// — no K8s object created, no volume added.
    async fn ensure_secrets(&self, launcher_name: &str, secrets: &[String]) -> anyhow::Result<Option<String>> {
        if secrets.is_empty() {
            return Ok(None);
        }
        let spc_name = format!("{launcher_name}-secrets");
        canoe_k8s::ensure_secret_provider_class(&self.client, &self.namespace, &spc_name, secrets).await?;
        Ok(Some(spc_name))
    }

    /// `spark-submit` argv driving cluster-mode submission against this
    /// cluster's API server, with the driver/executor pod specs the config
    /// asks for. `env` becomes `--conf spark.kubernetes.driverEnv.<KEY>=
    /// <VALUE>` per pair — cluster mode's standard way to set env vars on
    /// the driver pod spark-submit itself creates (there's no pod spec to
    /// author directly, unlike the launcher's own `apply_launcher` below).
    /// The actual PySpark entrypoint (`canoe_spark_runner.py`, baked into
    /// `spark.image`) reads these CANOE_* env vars to build its streaming
    /// query.
    ///
    /// **Known gap, pre-existing and unaddressed here**: unlike
    /// `pod_manager.rs`/`adapter_job.rs`, this launcher never forwards the
    /// coordinator's own `CANOE_ENCRYPT_KEY_*` env vars onto the driver
    /// pod — a Spark step's `CANOE_ENCRYPT_FIELDS` (below) tells
    /// `canoe_spark_runner.py` *which* fields to encrypt/decrypt, but the
    /// actual key material only reaches it if the driver pod happens to
    /// have ambient credentials some other way (IRSA in real EKS). Same
    /// for general S3 credential passthrough beyond what event logging
    /// needs (see `event_log_path` below, which closes this gap **only**
    /// for the specific env vars/confs event logging itself needs) —
    /// Spark support there stays explicitly out of scope for now (see
    /// README.md's "Local dev stack").
    ///
    /// `event_log_path`, when `Some` (from `canoe-coordinator`'s
    /// `activation::resolve_event_log` — this function itself does no
    /// merging, just consumes the final answer), adds
    /// `spark.eventLog.enabled=true`/`spark.eventLog.dir=<path>`, the
    /// `spark.hadoop.fs.s3a.*` confs Spark's bundled Hadoop S3A connector
    /// needs to actually write there, and the coordinator's own S3
    /// credentials as driver env vars (via `canoe_k8s::s3_passthrough_pairs`,
    /// keyed by `pod_s3_endpoint` the same way `pod_manager.rs`/
    /// `adapter_job.rs` already do for Python/uv/adapter pods) — scoped
    /// specifically to what event logging needs, not a general fix for
    /// the gap above. **Unverified in this sandbox** (no cluster to
    /// actually launch a driver against): whether `spark.eventLog.dir`
    /// needs to exist as a directory before Spark will write to it under
    /// S3A — if so, an explicit "ensure prefix exists" step would need to
    /// run before the first launch; not added here since it couldn't be
    /// confirmed either way.
    ///
    /// `secret_provider_class_name`, when `Some`, adds a CSI volume to the
    /// driver pod via `spark.kubernetes.driver.volumes.csi.*` confs —
    /// cluster mode's equivalent of `pod_manager.rs`'s `PodSpec.volumes`,
    /// since this launcher never authors the driver pod spec directly
    /// (spark-submit itself creates it). Deliberately does **not** switch
    /// `spark.kubernetes.authenticate.driver.serviceAccountName` to the
    /// dedicated `canoe-secrets` ServiceAccount `pod_manager.rs`/
    /// `adapter_job.rs` use — a Spark driver already needs `self.service_account`'s
    /// (`canoe-spark`) own pod-management permissions to create executors,
    /// which swapping would lose. Its IAM role needs Secrets Manager
    /// permission granted out-of-band instead, same as every other IRSA
    /// permission in this repo — see `deploy/k8s/rbac.yaml`.
    fn spark_submit_args(
        &self,
        app_name: &str,
        spark: &SparkJobConfig,
        env: &[(String, String)],
        secret_provider_class_name: Option<&str>,
        event_log_path: Option<&str>,
        pod_s3_endpoint: Option<&str>,
    ) -> Vec<String> {
        let mut args = vec![
            "--master".to_string(),
            format!("k8s://{}", self.k8s_api_server),
            "--deploy-mode".to_string(),
            "cluster".to_string(),
            "--name".to_string(),
            app_name.to_string(),
            "--conf".to_string(),
            format!("spark.kubernetes.namespace={}", self.namespace),
            "--conf".to_string(),
            format!(
                "spark.kubernetes.authenticate.driver.serviceAccountName={}",
                self.service_account
            ),
            "--conf".to_string(),
            format!("spark.kubernetes.container.image={}", spark.image),
            "--conf".to_string(),
            format!("spark.executor.instances={}", spark.executor_instances),
            "--conf".to_string(),
            format!("spark.executor.cores={}", spark.executor_cores),
            "--conf".to_string(),
            format!("spark.executor.memory={}", spark.executor_memory),
            "--conf".to_string(),
            format!("spark.driver.memory={}", spark.driver_memory),
        ];
        for (k, v) in &spark.extra_conf {
            args.push("--conf".to_string());
            args.push(format!("{k}={v}"));
        }

        // See this function's doc comment for what `event_log_path`
        // closes and what stays unverified.
        let s3_pairs = if event_log_path.is_some() { canoe_k8s::s3_passthrough_pairs(pod_s3_endpoint) } else { Vec::new() };
        args.extend(event_log_confs(event_log_path, &s3_pairs));

        let mut driver_env: Vec<(String, String)> = env.to_vec();
        driver_env.extend(s3_pairs);
        for (k, v) in &driver_env {
            args.push("--conf".to_string());
            args.push(format!("spark.kubernetes.driverEnv.{k}={v}"));
        }
        if let Some(spc_name) = secret_provider_class_name {
            let volume = "secrets-store";
            for conf in [
                format!("spark.kubernetes.driver.volumes.csi.{volume}.mount.path={}", canoe_k8s::SECRETS_MOUNT_PATH),
                format!("spark.kubernetes.driver.volumes.csi.{volume}.mount.readOnly=true"),
                format!("spark.kubernetes.driver.volumes.csi.{volume}.options.driver=secrets-store.csi.k8s.io"),
                format!("spark.kubernetes.driver.volumes.csi.{volume}.options.secretProviderClass={spc_name}"),
                format!("spark.kubernetes.driver.volumes.csi.{volume}.options.readOnly=true"),
            ] {
                args.push("--conf".to_string());
                args.push(conf);
            }
        }
        args.push("local:///opt/canoe/canoe_spark_runner.py".to_string());
        args
    }

    async fn apply_launcher(
        &self,
        name: &str,
        chain_name: &str,
        step_name: &str,
        spark: &SparkJobConfig,
        args: Vec<String>,
    ) -> anyhow::Result<()> {
        let mut labels = BTreeMap::new();
        labels.insert("app".to_string(), "canoe-spark-launcher".to_string());
        labels.insert("canoe.io/chain".to_string(), chain_name.to_string());
        labels.insert("canoe.io/step".to_string(), step_name.to_string());

        let container = Container {
            name: "spark-submit".to_string(),
            image: Some(spark.image.clone()),
            command: Some(vec!["/opt/spark/bin/spark-submit".to_string()]),
            args: Some(args),
            env: Some(vec![EnvVar {
                name: "CANOE_LAUNCHER_NAME".to_string(),
                value: Some(name.to_string()),
                ..Default::default()
            }]),
            ..Default::default()
        };

        let deployment = Deployment {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(self.namespace.clone()),
                labels: Some(labels.clone()),
                ..Default::default()
            },
            spec: Some(DeploymentSpec {
                replicas: Some(1),
                selector: LabelSelector {
                    match_labels: Some(labels.clone()),
                    ..Default::default()
                },
                template: PodTemplateSpec {
                    metadata: Some(ObjectMeta {
                        labels: Some(labels),
                        ..Default::default()
                    }),
                    spec: Some(PodSpec {
                        service_account_name: Some(self.service_account.clone()),
                        containers: vec![container],
                        restart_policy: Some("Always".to_string()),
                        ..Default::default()
                    }),
                },
                ..Default::default()
            }),
            ..Default::default()
        };

        let api: Api<Deployment> = Api::namespaced(self.client.clone(), &self.namespace);
        let pp = PatchParams::apply("canoe-coordinator").force();
        api.patch(name, &pp, &Patch::Apply(&deployment)).await?;
        Ok(())
    }
}

pub fn launcher_name(chain_name: &str, step_name: &str) -> String {
    format!("canoe-spark-{chain_name}-{step_name}").replace('_', "-")
}

/// Deletes a streaming-mode Spark step's launcher `Deployment` — the
/// "cancel" half of lifecycle control for `KafkaSparkSqlStep`/
/// `KafkaSparkDataFrameStep`. Spark streaming has no ongoing reconcile
/// loop at all (`ensure_sql_step`/`ensure_dataframe_step` are one-shot
/// idempotent applies of an always-on Deployment — see their docs), so
/// unlike a batch-mode step's pause/resume or a persistent-pod streaming
/// step's scale-to-zero, there's no loop to notice a pause flag on its
/// next tick. This is deliberately coarser: cancel deletes the
/// Deployment outright (foreground propagation, so its driver/executor
/// pods go with it — same semantics `canoe_k8s::delete_job` uses for a
/// batch-mode step's Job), and "re-run" is just calling
/// `ensure_sql_step`/`ensure_dataframe_step` again (the same function
/// that originally started it — an idempotent apply, not a special
/// "resume" path).
pub async fn delete_launcher(client: &Client, namespace: &str, name: &str) -> anyhow::Result<()> {
    let api: Api<Deployment> = Api::namespaced(client.clone(), namespace);
    match api.delete(name, &canoe_k8s::foreground_delete_params()).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(e)) if e.code == 404 => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// The `--conf spark.eventLog.*`/`spark.hadoop.fs.s3a.*` flag pairs
/// `spark_submit_args` adds when `event_log_path` is `Some` — pulled out
/// as a pure function (no `&self`, no K8s client) specifically so it's
/// unit-testable without constructing a real `SparkLauncher`/`kube::Client`,
/// same "extract the pure decision logic" pattern used for
/// `canoe-coordinator`'s `decide_batch_action`/`resolve_event_log`.
/// `s3_pairs` is whatever `canoe_k8s::s3_passthrough_pairs` resolved
/// (already computed by the caller, since it also needs to fold into
/// `driverEnv`) — this function only reads `AWS_ENDPOINT` out of it to
/// derive `fs.s3a.endpoint`/`connection.ssl.enabled`, it does no S3/env
/// lookups of its own. Returns an empty `Vec` — no confs at all — when
/// `event_log_path` is `None`.
fn event_log_confs(event_log_path: Option<&str>, s3_pairs: &[(String, String)]) -> Vec<String> {
    let Some(path) = event_log_path else {
        return Vec::new();
    };
    let mut confs = vec![
        "--conf".to_string(),
        "spark.eventLog.enabled=true".to_string(),
        "--conf".to_string(),
        format!("spark.eventLog.dir={path}"),
        "--conf".to_string(),
        "spark.hadoop.fs.s3a.path.style.access=true".to_string(),
    ];
    // `AWS_ENDPOINT`, if forwarded, is also this step's S3A endpoint
    // override (MinIO/any non-AWS S3-compatible store) — `ssl.enabled`
    // follows its scheme so both a local `http://` MinIO endpoint and a
    // `https://` one work without a separate flag. Omitted entirely when
    // nothing forwards an endpoint (real AWS S3, no override) — Hadoop's
    // S3A connector already defaults correctly there.
    if let Some((_, endpoint)) = s3_pairs.iter().find(|(k, _)| k == "AWS_ENDPOINT") {
        confs.push("--conf".to_string());
        confs.push(format!("spark.hadoop.fs.s3a.endpoint={endpoint}"));
        confs.push("--conf".to_string());
        confs.push(format!("spark.hadoop.fs.s3a.connection.ssl.enabled={}", endpoint.starts_with("https://")));
    }
    confs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_log_confs_empty_when_path_is_none() {
        assert!(event_log_confs(None, &[]).is_empty());
    }

    #[test]
    fn event_log_confs_present_when_path_is_some() {
        let confs = event_log_confs(Some("s3a://canoe-lake/_spark-events"), &[]);
        assert!(confs.contains(&"spark.eventLog.enabled=true".to_string()));
        assert!(confs.contains(&"spark.eventLog.dir=s3a://canoe-lake/_spark-events".to_string()));
        assert!(confs.contains(&"spark.hadoop.fs.s3a.path.style.access=true".to_string()));
    }

    #[test]
    fn event_log_confs_sets_s3a_endpoint_and_disables_ssl_for_an_http_minio_endpoint() {
        let s3_pairs = vec![("AWS_ENDPOINT".to_string(), "http://minio.canoe-system.svc.cluster.local:9000".to_string())];
        let confs = event_log_confs(Some("s3a://canoe-lake/_spark-events"), &s3_pairs);
        assert!(confs.contains(&"spark.hadoop.fs.s3a.endpoint=http://minio.canoe-system.svc.cluster.local:9000".to_string()));
        assert!(confs.contains(&"spark.hadoop.fs.s3a.connection.ssl.enabled=false".to_string()));
    }

    #[test]
    fn event_log_confs_enables_ssl_for_an_https_endpoint() {
        let s3_pairs = vec![("AWS_ENDPOINT".to_string(), "https://s3.example.com".to_string())];
        let confs = event_log_confs(Some("s3a://canoe-lake/_spark-events"), &s3_pairs);
        assert!(confs.contains(&"spark.hadoop.fs.s3a.connection.ssl.enabled=true".to_string()));
    }

    #[test]
    fn event_log_confs_omits_s3a_endpoint_when_nothing_forwards_one() {
        let confs = event_log_confs(Some("s3a://canoe-lake/_spark-events"), &[]);
        assert!(!confs.iter().any(|c| c.starts_with("spark.hadoop.fs.s3a.endpoint")));
        assert!(!confs.iter().any(|c| c.starts_with("spark.hadoop.fs.s3a.connection.ssl.enabled")));
    }
}
