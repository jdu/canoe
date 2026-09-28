//! Creates/lists/deletes the plain `Pod`s that make up a step's worker
//! "quorum" — deliberately *not* a Deployment/ReplicaSet. A Deployment's
//! pod template is required by the K8s API to use `restartPolicy: Always`;
//! there's no way to opt out of it, so a step's container crashing on a
//! deterministic bug would crash-loop forever under kubelet's own backoff
//! (still billed on Fargate — the task keeps running, just repeatedly
//! restarting). Plain `Pod`s support `restartPolicy: Never`: a crashed
//! pod just stays `Failed`, no restart, no recreation, until *this*
//! coordinator's own reconcile loop (`crates/canoe-k8s/src/autoscaler.rs`)
//! decides — on its own cadence, with its own circuit breaker — whether
//! to replace it. See README.md's "Step pods, not Deployments".
//!
//! Every step's pods share two labels (`canoe.io/chain`, `canoe.io/step`)
//! that together name its "pod set"; `Autoscaler` lists by that selector
//! to see current state and creates/deletes individual `Pod` objects
//! (each with its own generated name) to reach a desired count. All pods
//! in one set share the same Kafka consumer `group.id` (`pod_set_name`)
//! so they split partitions the normal consumer-group way — only each
//! `Pod`'s own `metadata.name` needs to be unique.

use canoe_config::{KafkaPythonStep, LookupConfig, UvStep};
use k8s_openapi::api::core::v1::{Container, EnvVar, Pod, PodSpec, ResourceRequirements, Volume, VolumeMount};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{ListParams, PostParams};
use kube::{Api, Client};
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub struct PodManager {
    client: Client,
    namespace: String,
    worker_image: String,
}

/// One pod's state, as far as the autoscaler's reconcile loop cares —
/// alive (counts toward current replicas) or dead (needs cleanup, and
/// factors into the circuit breaker). See `Autoscaler::reconcile`.
pub struct PodState {
    pub name: String,
    pub alive: bool,
    /// How long ago this pod was created — `None` if K8s hasn't recorded
    /// a creation timestamp yet (shouldn't happen in practice, treated as
    /// "just created" to be conservative about the fast-failure window).
    pub age: Duration,
}

impl PodManager {
    pub fn new(client: Client, namespace: impl Into<String>, worker_image: impl Into<String>) -> Self {
        Self {
            client,
            namespace: namespace.into(),
            worker_image: worker_image.into(),
        }
    }

    pub fn pod_set_name(chain_name: &str, step_name: &str) -> String {
        format!("canoe-{chain_name}-{step_name}").replace('_', "-")
    }

    pub fn uv_pod_set_name(chain_name: &str, step_name: &str) -> String {
        format!("canoe-uv-{chain_name}-{step_name}").replace('_', "-")
    }

    /// Lists every pod belonging to `pod_set_name` (by label selector),
    /// classified into alive/dead. Doesn't delete anything itself — the
    /// caller (`Autoscaler`) decides what to do with dead ones after
    /// folding them into the circuit breaker's failure count.
    pub async fn list_pods(&self, pod_set_name: &str) -> anyhow::Result<Vec<PodState>> {
        let api: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        let lp = ListParams::default().labels(&format!("canoe.io/pod-set={pod_set_name}"));
        let pods = api.list(&lp).await?;
        let now = SystemTime::now();

        Ok(pods
            .items
            .into_iter()
            .filter_map(|pod| {
                let name = pod.metadata.name.clone()?;
                let phase = pod.status.as_ref().and_then(|s| s.phase.as_deref()).unwrap_or("Pending");
                let alive = !matches!(phase, "Failed" | "Succeeded");
                let age = pod
                    .metadata
                    .creation_timestamp
                    .and_then(|t| now.duration_since(SystemTime::from(t.0)).ok())
                    .unwrap_or(Duration::ZERO);
                Some(PodState { name, alive, age })
            })
            .collect())
    }

    pub async fn delete_pod(&self, name: &str) -> anyhow::Result<()> {
        let api: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        // A pod that's already gone (deleted by something else, or this
        // is a retry after a prior successful delete whose response we
        // didn't get) isn't a real error — nothing left to clean up.
        match api.delete(name, &Default::default()).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(e)) if e.code == 404 => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// One `CANOE_LOOKUP_<NAME>_PATH` env var per declared `lookup` block
    /// (name uppercased, non-alphanumeric characters collapsed to `_`) —
    /// the worker fetches each lookup's `latest.json` snapshot itself at
    /// run time; canoe only hands it the resolved path. See
    /// `canoe_config::LookupConfig`'s docs.
    fn lookup_env_vars(lookups: &[LookupConfig]) -> Vec<EnvVar> {
        lookups
            .iter()
            .map(|l| {
                let key: String = l
                    .name
                    .chars()
                    .map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_uppercase() } else { '_' })
                    .collect();
                EnvVar {
                    name: format!("CANOE_LOOKUP_{key}_PATH"),
                    value: Some(l.path.clone()),
                    ..Default::default()
                }
            })
            .collect()
    }

    /// Similar env-var-building shape to what batch-mode `PythonStep`s
    /// used before they moved to Job-based execution (see
    /// `crates/canoe-k8s/src/step_job.rs` and
    /// `crates/canoe-coordinator/src/activation.rs::start_python_step`),
    /// for `dynamic_route`'s per-discovered-value pods — these stay on
    /// the old Kafka-topic env var contract (`CANOE_INPUT_TOPIC`/
    /// `CANOE_OUTPUT_TOPIC`/`CANOE_CONSUMER_GROUP`) since `KafkaPythonStep`
    /// is Kafka-topic transport, not S3 + signal — see `KafkaPythonStep`'s
    /// docs.
    pub async fn create_kafka_python_pod(
        &self,
        chain_name: &str,
        step: &KafkaPythonStep,
        bootstrap_servers: &str,
        metrics_url: &str,
        log_base_path: Option<&str>,
        pod_s3_endpoint: Option<&str>,
    ) -> anyhow::Result<()> {
        let pod_set_name = Self::pod_set_name(chain_name, &step.name);
        let mut labels = BTreeMap::new();
        labels.insert("app".to_string(), "canoe-worker".to_string());
        labels.insert("canoe.io/chain".to_string(), chain_name.to_string());
        labels.insert("canoe.io/step".to_string(), step.name.clone());
        labels.insert("canoe.io/pod-set".to_string(), pod_set_name.clone());

        let mut env = vec![
            EnvVar {
                name: "CANOE_KAFKA_BOOTSTRAP".to_string(),
                value: Some(bootstrap_servers.to_string()),
                ..Default::default()
            },
            EnvVar {
                name: "CANOE_INPUT_TOPIC".to_string(),
                value: Some(step.input_topic.clone()),
                ..Default::default()
            },
            EnvVar {
                name: "CANOE_OUTPUT_TOPIC".to_string(),
                value: Some(step.output_topic.clone()),
                ..Default::default()
            },
            EnvVar {
                name: "CANOE_CONSUMER_GROUP".to_string(),
                value: Some(pod_set_name.clone()),
                ..Default::default()
            },
            EnvVar {
                name: "CANOE_ENTRYPOINT".to_string(),
                value: Some(step.entrypoint.clone()),
                ..Default::default()
            },
            EnvVar {
                name: "CANOE_CODE_SOURCE".to_string(),
                value: Some(serde_json::to_string(&step.code).expect("CodeSource always serializes")),
                ..Default::default()
            },
            EnvVar {
                name: "CANOE_CHAIN_NAME".to_string(),
                value: Some(chain_name.to_string()),
                ..Default::default()
            },
            EnvVar {
                name: "CANOE_STEP_NAME".to_string(),
                value: Some(step.name.clone()),
                ..Default::default()
            },
            EnvVar {
                name: "CANOE_METRICS_URL".to_string(),
                value: Some(metrics_url.to_string()),
                ..Default::default()
            },
        ];
        if let Some(log_base_path) = log_base_path {
            env.push(EnvVar {
                name: "CANOE_LOG_BASE_PATH".to_string(),
                value: Some(log_base_path.to_string()),
                ..Default::default()
            });
        }
        if let Some(batch) = &step.batch {
            env.push(EnvVar {
                name: "CANOE_BATCH_SIZE".to_string(),
                value: Some(batch.size.to_string()),
                ..Default::default()
            });
            env.push(EnvVar {
                name: "CANOE_BATCH_FLUSH_INTERVAL_SECS".to_string(),
                value: Some(batch.flush_interval_secs.to_string()),
                ..Default::default()
            });
        }
        env.extend(crate::s3_env::s3_passthrough_env(pod_s3_endpoint));
        env.extend(crate::encrypt_env::encrypt_key_passthrough_env());
        for (key, value) in &step.env {
            env.push(EnvVar {
                name: key.clone(),
                value: Some(value.clone()),
                ..Default::default()
            });
        }

        // KafkaPythonStep has no `secrets` field — dynamic_route is out of
        // scope for Secrets Manager mounting, same as `lookup`/`encrypt`.
        self.create_pod(&pod_set_name, labels, env, &step.resources, &[]).await
    }

    pub async fn create_uv_pod(
        &self,
        chain_name: &str,
        step: &UvStep,
        bootstrap_servers: &str,
        metrics_url: &str,
        log_base_path: Option<&str>,
        pod_s3_endpoint: Option<&str>,
    ) -> anyhow::Result<()> {
        let pod_set_name = Self::uv_pod_set_name(chain_name, &step.name);
        let mut labels = BTreeMap::new();
        labels.insert("app".to_string(), "canoe-uv-worker".to_string());
        labels.insert("canoe.io/chain".to_string(), chain_name.to_string());
        labels.insert("canoe.io/step".to_string(), step.name.clone());
        labels.insert("canoe.io/pod-set".to_string(), pod_set_name.clone());

        // No CANOE_ENTRYPOINT: unlike a PythonStep's `def run(record)`
        // contract, a uv step's script is the whole program.
        let mut env = vec![
            EnvVar {
                name: "CANOE_KAFKA_BOOTSTRAP".to_string(),
                value: Some(bootstrap_servers.to_string()),
                ..Default::default()
            },
            EnvVar {
                name: "CANOE_INPUT_PATH".to_string(),
                value: Some(step.input_path.clone()),
                ..Default::default()
            },
            EnvVar {
                name: "CANOE_INPUT_SIGNAL".to_string(),
                value: Some(step.input_signal.clone()),
                ..Default::default()
            },
            EnvVar {
                name: "CANOE_OUTPUT_PATH".to_string(),
                value: Some(step.output_path.clone()),
                ..Default::default()
            },
            EnvVar {
                name: "CANOE_CHECKPOINT_PATH".to_string(),
                value: Some(format!("{}/.checkpoint.json", step.output_path.trim_end_matches('/'))),
                ..Default::default()
            },
            EnvVar {
                name: "CANOE_OUTPUT_BATCH_SIZE".to_string(),
                value: Some(step.output.batch_size.to_string()),
                ..Default::default()
            },
            EnvVar {
                name: "CANOE_OUTPUT_FLUSH_INTERVAL_SECS".to_string(),
                value: Some(step.output.flush_interval_secs.to_string()),
                ..Default::default()
            },
            EnvVar {
                name: "CANOE_OUTPUT_PARTITION_BY".to_string(),
                value: Some(step.output.partition_by.join(",")),
                ..Default::default()
            },
            EnvVar {
                name: "CANOE_CODE_SOURCE".to_string(),
                value: Some(serde_json::to_string(&step.code).expect("CodeSource always serializes")),
                ..Default::default()
            },
            EnvVar {
                name: "CANOE_CHAIN_NAME".to_string(),
                value: Some(chain_name.to_string()),
                ..Default::default()
            },
            EnvVar {
                name: "CANOE_STEP_NAME".to_string(),
                value: Some(step.name.clone()),
                ..Default::default()
            },
            EnvVar {
                name: "CANOE_METRICS_URL".to_string(),
                value: Some(metrics_url.to_string()),
                ..Default::default()
            },
        ];
        if let Some(log_base_path) = log_base_path {
            env.push(EnvVar {
                name: "CANOE_LOG_BASE_PATH".to_string(),
                value: Some(log_base_path.to_string()),
                ..Default::default()
            });
        }
        env.extend(crate::s3_env::s3_passthrough_env(pod_s3_endpoint));
        env.extend(crate::encrypt_env::encrypt_key_passthrough_env());
        env.extend(Self::lookup_env_vars(&step.lookups));
        env.extend(crate::encrypt_env::encrypt_fields_env_var(&step.encrypt));
        for (key, value) in &step.env {
            env.push(EnvVar {
                name: key.clone(),
                value: Some(value.clone()),
                ..Default::default()
            });
        }

        self.create_pod(&pod_set_name, labels, env, &step.resources, &step.secrets).await
    }

    /// Deterministic `SecretProviderClass` name for a given pod set — one
    /// per step (not per pod replica), same idempotent-ensure convention
    /// as the pod set itself: every replica references the same object,
    /// (re-)created once before each top-up rather than tracked per pod.
    fn secret_provider_class_name(pod_set_name: &str) -> String {
        format!("{pod_set_name}-secrets")
    }

    async fn create_pod(
        &self,
        pod_set_name: &str,
        labels: BTreeMap<String, String>,
        mut env: Vec<EnvVar>,
        resources: &canoe_config::PodResources,
        secrets: &[String],
    ) -> anyhow::Result<()> {
        let name = pod_instance_name(pod_set_name);

        let resource_requirements = ResourceRequirements {
            requests: Some(BTreeMap::from([
                ("cpu".to_string(), Quantity(resources.cpu_request.clone())),
                ("memory".to_string(), Quantity(resources.mem_request.clone())),
            ])),
            limits: Some(BTreeMap::from([
                ("cpu".to_string(), Quantity(resources.cpu_limit.clone())),
                ("memory".to_string(), Quantity(resources.mem_limit.clone())),
            ])),
            ..Default::default()
        };

        // Only when `secrets` is declared does this pod get the CSI
        // volume, the dedicated `canoe-secrets` ServiceAccount, and
        // `CANOE_SECRETS_PATH` — a pod with no declared secrets is
        // unaffected, keeping blast radius minimal. See `crate::secrets`.
        let mut volumes: Vec<Volume> = Vec::new();
        let mut volume_mounts: Vec<VolumeMount> = Vec::new();
        let mut service_account_name = None;
        if !secrets.is_empty() {
            let spc_name = Self::secret_provider_class_name(pod_set_name);
            crate::secrets::ensure_secret_provider_class(&self.client, &self.namespace, &spc_name, secrets).await?;
            volumes.push(crate::secrets::secrets_volume(&spc_name));
            volume_mounts.push(crate::secrets::secrets_volume_mount());
            env.push(crate::secrets::secrets_path_env_var());
            service_account_name = Some(crate::secrets::SECRETS_SERVICE_ACCOUNT.to_string());
        }

        let container = Container {
            name: "worker".to_string(),
            image: Some(self.worker_image.clone()),
            env: Some(env),
            resources: Some(resource_requirements),
            volume_mounts: if volume_mounts.is_empty() { None } else { Some(volume_mounts) },
            ..Default::default()
        };

        let pod = Pod {
            metadata: ObjectMeta {
                name: Some(name),
                namespace: Some(self.namespace.clone()),
                labels: Some(labels),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![container],
                // The whole point: a crashed container just stays
                // crashed (phase Failed), no kubelet restart, no silent
                // crash loop. See this module's docs.
                restart_policy: Some("Never".to_string()),
                volumes: if volumes.is_empty() { None } else { Some(volumes) },
                service_account_name,
                ..Default::default()
            }),
            ..Default::default()
        };

        let api: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        api.create(&PostParams::default(), &pod).await?;
        Ok(())
    }
}

/// K8s object names must be <= 253 chars and DNS-label-safe; a millis
/// timestamp keeps every pod in a set uniquely named without needing
/// `generateName` (which would leave the coordinator unable to name the
/// object it just asked for in its own logs/deletion calls).
fn pod_instance_name(pod_set_name: &str) -> String {
    let millis = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0);
    format!("{pod_set_name}-{millis}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_env_vars_uppercases_and_sanitizes_the_name() {
        let lookups = vec![LookupConfig {
            name: "customer_snapshot".to_string(),
            path: "s3://lake/customer_pipeline/v1/customer_snapshot".to_string(),
        }];
        let env = PodManager::lookup_env_vars(&lookups);
        assert_eq!(env.len(), 1);
        assert_eq!(env[0].name, "CANOE_LOOKUP_CUSTOMER_SNAPSHOT_PATH");
        assert_eq!(env[0].value.as_deref(), Some("s3://lake/customer_pipeline/v1/customer_snapshot"));
    }

    #[test]
    fn lookup_env_vars_collapses_non_alphanumeric_characters() {
        let lookups = vec![LookupConfig { name: "my-lookup!".to_string(), path: "s3://lake/x".to_string() }];
        let env = PodManager::lookup_env_vars(&lookups);
        assert_eq!(env[0].name, "CANOE_LOOKUP_MY_LOOKUP__PATH");
    }

    #[test]
    fn no_lookups_produces_no_env_vars() {
        assert!(PodManager::lookup_env_vars(&[]).is_empty());
    }

    #[test]
    fn multiple_lookups_each_get_their_own_env_var() {
        let lookups = vec![
            LookupConfig { name: "customers".to_string(), path: "s3://lake/a".to_string() },
            LookupConfig { name: "products".to_string(), path: "s3://lake/b".to_string() },
        ];
        let env = PodManager::lookup_env_vars(&lookups);
        assert_eq!(env.len(), 2);
        assert_eq!(env[0].name, "CANOE_LOOKUP_CUSTOMERS_PATH");
        assert_eq!(env[1].name, "CANOE_LOOKUP_PRODUCTS_PATH");
    }
}
