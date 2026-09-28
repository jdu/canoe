//! Builds and creates the one-shot K8s Job that runs a single batch-mode
//! Python step execution: fires, drains whatever `run_id=` backlog exists
//! under the step's `input_path`, exits. Generalizes `adapter_job.rs`'s
//! Job-spec pattern (one `Job` object per execution, `restartPolicy:
//! Never`, `backoffLimit`, `ttlSecondsAfterFinished`) with the same
//! `CANOE_*` env var contract `pod_manager.rs`'s now-removed
//! `create_python_pod` used to build for a persistent Pod — a batch-mode
//! step Job needs the identical contract, just on a Job spec instead of a
//! Pod spec (plus `CANOE_RUN_ONCE=true`, telling `workers/python-worker`
//! to drain backlog once and exit rather than loop forever). See
//! `crates/canoe-coordinator/src/activation.rs::start_python_step`, whose
//! reconcile loop calls `run_python_step_once` here instead of topping up
//! a pod set.

use canoe_config::LookupConfig;
use k8s_openapi::api::batch::v1::{Job, JobSpec};
use k8s_openapi::api::core::v1::{Container, EnvVar, PodSpec, PodTemplateSpec, ResourceRequirements, Volume, VolumeMount};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{ListParams, PostParams};
use kube::{Api, Client};
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone)]
pub struct StepJobRunner {
    client: Client,
    namespace: String,
    worker_image: String,
}

impl StepJobRunner {
    pub fn new(client: Client, namespace: impl Into<String>, worker_image: impl Into<String>) -> Self {
        Self {
            client,
            namespace: namespace.into(),
            worker_image: worker_image.into(),
        }
    }

    /// Deterministic label value shared by every Job execution of this
    /// step — same "one label groups every instance" convention
    /// `PodManager::pod_set_name` already uses for persistent pods,
    /// reused here so `list_in_flight` can find this step's own Jobs.
    pub fn job_set_name(chain_name: &str, step_name: &str) -> String {
        format!("canoe-batch-{chain_name}-{step_name}").replace('_', "-")
    }

    /// Names of this step's Jobs that haven't finished yet (no
    /// `succeeded`/`failed` count recorded) — the reconcile loop checks
    /// this before firing a new Job, so a step with a long-running
    /// execution doesn't get a second overlapping one on the next tick.
    /// Finished Jobs age out on their own via `ttlSecondsAfterFinished`,
    /// so this never needs to clean anything up itself.
    pub async fn list_in_flight(&self, job_set_name: &str) -> anyhow::Result<Vec<String>> {
        let api: Api<Job> = Api::namespaced(self.client.clone(), &self.namespace);
        let lp = ListParams::default().labels(&format!("canoe.io/job-set={job_set_name}"));
        let jobs = api.list(&lp).await?;
        Ok(jobs
            .items
            .into_iter()
            .filter_map(|job| {
                let name = job.metadata.name.clone()?;
                let status = job.status.unwrap_or_default();
                let finished = status.succeeded.unwrap_or(0) > 0 || status.failed.unwrap_or(0) > 0;
                if finished {
                    None
                } else {
                    Some(name)
                }
            })
            .collect())
    }

    /// Polls one Job's status by name — thin pass-through to
    /// `crate::adapter_job::job_status` (reused directly, not
    /// reimplemented: task Jobs and step Jobs are both plain `batch/v1`
    /// `Job`s, checked the identical way).
    pub async fn job_status(&self, job_name: &str) -> anyhow::Result<crate::JobStatus> {
        crate::adapter_job::job_status(&self.client, &self.namespace, job_name).await
    }

    /// See `crate::adapter_job::delete_job`'s docs.
    pub async fn delete_job(&self, job_name: &str) -> anyhow::Result<()> {
        crate::adapter_job::delete_job(&self.client, &self.namespace, job_name).await
    }

    /// Fires one Job for this step, using the same `CANOE_*` env var
    /// contract a persistent Pod would have used, plus `CANOE_RUN_ONCE`
    /// (see `crate` docs) so `workers/python-worker/canoe_worker/main.py`
    /// drains currently-available backlog once and exits instead of
    /// looping forever.
    #[allow(clippy::too_many_arguments)]
    pub async fn run_python_step_once(
        &self,
        chain_name: &str,
        step: &canoe_config::PythonStep,
        bootstrap_servers: &str,
        metrics_url: &str,
        log_base_path: Option<&str>,
        pod_s3_endpoint: Option<&str>,
    ) -> anyhow::Result<String> {
        let job_set_name = Self::job_set_name(chain_name, &step.name);

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
                name: "CANOE_ENTRYPOINT".to_string(),
                value: Some(step.entrypoint.clone()),
                ..Default::default()
            },
            EnvVar {
                name: "CANOE_CODE_SOURCE".to_string(),
                value: Some(serde_json::to_string(&step.code)?),
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
            // Tells the S3+signal loop to drain currently-available
            // backlog and then exit, instead of looping forever waiting
            // for the next signal — the one behavioral difference from a
            // persistent Pod's env var contract. `workers/python-worker`
            // treats an unset/absent value as "loop forever" (today's
            // only behavior), so this is additive, not a breaking change
            // to the worker's existing contract.
            EnvVar {
                name: "CANOE_RUN_ONCE".to_string(),
                value: Some("true".to_string()),
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
        env.extend(lookup_env_vars(&step.lookups));
        env.extend(crate::encrypt_env::encrypt_fields_env_var(&step.encrypt));
        for (key, value) in &step.env {
            env.push(EnvVar {
                name: key.clone(),
                value: Some(value.clone()),
                ..Default::default()
            });
        }

        let secret_provider_class_name = if step.secrets.is_empty() {
            None
        } else {
            let spc_name = format!("{job_set_name}-secrets");
            crate::secrets::ensure_secret_provider_class(&self.client, &self.namespace, &spc_name, &step.secrets).await?;
            Some(spc_name)
        };
        let mut volumes: Vec<Volume> = Vec::new();
        let mut volume_mounts: Vec<VolumeMount> = Vec::new();
        let mut service_account_name = None;
        if let Some(spc_name) = &secret_provider_class_name {
            volumes.push(crate::secrets::secrets_volume(spc_name));
            volume_mounts.push(crate::secrets::secrets_volume_mount());
            env.push(crate::secrets::secrets_path_env_var());
            service_account_name = Some(crate::secrets::SECRETS_SERVICE_ACCOUNT.to_string());
        }

        self.create_job(
            chain_name,
            &step.name,
            "worker",
            &self.worker_image.clone(),
            None,
            None,
            env,
            &step.resources,
            step.scaling.backoff_limit,
            volumes,
            volume_mounts,
            service_account_name,
        )
        .await
    }

    /// Generic Job creation shared by every batch-mode step kind —
    /// `run_python_step_once` above builds Python's own env vars then
    /// calls this; `canoe-spark`'s batch-mode Spark launch path
    /// (`SparkLauncher::ensure_sql_batch_step`/`ensure_dataframe_batch_step`)
    /// calls it directly with a `spark-submit` command/args instead of a
    /// `CANOE_*`-env-driven Python entrypoint. Everything about a
    /// batch-mode step's Job is identical regardless of engine — labels
    /// (so `list_in_flight`/`job_status` work the same way for either),
    /// `restartPolicy: Never`, `backoffLimit`, `ttlSecondsAfterFinished`
    /// — except the container's own image/command/args/env and its
    /// resource/volume/service-account wiring, which the caller supplies
    /// directly rather than this function inferring them from a step
    /// type it doesn't know about.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_job(
        &self,
        chain_name: &str,
        step_name: &str,
        container_name: &str,
        image: &str,
        command: Option<Vec<String>>,
        args: Option<Vec<String>>,
        env: Vec<EnvVar>,
        resources: &canoe_config::PodResources,
        backoff_limit: u32,
        volumes: Vec<Volume>,
        volume_mounts: Vec<VolumeMount>,
        service_account_name: Option<String>,
    ) -> anyhow::Result<String> {
        let job_set_name = Self::job_set_name(chain_name, step_name);
        let name = job_instance_name(&job_set_name);

        let mut labels = BTreeMap::new();
        labels.insert("app".to_string(), "canoe-batch-worker".to_string());
        labels.insert("canoe.io/chain".to_string(), chain_name.to_string());
        labels.insert("canoe.io/step".to_string(), step_name.to_string());
        labels.insert("canoe.io/job-set".to_string(), job_set_name);

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

        let container = Container {
            name: container_name.to_string(),
            image: Some(image.to_string()),
            command,
            args,
            env: Some(env),
            resources: Some(resource_requirements),
            volume_mounts: if volume_mounts.is_empty() { None } else { Some(volume_mounts) },
            ..Default::default()
        };

        let job = Job {
            metadata: ObjectMeta {
                name: Some(name.clone()),
                namespace: Some(self.namespace.clone()),
                labels: Some(labels.clone()),
                ..Default::default()
            },
            spec: Some(JobSpec {
                backoff_limit: Some(backoff_limit as i32),
                ttl_seconds_after_finished: Some(3600),
                template: PodTemplateSpec {
                    metadata: Some(ObjectMeta {
                        labels: Some(labels),
                        ..Default::default()
                    }),
                    spec: Some(PodSpec {
                        containers: vec![container],
                        restart_policy: Some("Never".to_string()),
                        volumes: if volumes.is_empty() { None } else { Some(volumes) },
                        service_account_name,
                        ..Default::default()
                    }),
                },
                ..Default::default()
            }),
            ..Default::default()
        };

        let api: Api<Job> = Api::namespaced(self.client.clone(), &self.namespace);
        api.create(&PostParams::default(), &job).await?;
        Ok(name)
    }
}

/// Same sanitizing convention `PodManager::lookup_env_vars` uses —
/// duplicated rather than shared since `pod_manager.rs`'s version is a
/// private method on a different type; kept in lockstep by the shared
/// unit tests in both modules asserting the identical env var shape.
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

fn job_instance_name(job_set_name: &str) -> String {
    let millis = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0);
    format!("{job_set_name}-{millis}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_set_name_matches_pod_set_naming_convention() {
        assert_eq!(StepJobRunner::job_set_name("orders", "validate"), "canoe-batch-orders-validate");
    }

    #[test]
    fn lookup_env_vars_shape_matches_pod_manager() {
        let lookups = vec![LookupConfig { name: "customers".to_string(), path: "s3://lake/a".to_string() }];
        let env = lookup_env_vars(&lookups);
        assert_eq!(env[0].name, "CANOE_LOOKUP_CUSTOMERS_PATH");
        assert_eq!(env[0].value.as_deref(), Some("s3://lake/a"));
    }

    #[test]
    fn job_instance_name_is_unique_per_call() {
        let a = job_instance_name("canoe-batch-orders-validate");
        std::thread::sleep(std::time::Duration::from_millis(2));
        let b = job_instance_name("canoe-batch-orders-validate");
        assert_ne!(a, b);
        assert!(a.starts_with("canoe-batch-orders-validate-"));
    }
}
