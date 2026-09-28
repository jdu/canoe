//! Builds and creates the one-shot K8s Job that runs a single adapter
//! invocation. Unlike a Python step's Deployment (one long-lived object
//! the coordinator patches in place), every adapter run gets its own Job
//! object — creation, not apply — since each run is a discrete event, not
//! ongoing desired state. `crates/canoe-coordinator/src/scheduler.rs`
//! calls `run_once` each time an adapter's schedule comes due.

use canoe_config::{AdapterConfig, TaskConfig};
use k8s_openapi::api::batch::v1::{Job, JobSpec};
use k8s_openapi::api::core::v1::{Container, EnvVar, PodSpec, PodTemplateSpec, ResourceRequirements, Volume, VolumeMount};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{DeleteParams, PostParams, PropagationPolicy};
use kube::{Api, Client};
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct AdapterJobRunner {
    client: Client,
    namespace: String,
    runner_image: String,
}

impl AdapterJobRunner {
    pub fn new(client: Client, namespace: impl Into<String>, runner_image: impl Into<String>) -> Self {
        Self {
            client,
            namespace: namespace.into(),
            runner_image: runner_image.into(),
        }
    }

    /// Creates one Job for this adapter run. Fire-and-forget: the
    /// coordinator doesn't wait on or track completion, matching an
    /// Airflow task instance that's been handed off to its executor — the
    /// Job's own `backoffLimit` covers retries, and `ttlSecondsAfterFinished`
    /// covers cleanup.
    pub async fn run_once(
        &self,
        adapter: &AdapterConfig,
        bootstrap_servers: &str,
        metrics_url: &str,
        log_base_path: Option<&str>,
        pod_s3_endpoint: Option<&str>,
    ) -> anyhow::Result<()> {
        let name = job_name(&adapter.name);
        // A deterministic name (not the per-run `name` above, which has a
        // millis suffix): the SecretProviderClass is provisioned once and
        // reused across every run of this adapter, not recreated per Job.
        let secret_provider_class_name = if adapter.secrets.is_empty() {
            None
        } else {
            let spc_name = format!("canoe-adapter-{}-secrets", adapter.name.replace('_', "-"));
            crate::secrets::ensure_secret_provider_class(&self.client, &self.namespace, &spc_name, &adapter.secrets).await?;
            Some(spc_name)
        };
        let job = self.build_manifest(
            adapter,
            bootstrap_servers,
            metrics_url,
            log_base_path,
            pod_s3_endpoint,
            &name,
            secret_provider_class_name.as_deref(),
        )?;
        let api: Api<Job> = Api::namespaced(self.client.clone(), &self.namespace);
        api.create(&PostParams::default(), &job).await?;
        Ok(())
    }

    fn build_manifest(
        &self,
        adapter: &AdapterConfig,
        bootstrap_servers: &str,
        metrics_url: &str,
        log_base_path: Option<&str>,
        pod_s3_endpoint: Option<&str>,
        name: &str,
        secret_provider_class_name: Option<&str>,
    ) -> anyhow::Result<Job> {
        let mut labels = BTreeMap::new();
        labels.insert("app".to_string(), "canoe-adapter".to_string());
        labels.insert("canoe.io/adapter".to_string(), adapter.name.clone());

        let mut env = vec![EnvVar {
            name: "CANOE_ADAPTER_NAME".to_string(),
            value: Some(adapter.name.clone()),
            ..Default::default()
        }];
        // Exactly one of `topic`/`output_path` is set — see
        // `AdapterConfig::topic`'s docs — so exactly one of these two env
        // var groups is emitted. Names/value formatting for the batch
        // group match `pod_manager.rs`'s step build_manifest exactly (no
        // `CANOE_CHECKPOINT_PATH`, unlike a step: an adapter has no
        // `input_path` of its own to track consumption progress against —
        // it produces from an external system on a schedule, not by
        // consuming another canoe node's output).
        if let Some(topic) = &adapter.topic {
            env.push(EnvVar {
                name: "CANOE_OUTPUT_TOPIC".to_string(),
                value: Some(topic.clone()),
                ..Default::default()
            });
        }
        if let Some(output_path) = &adapter.output_path {
            let output = adapter.output.clone().unwrap_or_default();
            env.push(EnvVar {
                name: "CANOE_OUTPUT_PATH".to_string(),
                value: Some(output_path.clone()),
                ..Default::default()
            });
            env.push(EnvVar {
                name: "CANOE_OUTPUT_BATCH_SIZE".to_string(),
                value: Some(output.batch_size.to_string()),
                ..Default::default()
            });
            env.push(EnvVar {
                name: "CANOE_OUTPUT_FLUSH_INTERVAL_SECS".to_string(),
                value: Some(output.flush_interval_secs.to_string()),
                ..Default::default()
            });
            env.push(EnvVar {
                name: "CANOE_OUTPUT_PARTITION_BY".to_string(),
                value: Some(output.partition_by.join(",")),
                ..Default::default()
            });
        }
        env.extend(vec![
            EnvVar {
                name: "CANOE_KAFKA_BOOTSTRAP".to_string(),
                value: Some(bootstrap_servers.to_string()),
                ..Default::default()
            },
            EnvVar {
                name: "CANOE_CODE_SOURCE".to_string(),
                value: Some(serde_json::to_string(&adapter.code)?),
                ..Default::default()
            },
            EnvVar {
                name: "CANOE_METRICS_URL".to_string(),
                value: Some(metrics_url.to_string()),
                ..Default::default()
            },
        ]);
        if let Some(log_base_path) = log_base_path {
            env.push(EnvVar {
                name: "CANOE_LOG_BASE_PATH".to_string(),
                value: Some(log_base_path.to_string()),
                ..Default::default()
            });
        }
        env.extend(crate::s3_env::s3_passthrough_env(pod_s3_endpoint));
        env.extend(crate::encrypt_env::encrypt_key_passthrough_env());
        env.extend(crate::encrypt_env::encrypt_fields_env_var(&adapter.encrypt));

        let resources = ResourceRequirements {
            requests: Some(BTreeMap::from([
                ("cpu".to_string(), Quantity(adapter.resources.cpu_request.clone())),
                ("memory".to_string(), Quantity(adapter.resources.mem_request.clone())),
            ])),
            limits: Some(BTreeMap::from([
                ("cpu".to_string(), Quantity(adapter.resources.cpu_limit.clone())),
                ("memory".to_string(), Quantity(adapter.resources.mem_limit.clone())),
            ])),
            ..Default::default()
        };

        // Only present when `adapter.secrets` is non-empty — see
        // `crate::secrets`'s docs.
        let mut volumes: Vec<Volume> = Vec::new();
        let mut volume_mounts: Vec<VolumeMount> = Vec::new();
        let mut service_account_name = None;
        if let Some(spc_name) = secret_provider_class_name {
            volumes.push(crate::secrets::secrets_volume(spc_name));
            volume_mounts.push(crate::secrets::secrets_volume_mount());
            env.push(crate::secrets::secrets_path_env_var());
            service_account_name = Some(crate::secrets::SECRETS_SERVICE_ACCOUNT.to_string());
        }

        let container = Container {
            name: "adapter".to_string(),
            image: Some(self.runner_image.clone()),
            env: Some(env),
            resources: Some(resources),
            volume_mounts: if volume_mounts.is_empty() { None } else { Some(volume_mounts) },
            ..Default::default()
        };

        Ok(Job {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(self.namespace.clone()),
                labels: Some(labels.clone()),
                ..Default::default()
            },
            spec: Some(JobSpec {
                backoff_limit: Some(adapter.backoff_limit as i32),
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
        })
    }
}

/// Whether a task's one-shot Job has finished yet — `crates/canoe-coordinator/src/tasks.rs`
/// polls this for a task that `depends_on` another task, since that's a
/// real completion dependency (unlike depending on a step/pipeline, which
/// only waits for a Deployment to be applied, never "finishes").
pub enum JobStatus {
    Running,
    Succeeded,
    Failed,
}

impl AdapterJobRunner {
    /// Creates one Job for a `task` block's single run — mechanically
    /// identical to `run_once`'s adapter Job (same image, same `uv run
    /// <script>`), just with `CANOE_TASK_NAME` instead of
    /// `CANOE_ADAPTER_NAME` and no `CANOE_OUTPUT_TOPIC` (a task has no
    /// topic of its own). Returns the created Job's name so the caller
    /// can poll `job_status` for task-depends-on-task completion.
    pub async fn run_task_once(
        &self,
        task: &TaskConfig,
        bootstrap_servers: &str,
        metrics_url: &str,
        log_base_path: Option<&str>,
        pod_s3_endpoint: Option<&str>,
    ) -> anyhow::Result<String> {
        let name = task_job_name(&task.name);
        let secret_provider_class_name = if task.secrets.is_empty() {
            None
        } else {
            let spc_name = format!("canoe-task-{}-secrets", task.name.replace('_', "-"));
            crate::secrets::ensure_secret_provider_class(&self.client, &self.namespace, &spc_name, &task.secrets).await?;
            Some(spc_name)
        };
        let job = self.build_task_manifest(
            task,
            bootstrap_servers,
            metrics_url,
            log_base_path,
            pod_s3_endpoint,
            &name,
            secret_provider_class_name.as_deref(),
        )?;
        let api: Api<Job> = Api::namespaced(self.client.clone(), &self.namespace);
        api.create(&PostParams::default(), &job).await?;
        Ok(name)
    }

    /// Polls a task Job's status by name (as returned from `run_task_once`).
    pub async fn job_status(&self, job_name: &str) -> anyhow::Result<JobStatus> {
        job_status(&self.client, &self.namespace, job_name).await
    }

    /// See `delete_job`'s docs.
    pub async fn delete_job(&self, job_name: &str) -> anyhow::Result<()> {
        delete_job(&self.client, &self.namespace, job_name).await
    }

    fn build_task_manifest(
        &self,
        task: &TaskConfig,
        bootstrap_servers: &str,
        metrics_url: &str,
        log_base_path: Option<&str>,
        pod_s3_endpoint: Option<&str>,
        name: &str,
        secret_provider_class_name: Option<&str>,
    ) -> anyhow::Result<Job> {
        let mut labels = BTreeMap::new();
        labels.insert("app".to_string(), "canoe-task".to_string());
        labels.insert("canoe.io/task".to_string(), task.name.clone());

        let mut env = vec![
            EnvVar {
                name: "CANOE_TASK_NAME".to_string(),
                value: Some(task.name.clone()),
                ..Default::default()
            },
            EnvVar {
                name: "CANOE_KAFKA_BOOTSTRAP".to_string(),
                value: Some(bootstrap_servers.to_string()),
                ..Default::default()
            },
            EnvVar {
                name: "CANOE_CODE_SOURCE".to_string(),
                value: Some(serde_json::to_string(&task.code)?),
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

        let resources = ResourceRequirements {
            requests: Some(BTreeMap::from([
                ("cpu".to_string(), Quantity(task.resources.cpu_request.clone())),
                ("memory".to_string(), Quantity(task.resources.mem_request.clone())),
            ])),
            limits: Some(BTreeMap::from([
                ("cpu".to_string(), Quantity(task.resources.cpu_limit.clone())),
                ("memory".to_string(), Quantity(task.resources.mem_limit.clone())),
            ])),
            ..Default::default()
        };

        // Only present when `task.secrets` is non-empty — see
        // `crate::secrets`'s docs.
        let mut volumes: Vec<Volume> = Vec::new();
        let mut volume_mounts: Vec<VolumeMount> = Vec::new();
        let mut service_account_name = None;
        if let Some(spc_name) = secret_provider_class_name {
            volumes.push(crate::secrets::secrets_volume(spc_name));
            volume_mounts.push(crate::secrets::secrets_volume_mount());
            env.push(crate::secrets::secrets_path_env_var());
            service_account_name = Some(crate::secrets::SECRETS_SERVICE_ACCOUNT.to_string());
        }

        let container = Container {
            name: "task".to_string(),
            image: Some(self.runner_image.clone()),
            env: Some(env),
            resources: Some(resources),
            volume_mounts: if volume_mounts.is_empty() { None } else { Some(volume_mounts) },
            ..Default::default()
        };

        Ok(Job {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(self.namespace.clone()),
                labels: Some(labels.clone()),
                ..Default::default()
            },
            spec: Some(JobSpec {
                backoff_limit: Some(task.backoff_limit as i32),
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
        })
    }
}

/// K8s object names must be <= 253 chars and DNS-label-safe; a Unix
/// millis suffix keeps every run's Job name unique without needing
/// generateName (which would leave the coordinator unable to name the
/// object it just asked for in its own logs).
fn job_name(adapter_name: &str) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    format!("canoe-adapter-{}-{millis}", adapter_name.replace('_', "-"))
}

/// Free-function form of `AdapterJobRunner::job_status`, so
/// `crate::step_job`'s batch-mode step Jobs can poll completion the same
/// way task Jobs already do, without needing to construct an
/// `AdapterJobRunner` (which carries a `runner_image` irrelevant to a
/// status check) just to call a method on it.
pub async fn job_status(client: &Client, namespace: &str, job_name: &str) -> anyhow::Result<JobStatus> {
    let api: Api<Job> = Api::namespaced(client.clone(), namespace);
    let job = api.get(job_name).await?;
    let status = job.status.unwrap_or_default();
    if status.succeeded.unwrap_or(0) > 0 {
        Ok(JobStatus::Succeeded)
    } else if status.failed.unwrap_or(0) > 0 {
        Ok(JobStatus::Failed)
    } else {
        Ok(JobStatus::Running)
    }
}

/// Deletes an in-flight Job by name — used by the new cancel handlers
/// (`crates/canoe-coordinator/src/activation.rs`/`tasks.rs`) to actually
/// stop a running batch-mode step/task execution rather than just
/// setting the pause flag and waiting for it to finish on its own.
/// Nothing in this codebase deletes a Job anywhere else today — only
/// `ttlSecondsAfterFinished`-based cleanup after a Job already finished.
///
/// Foreground propagation: the Job's pod is deleted too (not orphaned to
/// run to completion/be garbage-collected later), and this call doesn't
/// return until the deletion is at least accepted — `DeletePropagation::Foreground`
/// makes the API server block the Job object's own removal on its
/// dependents' removal first, the same "actually gone, not just
/// unlinked" semantics `kubectl delete job --cascade=foreground` uses.
/// A `NotFound` (already gone — finished and TTL-reaped, or a race with
/// another delete) is treated as success, not an error: the caller's
/// goal ("this Job is not running anymore") is already satisfied.
pub async fn delete_job(client: &Client, namespace: &str, job_name: &str) -> anyhow::Result<()> {
    let api: Api<Job> = Api::namespaced(client.clone(), namespace);
    match api.delete(job_name, &foreground_delete_params()).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(e)) if e.code == 404 => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// The `DeleteParams` every cancel-driven delete in this crate uses
/// (`delete_job` here, `canoe_spark::delete_launcher`) — pulled out as
/// its own pure function specifically so "this actually requests
/// foreground propagation" is unit-testable without a real `kube::Client`
/// (no mocking precedent anywhere in this codebase — this is the same
/// "test the constructed params/manifest instead" fallback
/// `step_job.rs`'s existing tests already use).
pub fn foreground_delete_params() -> DeleteParams {
    DeleteParams { propagation_policy: Some(PropagationPolicy::Foreground), ..Default::default() }
}

fn task_job_name(task_name: &str) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    format!("canoe-task-{}-{millis}", task_name.replace('_', "-"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn foreground_delete_params_requests_foreground_propagation() {
        let dp = foreground_delete_params();
        assert_eq!(dp.propagation_policy, Some(PropagationPolicy::Foreground));
    }
}
