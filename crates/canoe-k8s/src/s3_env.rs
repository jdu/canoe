//! Forwards static S3-compatible credentials from the coordinator's own
//! process environment onto every pod/Job it creates.
//!
//! In EKS, every pod/Job just carries ambient IRSA credentials, so
//! `pod_manager.rs`/`adapter_job.rs` have never needed to set any AWS/S3
//! env vars themselves — the coordinator's own in-process S3 calls
//! (`S3BacklogInspector`, `crate::pod_manager` doesn't do S3 I/O itself,
//! but `canoe-sink`'s `Checkpointer`/`run_writer` do, in-process inside
//! the coordinator for sinks) already pick up the standard AWS env vars
//! via `object_store`'s `AmazonS3Builder::from_env()`. Outside IRSA —
//! notably, a local MinIO-backed dev cluster, but equally any real S3
//! deployment authenticating with static keys instead of IRSA — a worker
//! pod/Job has no way to inherit those same credentials, since it's a
//! separate container with its own empty environment.
//!
//! This forwards a fixed allowlist, read from the coordinator's own
//! environment at pod-build time, onto every pod/Job — only the ones
//! actually set are included, so this is a no-op (adds nothing) in the
//! IRSA case. Exact var names confirmed against the vendored
//! `object_store` 0.11.2 source (`aws/builder.rs`'s `from_env` docs):
//! `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT` (not
//! `AWS_ENDPOINT_URL` — this version's `from_env` only documents
//! `AWS_ENDPOINT`, though the config-key parser also accepts
//! `AWS_ENDPOINT_URL` as an alias), `AWS_REGION`, `AWS_ALLOW_HTTP`.
//!
//! `AWS_ENDPOINT` is the one exception to "forward the coordinator's own
//! value verbatim": under a topology where the coordinator runs
//! somewhere pods can't reach by the same address (e.g. a host-run
//! coordinator against a local kind cluster — see README.md's "Local dev
//! stack"), the coordinator's own S3 endpoint (host-reachable) and the
//! endpoint pods need (in-cluster-reachable) legitimately differ. The
//! caller (`crate::pod_manager`/`crate::adapter_job`, ultimately
//! `canoe-coordinator`'s `--pod-s3-endpoint` CLI flag) passes an explicit
//! override for that one field when the two need to diverge; every other
//! key is the same for the coordinator and every pod it creates, so
//! those still come straight from the coordinator's own environment.

use k8s_openapi::api::core::v1::EnvVar;

const OTHER_KEYS: [&str; 4] = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION", "AWS_ALLOW_HTTP"];

/// Reads `OTHER_KEYS` via `lookup`, producing one `EnvVar` per key that's
/// actually set, plus `AWS_ENDPOINT` — from `endpoint_override` if given,
/// else falling back to `lookup("AWS_ENDPOINT")` (the coordinator's own
/// value, correct whenever the coordinator and its pods share a
/// network). Takes a lookup function rather than reading `std::env`
/// directly so it's testable without mutating real process environment
/// (which every `#[test]` in the crate shares).
fn s3_passthrough_env_from(endpoint_override: Option<&str>, lookup: impl Fn(&str) -> Option<String>) -> Vec<EnvVar> {
    let mut env: Vec<EnvVar> = OTHER_KEYS
        .iter()
        .filter_map(|key| lookup(key).map(|value| EnvVar { name: key.to_string(), value: Some(value), ..Default::default() }))
        .collect();
    let endpoint = endpoint_override.map(str::to_string).or_else(|| lookup("AWS_ENDPOINT"));
    if let Some(endpoint) = endpoint {
        env.push(EnvVar { name: "AWS_ENDPOINT".to_string(), value: Some(endpoint), ..Default::default() });
    }
    env
}

/// The coordinator's own static S3 credentials/endpoint override, if any
/// are set in its process environment, as pod/container env vars.
/// `pod_endpoint_override`, when `Some`, replaces `AWS_ENDPOINT` for the
/// pod's own copy — see the module docs.
pub(crate) fn s3_passthrough_env(pod_endpoint_override: Option<&str>) -> Vec<EnvVar> {
    s3_passthrough_env_from(pod_endpoint_override, |key| std::env::var(key).ok())
}

/// Same forwarding as `s3_passthrough_env`, as plain `(name, value)` pairs
/// instead of K8s `EnvVar` structs — for `canoe-spark`'s `spark_submit_args`,
/// which sets driver-pod env vars via `--conf
/// spark.kubernetes.driverEnv.<KEY>=<VALUE>` rather than authoring a pod
/// spec directly (cluster-mode spark-submit creates the driver pod
/// itself, so there's no `PodSpec.env` for canoe-spark to append to the
/// way `pod_manager.rs`/`adapter_job.rs` do). Public (unlike
/// `s3_passthrough_env`) since this crosses the crate boundary into
/// `canoe-spark` — see that crate's `spark_submit_args` docs for why this
/// is only ever called for a step that resolved an event-log path
/// (scoped specifically to what event logging needs, not a general fix
/// for the documented "Spark steps don't get S3 credential passthrough"
/// gap).
pub fn s3_passthrough_pairs(pod_endpoint_override: Option<&str>) -> Vec<(String, String)> {
    s3_passthrough_env_from(pod_endpoint_override, |key| std::env::var(key).ok())
        .into_iter()
        .filter_map(|e| e.value.map(|v| (e.name, v)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn forwards_only_keys_that_are_set() {
        let mut set = HashMap::new();
        set.insert("AWS_ACCESS_KEY_ID".to_string(), "minioadmin".to_string());
        set.insert("AWS_ENDPOINT".to_string(), "http://localhost:9000".to_string());

        let env = s3_passthrough_env_from(None, |key| set.get(key).cloned());

        let names: Vec<&str> = env.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["AWS_ACCESS_KEY_ID", "AWS_ENDPOINT"]);
        assert_eq!(env[0].value.as_deref(), Some("minioadmin"));
        assert_eq!(env[1].value.as_deref(), Some("http://localhost:9000"));
    }

    #[test]
    fn empty_when_nothing_set_and_no_override() {
        assert!(s3_passthrough_env_from(None, |_| None).is_empty());
    }

    #[test]
    fn endpoint_override_replaces_the_coordinators_own_endpoint() {
        let mut set = HashMap::new();
        set.insert("AWS_ACCESS_KEY_ID".to_string(), "minioadmin".to_string());
        set.insert("AWS_ENDPOINT".to_string(), "http://localhost:9000".to_string());

        let env = s3_passthrough_env_from(Some("http://minio.canoe-system.svc.cluster.local:9000"), |key| {
            set.get(key).cloned()
        });

        let endpoint = env.iter().find(|e| e.name == "AWS_ENDPOINT").expect("AWS_ENDPOINT present");
        assert_eq!(endpoint.value.as_deref(), Some("http://minio.canoe-system.svc.cluster.local:9000"));
    }

    #[test]
    fn endpoint_override_is_used_even_when_the_coordinators_own_endpoint_is_unset() {
        let env = s3_passthrough_env_from(Some("http://minio.canoe-system.svc.cluster.local:9000"), |_| None);
        let endpoint = env.iter().find(|e| e.name == "AWS_ENDPOINT").expect("AWS_ENDPOINT present");
        assert_eq!(endpoint.value.as_deref(), Some("http://minio.canoe-system.svc.cluster.local:9000"));
    }
}
