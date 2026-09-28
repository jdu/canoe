//! Provisions what a pod/Job needs to mount named AWS Secrets Manager
//! entries as files, via the Secrets Store CSI Driver + AWS provider
//! (ASCP) — see `canoe_config`'s `secrets` field docs on `PythonStep`/
//! `UvStep`/`AdapterConfig`/`TaskConfig`/`SparkSqlStep`/`SparkDataFrameStep`
//! for the DSL surface (`secrets = ["name", ...]`).
//!
//! canoe assumes the CSI driver itself is already installed on the target
//! cluster — same as it already assumes a Fargate profile or Spark-on-K8s
//! exist rather than installing them itself. This module only emits the
//! `SecretProviderClass` object (a CRD, so there's no `k8s-openapi` type
//! for it — built via `kube`'s `DynamicObject`) and the small building
//! blocks (`secrets_volume`/`secrets_volume_mount`/`secrets_path_env_var`)
//! `pod_manager.rs`/`adapter_job.rs` attach to a pod/Job's spec directly.
//! `canoe-spark` wires the equivalent in via spark-submit `--conf` flags
//! instead, since it never authors the actual driver pod spec itself (see
//! its own docs on `spark_submit_args`).

use k8s_openapi::api::core::v1::{EnvVar, Volume, VolumeMount};
use kube::api::{DynamicObject, Patch, PatchParams};
use kube::core::{ApiResource, GroupVersionKind};
use kube::{Api, Client};
use serde_json::json;
use std::collections::BTreeMap;

/// The ServiceAccount every python/uv step pod or adapter/task Job that
/// declares `secrets` runs under. Its IAM role (bound via an IRSA
/// annotation) is provisioned out-of-band — see `deploy/k8s/rbac.yaml`.
/// Spark steps deliberately do **not** switch to this ServiceAccount (see
/// `canoe-spark`'s docs) — a Spark driver already needs `canoe-spark`'s
/// own pod-management permissions, which swapping would lose.
pub const SECRETS_SERVICE_ACCOUNT: &str = "canoe-secrets";

/// Fixed mount path every declared secret shows up under, one file per
/// secret named after it — exposed to a pod/Job as `CANOE_SECRETS_PATH`
/// (`secrets_path_env_var`) so a script never needs to hardcode this.
pub const SECRETS_MOUNT_PATH: &str = "/mnt/secrets-store";

const CSI_VOLUME_NAME: &str = "secrets-store";

fn secret_provider_class_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("secrets-store.csi.x-k8s.io", "v1", "SecretProviderClass"),
        "secretproviderclasses",
    )
}

/// Idempotently ensures a `SecretProviderClass` named `name` exists,
/// listing every entry in `secret_names` as an AWS Secrets Manager
/// object. Server-side apply (same pattern `canoe-spark`'s launcher
/// Deployment already uses) rather than get-then-create, so calling this
/// again on every pod top-up/adapter run is always safe and cheap — a
/// second call with the same `secret_names` is a no-op write.
pub async fn ensure_secret_provider_class(client: &Client, namespace: &str, name: &str, secret_names: &[String]) -> anyhow::Result<()> {
    let resource = secret_provider_class_resource();
    let objects: String = secret_names.iter().map(|n| format!("- objectName: \"{n}\"\n  objectType: \"secretsmanager\"\n")).collect();

    let obj = DynamicObject::new(name, &resource).within(namespace).data(json!({
        "spec": {
            "provider": "aws",
            "parameters": { "objects": objects },
        }
    }));

    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), namespace, &resource);
    let pp = PatchParams::apply("canoe-coordinator").force();
    api.patch(name, &pp, &Patch::Apply(&obj)).await?;
    Ok(())
}

/// The CSI volume a pod/Job's `PodSpec.volumes` needs, referencing the
/// `SecretProviderClass` named `secret_provider_class_name`.
pub fn secrets_volume(secret_provider_class_name: &str) -> Volume {
    Volume {
        name: CSI_VOLUME_NAME.to_string(),
        csi: Some(k8s_openapi::api::core::v1::CSIVolumeSource {
            driver: "secrets-store.csi.k8s.io".to_string(),
            read_only: Some(true),
            volume_attributes: Some(BTreeMap::from([("secretProviderClass".to_string(), secret_provider_class_name.to_string())])),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// The matching `VolumeMount` a pod/Job's container needs, pairing with
/// `secrets_volume`.
pub fn secrets_volume_mount() -> VolumeMount {
    VolumeMount {
        name: CSI_VOLUME_NAME.to_string(),
        mount_path: SECRETS_MOUNT_PATH.to_string(),
        read_only: Some(true),
        ..Default::default()
    }
}

/// `CANOE_SECRETS_PATH=/mnt/secrets-store` — so a step/adapter script
/// never needs to hardcode the mount path.
pub fn secrets_path_env_var() -> EnvVar {
    EnvVar {
        name: "CANOE_SECRETS_PATH".to_string(),
        value: Some(SECRETS_MOUNT_PATH.to_string()),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secrets_volume_references_the_named_secret_provider_class() {
        let v = secrets_volume("canoe-orders-validate-secrets");
        assert_eq!(v.name, "secrets-store");
        let csi = v.csi.expect("csi volume source");
        assert_eq!(csi.driver, "secrets-store.csi.k8s.io");
        assert_eq!(csi.read_only, Some(true));
        assert_eq!(
            csi.volume_attributes.unwrap().get("secretProviderClass").map(String::as_str),
            Some("canoe-orders-validate-secrets")
        );
    }

    #[test]
    fn secrets_volume_mount_matches_the_volume_name_and_fixed_path() {
        let mount = secrets_volume_mount();
        assert_eq!(mount.name, "secrets-store");
        assert_eq!(mount.mount_path, SECRETS_MOUNT_PATH);
        assert_eq!(mount.read_only, Some(true));
    }

    #[test]
    fn secrets_path_env_var_is_the_fixed_mount_path() {
        let env = secrets_path_env_var();
        assert_eq!(env.name, "CANOE_SECRETS_PATH");
        assert_eq!(env.value.as_deref(), Some("/mnt/secrets-store"));
    }
}
