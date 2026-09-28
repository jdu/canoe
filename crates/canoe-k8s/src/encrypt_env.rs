//! Forwards `CANOE_ENCRYPT_KEY_*` env vars from the coordinator's own
//! process environment onto every pod/Job it creates — the field
//! encryption counterpart to `s3_env.rs`'s S3-credential passthrough, but
//! **prefix-matched** rather than a fixed allowlist: unlike the S3
//! credential var names (fixed, known ahead of time), a `CANOE_ENCRYPT_KEY_<NAME>`
//! var name depends on whatever key names pipeline authors actually
//! declared in `encrypt { key = "..." }` blocks across however many DSL
//! files this coordinator is running, which this crate has no static
//! knowledge of. See `canoe_config::EncryptConfig`'s docs for the wire
//! format and `crates/canoe-sink/src/crypto.rs::env_var_name_for_key` for
//! exactly how a key name maps to its env var name (kept in sync with
//! `canoe_worker.crypto.env_var_name_for_key` on the Python side too).
//!
//! Every key forwarded verbatim, unlike `s3_env.rs`'s `AWS_ENDPOINT`
//! special case — a key's raw bytes are the same wherever they're read
//! from, there's no host-vs-in-cluster address distinction for a secret.

use canoe_config::EncryptConfig;
use k8s_openapi::api::core::v1::EnvVar;

const PREFIX: &str = "CANOE_ENCRYPT_KEY_";

fn encrypt_key_passthrough_env_from(vars: impl IntoIterator<Item = (String, String)>) -> Vec<EnvVar> {
    let mut env: Vec<EnvVar> = vars
        .into_iter()
        .filter(|(name, _)| name.starts_with(PREFIX))
        .map(|(name, value)| EnvVar { name, value: Some(value), ..Default::default() })
        .collect();
    env.sort_by(|a, b| a.name.cmp(&b.name));
    env
}

/// Every `CANOE_ENCRYPT_KEY_*` var currently set in this process's own
/// environment, as pod/container env vars — empty when no `encrypt`
/// block anywhere in this coordinator's config is actually in use.
pub(crate) fn encrypt_key_passthrough_env() -> Vec<EnvVar> {
    encrypt_key_passthrough_env_from(std::env::vars())
}

/// A single `CANOE_ENCRYPT_FIELDS` env var carrying a node's own
/// `encrypt` blocks as a JSON array (`[{"fields": [...], "key":
/// "..."}]`) — `None` (no env var at all) when `encrypt` is empty, same
/// "only set what's actually used" convention `pod_manager.rs`'s
/// `lookup_env_vars` follows. The reader (a worker, or an adapter
/// script's own explicit call) resolves key material itself via the
/// `CANOE_ENCRYPT_KEY_<NAME>` vars `encrypt_key_passthrough_env` already
/// forwards — canoe only hands over *which* fields and under which key
/// name. Shared by `pod_manager.rs` (python/uv steps) and
/// `adapter_job.rs` (adapters — exposure only, not automatic; see
/// `canoe_config::AdapterConfig::encrypt`'s docs).
pub(crate) fn encrypt_fields_env_var(encrypt: &[EncryptConfig]) -> Option<EnvVar> {
    if encrypt.is_empty() {
        return None;
    }
    Some(EnvVar {
        name: "CANOE_ENCRYPT_FIELDS".to_string(),
        value: Some(serde_json::to_string(encrypt).expect("EncryptConfig always serializes")),
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forwards_only_canoe_encrypt_key_prefixed_vars() {
        let vars = vec![
            ("CANOE_ENCRYPT_KEY_PII_V1".to_string(), "base64stuff".to_string()),
            ("CANOE_ENCRYPT_KEY_OTHER".to_string(), "morebase64".to_string()),
            ("AWS_ACCESS_KEY_ID".to_string(), "minioadmin".to_string()),
            ("PATH".to_string(), "/usr/bin".to_string()),
        ];
        let env = encrypt_key_passthrough_env_from(vars);
        let names: Vec<&str> = env.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["CANOE_ENCRYPT_KEY_OTHER", "CANOE_ENCRYPT_KEY_PII_V1"]);
    }

    #[test]
    fn empty_when_nothing_matches() {
        let vars = vec![("PATH".to_string(), "/usr/bin".to_string())];
        assert!(encrypt_key_passthrough_env_from(vars).is_empty());
    }

    #[test]
    fn output_is_sorted_by_name_for_deterministic_pod_specs() {
        let vars = vec![
            ("CANOE_ENCRYPT_KEY_ZEBRA".to_string(), "z".to_string()),
            ("CANOE_ENCRYPT_KEY_ALPHA".to_string(), "a".to_string()),
        ];
        let env = encrypt_key_passthrough_env_from(vars);
        let names: Vec<&str> = env.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["CANOE_ENCRYPT_KEY_ALPHA", "CANOE_ENCRYPT_KEY_ZEBRA"]);
    }
}
