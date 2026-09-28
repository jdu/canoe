//! Application-level field encryption: AES-256-GCM over a single JSON
//! value, producing a self-describing ciphertext string — see
//! `canoe_config::EncryptConfig`'s docs for the full rationale (why
//! self-describing beats schema.avsc metadata here) and the field's
//! safety caveat (never encrypt a checkpoint/SCD key field).
//!
//! Wire format, same convention HashiCorp Vault's transit engine uses
//! (`vault:v1:...`):
//!
//! ```text
//! canoe:enc:v1:<key_name>:<base64(nonce(12 bytes) || ciphertext+tag)>
//! ```
//!
//! - AAD is the field name's UTF-8 bytes — binds a ciphertext to the
//!   column it came from, so a value can't be silently swapped between
//!   two encrypted columns even if both use the same key.
//! - Plaintext is the *JSON encoding* of the original `serde_json::Value`
//!   (`serde_json::to_vec`), not a raw string — so decrypt recovers the
//!   original type (`json.loads` on the Python side) rather than always
//!   producing a string.
//!
//! **Rust only implements encrypt, deliberately no decrypt** — the
//! coordinator process never needs plaintext of a field some upstream
//! step already encrypted; that field passes through as an opaque
//! ciphertext string, unchanged, through `Checkpointer`/`ScdView`/
//! `ParquetSink` alike. Only step/Spark code (Python/PySpark, which the
//! user's own `run()`/SQL touches) needs decrypt — see
//! `canoe_worker/crypto.py`, which must stay byte-for-byte compatible
//! with this module (see the cross-language round-trip test in
//! `crates/canoe-sink/tests/`).

use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng, Payload};
use aes_gcm::{Aes256Gcm, Key};
use anyhow::{bail, Context};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use canoe_config::EncryptConfig;
use serde_json::Value;

pub const ENVELOPE_PREFIX: &str = "canoe:enc:v1:";

/// The env var a key named `key_name` resolves from: `CANOE_ENCRYPT_KEY_`
/// + the key name uppercased, with every character that isn't
/// `[A-Z0-9_]` replaced by `_` (env var names can't hold arbitrary
/// characters). **Must stay byte-for-byte identical to
/// `canoe_worker.crypto.env_var_name_for_key`** — both sides need to
/// compute the same name for the same key, or a value encrypted by one
/// runtime silently can't find its key when decrypted (or re-encrypted)
/// by the other.
pub fn env_var_name_for_key(key_name: &str) -> String {
    let sanitized: String = key_name
        .to_uppercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' })
        .collect();
    format!("CANOE_ENCRYPT_KEY_{sanitized}")
}

/// Reads and base64-decodes `CANOE_ENCRYPT_KEY_<NAME>` from this
/// process's own environment.
pub fn resolve_key_bytes(key_name: &str) -> anyhow::Result<Vec<u8>> {
    let var = env_var_name_for_key(key_name);
    let encoded = std::env::var(&var)
        .with_context(|| format!("encryption key \"{key_name}\" requires env var {var}, which is not set"))?;
    BASE64.decode(encoded.trim()).with_context(|| format!("env var {var} is not valid base64"))
}

/// Applies every declared `encrypt` block to one JSON row (expected to be
/// a JSON object — a non-object row with a declared `encrypt` field is
/// left untouched, since there's no field to encrypt). Resolves each
/// block's key from this process's own environment via
/// `resolve_key_bytes` — a missing key here is a hard error (unlike the
/// read-side/decrypt path in `canoe_worker.crypto`, where a missing key
/// is expected and handled gracefully): a producer that declared
/// `encrypt` but can't actually encrypt should fail loudly, not silently
/// write plaintext.
pub fn encrypt_row(row: &Value, encrypt: &[EncryptConfig]) -> anyhow::Result<Value> {
    let Value::Object(obj) = row else {
        return Ok(row.clone());
    };
    let mut obj = obj.clone();
    for block in encrypt {
        let key = resolve_key_bytes(&block.key)?;
        for field in &block.fields {
            if let Some(value) = obj.get(field) {
                let encrypted = encrypt_field(value, field, &block.key, &key)?;
                obj.insert(field.clone(), encrypted);
            }
        }
    }
    Ok(Value::Object(obj))
}

/// Applies `encrypt_row` to every row in `rows`. Returns the original
/// slice unmodified (no clone) when `encrypt` is empty — the common case.
pub fn encrypt_rows<'a>(rows: &'a [Value], encrypt: &[EncryptConfig]) -> anyhow::Result<std::borrow::Cow<'a, [Value]>> {
    if encrypt.is_empty() {
        return Ok(std::borrow::Cow::Borrowed(rows));
    }
    let encrypted: Vec<Value> = rows.iter().map(|r| encrypt_row(r, encrypt)).collect::<anyhow::Result<_>>()?;
    Ok(std::borrow::Cow::Owned(encrypted))
}

/// Encrypts `value` for `field_name` under `key` (must be exactly 32
/// bytes — a base64-decoded `CANOE_ENCRYPT_KEY_<NAME>` env var). Returns
/// the envelope string, ready to be stored in place of the original
/// field value.
pub fn encrypt_field(value: &Value, field_name: &str, key_name: &str, key: &[u8]) -> anyhow::Result<Value> {
    if key.len() != 32 {
        bail!("encryption key \"{key_name}\" must decode to exactly 32 bytes (AES-256), got {}", key.len());
    }
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let plaintext = serde_json::to_vec(value).context("failed to JSON-encode value for encryption")?;
    let ciphertext = cipher
        .encrypt(&nonce, Payload { msg: &plaintext, aad: field_name.as_bytes() })
        .map_err(|e| anyhow::anyhow!("AES-GCM encryption failed: {e}"))?;

    let mut wire = Vec::with_capacity(nonce.len() + ciphertext.len());
    wire.extend_from_slice(&nonce);
    wire.extend_from_slice(&ciphertext);

    Ok(Value::String(format!("{ENVELOPE_PREFIX}{key_name}:{}", BASE64.encode(wire))))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> Vec<u8> {
        vec![7u8; 32]
    }

    #[test]
    fn encrypted_value_has_the_expected_envelope_shape() {
        let value = Value::String("[email protected]".to_string());
        let encrypted = encrypt_field(&value, "email", "pii_v1", &test_key()).unwrap();
        let Value::String(s) = &encrypted else { panic!("expected a string envelope") };

        assert!(s.starts_with(ENVELOPE_PREFIX), "expected prefix {ENVELOPE_PREFIX}, got {s}");
        let rest = s.strip_prefix(ENVELOPE_PREFIX).unwrap();
        let (key_name, b64) = rest.split_once(':').expect("envelope must have a key_name:base64 tail");
        assert_eq!(key_name, "pii_v1");

        let raw = BASE64.decode(b64).expect("payload must be valid base64");
        assert!(raw.len() > 12, "payload should be at least a 12-byte nonce plus ciphertext+tag");
    }

    #[test]
    fn two_encryptions_of_the_same_value_produce_different_ciphertext() {
        // Random nonce per call — exactly why an encrypted field must
        // never be used as a checkpoint/SCD dedup key (see
        // EncryptConfig's docs and the DSL compile-time rejection).
        let value = Value::Number(42.into());
        let a = encrypt_field(&value, "amount", "pii_v1", &test_key()).unwrap();
        let b = encrypt_field(&value, "amount", "pii_v1", &test_key()).unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn rejects_a_key_that_is_not_32_bytes() {
        let value = Value::Bool(true);
        let err = encrypt_field(&value, "flag", "bad_key", &[1, 2, 3]).unwrap_err();
        assert!(err.to_string().contains("32 bytes"), "{err}");
    }
}
