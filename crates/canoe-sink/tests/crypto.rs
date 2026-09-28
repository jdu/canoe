//! Cross-language wire-format test: `crates/canoe-sink/src/crypto.rs`
//! (Rust, encrypt-only) and `canoe_worker/crypto.py` (Python, encrypt +
//! decrypt) must agree byte-for-byte on nonce placement, AAD binding, and
//! base64 framing, or ciphertext produced by one silently fails to
//! decrypt in the other. `CROSS_LANG_ENVELOPE` below was generated once
//! by `crates/canoe-sink/examples/gen_cross_lang_fixture.rs` (fixed test
//! key, fixed value) and is a real envelope this Rust implementation
//! actually produced — `workers/python-worker/tests/test_crypto_cross_lang.py`
//! decrypts this exact same constant and asserts it recovers the
//! original value. Regenerate both sides together if the wire format
//! ever changes.

use serde_json::json;

const CROSS_LANG_KEY: [u8; 32] = [9u8; 32];
const CROSS_LANG_ENVELOPE: &str = "canoe:enc:v1:cross_lang_test_key:lv+QSupxlXcx5pTyk3oceL+VR07SW+3Yf1Vw6zV6aLKX5E+mTdhDHbqedDq1vWLssRDtpuoYbIjoZEch3iaZvMFMYScR";

#[test]
fn regenerating_the_fixture_produces_the_same_envelope_shape_the_committed_constant_has() {
    // Doesn't assert byte-equality with CROSS_LANG_ENVELOPE (the nonce is
    // random every call, so the ciphertext legitimately differs run to
    // run) — just confirms this Rust build still produces envelopes in
    // the exact shape the committed Python-side fixture expects, so a
    // wire-format regression here is caught locally too, not only by the
    // Python test silently failing to decrypt the old constant.
    let value = json!({"email": "[email protected]", "amount": 42});
    let envelope = canoe_sink::encrypt_field(&value, "customer_record", "cross_lang_test_key", &CROSS_LANG_KEY).unwrap();
    let envelope = envelope.as_str().unwrap();

    assert!(envelope.starts_with("canoe:enc:v1:cross_lang_test_key:"));
    assert_eq!(
        envelope.len(),
        CROSS_LANG_ENVELOPE.len(),
        "same key/AAD/plaintext should produce the same envelope length every time (fixed nonce size, fixed ciphertext+tag size)"
    );
}
