// Throwaway generator, not part of the crate's public surface: produces
// one fixed cross-language test fixture (a real Rust-encrypted envelope)
// for crates/canoe-sink/tests/crypto.rs and
// workers/python-worker/tests/test_crypto_cross_lang.py to both hardcode
// and verify against. Run once with `cargo run -p canoe-sink --example
// gen_cross_lang_fixture` whenever the fixture needs regenerating (e.g.
// the wire format changes) — the committed constant is what actually
// matters at test time, not a re-run of this script.
fn main() {
    let key = vec![9u8; 32];
    let value = serde_json::json!({"email": "[email protected]", "amount": 42});
    let envelope = canoe_sink::encrypt_field(&value, "customer_record", "cross_lang_test_key", &key).unwrap();
    println!("{}", envelope.as_str().unwrap());
}
