"""Cross-language wire-format test — the Python-side counterpart of
`crates/canoe-sink/tests/crypto.rs`. `CROSS_LANG_ENVELOPE` is a real
envelope produced once by the actual Rust encrypt path
(`crates/canoe-sink/examples/gen_cross_lang_fixture.rs`, fixed test key
and value); this test decrypts that exact constant with
`canoe_worker.crypto` and confirms it recovers the original value —
proving the two implementations agree on nonce placement, AAD binding,
and base64 framing. If this test ever fails after a wire-format change on
either side, regenerate the fixture on the Rust side and update both this
file and `crates/canoe-sink/tests/crypto.rs` together.

Run with:

    python -m unittest discover -s workers/python-worker/tests
"""
import unittest

from canoe_worker import crypto

CROSS_LANG_KEY = bytes([9] * 32)
CROSS_LANG_ENVELOPE = (
    "canoe:enc:v1:cross_lang_test_key:"
    "lv+QSupxlXcx5pTyk3oceL+VR07SW+3Yf1Vw6zV6aLKX5E+mTdhDHbqedDq1vWLssRDtpuoYbIjoZEch3iaZvMFMYScR"
)
CROSS_LANG_AAD_FIELD_NAME = "customer_record"
CROSS_LANG_EXPECTED_VALUE = {"email": "[email protected]", "amount": 42}


class CrossLanguageRoundTripTests(unittest.TestCase):
    def test_python_decrypts_a_rust_encrypted_envelope(self):
        value = crypto.decrypt_field(CROSS_LANG_ENVELOPE, CROSS_LANG_AAD_FIELD_NAME, CROSS_LANG_KEY)
        self.assertEqual(value, CROSS_LANG_EXPECTED_VALUE)

    def test_wrong_aad_field_name_fails_to_decrypt(self):
        # Confirms AAD is actually load-bearing (bound to the field name),
        # not just accepted and ignored.
        with self.assertRaises(Exception):
            crypto.decrypt_field(CROSS_LANG_ENVELOPE, "wrong_field_name", CROSS_LANG_KEY)

    def test_is_envelope_recognizes_the_rust_produced_string(self):
        self.assertTrue(crypto.is_envelope(CROSS_LANG_ENVELOPE))


if __name__ == "__main__":
    unittest.main()
