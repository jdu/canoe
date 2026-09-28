"""Unit tests for canoe_worker.crypto. Run with:

    python -m unittest discover -s workers/python-worker/tests
"""
import base64
import os
import unittest
from unittest.mock import patch

from canoe_worker import crypto


class EnvVarNameForKeyTests(unittest.TestCase):
    def test_uppercases_and_prefixes(self):
        self.assertEqual(crypto.env_var_name_for_key("pii_v1"), "CANOE_ENCRYPT_KEY_PII_V1")

    def test_collapses_non_alphanumeric_characters(self):
        self.assertEqual(crypto.env_var_name_for_key("pii-v1.x"), "CANOE_ENCRYPT_KEY_PII_V1_X")


class EncryptDecryptRoundTripTests(unittest.TestCase):
    def setUp(self):
        self.key = bytes(range(32))

    def test_round_trips_a_string_value(self):
        envelope = crypto.encrypt_field("[email protected]", "email", "pii_v1", self.key)
        self.assertTrue(envelope.startswith(crypto.ENVELOPE_PREFIX))
        self.assertEqual(crypto.decrypt_field(envelope, "email", self.key), "[email protected]")

    def test_round_trips_a_non_string_value_and_preserves_type(self):
        envelope = crypto.encrypt_field(42, "amount", "pii_v1", self.key)
        value = crypto.decrypt_field(envelope, "amount", self.key)
        self.assertEqual(value, 42)
        self.assertIsInstance(value, int)

    def test_two_encryptions_of_the_same_value_differ(self):
        # Random nonce per call — why an encrypted field must never be a
        # checkpoint/SCD dedup key (see EncryptConfig's docs).
        a = crypto.encrypt_field("x", "f", "k", self.key)
        b = crypto.encrypt_field("x", "f", "k", self.key)
        self.assertNotEqual(a, b)

    def test_decrypt_fails_with_the_wrong_field_name_aad(self):
        envelope = crypto.encrypt_field("secret", "ssn", "pii_v1", self.key)
        with self.assertRaises(Exception):
            crypto.decrypt_field(envelope, "not_ssn", self.key)

    def test_encrypt_rejects_a_key_that_is_not_32_bytes(self):
        with self.assertRaises(ValueError):
            crypto.encrypt_field("x", "f", "k", b"tooshort")


class ResolveKeyBytesTests(unittest.TestCase):
    def test_reads_and_decodes_the_env_var(self):
        raw = bytes(range(32))
        with patch.dict(os.environ, {"CANOE_ENCRYPT_KEY_PII_V1": base64.b64encode(raw).decode()}):
            self.assertEqual(crypto.resolve_key_bytes("pii_v1"), raw)

    def test_raises_when_the_env_var_is_unset(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(crypto.MissingKeyError):
                crypto.resolve_key_bytes("pii_v1")


class EncryptRowTests(unittest.TestCase):
    def test_encrypts_only_declared_fields(self):
        raw = bytes(range(32))
        with patch.dict(os.environ, {"CANOE_ENCRYPT_KEY_PII_V1": base64.b64encode(raw).decode()}):
            row = {"ssn": "123-45-6789", "amount": 9}
            out = crypto.encrypt_row(row, [{"fields": ["ssn"], "key": "pii_v1"}])
            self.assertTrue(crypto.is_envelope(out["ssn"]))
            self.assertEqual(out["amount"], 9)

    def test_missing_key_raises_rather_than_writing_plaintext(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(crypto.MissingKeyError):
                crypto.encrypt_row({"ssn": "x"}, [{"fields": ["ssn"], "key": "pii_v1"}])

    def test_empty_blocks_is_a_no_op(self):
        row = {"ssn": "123-45-6789"}
        self.assertEqual(crypto.encrypt_row(row, []), row)


class DecryptRowTests(unittest.TestCase):
    def test_decrypts_an_encrypted_field_in_place(self):
        raw = bytes(range(32))
        with patch.dict(os.environ, {"CANOE_ENCRYPT_KEY_PII_V1": base64.b64encode(raw).decode()}):
            envelope = crypto.encrypt_field("[email protected]", "email", "pii_v1", raw)
            out = crypto.decrypt_row({"email": envelope, "amount": 5})
            self.assertEqual(out["email"], "[email protected]")
            self.assertEqual(out["amount"], 5)

    def test_missing_key_leaves_ciphertext_and_does_not_raise(self):
        raw = bytes(range(32))
        envelope = crypto.encrypt_field("[email protected]", "email", "pii_v1", raw)
        with patch.dict(os.environ, {}, clear=True):
            out = crypto.decrypt_row({"email": envelope})  # must not raise
            self.assertEqual(out["email"], envelope)

    def test_non_envelope_values_pass_through_untouched(self):
        row = {"a": 1, "b": "plain string", "c": None}
        self.assertEqual(crypto.decrypt_row(row), row)


if __name__ == "__main__":
    unittest.main()
