"""Application-level field encryption: AES-256-GCM over a single JSON
value, producing a self-describing ciphertext string — the Python-side
counterpart of `crates/canoe-sink/src/crypto.rs`. See
`canoe_config::EncryptConfig`'s docs for the full rationale (why
self-describing beats schema.avsc metadata here) and the field's safety
caveat (never encrypt a checkpoint/SCD key field).

Wire format, same convention HashiCorp Vault's transit engine uses
(`vault:v1:...`):

    canoe:enc:v1:<key_name>:<base64(nonce(12 bytes) || ciphertext+tag)>

- AAD is the field name's UTF-8 bytes — binds a ciphertext to the column
  it came from, so a value can't be silently swapped between two
  encrypted columns even if both use the same key.
- Plaintext is the JSON encoding of the original value, not a raw string
  — so decrypt recovers the original type (`json.loads`) rather than
  always producing a string. It does *not* need to be byte-identical to
  what `serde_json` would produce on the Rust side — each side only ever
  parses JSON it (or the other runtime) wrote, and any valid JSON decodes
  the same way regardless of whitespace/key-order specifics.

Unlike the Rust module, this one implements **both** directions: workers
read data a Rust-written sink or another Python step may have encrypted
(decrypt), and write their own step's declared output fields (encrypt).
See the module docstring in `crates/canoe-sink/src/crypto.rs` for why
Rust itself never needs decrypt.
"""
from __future__ import annotations

import base64
import json
import logging
import os
import re

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

log = logging.getLogger("canoe_worker.crypto")

ENVELOPE_PREFIX = "canoe:enc:v1:"
_ENVELOPE_RE = re.compile(re.escape(ENVELOPE_PREFIX) + r"([^:]+):(.+)$")

# Field names we've already warned about missing a key for, this process
# lifetime — avoids spamming logs for a hot per-record loop that keeps
# hitting the same missing key. See `decrypt_row`'s docs.
_WARNED_MISSING_KEYS: set[str] = set()


def env_var_name_for_key(key_name: str) -> str:
    """The env var a key named `key_name` resolves from. **Must stay
    byte-for-byte identical to `crates/canoe-sink/src/crypto.rs`'s
    `env_var_name_for_key`** — both sides need to compute the same name
    for the same key, or a value encrypted by one runtime silently can't
    find its key when decrypted (or re-encrypted) by the other.
    """
    sanitized = "".join(c if c.isalnum() or c == "_" else "_" for c in key_name.upper())
    return f"CANOE_ENCRYPT_KEY_{sanitized}"


class MissingKeyError(Exception):
    """Raised by `encrypt_field`/`resolve_key_bytes` when a named key's
    env var isn't set. `decrypt_row` catches this itself (best-effort,
    see its docs) — this only ever propagates out of the *encrypt* path,
    where a producer that declared `encrypt` but can't actually encrypt
    should fail loudly, not silently write plaintext.
    """


def resolve_key_bytes(key_name: str) -> bytes:
    var = env_var_name_for_key(key_name)
    encoded = os.environ.get(var)
    if not encoded:
        raise MissingKeyError(f'encryption key "{key_name}" requires env var {var}, which is not set')
    return base64.b64decode(encoded.strip())


def encrypt_field(value, field_name: str, key_name: str, key: bytes) -> str:
    if len(key) != 32:
        raise ValueError(f'encryption key "{key_name}" must decode to exactly 32 bytes (AES-256), got {len(key)}')
    aesgcm = AESGCM(key)
    nonce = os.urandom(12)
    plaintext = json.dumps(value).encode("utf-8")
    ciphertext = aesgcm.encrypt(nonce, plaintext, field_name.encode("utf-8"))
    wire = base64.b64encode(nonce + ciphertext).decode("ascii")
    return f"{ENVELOPE_PREFIX}{key_name}:{wire}"


def decrypt_field(envelope: str, field_name: str, key: bytes):
    match = _ENVELOPE_RE.match(envelope)
    if not match:
        raise ValueError(f"not a valid {ENVELOPE_PREFIX} envelope: {envelope!r}")
    _key_name, wire_b64 = match.groups()
    raw = base64.b64decode(wire_b64)
    nonce, ciphertext = raw[:12], raw[12:]
    aesgcm = AESGCM(key)
    plaintext = aesgcm.decrypt(nonce, ciphertext, field_name.encode("utf-8"))
    return json.loads(plaintext)


def is_envelope(value) -> bool:
    return isinstance(value, str) and value.startswith(ENVELOPE_PREFIX)


def encrypt_row(row: dict, encrypt_blocks: list[dict]) -> dict:
    """Applies every declared `encrypt` block (`[{"fields": [...], "key":
    "..."}]`, `output_writer.py`'s `CANOE_ENCRYPT_FIELDS`-decoded shape)
    to one row. Resolves each block's key via `resolve_key_bytes` — a
    missing key here is a hard error (`MissingKeyError`), unlike
    `decrypt_row`: a producer that declared `encrypt` but can't actually
    encrypt should fail loudly, not silently write plaintext.
    """
    if not encrypt_blocks:
        return row
    out = dict(row)
    for block in encrypt_blocks:
        key = resolve_key_bytes(block["key"])
        for field in block["fields"]:
            if field in out:
                out[field] = encrypt_field(out[field], field, block["key"], key)
    return out


def decrypt_row(row: dict) -> dict:
    """For every string field value matching the `canoe:enc:v1:` prefix,
    resolves its key locally and decrypts. If that key isn't available in
    this pod's env (`MissingKeyError`), leaves the value as ciphertext and
    logs a warning **once per field name** rather than raising — a step
    that never touches that field shouldn't crash a whole batch just
    because it lacks a key it doesn't need. Malformed/tampered ciphertext
    (bad AAD, wrong key, corrupted bytes) also logs and passes through
    rather than raising, same reasoning.
    """
    out = dict(row)
    for field, value in row.items():
        if not is_envelope(value):
            continue
        match = _ENVELOPE_RE.match(value)
        key_name = match.group(1) if match else "?"
        try:
            key = resolve_key_bytes(key_name)
            out[field] = decrypt_field(value, field, key)
        except MissingKeyError:
            if field not in _WARNED_MISSING_KEYS:
                log.warning('field "%s" is encrypted with key "%s", which is not available in this pod — leaving ciphertext', field, key_name)
                _WARNED_MISSING_KEYS.add(field)
        except Exception:
            log.warning('field "%s" looks encrypted but failed to decrypt (wrong key, or corrupted) — leaving ciphertext', field)
    return out
