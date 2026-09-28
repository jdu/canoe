"""Minimal dependency-free ULID generator (https://github.com/ulid/spec):
48-bit millisecond timestamp + 80 bits of randomness, Crockford base32
encoded — lexically sortable by generation time, same property
`crates/canoe-sink` relies on (the Rust side uses the `ulid` crate; this
is a stdlib-only equivalent so the worker image doesn't need a new
dependency just for this one id).
"""
from __future__ import annotations

import os
import time

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def new() -> str:
    millis = int(time.time() * 1000)
    randomness = int.from_bytes(os.urandom(10), "big")
    value = (millis << 80) | randomness
    chars = []
    for _ in range(26):
        chars.append(_CROCKFORD[value & 0x1F])
        value >>= 5
    return "".join(reversed(chars))
