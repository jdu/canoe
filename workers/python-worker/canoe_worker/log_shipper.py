"""Ships this task's own logs to the global S3 log location
(`CANOE_LOG_BASE_PATH`) — see README.md's "Log collection" section for the
folder layout and why this exists.

Stdlib + boto3 only (boto3 is already a hard dependency of this image, same
as `mapping.py`'s S3 fetch path). `workers/adapter-runner/run.py` and
`workers/uv-step-worker/run.py` carry their own copies of the small parts
of this (ULID generation, S3 PUT) rather than importing it, mirroring
`metrics.py`'s docs on why — those two are standalone scripts without
`canoe_worker` installed.

No-ops entirely if `CANOE_LOG_BASE_PATH` isn't set, so running a step
locally without one configured doesn't need it.
"""
from __future__ import annotations

import logging
import os
import sys
import time

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def generate_ulid() -> str:
    """A real ULID: 48-bit millisecond timestamp + 80 bits of randomness,
    Crockford base32 encoded — lexically sortable by creation time, same
    as every other "naturally orderable run identifier" this codebase uses
    (see the `ulid` crate on the Rust side)."""
    ms = int(time.time() * 1000)
    rand = int.from_bytes(os.urandom(10), "big")
    value = (ms << 80) | rand
    chars = []
    for _ in range(26):
        chars.append(_CROCKFORD[value & 0x1F])
        value >>= 5
    return "".join(reversed(chars))


class LogShipper:
    """Buffers formatted log lines in memory for this task's whole
    lifetime and periodically (see `main.py`'s flush loop) overwrites a
    single S3 object — `<base_path>/<scope>/<task>/<year>/<month>/
    <run-date>_<ulid>.log` — with everything captured so far, plus a final
    flush on shutdown. One object per task run, generated once at
    construction; `scope` is the pipeline (chain) name for a step, or the
    literal string "adapter" for an adapter run.

    Deliberately simple: this re-uploads the whole buffer on every flush
    rather than appending (S3 has no native append), so it doesn't suit an
    extremely long-lived task with a huge log volume — see README.md's
    "Status / what's stubbed" entry on this.
    """

    def __init__(self, base_path: str | None, scope: str, task: str):
        self._enabled = bool(base_path)
        self._lines: list[str] = []
        if not self._enabled:
            return

        import boto3

        assert base_path is not None
        without_scheme = base_path[len("s3://") :] if base_path.startswith("s3://") else base_path
        bucket, _, prefix = without_scheme.partition("/")
        self._bucket = bucket
        prefix = prefix.strip("/")

        now = time.gmtime()
        run_id = generate_ulid()
        date_str = time.strftime("%Y-%m-%d", now)
        parts = [p for p in (prefix, scope, task, f"{now.tm_year:04d}", f"{now.tm_mon:02d}") if p]
        self._key = "/".join(parts) + f"/{date_str}_{run_id}.log"
        self._s3 = boto3.client("s3")

    def write(self, line: str) -> None:
        if self._enabled:
            self._lines.append(line)

    def flush(self) -> None:
        if not self._enabled or not self._lines:
            return
        body = ("\n".join(self._lines) + "\n").encode("utf-8")
        try:
            self._s3.put_object(Bucket=self._bucket, Key=self._key, Body=body)
        except Exception as e:  # noqa: BLE001 - shipping logs must never crash the task itself
            print(f"failed to ship logs to s3://{self._bucket}/{self._key}: {e}", file=sys.stderr)


class ShippingLogHandler(logging.Handler):
    """A `logging.Handler` that forwards every formatted record into a
    `LogShipper` — attach to the root logger so every module's logging
    (canoe_worker's own, and anything the user's step function does with
    `logging.getLogger(...)`) is captured, not just canoe_worker's."""

    def __init__(self, shipper: LogShipper):
        super().__init__()
        self._shipper = shipper
        self.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._shipper.write(self.format(record))
        except Exception:  # noqa: BLE001 - a broken handler must not break logging itself
            pass
