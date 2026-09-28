"""Entrypoint for every `engine = "uv"` step's Deployment pod.

Resolves the step's script per CANOE_CODE_SOURCE (same CodeSource JSON
schema as a Python step's CANOE_CODE_SOURCE, or an adapter's — see
canoe_worker/loader.py and workers/adapter-runner/run.py, which this
mirrors) and runs it with `uv run <file>`: uv reads the script's own
`# /// script` PEP 723 header and resolves an ephemeral venv for its
declared dependencies before executing it, so this entrypoint itself only
needs the standard library (+ boto3, for log shipping and the s3
CodeSource case).

A `uv` step's script owns its whole loop (this wrapper never reads or sets
CANOE_INPUT_*/CANOE_OUTPUT_*/CANOE_CHECKPOINT_PATH itself), so which
transport contract it's expected to speak depends on what kind of step
this pod was created for: a regular `engine = "uv"` step gets the S3 +
signal env vars (CANOE_INPUT_PATH, CANOE_INPUT_SIGNAL, CANOE_OUTPUT_PATH,
CANOE_CHECKPOINT_PATH, CANOE_OUTPUT_BATCH_SIZE,
CANOE_OUTPUT_FLUSH_INTERVAL_SECS, CANOE_OUTPUT_PARTITION_BY — see
README.md's "S3 + signal-driven pipeline transport" and
examples/orders-uv-step.canoe.hcl's `dedupe` step for a worked sketch of
reading them); `dynamic_route`'s own per-discovered-value pods still get
the older CANOE_INPUT_TOPIC/CANOE_OUTPUT_TOPIC/CANOE_CONSUMER_GROUP
Kafka-topic contract (see `canoe_config::KafkaPythonStep`'s docs) — either
way, CANOE_KAFKA_BOOTSTRAP is always set, since a script normally still
needs it (to consume signals, or to consume/produce a topic directly).

Previously this used os.execvp (process replacement) rather than waiting
on a subprocess, so that `uv run`'s child became PID 1 directly and K8s's
SIGTERM/restart-on-exit applied to the script with no wrapper in between.
Shipping this pod's logs to S3 (CANOE_LOG_BASE_PATH — see README.md's
"Log collection") needs to read the script's stdout/stderr as it runs,
which exec'ing away doesn't allow, so this now runs the script as a real
child process instead: it forwards SIGTERM to the child itself (see
`_forward_signals`) to keep shutdown behavior equivalent, at the cost of
one extra process in the container's tree.

Since the child never exits (the script is the whole program, expected to
loop forever), this wrapper still can't push a "finished successfully" metric
after the fact. It pushes a "started" gauge just before running the
script; the script itself owns any further metrics about what it
actually does with the stream, via the same wire protocol
(CANOE_METRICS_URL, README.md's "Metrics" section) canoe_worker's own
Python steps use.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

LOG_FLUSH_INTERVAL_SECS = 15
_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def generate_ulid() -> str:
    """Same Crockford-base32 ULID as canoe_worker.log_shipper — duplicated
    here rather than imported since this is a standalone stdlib(+boto3)
    script, not an installed package. See that module's docstring."""
    ms = int(time.time() * 1000)
    rand = int.from_bytes(os.urandom(10), "big")
    value = (ms << 80) | rand
    chars = []
    for _ in range(26):
        chars.append(_CROCKFORD[value & 0x1F])
        value >>= 5
    return "".join(reversed(chars))


class LogShipper:
    """Same buffer-and-periodically-overwrite S3 log shipper as
    workers/adapter-runner/run.py's copy — duplicated for the same
    "standalone script, no shared package" reason. No-ops entirely if
    `base_path` is falsy."""

    def __init__(self, base_path: str | None, scope: str, task: str):
        self._enabled = bool(base_path)
        self._lines: list[str] = []
        if not self._enabled:
            return
        import boto3

        without_scheme = base_path[len("s3://") :] if base_path.startswith("s3://") else base_path  # type: ignore[union-attr]
        bucket, _, prefix = without_scheme.partition("/")
        self._bucket = bucket
        prefix = prefix.strip("/")
        now = time.gmtime()
        date_str = time.strftime("%Y-%m-%d", now)
        parts = [p for p in (prefix, scope, task, f"{now.tm_year:04d}", f"{now.tm_mon:02d}") if p]
        self._key = "/".join(parts) + f"/{date_str}_{generate_ulid()}.log"
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
        except Exception as e:  # noqa: BLE001 - shipping logs must never crash the step
            print(f"failed to ship logs to s3://{self._bucket}/{self._key}: {e}", file=sys.stderr)


def env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"missing required env var {name}", file=sys.stderr)
        sys.exit(1)
    return value


def fetch_source(code_source: dict) -> str:
    kind = code_source["kind"]
    if kind == "inline":
        return code_source["source"]
    if kind == "s3":
        import boto3

        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=code_source["bucket"], Key=code_source["key"])
        return obj["Body"].read().decode("utf-8")
    if kind == "git":
        with tempfile.TemporaryDirectory() as tmp:
            subprocess.run(
                ["git", "clone", "--depth", "1", "--branch", code_source["ref"], code_source["repo"], tmp],
                check=True,
                capture_output=True,
            )
            with open(os.path.join(tmp, code_source["path"])) as f:
                return f.read()
    raise ValueError(f"unknown code source kind: {kind}")


def push_started_metric(url: str | None, chain_name: str, step_name: str) -> None:
    if not url:
        return
    labels = {"chain": chain_name, "step": step_name}
    metrics = [{"name": "canoe_uv_step_started_timestamp_seconds", "type": "gauge", "value": time.time(), "labels": labels}]
    payload = json.dumps({"metrics": metrics}).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
    try:
        urllib.request.urlopen(req, timeout=5).close()
    except (urllib.error.URLError, OSError) as e:
        print(f"failed to push metrics to {url}: {e}", file=sys.stderr)


def _forward_signals(proc: subprocess.Popen) -> None:
    """Passes SIGTERM/SIGINT straight through to the child — the
    replacement for what os.execvp used to get for free by making the
    child PID 1 itself."""

    def handler(signum, _frame):
        proc.send_signal(signum)

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)


def main() -> None:
    code_source = json.loads(env("CANOE_CODE_SOURCE"))
    source = fetch_source(code_source)
    chain_name = os.environ.get("CANOE_CHAIN_NAME", "")
    step_name = os.environ.get("CANOE_STEP_NAME", "")

    push_started_metric(os.environ.get("CANOE_METRICS_URL"), chain_name, step_name)

    log_shipper = LogShipper(os.environ.get("CANOE_LOG_BASE_PATH"), scope=chain_name, task=step_name)

    fd, path = tempfile.mkstemp(suffix=".py", prefix="canoe_uv_step_")
    with os.fdopen(fd, "w") as f:
        f.write(source)

    proc = subprocess.Popen(
        ["uv", "run", "--no-project", path],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    _forward_signals(proc)

    last_log_flush = time.monotonic()
    assert proc.stdout is not None
    try:
        for line in proc.stdout:
            line = line.rstrip("\n")
            print(line)
            log_shipper.write(line)
            now = time.monotonic()
            if now - last_log_flush >= LOG_FLUSH_INTERVAL_SECS:
                log_shipper.flush()
                last_log_flush = now
    finally:
        returncode = proc.wait()
        log_shipper.flush()
        os.unlink(path)

    sys.exit(returncode)


if __name__ == "__main__":
    main()
