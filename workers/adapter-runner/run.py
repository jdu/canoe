"""Entrypoint for every adapter's *and* every task's one-shot K8s Job —
the two are mechanically identical (same image, same `uv run <script>`
Job shape), just triggered differently: an adapter on a schedule
(crates/canoe-coordinator/src/scheduler.rs), a task once its
`depends_on` names are all satisfied (crates/canoe-coordinator/src/tasks.rs).
This wrapper tells them apart by which identity env var is set —
CANOE_ADAPTER_NAME or CANOE_TASK_NAME — and uses that to name its
metrics/logs accordingly (`canoe_adapter_*`/scope "adapter" vs.
`canoe_task_*`/scope "task").

Resolves the script per CANOE_CODE_SOURCE (same CodeSource JSON schema as
a Python step's CANOE_CODE_SOURCE — see canoe_worker/loader.py, which this
mirrors) and runs it as `uv run <file>`: uv reads the script's own
`# /// script` PEP 723 header and resolves an ephemeral venv for its
declared dependencies before executing it, so this entrypoint itself only
needs the standard library.

CANOE_KAFKA_BOOTSTRAP, CANOE_ADAPTER_NAME/CANOE_TASK_NAME (plus anything
else the Job's pod spec sets, e.g. a source-system API key from a mounted
Secret) reach the script unchanged via the inherited environment. An
adapter also gets exactly one of two env var groups depending on its
`mode` (see `canoe_config::AdapterConfig::topic`'s docs): `mode =
"streaming"` (default) gets CANOE_OUTPUT_TOPIC, and the script produces to
Kafka itself; `mode = "batch"` gets CANOE_OUTPUT_PATH/
CANOE_OUTPUT_BATCH_SIZE/CANOE_OUTPUT_FLUSH_INTERVAL_SECS/
CANOE_OUTPUT_PARTITION_BY (same names/formatting a batch-mode step's Job
gets), and the script writes to S3 by calling `canoe_adapter_output`'s
`write_batch` — baked into this image at /opt/canoe/, imported via
`sys.path.insert(0, "/opt/canoe")` (see that module's docstring). Either
way, producing output is the script's own responsibility, same "escape
hatch" ownership as a step's `python { code = ... }` block.

This wrapper (not the adapter/task script itself) automatically pushes a
few baseline metrics to CANOE_METRICS_URL after every run — success/failure
and duration are true regardless of what the script does, so there's no
reason to make every author push them by hand. As *gauges*, not counters:
each Job run is a fresh process with no memory of previous runs, so a
pushed value only ever means "this is what the last run looked like," not
an accumulating total — see crates/canoe-coordinator/src/metrics.rs's
`Store` docs for why a counter would misbehave here. A script that wants
its own richer metrics can push more, following the same wire protocol
(README.md's "Metrics" section).
"""
from __future__ import annotations

import json
import os
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
    """Buffers this run's captured stdout/stderr and periodically
    overwrites a single S3 object with everything captured so far, plus a
    final flush after the run completes. See README.md's "Log
    collection". No-ops entirely if `base_path` is falsy."""

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
        except Exception as e:  # noqa: BLE001 - shipping logs must never crash the run
            print(f"failed to ship logs to s3://{self._bucket}/{self._key}: {e}", file=sys.stderr)


def push_metrics(url: str | None, kind: str, name: str, success: bool, duration_seconds: float) -> None:
    if not url:
        return
    labels = {kind: name}
    metrics = [
        {"name": f"canoe_{kind}_last_run_success", "type": "gauge", "value": 1.0 if success else 0.0, "labels": labels},
        {"name": f"canoe_{kind}_last_run_duration_seconds", "type": "gauge", "value": duration_seconds, "labels": labels},
        {"name": f"canoe_{kind}_last_run_timestamp_seconds", "type": "gauge", "value": time.time(), "labels": labels},
    ]
    payload = json.dumps({"metrics": metrics}).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
    try:
        urllib.request.urlopen(req, timeout=5).close()
    except (urllib.error.URLError, OSError) as e:
        print(f"failed to push metrics to {url}: {e}", file=sys.stderr)


def env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"missing required env var {name}", file=sys.stderr)
        sys.exit(1)
    return value


def identity() -> tuple[str, str]:
    """Returns `(kind, name)` — `("adapter", <CANOE_ADAPTER_NAME>)` or
    `("task", <CANOE_TASK_NAME>)`, whichever env var the coordinator set
    for this Job. Exactly one must be present."""
    adapter_name = os.environ.get("CANOE_ADAPTER_NAME")
    task_name = os.environ.get("CANOE_TASK_NAME")
    if adapter_name and task_name:
        print("both CANOE_ADAPTER_NAME and CANOE_TASK_NAME are set — this Job's pod spec is misconfigured", file=sys.stderr)
        sys.exit(1)
    if adapter_name:
        return "adapter", adapter_name
    if task_name:
        return "task", task_name
    print("missing required env var: one of CANOE_ADAPTER_NAME or CANOE_TASK_NAME", file=sys.stderr)
    sys.exit(1)


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


def main() -> None:
    kind, name = identity()
    metrics_url = os.environ.get("CANOE_METRICS_URL")
    code_source = json.loads(env("CANOE_CODE_SOURCE"))
    source = fetch_source(code_source)

    log_shipper = LogShipper(os.environ.get("CANOE_LOG_BASE_PATH"), scope=kind, task=name)
    last_log_flush = time.monotonic()

    fd, path = tempfile.mkstemp(suffix=".py", prefix=f"canoe_{kind}_{name}_")
    started = time.monotonic()
    try:
        with os.fdopen(fd, "w") as f:
            f.write(source)
        # Piped (not inherited) so this wrapper can both forward the
        # script's output to its own stdout (kubectl logs stays useful)
        # and ship a copy to S3 — capture_output=True would only do the
        # former after the fact, with no chance for a periodic flush.
        proc = subprocess.Popen(
            ["uv", "run", "--no-project", path],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            line = line.rstrip("\n")
            print(line)
            log_shipper.write(line)
            now = time.monotonic()
            if now - last_log_flush >= LOG_FLUSH_INTERVAL_SECS:
                log_shipper.flush()
                last_log_flush = now
        returncode = proc.wait()
    finally:
        os.unlink(path)
        log_shipper.flush()

    push_metrics(metrics_url, kind, name, success=returncode == 0, duration_seconds=time.monotonic() - started)
    sys.exit(returncode)


if __name__ == "__main__":
    main()
