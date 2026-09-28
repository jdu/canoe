"""Spark-side equivalent of `crates/canoe-sink/src/run_writer.rs` /
`workers/python-worker/canoe_worker/output_writer.py` (writing),
`checkpoint.py` (checkpoint tracking), and `signal_publisher.py`
(completion signal) — combined into one module here rather than split
three ways, since batch-mode Spark's driver is the only caller of any of
this. Per-image duplication of the small JSON/ULID/Kafka-publish helpers,
same precedent `canoe_spark_runner.py`'s own `_get_json_object`/
`_encrypt_value` already established (no shared package between the
python-worker and spark-runner images).

Used only by `canoe_spark_runner.py`'s batch-mode path (`CANOE_TRANSPORT`
=`batch`) — the streaming path (`CANOE_TRANSPORT` unset, or `streaming`)
writes to a Kafka topic via `writeStream`, untouched, and never imports
this module.
"""
from __future__ import annotations

import json
import os
import time
from urllib.parse import urlparse

from pyspark.sql import DataFrame

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def _new_ulid() -> str:
    """Same algorithm as `canoe_worker.ulid.new`/the Rust `ulid` crate —
    48-bit millisecond timestamp + 80 bits of randomness, Crockford
    base32 — lexically sortable by generation time."""
    millis = int(time.time() * 1000)
    randomness = int.from_bytes(os.urandom(10), "big")
    value = (millis << 80) | randomness
    chars = []
    for _ in range(26):
        chars.append(_CROCKFORD[value & 0x1F])
        value >>= 5
    return "".join(reversed(chars))


def _put_json(uri: str, doc: dict) -> None:
    """Same `file://`-or-`s3://` dispatch as `canoe_spark_runner._get_json_object`,
    the write-side counterpart."""
    parsed = urlparse(uri)
    if parsed.scheme == "file" or not parsed.scheme:
        path = parsed.path if parsed.scheme == "file" else uri
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with open(path, "w") as f:
            json.dump(doc, f)
        return

    import boto3

    boto3.client("s3").put_object(
        Bucket=parsed.netloc, Key=parsed.path.lstrip("/"), Body=json.dumps(doc).encode("utf-8")
    )


def _get_json(uri: str) -> dict | None:
    parsed = urlparse(uri)
    if parsed.scheme == "file" or not parsed.scheme:
        path = parsed.path if parsed.scheme == "file" else uri
        if not os.path.isfile(path):
            return None
        with open(path) as f:
            return json.load(f)

    import boto3
    import botocore.exceptions

    try:
        obj = boto3.client("s3").get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))
        return json.loads(obj["Body"].read())
    except botocore.exceptions.ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return None
        raise


_SPARK_TO_AVRO = {
    "boolean": "boolean",
    "byte": "long",
    "short": "long",
    "integer": "long",
    "long": "long",
    "float": "double",
    "double": "double",
    "string": "string",
}


def _infer_avsc(df: DataFrame, record_name: str) -> dict:
    """Best-effort schema from `df`'s own declared Spark types — mirrors
    `canoe_worker.output_writer._infer_avsc`'s spirit (good enough for
    the debugging/comparison purpose `schema.avsc` serves, not a strict
    fidelity match with the Rust side's Arrow-derived `avro.rs`); any
    Spark type this doesn't recognize (structs, arrays, maps, timestamps,
    ...) falls back to `"string"` rather than failing the write.
    """
    fields = []
    for f in df.schema.fields:
        avro_t = _SPARK_TO_AVRO.get(f.dataType.typeName(), "string")
        fields.append({"name": f.name, "type": ["null", avro_t] if f.nullable else avro_t})
    return {"type": "record", "name": record_name, "fields": fields}


def write_run(
    df: DataFrame,
    output_path: str,
    record_name: str,
    partition_by: list[str] | None = None,
) -> tuple[str, int]:
    """Writes `df` as one new run under `output_path`
    (`<output_path>/run_id=<ulid>/`, Hive-partitioned by `partition_by`
    via Spark's own `DataFrameWriter.partitionBy` — the same directory
    convention every other writer in this codebase produces), a
    `schema.avsc` inside that run directory, and an updated `latest.json`
    pointer at `output_path`'s root. Returns `(run_id, row_count)`.

    Any `encrypt` block this step declared must already be applied to
    `df` by the caller (via `_encrypt_columns`) *before* this is called —
    same "encrypt before schema inference and before writing" ordering
    every other writer in this codebase uses; this function doesn't know
    about `CANOE_ENCRYPT_FIELDS` at all.
    """
    partition_by = partition_by or []
    run_id = _new_ulid()
    run_path = f"{output_path.rstrip('/')}/run_id={run_id}"

    row_count = df.count()
    writer = df.write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(run_path)

    _put_json(f"{run_path}/schema.avsc", _infer_avsc(df, record_name))

    latest = {
        "run_id": run_id,
        "path": f"{run_path}/",
        "written_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "row_count": row_count,
    }
    _put_json(f"{output_path.rstrip('/')}/latest.json", latest)

    return run_id, row_count


def read_through_run_id(checkpoint_path: str) -> str | None:
    """Mirrors `canoe_worker.checkpoint.read_through_run_id` — how far
    into `CANOE_INPUT_PATH`'s own run history this step has already
    processed. `None` means nothing's been processed yet (a fresh step,
    or its checkpoint file doesn't exist yet), matching
    `list_new_run_ids`'s "everything" behavior for that case.
    """
    doc = _get_json(checkpoint_path)
    if doc is None:
        return None
    return doc.get("through_run_id")


def list_run_ids(dataset_path: str) -> list[str]:
    """Lists `run_id=*` "directories" directly under `dataset_path`,
    sorted oldest-first (ULIDs sort lexically by generation time) —
    mirrors `canoe_worker.object_store.list_run_ids`/
    `crates/canoe-sink/src/run_writer.rs::list_run_ids` exactly (same
    per-runtime duplication precedent as everything else in this file).
    """
    parsed = urlparse(dataset_path)
    run_ids: list[str] = []
    if parsed.scheme == "file" or not parsed.scheme:
        base = parsed.path if parsed.scheme == "file" else dataset_path
        if not os.path.isdir(base):
            return []
        for name in os.listdir(base):
            if name.startswith("run_id=") and os.path.isdir(os.path.join(base, name)):
                run_ids.append(name[len("run_id="):])
        run_ids.sort()
        return run_ids

    import boto3

    client = boto3.client("s3")
    prefix = parsed.path.lstrip("/").rstrip("/") + "/"
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=parsed.netloc, Prefix=prefix, Delimiter="/"):
        for common_prefix in page.get("CommonPrefixes", []):
            name = common_prefix["Prefix"][len(prefix):].rstrip("/")
            if name.startswith("run_id="):
                run_ids.append(name[len("run_id="):])
    run_ids.sort()
    return run_ids


def list_new_run_ids(dataset_path: str, through_run_id: str | None) -> list[str]:
    """Every `run_id=*` under `dataset_path` newer than `through_run_id`,
    oldest first — `None` means "everything," matching a fresh step with
    no prior checkpoint. Mirrors `canoe_worker.checkpoint.list_new_run_ids`
    exactly, so batch-mode Spark drains backlog with the identical
    guarantee Python's batch mode already provides (every unprocessed run
    since the last checkpoint, not just whatever's currently latest).
    """
    all_run_ids = list_run_ids(dataset_path)
    if through_run_id is None:
        return all_run_ids
    return [run_id for run_id in all_run_ids if run_id > through_run_id]


def write_checkpoint(checkpoint_path: str, through_run_id: str) -> None:
    """Advances this step's own checkpoint past the input run it just
    processed — same `{"through_run_id": ...}` shape
    `canoe_worker.checkpoint.write_through_run_id` writes, read by the
    coordinator's `S3BacklogInspector` (`crates/canoe-k8s/src/s3_backlog.rs`)
    to decide whether this step has any unprocessed backlog left. Without
    this, the coordinator's reconcile loop would see the same backlog
    forever and refire a new batch Job every tick even though this run
    already processed it.
    """
    _put_json(checkpoint_path, {"through_run_id": through_run_id})


def publish_signal(bootstrap_servers: str, signal: str, chain: str, step: str, path: str, run_id: str, row_count: int) -> None:
    """Same wake-up signal every other writer in this codebase fires
    after a flush — mirrors `canoe_worker.signal_publisher.publish`
    exactly (same topic, same event shape). Purely a fast-wake latency
    optimization for whatever's downstream (its own reconcile loop
    already polls S3 backlog independently) — best-effort, a failed
    publish is logged, not raised.
    """
    import logging

    from confluent_kafka import Producer

    log = logging.getLogger("canoe_spark_runner")
    event = {
        "signal": signal,
        "chain": chain,
        "sink": step,
        "emitted_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "path": path,
        "run_id": run_id,
        "row_count": row_count,
    }
    try:
        producer = Producer({"bootstrap.servers": bootstrap_servers})
        producer.produce("_canoe.signals", json.dumps(event).encode("utf-8"), key=signal.encode("utf-8"))
        producer.flush(timeout=10)
    except Exception:
        log.exception("failed to publish signal %s", signal)
