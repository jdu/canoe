"""Helper module a `mode = "batch"` adapter's script calls to write its
output to S3 — baked into the adapter-runner image at `/opt/canoe/`, not
the script's own `uv run`-resolved venv (a genuinely separate process; see
`run.py`'s docstring). The script imports it explicitly:

    import sys
    sys.path.insert(0, "/opt/canoe")
    import canoe_adapter_output as output

    output.write_batch(rows)

This is a deliberate "script calls a provided helper" shape, not `run.py`
transparently capturing rows — an adapter's script owns its entire loop
(same reason `uv` steps work this way), so canoe never intercepts what it
produces; it only supplies the write logic as an importable function.

Mirrors `python-worker/canoe_worker/output_writer.py` +
`checkpoint.py`'s `list_run_ids`/`signal_publisher.py` combined into one
file — per-image duplication, same precedent as `crypto.py`/
`canoe_output_writer.py` in `spark-runner`.
"""
from __future__ import annotations

import io
import json
import os
import time
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

SIGNALS_TOPIC = "_canoe.signals"


def _new_ulid() -> str:
    """Same Crockford-base32 ULID as `canoe_worker.ulid`/`run.py`'s own
    `generate_ulid` — duplicated here for the same reason: a standalone
    stdlib(+boto3/pyarrow) module, not an installed package."""
    millis = int(time.time() * 1000)
    randomness = int.from_bytes(os.urandom(10), "big")
    value = (millis << 80) | randomness
    chars = []
    for _ in range(26):
        chars.append(_CROCKFORD[value & 0x1F])
        value >>= 5
    return "".join(reversed(chars))


def _parse_uri(uri: str) -> tuple[str, str, str]:
    parsed = urlparse(uri)
    if parsed.scheme == "s3":
        return "s3", parsed.netloc, parsed.path.lstrip("/")
    if parsed.scheme == "file":
        return "file", "", parsed.path
    return "file", "", uri


def _put_bytes(uri: str, data: bytes) -> None:
    scheme, bucket, key = _parse_uri(uri)
    if scheme == "file":
        path = Path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return
    import boto3

    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=data)


def _put_json(uri: str, value: dict) -> None:
    _put_bytes(uri, json.dumps(value).encode("utf-8"))


def _infer_avsc(rows: list[dict], record_name: str) -> dict:
    """Best-effort schema from the first occurrence of each field's Python
    type — same approach and same fidelity caveat as
    `output_writer._infer_avsc`'s docs."""
    fields = []
    seen = set()
    for row in rows:
        for key, value in row.items():
            if key in seen:
                continue
            seen.add(key)
            fields.append({"name": key, "type": _avro_type(value)})
    return {"type": "record", "name": record_name, "fields": fields}


def _avro_type(value) -> list | str:
    if isinstance(value, bool):
        avro_t = "boolean"
    elif isinstance(value, int):
        avro_t = "long"
    elif isinstance(value, float):
        avro_t = "double"
    else:
        avro_t = "string"
    return ["null", avro_t]


def _partition_key(row: dict, partition_by: list[str]) -> str:
    if not partition_by:
        return ""
    return "/".join(f"{col}={row.get(col, '_null')}" for col in partition_by)


def _write_run(output_path: str, rows: list[dict], record_name: str, partition_by: list[str]) -> tuple[str, int]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    run_id = _new_ulid()
    run_path = f"{output_path.rstrip('/')}/run_id={run_id}"

    buckets: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        buckets[_partition_key(row, partition_by)].append(row)

    for partition, partition_rows in buckets.items():
        table = pa.Table.from_pylist(partition_rows)
        buf = io.BytesIO()
        pq.write_table(table, buf)
        part_path = f"{run_path}/{partition}/part-0.parquet" if partition else f"{run_path}/part-0.parquet"
        _put_bytes(part_path, buf.getvalue())

    avsc = _infer_avsc(rows, record_name)
    _put_json(f"{run_path}/schema.avsc", avsc)

    latest = {
        "run_id": run_id,
        "path": f"{run_path}/",
        "written_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "row_count": len(rows),
    }
    _put_json(f"{output_path.rstrip('/')}/latest.json", latest)
    return run_id, len(rows)


def _publish_signal(bootstrap_servers: str, signal: str, path: str, run_id: str, row_count: int) -> None:
    """Best-effort, same as `signal_publisher.publish` — a failed publish
    is logged, not raised; a downstream consumer's own checkpoint-based
    backlog listing (`checkpoint.list_new_run_ids`) is what actually
    guarantees a run is never missed, not this signal."""
    from confluent_kafka import Producer

    event = {
        "signal": signal,
        "chain": signal,
        "sink": signal,
        "emitted_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "path": path,
        "run_id": run_id,
        "row_count": row_count,
    }
    try:
        producer = Producer({"bootstrap.servers": bootstrap_servers})
        producer.produce(SIGNALS_TOPIC, json.dumps(event).encode("utf-8"), key=signal.encode("utf-8"))
        producer.flush(timeout=10)
    except Exception as e:  # noqa: BLE001 - see docstring
        print(f"failed to publish signal {signal}: {e}")


def write_batch(rows: list[dict]) -> None:
    """Writes `rows` to this adapter's S3 output (`CANOE_OUTPUT_PATH`),
    chunked into at most `CANOE_OUTPUT_BATCH_SIZE` rows per run (mirrors a
    batch-mode step's own flush-size bound), Hive-partitioned by
    `CANOE_OUTPUT_PARTITION_BY` if set — one Parquet run + an updated
    `latest.json` pointer per chunk, publishing this adapter's own
    wake-up signal (keyed by `CANOE_ADAPTER_NAME`, matching
    `compile_graph`'s `name_to_signal` seeding for batch adapters) after
    each chunk. A no-op if `rows` is empty — the script can call this
    once per run with everything it fetched, without special-casing "got
    nothing this time" itself.
    """
    if not rows:
        return

    output_path = os.environ["CANOE_OUTPUT_PATH"]
    adapter_name = os.environ["CANOE_ADAPTER_NAME"]
    batch_size = int(os.environ.get("CANOE_OUTPUT_BATCH_SIZE", "10000"))
    partition_by = [p for p in os.environ.get("CANOE_OUTPUT_PARTITION_BY", "").split(",") if p]
    bootstrap_servers = os.environ.get("CANOE_KAFKA_BOOTSTRAP")

    for start in range(0, len(rows), batch_size):
        chunk = rows[start : start + batch_size]
        run_id, row_count = _write_run(output_path, chunk, adapter_name, partition_by)
        if bootstrap_servers:
            _publish_signal(
                bootstrap_servers, adapter_name, f"{output_path.rstrip('/')}/run_id={run_id}/", run_id, row_count
            )
