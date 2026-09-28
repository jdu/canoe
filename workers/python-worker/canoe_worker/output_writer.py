"""Python-side equivalent of `crates/canoe-sink/src/run_writer.rs`:
buffers rows and, once `CANOE_OUTPUT_BATCH_SIZE`/`CANOE_OUTPUT_FLUSH_INTERVAL_SECS`
says it's time, flushes them as one new run —
`<output_path>/run_id=<ulid>/part-*.parquet` (optionally Hive-partitioned
by `CANOE_OUTPUT_PARTITION_BY`), a `schema.avsc` inside that same run
directory, and an updated `latest.json` pointer at the output root — so a
Python-step worker's own output is byte-for-byte the same shape a Rust
sink's is, comparable the same way.
"""
from __future__ import annotations

import io
import time
from collections import defaultdict

from canoe_worker import crypto, object_store, ulid


def _infer_avsc(rows: list[dict], record_name: str) -> dict:
    """Best-effort schema from the first row's Python types — good enough
    for the debugging/comparison purpose `schema.avsc` serves (see
    README.md's "S3 + signal-driven pipeline transport"); not a strict
    Arrow-schema-fidelity match with the Rust side's `avro.rs`, since a
    Python step's records are plain dicts with no schema of their own to
    begin with.
    """
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
    elif value is None:
        avro_t = "string"
    else:
        avro_t = "string"
    return ["null", avro_t]


def _partition_key(row: dict, partition_by: list[str]) -> str:
    if not partition_by:
        return ""
    return "/".join(f"{col}={row.get(col, '_null')}" for col in partition_by)


def write_run(
    output_path: str,
    rows: list[dict],
    record_name: str,
    partition_by: list[str] | None = None,
    encrypt_blocks: list[dict] | None = None,
) -> str:
    """Writes `rows` as one new run under `output_path`, returns the new
    run_id. Mirrors `run_writer::write_run`'s layout exactly.

    `encrypt_blocks` (this step's own `CANOE_ENCRYPT_FIELDS`-decoded
    config, usually `None`/empty) is applied to every row via
    `crypto.encrypt_row` *before* schema inference and before writing —
    same place in the flow as the Rust side's `write_run`. An encrypted
    field always ends up typed as a plain string in the inferred schema,
    same as any other string field, regardless of its original type.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    partition_by = partition_by or []
    if encrypt_blocks:
        rows = [crypto.encrypt_row(row, encrypt_blocks) for row in rows]
    run_id = ulid.new()
    run_path = f"{output_path.rstrip('/')}/run_id={run_id}"

    buckets: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        buckets[_partition_key(row, partition_by)].append(row)

    for partition, partition_rows in buckets.items():
        table = pa.Table.from_pylist(partition_rows)
        buf = io.BytesIO()
        pq.write_table(table, buf)
        part_path = f"{run_path}/{partition}/part-0.parquet" if partition else f"{run_path}/part-0.parquet"
        object_store.put_bytes(part_path, buf.getvalue())

    avsc = _infer_avsc(rows, record_name)
    object_store.put_json(f"{run_path}/schema.avsc", avsc)

    latest = {
        "run_id": run_id,
        "path": f"{run_path}/",
        "written_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "row_count": len(rows),
    }
    object_store.put_json(f"{output_path.rstrip('/')}/latest.json", latest)

    return run_id
