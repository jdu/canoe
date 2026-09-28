"""Minimal object-store helper shared by checkpoint.py/output_writer.py:
reads/writes a handful of small files and lists "directories" under an
`s3://bucket/prefix` or local `file://`/plain-path URI. Deliberately tiny
(not a general object_store client) — canoe's worker images only ever need
"get one small file," "put one small file," and "list immediate child
prefixes," the same three operations `crates/canoe-sink`'s Rust side uses
via the `object_store` crate.

`s3://` is read via boto3 (already a python-worker dependency, used by
adapters); a bare path or `file://` URI is read directly off local disk —
useful for local development and the test suite, without needing a real
S3 bucket or a mocking library.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse


@dataclass
class ParsedUri:
    scheme: str  # "s3" or "file"
    bucket: str  # empty for "file"
    key: str  # S3 key, or local path


def parse_uri(uri: str) -> ParsedUri:
    parsed = urlparse(uri)
    if parsed.scheme == "s3":
        return ParsedUri(scheme="s3", bucket=parsed.netloc, key=parsed.path.lstrip("/"))
    if parsed.scheme == "file":
        return ParsedUri(scheme="file", bucket="", key=parsed.path)
    # No scheme at all: treat as a plain local path.
    return ParsedUri(scheme="file", bucket="", key=uri)


def _s3_client():
    import boto3

    return boto3.client("s3")


def get_bytes(uri: str) -> bytes | None:
    """Returns `None` if the object doesn't exist (not found is not an error here)."""
    parsed = parse_uri(uri)
    if parsed.scheme == "file":
        path = Path(parsed.key)
        if not path.is_file():
            return None
        return path.read_bytes()
    import botocore.exceptions

    try:
        response = _s3_client().get_object(Bucket=parsed.bucket, Key=parsed.key)
        return response["Body"].read()
    except botocore.exceptions.ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return None
        raise


def put_bytes(uri: str, data: bytes) -> None:
    parsed = parse_uri(uri)
    if parsed.scheme == "file":
        path = Path(parsed.key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return
    _s3_client().put_object(Bucket=parsed.bucket, Key=parsed.key, Body=data)


def get_json(uri: str) -> dict | None:
    data = get_bytes(uri)
    return json.loads(data) if data is not None else None


def put_json(uri: str, value: dict) -> None:
    put_bytes(uri, json.dumps(value).encode("utf-8"))


def read_run_rows(run_path: str) -> list[dict]:
    """Reads every `.parquet` file under `run_path` (recursing into Hive
    partition subdirectories) back into row dicts. Shared by `main.py`'s
    own S3+signal consume loop and `lookup.py`'s snapshot reads — the only
    difference between "read my own producer's newest run" and "read a
    lookup's current `latest.json`-pointed run" is *which path* gets
    passed in, not how the rows are read.
    """
    import io

    import pyarrow.parquet as pq

    rows: list[dict] = []
    parsed = parse_uri(run_path)
    if parsed.scheme == "file":
        base = Path(parsed.key)
        if not base.is_dir():
            return rows
        for parquet_file in base.rglob("*.parquet"):
            table = pq.read_table(parquet_file)
            rows.extend(table.to_pylist())
        return rows

    client = _s3_client()
    prefix = parsed.key.rstrip("/") + "/"
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=parsed.bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".parquet"):
                continue
            body = client.get_object(Bucket=parsed.bucket, Key=obj["Key"])["Body"].read()
            table = pq.read_table(io.BytesIO(body))
            rows.extend(table.to_pylist())
    return rows


def list_run_ids(dataset_uri: str) -> list[str]:
    """Lists `run_id=*` "directories" directly under `dataset_uri`, sorted
    oldest-first (ULIDs sort lexically by generation time) — the Python
    mirror of `crates/canoe-sink/src/checkpoint.rs`'s/`run_writer.rs`'s
    `list_run_ids`, duplicated per-runtime same as `MetricsReporter`/
    `LogShipper` already are elsewhere in this codebase.
    """
    parsed = parse_uri(dataset_uri)
    run_ids: list[str] = []
    if parsed.scheme == "file":
        base = Path(parsed.key)
        if not base.is_dir():
            return []
        for child in base.iterdir():
            if child.is_dir() and child.name.startswith("run_id="):
                run_ids.append(child.name[len("run_id=") :])
        run_ids.sort()
        return run_ids

    client = _s3_client()
    prefix = parsed.key.rstrip("/") + "/"
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=parsed.bucket, Prefix=prefix, Delimiter="/"):
        for common_prefix in page.get("CommonPrefixes", []):
            name = common_prefix["Prefix"][len(prefix) :].rstrip("/")
            if name.startswith("run_id="):
                run_ids.append(name[len("run_id=") :])
    run_ids.sort()
    return run_ids
