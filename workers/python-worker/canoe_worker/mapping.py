"""Loads a `map_values` op's JSON lookup table at pod startup.

Only ever called for an S3-sourced mapping (`map_values { mapping { bucket
= ..., key = ... } } }`) — inline text and local files are resolved by
canoe-dslc at *compile* time and baked into the generated step source as
a literal dict, so this only runs when the mapping can't be known until
the pod actually starts (or the mapping is meant to change independently
of the pipeline that reads it, without a recompile). Takes the same
CodeSource JSON shape as loader.py's `_fetch_source`, though only the
`s3` kind is meaningful here — see `canoe_dsl::codegen::python`'s
`MapValues` arm for what generates the call into this.

Fetched once at import time (module-level assignment in the generated
step source), not per record.
"""
from __future__ import annotations

import json


def load_json_mapping(code_source_json: str) -> dict:
    source = json.loads(code_source_json)
    kind = source["kind"]
    if kind == "s3":
        import boto3

        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=source["bucket"], Key=source["key"])
        mapping = json.loads(obj["Body"].read().decode("utf-8"))
    else:
        raise ValueError(f"unsupported map_values mapping source kind for a runtime load: {kind}")

    if not isinstance(mapping, dict):
        raise ValueError(
            f"mapping at s3://{source['bucket']}/{source['key']} must be a JSON object, got {type(mapping).__name__}"
        )
    return mapping
