"""Resolves a step's CANOE_CODE_SOURCE + CANOE_ENTRYPOINT into a callable.

CANOE_CODE_SOURCE is the JSON form of the Rust `CodeSource` enum
(canoe-config crate): {"kind": "inline", "source": "..."} |
{"kind": "s3", "bucket": ..., "key": ...} |
{"kind": "git", "repo": ..., "ref": ..., "path": ...}.

CANOE_ENTRYPOINT is a dotted path like "steps.validate.run". Since every
step's code arrives as a single fetched file rather than an installed
package, the module-path prefix is informational only: the fetched file is
loaded as one module and the callable is looked up by the entrypoint's
final segment (e.g. "run"). Document this simplification for anyone
extending it to multi-file step packages.
"""
from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
from typing import Callable


def load_step_function(code_source_json: str, entrypoint: str) -> Callable:
    source = _fetch_source(json.loads(code_source_json))
    function_name = entrypoint.rsplit(".", 1)[-1]

    fd, path = tempfile.mkstemp(suffix=".py", prefix="canoe_step_")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(source)
        spec = importlib.util.spec_from_file_location("canoe_step", path)
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        sys.modules["canoe_step"] = module
        spec.loader.exec_module(module)
    finally:
        os.unlink(path)

    fn = getattr(module, function_name, None)
    if fn is None or not callable(fn):
        raise AttributeError(
            f"entrypoint '{entrypoint}' resolved to function name "
            f"'{function_name}', which is not a callable in the loaded step module"
        )
    return fn


def _fetch_source(code_source: dict) -> str:
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
            file_path = os.path.join(tmp, code_source["path"])
            with open(file_path, "r") as f:
                return f.read()
    raise ValueError(f"unknown code source kind: {kind}")
