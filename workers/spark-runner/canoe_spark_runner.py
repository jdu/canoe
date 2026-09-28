"""Generic PySpark runner shared by every Spark step, both execution modes.

canoe-spark (the Rust launcher crate) submits this same script for every
spark_sql and spark_dataframe step; which transform to apply is decided at
runtime from CANOE_MODE + CANOE_SQL / CANOE_CODE_SOURCE, mirroring how the
Python worker image resolves its step function from env vars.

**CANOE_TRANSPORT** (`"streaming"`, the default, or `"batch"`) selects
which of two completely different I/O shapes this run uses — orthogonal
to CANOE_MODE (`"sql"`/`"dataframe"`, which transform to apply), matching
`canoe_config::StepConfig`'s `mode`/`engine` split. `"streaming"` is
everything below `main()`'s dispatch: reads CANOE_INPUT_TOPIC as a Kafka
source, applies the configured transform against a `input` view (SQL
mode) or `transform(df)` function (dataframe mode), and writes the result
to CANOE_OUTPUT_TOPIC via a long-running `writeStream` query — the
process never exits on its own. `"batch"` (see `_run_batch`) instead
drains every unprocessed `run_id=*` under CANOE_INPUT_PATH since this
step's own checkpoint (same guarantee the Python worker's batch mode
already gives — never just the current `latest.json` snapshot, which is
only correct for a `lookup`'s point-in-time read), applies the same kind
of transform, writes through `canoe_output_writer.write_run`, and
returns — the process exiting normally is what makes this a K8s Job
`canoe-k8s`'s `job_status` can observe completing, run by
`crates/canoe-coordinator/src/activation.rs`'s batch-mode Spark reconcile
loop instead of the always-on Deployment the streaming path still uses.

**Lookups** (CANOE_LOOKUPS, a JSON array of `{"name", "path"}` — see
`canoe_config::LookupConfig`): each is a snapshot join against a node's
*current* `latest.json`, not a second streaming source, so it can't just
be `spark.read.parquet(path)`'d once alongside the rest of this script's
one-time query construction — that would pin the join to whatever run
happened to be current at container startup, forever, for the life of
this long-running streaming job. Instead, when any lookups are declared,
the whole query runs inside `foreachBatch`: each micro-batch re-resolves
every lookup's `latest.json` fresh (via `_resolve_latest_run_path`,
plain boto3/local-file — no Spark needed for that part) immediately
before reading it, so a lookup can never be staler than one micro-batch
behind whatever the target last flushed. The no-lookups path (today's
only path) is untouched.

**Spark 4 behavior note** (image bumped from `3.5.1-python3` to
`4.2.0-python3`): confirmed against Spark's own SQL migration guide that
none of the PySpark APIs this script actually uses (`SparkSession`,
`spark.read`/`.readStream`.format/json/parquet, `DataFrame`, `.rdd.map`,
`col`/`from_json`/`udf`, `StructType`) changed or were removed between
3.5 and 4.2 — this script imports these explicitly (never `from
pyspark.sql.functions import *`, which did lose some non-function
re-exports in 4.0). One real, *silent* behavior change worth knowing
about even though nothing here needed to change for it: **`spark.sql.ansi.enabled`
defaults to `true` as of Spark 4.0** (previously `false`) — user-supplied
`spark_sql { sql = "..." }`/`spark_dataframe` transform code that relied
on lenient casting or silent null-on-overflow arithmetic under the old
default may now raise instead. Not worked around here (out of scope for
this change — canoe passes user SQL/DataFrame code through verbatim,
same as always); a pipeline hitting this can set `spark { conf =
{ "spark.sql.ansi.enabled" = "false" } }` to restore the old behavior for
just that step.
"""
from __future__ import annotations

import base64
import importlib.util
import json
import os
import re
import sys
import tempfile
from urllib.parse import urlparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StringType, StructType


def env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"missing required env var {name}", file=sys.stderr)
        sys.exit(1)
    return value


def load_transform(code_source_json: str):
    """Same CodeSource contract as canoe_worker.loader: a single fetched
    file exposing `def transform(df: DataFrame) -> DataFrame`."""
    source = json.loads(code_source_json)
    kind = source["kind"]
    if kind == "inline":
        code = source["source"]
    elif kind == "s3":
        import boto3

        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=source["bucket"], Key=source["key"])
        code = obj["Body"].read().decode("utf-8")
    elif kind == "git":
        import subprocess

        with tempfile.TemporaryDirectory() as tmp:
            subprocess.run(
                ["git", "clone", "--depth", "1", "--branch", source["ref"], source["repo"], tmp],
                check=True,
                capture_output=True,
            )
            with open(os.path.join(tmp, source["path"])) as f:
                code = f.read()
    else:
        raise ValueError(f"unknown code source kind: {kind}")

    fd, path = tempfile.mkstemp(suffix=".py")
    with os.fdopen(fd, "w") as f:
        f.write(code)
    spec = importlib.util.spec_from_file_location("canoe_spark_step", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    os.unlink(path)
    return module.transform


_ENVELOPE_PREFIX = "canoe:enc:v1:"
_ENVELOPE_RE = re.compile(re.escape(_ENVELOPE_PREFIX) + r"([^:]+):(.+)$")


def _env_var_name_for_key(key_name: str) -> str:
    """Mirrors `canoe_worker.crypto.env_var_name_for_key`/
    `crates/canoe-sink/src/crypto.rs::env_var_name_for_key` — all three
    runtimes must compute the same env var name for the same key name."""
    sanitized = "".join(c if c.isalnum() or c == "_" else "_" for c in key_name.upper())
    return f"CANOE_ENCRYPT_KEY_{sanitized}"


def _resolve_key_bytes(key_name: str) -> bytes | None:
    """Returns `None` (not an error) when the key isn't set in this
    driver's env — the driver may not carry every key every field's
    producer used; see `_decrypt_value`'s docs for how that's handled."""
    encoded = os.environ.get(_env_var_name_for_key(key_name))
    if not encoded:
        return None
    return base64.b64decode(encoded.strip())


def _encrypt_value(value_json: str, field_name: str, key_name: str) -> str:
    """`value_json` is already the field's JSON encoding (built by the
    caller). Raises if the key isn't available — a producer that declared
    `encrypt` but can't actually encrypt should fail loudly, not silently
    write plaintext, same as `canoe_worker.crypto.encrypt_row`/
    `crates/canoe-sink/src/crypto.rs::encrypt_row`.
    """
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    key = _resolve_key_bytes(key_name)
    if key is None:
        raise RuntimeError(
            f'encryption key "{key_name}" requires env var {_env_var_name_for_key(key_name)}, which is not set'
        )
    aesgcm = AESGCM(key)
    nonce = os.urandom(12)
    ciphertext = aesgcm.encrypt(nonce, value_json.encode("utf-8"), field_name.encode("utf-8"))
    wire = base64.b64encode(nonce + ciphertext).decode("ascii")
    return f"{_ENVELOPE_PREFIX}{key_name}:{wire}"


def _decrypt_value(value: str | None, field_name: str) -> str | None:
    """Best-effort, mirrors `canoe_worker.crypto.decrypt_row`: returns the
    decrypted value's *JSON encoding* as a string (not the original typed
    value) when `value` is an envelope this driver has the key for; the
    value unchanged otherwise (not an envelope, key unavailable, or
    decrypt failed — tampered/corrupted ciphertext included). Normalizing
    to a JSON string rather than Spark's own typed columns is a real,
    accepted limitation on this engine: unlike the Python worker (which
    returns the original Python type from a plain `dict`), a Spark
    DataFrame column has one fixed declared type, so a decrypted numeric/
    bool/struct field surfaces here as its JSON text — a step's own SQL/
    `transform()` can `CAST`/`from_json` it further if it needs the typed
    value back. There's no per-field warn-once logging here (unlike the
    Python side) since this runs inside a UDF on executors, where Python
    logging isn't reliably visible.
    """
    if value is None:
        return None
    match = _ENVELOPE_RE.match(value)
    if not match:
        return value
    key_name, wire_b64 = match.groups()
    key = _resolve_key_bytes(key_name)
    if key is None:
        return value
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        raw = base64.b64decode(wire_b64)
        nonce, ciphertext = raw[:12], raw[12:]
        plaintext = AESGCM(key).decrypt(nonce, ciphertext, field_name.encode("utf-8"))
        return plaintext.decode("utf-8")  # already a JSON encoding — json.dumps on the writer side
    except Exception:
        return value


def _decrypt_string_columns(df: DataFrame) -> DataFrame:
    """Applies `_decrypt_value` to every `StringType` column — the reader
    doesn't need to know ahead of time which columns are encrypted (the
    whole point of the self-describing envelope design), so this just
    checks every string column's values and decrypts any that match. A
    non-string column can't hold an encrypted value (an encrypted field
    always serializes as a string on write), so those are left alone.
    """
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            name = field.name
            decrypt_udf = udf(lambda v, _name=name: _decrypt_value(v, _name), StringType())
            df = df.withColumn(name, decrypt_udf(col(name)))
    return df


def _encrypt_columns(df: DataFrame, encrypt_blocks: list[dict]) -> DataFrame:
    """Applies this step's own declared `encrypt` blocks
    (`CANOE_ENCRYPT_FIELDS`-decoded, `[{"fields": [...], "key": "..."}]`)
    to the step's output DataFrame, right before it's serialized to
    Kafka — see `main()`'s call sites. Each declared field's column is
    replaced with its envelope string (see `_encrypt_value`); a field
    that isn't actually present in `df` is skipped.
    """
    for block in encrypt_blocks:
        key_name = block["key"]
        for field_name in block["fields"]:
            if field_name not in df.columns:
                continue
            encrypt_udf = udf(
                lambda v, _f=field_name, _k=key_name: _encrypt_value(json.dumps(v), _f, _k), StringType()
            )
            df = df.withColumn(field_name, encrypt_udf(col(field_name)))
    return df


def _get_json_object(uri: str) -> dict | None:
    """Tiny, duplicated-on-purpose `latest.json` reader — same three-line
    contract as `canoe_worker.object_store.get_json`, not shared with it
    (this image has no dependency on the python-worker package, same
    per-runtime duplication precedent as `MetricsReporter`/`LogShipper`
    elsewhere in this codebase). Returns `None` if the object doesn't
    exist — see `_resolve_latest_run_path`'s docs for why that's a
    legitimate outcome, not an error, here.
    """
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


def _resolve_latest_run_path(dataset_path: str) -> str | None:
    """`<dataset_path>/latest.json`'s `run_id`, turned into
    `<dataset_path>/run_id=<id>`. Returns `None` when nothing has been
    written to `dataset_path` yet — a legitimate state (the lookup target
    just hasn't flushed for the first time), and, since canoe-dsl can't
    validate a cross-file `lookup` reference at compile time, also what a
    typo'd `pipeline`/`node` looks like from here. Either way the caller
    falls back to an empty DataFrame rather than failing the whole query.
    """
    latest = _get_json_object(f"{dataset_path.rstrip('/')}/latest.json")
    if not latest or not latest.get("run_id"):
        return None
    return f"{dataset_path.rstrip('/')}/run_id={latest['run_id']}"


def _read_lookup_dataframes(spark: SparkSession, lookups: list[dict]) -> dict[str, DataFrame]:
    """One batch-read DataFrame per declared lookup, keyed by name —
    called fresh from inside `foreachBatch` on every micro-batch, never
    once up front, so each micro-batch sees whatever's currently in each
    lookup's `latest.json`. A lookup with nothing written yet gets an
    empty, columnless DataFrame — referencing one of its columns in SQL
    before it's ever flushed will fail that micro-batch's query, which is
    an acceptable first-pass limitation rather than something this
    function tries to paper over.
    """
    result: dict[str, DataFrame] = {}
    for entry in lookups:
        run_path = _resolve_latest_run_path(entry["path"])
        if run_path is None:
            result[entry["name"]] = spark.createDataFrame([], StructType([]))
        else:
            result[entry["name"]] = _decrypt_string_columns(spark.read.parquet(run_path))
    return result


def _run_batch(spark: SparkSession, mode: str) -> None:
    """`CANOE_TRANSPORT=batch` path: S3+signal transport, one-shot —
    drains every `run_id=*` under `CANOE_INPUT_PATH` newer than this
    step's own checkpoint (`canoe_output_writer.list_new_run_ids`, the
    same sorted-oldest-to-newest, "everything since last time" guarantee
    `canoe_worker.checkpoint.list_new_run_ids`/`crates/canoe-sink`'s
    `list_run_ids` already give Python/Rust — *not* just the current
    `latest.json` snapshot, which is only correct for a genuine
    point-in-time lookup, not a step's own primary input), applies the
    configured transform once over their union, writes the result
    through `canoe_output_writer.write_run`, advances this step's own
    checkpoint past the newest run actually processed, and returns — no
    `writeStream`/`awaitTermination()`, so the process exiting normally
    is itself what `canoe-k8s`'s `job_status` observes as this batch
    Job's completion.

    Nothing new since the checkpoint is a legitimate no-op (the
    coordinator's own `S3BacklogInspector` check is what decides whether
    to fire this Job at all, but a job can still observe empty backlog —
    e.g. a concurrent checkpoint advance, or a manual run) — completes
    successfully without writing anything, matching Python's batch mode
    behavior on an empty backlog, rather than treating it as an error.
    """
    import canoe_output_writer

    input_path = env("CANOE_INPUT_PATH")
    output_path = env("CANOE_OUTPUT_PATH")
    checkpoint_path = env("CANOE_CHECKPOINT_PATH")
    step_name = os.environ.get("CANOE_STEP_NAME", "output")
    partition_by = [p for p in os.environ.get("CANOE_OUTPUT_PARTITION_BY", "").split(",") if p]
    encrypt_blocks = json.loads(os.environ["CANOE_ENCRYPT_FIELDS"]) if os.environ.get("CANOE_ENCRYPT_FIELDS") else []
    lookups = json.loads(os.environ["CANOE_LOOKUPS"]) if os.environ.get("CANOE_LOOKUPS") else []

    through_run_id = canoe_output_writer.read_through_run_id(checkpoint_path)
    new_run_ids = canoe_output_writer.list_new_run_ids(input_path, through_run_id)
    if not new_run_ids:
        print(f"no new data under {input_path} since checkpoint run_id={through_run_id}, nothing to do")
        return

    input_base = input_path.rstrip("/")
    run_paths = [f"{input_base}/run_id={run_id}" for run_id in new_run_ids]
    # A single `spark.read.parquet` call over every unprocessed run's
    # path at once — Spark's own idiom for "union of several prefixes,"
    # rather than reading each individually and `DataFrame.union`-ing
    # them ourselves.
    records = _decrypt_string_columns(spark.read.parquet(*run_paths))
    lookup_dfs = _read_lookup_dataframes(spark, lookups)

    if mode == "sql":
        sql = env("CANOE_SQL")
        records.createOrReplaceTempView("input")
        for name, lookup_df in lookup_dfs.items():
            lookup_df.createOrReplaceTempView(name)
        result = spark.sql(sql)
    elif mode == "dataframe":
        code_source = env("CANOE_CODE_SOURCE")
        transform = load_transform(code_source)
        result: DataFrame = transform(records, lookups=lookup_dfs) if lookups else transform(records)
    else:
        raise ValueError(f"unknown CANOE_MODE: {mode}")

    result = _encrypt_columns(result, encrypt_blocks)
    run_id, row_count = canoe_output_writer.write_run(result, output_path, step_name, partition_by)

    # Advance past the *newest* input run actually processed this pass —
    # `new_run_ids` is sorted oldest-first, so the last entry is it.
    canoe_output_writer.write_checkpoint(checkpoint_path, new_run_ids[-1])

    bootstrap = os.environ.get("CANOE_KAFKA_BOOTSTRAP")
    input_signal = os.environ.get("CANOE_INPUT_SIGNAL")
    if bootstrap and input_signal:
        canoe_output_writer.publish_signal(
            bootstrap,
            input_signal,
            os.environ.get("CANOE_CHAIN_NAME", ""),
            step_name,
            f"{output_path.rstrip('/')}/run_id={run_id}/",
            run_id,
            row_count,
        )
    print(
        f"batch run complete: {row_count} row(s) from {len(new_run_ids)} input run(s) "
        f"(through run_id={new_run_ids[-1]}) written to {output_path}/run_id={run_id}/"
    )


def main() -> None:
    mode = env("CANOE_MODE")
    transport = os.environ.get("CANOE_TRANSPORT", "streaming")

    spark = SparkSession.builder.appName("canoe-spark-step").getOrCreate()

    if transport == "batch":
        _run_batch(spark, mode)
        return
    if transport != "streaming":
        raise ValueError(f"unknown CANOE_TRANSPORT: {transport} (expected \"batch\" or \"streaming\")")

    bootstrap = env("CANOE_KAFKA_BOOTSTRAP")
    input_topic = env("CANOE_INPUT_TOPIC")
    output_topic = env("CANOE_OUTPUT_TOPIC")

    # structured streaming's from_json needs an explicit schema, so sample
    # the topic once in batch mode to infer one before starting the
    # streaming query. Adequate while record shape is stable; a production
    # build should let steps pin an explicit schema instead of inferring.
    sample = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", input_topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .select(col("value").cast(StringType()).alias("json_value"))
        .limit(1000)
    )
    inferred_schema = spark.read.json(sample.rdd.map(lambda r: r.json_value)).schema

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", input_topic)
        .option("startingOffsets", "latest")
        .load()
    )
    records = raw.select(
        from_json(col("value").cast(StringType()), inferred_schema).alias("data")
    ).select("data.*")
    # Transparent decrypt on read, same as the Python worker's
    # `decrypt_row`/the Rust sink's callers: a step never needs to know
    # ahead of time which upstream columns are encrypted — see
    # `_decrypt_string_columns`'s docs.
    records = _decrypt_string_columns(records)

    lookups = json.loads(os.environ["CANOE_LOOKUPS"]) if os.environ.get("CANOE_LOOKUPS") else []
    encrypt_blocks = json.loads(os.environ["CANOE_ENCRYPT_FIELDS"]) if os.environ.get("CANOE_ENCRYPT_FIELDS") else []

    if not lookups:
        # Unchanged from before lookups existed: one query, built once.
        if mode == "sql":
            sql = env("CANOE_SQL")
            records.createOrReplaceTempView("input")
            result = spark.sql(sql)
        elif mode == "dataframe":
            code_source = env("CANOE_CODE_SOURCE")
            transform = load_transform(code_source)
            result: DataFrame = transform(records)
        else:
            raise ValueError(f"unknown CANOE_MODE: {mode}")

        result = _encrypt_columns(result, encrypt_blocks)
        query = (
            result.selectExpr("to_json(struct(*)) AS value")
            .writeStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("topic", output_topic)
            .option("checkpointLocation", f"/tmp/canoe-checkpoints/{output_topic}")
            .outputMode("append")
            .start()
        )
        query.awaitTermination()
        return

    # At least one `lookup` declared: see the module docstring for why
    # this needs `foreachBatch` instead of the single-query path above —
    # each micro-batch re-resolves every lookup's `latest.json` fresh
    # rather than joining against whatever was current at process start.
    if mode == "sql":
        sql = env("CANOE_SQL")

        def process_batch(batch_df: DataFrame, _batch_id: int) -> None:
            batch_df.createOrReplaceTempView("input")
            for name, lookup_df in _read_lookup_dataframes(spark, lookups).items():
                lookup_df.createOrReplaceTempView(name)
            out = _encrypt_columns(spark.sql(sql), encrypt_blocks)
            out.selectExpr("to_json(struct(*)) AS value").write.format("kafka").option(
                "kafka.bootstrap.servers", bootstrap
            ).option("topic", output_topic).save()

    elif mode == "dataframe":
        code_source = env("CANOE_CODE_SOURCE")
        transform = load_transform(code_source)

        def process_batch(batch_df: DataFrame, _batch_id: int) -> None:
            lookup_dfs = _read_lookup_dataframes(spark, lookups)
            out: DataFrame = _encrypt_columns(transform(batch_df, lookups=lookup_dfs), encrypt_blocks)
            out.selectExpr("to_json(struct(*)) AS value").write.format("kafka").option(
                "kafka.bootstrap.servers", bootstrap
            ).option("topic", output_topic).save()

    else:
        raise ValueError(f"unknown CANOE_MODE: {mode}")

    query = (
        records.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", f"/tmp/canoe-checkpoints/{output_topic}")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
