"""Generic Python step worker entrypoint.

Every canoe Python step's pods run this same image. On startup it loads
the specific step's function per CANOE_CODE_SOURCE/CANOE_ENTRYPOINT (see
loader.py), then runs a consume -> transform -> produce loop:

  input_topic (JSON record) -> user function -> output_topic (JSON record(s))

The user function's contract: `def run(record: dict) -> dict | list[dict] | None`.
Returning None drops the record; returning a list fans out to multiple
output messages.

**Batch mode**: if a step's DSL `batch { size = ... }` block set
CANOE_BATCH_SIZE, the worker instead buffers up to that many records (or
CANOE_BATCH_FLUSH_INTERVAL_SECS, whichever comes first) and calls the step
function once per batch with the whole list: `def run(records:
list[dict]) -> list[dict] | dict | None`. Only reachable for a `python {
code = ... }` escape-hatch step — canoe-dsl rejects `batch` on declarative
ops, which always compile to the per-record contract above. See
README.md's "Batching" for the tradeoffs (per-record error isolation and
message keys are both lost in batch mode).

**Kafka I/O batching, independent of the above**: even outside DSL `batch`
mode, `step_fn` is still called once per record, but the *producing* and
*committing* underneath it is windowed rather than done once per message
— see `_maybe_flush_producer`. An earlier version called
`Producer.flush()` (a blocking wait for every in-flight message's
delivery) after every single record, which forces a synchronous network
round trip per message and defeats librdkafka's own internal batching —
see README.md's "Producing efficiently" for the same trap in an adapter
script. `produce()`/`poll(0)` alone are cheap and non-blocking; only
`flush()` (and the `commit()` that has to wait for it, to keep "commit
only after produce" ordering) needs to be windowed.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time

from confluent_kafka import Consumer, KafkaError, Producer

from canoe_worker import checkpoint, crypto, lookup, object_store, output_writer, signal_publisher
from canoe_worker.loader import load_step_function
from canoe_worker.log_shipper import LogShipper, ShippingLogHandler
from canoe_worker.metrics import MetricsReporter

# How often the S3 + signal loop re-lists what's new under CANOE_INPUT_PATH
# even with no signal traffic at all — the self-heal backstop; a signal is
# a latency optimization, never a correctness dependency. See README.md's
# "S3 + signal-driven pipeline transport".
S3_LOOP_POLL_TIMEOUT_SECS = 5.0

METRICS_FLUSH_INTERVAL_SECS = 15
LOG_FLUSH_INTERVAL_SECS = 15
# How the per-record path (not DSL `batch` mode — see _flush_batch for
# that) windows its own produce-then-commit round trip. PRODUCE_FLUSH_INTERVAL_SECS
# matches consumer.poll(timeout=1.0)'s own granularity, so an idle step
# still flushes/commits promptly rather than sitting on unflushed output.
# PRODUCE_FLUSH_MAX_PENDING bounds how many messages would need
# redelivering if the pod died mid-window (uncommitted, but possibly
# already-delivered messages) — same "bounded blast radius, not unbounded"
# reasoning `dynamic_route`'s `max_branches` uses elsewhere in this repo.
PRODUCE_FLUSH_INTERVAL_SECS = 1.0
PRODUCE_FLUSH_MAX_PENDING = 500

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("canoe_worker")


def env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        log.error("missing required env var %s", name)
        sys.exit(1)
    return value


def main() -> None:
    entrypoint = env("CANOE_ENTRYPOINT")
    code_source = env("CANOE_CODE_SOURCE")

    chain_name = os.environ.get("CANOE_CHAIN_NAME", "")
    step_name = os.environ.get("CANOE_STEP_NAME", "")

    step_fn = load_step_function(code_source, entrypoint)
    log.info("loaded step function for entrypoint=%s", entrypoint)

    metrics = MetricsReporter(
        os.environ.get("CANOE_METRICS_URL"),
        base_labels={"chain": chain_name, "step": step_name},
    )

    # Ships this pod's own logging (canoe_worker's and the user step
    # function's alike) to the global S3 log location, if configured — see
    # README.md's "Log collection".
    log_shipper = LogShipper(os.environ.get("CANOE_LOG_BASE_PATH"), scope=chain_name, task=step_name)
    logging.getLogger().addHandler(ShippingLogHandler(log_shipper))

    # This image serves two distinct transports off the same env-var
    # contract, dispatched on which one the coordinator set: regular
    # `PythonStep` pods get CANOE_INPUT_PATH (S3 + signal — the default
    # today), while `dynamic_route`'s per-discovered-value pods
    # (`KafkaPythonStep`) still get CANOE_INPUT_TOPIC (Kafka-topic
    # transport, unmigrated pending a later phase) — see
    # `canoe_config::KafkaPythonStep`'s docs.
    if os.environ.get("CANOE_INPUT_PATH"):
        _run_s3_signal_loop(step_fn, entrypoint, chain_name, step_name, metrics, log_shipper)
        return

    bootstrap = env("CANOE_KAFKA_BOOTSTRAP")
    input_topic = env("CANOE_INPUT_TOPIC")
    output_topic = env("CANOE_OUTPUT_TOPIC")
    group_id = env("CANOE_CONSUMER_GROUP")

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    producer = Producer({"bootstrap.servers": bootstrap})
    consumer.subscribe([input_topic])

    batch_size = int(os.environ["CANOE_BATCH_SIZE"]) if os.environ.get("CANOE_BATCH_SIZE") else None
    batch_flush_interval_secs = float(os.environ.get("CANOE_BATCH_FLUSH_INTERVAL_SECS", "5"))

    if batch_size:
        log.info(
            "worker started (batch mode, size=%d, flush_interval_secs=%s): %s -> [%s] -> %s",
            batch_size,
            batch_flush_interval_secs,
            input_topic,
            entrypoint,
            output_topic,
        )
        _run_batch_loop(
            consumer, producer, step_fn, output_topic, metrics, log_shipper, batch_size, batch_flush_interval_secs
        )
        return

    log.info("worker started: %s -> [%s] -> %s", input_topic, entrypoint, output_topic)

    last_flush = time.monotonic()
    last_log_flush = time.monotonic()
    pending = 0
    last_produce_flush = time.monotonic()
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                pending, last_produce_flush = _maybe_flush_producer(producer, consumer, pending, last_produce_flush)
                last_flush = _maybe_flush_metrics(metrics, last_flush)
                last_log_flush = _maybe_flush_logs(log_shipper, last_log_flush)
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("kafka error: %s", msg.error())
                continue

            try:
                record = json.loads(msg.value())
                result = step_fn(record)
            except Exception:
                log.exception("step function raised, dropping message")
                metrics.inc("canoe_step_messages_total", result="error")
                # Forced: flushes/commits anything accumulated from
                # earlier successful records in this window *and* this
                # message's own offset (dropping it) in one round trip —
                # see _maybe_flush_producer's docs.
                pending, last_produce_flush = _maybe_flush_producer(
                    producer, consumer, pending, last_produce_flush, force=True
                )
                last_flush = _maybe_flush_metrics(metrics, last_flush)
                last_log_flush = _maybe_flush_logs(log_shipper, last_log_flush)
                continue

            for out_record in _as_list(result):
                producer.produce(output_topic, json.dumps(out_record).encode("utf-8"), key=msg.key())
            producer.poll(0)
            pending += 1
            pending, last_produce_flush = _maybe_flush_producer(producer, consumer, pending, last_produce_flush)

            metrics.inc("canoe_step_messages_total", result="ok")
            metrics.set("canoe_step_last_message_timestamp_seconds", time.time())
            last_flush = _maybe_flush_metrics(metrics, last_flush)
    except KeyboardInterrupt:
        pass
    finally:
        _maybe_flush_producer(producer, consumer, pending, last_produce_flush, force=True)
        metrics.flush()
        log_shipper.flush()
        consumer.close()


def _run_once_from_env() -> bool:
    """CANOE_RUN_ONCE=true (set by `canoe-k8s`'s batch-mode Job spec — see
    `crates/canoe-k8s/src/step_job.rs`) tells `_run_s3_signal_loop` to
    drain currently-available backlog once and exit, instead of looping
    forever waiting for the next signal/poll interval. Unset (every
    persistent-pod caller, and any caller that predates this env var) —
    the default — preserves today's loop-forever behavior exactly."""
    return os.environ.get("CANOE_RUN_ONCE", "").strip().lower() == "true"


def _run_s3_signal_loop(
    step_fn,
    entrypoint: str,
    chain_name: str,
    step_name: str,
    metrics: MetricsReporter,
    log_shipper: LogShipper,
) -> None:
    """S3 + signal transport: subscribes to the coordinator's internal
    signals topic filtered to CANOE_INPUT_SIGNAL for a fast wake-up, but
    on every poll timeout (not just a matching signal) also lists new
    `run_id=*` runs under CANOE_INPUT_PATH past this pod's own
    CANOE_CHECKPOINT_PATH — the self-heal backstop, see module docstring
    and README.md's "S3 + signal-driven pipeline transport". Each new run
    is read, passed through `step_fn` once per record (batch mode isn't
    supported on this path yet — CANOE_BATCH_SIZE only applies to the
    legacy Kafka-topic loop), buffered, and flushed via `output_writer`
    once CANOE_OUTPUT_BATCH_SIZE/CANOE_OUTPUT_FLUSH_INTERVAL_SECS says
    it's time.

    If any `CANOE_LOOKUP_<NAME>_PATH` env vars are set (one per `lookup
    {}` block the DSL compiled onto this step — see
    `crates/canoe-k8s/src/pod_manager.rs::lookup_env_vars`), `step_fn` is
    called with an extra `lookups: dict[str, list[dict]]` keyword argument
    instead of the single-arg call above — config-driven dispatch, not
    runtime signature introspection, same as how DSL `batch {}` changes
    the call shape elsewhere in this file.

    **CANOE_RUN_ONCE** (see `_run_once_from_env`): when set, this function
    processes exactly one pass — `checkpoint.list_new_run_ids` already
    returns *every* currently-unprocessed run in one call, so a single
    pass already drains all backlog that exists right now, no repeated
    polling needed. The output buffer is force-flushed at the end of that
    pass regardless of `CANOE_OUTPUT_BATCH_SIZE`/`_FLUSH_INTERVAL_SECS`
    (if either threshold isn't yet met, nothing would otherwise ever flush
    it before the process exits — a run-once Job never gets a "later" to
    catch up on), and the signals-topic consumer is skipped entirely
    (waiting for a wake-up is meaningless when the process is about to
    exit either way); `signal_publisher`'s completion signal is still
    published after a real flush, purely as the existing fast-wake
    optimization for whatever's downstream.
    """
    run_once = _run_once_from_env()
    bootstrap = env("CANOE_KAFKA_BOOTSTRAP")
    input_path = env("CANOE_INPUT_PATH")
    input_signal = env("CANOE_INPUT_SIGNAL")
    output_path = env("CANOE_OUTPUT_PATH")
    checkpoint_path = env("CANOE_CHECKPOINT_PATH")
    output_batch_size = int(os.environ.get("CANOE_OUTPUT_BATCH_SIZE", "10000"))
    output_flush_interval_secs = float(os.environ.get("CANOE_OUTPUT_FLUSH_INTERVAL_SECS", "30"))
    partition_by = [p for p in os.environ.get("CANOE_OUTPUT_PARTITION_BY", "").split(",") if p]
    encrypt_blocks = json.loads(os.environ["CANOE_ENCRYPT_FIELDS"]) if os.environ.get("CANOE_ENCRYPT_FIELDS") else []

    lookup_paths = _collect_lookup_paths()
    lookup_cache = lookup.LookupCache(lookup_paths) if lookup_paths else None

    consumer = None
    if not run_once:
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": f"canoe-s3-{chain_name}-{step_name}",
                "enable.auto.commit": True,
                "auto.offset.reset": "latest",
            }
        )
        consumer.subscribe([signal_publisher.SIGNALS_TOPIC])
    producer = Producer({"bootstrap.servers": bootstrap})

    through_run_id = checkpoint.read_through_run_id(checkpoint_path)
    log.info(
        "worker started (S3 + signal transport%s): %s -> [%s] -> %s (resuming past run_id=%s)",
        ", run-once" if run_once else "",
        input_path,
        entrypoint,
        output_path,
        through_run_id,
    )

    buffer: list[dict] = []
    last_flush = time.monotonic()
    last_metrics_flush = time.monotonic()
    last_log_flush = time.monotonic()

    try:
        while True:
            if consumer is not None:
                # Only used to wake up promptly — matched signal or not,
                # poll timeout or not, what happens next (list what's
                # actually new) is identical either way. See the
                # self-heal note above. Skipped entirely in run-once
                # mode: nothing to wake up for when about to exit anyway.
                consumer.poll(timeout=S3_LOOP_POLL_TIMEOUT_SECS)

            # Refreshed once per outer poll iteration, not per record —
            # see `LookupCache`'s docs for why a hot per-record loop
            # shouldn't refetch a lookup's snapshot on every call.
            lookups = lookup_cache.get_all() if lookup_cache else None

            for run_id in checkpoint.list_new_run_ids(input_path, through_run_id):
                run_path = f"{input_path.rstrip('/')}/run_id={run_id}"
                for record in object_store.read_run_rows(run_path):
                    record = crypto.decrypt_row(record)
                    try:
                        result = _call_step(step_fn, record, lookups)
                    except Exception:
                        log.exception("step function raised, dropping record")
                        metrics.inc("canoe_step_messages_total", result="error")
                        continue
                    for out_record in _as_list(result):
                        buffer.append(out_record)
                        metrics.inc("canoe_step_messages_total", result="ok")
                through_run_id = run_id
                metrics.set("canoe_step_last_message_timestamp_seconds", time.time())

            due = len(buffer) >= output_batch_size or (
                buffer and time.monotonic() - last_flush >= output_flush_interval_secs
            )
            # Run-once mode force-flushes whatever's buffered even if
            # neither threshold above was hit yet — see the docstring.
            if due or (run_once and buffer):
                n = len(buffer)
                new_run_id = output_writer.write_run(
                    output_path, buffer, step_name or "output", partition_by, encrypt_blocks
                )
                buffer = []
                if through_run_id is not None:
                    checkpoint.write_through_run_id(checkpoint_path, through_run_id)
                signal_publisher.publish(
                    producer, input_signal, chain_name, step_name, f"{output_path}/run_id={new_run_id}/", new_run_id, n
                )
                last_flush = time.monotonic()

            last_metrics_flush = _maybe_flush_metrics(metrics, last_metrics_flush)
            last_log_flush = _maybe_flush_logs(log_shipper, last_log_flush)

            if run_once:
                log.info("run-once: backlog drained, exiting")
                break
    except KeyboardInterrupt:
        pass
    finally:
        metrics.flush()
        log_shipper.flush()
        if consumer is not None:
            consumer.close()


def _call_step(step_fn, record: dict, lookups: dict[str, list[dict]] | None):
    """Config-driven call-shape dispatch: `lookups` is only ever non-`None`
    when the step's compiled config declared at least one `lookup {}`
    block (`_collect_lookup_paths` returned something, so `lookup_cache`
    was constructed) — never based on inspecting `step_fn`'s own
    signature. Same principle DSL `batch {}` already uses elsewhere in
    this file to pick `run(record)` vs `run(records)`.
    """
    if lookups is not None:
        return step_fn(record, lookups=lookups)
    return step_fn(record)


def _collect_lookup_paths() -> dict[str, str]:
    """Every `CANOE_LOOKUP_<NAME>_PATH` env var, keyed by `<name>` lowercased.
    `pod_manager.rs::lookup_env_vars` uppercases the DSL's `lookup "name"
    {}` label to build the env var (collapsing any non-alphanumeric
    character to `_`) — lowercasing it back recovers the original name
    exactly for the conventional snake_case names this codebase's DSL
    blocks use everywhere else; a lookup name that isn't already
    lowercase/digits/underscores won't round-trip perfectly, a known,
    accepted limitation of deriving the dict key from the env var name
    rather than threading it through separately.
    """
    prefix, suffix = "CANOE_LOOKUP_", "_PATH"
    return {
        key[len(prefix) : -len(suffix)].lower(): value
        for key, value in os.environ.items()
        if key.startswith(prefix) and key.endswith(suffix) and value
    }


def _run_batch_loop(
    consumer: Consumer,
    producer: Producer,
    step_fn,
    output_topic: str,
    metrics: MetricsReporter,
    log_shipper: LogShipper,
    batch_size: int,
    batch_flush_interval_secs: float,
) -> None:
    """Same poll loop as the per-record path in `main()`, but buffers
    decoded records instead of calling `step_fn` immediately — flushed
    once the buffer reaches `batch_size` or `batch_flush_interval_secs`
    elapses since the last flush, whichever comes first. `step_fn` gets
    the whole buffered list at once: `def run(records: list[dict]) ->
    list[dict] | dict | None`.

    Two things a per-record step doesn't have to think about: batch mode
    has no per-record error isolation (a raised exception drops the
    *whole* batch, not just the record that caused it — same "commit
    anyway, don't redeliver" choice the per-record path already makes for
    a single bad record, just at batch granularity), and no per-record
    Kafka message key (there's no single natural key for a batch's worth
    of output, so every produced record goes out with `key=None`).
    """
    buffer: list[dict] = []
    last_batch_flush = time.monotonic()
    last_flush = time.monotonic()
    last_log_flush = time.monotonic()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                if buffer and time.monotonic() - last_batch_flush >= batch_flush_interval_secs:
                    _flush_batch(buffer, step_fn, producer, output_topic, consumer, metrics)
                    last_batch_flush = time.monotonic()
                last_flush = _maybe_flush_metrics(metrics, last_flush)
                last_log_flush = _maybe_flush_logs(log_shipper, last_log_flush)
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("kafka error: %s", msg.error())
                continue

            try:
                buffer.append(json.loads(msg.value()))
            except Exception:
                log.exception("failed to decode message, dropping")
                consumer.commit(msg, asynchronous=False)
                continue

            if len(buffer) >= batch_size:
                _flush_batch(buffer, step_fn, producer, output_topic, consumer, metrics)
                last_batch_flush = time.monotonic()
            last_flush = _maybe_flush_metrics(metrics, last_flush)
            last_log_flush = _maybe_flush_logs(log_shipper, last_log_flush)
    except KeyboardInterrupt:
        pass
    finally:
        _flush_batch(buffer, step_fn, producer, output_topic, consumer, metrics)
        metrics.flush()
        log_shipper.flush()
        consumer.close()


def _flush_batch(
    buffer: list[dict],
    step_fn,
    producer: Producer,
    output_topic: str,
    consumer: Consumer,
    metrics: MetricsReporter,
) -> None:
    if not buffer:
        return
    n = len(buffer)
    try:
        result = step_fn(buffer)
    except Exception:
        log.exception("batch step function raised, dropping batch of %d record(s)", n)
        metrics.inc("canoe_step_messages_total", value=n, result="error")
        # Same "commit anyway" choice the per-record path makes for a
        # single bad record (see module docstring) — commits whatever
        # this consumer has locally stored as consumed so far, i.e. every
        # message polled since the last commit, which is exactly this
        # batch.
        consumer.commit(asynchronous=False)
        buffer.clear()
        return

    for out_record in _as_list(result):
        producer.produce(output_topic, json.dumps(out_record).encode("utf-8"))
    producer.flush(timeout=10)
    consumer.commit(asynchronous=False)

    metrics.inc("canoe_step_messages_total", value=n, result="ok")
    metrics.set("canoe_step_last_message_timestamp_seconds", time.time())
    buffer.clear()


def _maybe_flush_producer(
    producer: Producer,
    consumer: Consumer,
    pending: int,
    last_flush: float,
    force: bool = False,
) -> tuple[int, float]:
    """Windows the per-record path's produce-then-commit round trip across
    up to `PRODUCE_FLUSH_MAX_PENDING` records or `PRODUCE_FLUSH_INTERVAL_SECS`,
    instead of a blocking `Producer.flush()` (and the `Consumer.commit()`
    that has to wait for it) after every single message — see the module
    docstring's "Kafka I/O batching" note for why.

    `producer.flush()` waits for every message produced since the last
    flush to be delivered, then `consumer.commit()` (no message argument:
    commits every offset this consumer has locally stored as consumed —
    `enable.auto.offset.store` defaults to true, so that's every message
    `poll()` has returned, whether this pod produced anything for it or
    not) advances the committed offset only that far. Ordering is the
    same "commit only after produce" guarantee the old per-message version
    had, just amortized across a window of records instead of one — a pod
    that dies mid-window redelivers up to `PRODUCE_FLUSH_MAX_PENDING`
    records (or however many landed in the last ~`PRODUCE_FLUSH_INTERVAL_SECS`)
    on restart, not just the one in flight.

    Returns `(pending, last_flush)` for the caller to carry into the next
    call, same threading-state-through-return-values shape as
    `_maybe_flush_metrics`/`_maybe_flush_logs`.
    """
    now = time.monotonic()
    # `force` always commits — even with nothing pending to produce/flush,
    # the caller needs a dropped (errored) message's offset committed past
    # right away, not deferred to the next size/time-based trigger. The
    # size/time triggers themselves only fire when there's actually
    # something accumulated (no point committing an unchanged position).
    due = force or (pending > 0 and (pending >= PRODUCE_FLUSH_MAX_PENDING or now - last_flush >= PRODUCE_FLUSH_INTERVAL_SECS))
    if not due:
        return pending, last_flush
    producer.flush(timeout=10)
    consumer.commit(asynchronous=False)
    return 0, now


def _maybe_flush_metrics(metrics: MetricsReporter, last_flush: float) -> float:
    now = time.monotonic()
    if now - last_flush >= METRICS_FLUSH_INTERVAL_SECS:
        metrics.flush()
        return now
    return last_flush


def _maybe_flush_logs(log_shipper: LogShipper, last_flush: float) -> float:
    now = time.monotonic()
    if now - last_flush >= LOG_FLUSH_INTERVAL_SECS:
        log_shipper.flush()
        return now
    return last_flush


def _as_list(result):
    if result is None:
        return []
    if isinstance(result, list):
        return result
    return [result]


if __name__ == "__main__":
    main()
