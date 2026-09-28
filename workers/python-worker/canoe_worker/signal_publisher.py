"""Publishes this worker's own automatic wake-up signal after each flush —
the Python-worker-side half of `crates/canoe-coordinator/src/signals.rs`'s
`publish_run`. Tiny `confluent_kafka.Producer`-based publish, mirroring
`metrics.py`'s existing stdlib-plus-one-client style; best effort, same as
the Rust side (a failed publish is logged, not raised — self-heal
checkpoint listing on the consumer side is what actually guarantees a run
is never missed, see README.md's "S3 + signal-driven pipeline transport").
"""
from __future__ import annotations

import json
import logging
import time

from confluent_kafka import Producer

log = logging.getLogger("canoe_worker")

SIGNALS_TOPIC = "_canoe.signals"


def publish(
    producer: Producer,
    signal: str,
    chain: str,
    step: str,
    path: str,
    run_id: str,
    row_count: int,
    key: str | None = None,
) -> None:
    event = {
        "signal": signal,
        "chain": chain,
        "sink": step,
        "emitted_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "path": path,
        "run_id": run_id,
        "row_count": row_count,
    }
    if key is not None:
        event["key"] = key
    try:
        producer.produce(SIGNALS_TOPIC, json.dumps(event).encode("utf-8"), key=signal.encode("utf-8"))
        producer.flush(timeout=10)
    except Exception:
        log.exception("failed to publish signal %s", signal)
