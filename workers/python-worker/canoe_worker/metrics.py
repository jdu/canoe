"""Pushes metrics to the coordinator's built-in Pushgateway-style endpoint
(`CANOE_METRICS_URL`, `POST .../metrics/report`) — see
`crates/canoe-coordinator/src/metrics.rs` for the wire protocol and why
tasks push their own metrics rather than being scraped directly (a
worker Deployment scales up and down on its own; an adapter's Job may
finish before Prometheus's next scrape interval ever reaches it).

Stdlib-only (`urllib`), deliberately: an adapter's script or a `uv` step's
script don't have `canoe_worker` installed (they're standalone scripts by
design), so `workers/adapter-runner/run.py` and
`workers/uv-step-worker/run.py` carry their own copies of this same small
reporter rather than importing it — see README.md's "Metrics" section for
the wire protocol directly, if writing a step/adapter script that wants
to push its own custom metrics beyond what canoe already reports for it.

No-ops entirely if `CANOE_METRICS_URL` isn't set, so running a step
locally without a coordinator doesn't need one configured.
"""
from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request

log = logging.getLogger("canoe_worker.metrics")

_LabelKey = tuple[tuple[str, str], ...]


class MetricsReporter:
    """Counters track their own cumulative total (push the current sum
    each flush, not a delta — see the module docs on why the coordinator
    treats a push as "replace," not "add"). Gauges just hold the latest
    value set. `base_labels` (e.g. chain/step name) are attached to every
    metric this reporter pushes.
    """

    def __init__(self, url: str | None, base_labels: dict[str, str]):
        self._url = url
        self._base_labels = dict(base_labels)
        self._counters: dict[tuple[str, _LabelKey], float] = {}
        self._gauges: dict[tuple[str, _LabelKey], float] = {}

    def inc(self, name: str, value: float = 1.0, **extra_labels: str) -> None:
        key = self._key(name, extra_labels)
        self._counters[key] = self._counters.get(key, 0.0) + value

    def set(self, name: str, value: float, **extra_labels: str) -> None:
        key = self._key(name, extra_labels)
        self._gauges[key] = value

    def _key(self, name: str, extra_labels: dict[str, str]) -> tuple[str, _LabelKey]:
        labels = {**self._base_labels, **extra_labels}
        return (name, tuple(sorted(labels.items())))

    def flush(self) -> None:
        if not self._url:
            return
        metrics = [
            {"name": name, "type": "counter", "value": value, "labels": dict(labels)}
            for (name, labels), value in self._counters.items()
        ]
        metrics += [
            {"name": name, "type": "gauge", "value": value, "labels": dict(labels)}
            for (name, labels), value in self._gauges.items()
        ]
        if not metrics:
            return

        payload = json.dumps({"metrics": metrics}).encode("utf-8")
        req = urllib.request.Request(
            self._url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            urllib.request.urlopen(req, timeout=5).close()
        except (urllib.error.URLError, OSError) as e:
            log.warning("failed to push metrics to %s: %s", self._url, e)
