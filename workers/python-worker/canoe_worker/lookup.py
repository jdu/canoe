"""Fetches a `lookup { ... }` block's current snapshot — a `CANOE_LOOKUP_
<NAME>_PATH` env var (see `crates/canoe-k8s/src/pod_manager.rs::lookup_env_vars`)
points at a node's dataset root; this reads that node's `latest.json`
pointer and returns its rows, no checkpoint/incremental tracking at all —
"whatever's current right now," same as any dimension-table/stream-static
join. Unlike `checkpoint.py`'s `list_new_run_ids` (this worker's own
producer, tracked incrementally), a lookup target may be a node in a
wholly separate `.hcl` file/deployment; the only contract it relies on is
that every S3-writing node already writes `latest.json` on every flush
(`output_writer.py`), so there's nothing new on the writer side here.
"""
from __future__ import annotations

import time

from canoe_worker import crypto, object_store

# How long a fetched lookup snapshot is reused before re-checking
# `latest.json` — avoids refetching (and re-downloading/re-parsing a
# whole dataset) on every single record in a hot per-record loop. Not
# user-configurable in this first pass; matched loosely to
# S3_LOOP_POLL_TIMEOUT_SECS in main.py so a lookup refreshes on roughly
# the same cadence the main input's own self-heal backstop already polls.
DEFAULT_TTL_SECS = 30.0


class LookupCache:
    """Per-pod cache of fetched lookup snapshots, keyed by name. One
    instance lives for the pod's whole lifetime (constructed once in
    `main.py`'s S3+signal loop) so a hot loop reuses the same in-memory
    rows across many records instead of hitting the object store per call.
    """

    def __init__(self, lookups: dict[str, str], ttl_secs: float = DEFAULT_TTL_SECS) -> None:
        self._paths = lookups  # name -> dataset path
        self._ttl_secs = ttl_secs
        self._rows: dict[str, list[dict]] = {}
        self._fetched_at: dict[str, float] = {}

    def get_all(self) -> dict[str, list[dict]]:
        """Returns every declared lookup's current rows, refreshing any
        that are missing or past their TTL. Called once per outer poll
        iteration (not per record) — see `main.py`'s S3+signal loop.
        """
        now = time.monotonic()
        for name, path in self._paths.items():
            last = self._fetched_at.get(name)
            if last is None or now - last >= self._ttl_secs:
                self._rows[name] = fetch_lookup(path)
                self._fetched_at[name] = now
        return dict(self._rows)


def fetch_lookup(path: str) -> list[dict]:
    """Reads `<path>/latest.json`, then that run's rows. Returns an empty
    list (not an error) if `latest.json` doesn't exist yet — a lookup
    target that hasn't written anything yet is a legitimate, if empty,
    join input, not a failure; a genuinely wrong `pipeline`/`node`
    reference looks identical to this from the reader's side, since
    canoe-dsl can't validate a cross-file reference at compile time (see
    `canoe_config::LookupConfig`'s docs).
    """
    latest = object_store.get_json(f"{path.rstrip('/')}/latest.json")
    if latest is None:
        return []
    run_id = latest.get("run_id")
    if not run_id:
        return []
    run_path = f"{path.rstrip('/')}/run_id={run_id}"
    return [crypto.decrypt_row(row) for row in object_store.read_run_rows(run_path)]
