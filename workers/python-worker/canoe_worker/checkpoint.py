"""Tracks how far this worker pod has read into its producer's own
`run_id=*` history — a tiny private file at `CANOE_CHECKPOINT_PATH`
recording only `{"through_run_id": "..."}`. Mirrors
`crates/canoe-sink/src/checkpoint.rs`'s listing algorithm, and is a
different concept from that module's own compaction checkpoint (a sink's
deduplicated view of *its own* history) — this one just answers "which of
the producer's runs has this consumer already processed."
"""
from __future__ import annotations

from canoe_worker import object_store


def read_through_run_id(checkpoint_path: str) -> str | None:
    doc = object_store.get_json(checkpoint_path)
    if doc is None:
        return None
    return doc.get("through_run_id")


def write_through_run_id(checkpoint_path: str, run_id: str) -> None:
    object_store.put_json(checkpoint_path, {"through_run_id": run_id})


def list_new_run_ids(input_path: str, through_run_id: str | None) -> list[str]:
    """Every `run_id=*` under `input_path` newer than `through_run_id`,
    oldest first — `None` means "everything," matching a fresh worker
    with no prior checkpoint.
    """
    all_run_ids = object_store.list_run_ids(input_path)
    if through_run_id is None:
        return all_run_ids
    return [run_id for run_id in all_run_ids if run_id > through_run_id]
