"""Unit tests for canoe_worker.lookup (fetch_lookup, LookupCache) and
canoe_worker.main's config-driven call dispatch (_call_step,
_collect_lookup_paths). Run with:

    python -m unittest discover -s workers/python-worker/tests
"""
import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq

from canoe_worker import lookup
from canoe_worker.main import _call_step, _collect_lookup_paths


def _seed_run(base: Path, run_id: str, rows: list[dict]) -> None:
    run_dir = base / f"run_id={run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, run_dir / "part-0.parquet")
    (base / "latest.json").write_text(json.dumps({"run_id": run_id, "row_count": len(rows)}))


class FetchLookupTests(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp(prefix="canoe-lookup-test-"))
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)

    def test_reads_the_run_latest_json_points_at(self):
        _seed_run(self.dir, "01AAA", [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}])
        rows = lookup.fetch_lookup(f"file://{self.dir}")
        self.assertEqual(sorted(rows, key=lambda r: r["id"]), [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}])

    def test_only_reads_the_run_latest_json_names_not_every_run(self):
        _seed_run(self.dir, "01AAA", [{"id": 1}])
        _seed_run(self.dir, "01BBB", [{"id": 2}])  # this one is now "latest"
        rows = lookup.fetch_lookup(f"file://{self.dir}")
        self.assertEqual(rows, [{"id": 2}])

    def test_missing_latest_json_returns_empty_not_an_error(self):
        # No writer has ever flushed here — a legitimate state (see
        # fetch_lookup's docstring), and indistinguishable from a
        # typo'd cross-file `pipeline`/`node` reference, since canoe-dsl
        # can't validate that at compile time.
        rows = lookup.fetch_lookup(f"file://{self.dir}/never-written")
        self.assertEqual(rows, [])


class LookupCacheTests(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp(prefix="canoe-lookup-cache-test-"))
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)

    def test_get_all_fetches_every_declared_lookup_by_name(self):
        customers_dir = self.dir / "customers"
        products_dir = self.dir / "products"
        _seed_run(customers_dir, "01AAA", [{"id": 1}])
        _seed_run(products_dir, "01BBB", [{"sku": "x"}])

        cache = lookup.LookupCache({"customers": f"file://{customers_dir}", "products": f"file://{products_dir}"})
        rows = cache.get_all()
        self.assertEqual(rows, {"customers": [{"id": 1}], "products": [{"sku": "x"}]})

    def test_within_ttl_reuses_the_cached_snapshot_without_refetching(self):
        customers_dir = self.dir / "customers"
        _seed_run(customers_dir, "01AAA", [{"id": 1}])

        cache = lookup.LookupCache({"customers": f"file://{customers_dir}"}, ttl_secs=999.0)
        first = cache.get_all()
        # Write a new run — if the cache refetched, this would show up.
        _seed_run(customers_dir, "01BBB", [{"id": 2}])
        second = cache.get_all()
        self.assertEqual(first, second, "still within the TTL window, should not have refetched")

    def test_past_ttl_refetches(self):
        customers_dir = self.dir / "customers"
        _seed_run(customers_dir, "01AAA", [{"id": 1}])

        cache = lookup.LookupCache({"customers": f"file://{customers_dir}"}, ttl_secs=0.0)
        first = cache.get_all()
        _seed_run(customers_dir, "01BBB", [{"id": 2}])
        second = cache.get_all()
        self.assertEqual(first, {"customers": [{"id": 1}]})
        self.assertEqual(second, {"customers": [{"id": 2}]}, "past the (zero-length) TTL, should have refetched")


class CollectLookupPathsTests(unittest.TestCase):
    def test_no_lookup_env_vars_returns_empty_dict(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(_collect_lookup_paths(), {})

    def test_collects_and_lowercases_declared_lookups(self):
        env = {
            "CANOE_LOOKUP_CUSTOMERS_PATH": "s3://lake/customer_pipeline/v1/customer_snapshot",
            "CANOE_LOOKUP_PRODUCTS_PATH": "s3://lake/catalog_pipeline/v1/product_snapshot",
            "CANOE_INPUT_PATH": "s3://lake/orders/v1/orders_adapter",  # unrelated, should be ignored
        }
        with patch.dict(os.environ, env, clear=True):
            self.assertEqual(
                _collect_lookup_paths(),
                {
                    "customers": "s3://lake/customer_pipeline/v1/customer_snapshot",
                    "products": "s3://lake/catalog_pipeline/v1/product_snapshot",
                },
            )


class CallStepDispatchTests(unittest.TestCase):
    def test_without_lookups_calls_step_fn_with_only_the_record(self):
        step_fn = MagicMock(return_value={"ok": True})
        result = _call_step(step_fn, {"id": 1}, None)
        step_fn.assert_called_once_with({"id": 1})
        self.assertEqual(result, {"ok": True})

    def test_with_lookups_calls_step_fn_with_a_lookups_keyword_argument(self):
        step_fn = MagicMock(return_value={"ok": True})
        lookups = {"customers": [{"id": 1}]}
        result = _call_step(step_fn, {"id": 1}, lookups)
        step_fn.assert_called_once_with({"id": 1}, lookups=lookups)
        self.assertEqual(result, {"ok": True})

    def test_an_empty_but_non_none_lookups_dict_still_uses_the_lookups_call_shape(self):
        # A step that declared a `lookup {}` block whose target hasn't
        # written anything yet gets an empty list for that name (see
        # FetchLookupTests), not a missing lookups argument entirely —
        # dispatch is config-driven (did this step declare lookups at
        # all), not based on whether any lookup currently has rows.
        step_fn = MagicMock(return_value=None)
        _call_step(step_fn, {"id": 1}, {})
        step_fn.assert_called_once_with({"id": 1}, lookups={})


if __name__ == "__main__":
    unittest.main()
