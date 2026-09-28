"""Unit tests for canoe_adapter_output.write_batch — a mode = "batch"
adapter's script-facing helper (see that module's docstring). Same
"no S3 mocking needed" precedent as python-worker's test_run_once.py:
exercised against a real `file://` fixture, with only the Kafka
`Producer`/signal publish mocked out (no local broker in this sandbox).

Run with (from this directory, so `canoe_adapter_output` resolves via cwd,
same convention python-worker's tests use for `canoe_worker`):

    cd workers/adapter-runner && python3 -m unittest discover -s tests
"""
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import canoe_adapter_output as output


class WriteBatchTests(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp(prefix="canoe-adapter-output-test-"))
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.output_path = f"file://{self.dir}/output"

    def _env(self, **extra):
        env = {
            "CANOE_OUTPUT_PATH": self.output_path,
            "CANOE_ADAPTER_NAME": "orders_adapter",
            "CANOE_KAFKA_BOOTSTRAP": "unused:9092",
        }
        env.update(extra)
        return env

    @patch("canoe_adapter_output._publish_signal")
    def test_empty_rows_is_a_no_op(self, mock_publish):
        with patch.dict("os.environ", self._env(), clear=False):
            output.write_batch([])
        self.assertFalse((self.dir / "output").exists())
        mock_publish.assert_not_called()

    @patch("canoe_adapter_output._publish_signal")
    def test_writes_run_scoped_layout_with_schema_and_latest_pointer(self, mock_publish):
        rows = [{"id": 1, "amount": 4.5}, {"id": 2, "amount": 9.0}]
        with patch.dict("os.environ", self._env(), clear=False):
            output.write_batch(rows)

        latest = json.loads((self.dir / "output" / "latest.json").read_text())
        self.assertEqual(latest["row_count"], 2)
        run_id = latest["run_id"]
        run_dir = self.dir / "output" / f"run_id={run_id}"
        self.assertTrue((run_dir / "part-0.parquet").exists())

        schema = json.loads((run_dir / "schema.avsc").read_text())
        self.assertEqual(schema["name"], "orders_adapter")
        field_names = {f["name"] for f in schema["fields"]}
        self.assertEqual(field_names, {"id", "amount"})

        mock_publish.assert_called_once()
        args, _ = mock_publish.call_args
        self.assertEqual(args[0], "unused:9092")
        self.assertEqual(args[1], "orders_adapter")
        self.assertEqual(args[3], run_id)
        self.assertEqual(args[4], 2)

    @patch("canoe_adapter_output._publish_signal")
    def test_partition_by_splits_rows_into_hive_partitions(self, mock_publish):
        rows = [{"id": 1, "region": "US"}, {"id": 2, "region": "EU"}, {"id": 3, "region": "US"}]
        with patch.dict("os.environ", self._env(CANOE_OUTPUT_PARTITION_BY="region"), clear=False):
            output.write_batch(rows)

        latest = json.loads((self.dir / "output" / "latest.json").read_text())
        run_dir = self.dir / "output" / f"run_id={latest['run_id']}"
        self.assertTrue((run_dir / "region=US" / "part-0.parquet").exists())
        self.assertTrue((run_dir / "region=EU" / "part-0.parquet").exists())

    @patch("canoe_adapter_output._publish_signal")
    def test_batch_size_chunks_into_multiple_runs(self, mock_publish):
        rows = [{"id": i} for i in range(5)]
        with patch.dict("os.environ", self._env(CANOE_OUTPUT_BATCH_SIZE="2"), clear=False):
            output.write_batch(rows)

        run_dirs = sorted(p.name for p in (self.dir / "output").iterdir() if p.name.startswith("run_id="))
        self.assertEqual(len(run_dirs), 3)
        self.assertEqual(mock_publish.call_count, 3)
        # latest.json points at the last chunk written (3rd run, 1 row).
        latest = json.loads((self.dir / "output" / "latest.json").read_text())
        self.assertEqual(latest["row_count"], 1)

    @patch("canoe_adapter_output._publish_signal")
    def test_missing_bootstrap_skips_signal_publish_but_still_writes(self, mock_publish):
        rows = [{"id": 1}]
        env = self._env()
        del env["CANOE_KAFKA_BOOTSTRAP"]
        with patch.dict("os.environ", env, clear=True):
            output.write_batch(rows)
        self.assertTrue((self.dir / "output" / "latest.json").exists())
        mock_publish.assert_not_called()


if __name__ == "__main__":
    unittest.main()
