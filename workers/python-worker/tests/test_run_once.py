"""Unit tests for canoe_worker.main's CANOE_RUN_ONCE handling
(_run_once_from_env, and _run_s3_signal_loop's run-once behavior) — see
crates/canoe-k8s/src/step_job.rs for the coordinator side that sets this
env var on a batch-mode step's Job. Run with:

    python -m unittest discover -s workers/python-worker/tests
"""
import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from canoe_worker.main import _run_once_from_env, _run_s3_signal_loop
from canoe_worker.metrics import MetricsReporter
from canoe_worker.log_shipper import LogShipper


class RunOnceFromEnvTests(unittest.TestCase):
    def test_unset_is_false(self):
        with patch.dict("os.environ", {}, clear=False):
            os.environ.pop("CANOE_RUN_ONCE", None)
            self.assertFalse(_run_once_from_env())

    def test_true_is_true_case_insensitive(self):
        with patch.dict("os.environ", {"CANOE_RUN_ONCE": "True"}):
            self.assertTrue(_run_once_from_env())

    def test_false_string_is_false(self):
        with patch.dict("os.environ", {"CANOE_RUN_ONCE": "false"}):
            self.assertFalse(_run_once_from_env())


class RunS3SignalLoopRunOnceTests(unittest.TestCase):
    """Exercises `_run_s3_signal_loop` end to end against a real `file://`
    fixture (same "no S3 mocking needed" precedent as test_lookup.py),
    with only the Kafka `Consumer`/`Producer` and the completion-signal
    publish mocked out (no local broker in this sandbox)."""

    def setUp(self):
        self.dir = Path(tempfile.mkdtemp(prefix="canoe-run-once-test-"))
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.input_path = f"file://{self.dir}/input"
        self.output_path = f"file://{self.dir}/output"
        self.checkpoint_path = f"file://{self.dir}/checkpoint.json"

    def _seed_input_run(self, run_id: str, rows: list[dict]) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq

        run_dir = Path(f"{self.dir}/input/run_id={run_id}")
        run_dir.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows), run_dir / "part-0.parquet")

    def _run_env(self):
        return {
            "CANOE_RUN_ONCE": "true",
            "CANOE_KAFKA_BOOTSTRAP": "unused:9092",
            "CANOE_INPUT_PATH": self.input_path,
            "CANOE_INPUT_SIGNAL": "upstream",
            "CANOE_OUTPUT_PATH": self.output_path,
            "CANOE_CHECKPOINT_PATH": self.checkpoint_path,
        }

    @patch("canoe_worker.main.signal_publisher.publish")
    @patch("canoe_worker.main.Producer")
    def test_drains_backlog_force_flushes_and_returns_without_looping(self, mock_producer_cls, mock_publish):
        self._seed_input_run("01AAA", [{"id": 1}, {"id": 2}])
        step_fn = MagicMock(side_effect=lambda record, **_: {**record, "doubled": record["id"] * 2})

        with patch.dict("os.environ", self._run_env(), clear=False):
            # A real Consumer would block/require a broker — run-once
            # mode must never construct one at all (see the docstring),
            # so leaving `Consumer` unpatched and unmocked here is itself
            # part of the assertion: if the run-once path tried to build
            # one, this test would hang or error.
            _run_s3_signal_loop(
                step_fn,
                "steps.generated.run",
                "orders",
                "double",
                MetricsReporter(None, base_labels={}),
                LogShipper(None, scope="orders", task="double"),
            )

        # Buffer was force-flushed even though neither
        # CANOE_OUTPUT_BATCH_SIZE nor _FLUSH_INTERVAL_SECS was ever hit.
        latest_path = Path(f"{self.dir}/output/latest.json")
        self.assertTrue(latest_path.is_file(), "run-once must flush its buffer before exiting")
        latest = json.loads(latest_path.read_text())
        self.assertEqual(latest["row_count"], 2)

        # Checkpoint advanced past the drained run.
        checkpoint_doc = json.loads(Path(f"{self.dir}/checkpoint.json").read_text())
        self.assertEqual(checkpoint_doc["through_run_id"], "01AAA")

        # The completion signal still fires once, purely as the existing
        # fast-wake optimization for whatever's downstream.
        mock_publish.assert_called_once()
        # Never constructed a Kafka Consumer for incoming signals.
        self.assertEqual(step_fn.call_count, 2)

    @patch("canoe_worker.main.signal_publisher.publish")
    @patch("canoe_worker.main.Producer")
    def test_empty_backlog_returns_immediately_with_no_flush(self, mock_producer_cls, mock_publish):
        step_fn = MagicMock()

        with patch.dict("os.environ", self._run_env(), clear=False):
            _run_s3_signal_loop(
                step_fn,
                "steps.generated.run",
                "orders",
                "double",
                MetricsReporter(None, base_labels={}),
                LogShipper(None, scope="orders", task="double"),
            )

        step_fn.assert_not_called()
        mock_publish.assert_not_called()
        self.assertFalse(Path(f"{self.dir}/output/latest.json").exists())

    @patch("canoe_worker.main.Consumer")
    def test_run_once_never_constructs_a_kafka_consumer(self, mock_consumer_cls):
        with patch.dict("os.environ", self._run_env(), clear=False), patch(
            "canoe_worker.main.Producer"
        ), patch("canoe_worker.main.signal_publisher.publish"):
            _run_s3_signal_loop(
                MagicMock(),
                "steps.generated.run",
                "orders",
                "double",
                MetricsReporter(None, base_labels={}),
                LogShipper(None, scope="orders", task="double"),
            )
        mock_consumer_cls.assert_not_called()


if __name__ == "__main__":
    unittest.main()
