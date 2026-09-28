"""Unit tests for canoe_worker.main._maybe_flush_producer — the
per-record path's windowed produce-then-commit helper (see its docstring
and README.md's "Kafka I/O batching" note in main.py). Run with:

    python -m unittest discover -s workers/python-worker/tests
"""
import unittest
from unittest.mock import MagicMock

from canoe_worker.main import PRODUCE_FLUSH_MAX_PENDING, _maybe_flush_producer


class MaybeFlushProducerTests(unittest.TestCase):
    def setUp(self):
        self.producer = MagicMock()
        self.consumer = MagicMock()

    def test_nothing_pending_and_not_forced_is_a_no_op(self):
        pending, last_flush = _maybe_flush_producer(self.producer, self.consumer, pending=0, last_flush=1000.0)
        self.assertEqual((pending, last_flush), (0, 1000.0))
        self.producer.flush.assert_not_called()
        self.consumer.commit.assert_not_called()

    def test_below_thresholds_defers_the_flush(self):
        import time

        now = time.monotonic()
        pending, last_flush = _maybe_flush_producer(self.producer, self.consumer, pending=3, last_flush=now)
        self.assertEqual(pending, 3, "shouldn't flush yet: below both the count and time thresholds")
        self.producer.flush.assert_not_called()
        self.consumer.commit.assert_not_called()

    def test_reaching_max_pending_triggers_a_flush_and_commit(self):
        import time

        pending, last_flush = _maybe_flush_producer(
            self.producer, self.consumer, pending=PRODUCE_FLUSH_MAX_PENDING, last_flush=time.monotonic()
        )
        self.assertEqual(pending, 0, "pending resets after a flush")
        self.producer.flush.assert_called_once_with(timeout=10)
        self.consumer.commit.assert_called_once_with(asynchronous=False)
        self.assertGreater(last_flush, 0)

    def test_elapsed_interval_triggers_a_flush_even_below_max_pending(self):
        pending, last_flush = _maybe_flush_producer(self.producer, self.consumer, pending=1, last_flush=0.0)
        self.assertEqual(pending, 0)
        self.producer.flush.assert_called_once_with(timeout=10)
        self.consumer.commit.assert_called_once_with(asynchronous=False)

    def test_force_flushes_even_with_nothing_pending(self):
        # The error path calls this with force=True and pending=0 to
        # commit past a dropped message even though nothing was produced
        # for it — see main()'s exception handler.
        pending, last_flush = _maybe_flush_producer(self.producer, self.consumer, pending=0, last_flush=1e9, force=True)
        self.assertEqual(pending, 0)
        self.producer.flush.assert_called_once_with(timeout=10)
        self.consumer.commit.assert_called_once_with(asynchronous=False)

    def test_force_flushes_pending_records_too(self):
        pending, last_flush = _maybe_flush_producer(self.producer, self.consumer, pending=2, last_flush=1e9, force=True)
        self.assertEqual(pending, 0)
        self.producer.flush.assert_called_once_with(timeout=10)
        self.consumer.commit.assert_called_once_with(asynchronous=False)

    def test_commit_is_called_without_a_message_argument(self):
        # Bare commit() commits every offset this consumer has stored as
        # consumed (enable.auto.offset.store defaults to true) — not a
        # specific message — since the point is to advance past
        # everything accumulated in the window, not just the last one.
        _maybe_flush_producer(self.producer, self.consumer, pending=1, last_flush=0.0)
        args, kwargs = self.consumer.commit.call_args
        self.assertEqual(args, ())
        self.assertEqual(kwargs, {"asynchronous": False})


if __name__ == "__main__":
    unittest.main()
