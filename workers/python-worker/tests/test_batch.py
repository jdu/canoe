"""Unit tests for canoe_worker.main's batch mode (_flush_batch,
_run_batch_loop). Run with:

    python -m unittest discover -s workers/python-worker/tests
"""
import json
import unittest
from unittest.mock import MagicMock, call

from canoe_worker.main import _flush_batch, _run_batch_loop
from canoe_worker.metrics import MetricsReporter


class FlushBatchTests(unittest.TestCase):
    def setUp(self):
        self.producer = MagicMock()
        self.consumer = MagicMock()
        self.metrics = MetricsReporter(None, base_labels={})

    def test_empty_buffer_is_a_no_op(self):
        buffer = []
        _flush_batch(buffer, MagicMock(), self.producer, "out", self.consumer, self.metrics)
        self.producer.produce.assert_not_called()
        self.consumer.commit.assert_not_called()

    def test_list_result_produces_every_record_and_commits(self):
        buffer = [{"id": 1}, {"id": 2}]
        received = []

        def fake_step_fn(records):
            # `records` is the same list object `_flush_batch` clears in
            # place after this returns — snapshot it now, not the
            # (by-then-empty) reference `call_args` would otherwise show.
            received.append(list(records))
            return [{"id": 1, "doubled": True}, {"id": 2, "doubled": True}]

        step_fn = MagicMock(side_effect=fake_step_fn)

        _flush_batch(buffer, step_fn, self.producer, "out-topic", self.consumer, self.metrics)

        self.assertEqual(received, [[{"id": 1}, {"id": 2}]])
        self.assertEqual(self.producer.produce.call_count, 2)
        produced = [json.loads(c.args[1]) for c in self.producer.produce.call_args_list]
        self.assertEqual(produced, [{"id": 1, "doubled": True}, {"id": 2, "doubled": True}])
        self.producer.flush.assert_called_once()
        self.consumer.commit.assert_called_once_with(asynchronous=False)
        self.assertEqual(buffer, [], "buffer must be cleared after a successful flush")

    def test_single_dict_result_is_normalized_to_one_record(self):
        buffer = [{"id": 1}]
        step_fn = MagicMock(return_value={"id": 1, "summary": True})

        _flush_batch(buffer, step_fn, self.producer, "out", self.consumer, self.metrics)

        self.producer.produce.assert_called_once()
        self.assertEqual(json.loads(self.producer.produce.call_args.args[1]), {"id": 1, "summary": True})

    def test_none_result_produces_nothing_but_still_commits(self):
        buffer = [{"id": 1}, {"id": 2}]
        step_fn = MagicMock(return_value=None)

        _flush_batch(buffer, step_fn, self.producer, "out", self.consumer, self.metrics)

        self.producer.produce.assert_not_called()
        self.consumer.commit.assert_called_once_with(asynchronous=False)
        self.assertEqual(buffer, [])

    def test_exception_drops_the_whole_batch_but_still_commits(self):
        buffer = [{"id": 1}, {"id": 2}, {"id": 3}]
        step_fn = MagicMock(side_effect=RuntimeError("boom"))

        _flush_batch(buffer, step_fn, self.producer, "out", self.consumer, self.metrics)

        self.producer.produce.assert_not_called()
        self.consumer.commit.assert_called_once_with(asynchronous=False)
        self.assertEqual(buffer, [], "buffer must still be cleared so the batch isn't reprocessed")

    def test_metrics_counted_by_batch_size_not_per_call(self):
        buffer = [{"id": 1}, {"id": 2}, {"id": 3}]
        step_fn = MagicMock(return_value=[])

        _flush_batch(buffer, step_fn, self.producer, "out", self.consumer, self.metrics)

        # canoe_step_messages_total{result="ok"} should read 3 (the batch
        # size), not 1 (one call to step_fn).
        rendered_metrics = []
        self.metrics.flush = lambda: rendered_metrics.append(  # type: ignore[method-assign]
            {k: v for k, v in self.metrics._counters.items()}
        )
        self.metrics.flush()
        ((name, labels), value), = rendered_metrics[0].items()
        self.assertEqual(name, "canoe_step_messages_total")
        self.assertEqual(value, 3)


class FakeMessage:
    def __init__(self, value: dict):
        self._value = json.dumps(value).encode("utf-8")

    def value(self):
        return self._value

    def error(self):
        return None


class RunBatchLoopTests(unittest.TestCase):
    def test_flushes_once_batch_size_is_reached(self):
        messages = [FakeMessage({"id": i}) for i in range(5)]

        consumer = MagicMock()
        # 5 real messages, then interrupt to end the otherwise-infinite loop.
        consumer.poll.side_effect = [*messages, KeyboardInterrupt()]

        producer = MagicMock()
        call_sizes = []

        def fake_step_fn(records):
            # Same by-reference-mutation reasoning as
            # FlushBatchTests.test_list_result_produces_every_record_and_commits
            # — snapshot the size now, before the caller clears the list.
            call_sizes.append(len(records))
            return [{"n": len(records)}]

        step_fn = MagicMock(side_effect=fake_step_fn)
        metrics = MetricsReporter(None, base_labels={})
        log_shipper = MagicMock()
        log_shipper.flush = MagicMock()

        _run_batch_loop(consumer, producer, step_fn, "out", metrics, log_shipper, batch_size=2, batch_flush_interval_secs=999)

        # 5 records at batch_size=2 -> two full flushes (2, 2) inside the
        # loop, plus the `finally` block's flush of the last partial one (1).
        self.assertEqual(step_fn.call_count, 3)
        self.assertEqual(call_sizes, [2, 2, 1])
        consumer.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
