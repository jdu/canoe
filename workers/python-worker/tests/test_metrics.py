"""Stdlib-only unit tests for canoe_worker.metrics.MetricsReporter — no
pytest/requests-mock dependency needed, consistent with the reporter
itself only ever using the standard library. Run with:

    python -m unittest discover -s workers/python-worker/tests
"""
import json
import unittest
from unittest.mock import MagicMock, patch

from canoe_worker.metrics import MetricsReporter


class MetricsReporterTests(unittest.TestCase):
    def test_flush_is_a_no_op_without_a_url(self):
        reporter = MetricsReporter(None, base_labels={"step": "validate"})
        reporter.inc("canoe_step_messages_total")
        with patch("urllib.request.urlopen") as urlopen:
            reporter.flush()
        urlopen.assert_not_called()

    def test_counter_accumulates_across_increments(self):
        reporter = MetricsReporter("http://coordinator:9095/metrics/report", base_labels={"step": "validate"})
        reporter.inc("canoe_step_messages_total", result="ok")
        reporter.inc("canoe_step_messages_total", result="ok")
        reporter.inc("canoe_step_messages_total", result="ok")

        with patch("urllib.request.urlopen") as urlopen:
            urlopen.return_value = MagicMock()
            reporter.flush()

        request = urlopen.call_args[0][0]
        payload = json.loads(request.data)
        metric = next(m for m in payload["metrics"] if m["labels"]["result"] == "ok")
        self.assertEqual(metric["value"], 3)
        self.assertEqual(metric["type"], "counter")

    def test_gauge_holds_latest_value_not_a_sum(self):
        reporter = MetricsReporter("http://coordinator:9095/metrics/report", base_labels={})
        reporter.set("canoe_step_last_message_timestamp_seconds", 100.0)
        reporter.set("canoe_step_last_message_timestamp_seconds", 200.0)

        with patch("urllib.request.urlopen") as urlopen:
            urlopen.return_value = MagicMock()
            reporter.flush()

        payload = json.loads(urlopen.call_args[0][0].data)
        self.assertEqual(len(payload["metrics"]), 1)
        self.assertEqual(payload["metrics"][0]["value"], 200.0)

    def test_base_labels_are_attached_to_every_metric(self):
        reporter = MetricsReporter("http://coordinator:9095/metrics/report", base_labels={"chain": "orders-1", "step": "validate"})
        reporter.inc("canoe_step_messages_total", result="ok")

        with patch("urllib.request.urlopen") as urlopen:
            urlopen.return_value = MagicMock()
            reporter.flush()

        payload = json.loads(urlopen.call_args[0][0].data)
        labels = payload["metrics"][0]["labels"]
        self.assertEqual(labels["chain"], "orders-1")
        self.assertEqual(labels["step"], "validate")
        self.assertEqual(labels["result"], "ok")

    def test_network_failure_is_swallowed_not_raised(self):
        import urllib.error

        reporter = MetricsReporter("http://coordinator:9095/metrics/report", base_labels={})
        reporter.inc("canoe_step_messages_total")

        with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("connection refused")):
            reporter.flush()  # must not raise

    def test_flush_with_nothing_reported_sends_no_request(self):
        reporter = MetricsReporter("http://coordinator:9095/metrics/report", base_labels={})
        with patch("urllib.request.urlopen") as urlopen:
            reporter.flush()
        urlopen.assert_not_called()


if __name__ == "__main__":
    unittest.main()
