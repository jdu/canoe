"""Unit tests for canoe_worker.log_shipper. Run with:

    python -m unittest discover -s workers/python-worker/tests
"""
import unittest
from unittest.mock import MagicMock, patch

from canoe_worker.log_shipper import LogShipper, ShippingLogHandler, generate_ulid


class GenerateUlidTests(unittest.TestCase):
    def test_ulids_are_26_chars_and_lexically_increasing_over_time(self):
        a = generate_ulid()
        self.assertEqual(len(a), 26)
        b = generate_ulid()
        self.assertLessEqual(a[:10], b[:10])  # timestamp-derived prefix never goes backwards


class LogShipperTests(unittest.TestCase):
    def test_disabled_without_a_base_path(self):
        shipper = LogShipper(None, scope="orders", task="validate")
        shipper.write("hello")
        with patch("boto3.client") as client:
            shipper.flush()
        client.assert_not_called()

    def test_flush_with_nothing_written_sends_no_request(self):
        with patch("boto3.client") as client:
            shipper = LogShipper("s3://canoe-logs", scope="orders", task="validate")
            shipper.flush()
        client.return_value.put_object.assert_not_called()

    def test_flush_puts_every_buffered_line_joined_by_newlines(self):
        with patch("boto3.client") as client:
            shipper = LogShipper("s3://canoe-logs", scope="orders", task="validate")
            shipper.write("line one")
            shipper.write("line two")
            shipper.flush()

        put = client.return_value.put_object
        put.assert_called_once()
        kwargs = put.call_args.kwargs
        self.assertEqual(kwargs["Bucket"], "canoe-logs")
        self.assertEqual(kwargs["Body"].decode("utf-8"), "line one\nline two\n")

    def test_key_layout_is_scope_task_year_month_date_ulid(self):
        with patch("boto3.client") as client:
            shipper = LogShipper("s3://canoe-logs/root", scope="orders", task="validate")
            shipper.write("x")
            shipper.flush()

        key = client.return_value.put_object.call_args.kwargs["Key"]
        parts = key.split("/")
        self.assertEqual(parts[0], "root")
        self.assertEqual(parts[1], "orders")
        self.assertEqual(parts[2], "validate")
        self.assertEqual(len(parts[3]), 4)  # year
        self.assertEqual(len(parts[4]), 2)  # month
        filename = parts[5]
        self.assertTrue(filename.endswith(".log"))
        date_part, _, ulid_part = filename[: -len(".log")].partition("_")
        self.assertEqual(len(date_part), 10)  # YYYY-MM-DD
        self.assertEqual(len(ulid_part), 26)

    def test_repeated_flush_reuploads_the_full_buffer_not_just_new_lines(self):
        with patch("boto3.client") as client:
            shipper = LogShipper("s3://canoe-logs", scope="adapter", task="fetch_orders")
            shipper.write("first")
            shipper.flush()
            shipper.write("second")
            shipper.flush()

        put = client.return_value.put_object
        self.assertEqual(put.call_count, 2)
        self.assertEqual(put.call_args.kwargs["Body"].decode("utf-8"), "first\nsecond\n")

    def test_put_failure_is_swallowed_not_raised(self):
        with patch("boto3.client") as client:
            client.return_value.put_object.side_effect = Exception("boom")
            shipper = LogShipper("s3://canoe-logs", scope="orders", task="validate")
            shipper.write("x")
            shipper.flush()  # must not raise


class ShippingLogHandlerTests(unittest.TestCase):
    def test_emitted_records_are_written_to_the_shipper(self):
        import logging

        shipper = MagicMock()
        handler = ShippingLogHandler(shipper)
        record = logging.LogRecord("canoe_worker", logging.INFO, __file__, 1, "hello %s", ("world",), None)
        handler.emit(record)
        shipper.write.assert_called_once()
        self.assertIn("hello world", shipper.write.call_args[0][0])


if __name__ == "__main__":
    unittest.main()
