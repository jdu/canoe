# A second adapter example, this one with its script inlined via
# `code = <<-PY ... PY` instead of a separate file (contrast with
# examples/stripe-adapter.canoe.hcl, which uses `file = "..."`). Inlining
# is the better fit for a script this short — reach for `file = ...`
# once a script is long enough that editing it as a real `.py` file (with
# syntax highlighting, a linter, etc.) beats reading it as a heredoc.

adapter "heartbeat" {
  topic = "canoe.heartbeat"

  schedule {
    interval = "1m"
  }

  script {
    code = <<-PY
      # /// script
      # dependencies = ["confluent-kafka>=2.4"]
      # ///
      """Publishes one liveness record every run, so a consumer watching
      canoe.heartbeat can alert if this adapter's schedule ever stalls
      (no message for N minutes) independent of any real source system
      being reachable.
      """
      import json
      import os
      import socket
      import time

      from confluent_kafka import Producer

      producer = Producer({"bootstrap.servers": os.environ["CANOE_KAFKA_BOOTSTRAP"]})
      payload = {
          "adapter": os.environ["CANOE_ADAPTER_NAME"],
          "emitted_at": time.time(),
          "host": socket.gethostname(),
      }
      producer.produce(os.environ["CANOE_OUTPUT_TOPIC"], json.dumps(payload).encode("utf-8"))
      producer.flush(timeout=10)
    PY
  }
}
