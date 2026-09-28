# /// script
# dependencies = [
#   "stripe>=10",
#   "confluent-kafka>=2.4",
# ]
# ///
"""Pulls charges created since the last run and publishes them.

Reads the Stripe API key from the file the `secrets = ["stripe_api_key"]`
declaration in stripe-adapter.canoe.hcl mounts — one file per declared
secret, named after it, under `CANOE_SECRETS_PATH` (canoe always sets
this once at least one secret is declared; see "Secrets Manager
integration" in README.md). Also reads CANOE_KAFKA_BOOTSTRAP /
CANOE_OUTPUT_TOPIC, which the coordinator always sets for every adapter
run regardless of `secrets`.
"""
import json
import os
import time
from pathlib import Path

import stripe
from confluent_kafka import Producer

stripe.api_key = (Path(os.environ["CANOE_SECRETS_PATH"]) / "stripe_api_key").read_text().strip()
producer = Producer({"bootstrap.servers": os.environ["CANOE_KAFKA_BOOTSTRAP"]})
topic = os.environ["CANOE_OUTPUT_TOPIC"]

since = int(time.time()) - 600  # small overlap with the previous run
for charge in stripe.Charge.list(created={"gte": since}).auto_paging_iter():
    producer.produce(topic, json.dumps(charge.to_dict(), default=str).encode("utf-8"), key=charge.id)
    # Non-blocking: services delivery-report callbacks (and the internal
    # produce queue generally) as we go, rather than only at the final
    # flush() below. produce() alone already batches efficiently —
    # librdkafka accumulates messages per partition and sends them in
    # as few requests as `linger.ms`/`batch.size` allow, it does *not*
    # make one request per produce() call — but on a page count large
    # enough to fill the local queue (`queue.buffering.max.messages`,
    # default 100_000) before anything drains it, produce() would start
    # raising BufferError. poll(0) is what drains it.
    producer.poll(0)
producer.flush(timeout=30)
