# /// script
# dependencies = [
#   "confluent-kafka>=2.4",
#   "requests>=2.31",
# ]
# ///
"""Looks up a lat/lon for each order's shipping city and republishes it.

`requests` isn't installed in workers/python-worker's shared image (and
isn't worth adding there just for this one step) — this script declares
it itself via the header above, and `uv run` resolves an ephemeral venv
for it before this even starts. Reads CANOE_INPUT_TOPIC/CANOE_OUTPUT_TOPIC/
CANOE_KAFKA_BOOTSTRAP, set by the coordinator for every `engine = "uv"`
step the same way CANOE_OUTPUT_TOPIC is set for an adapter — see
workers/uv-step-worker/run.py.

Unlike a `python { code = ... }` step's `def run(record): ...` contract
(called once per message from inside the long-lived python-worker
process), this script owns its entire consume/produce loop itself: it's
the whole program, not a function plugged into one.

GEOCODE_API_URL/GEOCODE_TIMEOUT_SECS come from this step's `env { ... }`
block in orders-uv-step.canoe.hcl, not a hardcoded constant here — the
same script can point at a staging vs. prod geocoder, or have its timeout
tuned, purely by editing the HCL, no code change or rebuild needed. See
"Custom scripts in pipelines" / "env vars" in README.md.
"""
import json
import os

import requests
from confluent_kafka import Consumer, Producer

GEOCODE_CACHE: dict[str, tuple[float, float]] = {}
GEOCODE_API_URL = os.environ.get("GEOCODE_API_URL", "https://geocode.example.com/v1/search")
GEOCODE_TIMEOUT_SECS = float(os.environ.get("GEOCODE_TIMEOUT_SECS", "5"))


def geocode(city: str) -> tuple[float, float] | None:
    if city in GEOCODE_CACHE:
        return GEOCODE_CACHE[city]
    resp = requests.get(
        GEOCODE_API_URL,
        params={"q": city, "limit": 1},
        timeout=GEOCODE_TIMEOUT_SECS,
    )
    resp.raise_for_status()
    results = resp.json().get("results", [])
    if not results:
        return None
    coords = (results[0]["lat"], results[0]["lon"])
    GEOCODE_CACHE[city] = coords
    return coords


def main() -> None:
    bootstrap = os.environ["CANOE_KAFKA_BOOTSTRAP"]
    input_topic = os.environ["CANOE_INPUT_TOPIC"]
    output_topic = os.environ["CANOE_OUTPUT_TOPIC"]
    group_id = os.environ["CANOE_CONSUMER_GROUP"]

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    producer = Producer({"bootstrap.servers": bootstrap})
    consumer.subscribe([input_topic])

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            continue

        record = json.loads(msg.value())
        city = record.get("shipping_city")
        if city:
            coords = geocode(city)
            if coords:
                record["shipping_lat"], record["shipping_lon"] = coords

        producer.produce(output_topic, json.dumps(record).encode("utf-8"), key=msg.key())
        producer.flush(timeout=10)
        consumer.commit(msg, asynchronous=False)


if __name__ == "__main__":
    main()
