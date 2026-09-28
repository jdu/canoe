# A `python { code = ... }` / `python { file = ... }` step's function
# runs inside workers/python-worker's shared, long-lived process — so
# it's limited to whatever third-party packages that image happens to
# carry. `engine = "uv"` is for when a step genuinely needs something
# that image doesn't (and installing it there for one step isn't the
# right tradeoff): its script is a full standalone `uv run` program —
# same PEP 723 "own header declares its own dependencies" convention as
# an adapter's script — that owns its entire Kafka consume/produce loop
# itself, running continuously in a K8s Deployment rather than a one-shot
# Job. See "Custom scripts in pipelines" in README.md and
# workers/uv-step-worker/run.py.
#
# Same `code = ...` / `file = ...` choice every other DSL escape hatch
# offers: `geocode` below reads its script from its own file
# (examples/steps/geocode_enrich.py, easier to edit with real syntax
# highlighting/linting once a script gets long), `dedupe` inlines a
# shorter one directly.

storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders_uv" {
  source = "orders.raw"
}

step "validate" {
  engine = "python"
  input  = "orders.raw"
  filter {
    predicate = "amount > 0"
  }
}

step "geocode" {
  engine = "uv"
  input  = "validate"

  script {
    file = "steps/geocode_enrich.py"
  }

  # Plain config the script reads via os.environ — GEOCODE_API_URL/
  # GEOCODE_TIMEOUT_SECS in this case (see geocode_enrich.py) — declared
  # here instead of hardcoded in the script, since this is where every
  # other knob for this step already lives. Values are coerced to
  # strings (K8s env vars are always strings anyway), so an unquoted
  # number like `10` below works the same as a quoted one.
  env {
    GEOCODE_API_URL     = "https://geocode.example.com/v1/search"
    GEOCODE_TIMEOUT_SECS = 10
  }

  # min_replicas = 0 means this step's Deployment scales all the way
  # down to zero pods when its input topic has no lag, and the
  # coordinator's own autoscale reconcile loop (which keeps running even
  # at zero replicas — it reads lag with its own throwaway consumer, not
  # this step's) scales it back up once geocoding work actually shows up.
  # See "Scale to zero" in README.md.
  scaling {
    min_replicas = 0
    max_replicas = 5
  }

  resources {
    cpu_request = "250m"
    mem_request = "384Mi"
  }
}

step "dedupe" {
  engine = "uv"
  input  = "geocode"

  script {
    code = <<-PY
      # /// script
      # dependencies = ["confluent-kafka>=2.4"]
      # ///
      """Drops orders whose id was already seen by this pod, keeping an
      in-memory ring buffer of recent ids rather than anything
      persistent — good enough for collapsing retried/duplicate
      produces from upstream, not a substitute for exactly-once
      semantics.

      S3 + signal transport (see README.md's "S3 + signal-driven
      pipeline transport"): a `uv` step owns its whole loop, so it's
      responsible for its own checkpointing/listing/writing against the
      CANOE_INPUT_PATH/CANOE_OUTPUT_PATH env vars below, same as an
      adapter script owns its own produce loop. This sketch shows the
      shape; a real script would use boto3 (or any S3 client) plus
      pyarrow to actually read/write Parquet.
      """
      import json
      import os

      from confluent_kafka import Consumer, Producer

      SEEN_MAX = 100_000
      seen_ids: set[str] = set()
      seen_order: list[str] = []

      bootstrap = os.environ["CANOE_KAFKA_BOOTSTRAP"]
      input_path = os.environ["CANOE_INPUT_PATH"]
      input_signal = os.environ["CANOE_INPUT_SIGNAL"]
      output_path = os.environ["CANOE_OUTPUT_PATH"]
      checkpoint_path = os.environ["CANOE_CHECKPOINT_PATH"]

      consumer = Consumer({
          "bootstrap.servers": bootstrap,
          "group.id": f"canoe-uv-dedupe-{input_signal}",
          "enable.auto.commit": True,
          "auto.offset.reset": "latest",
      })
      producer = Producer({"bootstrap.servers": bootstrap})
      consumer.subscribe(["_canoe.signals"])

      def process_new_runs():
          # List run_id=* directories under input_path newer than
          # checkpoint_path (mirrors crates/canoe-sink/src/checkpoint.rs's
          # Checkpointer algorithm), dedupe each run's rows against
          # seen_ids, write survivors to output_path/run_id=<ulid>/, then
          # advance checkpoint_path and publish a signal under this
          # step's own name — left as a sketch here.
          pass

      while True:
          msg = consumer.poll(timeout=5.0)
          if msg is not None and not msg.error():
              signal = json.loads(msg.value())
              if signal.get("signal") != input_signal:
                  continue
          # Woken by a signal, or just the periodic self-heal timeout —
          # either way, go list what's actually new under input_path
          # rather than trusting the signal payload itself.
          process_new_runs()
    PY
  }
}

sink "land" {
  input      = "dedupe"
  format     = "parquet"
  base_path  = "s3://canoe-lake"
  dataset_id = "orders_uv"
}
