# Demonstrates the orthogonal batch/streaming execution-mode split
# (`mode = "batch" | "streaming"`, independent of `engine`): a batch-mode
# `spark_sql` step (a one-shot Job draining S3 backlog via
# `spark.read.parquet`, not the always-on streaming Deployment),
# `depends_on`/`depends_on_complete` gating a batch-mode step's own
# Job-firing reconcile loop, a regular `step` block declaring
# `mode = "streaming"` (the same Kafka-topic-transport runtime
# `dynamic_route`'s internal machinery already uses, now reachable as a
# first-class DSL choice on any step, not only through dynamic_route), and
# an `adapter` with `mode = "batch"` — the same vocabulary extended to
# adapters, so a script can write Parquet straight to S3 via the
# `canoe_adapter_output` helper instead of producing to Kafka first.
#
#   canoe-dslc examples/orders-execution-modes.canoe.hcl -o examples/orders-execution-modes.generated.yaml

storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

adapter "orders_modes_adapter" {
  topic = "orders.modes.raw"

  schedule {
    interval = "1m"
  }

  script {
    code = <<-PY
      # /// script
      # dependencies = ["confluent-kafka>=2.4"]
      # ///
      import json
      import os
      import time

      from confluent_kafka import Producer

      producer = Producer({"bootstrap.servers": os.environ["CANOE_KAFKA_BOOTSTRAP"]})
      order = {"id": int(time.time()), "amount": 42.0, "region": "US"}
      producer.produce(os.environ["CANOE_OUTPUT_TOPIC"], json.dumps(order).encode("utf-8"))
      producer.flush(timeout=10)
    PY
  }
}

pipeline "orders_modes" {
  source = "orders.modes.raw"
}

# `mode = "batch"` is already the default for engine = "python" — spelled
# out here anyway, for contrast with `rollup_streaming` below. S3+signal
# transport, one-shot Job per execution, draining backlog via
# `CANOE_RUN_ONCE`.
step "validate" {
  engine = "python"
  mode   = "batch"
  input  = "orders.modes.raw"

  filter {
    predicate = "amount > 0"
  }
}

# A batch-mode spark_sql step: `mode = "batch"` here is NOT the default
# for engine = "spark_sql" (spark_sql defaults to "streaming", preserving
# every pipeline written before this attribute existed) — declaring it
# explicitly is what makes this a one-shot Job draining S3 backlog via
# `spark.read.parquet` across every unprocessed run since its own
# checkpoint, instead of a long-running `spark.readStream` query.
#
# `spark { conf = { ... } }` passes arbitrary extra `spark-submit --conf`
# entries straight through, keyed by Spark's own dotted setting names —
# deliberately not given canoe-invented aliases (no `partitions` shorthand
# for `spark.sql.shuffle.partitions`), so this stays a plain passthrough
# to whatever Spark itself calls a setting rather than a second
# vocabulary to keep in sync with Spark's own docs. An object literal,
# not a nested block, since HCL body-attribute identifiers can't contain
# dots the way Spark's setting names always do.
step "spark_batch_enrich" {
  engine = "spark_sql"
  mode   = "batch"
  input  = "validate"

  spark {
    executor_instances = 3
    conf = {
      "spark.sql.shuffle.partitions"    = "200"
      "spark.sql.files.maxPartitionBytes" = "134217728"
    }

    # Per-step override of the coordinator's global `spark.event_log`
    # default (see examples/global-config.yaml, loaded via
    # `canoe-coordinator --global-config examples/global-config.yaml`) —
    # this whole `event_log { ... }` block is optional; most steps
    # declare nothing here and just inherit whatever the global config
    # sets. Opting this one step out even though the example global
    # config enables event logging everywhere else: `enabled = false`
    # here wins over the global default field-by-field (see
    # `canoe-coordinator`'s `activation::resolve_event_log`) — `base_path`
    # is left unset since it's irrelevant once this step's own `enabled`
    # is false.
    event_log {
      enabled = false
    }
  }

  spark_sql {
    sql = "SELECT *, amount * 1.1 AS amount_with_tax FROM input"
  }
}

sink "land" {
  input      = "spark_batch_enrich"
  format     = "parquet"
  base_path  = "s3://canoe-lake"
  dataset_id = "orders_modes"
}

# depends_on / depends_on_complete: this step's own batch Job won't fire
# until "spark_batch_enrich" has both been applied (depends_on — reuses
# the coordinator's existing `ctx.activated`) and actually *completed* at
# least once (depends_on_complete — reuses `ctx.completed`, populated
# when a batch-mode step's Job succeeds). Only meaningful because
# spark_batch_enrich is batch-mode — a one-shot Job genuinely has a
# "done"; canoe-dsl rejects depends_on_complete naming a streaming-mode
# step like "rollup_streaming" below, since it never completes.
step "notify_after_first_batch" {
  engine = "python"
  input  = "validate"
  depends_on          = ["spark_batch_enrich"]
  depends_on_complete = ["spark_batch_enrich"]

  python {
    code = <<-PY
      def run(record: dict) -> dict:
          record["notified_after_batch_enrich"] = True
          return record
    PY
  }
}

sink "land_notify" {
  input      = "notify_after_first_batch"
  format     = "parquet"
  base_path  = "s3://canoe-lake"
  dataset_id = "orders_modes_notify"
}

# A regular `step` block declaring `mode = "streaming"` explicitly —
# Kafka-topic transport, a genuinely continuous `spark.readStream` query
# (KafkaSparkSqlStep), the exact same runtime `dynamic_route`'s internal
# per-discovered-value pods already use, now reachable on an ordinary
# step rather than only through dynamic_route's own machinery. Reads
# directly from the adapter's own Kafka topic (`source`), since a
# streaming-mode step can only wire to/from other Kafka-transport nodes.
step "rollup_streaming" {
  engine = "spark_sql"
  mode   = "streaming"
  input  = "orders.modes.raw"

  spark_sql {
    sql = "SELECT region, sum(amount) AS total_amount FROM input GROUP BY region"
  }
}

# NOTE on realism (same caveat examples/orders-rollup.canoe.hcl already
# documents for its own Kafka-transport spark_sql step): wiring a
# streaming-mode step's Kafka-topic output directly into a sink (which
# always expects an S3 path) isn't actually semantically valid at run
# time — canoe-dsl doesn't reject the transport mismatch at compile time
# either, a known, already-documented gap (see `StepConfig::input_ref`'s
# docs: "real mixed-transport rejection is expected in a later phase").
# Wired up here only so "rollup_streaming" has a downstream consumer and
# the file compiles, same accepted pattern examples/orders.canoe.hcl's
# own streaming spark_sql steps already rely on; a real pipeline would
# feed a streaming step's output into another Kafka-transport node
# instead (another streaming step, or eventually `dynamic_step`).
sink "land_rollup_streaming" {
  input      = "rollup_streaming"
  format     = "parquet"
  base_path  = "s3://canoe-lake"
  dataset_id = "orders_modes_rollup"
}

# A `mode = "batch"` adapter: writes Parquet straight to S3
# (`output_path`, resolved to `<base_path>/<version>/<name>/` — an
# adapter has no owning pipeline the way a step does, so there's no
# `<pipeline>` segment to include) instead of producing to a Kafka topic.
# `topic` is rejected alongside `mode = "batch"` (S3 + Kafka transport
# can't coexist on one adapter, same discipline `lookup`/`encrypt`
# validation already follows elsewhere) — `output {}` configures the
# batch_size/flush_interval_secs/partition_by canoe passes to the script
# via CANOE_OUTPUT_BATCH_SIZE/CANOE_OUTPUT_FLUSH_INTERVAL_SECS/
# CANOE_OUTPUT_PARTITION_BY, same env vars a batch-mode step already gets.
# The script owns its entire loop, same as the streaming case above — it
# calls the `canoe_adapter_output` helper baked into the adapter-runner
# image at `/opt/canoe/` rather than canoe transparently capturing rows.
adapter "orders_modes_batch_adapter" {
  mode = "batch"

  schedule {
    interval = "1m"
  }

  output {
    batch_size          = 500
    flush_interval_secs = 30
    partition_by         = ["region"]
  }

  script {
    code = <<-PY
      # /// script
      # dependencies = ["boto3", "pyarrow", "confluent-kafka>=2.4"]
      # ///
      import sys
      import time

      sys.path.insert(0, "/opt/canoe")
      import canoe_adapter_output as output

      order = {"id": int(time.time()), "amount": 42.0, "region": "US"}
      output.write_batch([order])
    PY
  }
}

# The adapter's own name resolves to its real S3 output path here — see
# `compiler.rs`'s `compile_graph` batch-adapter seeding of
# `name_to_ref`/`name_to_signal`, and `resolve_pipeline_source` (a
# pipeline's `source` gets the same by-name resolution a downstream
# step's `input` does, since a batch adapter has no topic literal to
# match against the way a streaming adapter's `source = <topic>` does).
pipeline "orders_modes_batch" {
  source = "orders_modes_batch_adapter"
}

step "batch_adapter_enrich" {
  engine = "python"
  input  = "orders_modes_batch_adapter"

  python {
    code = <<-PY
      def run(record: dict) -> dict:
          record["seen_batch_adapter"] = True
          return record
    PY
  }
}

sink "land_batch_adapter" {
  input      = "batch_adapter_enrich"
  format     = "parquet"
  base_path  = "s3://canoe-lake"
  dataset_id = "orders_modes_batch_adapter"
}
