# Minimal pipeline for smoke-testing the local `kind` dev stack (see
# `scripts/kind-up.sh`, `scripts/run-coordinator.sh`, and README.md's
# "Local dev stack" section) — one adapter, one `python` step, one sink,
# deliberately no `spark_sql`/`spark_dataframe` step so this doesn't also
# need Spark-on-K8s RBAC/driver setup just to prove the core loop works.
#
# Known limitation shared with every other example in this directory
# (orders.canoe.hcl, orders-uv-step.canoe.hcl, ...), not specific to this
# file: adapters haven't moved to the S3 + signal transport yet (that's
# Phase 2 of the migration described in README.md's "S3 + signal-driven
# pipeline transport" — adapters still publish to a Kafka topic). A
# pipeline's very first step reads from `pipeline.source` directly, and
# the compiler only resolves that to a real S3 path/signal name when it
# matches another compiled node's name — for an (unmigrated) adapter
# source, it doesn't, so it falls through to the literal source string
# unchanged. That means `validate`'s `CANOE_INPUT_PATH`/`S3BacklogInspector`
# below will currently be handed "local.smoke.raw" (a Kafka topic name,
# not a valid object-store URI) instead of a real S3 path, and will fail
# to parse it. This stack still validates everything up through the
# adapter firing and publishing to Kafka; validating a step actually
# consuming an adapter's output needs Phase 2 to land first.
#
# Compile with:
#   canoe-dslc examples/local-smoke.canoe.hcl -o examples/local-smoke.generated.yaml

storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

adapter "local_smoke_source" {
  topic = "local.smoke.raw"

  schedule {
    interval = "1m"
  }

  resources {
    cpu_request = "100m"
    mem_request = "128Mi"
  }

  script {
    code = <<-PY
      # /// script
      # dependencies = ["confluent-kafka>=2.4"]
      # ///
      """Publishes a handful of fake order records every run — enough to
      prove the adapter -> Kafka -> (eventually) S3 path is wired up
      against the local kind + Kafka + MinIO stack, without depending on
      any real external source system.
      """
      import json
      import os
      import time

      from confluent_kafka import Producer

      producer = Producer({"bootstrap.servers": os.environ["CANOE_KAFKA_BOOTSTRAP"]})
      for i in range(5):
          record = {
              "order_id": f"local-{int(time.time())}-{i}",
              "amount": 9.99 + i,
              "region": "US" if i % 2 == 0 else "EU",
          }
          producer.produce(os.environ["CANOE_OUTPUT_TOPIC"], json.dumps(record).encode("utf-8"))
      producer.flush(timeout=10)
    PY
  }
}

pipeline "local_smoke" {
  source = "local.smoke.raw"
}

step "validate" {
  engine = "python"
  input  = "local.smoke.raw"

  filter {
    predicate = "amount > 0"
  }

  scaling {
    min_replicas = 0
    max_replicas = 2
  }
}

sink "land" {
  input      = "validate"
  format     = "parquet"
  base_path  = "s3://canoe-lake"
  dataset_id = "local_smoke"
}
