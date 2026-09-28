# Same "orders" pipeline as examples/orders-pipeline.yaml, but authored
# declaratively and with a region-based branch. Compile it with:
#
#   canoe-dslc examples/orders.canoe.hcl > examples/orders-pipeline.generated.yaml
#
# Every block here — pipeline/step/route/merge/sink/task — is a discrete,
# root-level declaration referencing the others **by name**, Terraform
# resource style: nothing is nested inside a `pipeline` block. A
# pipeline's own steps/routes/sinks aren't listed anywhere explicitly —
# canoe-dslc discovers them by tracing every `input = "..."` reference
# reachable from `source`, so a pipeline is really just its entry
# point and activation policy. The compiler still assigns every internal
# Kafka topic for you; only `source` and each `sink`'s `base_path`
# are real, external names.

# Global, file-wide durability/debugging policy for every internal
# step-to-step topic this file's pipelines produce (never the external
# `source` — canoe doesn't consider that one its own to manage).
# Both `retention` and `archive` are independently optional; either can
# be omitted (or the whole `topics` block dropped) to leave that concern
# alone. See README.md's "Durability and archival" section.
topics {
  retention {
    duration = "7d"
  }

  archive {
    base_path = "s3://canoe-archive"
    interval  = "10m"
  }
}

storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders" {
  source = "orders.raw"
}

# Declarative ops: no python/spark code written by hand.
step "validate" {
  engine = "python"
  input  = "orders.raw"

  filter {
    predicate = "amount > 0"
  }

  rename {
    from = "cust_id"
    to   = "customer_id"
  }

  drop {
    fields = ["internal_debug_flag"]
  }

  # Normalize a coded field via a lookup table kept in its own file
  # (examples/mappings/payment_method.json), resolved relative to this
  # .hcl file at compile time — canoe-dslc reads and embeds it as a
  # Python dict literal, same "file = ..." convention as a step's
  # escape-hatch code. Values not found in the mapping fall back to
  # `default` rather than erroring, since new payment method codes
  # shouldn't break every downstream chain until this file catches up.
  map_values {
    field   = "payment_method_code"
    to      = "payment_method"
    default = "other"
    mapping {
      file = "mappings/payment_method.json"
    }
  }

  scaling {
    min_replicas          = 2
    max_replicas           = 20
    target_lag_per_replica = 5000
  }
}

# Branch by region: each branch is its own filtered consumer group
# reading "validate"'s output, so this reads top-to-bottom like a
# flowchart — no manual topic wiring.
route "split_by_region" {
  input = "validate"

  branch "us" {
    when = "region = 'US'"
  }
  branch "eu" {
    when = "region = 'EU'"
  }
  default {}
}

step "enrich_us" {
  engine = "spark_sql"
  input  = "split_by_region.us"

  with_field {
    name       = "region_tax_rate"
    expression = "0.07"
  }

  with_field {
    name       = "processed_date"
    expression = "date_format(current_timestamp(), 'yyyy-MM-dd')"
  }
}

step "enrich_eu" {
  engine = "spark_sql"
  input  = "split_by_region.eu"

  with_field {
    name       = "region_tax_rate"
    expression = "0.20"
  }

  with_field {
    name       = "processed_date"
    expression = "date_format(current_timestamp(), 'yyyy-MM-dd')"
  }
}

step "enrich_other" {
  engine = "spark_sql"
  input  = "split_by_region.default"

  with_field {
    name       = "region_tax_rate"
    expression = "0.0"
  }

  with_field {
    name       = "processed_date"
    expression = "date_format(current_timestamp(), 'yyyy-MM-dd')"
  }
}

# The three regional branches converge back into one topic here; no pod
# runs for a merge, it's just shared output topic naming.
merge "enriched" {
  inputs = ["enrich_us", "enrich_eu", "enrich_other"]
}

# Firing this signal after a successful flush lets other pipelines start
# when this write lands — see examples/orders-rollup.canoe.hcl.
sink "land" {
  input        = "enriched"
  format       = "parquet"
  base_path    = "s3://canoe-lake"
  dataset_id   = "orders"
  partition_by = ["processed_date"]
  signal       = "orders_landed"

  # After every flush, folds this sink's run_id=* history into a
  # deduplicated checkpoint=<ulid>/ view keyed by customer_id — later
  # runs win for orders sharing a customer, same "last write wins"
  # semantics as any changelog compaction. See "Checkpoints" in
  # README.md.
  checkpoint {
    keys = ["customer_id"]
  }

  # Independently of the checkpoint above, also rolls up a full Type-2
  # history per customer: only `payment_method` is watched, so a
  # customer's row only versions when *that* field actually changes,
  # not on every order (which would version on every single run). See
  # "Slowly changing dimensions" in README.md.
  scd {
    keys         = ["customer_id"]
    track_fields = ["payment_method"]
  }
}

# A one-shot side effect that isn't part of the data flow at all — it
# doesn't read from or write to any topic, it just needs to happen once
# "enrich_us" is up. `depends_on` names a step, pipeline, dynamic_route,
# or another task; here it's a step. See "Tasks" in README.md.
task "notify_ops" {
  depends_on = ["enrich_us"]

  script {
    code = <<-PY
      # /// script
      # dependencies = ["requests"]
      # ///
      """Pings an internal webhook once enrich_us's Deployment has been
      applied, so on-call knows the US-region enrichment path is live
      after a deploy. Runs once and exits — same one-shot Job shape as
      an adapter, just triggered by a dependency instead of a schedule.
      """
      import os

      import requests

      requests.post(
          os.environ["OPS_WEBHOOK_URL"],
          json={"event": "orders.enrich_us.deployed"},
          timeout=10,
      )
    PY
  }
}
