# Demonstrates `lookup` blocks: joining this pipeline's own stream against
# data produced by a *wholly separate* pipeline in a *different* `.hcl`
# file — examples/customer-snapshot.canoe.hcl, compiled and deployed
# independently of this file. Unlike a normal `input = "..."` reference
# (same file, resolved through this compiler invocation's own node graph,
# incrementally tracked via checkpoint), a `lookup` is resolved purely by
# string-formatting a path (`<base_path>/<pipeline>/<version>/<node>`) and
# read as a point-in-time snapshot (that node's `latest.json`) each time —
# canoe-dsl never parses customer-snapshot.canoe.hcl to compile this file,
# and never checks that the lookup target actually exists. A typo here
# only surfaces at runtime, as a missing/empty `latest.json` fetch. See
# "Cross-pipeline lookups" in README.md.
#
#   canoe-dslc examples/customer-snapshot.canoe.hcl -o examples/customer-snapshot.generated.yaml
#   canoe-dslc examples/orders-with-customer-lookup.canoe.hcl -o examples/orders-with-customer-lookup.generated.yaml

storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders_enriched" {
  source = "orders.raw"
}

# Spark SQL join: each `lookup` block becomes its own temp view (named
# after the block's own label), read via `spark.read.parquet(...)` against
# that snapshot's current run — a batch read, not a streaming one, joined
# against the streaming "input" view exactly like a Structured Streaming
# stream-static join. Re-resolved fresh every micro-batch (see
# workers/spark-runner/canoe_spark_runner.py), so this always sees
# customer-snapshot.canoe.hcl's *latest* write, not a stale snapshot
# pinned at container startup.
step "join_customer_data" {
  engine = "spark_sql"
  input  = "orders.raw"

  lookup "customers" {
    pipeline = "customers"
    node     = "snapshot"
  }

  lookup "region_rates" {
    pipeline = "customers"
    node     = "region_rates"
  }

  spark_sql {
    sql = <<-SQL
      SELECT input.*,
             customers.email,
             region_rates.tax_rate
      FROM input
      LEFT JOIN customers    ON input.customer_id = customers.id
      LEFT JOIN region_rates ON input.region       = region_rates.region
    SQL
  }
}

# Same lookup target, python engine this time: a step with at least one
# `lookup` block gets `def run(record: dict, lookups: dict[str, list[dict]])`
# instead of the usual single-arg `def run(record: dict)` — `lookups`'s
# keys are the `lookup` blocks' own labels ("customers" here), each value
# the target's current snapshot rows. No incremental tracking on the
# lookup side: every invocation sees whatever customer-snapshot.canoe.hcl
# most recently wrote, refetched on a short cache TTL rather than per
# record (see workers/python-worker/canoe_worker/lookup.py).
step "flag_high_value_customers" {
  engine = "python"
  input  = "join_customer_data"

  lookup "customers" {
    pipeline = "customers"
    node     = "snapshot"
  }

  python {
    code = <<-PY
      """Flags an order if the customer it belongs to has no email on
      file in the customer snapshot — an example of a per-record check
      against externally-joined reference data, distinct from the
      column-level join `join_customer_data` already did above via SQL.
      """
      def run(record: dict, lookups: dict[str, list[dict]]) -> dict:
          customers_by_id = {c["id"]: c for c in lookups["customers"]}
          customer = customers_by_id.get(record.get("customer_id"))
          record["missing_customer_email"] = customer is None or not customer.get("email")
          return record
    PY
  }
}

sink "land" {
  input      = "flag_high_value_customers"
  format     = "parquet"
  base_path  = "s3://canoe-lake"
  dataset_id = "orders_enriched"
}
