# Started by the "orders" pipeline's `land` sink (examples/orders.canoe.hcl),
# not run continuously from coordinator startup: the `schedule` block below
# names the same signal that sink fires. Until that signal is seen, the
# coordinator creates no Deployment and runs no consumer for this pipeline
# at all — see crates/canoe-coordinator/src/signals.rs.
#
# Once activated it behaves like any other chain (its own lag-driven
# autoscaling takes over from there); it does not automatically go back to
# sleep. See the DSL section of README.md for that known limitation.
#
# NOTE on realism: this step reads a Kafka topic like every other
# spark_sql step, purely to demonstrate signal/schedule wiring with the
# existing streaming runner. A `land` sink writes Parquet to S3, not back
# to a Kafka topic — nothing actually publishes to "orders.enriched" here.
# A real "reprocess what was just written to the datalake" rollup would
# need a batch-read Spark entrypoint (`spark.read.parquet(path)` against
# the sink's own `path`) that `workers/spark-runner` doesn't have yet; see
# README's "what's stubbed" list.

pipeline "orders_rollup" {
  schedule {
    signal = "orders_landed"
  }

  source = "orders.enriched"
}

step "daily_totals" {
  engine = "spark_sql"
  input  = "orders.enriched"

  spark_sql {
    sql = <<-SQL
      SELECT processed_date,
             region_tax_rate,
             sum(amount) AS total_amount,
             count(*)    AS order_count
      FROM input
      GROUP BY processed_date, region_tax_rate
    SQL
  }
}

sink "land_rollup" {
  input      = "daily_totals"
  format     = "parquet"
  base_path  = "s3://canoe-lake"
  dataset_id = "orders_rollup"
}
