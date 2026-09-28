# Demonstrates `batch` on a `python { code = ... }` escape-hatch step:
# batching lets a step amortize a genuinely per-call cost (here, one FX
# rate lookup per batch instead of one per order) across many records at
# once. `batch` only works with an escape-hatch step's raw
# `def run(records: list[dict]) -> list[dict]` — declarative ops (filter,
# rename, ...) always compile to a fixed one-record-at-a-time function, so
# canoe-dsl rejects `batch` on them. See "Batching" in README.md.
#
#   canoe-dslc examples/orders-batch.canoe.hcl > examples/orders-batch.generated.yaml

storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders_batch" {
  source = "orders.raw"
}

step "enrich_with_fx_rate" {
  engine = "python"
  input  = "orders.raw"

  python {
    code = <<-PY
      """Looks up one currency's exchange rate per *batch*, not per
      order, and applies it to every order in that batch. The lookup
      below stands in for a real HTTP call to a rates API — exactly
      the kind of per-call overhead `batch` exists to amortize; a
      `def run(record)` step would pay that cost on every single
      order instead of once per up-to-500.
      """
      def fetch_fx_rate(currency: str) -> float:
          return {"eur": 0.92, "gbp": 0.79}.get(currency, 1.0)


      def run(records: list[dict]) -> list[dict]:
          currency = records[0].get("currency", "usd") if records else "usd"
          rate = fetch_fx_rate(currency)
          for record in records:
              record["amount_usd"] = round(record["amount"] * rate, 2)
          return records
    PY
  }

  # Calls `run` with up to 500 buffered records, or whatever's
  # accumulated after 5s, whichever comes first.
  batch {
    size                 = 500
    flush_interval_secs  = 5
  }

  scaling {
    min_replicas = 1
    max_replicas = 5
  }
}

sink "land" {
  input      = "enrich_with_fx_rate"
  format     = "parquet"
  base_path  = "s3://canoe-lake"
  dataset_id = "orders_batch"
}
