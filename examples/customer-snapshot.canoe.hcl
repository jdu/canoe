# A small, standalone pipeline that exists to be *looked up* by
# examples/orders-with-customer-lookup.canoe.hcl — a completely separate
# `.hcl` file, compiled and deployed independently of this one. There's
# nothing special about this file otherwise; it reads like any other
# pipeline. What makes it a lookup target is simply that another file's
# `lookup { pipeline = "customers"; node = "snapshot" }` block names this
# pipeline and this step by name.
#
# IMPORTANT: a `lookup` resolves to <base_path>/<pipeline>/<version>/<node>
# — the same convention step-to-step `input =` paths already use. That
# convention only applies to *steps* (and adapters), not sinks: a `sink`
# writes to its own configured `base_path`/`dataset_id` instead (see
# README's "Datalake writes"). So the lookup target below is the "snapshot"
# step itself, not the "land_customers" sink further down — point a
# `lookup` at a step's name, never a sink's.

storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "customers" {
  source = "customers.raw"
}

# The lookup target: reachable at
# s3://canoe-lake/customers/v1/snapshot/ once this pipeline is running —
# exactly what `orders-with-customer-lookup.canoe.hcl`'s `lookup { pipeline
# = "customers"; node = "snapshot" }` block resolves to, via plain string
# formatting, without ever needing to parse this file.
#
# `email` is encrypted at rest here (see `encrypt` below) — demonstrates
# that encryption and `lookup` compose for free: a consumer reading this
# step via `lookup` gets `email` transparently decrypted, exactly the
# same as reading it via a same-file `input =` would, with nothing extra
# to declare on the consumer's side. See "Column-level field encryption"
# in README.md, and `CANOE_ENCRYPT_KEY_PII_V1` in the "Local dev stack"
# section for where the key itself comes from.
step "snapshot" {
  engine = "python"
  input  = "customers.raw"

  filter {
    predicate = "email != null"
  }

  rename {
    from = "cust_id"
    to   = "id"
  }

  encrypt {
    fields = ["email"]
    key    = "pii_v1"
  }
}

# A second, independent lookup target in this same file — used by
# orders-with-customer-lookup.canoe.hcl to show a step declaring
# *multiple* `lookup` blocks at once (confirmed as a requirement: a join
# step can pull in several external snapshots together, not just one).
# Reachable at s3://canoe-lake/customers/v1/region_rates/.
step "region_rates" {
  engine = "python"
  input  = "customers.raw"

  python {
    code = <<-PY
      """Stands in for a real tax-rate reference table keyed by region —
      the point is just that this is a second, differently-shaped
      dataset a downstream pipeline can join against independently of
      the "snapshot" step above.
      """
      RATES = {"US": 0.07, "EU": 0.20}


      def run(record: dict) -> dict:
          return {"region": record.get("region", "US"), "tax_rate": RATES.get(record.get("region", "US"), 0.0)}
    PY
  }
}

# Also landed in the datalake in its own right — independent of, and not
# required for, the lookup use case above (a lookup only ever reads
# "snapshot"'s own `latest.json`, not this sink's).
#
# `metadata` demonstrates column metadata at sink points: labels/
# descriptions/sensitivity tags for "id"/"email" live in
# customer-column-metadata.yaml (adjacent to this file, not inlined
# here), resolved by canoe-dslc at compile time and embedded as a
# `canoe.metadata` custom property per field in every flush's
# schema.avsc. Note `email`'s `sensitivity: "restricted"` metadata
# describes the *plaintext* semantics even though `email` is also
# `encrypt`ed above — the two features are independent (metadata isn't
# gated by encryption, or vice versa).
sink "land_customers" {
  input      = "snapshot"
  format     = "parquet"
  base_path  = "s3://canoe-lake"
  dataset_id = "customers"
  metadata   = "customer-column-metadata.yaml"
}

# Every node needs a downstream consumer *within this file* to compile —
# canoe-dsl's reachability check has no way to know
# orders-with-customer-lookup.canoe.hcl's `lookup` block reads
# "region_rates" too, since a lookup isn't part of this file's compiled
# graph at all. Sinking it here satisfies that check and is genuinely
# useful in its own right (a durable copy of the rates table), same as
# "land_customers" above.
sink "land_region_rates" {
  input      = "region_rates"
  format     = "parquet"
  base_path  = "s3://canoe-lake"
  dataset_id = "region_rates"
}
