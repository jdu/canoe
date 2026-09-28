# Dynamic version of orders.canoe.hcl's `split_by_region` route: instead
# of listing known region codes up front (`branch "us" { when = ... }`),
# this splits by whatever values `region` actually has — an
# Airflow-`.expand()`-style dynamic fan-out, not a fixed set of branches.
#
# The coordinator discovers each distinct `region` value at run time and
# provisions a dedicated Kafka topic + K8s Deployment (running `task`)
# for it the first time it's seen — see "Dynamic splitting" in README.md
# and crates/canoe-coordinator/src/dynamic_router.rs. Contrast with
# orders.canoe.hcl's `route`, which is the right tool when you *do* know
# the branch values ahead of time (compiles to less runtime machinery,
# and each branch can have its own distinct predicate/logic rather than
# one template applied uniformly).

storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders_dynamic" {
  source = "orders.raw"
}

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
}

dynamic_route "split_by_region" {
  input = "validate"
  field = "region"

  # Hard cap on distinct values this will ever provision a Deployment
  # for — required, not optional: an unbounded version of this risks
  # runaway K8s object creation on a high-cardinality or adversarial
  # field, the same reason Airflow's .expand() has max_map_length.
  # Defaults to 1024 if omitted.
  max_branches = 50

  # Applied to every discovered region: same declarative ops (or
  # engine/escape-hatch) vocabulary as an ordinary step, just templated
  # rather than input/output-topic-specific — the coordinator fills
  # those in per value when it instantiates a real step from this.
  # (`task` here is this block's own nested template, unrelated to a
  # root-level `task { depends_on = [...] }` block elsewhere in this file.)
  task {
    engine = "spark_sql"
    with_field {
      name       = "region_tag"
      expression = "region"
    }
    with_field {
      name       = "processed_date"
      expression = "date_format(current_timestamp(), 'yyyy-MM-dd')"
    }
  }

  # Provisioned once, up front (not dynamically): catches anything past
  # max_branches, plus any record whose region is missing/null. Without
  # this, records that would overflow the cap are just dropped (with a
  # logged warning) instead.
  overflow {
    engine = "python"
    with_field {
      name       = "region_tag"
      expression = "'unclassified'"
    }
  }
}

# Every value's task (and overflow) writes to the same implicit merged
# topic — there's no way to `merge { inputs = [...] }` branches whose
# names aren't known statically, so a dynamic_route merges its own
# output. Downstream just reads it by the dynamic_route's own name,
# same as reading from a `merge`.
sink "land" {
  input        = "split_by_region"
  format       = "parquet"
  base_path    = "s3://canoe-lake"
  dataset_id   = "orders_dynamic"
  partition_by = ["processed_date"]
}
