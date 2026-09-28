# Tutorial: building a pipeline in canoe

This is a hands-on walkthrough for someone new to canoe. It builds one
pipeline from scratch, a piece at a time, and along the way exercises
every major feature currently implemented: adapters, declarative and
custom steps, batching, routing, cross-pipeline lookups, column
encryption, secrets, column metadata, and checkpoints/SCD views.

It's deliberately narrower and more guided than `README.md`, which is
the complete reference — reach for that once this gets you oriented, or
whenever a section here says "see README for the full story."

## The mental model, in one paragraph

A canoe pipeline is a named graph of **steps** wired together by name
(`input = "some_other_step"`), rooted at a **source** (usually an
**adapter** — a scheduled job that pulls data from somewhere external)
and ending at one or more **sinks** (where data lands durably in a
datalake, as Parquet on S3). Steps process data in bounded batches, not
an unbounded stream: every write is its own immutable **run**
(`run_id=<ulid>/`), and a **signal** on Kafka (tiny — a pointer, never
the actual row data) tells a downstream step "new data is ready" so it
can wake up, process it, and go back to idle. You write pipelines in an
HCL-based **DSL** (`.canoe.hcl` files), which `canoe-dslc` compiles to
the plain YAML the coordinator actually reads.

## Before you start

You don't need a real AWS account or Kubernetes cluster to follow along
— canoe ships a disposable local stack on [kind](https://kind.sigs.k8s.io/)
with Kafka and a MinIO (S3-compatible) store. From the repo root:

```
scripts/kind-up.sh
```

This builds the worker images, loads them into the cluster, and stands
up Kafka + MinIO. You'll use `canoe-dslc` throughout this tutorial to
compile `.hcl` files, and `scripts/run-coordinator.sh` to actually run a
compiled pipeline against the local stack. See "Local dev stack" in
README.md for the full picture (what each script does, how to inspect
MinIO, how to tear down). Nothing in this tutorial requires the local
stack to be running until "Running it for real" below — the earlier
sections are about writing and compiling `.hcl`, which works with no
cluster at all.

## Step 1: an adapter — getting data in

Data enters canoe through an **adapter**: a scheduled script that pulls
from some external source and publishes to a topic. Create
`tutorial-orders.canoe.hcl`:

```hcl
adapter "orders_adapter" {
  topic = "orders.raw"

  schedule {
    interval = "1m"
  }

  script {
    code = <<-PY
      # /// script
      # dependencies = ["confluent-kafka>=2.4"]
      # ///
      """Stands in for a real source system — a payments API, a
      database CDC feed, whatever actually produces order events in
      your world. In a real adapter this would call out to that
      system; here it just fabricates one row per run so the rest of
      this tutorial has something to process.
      """
      import json
      import os
      import time

      from confluent_kafka import Producer

      producer = Producer({"bootstrap.servers": os.environ["CANOE_KAFKA_BOOTSTRAP"]})
      order = {
          "id": int(time.time()),
          "cust_id": "cust-42",
          "amount": 19.99,
          "region": "US",
          "email": "person@example.com",
      }
      producer.produce(os.environ["CANOE_OUTPUT_TOPIC"], json.dumps(order).encode("utf-8"))
      producer.flush(timeout=10)
    PY
  }
}
```

An adapter's script owns its entire loop — it's a standalone `uv run`
program (the `# /// script` header is
[PEP 723](https://peps.python.org/pep-0723/) inline dependency
metadata), not a function canoe calls into. `CANOE_KAFKA_BOOTSTRAP` and
`CANOE_OUTPUT_TOPIC` are set for you; the script just needs to produce
JSON to that topic. See "Adapters" in README.md for the `cron`-schedule
alternative to `interval`, and `examples/stripe-adapter.canoe.hcl` for a
closer-to-real-world example that reads from an actual HTTP API.

This alone doesn't compile yet — canoe-dsl validates that every node
(the adapter's output topic included) is actually consumed by
*something*. On to the next piece.

## Step 2: your first step — declarative ops

A `step` reads from a named producer and writes its own output. Add
this above or below the adapter block, in the same file:

```hcl
storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "orders" {
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
```

`storage { base_path; version }` is file-wide: every step/adapter's
resolved S3 path is `<base_path>/<pipeline>/<version>/<name>/`. `pipeline
"orders" { source = "orders.raw" }` declares that this pipeline starts
at the adapter's topic — nothing else about a pipeline's shape is
declared explicitly; canoe-dsl discovers every step reachable by
following `input =` references from `source` outward.

`filter`/`rename` are two of several **declarative ops** — no Python
code written by hand. They compile to a generated `def run(record):
...` function. The full op vocabulary (`drop`, `map_values`,
`with_field`, ...) is in "The DSL" in README.md; `examples/orders.canoe.hcl`
demonstrates most of them, including a lookup-table-driven `map_values`.

## Step 3: a sink — landing the data

```hcl
sink "land" {
  input      = "validate"
  format     = "parquet"
  base_path  = "s3://canoe-lake"
  dataset_id = "orders"
  partition_by = ["region"]
}
```

Every sink flush is its own immutable run:
`s3://canoe-lake/dataset_id=orders/run_id=<ulid>/region=US/part-*.parquet`,
plus a `schema.avsc` (an Avro schema document — inspectable JSON,
diffable across runs) written alongside it in the same run directory,
and a `latest.json` pointer at the dataset root so a reader who just
wants "the current data" doesn't need to list and sort `run_id=`
directories itself.

## Checkpoint: compile it

You should now have one file with four blocks: `adapter`, `storage`,
`pipeline`, `step "validate"`, `sink "land"`. Compile it:

```
cargo run -p canoe-dslc -- tutorial-orders.canoe.hcl -o tutorial-orders.generated.yaml
```

If that succeeds, you have a real, deployable pipeline — a schedule that
fabricates one order every minute, a step that drops non-positive
amounts and renames a field, and a sink that lands the result as
partitioned Parquet. Everything below adds to this same file.

## Running it for real

With `scripts/kind-up.sh` already run (see "Before you start"):

```
scripts/run-coordinator.sh tutorial-orders.generated.yaml
```

This runs the coordinator on your own machine (not in the cluster —
much faster to iterate on than rebuilding a container image every time)
against the kind cluster's Kafka/MinIO. Leave it running; in another
terminal:

```
kubectl get pods -n canoe-workers          # the adapter's Job, then validate's step pod, as they run
mc alias set local http://localhost:9000 minioadmin minioadmin
mc ls local/canoe-lake/dataset_id=orders/  # each run_id= directory as it lands
```

Ctrl-C the coordinator, `scripts/kind-down.sh` when you're done for the
day (or leave it up — `kind-up.sh` is idempotent-ish, it'll just reuse
what's there).

## Watching it with the TUI

`canoe-tui` polls the coordinator's status endpoint and shows every
pipeline/adapter's live state — replica counts, backlog, circuit-breaker
status — without you needing to `kubectl get pods` by hand:

```
cargo run -p canoe-tui -- --coordinator-url http://localhost:9095
```

Arrow keys / `j`/`k` to move, `Enter` to drill into a pipeline's steps,
`Esc` to go back, `q` to quit.

## Step 4: custom code — the escape hatch

Declarative ops cover common cases; for anything else, a step can carry
its own Python function instead. Add a second step between `validate`
and `land`:

```hcl
step "enrich" {
  engine = "python"
  input  = "validate"

  python {
    code = <<-PY
      def run(record: dict) -> dict:
          record["amount_cents"] = round(record["amount"] * 100)
          return record
    PY
  }
}
```

...and repoint the sink: `sink "land" { input = "enrich"; ... }`. A
`python { code = ... }` block can also be `python { file = "some_file.py" }`
— resolved relative to the `.hcl` file at compile time, easier to edit
with real syntax highlighting once a script gets long. This is exactly
what `engine = "spark_sql"`/`"spark_dataframe"` steps look like too, just
with a `spark_sql { sql = ... }` block instead — see
`examples/orders-rollup.canoe.hcl` for a Spark SQL step.

## Step 5: batching

`enrich` above runs once per record. If a step's real cost is something
worth amortizing across many records at once (a batch API call, a
vectorized computation), add `batch`:

```hcl
step "enrich" {
  engine = "python"
  input  = "validate"

  batch {
    size               = 500
    flush_interval_secs = 5
  }

  python {
    code = <<-PY
      def run(records: list[dict]) -> list[dict]:
          for record in records:
              record["amount_cents"] = round(record["amount"] * 100)
          return records
    PY
  }
}
```

The function's signature changes to `list[dict] -> list[dict]` — the
worker buffers up to `size` records (or `flush_interval_secs`,
whichever comes first) and calls your function once per batch. Only
valid on an escape-hatch step (declarative ops always compile to a
fixed per-record function). See `examples/orders-batch.canoe.hcl`.

## Step 6: routing — branching by a field

Say US and EU orders need different tax handling downstream. A `route`
block branches a step's output by a predicate, each branch its own
independent consumer:

```hcl
route "split_by_region" {
  input = "enrich"

  branch "us" {
    when = "region = 'US'"
  }
  branch "eu" {
    when = "region = 'EU'"
  }
}

step "tax_us" {
  engine = "python"
  input  = "split_by_region.us"

  python {
    code = <<-PY
      def run(record: dict) -> dict:
          record["tax_rate"] = 0.07
          return record
    PY
  }
}

step "tax_eu" {
  engine = "python"
  input  = "split_by_region.eu"

  python {
    code = <<-PY
      def run(record: dict) -> dict:
          record["tax_rate"] = 0.20
          return record
    PY
  }
}

merge "taxed" {
  inputs = ["tax_us", "tax_eu"]
}
```

...and repoint the sink to `input = "taxed"`. `merge` doesn't run any
code of its own — branches that don't need independent per-region logic
can just converge back into one shared output. A route can also declare
a `default {}` branch to catch anything not matched by `us`/`eu` — left
out here for brevity, but like every other node it needs its own
downstream consumer to compile (canoe-dsl rejects any node nothing reads
from); see "Routing" in README.md, and `examples/orders.canoe.hcl` for a
fuller three-way branch that does wire one up.

## Step 7: joining data from another pipeline

So far everything's been self-contained in one `.hcl` file. Real
systems have many pipelines — a `lookup` block lets a step join against
another pipeline's *current* data, even one declared in a completely
separate `.hcl` file, without that file ever being parsed by this
compilation.

First, a small standalone pipeline to look up — save this as
`tutorial-customers.canoe.hcl`:

```hcl
storage {
  base_path = "s3://canoe-lake"
  version   = "v1"
}

pipeline "customers" {
  source = "customers.raw"
}

step "snapshot" {
  engine = "python"
  input  = "customers.raw"

  rename {
    from = "cust_id"
    to   = "id"
  }
}

sink "land_customers" {
  input      = "snapshot"
  format     = "parquet"
  base_path  = "s3://canoe-lake"
  dataset_id = "customers"
}
```

(You'd also need a `customers.raw`-producing adapter for this to
actually run — omitted here since the point is the `lookup` syntax, not
a second full adapter. See `examples/customer-snapshot.canoe.hcl` for a
complete, runnable version of this pattern, paired with
`examples/orders-with-customer-lookup.canoe.hcl`.)

Back in `tutorial-orders.canoe.hcl`, add a `lookup` block to a step:

```hcl
step "join_customer" {
  engine = "spark_sql"
  input  = "taxed"

  lookup "customers" {
    pipeline = "customers"
    node     = "snapshot"
  }

  spark_sql {
    sql = "SELECT input.*, customers.email FROM input LEFT JOIN customers ON input.customer_id = customers.id"
  }
}
```

A `lookup` resolves to a literal path
(`<base_path>/<pipeline>/<version>/<node>`) at compile time — no graph
walk, since the target lives outside this file entirely. It reads that
node's **current snapshot** (its `latest.json`), not an incremental
stream: this is a stream-to-static join, the same pattern Spark
Structured Streaming and Flink call it. A `python` step can declare
`lookup` blocks too — its `run()` function gains a second argument,
`lookups: dict[str, list[dict]]`, keyed by each lookup block's own name.
A step can declare more than one `lookup` — see
"Cross-pipeline lookups" in README.md.

## Step 8: protecting sensitive columns

`email`, just joined in above, is exactly the kind of column you don't
want sitting in plaintext Parquet. Add `encrypt` to the step that
produces it — the `customers` pipeline's `snapshot` step:

```hcl
step "snapshot" {
  engine = "python"
  input  = "customers.raw"

  rename {
    from = "cust_id"
    to   = "id"
  }

  encrypt {
    fields = ["email"]
    key    = "pii_v1"
  }
}
```

Before you can run this, the key needs to exist somewhere canoe can
read it — a base64-encoded 32-byte AES-256 key, as an env var:

```
export CANOE_ENCRYPT_KEY_PII_V1=$(head -c32 /dev/urandom | base64)
```

Set that before running `scripts/run-coordinator.sh` (it forwards any
`CANOE_ENCRYPT_KEY_*` var it finds in its own environment to every pod
it creates). From here it's transparent: `email` is encrypted at rest
(a self-describing ciphertext string, `canoe:enc:v1:pii_v1:...`) the
moment `snapshot` writes it, and any reader with the same key —
including `join_customer`'s `lookup` above, with **nothing extra
declared on its side** — sees the decrypted plaintext automatically.
See "Column-level field encryption" in README.md for the full design
(why application-level rather than Parquet-native, the wire format, and
which runtimes get transparent decrypt vs. which don't).

## Step 9: pulling in a real secret

The fabricated adapter script in Step 1 doesn't need real credentials.
A real one — the closer-to-life `examples/stripe-adapter.canoe.hcl` —
does. Declare which AWS Secrets Manager entries a step or adapter needs:

```hcl
adapter "orders_adapter" {
  topic   = "orders.raw"
  secrets = ["orders_api_key"]
  ...
}
```

canoe provisions a `SecretProviderClass` (via the Secrets Store CSI
Driver + AWS provider — assumed already installed on a real cluster,
same as canoe assumes Fargate/Spark-on-K8s already exist) and mounts
each named secret as a file:

```python
from pathlib import Path
import os

api_key = (Path(os.environ["CANOE_SECRETS_PATH"]) / "orders_api_key").read_text().strip()
```

This needs a real AWS Secrets Manager account and IAM role to actually
exercise — not something the local `kind` stack can do end-to-end. See
"Secrets Manager integration" in README.md.

## Step 10: documenting your columns

Separately from encryption, you can attach human-readable metadata to a
sink's output columns — labels, descriptions, sensitivity tags — kept
in a YAML file next to the `.hcl` file rather than inlined:

`tutorial-customer-metadata.yaml`:
```yaml
id:
  label: "Customer ID"
email:
  label: "Email Address"
  sensitivity: "restricted"
  classification: ["pii", "contact"]
```

```hcl
sink "land_customers" {
  input      = "snapshot"
  format     = "parquet"
  base_path  = "s3://canoe-lake"
  dataset_id = "customers"
  metadata   = "tutorial-customer-metadata.yaml"
}
```

Read at compile time and embedded as a `canoe.metadata` property per
matching field in every run's `schema.avsc` — the same per-run schema
file the "landing the data" step already described, now carrying
descriptive metadata alongside the shape. Any reader — a person opening
the file, `canoe-tui`, an external catalog tool — sees it as plain
JSON, no special loading step needed. Independent of `encrypt`: a field
can be both encrypted and documented, as `email` is here.

## Step 11: deduplication and slowly-changing history

A sink's `checkpoint` block folds its `run_id=*` write history into one
deduplicated view, keyed by one or more columns (last write per key
wins):

```hcl
sink "land_customers" {
  ...
  checkpoint {
    keys = ["id"]
  }
}
```

`scd` instead keeps a full Type-2 slowly-changing-dimension history —
every version of a row, each with its own valid-from/valid-to, only
opening a new version when a *tracked* field actually changes:

```hcl
  scd {
    keys         = ["id"]
    track_fields = ["email"]
  }
```

Both run after every successful flush, compacting incrementally (only
the runs since the last pass) rather than reprocessing full history each
time. See "Checkpoints"/"Slowly changing dimensions" in README.md.

## Step 12: execution modes and dependencies

Every step so far has quietly been running in `mode = "batch"` — the
default for `engine = "python"`: a one-shot K8s Job fires when there's
real S3 backlog, drains it, and exits, rather than a pod sitting idle
between batches. `spark_sql`/`spark_dataframe` default the *other* way,
to `mode = "streaming"` (a long-running `spark.readStream` query against
a Kafka topic — preserves how every Spark step behaved before batch mode
existed for Spark at all), so `join_customer` above has actually been a
streaming step this whole time, even though nothing said so explicitly.

Add a second `spark_sql` step, this time declaring `mode = "batch"`
explicitly — an aggregate over `taxed`'s S3 output instead of a Kafka
stream:

```hcl
step "daily_totals_batch" {
  engine = "spark_sql"
  mode   = "batch"
  input  = "taxed"

  spark_sql {
    sql = "SELECT region, sum(amount) AS total_amount FROM input GROUP BY region"
  }
}

sink "land_daily_totals" {
  input      = "daily_totals_batch"
  format     = "parquet"
  base_path  = "s3://canoe-lake"
  dataset_id = "orders_daily_totals"
}
```

Mechanically this is the same Job-firing machinery `validate`/`enrich`
already use — `spark.read.parquet` across every unprocessed `run_id=`
under `taxed`'s output since this step's own checkpoint, instead of
`spark.readStream`. (Streaming mode is still there when you want a
genuinely continuous query — see "Execution modes: batch vs. streaming"
in README.md, and `examples/orders-execution-modes.canoe.hcl` for a
streaming-mode step wired up explicitly.)

Now make another step wait on `daily_totals_batch` — not for its data
(`input` still points elsewhere), just for it to run:

```hcl
step "notify_after_totals" {
  engine = "python"
  input  = "taxed"
  depends_on          = ["daily_totals_batch"]
  depends_on_complete = ["daily_totals_batch"]

  python {
    code = <<-PY
      def run(record: dict) -> dict:
          record["notified_after_daily_totals"] = True
          return record
    PY
  }
}

sink "land_notify" {
  input      = "notify_after_totals"
  format     = "parquet"
  base_path  = "s3://canoe-lake"
  dataset_id = "orders_notify"
}
```

`depends_on` is the weaker of the two: it's satisfied the moment
`daily_totals_batch` has been *applied* (its first Job created), not
once it's finished. `depends_on_complete` is the real gate — satisfied
only once a `daily_totals_batch` Job has actually *succeeded* — and only
works because `daily_totals_batch` is batch-mode: a batch-mode step
always has a "done" (Job completion); a streaming-mode step like
`join_customer` never does, so canoe-dsl would reject
`depends_on_complete = ["join_customer"]` at compile time with a clear
error. Both attributes work on `task` blocks too — `task`'s own
`depends_on` (see "Tasks" in README.md) already worked this way; this
just extends it to regular steps and adds the completion variant.

## What's next

This tutorial deliberately left out `dynamic_route` (fan-out to one pod
per discovered key value, still Kafka-topic transport — see "Dynamic
splitting" in README.md) and `task` (one-shot side effects triggered by
a dependency rather than a schedule or a topic — see "Tasks"). Both are
real, working features; they just don't fit naturally into the single
progressive example this tutorial builds.

From here:
- **README.md** is the complete reference — every block, every
  attribute, the full architecture (why plain `Pod`s instead of
  Deployments, batch mode's Job-firing reconcile loop vs. streaming
  mode's autoscaling/circuit-breaking, the S3+signal transport design).
- **`examples/`** has complete, `canoe-dslc`-verified `.hcl` files for
  every feature this tutorial touched, plus a couple it didn't.
- **`crates/canoe-dsl/tests/`** is a good place to see the exact
  compiled shape of any DSL block, if you want ground truth over prose.
