# Contributing to canoe

canoe is a Rust workspace (the coordinator, its supporting crates, and the
DSL/TUI tooling) plus four Python worker images it manages in Kubernetes.
This document covers how to build, test, and extend it; `README.md` is the
architecture/feature reference, and `TUTORIAL.md` is the guided walkthrough
for someone using it for the first time — read those first if you haven't.

## Getting set up

You need a Rust toolchain (stable; `cargo build --workspace` is the bar
every change needs to clear) and, for the Python workers, Python 3.12+.
Nothing else is required to build and test the Rust side — the Python
worker tests run against real `file://` fixtures, not a live cluster.

To actually run a pipeline end to end, see "Local dev stack" in
`README.md`: `scripts/kind-up.sh` stands up a disposable
[kind](https://kind.sigs.k8s.io/) cluster with Kafka and a MinIO
(S3-compatible) store, so you don't need a real AWS account or EKS
cluster to try a change against something real. Several things (Spark
job execution, an adapter's Job actually firing, an event log landing on
MinIO) are only verifiable this way — the test suite and `canoe-dslc`
compile checks catch everything else, but don't mistake "compiles and
unit tests pass" for "verified against a live cluster" when a change
touches K8s resource creation.

## Build and test

```
cargo build --workspace
cargo test --workspace
```

Both must be clean — no warnings, no failures — before a change is done.
This is not a suggestion: every feature in this codebase's history was
held to "fully green, iterate until it is," and that's the bar for new
work too.

Python worker tests, per image:

```
python -m unittest discover -s workers/python-worker/tests
```

`workers/spark-runner`'s scripts have no live-cluster test coverage (no
Spark session in CI/most dev environments) — verify changes there with a
syntax/AST check (`python -m py_compile <file>` or `ast.parse`) at
minimum, and say so explicitly if that's as far as you could verify a
change, rather than implying more than was actually checked.

## Adding a DSL feature

Nearly every feature in this codebase followed the same shape, and new
ones should too:

1. **`crates/canoe-dsl/src/ast.rs`** — parse the new HCL syntax into a
   `Raw*` struct. Look for an existing block/attribute that's structurally
   similar to what you're adding (a repeatable named block like `lookup`,
   a plain string-list attribute like `secrets`, an object-literal
   attribute like `spark`'s `conf`) and mirror its parsing pattern rather
   than inventing a new one — this codebase has a `Raw*` shape for nearly
   every attribute style HCL supports.
2. **`crates/canoe-dsl/src/compiler.rs`** — resolve the raw AST into the
   compiled `canoe_config::PipelineConfig` shape. If the new attribute
   references another node by name, decide explicitly whether it needs
   the file-local name→path graph (`input =`-style, validated at compile
   time) or a literal/cross-file reference (`lookup`-style, resolved by
   string formatting, unchecked at compile time) — these are genuinely
   different and the codebase has both; don't default to the graph-based
   one just because it's more common.
3. **`crates/canoe-config/src/lib.rs`** — the compiled struct field(s).
   `#[serde(default)]` on anything new, so every existing example and
   fixture keeps compiling unchanged — a new attribute must be additive,
   never a breaking change to what's already there.
4. **Runtime wiring** — `crates/canoe-k8s`/`crates/canoe-coordinator`/
   `crates/canoe-spark` for anything that reaches a pod/Job spec;
   `workers/python-worker`/`workers/spark-runner` for anything a running
   step needs to act on. Check whether the env var name you need already
   exists under a different but equivalent name before inventing one —
   `pod_manager.rs`'s env var list is the canonical reference for what a
   step pod already receives.
5. **Tests** — a `crates/canoe-dsl/tests/<feature>.rs` fixture file
   (`compile_str`/`compile_str_with_base`) is the fast, K8s-free way to
   verify DSL parsing and resolution. Nothing in this codebase mocks
   `kube::Client` — there's no precedent for it and no harness exists.
   For anything that would otherwise need one (a coordinator reconcile
   loop, a K8s Job/pod builder), extract the actual decision logic into a
   small pure function and unit-test *that* instead (`decide_batch_action`,
   `resolve_event_log`, `dependencies_satisfied` are the examples to
   follow) — don't skip coverage just because the surrounding async loop
   itself can't be exercised without a cluster.
6. **An example** — add or extend a file under `examples/`, and actually
   run it through `canoe-dslc` before considering the feature done:
   ```
   cargo run -p canoe-dslc -- examples/your-file.canoe.hcl -o /tmp/check.yaml
   ```
   A snippet that "looks plausible" but was never actually compiled is
   not acceptable — every example and every doc snippet in this repo's
   history was verified against the real compiler, including ones that
   turned out to have real bugs on the first attempt (an orphan-node
   rejection, a missing sink, a malformed object literal) that only
   showed up by actually running it.
7. **`README.md`** (and `TUTORIAL.md`, if the feature is the kind of
   thing a newcomer would hit early) — new features get a `###`
   subsection matching the depth of neighboring ones. Reference/excerpt
   from the example you just verified rather than retyping a fresh,
   unverified snippet into the doc.

## Code conventions

- **Comments explain *why*, not *what*.** A comment restating what the
  next line obviously does gets deleted, not added. A comment is worth
  writing when it captures a non-obvious constraint, a reason a simpler
  approach doesn't work, or a deliberate tradeoff — read any existing
  file in `crates/` for the calibration; this codebase leans toward
  fairly dense doc comments on public items specifically because the
  reasoning behind a design choice is usually more valuable than the
  code itself.
- **Reuse before you build.** Before adding a new helper, grep for
  something that already does most of it — this codebase has a strong
  track record of finding an existing pattern to extend (`attr_str_list`
  for a new list attribute, `job_status` for a new thing that needs Job
  completion, the `PythonStep`/`KafkaPythonStep` split as the template
  for the same split done again for Spark) rather than writing a parallel
  implementation.
- **Per-image duplication is intentional, not an oversight.** Each
  worker image (`python-worker`, `spark-runner`, `adapter-runner`,
  `uv-step-worker`) is a standalone container with no shared Python
  package between them — small helpers (a metrics reporter, a log
  shipper, the crypto envelope implementation) are deliberately
  duplicated per image rather than factored into a shared library. Don't
  "fix" this by introducing a shared package unless you're prepared to
  also solve how four independently-built Docker images would consume
  it.
- **Don't silently paper over a known gap.** Several real limitations in
  this codebase are deliberately documented rather than fixed in the
  same pass that found them (mixed-transport wiring isn't rejected at
  compile time; a re-pushed project doesn't tear down removed chains;
  `dynamic_route` chain names aren't project-prefixed). If you find
  something like this and it's out of scope for your change, say so
  explicitly in a comment or in your PR description — don't leave it
  undiscoverable, and don't feel obligated to fix everything you notice
  along the way either.
- **No speculative abstraction.** A block/attribute gets a real DSL
  surface when something needs it, not ahead of time "for
  extensibility." `column_metadata`'s open `BTreeMap<String,
  serde_json::Value>` shape (rather than a fixed struct) is the
  exception that proves the rule: it's open because the whole point is
  that canoe shouldn't need to know every metadata key in advance, not
  because open-ended is the default.

## Commits and PRs

Keep commits scoped to one coherent change with a clear "why" in the
message, not just "what changed." When a change touches multiple crates
for one feature (which is common here — a DSL attribute usually ripples
through `canoe-dsl` → `canoe-config` → `canoe-k8s`/`canoe-coordinator` →
a worker image), that's still one commit/PR, not one per crate — split
by feature, not by file.

Before opening a PR: `cargo build --workspace && cargo test --workspace`
clean, every touched/added example actually compiled via `canoe-dslc`,
and README/TUTORIAL updated if the change adds or changes user-facing
DSL surface. State plainly in the PR description what was and wasn't
verified (unit tests vs. an actual run against the `kind` stack vs.
neither) — this codebase's own history treats overclaiming verification
as a real defect, not a rounding error.

## License

Every crate's `Cargo.toml` declares `license = "Apache-2.0"`
(`[workspace.package]` in the root `Cargo.toml`) — contributions are
expected under the same terms. Note: no `LICENSE` file exists in the
repo yet to make that formal; that's worth adding alongside (or before)
this file, not something this document can resolve on its own.
