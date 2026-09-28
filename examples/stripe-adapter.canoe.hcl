# A source adapter: not part of any pipeline's DAG, just a scheduled job
# that publishes to a topic. examples/orders.canoe.hcl (or any other
# pipeline) can pick this up by setting source = "stripe.charges.raw".
#
# The script is a standalone `uv run` script kept in its own file
# (examples/adapters/stripe_charges.py) rather than inlined here — `file`
# is resolved relative to this .hcl file's own directory at compile time
# (canoe-dslc reads it and embeds it as CodeSource::Inline in the
# compiled YAML; there's no runtime file lookup involved). Its own
# `# /// script` header declares dependencies, so `uv` resolves an
# ephemeral venv for it per run — no image rebuild needed to add one.
# The coordinator only ever sets CANOE_KAFKA_BOOTSTRAP/CANOE_OUTPUT_TOPIC
# as env vars and runs `uv run <the script>`; producing to Kafka is the
# script's own job, same "escape hatch" ownership model as a step's
# `python { file = ... }` block.

adapter "stripe_charges" {
  topic = "stripe.charges.raw"

  # Every 5 minutes. Could instead be `schedule { cron = "*/5 * * * *" }`
  # for calendar-aligned scheduling (e.g. "2am daily") — interval is
  # simpler when you just want a fixed cadence from the last run.
  schedule {
    interval = "5m"
  }

  resources {
    cpu_request = "100m"
    mem_request = "256Mi"
  }

  # Mounted as a file, not an env var — canoe provisions a
  # SecretProviderClass (Secrets Store CSI Driver + AWS provider) naming
  # this Secrets Manager entry, and mounts it into the adapter's Job at
  # `$CANOE_SECRETS_PATH/stripe_api_key`. See "Secrets Manager
  # integration" in README.md and examples/adapters/stripe_charges.py for
  # how the script reads it.
  secrets = ["stripe_api_key"]

  script {
    file = "adapters/stripe_charges.py"
  }
}
