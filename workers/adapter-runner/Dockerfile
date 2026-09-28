# Bundles uv + Python; run.py itself only needs the standard library (and
# boto3, for the s3 CodeSource case — installed here so it's available
# without the adapter script needing to declare it itself).
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*
RUN uv pip install --system --no-cache boto3

WORKDIR /app
COPY run.py .

# Baked in at a fixed path outside the script's own `uv run`-resolved
# venv, for a `mode = "batch"` adapter's script to import explicitly via
# `sys.path.insert(0, "/opt/canoe")` — see canoe_adapter_output.py's
# docstring.
COPY canoe_adapter_output.py /opt/canoe/canoe_adapter_output.py

ENTRYPOINT ["python", "run.py"]
