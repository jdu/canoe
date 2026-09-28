# Bundles uv + Python; run.py itself only needs the standard library (and
# boto3, for the s3 CodeSource case — installed here so it's available
# without the step script needing to declare it itself). Same base image
# as workers/adapter-runner, but this one's Deployment pods run forever
# rather than exiting after one invocation — see run.py's exec vs.
# subprocess-wait explanation.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*
RUN uv pip install --system --no-cache boto3

WORKDIR /app
COPY run.py .

ENTRYPOINT ["python", "run.py"]
