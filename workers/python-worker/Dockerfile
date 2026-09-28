FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    build-essential \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml ./
COPY canoe_worker ./canoe_worker

RUN pip install --no-cache-dir .

ENTRYPOINT ["python", "-m", "canoe_worker.main"]
