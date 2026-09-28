# Base on the official Spark image so spark-submit, the JVM, and
# spark-on-kubernetes entrypoint scripts are already present; this image
# is used for both the launcher pod (running spark-submit) and the
# driver/executor pods it creates (spark.kubernetes.container.image).
#
# `4.2.0-python3` confirmed against Docker Hub's `apache/spark` registry
# at implementation time (2026-09-25) — the current latest published 4.x
# tag matching the same `-python3` (PySpark-inclusive) convention
# `3.5.1-python3` used, multi-arch (amd64/arm64), verified pullable
# (`docker manifest inspect apache/spark:4.2.0-python3` /
# `docker pull apache/spark:4.2.0-python3` both succeeded in this
# sandbox). Built for Hadoop 3.5.0 (`/opt/spark/RELEASE` inside the image).
FROM apache/spark:4.2.0-python3

USER root
# confluent-kafka: batch-mode's completion signal publish
# (canoe_output_writer.publish_signal) — the streaming path already needs
# a Kafka client too, just via Spark's own kafka connector, not this one.
RUN pip install --no-cache-dir boto3 cryptography confluent-kafka

# s3a:// support for `spark.eventLog.dir` (and, incidentally, for
# `spark.read.parquet("s3://...")`/`spark.read.format("kafka")` output
# paths — see the "Known gap" note in canoe-spark's `spark_submit_args`
# docs for why those still don't get credentials on real S3/MinIO without
# IRSA today, independent of these jars being present).
#
# Verified (2026-09-25) by pulling this exact image and inspecting
# /opt/spark/jars/: it ships `hadoop-client-api-3.5.0.jar` and
# `hadoop-client-runtime-3.5.0.jar` (Hadoop's own "thin client" jars) but
# **not** `hadoop-aws` or any AWS SDK — the S3AFileSystem class isn't
# present anywhere in the image, so s3a:// paths fail without adding it.
#
# `hadoop-aws-3.5.0.jar` matches the image's Hadoop client version
# exactly (`/opt/spark/RELEASE`: "built for Hadoop 3.5.0"). Its AWS SDK
# dependency, confirmed against hadoop-aws 3.5.0's own published POM on
# Maven Central: **`software.amazon.awssdk:bundle:2.35.4`** (AWS SDK for
# Java **v2**) — not `com.amazonaws:aws-java-sdk-bundle` (the old v1
# artifact commonly referenced in older Hadoop-S3A docs/tutorials, which
# hadoop-aws 3.5.0's POM lists only as a `provided`-scope dependency for
# an optional legacy v1 credential provider, not what's actually needed
# here). Getting this wrong (grabbing the v1 bundle instead) would fail
# at runtime with a `ClassNotFoundException` for an AWS SDK v2 class
# hadoop-aws's compiled code actually calls.
#
# Both jars' existence/downloadability confirmed via direct HTTP HEAD/GET
# against Maven Central at implementation time; `hadoop-aws-3.5.0.jar`
# was downloaded and inspected to confirm it contains
# `org/apache/hadoop/fs/s3a/S3AFileSystem.class` and the exact
# `fs.s3a.endpoint`/`fs.s3a.path.style.access`/
# `fs.s3a.connection.ssl.enabled`/`fs.s3a.access.key`/`fs.s3a.secret.key`
# property-name string constants `canoe-spark`'s `spark_submit_args`
# relies on. **Not verified in this sandbox**: an actual S3A read/write
# against MinIO succeeding end to end (no cluster available here) —
# property names and jar presence are confirmed, the full runtime path
# isn't.
#
# The AWS SDK v2 bundle jar is large (~655MiB) — this is a real, inherent
# image-size cost of s3a:// support with this Hadoop version, not
# something to trim further without hand-assembling a partial SDK jar
# (fragile across versions, not attempted here).
RUN curl -fsSL -o /opt/spark/jars/hadoop-aws-3.5.0.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.5.0/hadoop-aws-3.5.0.jar \
    && curl -fsSL -o /opt/spark/jars/bundle-2.35.4.jar \
      https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.35.4/bundle-2.35.4.jar

COPY canoe_spark_runner.py /opt/canoe/canoe_spark_runner.py
COPY canoe_output_writer.py /opt/canoe/canoe_output_writer.py

USER spark
