# =============================================================================
# Formation Data Architecture — Image Jupyter Custom (air-gapped)
# Base : jupyter/pyspark-notebook:spark-3.5.0
# Build (depuis la RACINE du repo) :
#   docker compose build jupyter
# =============================================================================

FROM jupyter/pyspark-notebook:spark-3.5.0

LABEL maintainer="formation-data-architecture" \
      version="1.0.0"

USER root

ARG ICEBERG_VERSION=1.11.0
ARG NESSIE_VERSION=0.105.3
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
ARG MAVEN=https://repo1.maven.org/maven2

RUN mkdir -p /opt/spark-jars && \
    echo "── Iceberg Spark Runtime ────────────────────────────────" && \
    wget -q "${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
         -O "/opt/spark-jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" && \
    echo "  ok iceberg-spark-runtime" && \
    \
    echo "── Iceberg AWS Bundle (SDK v2 + S3FileIO) ───────────────" && \
    wget -q "${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
         -O "/opt/spark-jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" && \
    echo "  ok iceberg-aws-bundle" && \
    \
    echo "── Nessie Spark Extensions ──────────────────────────────" && \
    wget -q "${MAVEN}/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/${NESSIE_VERSION}/nessie-spark-extensions-3.5_2.12-${NESSIE_VERSION}.jar" \
         -O "/opt/spark-jars/nessie-spark-extensions-3.5_2.12-${NESSIE_VERSION}.jar" && \
    echo "  ok nessie-spark-extensions" && \
    \
    echo "── Hadoop AWS ───────────────────────────────────────────" && \
    wget -q "${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
         -O "/opt/spark-jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar" && \
    echo "  ok hadoop-aws" && \
    \
    echo "── AWS Java SDK Bundle (v1) ─────────────────────────────" && \
    wget -q "${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
         -O "/opt/spark-jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" && \
    echo "  ok aws-java-sdk-bundle" && \
    \
    ls -lh /opt/spark-jars/ && \
    chmod -R 755 /opt/spark-jars

COPY infra/spark/spark-defaults.conf /usr/local/spark/conf/spark-defaults.conf

COPY infra/jupyter/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && \
    echo "✅ Python packages installés" && \
    python3 -c "import importlib.metadata as m; pkgs=['pyiceberg','pynessie','mlflow','feast','dbt-trino']; [print(f'  {p}=={m.version(p)}') for p in pkgs]"

RUN fix-permissions /opt/spark-jars && \
    fix-permissions /usr/local/spark/conf

USER ${NB_UID}

ENV JUPYTER_ENABLE_LAB=yes \
    MINIO_ENDPOINT=http://minio:9000 \
    POLARIS_URI=http://polaris:8181/api/catalog \
    NESSIE_URI=http://nessie:19120/api/v2 \
    MLFLOW_TRACKING_URI=http://mlflow:5000 \
    MLFLOW_S3_ENDPOINT_URL=http://minio:9000
