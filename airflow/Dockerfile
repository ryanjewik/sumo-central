# Airflow 2.8.1 base (pick the same Python tag you use everywhere)
FROM apache/airflow:2.8.1-python3.8

# ---- System deps + Java + Spark installed as root ----
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates openjdk-17-jdk tini \
 && rm -rf /var/lib/apt/lists/*

# Java env
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Spark 3.5.2 (Hadoop 3)
ARG SPARK_VERSION=3.5.2
ARG HADOOP_VERSION=3
ARG SPARK_ARCHIVE="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"
RUN curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_ARCHIVE}.tgz" -o /tmp/spark.tgz \
 && tar -xzf /tmp/spark.tgz -C /opt \
 && mv "/opt/${SPARK_ARCHIVE}" /opt/spark \
 && rm -f /tmp/spark.tgz \
 && chown -R airflow:root /opt/spark

# Spark env
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV PYSPARK_PYTHON=python
ENV PYSPARK_DRIVER_PYTHON=python

# ---- Python deps installed as airflow (not root) ----
USER airflow
COPY --chown=airflow:root requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
