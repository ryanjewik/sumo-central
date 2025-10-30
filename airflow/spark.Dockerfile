FROM apache/spark:3.5.2-java17-python3

USER root
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        wget \
        gnupg2 \
        software-properties-common \
        procps \
        ; \
    apt-get install -y --no-install-recommends python3.8 python3.8-venv python3.8-distutils || true; \
    if ! command -v python3.8 >/dev/null 2>&1; then \
        add-apt-repository ppa:deadsnakes/ppa -y || true; \
        apt-get update; \
        apt-get install -y --no-install-recommends python3.8 python3.8-venv python3.8-distutils || true; \
    fi; \
    if command -v python3.8 >/dev/null 2>&1; then \
        wget https://bootstrap.pypa.io/pip/3.8/get-pip.py -O /tmp/get-pip.py; \
        python3.8 /tmp/get-pip.py; \
        rm -f /tmp/get-pip.py; \
        ln -sf /usr/bin/python3.8 /usr/bin/python3; \
        ln -sf /usr/bin/python3.8 /usr/bin/python || true; \
        python3.8 -m pip install --no-cache-dir \
            "pymongo>=4.0,<5" \
            "python-dotenv>=0.21.0" \
            "psycopg2-binary>=2.9" \
            "boto3>=1.26,<2" ; \
    fi; \
    apt-get clean && rm -rf /var/lib/apt/lists/* /var/cache/apt/*

ENV PYTHONUNBUFFERED=1
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3
ENV SPARK_PYTHON=/usr/bin/python3

# Postgres JDBC
ENV POSTGRES_JDBC_VERSION=42.6.0
RUN mkdir -p /opt/spark/jars-baked && \
    wget -q -O /opt/spark/jars-baked/postgresql-${POSTGRES_JDBC_VERSION}.jar \
      https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar


# S3 jars
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_BUNDLE_VERSION=1.12.262
RUN wget -q -O /opt/spark/jars-baked/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    wget -q -O /opt/spark/jars-baked/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar

# make spark see them even if we don't copy to the volume
ENV SPARK_DIST_CLASSPATH=/opt/spark/jars-baked/hadoop-aws-${HADOOP_AWS_VERSION}.jar:/opt/spark/jars-baked/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar:$SPARK_DIST_CLASSPATH

# go back to spark user
USER 185
WORKDIR /opt/spark/work-dir
