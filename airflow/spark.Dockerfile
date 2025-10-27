FROM apache/spark:3.5.2-java17-python3

# Install Python 3.8 and pip so the Spark images use the same Python minor
# version as the Airflow container (which is Python 3.8.x). This helps
# avoid PySpark [PYTHON_VERSION_MISMATCH] errors between driver and workers.
USER root
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        wget \
        gnupg2 \
        software-properties-common \
        ; \
    # Install python3.8 (some distros may already have it); try apt first
    apt-get install -y --no-install-recommends python3.8 python3.8-venv python3.8-distutils || true; \
    # If python3.8 not provided by apt, attempt deadsnakes PPA (best-effort)
    if ! command -v python3.8 >/dev/null 2>&1; then \
        add-apt-repository ppa:deadsnakes/ppa -y || true; \
        apt-get update; \
        apt-get install -y --no-install-recommends python3.8 python3.8-venv python3.8-distutils || true; \
    fi; \
    # Install pip for python3.8 using the get-pip bootstrap for 3.8
        if command -v python3.8 >/dev/null 2>&1; then \
        wget https://bootstrap.pypa.io/pip/3.8/get-pip.py -O /tmp/get-pip.py; \
        python3.8 /tmp/get-pip.py; \
        rm -f /tmp/get-pip.py; \
        # Make python3.8 the default python and python3 on the image
        ln -sf /usr/bin/python3.8 /usr/bin/python3; \
        ln -sf /usr/bin/python3.8 /usr/bin/python || true; \
        # Install runtime Python packages used by spark_homepage.py and executor-side writes.
        # - pymongo: executor writes to MongoDB
        # - python-dotenv: load_dotenv() at top-level in spark_homepage.py
        # - psycopg2-binary: fallback DB driver (binary wheel avoids build deps)
        python3.8 -m pip install --no-cache-dir \
            "pymongo>=4.0,<5" \
            "python-dotenv>=0.21.0" \
            "psycopg2-binary>=2.9" ; \
    fi; \
    apt-get clean && rm -rf /var/lib/apt/lists/* /var/cache/apt/*

USER root

ENV PYTHONUNBUFFERED=1

# Install PostgreSQL JDBC driver into Spark's jars directory so Spark jobs
# can use the driver without needing spark.jars.packages at runtime.
ENV POSTGRES_JDBC_VERSION=42.6.0
RUN set -eux; \
    JDBC_JAR_URL="https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"; \
    mkdir -p /opt/spark/jars; \
    if [ ! -f /opt/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar ]; then \
        echo "Downloading Postgres JDBC ${POSTGRES_JDBC_VERSION} to /opt/spark/jars"; \
        wget -q -O /opt/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar "$JDBC_JAR_URL"; \
    else \
        echo "Postgres JDBC already present"; \
    fi
