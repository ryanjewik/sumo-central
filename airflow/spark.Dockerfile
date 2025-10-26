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
    fi; \
    apt-get clean && rm -rf /var/lib/apt/lists/* /var/cache/apt/*

USER root

ENV PYTHONUNBUFFERED=1
