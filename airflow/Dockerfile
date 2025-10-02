FROM apache/airflow:3.1.0

# Use the same constraints Airflow uses
ENV PIP_CONSTRAINT="https://raw.githubusercontent.com/apache/airflow/constraints-3.1.0/constraints-3.12.txt"

# Copy requirements (auth + providers you need right now)
COPY requirements.txt /tmp/requirements.txt

# Install as the airflow user (best practice)
USER airflow
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Back to default cmd handled by compose
