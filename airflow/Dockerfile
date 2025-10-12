
FROM apache/airflow:2.8.1-python3.8
ENV PIP_CONSTRAINT="https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.8.txt"

COPY requirements.txt /tmp/requirements.txt

RUN curl -sS https://bootstrap.pypa.io/pip/3.8/get-pip.py -o get-pip.py && python get-pip.py
RUN pip install --force-reinstall --upgrade setuptools==68.2.2
RUN pip install jaraco.functools jaraco.text
RUN pip install --no-cache-dir -r /tmp/requirements.txt