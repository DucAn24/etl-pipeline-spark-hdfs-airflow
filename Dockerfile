FROM apache/airflow:latest-python3.10

USER root
# Install additional system dependencies if needed
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-libmysqlclient-dev \
    gcc \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt