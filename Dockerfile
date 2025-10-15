FROM apache/airflow:3.1.0

USER root

# Install OpenJDK 17
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

USER airflow

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt



