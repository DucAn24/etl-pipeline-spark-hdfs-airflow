FROM apache/hadoop:3

USER root

# Create necessary directories
RUN mkdir -p /hadoop/dfs/data && \
    mkdir -p /hadoop/config && \
    mkdir -p /tmp/hadoop && \
    chown -R hadoop:hadoop /hadoop/dfs/data /hadoop/config /tmp/hadoop

# Copy Hadoop configuration files to a custom location
COPY docker/hadoop-config/core-site.xml /hadoop/config/core-site.xml
COPY docker/hadoop-config/hdfs-site.xml /hadoop/config/hdfs-site.xml

# Set environment variables to use custom config location
ENV HADOOP_CONF_DIR=/hadoop/config
ENV HDFS_CONF_DIR=/hadoop/config
ENV HADOOP_TMP_DIR=/tmp/hadoop

USER hadoop

# Override the default entrypoint to skip envtoconf.py
ENTRYPOINT []
CMD ["hdfs", "datanode"]

