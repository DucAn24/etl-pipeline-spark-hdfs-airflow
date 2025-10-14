FROM apache/hadoop:3

USER root

# Create necessary directories
RUN mkdir -p /hadoop/dfs/name && \
    mkdir -p /hadoop/config && \
    chown -R hadoop:hadoop /hadoop/dfs/name /hadoop/config

# Copy Hadoop configuration files to a custom location
COPY docker/hadoop-config/core-site.xml /hadoop/config/core-site.xml
COPY docker/hadoop-config/hdfs-site.xml /hadoop/config/hdfs-site.xml

# Copy entrypoint script
COPY docker/hadoop-config/namenode-entrypoint.sh /opt/namenode-entrypoint.sh
RUN chmod +x /opt/namenode-entrypoint.sh

# Set environment variables to use custom config location
ENV HADOOP_CONF_DIR=/hadoop/config
ENV HDFS_CONF_DIR=/hadoop/config

USER hadoop

# Override the default entrypoint to use our custom script
ENTRYPOINT ["/opt/namenode-entrypoint.sh"]

