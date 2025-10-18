#!/bin/bash

# Exit on error
set -e

echo "Starting NameNode initialization..."

# Check if NameNode is already formatted
if [ ! -d "/hadoop/dfs/name/current" ]; then
    echo "NameNode is not formatted. Formatting now..."
    hdfs namenode -format -force -clusterID ${CLUSTER_NAME:-hadoop_cluster}
    echo "NameNode formatted successfully!"
else
    echo "NameNode already formatted. Skipping format step."
fi

echo "Starting NameNode..."
# Start NameNode directly without calling the default entrypoint
exec hdfs namenode


