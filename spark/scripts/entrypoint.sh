#!/bin/bash
set -e

echo "SPARK_MODE: $SPARK_MODE"

case "$SPARK_MODE" in
  master)
    echo "Starting Spark Master..."
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
      --host 0.0.0.0 \
      --port ${SPARK_MASTER_PORT:-7077} \
      --webui-port ${SPARK_MASTER_WEBUI_PORT:-8080}
    ;;
  worker)
    echo "Starting Spark Worker..."
    echo "Master URL: ${SPARK_MASTER_URL}"
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
      ${SPARK_MASTER_URL} \
      --cores ${SPARK_WORKER_CORES:-1} \
      --memory ${SPARK_WORKER_MEMORY:-1g} \
      --port ${SPARK_WORKER_PORT:-8881} \
      --webui-port ${SPARK_WORKER_WEBUI_PORT:-8081}
    ;;
  *)
    echo "Unknown SPARK_MODE: $SPARK_MODE. Use 'master' or 'worker'"
    exit 1
    ;;
esac