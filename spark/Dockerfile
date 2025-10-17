# Bắt đầu từ image spark-iceberg gốc
FROM tabulario/spark-iceberg:3.5.5_1.8.1
WORKDIR /opt/spark
# Copy thêm các JAR cần thiết để Spark làm việc với S3 (MinIO)
COPY jars/*.jar /opt/spark/jars/

# Copy custom entrypoint
COPY scripts/entrypoint.sh /opt/spark/entrypoint.sh
RUN chmod +x /opt/spark/entrypoint.sh

WORKDIR /home/iceberg/notebooks
# Override the default entrypoint
ENTRYPOINT ["/opt/spark/entrypoint.sh"]
