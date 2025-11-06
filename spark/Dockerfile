# Bắt đầu từ image spark-iceberg gốc
FROM tabulario/spark-iceberg:3.5.5_1.8.1

# ===== Cài Papermill + Jupyter để chạy notebook =====
USER root
RUN pip install --no-cache-dir \
    papermill==2.4.0 \
    nbconvert==7.16.4 \
    nbformat==5.10.4 \
    ipykernel==6.29.5 \
    jupyter==1.0.0

# Install Python kernel
RUN python -m ipykernel install --name=python3

WORKDIR /opt/spark
# Copy thêm các JAR cần thiết để Spark làm việc với S3 (MinIO)
COPY jars/*.jar /opt/spark/jars/

# Copy custom entrypoint
COPY scripts/entrypoint.sh /opt/spark/entrypoint.sh
RUN chmod +x /opt/spark/entrypoint.sh

WORKDIR /home/iceberg/notebooks
# Override the default entrypoint
ENTRYPOINT ["/opt/spark/entrypoint.sh"]
