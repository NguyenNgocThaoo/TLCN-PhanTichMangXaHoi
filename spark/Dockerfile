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

# ===== Cài đặt các thư viện ML cho Multi-Task Model =====
RUN pip install --no-cache-dir \
    torch==2.1.2 \
    transformers==4.36.2 \
    vncorenlp==1.0.3 \
    pandas==2.0.3 \
    pyarrow==14.0.1 \
    scikit-learn==1.3.2

# Install Python kernel
RUN python -m ipykernel install --name=python3

WORKDIR /opt/spark
# Copy thêm các JAR cần thiết để Spark làm việc với S3 (MinIO)
# COPY jars/*.jar /opt/spark/jars/

# Copy scripts folder
COPY scripts/ /opt/spark/scripts/
RUN chmod +x /opt/spark/scripts/entrypoint.sh

WORKDIR /home/iceberg/notebooks
# Override the default entrypoint
ENTRYPOINT ["/opt/spark/scripts/entrypoint.sh"]
