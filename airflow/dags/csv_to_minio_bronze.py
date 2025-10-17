from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import boto3

# ================================
# CONFIG
# ================================
LOCAL_FOLDER = "/opt/airflow/data/raw"   # thư mục mount từ host
MINIO_ENDPOINT = "http://minio:9000"
MINIO_BUCKET = "bronze"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"

# ================================
# FUNCTION: Upload CSVs
# ================================
def upload_csv_to_minio():
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    # Tạo bucket nếu chưa tồn tại
    buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if MINIO_BUCKET not in buckets:
        s3.create_bucket(Bucket=MINIO_BUCKET)

    # Duyệt qua tất cả file trong thư mục
    for root, _, files in os.walk(LOCAL_FOLDER):
        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                object_name = file  # tên file trên MinIO
                print(f"Uploading {file_path} -> s3://{MINIO_BUCKET}/{object_name}")
                s3.upload_file(file_path, MINIO_BUCKET, object_name)

# ================================
# DAG DEFINITION
# ================================
with DAG(
    dag_id="csv_to_minio_bronze",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,    # chỉ chạy thủ công
    catchup=False,
    tags=["bronze", "minio", "csv"],
) as dag:

    upload_task = PythonOperator(
        task_id="upload_csv_files",
        python_callable=upload_csv_to_minio,
    )

    upload_task
