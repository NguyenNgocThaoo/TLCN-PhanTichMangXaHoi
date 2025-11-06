"""
Airflow DAG: Submit PySpark Script lên Spark Cluster để load data Bronze -> Silver

DAG này sử dụng spark-submit để chạy file Python script trực tiếp trên Spark Cluster,
thay vì chạy Jupyter notebook.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import os


# ================================
# CONFIG
# ================================
SPARK_MASTER_URL = "spark://spark-master:7077"
PYSPARK_SCRIPT_PATH = "/opt/airflow/scripts/load_bronze_to_silver.py"
SPARK_CONTAINER = "spark-master"


# ================================
# FUNCTION: Check Spark Cluster Health
# ================================
def check_spark_cluster(**context):
    """
    Kiểm tra trạng thái Spark Cluster trước khi submit job
    """
    import requests
    
    print("🔍 Kiểm tra trạng thái Spark Cluster...")
    
    # Kiểm tra Spark Master UI
    spark_master_ui = "http://spark-master:8080"
    try:
        response = requests.get(f"{spark_master_ui}/json/", timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Spark Master đang hoạt động")
            print(f"📊 Status: {data.get('status', 'N/A')}")
            print(f"👷 Workers: {len(data.get('workers', []))}")
            print(f"💻 Cores: {data.get('cores', 'N/A')}")
            print(f"💾 Memory: {data.get('memory', 'N/A')}")
        else:
            print(f"⚠️  Spark Master không phản hồi đúng: {response.status_code}")
            raise Exception("Spark Master không hoạt động")
            
    except Exception as e:
        print(f"❌ Không thể kết nối đến Spark Master: {str(e)}")
        raise
    
    # Kiểm tra Python script có tồn tại không
    print("\n🔍 Kiểm tra Python script trong Spark Master container...")
    try:
        check_cmd = ['docker', 'exec', SPARK_CONTAINER, 'ls', '-la', PYSPARK_SCRIPT_PATH]
        result = subprocess.run(check_cmd, capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            print("✅ Python script đã được mount vào Spark Master")
            print(f"📁 File info:\n{result.stdout}")
        else:
            print(f"❌ Không thể truy cập Python script: {result.stderr}")
            raise Exception(f"Python script không tồn tại: {PYSPARK_SCRIPT_PATH}")
            
    except Exception as e:
        print(f"❌ Lỗi khi kiểm tra Python script: {str(e)}")
        raise
    
    print("\n✅ Tất cả kiểm tra đã PASS!")
    return True


# ================================
# FUNCTION: Submit PySpark Job to Spark Cluster
# ================================
def submit_pyspark_job_to_cluster(**context):
    """
    Submit PySpark script lên Spark Cluster bằng spark-submit
    Chạy trực tiếp trong Spark Master container
    """
    execution_date = context['execution_date'].strftime('%Y%m%d_%H%M%S')
    
    print(f"🚀 Đang submit PySpark job lên Spark Cluster...")
    print(f"🔗 Spark Master URL: {SPARK_MASTER_URL}")
    print(f"📝 Script: {PYSPARK_SCRIPT_PATH}")
    print(f"⏰ Execution Date: {execution_date}")
    
    try:
        # Xây dựng spark-submit command
        # Chạy spark-submit TRONG spark-master container
        spark_submit_cmd = [
            'docker', 'exec', SPARK_CONTAINER,
            'spark-submit',
            '--master', SPARK_MASTER_URL,
            '--deploy-mode', 'client',
            '--name', f'Load_Bronze_To_Silver_{execution_date}',
            # Iceberg & Nessie configurations
            '--conf', 'spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog',
            '--conf', 'spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog',
            '--conf', 'spark.sql.catalog.nessie.uri=http://nessie:19120/api/v1',
            '--conf', 'spark.sql.catalog.nessie.ref=main',
            '--conf', 'spark.sql.catalog.nessie.warehouse=s3a://silver/',
            # MinIO S3 configurations
            '--conf', 'spark.sql.catalog.nessie.s3.endpoint=http://minio:9000',
            '--conf', 'spark.sql.catalog.nessie.s3.access-key=admin',
            '--conf', 'spark.sql.catalog.nessie.s3.secret-key=admin123',
            '--conf', 'spark.sql.catalog.nessie.s3.path-style-access=true',
            '--conf', 'spark.hadoop.fs.s3a.endpoint=http://minio:9000',
            '--conf', 'spark.hadoop.fs.s3a.access.key=admin',
            '--conf', 'spark.hadoop.fs.s3a.secret.key=admin123',
            '--conf', 'spark.hadoop.fs.s3a.path.style.access=true',
            '--conf', 'spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem',
            # Resource configurations
            '--driver-memory', '2g',
            '--executor-memory', '2g',
            '--executor-cores', '2',
            '--total-executor-cores', '4',
            # Python script
            PYSPARK_SCRIPT_PATH
        ]
        
        print("\n⏳ Đang thực thi spark-submit...")
        print(f"🔗 Command: {' '.join(spark_submit_cmd)}")
        
        # Chạy spark-submit
        result = subprocess.run(
            spark_submit_cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=3600  # 1 hour timeout
        )
        
        print("\n✅ Spark job đã hoàn thành thành công!")
        print(f"🔍 Xem Spark jobs tại: http://spark-master:8080")
        
        # In output
        if result.stdout:
            print(f"\n📋 Output (last 3000 chars):\n{result.stdout[-3000:]}")
        
        if result.stderr:
            print(f"\n⚠️  Stderr (last 2000 chars):\n{result.stderr[-2000:]}")
        
        return result.stdout
        
    except subprocess.TimeoutExpired as e:
        print(f"⏰ Timeout: Spark job chạy quá lâu (> 1 giờ)")
        if e.stdout:
            print(f"📊 Stdout (last 2000 chars):\n{e.stdout[-2000:]}")
        if e.stderr:
            print(f"📊 Stderr (last 2000 chars):\n{e.stderr[-2000:]}")
        raise
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Lỗi khi submit Spark job: {str(e)}")
        print(f"📊 Return code: {e.returncode}")
        if e.stdout:
            print(f"📊 Stdout (last 2000 chars):\n{e.stdout[-2000:]}")
        if e.stderr:
            print(f"📊 Stderr (last 2000 chars):\n{e.stderr[-2000:]}")
        raise
        
    except Exception as e:
        print(f"❌ Lỗi không xác định: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


# ================================
# FUNCTION: Verify Data in Silver Layer
# ================================
def verify_silver_data(**context):
    """
    Kiểm tra dữ liệu đã được load vào Silver layer
    Chạy trên Spark Master container
    """
    print("🔍 Kiểm tra dữ liệu trong Silver layer...")
    
    # Tạo script Python để verify data
    verify_script = """
from pyspark.sql import SparkSession

# Tạo Spark Session
spark = (
    SparkSession.builder
    .appName("Verify_Silver_Data")
    .master("spark://spark-master:7077")
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
    .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1")
    .config("spark.sql.catalog.nessie.ref", "main")
    .config("spark.sql.catalog.nessie.warehouse", "s3a://silver/")
    .config("spark.sql.catalog.nessie.s3.endpoint", "http://minio:9000")
    .config("spark.sql.catalog.nessie.s3.access-key", "admin")
    .config("spark.sql.catalog.nessie.s3.secret-key", "admin123")
    .config("spark.sql.catalog.nessie.s3.path-style-access", "true")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "admin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    # Kiểm tra bảng school
    print("\\n📊 Kiểm tra bảng nessie.silver_tables.school:")
    df_school = spark.table("nessie.silver_tables.school")
    school_count = df_school.count()
    print(f"✅ Số dòng trong bảng school: {school_count}")
    
    # Kiểm tra bảng major
    print("\\n📊 Kiểm tra bảng nessie.silver_tables.major:")
    df_major = spark.table("nessie.silver_tables.major")
    major_count = df_major.count()
    print(f"✅ Số dòng trong bảng major: {major_count}")
    
    # Kiểm tra bảng major_group
    print("\\n📊 Kiểm tra bảng nessie.silver_tables.major_group:")
    df_major_group = spark.table("nessie.silver_tables.major_group")
    major_group_count = df_major_group.count()
    print(f"✅ Số dòng trong bảng major_group: {major_group_count}")
    
    print(f"\\n✅ VERIFICATION_SUCCESS: school={school_count}, major={major_count}, major_group={major_group_count}")
    
except Exception as e:
    print(f"❌ VERIFICATION_FAILED: {str(e)}")
    import traceback
    traceback.print_exc()
    raise
finally:
    spark.stop()
"""
    
    try:
        # Lưu script vào file tạm trong container
        script_path = "/tmp/verify_silver_data.py"
        
        # Write script to container
        write_cmd = ['docker', 'exec', '-i', SPARK_CONTAINER, 'bash', '-c', 
                    f'cat > {script_path}']
        subprocess.run(write_cmd, input=verify_script.encode(), check=True)
        
        # Chạy script trong spark-master container
        print("⏳ Đang chạy verification script trên Spark Master...")
        cmd = [
            'docker', 'exec', SPARK_CONTAINER,
            'python3', script_path
        ]
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=300  # 5 minutes timeout
        )
        
        print("✅ Verification thành công!")
        print(f"\n📋 Output:\n{result.stdout}")
        
        # Parse kết quả
        if "VERIFICATION_SUCCESS" in result.stdout:
            for line in result.stdout.split('\n'):
                if "VERIFICATION_SUCCESS" in line:
                    print(f"\n🎉 {line}")
        
        return result.stdout
        
    except subprocess.TimeoutExpired as e:
        print(f"⏰ Timeout: Verification chạy quá lâu")
        if e.stdout:
            print(f"📊 Stdout:\n{e.stdout}")
        raise
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Lỗi khi verify dữ liệu: {str(e)}")
        if e.stdout:
            print(f"📊 Stdout:\n{e.stdout}")
        if e.stderr:
            print(f"📊 Stderr:\n{e.stderr}")
        raise
        
    except Exception as e:
        print(f"❌ Lỗi không xác định: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


# ================================
# DAG DEFINITION
# ================================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='spark_submit_bronze_to_silver',
    default_args=default_args,
    description='Submit PySpark script để load data từ Bronze sang Silver layer',
    schedule_interval='@daily',  # Chạy hàng ngày
    catchup=False,
    tags=['bronze', 'silver', 'spark', 'pyspark', 'etl', 'spark-submit'],
) as dag:

    # Task 1: Kiểm tra Spark Cluster
    check_cluster_task = PythonOperator(
        task_id='check_spark_cluster',
        python_callable=check_spark_cluster,
        provide_context=True
    )

    # Task 2: Submit PySpark Job lên Spark Cluster
    submit_job_task = PythonOperator(
        task_id='submit_pyspark_job',
        python_callable=submit_pyspark_job_to_cluster,
        provide_context=True
    )

    # Task 3: Kiểm tra dữ liệu sau khi load
    verify_data_task = PythonOperator(
        task_id='verify_silver_data',
        python_callable=verify_silver_data,
        provide_context=True
    )

    # Define task dependencies
    check_cluster_task >> submit_job_task >> verify_data_task
