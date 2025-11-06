"""
Airflow DAG để chạy notebook Load_Data_Bronze_To_Silver.ipynb trên Spark Cluster
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
import papermill as pm
import os

# ================================
# CONFIG
# ================================
NOTEBOOK_PATH = "/opt/airflow/notebooks/bronze/Load_Data_Bronze_To_Silver.ipynb"
OUTPUT_NOTEBOOK_PATH = "/opt/airflow/notebooks/bronze/Load_Data_Bronze_To_Silver_output_{}.ipynb"
SPARK_MASTER_URL = "spark://spark-master:7077"

# ================================
# FUNCTION: Execute Notebook ON Spark Master Container (BEST SOLUTION)
# ================================
def execute_notebook_on_spark_master(**context):
    """
    🎯 GIẢI PHÁP TỐT NHẤT: Chạy notebook TRỰC TIẾP trên Spark Master container
    Vì Spark Master đã có Java và PySpark, notebook sẽ chạy không cần Java gateway local
    """
    import subprocess
    
    execution_date = context['execution_date'].strftime('%Y%m%d_%H%M%S')
    output_path = OUTPUT_NOTEBOOK_PATH.format(execution_date)
    
    print(f"🚀 Đang submit notebook lên Spark Master container...")
    print(f"📝 Input: {NOTEBOOK_PATH}")
    print(f"📝 Output: {output_path}")
    
    # Kiểm tra xem file notebook có tồn tại trong container không
    print("\n🔍 Kiểm tra file notebook trong Spark Master container...")
    try:
        check_cmd = ['docker', 'exec', 'spark-master', 'ls', '-la', NOTEBOOK_PATH]
        check_result = subprocess.run(check_cmd, capture_output=True, text=True)
        print(f"📁 File check result:\n{check_result.stdout}")
        if check_result.returncode != 0:
            print(f"⚠️  Warning: {check_result.stderr}")
    except Exception as e:
        print(f"⚠️  Không thể kiểm tra file: {str(e)}")
    
    try:
        # Chạy papermill TRONG spark-master container
        # Không dùng --parameters với JSON, thay vào đó dùng -p cho từng parameter
        cmd = [
            'docker', 'exec', 'spark-master',
            'papermill',
            NOTEBOOK_PATH,
            output_path,
            '--kernel', 'python3',
            '--log-output',
            '--cwd', '/opt/airflow/notebooks/bronze'  # Set working directory
        ]
        
        print("⏳ Đang thực thi notebook trên Spark Master...")
        print(f"🔗 Command: {' '.join(cmd)}")
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=3600  # 1 hour timeout
        )
        
        print("✅ Notebook đã thực thi thành công trên Spark Master!")
        print(f"📊 Kiểm tra kết quả tại: {output_path}")
        print(f"🔍 Xem Spark jobs tại: http://spark-master:8080")
        
        if result.stdout:
            print(f"\n📋 Output (last 2000 chars):\n{result.stdout[-2000:]}")
        
        return output_path
        
    except subprocess.TimeoutExpired as e:
        print(f"⏰ Timeout: Notebook chạy quá lâu (> 1 giờ)")
        print(f"📊 Stdout:\n{e.stdout}")
        print(f"📊 Stderr:\n{e.stderr}")
        raise
    except subprocess.CalledProcessError as e:
        print(f"❌ Lỗi khi thực thi notebook: {str(e)}")
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
# FUNCTION: Execute Notebook with Papermill (Chạy notebook, PySpark submit jobs lên cluster)
# ================================
def execute_notebook_with_papermill(**context):
    """
    ⚠️ KHÔNG HOẠT ĐỘNG: Vì Airflow không có Java, không thể khởi tạo PySpark local gateway
    Sử dụng execute_notebook_on_spark_master thay thế
    """
    execution_date = context['execution_date'].strftime('%Y%m%d_%H%M%S')
    output_path = OUTPUT_NOTEBOOK_PATH.format(execution_date)
    
    print(f"🚀 Bắt đầu thực thi notebook: {NOTEBOOK_PATH}")
    print(f"📝 Kết quả sẽ được lưu tại: {output_path}")
    print(f"⚡ PySpark jobs sẽ chạy trên Spark cluster: {SPARK_MASTER_URL}")
    
    try:
        # Sử dụng Papermill để thực thi notebook
        # Notebook sẽ tạo SparkSession và submit jobs lên Spark cluster
        pm.execute_notebook(
            input_path=NOTEBOOK_PATH,
            output_path=output_path,
            parameters={
                'spark_master_url': SPARK_MASTER_URL,
                'execution_date': execution_date
            },
            kernel_name='python3',
            progress_bar=False,
            log_output=True
        )
        
        print("✅ Notebook đã thực thi thành công!")
        print(f"📊 Kiểm tra kết quả tại: {output_path}")
        print(f"🔍 Xem Spark jobs tại: http://spark-master:8080")
        
        return output_path
        
    except Exception as e:
        print(f"❌ Lỗi khi thực thi notebook: {str(e)}")
        raise


# ================================
# FUNCTION: Execute Notebook on Spark Cluster via HTTP (Remote Execution)
# ================================
def execute_notebook_on_spark_remote(**context):
    """
    Submit và chạy notebook trực tiếp trên Spark cluster thông qua Livy REST API
    Yêu cầu: Livy server chạy trên Spark cluster
    """
    import requests
    import time
    
    execution_date = context['execution_date'].strftime('%Y%m%d_%H%M%S')
    livy_url = "http://spark-master:8998"  # Livy REST API endpoint
    
    print(f"🚀 Submit notebook lên Spark cluster qua Livy...")
    print(f"🔗 Livy URL: {livy_url}")
    
    # Đọc notebook và convert sang code
    with open(NOTEBOOK_PATH) as f:
        nb = nbformat.read(f, as_version=4)
    
    # Extract Python code từ notebook
    code_cells = [cell['source'] for cell in nb.cells if cell['cell_type'] == 'code']
    code = '\n\n'.join(code_cells)
    
    # Create Livy session
    session_data = {
        "kind": "pyspark",
        "conf": {
            "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.nessie.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
            "spark.sql.catalog.nessie.uri": "http://nessie:19120/api/v1",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "admin",
            "spark.hadoop.fs.s3a.secret.key": "admin123"
        }
    }
    
    try:
        # Create session
        print("📝 Tạo Livy session...")
        response = requests.post(f"{livy_url}/sessions", json=session_data)
        session_id = response.json()['id']
        print(f"✅ Session ID: {session_id}")
        
        # Wait for session ready
        while True:
            status = requests.get(f"{livy_url}/sessions/{session_id}").json()
            if status['state'] == 'idle':
                break
            time.sleep(5)
        
        # Submit code
        print("🚀 Submit code lên Spark...")
        code_data = {"code": code}
        response = requests.post(f"{livy_url}/sessions/{session_id}/statements", json=code_data)
        statement_id = response.json()['id']
        
        # Wait for completion
        while True:
            result = requests.get(f"{livy_url}/sessions/{session_id}/statements/{statement_id}").json()
            if result['state'] in ['available', 'error', 'cancelled']:
                break
            time.sleep(10)
        
        print("✅ Notebook đã chạy xong trên Spark cluster!")
        return result
        
    except Exception as e:
        print(f"❌ Lỗi: {str(e)}")
        raise
    finally:
        # Cleanup session
        try:
            requests.delete(f"{livy_url}/sessions/{session_id}")
        except:
            pass


# ================================
# FUNCTION: Execute Notebook with nbconvert (Alternative)
# ================================
def execute_notebook_with_nbconvert(**context):
    """
    Thực thi notebook sử dụng nbconvert
    Phương pháp thay thế nếu không dùng Papermill
    """
    execution_date = context['execution_date'].strftime('%Y%m%d_%H%M%S')
    output_path = OUTPUT_NOTEBOOK_PATH.format(execution_date)
    
    print(f"🚀 Bắt đầu thực thi notebook: {NOTEBOOK_PATH}")
    print(f"📝 Kết quả sẽ được lưu tại: {output_path}")
    
    try:
        # Đọc notebook
        with open(NOTEBOOK_PATH) as f:
            nb = nbformat.read(f, as_version=4)
        
        # Cấu hình executor
        ep = ExecutePreprocessor(
            timeout=3600,  # 1 giờ timeout
            kernel_name='python3',
            allow_errors=False  # Dừng nếu có lỗi
        )
        
        # Thực thi notebook
        print("⏳ Đang thực thi notebook...")
        ep.preprocess(nb)
        
        # Lưu kết quả
        with open(output_path, 'w', encoding='utf-8') as f:
            nbformat.write(nb, f)
        
        print("✅ Notebook đã thực thi thành công!")
        print(f"📊 Kiểm tra kết quả tại: {output_path}")
        
        return output_path
        
    except Exception as e:
        print(f"❌ Lỗi khi thực thi notebook: {str(e)}")
        raise


# ================================
# FUNCTION: Submit PySpark Job to Cluster
# ================================
def submit_pyspark_job(**context):
    """
    Submit PySpark job lên Spark Cluster bằng spark-submit
    Phương pháp này convert notebook thành Python script và submit lên cluster
    """
    import subprocess
    import json
    
    execution_date = context['execution_date'].strftime('%Y%m%d_%H%M%S')
    
    print(f"🚀 Đang submit PySpark job lên Spark Cluster...")
    print(f"🔗 Spark Master URL: {SPARK_MASTER_URL}")
    
    # Convert notebook to Python script
    script_path = f"/tmp/load_bronze_to_silver_{execution_date}.py"
    
    try:
        # Convert notebook sang Python script
        print("📝 Đang convert notebook sang Python script...")
        subprocess.run([
            'jupyter', 'nbconvert',
            '--to', 'script',
            '--output', script_path,
            NOTEBOOK_PATH
        ], check=True)
        
        print(f"✅ Đã convert notebook thành: {script_path}")
        
        # Submit job lên Spark cluster
        print("🚀 Đang submit job lên Spark cluster...")
        
        spark_submit_cmd = [
            'spark-submit',
            '--master', SPARK_MASTER_URL,
            '--deploy-mode', 'client',
            '--conf', 'spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog',
            '--conf', 'spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog',
            '--conf', 'spark.sql.catalog.nessie.uri=http://nessie:19120/api/v1',
            '--conf', 'spark.sql.catalog.nessie.ref=main',
            '--conf', 'spark.sql.catalog.nessie.warehouse=s3a://silver/',
            '--conf', 'spark.sql.catalog.nessie.s3.endpoint=http://minio:9000',
            '--conf', 'spark.sql.catalog.nessie.s3.access-key=admin',
            '--conf', 'spark.sql.catalog.nessie.s3.secret-key=admin123',
            '--conf', 'spark.sql.catalog.nessie.s3.path-style-access=true',
            '--conf', 'spark.hadoop.fs.s3a.endpoint=http://minio:9000',
            '--conf', 'spark.hadoop.fs.s3a.access.key=admin',
            '--conf', 'spark.hadoop.fs.s3a.secret.key=admin123',
            '--conf', 'spark.hadoop.fs.s3a.path.style.access=true',
            '--conf', 'spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem',
            '--driver-memory', '2g',
            '--executor-memory', '2g',
            '--executor-cores', '2',
            script_path
        ]
        
        # Chạy spark-submit
        result = subprocess.run(
            spark_submit_cmd,
            capture_output=True,
            text=True,
            check=True
        )
        
        print("✅ Spark job đã hoàn thành thành công!")
        print(f"📊 Output:\n{result.stdout}")
        
        if result.stderr:
            print(f"⚠️  Stderr:\n{result.stderr}")
        
        # Clean up
        if os.path.exists(script_path):
            os.remove(script_path)
            print(f"🧹 Đã xóa file tạm: {script_path}")
        
        return result.stdout
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Lỗi khi submit Spark job: {str(e)}")
        print(f"📊 Output:\n{e.stdout}")
        print(f"📊 Error:\n{e.stderr}")
        raise
    except Exception as e:
        print(f"❌ Lỗi: {str(e)}")
        raise


# ================================
# FUNCTION: Check Spark Cluster Health
# ================================
def check_spark_cluster(**context):
    """
    Kiểm tra trạng thái Spark Cluster và notebook mount trước khi chạy job
    """
    import requests
    import subprocess
    
    print("🔍 Kiểm tra trạng thái Spark Cluster...")
    
    # 1. Kiểm tra Spark Master UI
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
            return False
            
    except Exception as e:
        print(f"❌ Không thể kết nối đến Spark Master: {str(e)}")
        raise
    
    # 2. Kiểm tra notebook có được mount vào Spark Master không
    print("\n🔍 Kiểm tra notebook mount trong Spark Master...")
    try:
        check_cmd = ['docker', 'exec', 'spark-master', 'ls', '-la', '/opt/airflow/notebooks/bronze/']
        result = subprocess.run(check_cmd, capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            print("✅ Notebooks đã được mount vào Spark Master")
            print(f"📁 Files:\n{result.stdout}")
            
            # Kiểm tra file notebook cụ thể
            if "Load_Data_Bronze_To_Silver.ipynb" in result.stdout:
                print("✅ File notebook Load_Data_Bronze_To_Silver.ipynb tồn tại!")
            else:
                print("⚠️  File notebook Load_Data_Bronze_To_Silver.ipynb KHÔNG tìm thấy!")
                return False
        else:
            print(f"❌ Không thể truy cập notebooks directory: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Lỗi khi kiểm tra notebook mount: {str(e)}")
        raise
    
    # 3. Kiểm tra papermill đã được cài đặt trong Spark Master chưa
    print("\n🔍 Kiểm tra Papermill trong Spark Master...")
    try:
        check_papermill = ['docker', 'exec', 'spark-master', 'which', 'papermill']
        result = subprocess.run(check_papermill, capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            print(f"✅ Papermill đã được cài đặt: {result.stdout.strip()}")
        else:
            print("⚠️  Papermill chưa được cài đặt trong Spark Master!")
            print("💡 Cần rebuild Spark Docker image với Papermill")
            return False
            
    except Exception as e:
        print(f"❌ Lỗi khi kiểm tra Papermill: {str(e)}")
        raise
    
    print("\n✅ Tất cả kiểm tra đã PASS!")
    return True


# ================================
# FUNCTION: Verify Data in Silver Layer (Alternative - Run on Spark Master)
# ================================
def verify_silver_data(**context):
    """
    Kiểm tra dữ liệu đã được load vào Silver layer
    Chạy trên Spark Master container để tránh lỗi Java gateway
    """
    import subprocess
    
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
    
    print(f"\\n✅ VERIFICATION_SUCCESS: school={school_count}, major={major_count}")
except Exception as e:
    print(f"❌ VERIFICATION_FAILED: {str(e)}")
    raise
finally:
    spark.stop()
"""
    
    try:
        # Lưu script vào file tạm trong container
        script_path = "/tmp/verify_silver.py"
        
        # Write script to container
        write_cmd = ['docker', 'exec', '-i', 'spark-master', 'bash', '-c', 
                    f'cat > {script_path}']
        subprocess.run(write_cmd, input=verify_script.encode(), check=True)
        
        # Chạy script trong spark-master container
        print("⏳ Đang chạy verification script trên Spark Master...")
        cmd = [
            'docker', 'exec', 'spark-master',
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
            # Extract counts from output
            for line in result.stdout.split('\n'):
                if "VERIFICATION_SUCCESS" in line:
                    print(f"\n🎉 {line}")
        
        return result.stdout
        
    except subprocess.TimeoutExpired as e:
        print(f"⏰ Timeout: Verification chạy quá lâu")
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
    dag_id='run_notebook_bronze_to_silver',
    default_args=default_args,
    description='Chạy notebook Load_Data_Bronze_To_Silver.ipynb trên Spark Cluster',
    schedule_interval='@daily',  # Chạy hàng ngày
    catchup=False,
    tags=['bronze', 'silver', 'spark', 'notebook', 'etl'],
) as dag:

    # Task 1: Kiểm tra Spark Cluster
    check_cluster_task = PythonOperator(
        task_id='check_spark_cluster',
        python_callable=check_spark_cluster,
        provide_context=True
    )    # Task 2: Thực thi notebook (Chọn 1 trong 3 phương pháp)
    
    # Phương pháp 1: Chạy notebook trên Spark Master container (Khuyên dùng) ✅
    execute_notebook_task = PythonOperator(
        task_id='execute_notebook_on_spark_master',
        python_callable=execute_notebook_on_spark_master,
        provide_context=True
    )
    
    # Phương pháp 2: Sử dụng nbconvert (Alternative - bỏ comment nếu muốn dùng)
    # execute_notebook_task = PythonOperator(
    #     task_id='execute_notebook_nbconvert',
    #     python_callable=execute_notebook_with_nbconvert,
    #     provide_context=True
    # )
    
    # Phương pháp 3: Submit Spark Job trực tiếp (Không khuyên dùng - cần JAVA_HOME)
    # execute_notebook_task = PythonOperator(
    #     task_id='submit_pyspark_job',
    #     python_callable=submit_pyspark_job,
    #     provide_context=True
    # )

    # Task 3: Kiểm tra dữ liệu sau khi load
    verify_data_task = PythonOperator(
        task_id='verify_silver_data',
        python_callable=verify_silver_data,
        provide_context=True
    )

    # Define task dependencies
    check_cluster_task >> execute_notebook_task >> verify_data_task
