# 🎉 CHUYỂN ĐỔI TỪ NOTEBOOK SANG PYSPARK SCRIPT

## 📝 Tóm Tắt Những Gì Đã Làm

### 1. ✅ Tạo PySpark Script từ Notebook

**File mới:** `airflow/scripts/load_bronze_to_silver.py`

- Chuyển đổi notebook `Load_Data_Bronze_To_Silver.ipynb` thành Python script
- Tổ chức code thành các functions:
  - `create_spark_session()`: Khởi tạo Spark
  - `load_school_data()`: Load dữ liệu School
  - `load_major_data()`: Load dữ liệu Major  
  - `load_major_group_data()`: Load dữ liệu Major Group
  - `main()`: Main function

**Ưu điểm:**
- ✅ Production-ready
- ✅ Dễ debug và maintain
- ✅ Better performance
- ✅ Clean logs

### 2. ✅ Tạo Airflow DAG Mới

**File mới:** `airflow/dags/spark_submit_bronze_to_silver.py`

**3 Tasks:**
1. `check_spark_cluster`: Kiểm tra Spark cluster health
2. `submit_pyspark_job`: Submit script lên Spark cluster bằng `spark-submit`
3. `verify_silver_data`: Verify dữ liệu đã load thành công

**Phương pháp:**
- Sử dụng `docker exec` để chạy `spark-submit` TRONG Spark Master container
- Truyền đầy đủ Spark configs (Iceberg, Nessie, MinIO)
- Capture output và errors để debug

### 3. ✅ Cập Nhật Docker Compose

**File cập nhật:** `docker-compose.yaml`

**Thay đổi:**

#### Spark Master:
```yaml
volumes:
  - ./spark/conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf
  - ./notebooks:/opt/airflow/notebooks  # ← Mới
  - ./airflow/scripts:/opt/airflow/scripts  # ← Mới
```

#### Airflow:
```yaml
volumes:
  - ./airflow/dags:/opt/airflow/dags
  - ./airflow/logs:/opt/airflow/logs
  - ./airflow/plugins:/opt/airflow/plugins
  - ./airflow/entrypoint.sh:/opt/airflow/entrypoint.sh
  - ./notebooks:/opt/airflow/notebooks
  - ./airflow/scripts:/opt/airflow/scripts  # ← Mới
  - /var/run/docker.sock:/var/run/docker.sock  # ← Mới (để Airflow control Docker)
```

### 4. ✅ Cập Nhật Airflow Dockerfile

**File cập nhật:** `airflow/Dockerfile`

**Thêm Docker CLI:**
```dockerfile
# Cài đặt Docker CLI để Airflow có thể chạy docker exec
RUN apt-get update && \
    apt-get install -y docker-ce-cli
```

### 5. ✅ Tạo Tài Liệu

**Files mới:**
- `airflow/README_SPARK_SUBMIT.md`: Hướng dẫn chi tiết
- `setup_spark_submit.sh`: Script tự động setup

---

## 🚀 Cách Sử Dụng

### Quick Start

```bash
# 1. Chạy script setup
bash setup_spark_submit.sh

# 2. Mở Airflow UI
# http://localhost:8088

# 3. Enable và trigger DAG: spark_submit_bronze_to_silver

# 4. Monitor Spark jobs
# http://localhost:8080
```

### Manual Steps

```bash
# 1. Stop containers
docker-compose down

# 2. Build lại
docker-compose build --no-cache airflow spark-master

# 3. Start services
docker-compose up -d

# 4. Kiểm tra mount
docker exec spark-master ls -la /opt/airflow/scripts/
docker exec airflow ls -la /opt/airflow/scripts/

# 5. Trigger DAG
docker exec airflow airflow dags trigger spark_submit_bronze_to_silver

# 6. Xem logs
docker exec airflow airflow tasks logs spark_submit_bronze_to_silver submit_pyspark_job <execution_date>
```

---

## 📊 So Sánh: Cũ vs Mới

### Cách Cũ (Notebook)
```
Airflow → Papermill → Execute Notebook (trong Spark Master)
```

**Vấn đề:**
- ❌ Notebook overhead
- ❌ Khó debug
- ❌ Không production-ready
- ❌ Logs lộn xộn

### Cách Mới (PySpark Script)
```
Airflow → spark-submit → Python Script (trên Spark Cluster)
```

**Ưu điểm:**
- ✅ Hiệu suất cao hơn
- ✅ Dễ debug
- ✅ Production-ready
- ✅ Logs rõ ràng
- ✅ Scalable

---

## 🔧 Kiến Trúc Hoạt Động

```
┌─────────────────┐
│  Airflow DAG    │
└────────┬────────┘
         │
         │ (1) Check cluster health
         ▼
┌─────────────────┐
│ Spark Master    │ ← Check port 8080 (REST API)
└─────────────────┘
         │
         │ (2) docker exec spark-master spark-submit ...
         ▼
┌─────────────────────────────────────────────┐
│ spark-submit                                │
│   --master spark://spark-master:7077       │
│   --deploy-mode client                      │
│   /opt/airflow/scripts/load_bronze_to_silver.py │
└────────┬────────────────────────────────────┘
         │
         │ (3) Submit jobs to cluster
         ▼
┌─────────────────┐      ┌─────────────────┐
│ Spark Master    │──────│ Spark Worker(s) │
└────────┬────────┘      └─────────────────┘
         │
         │ (4) Read from Bronze / Write to Silver
         ▼
┌─────────────────┐      ┌─────────────────┐
│  MinIO (S3)     │      │  Nessie Catalog │
│  - Bronze       │      │  - silver_tables│
│  - Silver       │      └─────────────────┘
└─────────────────┘
         │
         │ (5) Verify data
         ▼
┌─────────────────┐
│ Verification    │
│ Script          │
└─────────────────┘
```

---

## 📁 Cấu Trúc Thư Mục

```
TLCN-Source/
├── airflow/
│   ├── dags/
│   │   ├── spark_submit_bronze_to_silver.py    # ← DAG mới ✨
│   │   └── run_notebook_bronze_to_silver.py    # ← DAG cũ
│   ├── scripts/
│   │   └── load_bronze_to_silver.py            # ← PySpark script ✨
│   ├── Dockerfile                              # ← Updated (Docker CLI) ✨
│   ├── README_SPARK_SUBMIT.md                  # ← Hướng dẫn ✨
│   └── ...
├── notebooks/
│   └── bronze/
│       └── Load_Data_Bronze_To_Silver.ipynb    # ← Original notebook
├── docker-compose.yaml                         # ← Updated (mounts) ✨
├── setup_spark_submit.sh                       # ← Setup script ✨
└── ...
```

---

## ✅ Checklist Sau Khi Deploy

- [ ] Services đang chạy: `docker-compose ps`
- [ ] Spark Master healthy: http://localhost:8080
- [ ] Airflow UI accessible: http://localhost:8088
- [ ] Scripts được mount: `docker exec spark-master ls /opt/airflow/scripts/`
- [ ] Docker CLI hoạt động: `docker exec airflow docker --version`
- [ ] DAG xuất hiện trong Airflow UI
- [ ] Test chạy DAG thành công
- [ ] Verify data trong Silver layer

---

## 🐛 Troubleshooting Common Issues

### Issue 1: Script không tìm thấy
```bash
# Check mount
docker exec spark-master ls -la /opt/airflow/scripts/

# Fix: Restart container
docker-compose restart spark-master
```

### Issue 2: Docker command not found trong Airflow
```bash
# Check Docker CLI
docker exec airflow docker --version

# Fix: Rebuild Airflow
docker-compose build --no-cache airflow
```

### Issue 3: Permission denied on docker.sock
```bash
# Check permissions
docker exec airflow ls -la /var/run/docker.sock

# Fix: Add airflow user to docker group (in Dockerfile)
# hoặc chmod docker.sock
sudo chmod 666 /var/run/docker.sock
```

### Issue 4: Spark job timeout
```bash
# Tăng timeout trong DAG
timeout=7200  # 2 hours

# Hoặc optimize script (cache, repartition, etc.)
```

---

## 🎓 Best Practices Đã Áp Dụng

1. ✅ **Separation of Concerns**: DAG chỉ orchestrate, logic ở script
2. ✅ **Error Handling**: Try-catch ở mọi nơi có thể fail
3. ✅ **Logging**: Print rõ ràng từng bước
4. ✅ **Validation**: Check cluster health trước khi submit
5. ✅ **Verification**: Verify data sau khi load
6. ✅ **Resource Management**: Set memory/cores appropriately
7. ✅ **Timeout**: Set timeout để tránh jobs chạy mãi
8. ✅ **Documentation**: README chi tiết

---

## 📚 Tài Liệu Tham Khảo

- [Spark Submit Guide](https://spark.apache.org/docs/latest/submitting-applications.html)
- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [Docker in Docker](https://docs.docker.com/engine/security/rootless/)
- [Iceberg + Spark](https://iceberg.apache.org/docs/latest/spark/)

---

## 🎉 Kết Luận

Bạn đã thành công chuyển đổi từ Jupyter Notebook sang PySpark Script production-ready!

**Key Achievements:**
- ✅ Converted notebook to Python script
- ✅ Created new Airflow DAG with spark-submit
- ✅ Updated Docker configs for proper mounts
- ✅ Added Docker CLI to Airflow
- ✅ Comprehensive documentation

**Next Steps:**
- Scale up Spark workers
- Add more data quality checks
- Implement monitoring & alerting
- Optimize performance
- Add more tables/transformations

**Happy Data Engineering! 🚀**
