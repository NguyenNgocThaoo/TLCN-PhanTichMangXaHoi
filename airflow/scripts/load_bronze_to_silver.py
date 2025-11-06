#!/usr/bin/env python
# coding: utf-8

# # Load Data từ Bronze Layer sang Silver Layer
# 
# Notebook này sẽ đọc dữ liệu từ Bronze layer (MinIO) và xử lý để load vào các bảng Iceberg trong Silver layer với Nessie catalog.

# ## 1. Import Libraries và Khởi tạo Spark Session

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import os

# Khởi tạo Spark Session với Iceberg và Nessie catalog
# ⚠️ CRITICAL: .master() phải đứng NGAY SAU .builder để avoid local mode initialization
spark = (
    SparkSession.builder
    .master("spark://spark-master:7077")  # ✅ FIRST: Connect to Spark cluster
    .appName("Load_Bronze_To_Silver")
    # ===== Iceberg Catalog qua Nessie =====
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
    .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1")
    .config("spark.sql.catalog.nessie.ref", "main")
    .config("spark.sql.catalog.nessie.warehouse", "s3a://silver/")
    # ===== Cấu hình MinIO (S3-compatible) =====
    .config("spark.sql.catalog.nessie.s3.endpoint", "http://minio:9000")
    .config("spark.sql.catalog.nessie.s3.access-key", "admin")
    .config("spark.sql.catalog.nessie.s3.secret-key", "admin123")
    .config("spark.sql.catalog.nessie.s3.path-style-access", "true")
    # ===== Spark + Hadoop S3 connector =====
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "admin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark Session đã được khởi tạo với Nessie catalog!")
print(f"🔗 Connected to Spark Master: {spark.sparkContext.master}")
print(f"📊 Spark Version: {spark.version}")


# ## 2. Load Dữ Liệu School từ Bronze Layer

# In[2]:


# Đường dẫn đến các file CSV trong Bronze layer trên MinIO (2021-2025)
bronze_school_files = [
    "s3a://bronze/structured_data/danh sách các trường Đại Học (2021-2025)/Danh_sách_các_trường_Đại_Học_2021.csv",
    "s3a://bronze/structured_data/danh sách các trường Đại Học (2021-2025)/Danh_sách_các_trường_Đại_Học_2022.csv",
    "s3a://bronze/structured_data/danh sách các trường Đại Học (2021-2025)/Danh_sách_các_trường_Đại_Học_2023.csv",
    "s3a://bronze/structured_data/danh sách các trường Đại Học (2021-2025)/Danh_sách_các_trường_Đại_Học_2024.csv",
    "s3a://bronze/structured_data/danh sách các trường Đại Học (2021-2025)/Danh_sách_các_trường_Đại_Học_2025.csv"
]

# Đọc dữ liệu từ Bronze layer
print("📖 Đang đọc dữ liệu School từ Bronze layer (2021-2025)...")

dataframes = []
for file_path in bronze_school_files:
    try:
        year = file_path.split("_")[-1].replace(".csv", "")
        print(f"  ⏳ Đang đọc file năm {year}...")

        df_temp = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(file_path)

        # Chỉ giữ 3 cột: TenTruong, MaTruong, TinhThanh
        df_temp = df_temp.select(
            col("TenTruong"),
            col("MaTruong"),
            col("TinhThanh")
        )

        dataframes.append(df_temp)
        print(f"  ✅ Đọc được {df_temp.count()} dòng từ năm {year}")

    except Exception as e:
        print(f"  ❌ Lỗi khi đọc file {file_path}: {str(e)}")

# Ghép tất cả các DataFrame lại với nhau
if dataframes:
    print("\n🔗 Đang ghép tất cả các file lại...")
    df_school_bronze = dataframes[0]
    for df in dataframes[1:]:
        df_school_bronze = df_school_bronze.union(df)

    print(f"✅ Tổng số dòng sau khi ghép: {df_school_bronze.count()}")

    # Lọc unique theo 3 cột
    print("\n🔄 Đang lọc dữ liệu unique...")
    df_school_bronze = df_school_bronze.dropDuplicates(["TenTruong", "MaTruong", "TinhThanh"])

    print(f"✅ Số dòng sau khi lọc unique: {df_school_bronze.count()}")
    print("\n📊 Schema của dữ liệu Bronze:")
    df_school_bronze.printSchema()
    print("\n🔍 Xem 10 dòng đầu tiên:")
    df_school_bronze.show(10, truncate=False)
else:
    print("❌ Không đọc được dữ liệu từ bất kỳ file nào!")


# ## 3. Xử Lý và Transform Dữ Liệu School

# In[3]:


# Transform dữ liệu để phù hợp với schema Silver
print("🔄 Đang xử lý dữ liệu School...")

# Thêm timestamp cho created_at và updated_at
current_ts = current_timestamp()

df_school_silver = df_school_bronze.select(
    col("MaTruong").cast("string").alias("schoolId"),
    col("TenTruong").cast("string").alias("schoolName"),
    col("TinhThanh").cast("string").alias("province"),
    current_ts.alias("created_at"),
    current_ts.alias("updated_at")
)

# Làm sạch dữ liệu: loại bỏ các dòng có giá trị null ở các cột quan trọng
df_school_silver = df_school_silver.filter(
    col("schoolId").isNotNull() & 
    col("schoolName").isNotNull()
)

print(f"✅ Đã xử lý xong {df_school_silver.count()} dòng dữ liệu")
print("\n📊 Schema của dữ liệu Silver:")
df_school_silver.printSchema()
print("\n🔍 Xem 10 dòng sau khi xử lý:")
df_school_silver.show(10, truncate=False)


# ## 4. Load Dữ Liệu vào Bảng School trong Silver Layer

# In[4]:


# Load dữ liệu vào bảng Iceberg trong Silver layer
print("💾 Đang ghi dữ liệu vào bảng nessie.silver_tables.school...")

try:
    # Ghi dữ liệu vào bảng Iceberg với mode append hoặc overwrite
    df_school_silver.writeTo("nessie.silver_tables.school") \
        .using("iceberg") \
        .createOrReplace()

    print("✅ Đã ghi dữ liệu thành công vào bảng school!")

except Exception as e:
    print(f"❌ Lỗi khi ghi dữ liệu: {str(e)}")


# ## 5. Kiểm Tra Dữ Liệu Đã Load vào Silver Layer

# In[5]:


# Đọc và kiểm tra dữ liệu từ bảng Silver
print("🔍 Kiểm tra dữ liệu trong bảng nessie.silver_tables.school...")

try:
    df_verify = spark.table("nessie.silver_tables.school")

    print(f"\n📊 Tổng số dòng trong bảng: {df_verify.count()}")
    print("\n🔍 Schema của bảng:")
    df_verify.printSchema()

    print("\n🔍 10 dòng đầu tiên:")
    df_verify.show(10, truncate=False)

    print("\n📈 Thống kê theo tỉnh/thành phố:")
    df_verify.groupBy("province").count().orderBy(desc("count")).show(10, truncate=False)

except Exception as e:
    print(f"❌ Lỗi khi đọc dữ liệu: {str(e)}")


# ---
# 
# # LOAD DỮ LIỆU BẢNG MAJOR (NGÀNH HỌC)
# 
# ---

# ## 6. Load Dữ Liệu Major từ Bronze Layer

# In[6]:


# Đường dẫn đến file CSV trong Bronze layer trên MinIO
bronze_major_path = "s3a://bronze/structured_data/danh sách các nhóm ngành đại học/Danh_sách_các_ngành_theo_nhóm_ngành.csv"

# Đọc dữ liệu từ Bronze layer
print("📖 Đang đọc dữ liệu Major từ Bronze layer...")

try:
    df_major_bronze = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("encoding", "UTF-8") \
        .csv(bronze_major_path)

    print(f"✅ Đã đọc được {df_major_bronze.count()} dòng dữ liệu")
    print("\n📊 Schema của dữ liệu Bronze:")
    df_major_bronze.printSchema()
    print("\n🔍 Xem 10 dòng đầu tiên:")
    df_major_bronze.show(10, truncate=False)

    # Xem các cột có trong file
    print("\n📋 Các cột trong file:")
    print(df_major_bronze.columns)

except Exception as e:
    print(f"❌ Lỗi khi đọc dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# ## 7. Xử Lý và Transform Dữ Liệu Major

# In[10]:


# Transform dữ liệu để phù hợp với schema Silver
print("🔄 Đang xử lý dữ liệu Major...")

try:
    # Thêm timestamp cho created_at và updated_at
    current_ts = current_timestamp()

    # Giả sử file có các cột: MaNganh, TenNganh, MaNhomNganh
    # Điều chỉnh tên cột theo file thực tế của bạn
    df_major_silver = df_major_bronze.select(
        col(df_major_bronze.columns[0]).cast("string").alias("majorId"),          # Cột đầu tiên: Mã ngành
        col(df_major_bronze.columns[1]).cast("string").alias("majorName"),        # Cột thứ 2: Tên ngành
        col(df_major_bronze.columns[2]).cast("int").alias("majorGroupId"),        # Cột thứ 3: Mã nhóm ngành
        current_ts.alias("created_at"),
        current_ts.alias("updated_at")
    )

    # Làm sạch dữ liệu: loại bỏ các dòng có giá trị null ở các cột quan trọng
    df_major_silver = df_major_silver.filter(
        col("majorId").isNotNull() & 
        col("majorName").isNotNull() &
        col("majorGroupId").isNotNull()
    )

    # Lọc unique theo majorId và majorName
    print("\n🔄 Đang lọc dữ liệu unique...")
    df_major_silver = df_major_silver.dropDuplicates(["majorId", "majorName"])

    print(f"✅ Đã xử lý xong {df_major_silver.count()} dòng dữ liệu")
    print("\n📊 Schema của dữ liệu Silver:")
    df_major_silver.printSchema()
    print("\n🔍 Xem 10 dòng sau khi xử lý:")
    df_major_silver.show(10, truncate=False)

    # Thống kê theo nhóm ngành
    print("\n📈 Thống kê theo nhóm ngành:")
    df_major_silver.groupBy("majorGroupId").count().orderBy("majorGroupId").show(truncate=False)

except Exception as e:
    print(f"❌ Lỗi khi xử lý dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# ## 8. Load Dữ Liệu vào Bảng Major trong Silver Layer

# In[11]:


# Load dữ liệu vào bảng Iceberg trong Silver layer
print("💾 Đang ghi dữ liệu vào bảng nessie.silver_tables.major...")

try:
    # Ghi dữ liệu vào bảng Iceberg với mode append hoặc overwrite
    df_major_silver.writeTo("nessie.silver_tables.major") \
        .using("iceberg") \
        .createOrReplace()

    print("✅ Đã ghi dữ liệu thành công vào bảng major!")
    print(f"📊 Tổng số dòng đã ghi: {df_major_silver.count()}")

except Exception as e:
    print(f"❌ Lỗi khi ghi dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# ## 9. Kiểm Tra Dữ Liệu Major Đã Load vào Silver Layer

# In[12]:


# Đọc và kiểm tra dữ liệu từ bảng Silver
print("🔍 Kiểm tra dữ liệu trong bảng nessie.silver_tables.major...")

try:
    df_verify_major = spark.table("nessie.silver_tables.major")

    print(f"\n📊 Tổng số dòng trong bảng: {df_verify_major.count()}")
    print("\n🔍 Schema của bảng:")
    df_verify_major.printSchema()

    print("\n🔍 10 dòng đầu tiên:")
    df_verify_major.show(10, truncate=False)

    print("\n📈 Thống kê theo nhóm ngành:")
    df_verify_major.groupBy("majorGroupId").count().orderBy("majorGroupId").show(20, truncate=False)

    print("\n🔍 Sample một vài ngành:")
    df_verify_major.select("majorId", "majorName", "majorGroupId").show(20, truncate=False)

except Exception as e:
    print(f"❌ Lỗi khi đọc dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# In[ ]:





# ---
# 
# # LOAD DỮ LIỆU BẢNG MAJOR_GROUP (NHÓM NGÀNH)
# 
# ---

# ## 10. Load Dữ Liệu Major Group từ Bronze Layer

# In[13]:


# Đường dẫn đến file CSV trong Bronze layer trên MinIO
bronze_major_group_path = "s3a://bronze/structured_data/danh sách các nhóm ngành đại học/Danh_sách_các_nhóm_ngành_đại_học.csv"

# Đọc dữ liệu từ Bronze layer
print("📖 Đang đọc dữ liệu Major Group từ Bronze layer...")

try:
    df_major_group_bronze = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("encoding", "UTF-8") \
        .csv(bronze_major_group_path)

    print(f"✅ Đã đọc được {df_major_group_bronze.count()} dòng dữ liệu")
    print("\n📊 Schema của dữ liệu Bronze:")
    df_major_group_bronze.printSchema()
    print("\n🔍 Xem tất cả dữ liệu:")
    df_major_group_bronze.show(100, truncate=False)

    # Xem các cột có trong file
    print("\n📋 Các cột trong file:")
    print(df_major_group_bronze.columns)

except Exception as e:
    print(f"❌ Lỗi khi đọc dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# ## 11. Xử Lý và Transform Dữ Liệu Major Group

# In[14]:


# Transform dữ liệu để phù hợp với schema Silver
print("🔄 Đang xử lý dữ liệu Major Group...")

try:
    # Thêm timestamp cho created_at và updated_at
    current_ts = current_timestamp()

    # Giả sử file có các cột: MaNhomNganh, TenNhomNganh
    # Điều chỉnh tên cột theo file thực tế của bạn
    df_major_group_silver = df_major_group_bronze.select(
        col(df_major_group_bronze.columns[2]).cast("int").alias("majorGroupId"),      # Cột đầu tiên: Mã nhóm ngành
        col(df_major_group_bronze.columns[1]).cast("string").alias("majorGroupName"), # Cột thứ 2: Tên nhóm ngành
        current_ts.alias("created_at"),
        current_ts.alias("updated_at")
    )

    # Làm sạch dữ liệu: loại bỏ các dòng có giá trị null ở các cột quan trọng
    df_major_group_silver = df_major_group_silver.filter(
        col("majorGroupId").isNotNull() & 
        col("majorGroupName").isNotNull()
    )

    # Lọc unique theo majorGroupId
    print("\n🔄 Đang lọc dữ liệu unique...")
    df_major_group_silver = df_major_group_silver.dropDuplicates(["majorGroupId"])

    print(f"✅ Đã xử lý xong {df_major_group_silver.count()} dòng dữ liệu")
    print("\n📊 Schema của dữ liệu Silver:")
    df_major_group_silver.printSchema()
    print("\n🔍 Xem tất cả dữ liệu sau khi xử lý:")
    df_major_group_silver.show(100, truncate=False)

except Exception as e:
    print(f"❌ Lỗi khi xử lý dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# ## 12. Load Dữ Liệu vào Bảng Major Group trong Silver Layer

# In[15]:


# Load dữ liệu vào bảng Iceberg trong Silver layer
print("💾 Đang ghi dữ liệu vào bảng nessie.silver_tables.major_group...")

try:
    df_major_group_silver.writeTo("nessie.silver_tables.major_group") \
    .using("iceberg") \
    .createOrReplace()

    print(" Đã ghi dữ liệu thành công vào bảng major_group!")
    print(f" Tổng số dòng đã ghi: {df_major_group_silver.count()}")

except Exception as e:
    print(f"❌ Lỗi khi ghi dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# ## 13. Kiểm Tra Dữ Liệu Major Group Đã Load vào Silver Layer

# In[16]:


# Đọc và kiểm tra dữ liệu từ bảng Silver
print("🔍 Kiểm tra dữ liệu trong bảng nessie.silver_tables.major_group...")

try:
    df_verify_major_group = spark.table("nessie.silver_tables.major_group")

    print(f"\n📊 Tổng số dòng trong bảng: {df_verify_major_group.count()}")
    print("\n🔍 Schema của bảng:")
    df_verify_major_group.printSchema()

    print("\n🔍 Tất cả nhóm ngành:")
    df_verify_major_group.orderBy("majorGroupId").show(100, truncate=False)

    # Kiểm tra xem có nhóm ngành nào bị thiếu không
    print("\n📊 Số lượng nhóm ngành:")
    print(f"Total: {df_verify_major_group.count()} nhóm ngành")

except Exception as e:
    print(f"❌ Lỗi khi đọc dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# In[ ]:





# ---
# 
# # LOAD DỮ LIỆU BẢNG SUBJECT_GROUP (NHÓM MÔN/KHỐI THI)
# 
# ---

# ## 14. Load Dữ Liệu Subject Group từ Bronze Layer

# In[27]:


# Đường dẫn đến file CSV trong Bronze layer trên MinIO
bronze_subject_group_path = "s3a://bronze/structured_data/tohop_mon.csv"

print("📖 Đang đọc dữ liệu Subject Group từ Bronze layer...")

try:
    df_subject_group_bronze = (
        spark.read
        .format("csv")
        .option("header", "true")           # Có dòng tiêu đề
        .option("inferSchema", "true")      # Tự động suy luận kiểu dữ liệu
        .option("encoding", "UTF-8")        # Đọc tiếng Việt
        .csv(bronze_subject_group_path)
    )


    print(f"✅ Đã đọc được {df_subject_group_bronze.count()} dòng dữ liệu")
    print("\n📊 Schema của dữ liệu Bronze:")
    df_subject_group_bronze.printSchema()

    print("\n🔍 Xem dữ liệu mẫu:")
    df_subject_group_bronze.show(truncate=False)

    print("\n📋 Các cột trong file:")
    print(df_subject_group_bronze.columns)

except Exception as e:
    print(f"❌ Lỗi khi đọc dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# In[30]:


# Transform dữ liệu để phù hợp với schema Silver
print("🔄 Đang xử lý dữ liệu Subject Group...")

try:
    # Thêm timestamp cho created_at và updated_at
    current_ts = current_timestamp()

    # File có 3 cột: STT, Tổ hợp, Môn chi tiết
    # Column 0: STT (subjectGroupId)
    # Column 1: Tổ hợp (subjectGroupName) - VD: "D01"
    # Column 2: Môn chi tiết (subjectCombination) - VD: "Toán, Ngữ Văn, Tiếng Anh"

    df_subject_group_silver = df_subject_group_bronze.select(
        col(df_subject_group_bronze.columns[0]).cast("int").alias("subjectGroupId"),           # STT
        col(df_subject_group_bronze.columns[1]).cast("string").alias("subjectGroupName"),      # Tổ hợp (VD: D01, A00, A01)
        col(df_subject_group_bronze.columns[2]).cast("string").alias("subjectCombination"),    # Môn chi tiết
        current_ts.alias("created_at"),
        current_ts.alias("updated_at")
    )

    # Làm sạch dữ liệu: loại bỏ các dòng có giá trị null ở các cột quan trọng
    df_subject_group_silver = df_subject_group_silver.filter(
        col("subjectGroupId").isNotNull() & 
        col("subjectGroupName").isNotNull() &
        col("subjectCombination").isNotNull()
    )

    # Lọc unique theo subjectGroupName
    print("\n🔄 Đang lọc dữ liệu unique...")
    df_subject_group_silver = df_subject_group_silver.dropDuplicates(["subjectGroupName","subjectCombination"])

    print(f"✅ Đã xử lý xong {df_subject_group_silver.count()} dòng dữ liệu")
    print("\n📊 Schema của dữ liệu Silver:")
    df_subject_group_silver.printSchema()
    print("\n🔍 Xem 20 dòng đầu tiên sau khi xử lý:")
    df_subject_group_silver.show(20, truncate=False)

    # Thống kê
    print("\n📊 Thống kê:")
    print(f"Tổng số tổ hợp môn: {df_subject_group_silver.count()}")

except Exception as e:
    print(f"❌ Lỗi khi xử lý dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# ## 16. Load Dữ Liệu vào Bảng Subject Group trong Silver Layer

# In[31]:


# Load dữ liệu vào bảng Iceberg trong Silver layer
print("💾 Đang ghi dữ liệu vào bảng nessie.silver_tables.subject_group...")

try:
    df_subject_group_silver.writeTo("nessie.silver_tables.subject_group") \
    .using("iceberg") \
    .createOrReplace()

    print("✅ Đã ghi dữ liệu thành công vào bảng subject_group!")
    print(f"📊 Tổng số dòng đã ghi: {df_subject_group_silver.count()}")

except Exception as e:
    print(f"❌ Lỗi khi ghi dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# ## 17. Kiểm Tra Dữ Liệu Subject Group Đã Load vào Silver Layer

# In[32]:


# Đọc và kiểm tra dữ liệu từ bảng Silver
print("🔍 Kiểm tra dữ liệu trong bảng nessie.silver_tables.subject_group...")

try:
    df_verify_subject_group = spark.table("nessie.silver_tables.subject_group")

    print(f"\n📊 Tổng số dòng trong bảng: {df_verify_subject_group.count()}")
    print("\n🔍 Schema của bảng:")
    df_verify_subject_group.printSchema()

    print("\n🔍 20 tổ hợp môn đầu tiên:")
    df_verify_subject_group.orderBy("subjectGroupId").show(20, truncate=False)

    print("\n🔍 Một số tổ hợp môn cụ thể:")
    df_verify_subject_group.filter(col("subjectGroupName").isin(["A00", "A01", "D01", "C00"])).show(truncate=False)

    # Thống kê
    print("\n📊 Thống kê:")
    print(f"Tổng số tổ hợp môn: {df_verify_subject_group.count()}")

except Exception as e:
    print(f"❌ Lỗi khi đọc dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# ## 18. Load Dữ Liệu Selection Method từ Bronze Layer

# In[33]:


# Đường dẫn đến file CSV điểm chuẩn trong Bronze layer trên MinIO
# File này chứa thông tin về phương thức xét tuyển trong cột PhuongThuc
bronze_benchmark_path = "s3a://bronze/structured_data/điểm chuẩn các trường (2021-2025)/Điểm_chuẩn_các_ngành_đại_học_năm(2021-2025)*.csv"

# Đọc dữ liệu từ Bronze layer
print("📖 Đang đọc dữ liệu điểm chuẩn để lấy Selection Method từ Bronze layer...")

try:
    df_benchmark_bronze = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("encoding", "UTF-8") \
        .csv(bronze_benchmark_path)

    print(f"✅ Đã đọc được {df_benchmark_bronze.count()} dòng dữ liệu")
    print("\n📊 Schema của dữ liệu Bronze:")
    df_benchmark_bronze.printSchema()

    # Xem các giá trị trong cột PhuongThuc
    print("\n🔍 Các giá trị trong cột PhuongThuc:")
    df_benchmark_bronze.select("PhuongThuc").distinct().show(50, truncate=False)

except Exception as e:
    print(f"❌ Lỗi khi đọc dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# ## 19. Xử Lý và Transform Dữ Liệu Selection Method

# In[34]:


from pyspark.sql.functions import regexp_replace, trim, row_number
from pyspark.sql.window import Window

# Transform dữ liệu để phù hợp với schema Silver
print("🔄 Đang xử lý dữ liệu Selection Method...")

try:
    # Thêm timestamp cho created_at và updated_at
    current_ts = current_timestamp()

    # Lấy cột PhuongThuc và loại bỏ phần "năm ..." 
    # VD: "Xét tuyển theo điểm thi THPT năm 2023" -> "Xét tuyển theo điểm thi THPT"
    df_selection_method = df_benchmark_bronze.select(
        regexp_replace(col("PhuongThuc"), r"\s*năm\s+\d{4}.*$", "").alias("selectionMethodName_raw")
    ).distinct()

    # Loại bỏ khoảng trắng thừa và filter null
    df_selection_method = df_selection_method.withColumn(
        "selectionMethodName",
        trim(col("selectionMethodName_raw"))
    ).filter(
        col("selectionMethodName").isNotNull() & 
        (col("selectionMethodName") != "")
    ).select("selectionMethodName").distinct()

    # Tạo selectionMethodId tự động bằng row_number
    window_spec = Window.orderBy("selectionMethodName")
    df_selection_method_silver = df_selection_method.withColumn(
        "selectionMethodId",
        row_number().over(window_spec)
    ).select(
        col("selectionMethodId").cast("int"),
        col("selectionMethodName").cast("string"),
        current_ts.alias("created_at"),
        current_ts.alias("updated_at")
    )

    print(f"✅ Đã xử lý xong {df_selection_method_silver.count()} phương thức xét tuyển")
    print("\n📊 Schema của dữ liệu Silver:")
    df_selection_method_silver.printSchema()
    print("\n🔍 Tất cả các phương thức xét tuyển sau khi xử lý:")
    df_selection_method_silver.orderBy("selectionMethodId").show(100, truncate=False)

except Exception as e:
    print(f"❌ Lỗi khi xử lý dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# ## 20. Load Dữ Liệu vào Bảng Selection Method trong Silver Layer

# In[35]:


# Load dữ liệu vào bảng Iceberg trong Silver layer
print("💾 Đang ghi dữ liệu vào bảng nessie.silver_tables.selection_method...")

try:
    df_selection_method_silver.writeTo("nessie.silver_tables.selection_method") \
        .using("iceberg") \
        .createOrReplace()

    print("✅ Đã ghi dữ liệu thành công vào bảng selection_method!")
    print(f"📊 Tổng số dòng đã ghi: {df_selection_method_silver.count()}")

except Exception as e:
    print(f"❌ Lỗi khi ghi dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# ## 21. Kiểm Tra Dữ Liệu Selection Method Đã Load vào Silver Layer

# In[36]:


# Đọc và kiểm tra dữ liệu từ bảng Silver
print("🔍 Kiểm tra dữ liệu trong bảng nessie.silver_tables.selection_method...")

try:
    df_verify_selection_method = spark.table("nessie.silver_tables.selection_method")

    print(f"\n📊 Tổng số dòng trong bảng: {df_verify_selection_method.count()}")
    print("\n🔍 Schema của bảng:")
    df_verify_selection_method.printSchema()

    print("\n🔍 Tất cả các phương thức xét tuyển:")
    df_verify_selection_method.orderBy("selectionMethodId").show(100, truncate=False)

    # Thống kê
    print("\n📊 Thống kê:")
    print(f"Tổng số phương thức xét tuyển: {df_verify_selection_method.count()}")

except Exception as e:
    print(f"❌ Lỗi khi đọc dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# In[ ]:





# ---
# 
# # LOAD DỮ LIỆU BẢNG BENCHMARK (ĐIỂM CHUẨN)
# 
# ---

# ## 22. Load Dữ Liệu Benchmark từ Bronze Layer

# In[37]:


# Đường dẫn đến file CSV điểm chuẩn trong Bronze layer trên MinIO
bronze_benchmark_path = "s3a://bronze/structured_data/điểm chuẩn các trường (2021-2025)/Điểm_chuẩn_các_ngành_đại_học_năm(2021-2025)*.csv"

# Đọc dữ liệu từ Bronze layer
print("📖 Đang đọc dữ liệu Benchmark từ Bronze layer...")

try:
    df_benchmark_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("encoding", "UTF-8") \
        .csv(bronze_benchmark_path)

    print(f"✅ Đã đọc được {df_benchmark_raw.count()} dòng dữ liệu")
    print("\n📊 Schema của dữ liệu Bronze:")
    df_benchmark_raw.printSchema()

    print("\n📋 Các cột trong file:")
    print(df_benchmark_raw.columns)

    print("\n🔍 Xem 10 dòng đầu tiên:")
    df_benchmark_raw.show(10, truncate=False)

except Exception as e:
    print(f"❌ Lỗi khi đọc dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# ## 23. Xử Lý và Transform Dữ Liệu Benchmark

# In[38]:


from pyspark.sql.functions import regexp_replace, trim, monotonically_increasing_id, lit

# Transform dữ liệu để phù hợp với schema Silver
print("🔄 Đang xử lý dữ liệu Benchmark...")

try:
    # Thêm timestamp cho created_at và updated_at
    current_ts = current_timestamp()

    # Các cột trong file CSV:
    # STT, Nam, MaTruong, PhuongThuc, MaNganh, TenNganh, KhoiThi, DiemChuan, GhiChu

    # Bước 1: Xử lý PhuongThuc - loại bỏ phần "năm..."
    df_benchmark_processed = df_benchmark_raw.withColumn(
        "PhuongThuc_cleaned",
        trim(regexp_replace(col("PhuongThuc"), r"\s*năm\s+\d{4}.*$", ""))
    )

    # Bước 2: Join với bảng selection_method để lấy selectionMethodId
    df_selection_method_lookup = spark.table("nessie.silver_tables.selection_method")

    df_benchmark_with_method = df_benchmark_processed.join(
        df_selection_method_lookup,
        df_benchmark_processed["PhuongThuc_cleaned"] == df_selection_method_lookup["selectionMethodName"],
        "left"
    )

    # Bước 3: Lấy subjectGroupId từ bảng subject_group dựa trên KhoiThi
    df_subject_group_lookup = spark.table("nessie.silver_tables.subject_group")

    df_benchmark_with_subject = df_benchmark_with_method.join(
        df_subject_group_lookup,
        df_benchmark_with_method["KhoiThi"] == df_subject_group_lookup["subjectGroupName"],
        "left"
    )

    # Bước 4: Select và cast các cột theo schema Silver
    df_benchmark_silver = df_benchmark_with_subject.select(
        col("MaTruong").cast("string").alias("schoolId"),
        col("MaNganh").cast("string").alias("majorId"),
        col("subjectGroupId").cast("int"),
        col("selectionMethodId").cast("int"),
        col("Nam").cast("int").alias("year"),
        col("DiemChuan").cast("double").alias("score"),
        current_ts.alias("created_at"),
        current_ts.alias("updated_at")
    )

    # Bước 5: Làm sạch dữ liệu - loại bỏ các dòng có giá trị null ở các cột quan trọng
    df_benchmark_silver = df_benchmark_silver.filter(
        col("schoolId").isNotNull() & 
        col("majorId").isNotNull() &
        col("year").isNotNull() &
        col("score").isNotNull() &
        col("selectionMethodId").isNotNull() &
        col("subjectGroupId").isNotNull()
    )

    # Bước 6: Thêm benchmarkId tự động tăng
    df_benchmark_silver = df_benchmark_silver.withColumn(
        "benchmarkId",
        monotonically_increasing_id().cast("int")
    ).select(
        "benchmarkId",
        "schoolId",
        "majorId",
        "subjectGroupId",
        "selectionMethodId",
        "year",
        "score",
        "created_at",
        "updated_at"
    )

    # Bước 7: Lọc unique để tránh trùng lặp
    print("\n🔄 Đang lọc dữ liệu unique...")
    df_benchmark_silver = df_benchmark_silver.dropDuplicates([
        "schoolId", "majorId", "subjectGroupId", "selectionMethodId", "year"
    ])

    print(f"✅ Đã xử lý xong {df_benchmark_silver.count()} dòng dữ liệu")
    print("\n📊 Schema của dữ liệu Silver:")
    df_benchmark_silver.printSchema()

    print("\n🔍 Xem 20 dòng đầu tiên sau khi xử lý:")
    df_benchmark_silver.orderBy("year", "schoolId").show(20, truncate=False)

    # Thống kê theo năm
    print("\n📊 Thống kê theo năm:")
    df_benchmark_silver.groupBy("year").count().orderBy("year").show()

    # Thống kê điểm chuẩn
    print("\n📊 Thống kê điểm chuẩn:")
    df_benchmark_silver.select(
        avg("score").alias("Điểm TB"),
        min("score").alias("Điểm Min"),
        max("score").alias("Điểm Max")
    ).show()

except Exception as e:
    print(f"❌ Lỗi khi xử lý dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# ## 24. Load Dữ Liệu vào Bảng Benchmark trong Silver Layer

# In[39]:


# Load dữ liệu vào bảng Iceberg trong Silver layer
print("💾 Đang ghi dữ liệu vào bảng nessie.silver_tables.benchmark...")

try:
    # Ghi dữ liệu vào bảng Iceberg với partitioning theo năm
    df_benchmark_silver.writeTo("nessie.silver_tables.benchmark") \
        .using("iceberg") \
        .createOrReplace()

    print("✅ Đã ghi dữ liệu thành công vào bảng benchmark!")
    print(f"📊 Tổng số dòng đã ghi: {df_benchmark_silver.count()}")

except Exception as e:
    print(f"❌ Lỗi khi ghi dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# ## 25. Kiểm Tra Dữ Liệu Benchmark Đã Load vào Silver Layer

# In[40]:


# Đọc và kiểm tra dữ liệu từ bảng Silver
print("🔍 Kiểm tra dữ liệu trong bảng nessie.silver_tables.benchmark...")

try:
    df_verify_benchmark = spark.table("nessie.silver_tables.benchmark")

    print(f"\n📊 Tổng số dòng trong bảng: {df_verify_benchmark.count()}")
    print("\n🔍 Schema của bảng:")
    df_verify_benchmark.printSchema()

    print("\n🔍 20 dòng đầu tiên:")
    df_verify_benchmark.orderBy("year", "score").show(20, truncate=False)

    print("\n📈 Thống kê theo năm:")
    df_verify_benchmark.groupBy("year") \
        .agg(
            count("*").alias("Số lượng"),
            avg("score").alias("Điểm TB"),
            min("score").alias("Điểm Min"),
            max("score").alias("Điểm Max")
        ) \
        .orderBy("year") \
        .show()

    print("\n📈 Top 10 điểm chuẩn cao nhất năm 2025:")
    df_verify_benchmark.filter(col("year") == 2025) \
        .orderBy(desc("score")) \
        .show(10, truncate=False)

    print("\n📊 Phân bố theo phương thức xét tuyển:")
    df_verify_benchmark.groupBy("selectionMethodId").count() \
        .orderBy(desc("count")) \
        .show(10)

    print("\n📊 Phân bố theo khối thi:")
    df_verify_benchmark.filter(col("subjectGroupId").isNotNull()) \
        .groupBy("subjectGroupId").count() \
        .orderBy(desc("count")) \
        .show(10)

    # Kiểm tra dữ liệu null
    print("\n⚠️ Kiểm tra dữ liệu null/missing:")
    print(f"Số dòng có subjectGroupId = null: {df_verify_benchmark.filter(col('subjectGroupId').isNull()).count()}")
    print(f"Số dòng có selectionMethodId = null: {df_verify_benchmark.filter(col('selectionMethodId').isNull()).count()}")

except Exception as e:
    print(f"❌ Lỗi khi đọc dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# In[ ]:





# ---
# 
# # LOAD DỮ LIỆU BẢNG REGION (KHU VỰC)
# 
# ---

# ## 26. Load Dữ Liệu Region từ Bronze Layer

# In[41]:


# Đường dẫn đến file CSV region trong Bronze layer trên MinIO
bronze_region_path = "s3a://bronze/structured_data/region.csv"

# Đọc dữ liệu từ Bronze layer
print("📖 Đang đọc dữ liệu Region từ Bronze layer...")

try:
    df_region_bronze = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("encoding", "UTF-8") \
        .csv(bronze_region_path)

    print(f"✅ Đã đọc được {df_region_bronze.count()} dòng dữ liệu")
    print("\n📊 Schema của dữ liệu Bronze:")
    df_region_bronze.printSchema()

    print("\n📋 Các cột trong file:")
    print(df_region_bronze.columns)

    print("\n🔍 Xem tất cả dữ liệu:")
    df_region_bronze.show(100, truncate=False)

except Exception as e:
    print(f"❌ Lỗi khi đọc dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# ## 27. Xử Lý và Transform Dữ Liệu Region

# In[42]:


# Transform dữ liệu để phù hợp với schema Silver
print("🔄 Đang xử lý dữ liệu Region...")

try:
    # Thêm timestamp cho created_at và updated_at
    current_ts = current_timestamp()

    # File có các cột: regionId, regionName (hoặc tương tự)
    # Điều chỉnh tên cột theo file thực tế

    # Nếu file không có tên cột chuẩn, sử dụng index
    df_region_silver = df_region_bronze.select(
        col(df_region_bronze.columns[0]).cast("string").alias("regionId"),
        col(df_region_bronze.columns[1]).cast("string").alias("regionName"),
        current_ts.alias("created_at"),
        current_ts.alias("updated_at")
    )

    # Làm sạch dữ liệu: loại bỏ các dòng có giá trị null ở các cột quan trọng
    df_region_silver = df_region_silver.filter(
        col("regionId").isNotNull() & 
        col("regionName").isNotNull()
    )

    # Lọc unique theo regionId
    print("\n🔄 Đang lọc dữ liệu unique...")
    df_region_silver = df_region_silver.dropDuplicates(["regionId"])

    print(f"✅ Đã xử lý xong {df_region_silver.count()} dòng dữ liệu")
    print("\n📊 Schema của dữ liệu Silver:")
    df_region_silver.printSchema()

    print("\n🔍 Tất cả các khu vực sau khi xử lý:")
    df_region_silver.orderBy("regionId").show(100, truncate=False)

    # Thống kê
    print("\n📊 Thống kê:")
    print(f"Tổng số khu vực: {df_region_silver.count()}")

except Exception as e:
    print(f"❌ Lỗi khi xử lý dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# ## 28. Load Dữ Liệu vào Bảng Region trong Silver Layer

# In[43]:


# Load dữ liệu vào bảng Iceberg trong Silver layer
print("💾 Đang ghi dữ liệu vào bảng nessie.silver_tables.region...")

try:
    df_region_silver.writeTo("nessie.silver_tables.region") \
        .using("iceberg") \
        .createOrReplace()

    print("✅ Đã ghi dữ liệu thành công vào bảng region!")
    print(f"📊 Tổng số dòng đã ghi: {df_region_silver.count()}")

except Exception as e:
    print(f"❌ Lỗi khi ghi dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# ## 29. Kiểm Tra Dữ Liệu Region Đã Load vào Silver Layer

# In[44]:


# Đọc và kiểm tra dữ liệu từ bảng Silver
print("🔍 Kiểm tra dữ liệu trong bảng nessie.silver_tables.region...")

try:
    df_verify_region = spark.table("nessie.silver_tables.region")

    print(f"\n📊 Tổng số dòng trong bảng: {df_verify_region.count()}")
    print("\n🔍 Schema của bảng:")
    df_verify_region.printSchema()

    print("\n🔍 Tất cả các khu vực:")
    df_verify_region.orderBy("regionId").show(100, truncate=False)

    # Thống kê
    print("\n📊 Thống kê:")
    print(f"Tổng số khu vực: {df_verify_region.count()}")

    # Lưu DataFrame để sử dụng cho việc map với student_scores sau này
    print("\n💾 Lưu thông tin region để sử dụng cho mapping...")

except Exception as e:
    print(f"❌ Lỗi khi đọc dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# ---
# 
# # LOAD DỮ LIỆU BẢNG STUDENT_SCORES (ĐIỂM THI SINH VIÊN)
# 
# ---

# ## 30. Load Dữ Liệu Student Scores từ Bronze Layer

# In[65]:


# Đường dẫn đến các thư mục chứa file CSV điểm thi từng năm
# Cấu trúc: bronze/structured_data/điểm từng thi sinh/[2021-2025]/Diem_thi_*.csv
years = [2021, 2022, 2023, 2024, 2025]

print("📖 Đang đọc dữ liệu Student Scores từ Bronze layer (2021-2025)...")

try:
    all_dataframes = []

    for year in years:
        bronze_path = f"s3a://bronze/structured_data/điểm từng thí sinh/{year}/*.csv"
        print(f"\n⏳ Đang đọc dữ liệu năm {year}...")

        try:
            # Đọc tất cả file CSV trong thư mục năm
            df_year = spark.read \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .option("encoding", "UTF-8") \
                .csv(bronze_path)

            # Thêm cột năm để phân biệt
            df_year = df_year.withColumn("Year", lit(year))

            count = df_year.count()
            print(f"  ✅ Đọc được {count:,} dòng từ năm {year}")

            all_dataframes.append(df_year)

        except Exception as e:
            print(f"  ⚠️ Không tìm thấy hoặc lỗi khi đọc dữ liệu năm {year}: {str(e)}")

    # Ghép tất cả các DataFrame lại
    if all_dataframes:
        print("\n🔗 Đang ghép tất cả dữ liệu các năm...")
        df_student_scores_bronze = all_dataframes[0]
        for df in all_dataframes[1:]:
            df_student_scores_bronze = df_student_scores_bronze.union(df)

        total_count = df_student_scores_bronze.count()
        print(f"✅ Tổng số dòng sau khi ghép: {total_count:,}")

        print("\n📊 Schema của dữ liệu Bronze:")
        df_student_scores_bronze.printSchema()

        print("\n📋 Các cột trong file:")
        print(df_student_scores_bronze.columns)

        print("\n🔍 Xem 10 dòng đầu tiên:")
        df_student_scores_bronze.show(10, truncate=False)

    else:
        print("❌ Không đọc được dữ liệu từ bất kỳ năm nào!")

except Exception as e:
    print(f"❌ Lỗi khi đọc dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# ## 31. Xử Lý và Transform Dữ Liệu Student Scores

# In[66]:


from pyspark.sql.functions import split, expr, create_map, concat, lpad
from pyspark.sql.types import MapType, StringType, DoubleType
from itertools import chain

# Transform dữ liệu để phù hợp với schema Silver
print("🔄 Đang xử lý dữ liệu Student Scores...")

try:
    # Thêm timestamp
    current_ts = current_timestamp()

    # Bước 1: Tạo studentId = SBD + Year (VD: 010000012021)
    df_processed = df_student_scores_bronze.withColumn(
        "studentId",
        concat(col("SBD"), col("Year").cast("string"))
    )
    print("\n🔍 Xem 2 dòng sau khi tạo studentId:")
    df_processed.show(2, truncate=False)

    # Bước 2: Xử lý cột DiemThi - chuyển từ string sang MAP
    # Format: "Toán:2.2,Văn:3.5,Sử:2.5,Địa:5.5"
    # Cần chuyển thành: {"Toán": 2.2, "Văn": 3.5, "Sử": 2.5, "Địa": 5.5}

    # Split string thành array các cặp "môn:điểm"
    df_processed = df_processed.withColumn(
        "score_pairs",
        split(col("DiemThi"), ",")
    )


    # UDF để parse điểm từ string sang MAP
    from pyspark.sql.functions import udf
    from typing import Dict

    def parse_scores(score_string: str) -> Dict[str, float]:
        """Parse score string to dictionary"""
        if not score_string or score_string.strip() == "":
            return {}

        scores_dict = {}
        try:
            # Split by comma
            pairs = score_string.split(",")
            for pair in pairs:
                if ":" in pair:
                    subject, score = pair.split(":")
                    subject = subject.strip()
                    try:
                        scores_dict[subject] = float(score.strip())
                    except ValueError:
                        # Nếu không parse được điểm, bỏ qua
                        pass
        except Exception:
            pass

        return scores_dict

    # Đăng ký UDF
    parse_scores_udf = udf(parse_scores, MapType(StringType(), DoubleType()))

    # Apply UDF để tạo MAP scores
    df_processed = df_processed.withColumn(
        "scores",
        parse_scores_udf(col("DiemThi"))
    )
    print("\n🔍 Xem 2 dòng sau khi xử lý cột scores:")
    df_processed.show(1, truncate=False)
    # Bước 3: Xử lý cột SBD để lấy regionId
    # 2 ký tự đầu tiên của SBD chính là mã khu vực (regionId)
    # VD: SBD = "01000001" -> regionId = "01"
    df_processed = df_processed.withColumn(
        "regionId",
        substring(col("SBD"), 1, 2).cast("string")
    )
    print("\n🔍 Xem 2 dòng sau khi xử lý cột regionId:")
    df_processed.show(2, truncate=False)
    # Bước 4: Select các cột theo schema Silver
    df_student_scores_silver = df_processed.select(
        col("studentId").cast("string"),
        col("regionId").cast("string"),
        col("Year").cast("int").alias("year"),
        col("scores"),
        current_ts.alias("created_at"),
        current_ts.alias("updated_at")
    )

    # Bước 5: Làm sạch dữ liệu
    df_student_scores_silver = df_student_scores_silver.filter(
        col("studentId").isNotNull() & 
        col("year").isNotNull() &
        col("scores").isNotNull()
    )

    # Bước 6: Lọc unique theo studentId
    print("\n🔄 Đang lọc dữ liệu unique...")
    df_student_scores_silver = df_student_scores_silver.dropDuplicates(["studentId"])

    print(f"✅ Đã xử lý xong {df_student_scores_silver.count():,} dòng dữ liệu")
    print("\n📊 Schema của dữ liệu Silver:")
    df_student_scores_silver.printSchema()

    print("\n🔍 Xem 10 dòng đầu tiên sau khi xử lý:")
    df_student_scores_silver.show(10, truncate=False)

    # Thống kê theo năm
    print("\n📊 Thống kê theo năm:")
    df_student_scores_silver.groupBy("year").count().orderBy("year").show()

    # Thống kê về số môn thi
    print("\n📊 Thống kê số môn thi:")
    df_student_scores_silver.select(
        size(col("scores")).alias("num_subjects")
    ).groupBy("num_subjects").count().orderBy("num_subjects").show()

except Exception as e:
    print(f"❌ Lỗi khi xử lý dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# ## 32. Load Dữ Liệu vào Bảng Student Scores trong Silver Layer

# In[67]:


# Load dữ liệu vào bảng Iceberg trong Silver layer
print("💾 Đang ghi dữ liệu vào bảng nessie.silver_tables.student_scores...")

try:
    # Ghi dữ liệu vào bảng Iceberg với partitioning theo năm
    df_student_scores_silver.writeTo("nessie.silver_tables.student_scores") \
        .using("iceberg") \
        .createOrReplace()

    print("✅ Đã ghi dữ liệu thành công vào bảng student_scores!")
    print(f"📊 Tổng số dòng đã ghi: {df_student_scores_silver.count():,}")

except Exception as e:
    print(f"❌ Lỗi khi ghi dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# In[68]:


# Đọc và kiểm tra dữ liệu từ bảng Silver
print("🔍 Kiểm tra dữ liệu trong bảng nessie.silver_tables.student_scores...")

try:
    df_verify_student_scores = spark.table("nessie.silver_tables.student_scores")

    print(f"\n📊 Tổng số dòng trong bảng: {df_verify_student_scores.count():,}")
    print("\n🔍 Schema của bảng:")
    df_verify_student_scores.printSchema()

    print("\n🔍 20 dòng đầu tiên:")
    df_verify_student_scores.orderBy("year", "studentId").show(20, truncate=False)

    # print("\n📈 Thống kê theo năm:")
    # df_verify_student_scores.groupBy("year") \
    #     .agg(
    #         count("*").alias("Số thí sinh")
    #     ) \
    #     .orderBy("year") \
    #     .show()

    # print("\n📊 Phân bố số môn thi:")
    # df_verify_student_scores.select(
    #     size(col("scores")).alias("num_subjects")
    # ).groupBy("num_subjects") \
    #     .count() \
    #     .orderBy("num_subjects") \
    #     .show()

    print("\n🔍 Xem chi tiết điểm một vài thí sinh:")
    df_verify_student_scores.select(
        "studentId",
        "year",
        "scores"
    ).show(10, truncate=False)

    # # Thống kê về điểm số (ví dụ: môn Toán)
    # print("\n📊 Thống kê điểm môn Toán (nếu có):")
    # df_verify_student_scores.select(
    #     col("scores")["Toán"].alias("diem_toan")
    # ).filter(
    #     col("diem_toan").isNotNull()
    # ).select(
    #     avg("diem_toan").alias("Điểm TB Toán"),
    #     min("diem_toan").alias("Điểm Min Toán"),
    #     max("diem_toan").alias("Điểm Max Toán")
    # ).show()

    # # Kiểm tra dữ liệu null
    # print("\n⚠️ Kiểm tra dữ liệu null/missing:")
    # print(f"Số dòng có regionId = null: {df_verify_student_scores.filter(col('regionId').isNull()).count():,}")
    # print(f"Số dòng có scores rỗng: {df_verify_student_scores.filter(size(col('scores')) == 0).count():,}")

    # # Thống kê các môn thi phổ biến
    # print("\n📊 Các môn thi có trong dữ liệu (lấy mẫu):")
    # sample_df = df_verify_student_scores.limit(1000)
    # # Lấy tất cả keys từ MAP scores
    # all_keys = sample_df.select(explode(map_keys(col("scores")))).distinct()
    # print("Các môn thi:")
    # all_keys.show(50, truncate=False)

except Exception as e:
    print(f"❌ Lỗi khi đọc dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# In[ ]:





# ---
# 
# # LOAD DỮ LIỆU BẢNG SUBJECT (MÔN HỌC)
# 
# ---

# ## 34. Load và Tách Dữ Liệu Subject từ Bronze Layer

# In[69]:


# Đường dẫn đến file CSV trong Bronze layer
bronze_subject_group_path = "s3a://bronze/structured_data/tohop_mon.csv"

print("📖 Đang đọc dữ liệu Subject từ Bronze layer...")

try:
    # Đọc file tohop_mon.csv
    df_tohop_mon = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("encoding", "UTF-8") \
        .csv(bronze_subject_group_path)

    print(f"✅ Đã đọc được {df_tohop_mon.count()} dòng dữ liệu")
    print("\n📊 Schema của dữ liệu:")
    df_tohop_mon.printSchema()

    print("\n📋 Các cột trong file:")
    print(df_tohop_mon.columns)

    print("\n🔍 Xem dữ liệu mẫu:")
    df_tohop_mon.show(10, truncate=False)

    # Lấy cột "Môn chi tiết" (cột thứ 3)
    mon_chi_tiet_col = df_tohop_mon.columns[2]
    print(f"\n📌 Tên cột môn chi tiết: {mon_chi_tiet_col}")

    # Lấy tất cả các giá trị môn chi tiết
    df_mon_chi_tiet = df_tohop_mon.select(col(mon_chi_tiet_col).alias("monChiTiet"))

    print("\n🔍 Các tổ hợp môn:")
    df_mon_chi_tiet.show(20, truncate=False)

except Exception as e:
    print(f"❌ Lỗi khi đọc dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# ## 35. Xử Lý và Tách Môn Học từ Cột Môn Chi Tiết

# In[70]:


from pyspark.sql.functions import explode, split, trim, row_number
from pyspark.sql.window import Window

print("🔄 Đang xử lý và tách môn học...")

try:
    # Bước 1: Tách chuỗi môn chi tiết thành array
    # VD: "Toán-Ngữ Văn-Tiếng Anh" -> ["Toán", "Ngữ Văn", "Tiếng Anh"]
    df_mon_array = df_mon_chi_tiet.withColumn(
        "mon_array",
        split(col("monChiTiet"), "-")
    )

    print("\n🔍 Sau khi tách thành array:")
    df_mon_array.show(5, truncate=False)

    # Bước 2: Explode array thành các dòng riêng lẻ
    df_mon_exploded = df_mon_array.select(
        explode(col("mon_array")).alias("subjectName")
    )

    print(f"\n📊 Tổng số dòng sau khi explode: {df_mon_exploded.count()}")

    # Bước 3: Trim spaces và lấy unique
    df_mon_unique = df_mon_exploded.withColumn(
        "subjectName",
        trim(col("subjectName"))
    ).filter(
        col("subjectName").isNotNull() & 
        (col("subjectName") != "")
    ).distinct()

    print(f"\n📊 Số lượng môn học unique: {df_mon_unique.count()}")

    print("\n🔍 Danh sách các môn học:")
    df_mon_unique.orderBy("subjectName").show(50, truncate=False)

    # Bước 4: Thêm subjectId tự động
    current_ts = current_timestamp()
    window_spec = Window.orderBy("subjectName")

    df_subject_silver = df_mon_unique.withColumn(
        "subjectId",
        row_number().over(window_spec)
    ).select(
        col("subjectId").cast("int"),
        col("subjectName").cast("string"),
        current_ts.alias("created_at"),
        current_ts.alias("updated_at")
    )

    print(f"\n✅ Đã xử lý xong {df_subject_silver.count()} môn học")
    print("\n📊 Schema của dữ liệu Silver:")
    df_subject_silver.printSchema()

    print("\n🔍 Danh sách môn học với ID:")
    df_subject_silver.orderBy("subjectId").show(50, truncate=False)

except Exception as e:
    print(f"❌ Lỗi khi xử lý dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# ## 36. Load Dữ Liệu vào Bảng Subject trong Silver Layer

# In[71]:


# Load dữ liệu vào bảng Iceberg trong Silver layer
print("💾 Đang ghi dữ liệu vào bảng nessie.silver_tables.subject...")

try:
    df_subject_silver.writeTo("nessie.silver_tables.subject") \
        .using("iceberg") \
        .createOrReplace()

    print("✅ Đã ghi dữ liệu thành công vào bảng subject!")
    print(f"📊 Tổng số dòng đã ghi: {df_subject_silver.count()}")

except Exception as e:
    print(f"❌ Lỗi khi ghi dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# ## 37. Kiểm Tra Dữ Liệu Subject Đã Load vào Silver Layer

# In[72]:


# Đọc và kiểm tra dữ liệu từ bảng Silver
print("🔍 Kiểm tra dữ liệu trong bảng nessie.silver_tables.subject...")

try:
    df_verify_subject = spark.table("nessie.silver_tables.subject")

    print(f"\n📊 Tổng số dòng trong bảng: {df_verify_subject.count()}")
    print("\n🔍 Schema của bảng:")
    df_verify_subject.printSchema()

    print("\n🔍 Tất cả các môn học:")
    df_verify_subject.orderBy("subjectId").show(50, truncate=False)

    # Thống kê
    print("\n📊 Thống kê:")
    print(f"Tổng số môn học: {df_verify_subject.count()}")

    # Liệt kê các môn học theo thứ tự alphabet
    print("\n📋 Danh sách môn học theo thứ tự alphabet:")
    df_verify_subject.orderBy("subjectName").show(50, truncate=False)

except Exception as e:
    print(f"❌ Lỗi khi đọc dữ liệu: {str(e)}")
    import traceback
    traceback.print_exc()


# In[ ]:




