# TLCN-PhanTichMangXaHoi

## Giới thiệu

Đồ án này xây dựng một hệ thống phân tích mạng xã hội sử dụng kiến trúc Data Lakehouse, kết hợp các công nghệ như Apache Airflow, Apache Spark, Dremio, và các mô hình học sâu (PhoBERT) để thu thập, xử lý, phân tích và trực quan hóa dữ liệu lớn từ mạng xã hội.

## Cấu trúc thư mục chính

- **dremio/**: Cấu hình cho Dremio - nền tảng Data Lakehouse để truy vấn, phân tích dữ liệu.
- **finetune_model/**: Notebook Jupyter để fine-tune các mô hình PhoBERT cho các tác vụ như NER, intent, topic, sentiment.
- **init-db/**: Script SQL khởi tạo nhiều database phục vụ cho các tầng dữ liệu.
- **notebooks/**: Notebook Jupyter mô tả quy trình xử lý dữ liệu ở các tầng (bronze, silver, gold), áp dụng mô hình, mapping thực thể, tổng hợp kết quả.
- **spark/**: Dockerfile, cấu hình và script cho Spark cluster xử lý dữ liệu lớn.
- **docker-compose.yaml**: Khởi tạo toàn bộ hệ thống (Airflow, Spark, Dremio, ...).

## Hướng dẫn khởi động nhanh

1. Cài đặt Docker & Docker Compose.
2. Chạy lệnh sau tại thư mục này:
   ```sh
   docker-compose up -d
   ```
3. Truy cập các dịch vụ:
   - Airflow: http://localhost:8080
   - Dremio: http://localhost:9047
   - Spark (nếu có expose port)

## Tài liệu & Notebook quan trọng
- `notebooks/bronze/`, `notebooks/silver/`, `notebooks/gold/`: Quy trình xử lý dữ liệu các tầng.
- `finetune_model/`: Fine-tune và áp dụng các mô hình NLP tiếng Việt.
- `airflow/dags/`: Các DAGs tự động hóa pipeline ETL.
