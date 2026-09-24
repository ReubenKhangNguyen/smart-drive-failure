# Khởi động nhanh

Tài liệu này khởi động hạ tầng MVP cho dự án SMART Drive Failure: HDFS, Spark và Streamlit. Dữ liệu Backblaze và các Spark job sẽ được thêm ở các bước pipeline kế tiếp.

## Điều kiện

- Docker Desktop đang chạy và được cấp tối thiểu 8 GB RAM. Mức 16 GB phù hợp hơn khi dùng cả ba DataNode và hai Spark Worker.
- Docker Compose v2.

## 1. Tạo cấu hình cục bộ

Trên macOS/Linux:

```bash
cp .env.example .env
```

Trên PowerShell:

```powershell
Copy-Item .env.example .env
```

Máy có 8 GB RAM nên đặt hai biến sau trong `.env` trước khi khởi động:

```text
SPARK_WORKER_MEMORY=1G
SPARK_WORKER_CORES=1
```

## 2. Khởi động hệ thống

```bash
docker compose up -d --build
docker compose ps
```

Mở các giao diện sau:

| Thành phần | Địa chỉ |
|---|---|
| HDFS NameNode | http://localhost:9870 |
| Spark Master | http://localhost:8080 |
| Streamlit dashboard | http://localhost:8501 |

`docker compose ps` phải cho thấy NameNode, ba DataNode, Spark Master, hai Spark Worker và `ui-dashboard` ở trạng thái running. Trong Spark Master UI cần có hai worker đã đăng ký.

## 3. Kiểm tra HDFS

Tạo các tầng dữ liệu chuẩn trên HDFS:

```bash
docker compose exec namenode hdfs dfs -mkdir -p \
  /smart-drive/bronze/year=2026/quarter=Q1 \
  /smart-drive/silver/daily \
  /smart-drive/gold/features \
  /smart-drive/gold/analytics \
  /smart-drive/gold/predictions

docker compose exec namenode hdfs dfs -ls -R /smart-drive
```

Đường dẫn mà các Spark job sẽ dùng:

```text
hdfs://namenode:9000/smart-drive/bronze/year=2026/quarter=Q1
hdfs://namenode:9000/smart-drive/silver/daily
hdfs://namenode:9000/smart-drive/gold
```

## 4. Vòng chạy pipeline sau khi có source

```text
Backblaze CSV
  -> upload HDFS Bronze
  -> PySpark ingest / clean
  -> HDFS Silver Parquet
  -> feature and 7-day label job
  -> Spark MLlib train / evaluate / score
  -> HDFS Gold analytics and predictions
  -> Streamlit dashboard
```

Mỗi Spark job cần submit tới `spark://spark-master:7077`, đọc và ghi HDFS qua hostname nội bộ `namenode`. Repo được mount read-only vào container `spark-master` tại `/opt/smart-drive` (working dir mặc định), ví dụ:

```bash
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  processing/spark_jobs/<job>.py
```

## Dừng hệ thống

```bash
docker compose down
```

Lệnh trên giữ các named volume HDFS. Chỉ dùng `docker compose down -v` khi chủ động muốn xóa toàn bộ dữ liệu HDFS và khởi tạo lại cluster.
