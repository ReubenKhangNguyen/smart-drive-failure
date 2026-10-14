# Khởi động nhanh

Tài liệu này khởi động và kiểm tra hệ thống SMART Drive Failure: HDFS, Spark, Kafka, Airflow, FastAPI và React.

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
| Spark Master | http://localhost:8082 |
| Airflow | http://localhost:8081 |
| FastAPI | http://localhost:8000/docs |
| React dashboard | http://localhost:5173 |

`docker compose ps` phải cho thấy NameNode, ba DataNode, Spark Master, hai Spark Worker, Kafka, Postgres, Airflow, `api` và `frontend` ở trạng thái running. Trong Spark Master UI cần có hai worker đã đăng ký. Airflow mặc định dùng `airflow` / `airflow`.

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

## 4. Chạy pipeline

```text
Backblaze CSV
  -> upload HDFS Bronze
  -> PySpark ingest / clean
  -> HDFS Silver Parquet
  -> feature and 7-day label job
  -> Spark MLlib train / evaluate / score
  -> HDFS Gold analytics and predictions
  -> FastAPI -> React dashboard
```

Chạy ETL và Gold từ Spark client:

```bash
docker compose exec spark-client /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 /opt/project/pipeline/batch_pipeline.py
```

Sau khi có Gold features, chạy huấn luyện:

```bash
docker compose exec spark-client /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 /opt/project/pipeline/training_pipeline.py
```

Mỗi job submit tới `spark://spark-master:7077`, đọc và ghi HDFS qua hostname nội bộ `namenode`. Dùng `hdfs dfsadmin -report`, `hdfs fsck`, log `ETL_RESULT` và Spark UI làm minh chứng.

## 5. K-Means phân cụm sức khỏe ổ cứng

RFM chỉ phù hợp dữ liệu giao dịch. Đồ án dùng K-Means để gom ổ cứng theo các chỉ số S.M.A.R.T. tại lần quan sát mới nhất.

```powershell
docker compose exec spark-client /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 `
  /opt/project/analytics/kmeans_health_clusters.py --k 3
```

Kết quả nằm tại `/smart-drive/gold/health-clusters`.

## 6. Kafka Streaming

Trong cửa sổ PowerShell thứ nhất, chạy Spark Structured Streaming:

```powershell
docker compose exec spark-client /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 `
  /opt/project/processing/spark_jobs/kafka_streaming.py
```

Trong cửa sổ thứ hai, gửi 1.000 sự kiện S.M.A.R.T. từ CSV mẫu:

```powershell
docker compose run --rm kafka-producer --limit 1000 --delay-seconds 0.01
```

Kiểm tra topic, dữ liệu Parquet và checkpoint:

```powershell
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list
docker compose exec namenode hdfs dfs -ls -R /smart-drive/bronze/kafka-events
docker compose exec namenode hdfs dfs -ls -R /smart-drive/checkpoints/kafka-events
```

Nhấn `Ctrl+C` tại cửa sổ streaming sau khi có output.

## 7. Airflow DAG

Airflow có DAG `smart_drive_batch`, chạy theo thứ tự **Bronze → Silver → Gold survey → K-Means**. Mở `http://localhost:8081`, bật DAG và chọn **Trigger DAG**. DAG không có lịch tự động để tránh ghi đè dữ liệu khi demo.

## Dừng hệ thống

```bash
docker compose down
```

Lệnh trên giữ các named volume HDFS. Chỉ dùng `docker compose down -v` khi chủ động muốn xóa toàn bộ dữ liệu HDFS và khởi tạo lại cluster.
