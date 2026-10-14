# SMART Drive Failure

Đây là đồ án **Phân tích dữ liệu S.M.A.R.T. và dự đoán nguy cơ hỏng ổ cứng sử dụng Apache Spark**. Dữ liệu ngày của Backblaze được lưu phân tán trên HDFS, làm sạch bằng PySpark, ghi Silver Parquet, sau đó tạo Gold analytics, đặc trưng và dự đoán nguy cơ hỏng trong 7 ngày.

Hệ thống gồm 1 NameNode, 3 DataNode với replication factor 2, 1 Spark Master, 2 Spark Worker và Spark client. Kafka mô phỏng luồng sự kiện S.M.A.R.T. để Spark Structured Streaming ghi Parquet vào HDFS. Airflow điều phối batch ETL → Gold survey → K-Means. Phần ứng dụng là FastAPI và React/Vite: Spark xuất JSON Gold nhỏ vào `artifacts/dashboard`, FastAPI chỉ đọc các export đó, React gọi API. Không dùng Neo4j hoặc MinIO.

Luồng batch: **Backblaze CSV → HDFS Bronze → Spark Silver Parquet → Gold features/analytics → Spark MLlib → Gold predictions → FastAPI → React**. Luồng streaming: **CSV mẫu → Kafka → Spark Structured Streaming → HDFS Bronze Kafka Parquet**. Nhãn `fail_within_7_days` chỉ dùng failure sau ngày quan sát; `serial_number`, `failure` cùng ngày và dữ liệu tương lai bị loại khỏi đặc trưng. Xem `docs/quick-start.md` để khởi động và lấy minh chứng.
