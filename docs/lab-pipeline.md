# Pipeline thực hành 9 bước

| Bước của lab | Cách triển khai trong SMART Drive Failure | Minh chứng |
| --- | --- | --- |
| 1. Chuẩn bị HDFS | Tạo Bronze, Silver, Gold, checkpoints trên HDFS | `hdfs dfs -ls -R /smart-drive` |
| 2. Upload dữ liệu | CSV Backblaze Q1/2026 được đưa vào Bronze | `hdfs dfs -ls /smart-drive/bronze/year=2026/quarter=Q1` |
| 3. Đọc bằng Spark | PySpark đọc CSV trực tiếp từ HDFS | Spark Master UI và log job |
| 4. Làm sạch dữ liệu | `smart_etl.py` ép kiểu, kiểm tra null/duplicate và ghi Parquet Silver | `ETL_RESULT`, Silver Parquet |
| 5. Spark SQL | SQL tổng hợp số bản ghi, lỗi theo ngày/model và chỉ số S.M.A.R.T. | PySpark SQL output |
| 6. K-Means | `kmeans_health_clusters.py` nhóm ổ đĩa theo chỉ số S.M.A.R.T. | Gold `health-clusters` |
| 7. Lưu kết quả | Silver/Gold dùng Parquet và HDFS replication factor 2 | HDFS `fsck` |
| 8. Kafka ingest | Producer JSON → Kafka → Spark Structured Streaming → HDFS Parquet + checkpoint | topic, Spark UI, HDFS output |
| 9. Airflow DAG | DAG `smart_drive_batch` chạy ETL → Survey → K-Means | Airflow Graph view và task log |

K-Means chỉ mô tả các nhóm hành vi sức khỏe. Mô hình dự đoán hỏng dùng bảng Gold có nhãn `fail_within_7_days` ở giai đoạn ML riêng.
