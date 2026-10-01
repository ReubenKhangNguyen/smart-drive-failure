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

`spark-master`/`spark-worker1`/`spark-worker2` giờ build từ `Dockerfile.spark` (base `apache/spark:3.5.1` + `numpy`, cần cho `pyspark.ml`) thay vì kéo thẳng image gốc — `docker compose up -d --build` tự build lần đầu; nếu cụm đã chạy sẵn từ trước và chỉ `git pull` code mới, phải chạy `docker compose build spark-master spark-worker1 spark-worker2` rồi `docker compose up -d` lại thì thay đổi mới có hiệu lực.

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

## 5. Chạy test

Test chạy trong container `tests` (profile `test`), dựa trên `apache/spark:3.5.1` nên PySpark khớp đúng phiên bản cụm — không cần cài PySpark/JDK/winutils trên máy host:

```bash
docker compose run --rm tests python3 -m pytest -q
```

Container này mount code ở chế độ chỉ đọc và mount ghi được `./artifacts:/opt/smart-drive/artifacts`, dùng khi test cần đọc/ghi báo cáo.

## 6. Dữ liệu ngoài repo

Khi dữ liệu Backblaze đã tải về nằm ngoài `D:\Projects\smart-drive-failure` (ví dụ ổ D: không đủ chỗ), mount trực tiếp thư mục đó vào `namenode` thay vì copy vào repo. Ví dụ `docker-compose.yml`:

```yaml
namenode:
  volumes:
    - namenode_data:/hadoop/dfs/name
    - "C:/dataset_smart_drive_failure/data_Q1_2026:/external_data:ro"
```

Sau khi thêm, kiểm tra cú pháp trước khi áp dụng:

```bash
MSYS_NO_PATHCONV=1 docker compose config
docker compose up -d namenode
docker compose exec namenode ls /external_data
```

Script nạp HDFS (`ingestion/hdfs_loader.py`, Phase 2) đọc từ đường dẫn mount này thay vì `dataset/raw/` trong repo.

## 7. Demo streaming Kafka (Phase 2.b, lớp trình diễn bổ sung)

Mô phỏng luồng SMART hằng ngày qua Kafka + Spark Structured Streaming. **Batch pipeline (Bronze → Silver → Gold qua Spark) vẫn là nguồn chính cho phân tích và huấn luyện** — đây chỉ là lớp trình diễn bổ sung minh họa khả năng chấm điểm gần thời gian thực, không thay thế batch.

Kiểm tra RAM còn trống trước khi bật (Kafka + Zookeeper cần thêm ~1-2 GB):

```bash
docker stats --no-stream
```

Bật profile `streaming` (không tự khởi động cùng `docker compose up -d` mặc định):

```bash
docker compose --profile streaming up -d zookeeper kafka
```

Chạy demo (producer phát 1-3 ngày mẫu, consumer Spark Structured Streaming tiêu thụ và ghi `/smart-drive/streaming_output`):

```bash
python scripts/run_streaming_demo.py \
  --host-source-dir "C:/dataset_smart_drive_failure/data_Q1_2026/data_Q1_2026" \
  --dates 2026-01-01
```

`pipeline/streaming_consumer.py` dùng connector Kafka của Spark qua `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1` (tải qua Maven lúc chạy, cần mạng; `scripts/run_streaming_demo.py` đã tự thêm flag này). Logic làm sạch dùng lại `processing/spark_jobs/smart_etl.cast_and_clean_columns` — cùng hàm với batch pipeline (Phase 3), chỉ bỏ bước khử trùng theo khóa vì Spark Structured Streaming không hỗ trợ window không giới hạn thời gian.

Sau khi demo xong, tắt profile để trả lại RAM:

```bash
docker compose --profile streaming stop
```

## Dừng hệ thống

```bash
docker compose down
```

Lệnh trên giữ các named volume HDFS. Chỉ dùng `docker compose down -v` khi chủ động muốn xóa toàn bộ dữ liệu HDFS và khởi tạo lại cluster.

## 8. Dashboard (Phase 8)

Dashboard chỉ đọc `artifacts/dashboard/` (và `artifacts/reports/` cho log pipeline), không đọc HDFS. Tạo các bảng nó cần (chạy tuần tự, trên Git Bash đặt `MSYS_NO_PATHCONV=1`):

```bash
# HĐ6: AFR, phân phối SMART, tín hiệu trước khi hỏng
docker compose exec -e PYTHONPATH=/opt/smart-drive spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/smart-drive/analytics/build_analytics.py
# HĐ7: phân cụm K-Means (chạy riêng, ~8-12 phút)
docker compose exec -e PYTHONPATH=/opt/smart-drive spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/smart-drive/analytics/kmeans_segmentation.py
# HĐ8: bảng tổng hợp, Top-K, metric model (cần Gold predictions đã chấm điểm)
docker compose exec -e PYTHONPATH=/opt/smart-drive spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/smart-drive/analytics/export_dashboard.py
docker compose restart ui-dashboard
```

Mở http://localhost:8501. Bảng nào chưa có thì trang tương ứng hiện hướng dẫn tạo bảng đó, không báo lỗi. Test dashboard chạy trong container `tests` (đã có `streamlit==1.38.0`): `docker compose run --rm tests python3 -m pytest -q tests/test_dashboard_data.py tests/test_dashboard_pages.py`.

## 9. Benchmark Big Data (Phase 9)

Benchmark chỉ đo được khi cụm rảnh: **không chạy pytest, K-Means, export hay pipeline song song**, và đóng các ứng dụng nặng trên máy host (RAM trống ít sẽ làm sai số đo). Runner tự dừng (mã thoát 2) nếu master Spark đang chạy ứng dụng khác. Chạy tuần tự; mỗi lệnh dưới đây dùng chung tiền tố:

```bash
SUBMIT="docker compose exec -e PYTHONPATH=/opt/smart-drive spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --executor-cores 2 --executor-memory 1g"
python scripts/benchmark_env.py                      # trên máy host, ngay trước khi đo: RAM, CPU, Docker
$SUBMIT --total-executor-cores 4 /opt/smart-drive/scripts/run_benchmark.py --experiment format --scope 7d
$SUBMIT --total-executor-cores 4 /opt/smart-drive/scripts/run_benchmark.py --experiment format --scope q1   # CSV toàn Q1 (lâu)
$SUBMIT --total-executor-cores 4 /opt/smart-drive/scripts/run_benchmark.py --experiment small_files
for W in csv_7d silver_q1; do
  $SUBMIT --total-executor-cores 2 /opt/smart-drive/scripts/run_benchmark.py --experiment workers --label cores_2_executors_1 --workload $W
  $SUBMIT --total-executor-cores 4 /opt/smart-drive/scripts/run_benchmark.py --experiment workers --label cores_4_executors_2 --workload $W
done
$SUBMIT --total-executor-cores 2 /opt/smart-drive/scripts/build_benchmark_report.py   # gộp -> benchmark.md + 2 bảng HĐ9
docker compose restart ui-dashboard
```

Giới hạn số worker bằng `--total-executor-cores` (2 core = 1 executor trên một worker, 4 core = 2 executor), không dừng hay xóa container nào; runner ghi số executor thật và đánh dấu không hợp lệ nếu không đúng nhãn. Bản sao tạm của benchmark nằm ở `/smart-drive/benchmark_tmp` (dùng lại nếu đã có, không bao giờ tự ghi đè hay xóa; dọn khi nhóm đồng ý). Kết quả: `artifacts/reports/benchmark.md`, `artifacts/dashboard/benchmark.parquet` và `benchmark_environment.parquet`; xem ở trang Hiệu năng cụm.

## 10. Airflow DAG (Phase 7.b, bước 9 của giảng viên)

DAG `smart_drive_pipeline` điều phối cả dự án bằng cách gọi lại các script có sẵn (không viết lại logic), chạy thủ công (không có lịch). Tám task tuần tự:

| Task | Gọi | Ghi chú |
|---|---|---|
| `verify_bronze` | `scripts/verify_bronze.py` | **Chỉ xác nhận** (chỉ đọc, qua WebHDFS) Bronze đã đủ file cho từng ngày; **không nạp dữ liệu** |
| `clean_silver` | `scripts/run_batch_pipeline.py --steps silver_etl` | Bronze → Silver |
| `build_analytics_and_health` | `scripts/run_batch_pipeline.py --steps analytics,health_status` | Gold analytics (HĐ6) và health_status |
| `segment_drives_kmeans` | `analytics/kmeans_segmentation.py` | K-Means (HĐ7), chỉ có trong DAG, không nằm trong `batch_pipeline` |
| `build_features` | `scripts/run_batch_pipeline.py --steps features` | Gold features |
| `train_model` | `scripts/run_training_pipeline.py` | Chỉ train + validation, **không chạy test**, không ghi đè model đã lưu |
| `score_predictions` | `scripts/run_scoring_pipeline.py` | Gold predictions |
| `export_dashboard` | `analytics/export_dashboard.py` | Bảng cho dashboard (HĐ8) |

**Nạp Bronze là bước thủ công** `python scripts/upload_to_hdfs.py ...` chạy trên máy host (script cần docker CLI và đường dẫn dữ liệu trên host, mục 2 ở trên); DAG chỉ xác nhận kết quả. Nếu thiếu file, `verify_bronze` dừng DAG và in hướng dẫn.

Cách làm: Airflow 2.10.5 (bản mới nhất còn hỗ trợ Python 3.8) chạy trong image dựng từ image Spark của dự án (cùng Python 3.8, Java 11, Spark 3.5.1 với cụm), trong venv riêng; mỗi task Spark chạy `spark-submit` ở client mode từ container Airflow tới `spark://spark-master:7077` với executor mặc định (như các lệnh chạy tay). SQLite + SequentialExecutor, chỉ dành cho demo. Không mount docker socket.

Trước khi chạy (RAM Docker 7.36 GiB là giới hạn): đóng ứng dụng nặng trên máy host, cắm sạc, tắt chế độ ngủ, dừng dashboard (`docker compose stop ui-dashboard`, không xóa gì) và không chạy job Spark khác. Mỗi task Spark tự kiểm tra master không có ứng dụng nào khác (`scripts/check_spark_idle.py`) và dùng pool `spark_cluster` 1 slot.

```bash
# 1. Đặt mật khẩu admin demo trong .env (không commit .env): AIRFLOW_ADMIN_PASSWORD=<mật khẩu của bạn>
docker compose build spark-master                     # image Spark gốc (nếu chưa có)
docker compose --profile orchestration build          # image Airflow (lần đầu vài phút)
docker compose stop ui-dashboard
docker compose --profile orchestration up -d          # airflow-init chạy một lần rồi thoát; giao diện: http://127.0.0.1:8081
# 2. Trigger DAG (hoặc bấm Trigger DAG trên giao diện), rồi xem trạng thái
docker compose exec airflow-scheduler airflow dags trigger smart_drive_pipeline
docker compose exec airflow-scheduler airflow dags list-runs -d smart_drive_pipeline
# 3. Dọn: tắt Airflow và bật lại dashboard (không xóa volume)
docker compose --profile orchestration stop airflow-webserver airflow-scheduler
docker compose start ui-dashboard
```

Kiểm tra DAG import không lỗi và đúng thứ tự phụ thuộc (chạy trong container Airflow, vì Airflow không có trong image `tests`): `docker compose --profile orchestration run --rm --no-deps airflow-scheduler /opt/airflow-venv/bin/python -m pytest -q tests/test_dag.py`. Phần còn lại của test DAG (thứ tự task, script tồn tại, không đổi cỡ executor, train không đụng test) nằm trong `tests/test_dag_spec.py` và chạy trong container `tests`.

