# Benchmark Big Data — 2026-10-03

Truy vấn đo: `count+groupBy(model)` (đọc CSV có header, không `inferSchema`). Mỗi biến thể: 1 lần khởi động không tính, rồi 3 lần tính trung vị (kèm min/max); các biến thể xen kẽ (riêng nhóm workers mỗi cấu hình là một lần spark-submit riêng nên không xen kẽ); đo với bộ nhớ đệm hệ điều hành đã ấm. Một lần chạy quá 10 phút thì chỉ chạy 1 lần và ghi rõ ở cột ghi chú. Nhóm `pipeline_steps` lấy từ nhật ký đã có, không chạy lại.

Tóm tắt (tính từ trung vị): Parquet 197 cột nhanh gấp 6.8 lần CSV (cùng 7 ngày); Silver Parquet nhanh gấp 7.3 lần CSV (7 ngày); Silver Parquet nhanh gấp 21.6 lần CSV (toàn Q1); Bản gộp file nhanh gấp 2.55 lần bản gốc.

## Định dạng: CSV so với Parquet

| Biến thể | Truy vấn | Lần | Trung vị (s) | Min (s) | Max (s) | Dung lượng | Số file | Partition | Executor | Số dòng | Ghi chú |
|---|---|---|---|---|---|---|---|---|---|---|---|
| csv_7d | count+groupBy(model) | 3 | 4.33 | 4.00 | 4.45 | 887.7 MB | 7 | 7 | 2 | 2,370,587 | — |
| csv_q1 | count+groupBy(model) | 3 | 46.04 | 45.65 | 48.23 | 11.2 GB | 90 | 92 | 2 | 30,597,484 | — |
| parquet_all_columns_7d | count+groupBy(model) | 3 | 0.63 | 0.59 | 0.78 | 101.4 MB | 7 | 7 | 2 | 2,370,587 | — |
| silver_parquet_7d | count+groupBy(model) | 3 | 0.59 | 0.59 | 0.64 | 22.3 MB | 7 | 4 | 2 | 2,370,587 | — |
| silver_parquet_q1 | count+groupBy(model) | 3 | 2.13 | 2.08 | 2.29 | 287.0 MB | 90 | 5 | 2 | 30,597,484 | — |

## Số worker: 1 so với 2

| Biến thể | Truy vấn | Lần | Trung vị (s) | Min (s) | Max (s) | Dung lượng | Số file | Partition | Executor | Số dòng | Ghi chú |
|---|---|---|---|---|---|---|---|---|---|---|---|
| cores_2_executors_1 | count+groupBy(model) @ csv_7d | 3 | 7.52 | 7.50 | 7.70 | 887.7 MB | 7 | 7 | 1 | 2,370,587 | — |
| cores_2_executors_1 | count+groupBy(model) @ silver_q1 | 3 | 2.62 | 2.52 | 2.84 | 287.0 MB | 90 | 5 | 1 | 30,597,484 | — |
| cores_4_executors_2 | count+groupBy(model) @ csv_7d | 3 | 4.59 | 4.56 | 4.87 | 887.7 MB | 7 | 7 | 2 | 2,370,587 | — |
| cores_4_executors_2 | count+groupBy(model) @ silver_q1 | 3 | 2.41 | 2.11 | 2.51 | 287.0 MB | 90 | 5 | 2 | 30,597,484 | — |

## File nhỏ: Gold features gốc so với bản gộp

| Biến thể | Truy vấn | Lần | Trung vị (s) | Min (s) | Max (s) | Dung lượng | Số file | Partition | Executor | Số dòng | Ghi chú |
|---|---|---|---|---|---|---|---|---|---|---|---|
| features_as_is | count+groupBy(model) | 3 | 3.54 | 3.40 | 3.95 | 545.0 MB | 540 | 21 | 2 | 18,049,584 | — |
| features_coalesced | count+groupBy(model) | 3 | 1.39 | 1.26 | 1.45 | 465.8 MB | 60 | 6 | 2 | 18,049,584 | — |

## Thời gian từng bước pipeline (từ nhật ký đã có)

| Biến thể | Truy vấn | Lần | Trung vị (s) | Min (s) | Max (s) | Dung lượng | Số file | Partition | Executor | Số dòng | Ghi chú |
|---|---|---|---|---|---|---|---|---|---|---|---|
| batch_pipeline/analytics | pipeline step | 1 | 408.99 | 408.99 | 408.99 | — | — | — | — | — | 1 lần, không phải trung vị |
| batch_pipeline/features | pipeline step | 1 | 372.96 | 372.96 | 372.96 | — | — | — | — | — | 1 lần, không phải trung vị |
| batch_pipeline/health_status | pipeline step | 1 | 108.11 | 108.11 | 108.11 | — | — | — | — | — | 1 lần, không phải trung vị |
| batch_pipeline/silver_etl | pipeline step | 1 | 269.35 | 269.35 | 269.35 | — | — | — | — | — | 1 lần, không phải trung vị |
| scoring_pipeline/score | pipeline step | 2 | 32.77 | 27.63 | 37.91 | — | — | — | — | — | 2 lần ghi nhận trong nhật ký pipeline |
| training_pipeline/train_and_evaluate_on_val | pipeline step | 1 | 867.55 | 867.55 | 867.55 | — | — | — | — | — | 1 lần, không phải trung vị |

## Cấu hình máy và cụm

| Mục | Giá trị |
|---|---|
| run_date | 2026-10-03 |
| docker_mem_bytes | 7898546176 |
| docker_ncpu | 12 |
| git_commit | 286970f |
| hdfs_blocksize | 134217728 |
| hdfs_replication | 2 |
| host_cores | 6 |
| host_cpu | AMD Ryzen 5 6600H with Radeon Graphics |
| host_logical_cpus | 12 |
| host_ram_free_gb_at_start | 3.9 |
| host_ram_gb | 15.2 |
| driver_memory | 1g (mặc định) |
| executor_cores | 2 |
| executor_memory | 1g |
| spark_cores_max | 4 |
| spark_master_total_cores | 4 |
| spark_version | 3.5.1 |
| spark_workers | 2 worker, mỗi worker 2 core / 2048 MB |
| input_sizes | cores_2_executors_1=287.0 MB; cores_4_executors_2=287.0 MB; csv_7d=887.7 MB; csv_q1=11.2 GB; features_as_is=545.0 MB; features_coalesced=465.8 MB; parquet_all_columns_7d=101.4 MB; silver_parquet_7d=22.3 MB; silver_parquet_q1=287.0 MB |
| measurement_protocol | 1 lần khởi động không tính + 3 lần tính trung vị, xen kẽ giữa các biến thể, đo với bộ nhớ đệm hệ điều hành đã ấm; một lần chạy >10 phút thì chỉ chạy 1 lần; nhóm workers không xen kẽ (mỗi cấu hình một spark-submit) |
