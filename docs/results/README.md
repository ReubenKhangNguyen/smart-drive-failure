# Results snapshots / Ảnh chụp kết quả

These files are **snapshots of reports produced by real runs** on Backblaze Drive Stats 2026-Q1 (Docker Compose cluster: 1 NameNode, 3 DataNodes, 1 Spark master, 2 workers). They are copied here so they can be read without running the cluster. The working copies are regenerated under `artifacts/reports/` (not tracked by git). Numbers are not edited by hand after copying; if you re-run a job the new output will differ in timings and may differ in other details.

Các file này là **ảnh chụp báo cáo của các lần chạy thật** trên dữ liệu Backblaze Drive Stats 2026-Q1. Bản làm việc được tạo lại trong `artifacts/reports/` (git không theo dõi). Chạy lại job sẽ cho thời gian khác và có thể khác ở một số chi tiết.

| File | Content | How to regenerate (see [`docs/quick-start.md`](../quick-start.md)) |
|---|---|---|
| `data_quality.md` | Rows in/out and duplicates of the Bronze → Silver step (30,597,484 rows) | Bronze → Silver: `scripts/run_batch_pipeline.py --steps silver_etl` (its run log is `pipeline_run_<date>.md`) |
| `eda_summary.md` | SMART analysis: AFR, healthy vs failed, by manufacturer/model | `analytics/smart_analysis.py`, `analytics/failure_analysis.py`, `analytics/drive_model_analysis.py` |
| `health_baseline.md` | Baseline rule `rules_v1` and the Gold `health_status` table | `analytics/health_status.py` (also in `run_batch_pipeline.py --steps health_status`) |
| `dataset_split.md` | Time-based train/validation/test split, warm-up and right-censoring | `features/label.py`, `features/build_features.py` (`run_batch_pipeline.py --steps features`) |
| `kmeans_segmentation.md` | K-Means segmentation of drives (K selection, clusters, health cross-tab) | `analytics/kmeans_segmentation.py` |
| `val_tiebreak_sensitivity.md` | Sensitivity of recall@100 on validation to the tie-break rule | `scripts/check_val_tiebreak.py` |
| `pipeline_run_2026-10-03.md` | Per-step timings and row counts of one end-to-end batch, training and scoring run | `scripts/run_batch_pipeline.py`, `scripts/run_training_pipeline.py`, `scripts/run_scoring_pipeline.py` (or the Airflow DAG) |
| `benchmark.md` | Big Data benchmark (CSV vs Parquet, worker count, small files) on one laptop | `scripts/benchmark_env.py`, `scripts/run_benchmark.py`, `scripts/build_benchmark_report.py` |
| `streaming_demo.md` | Kafka + Spark Structured Streaming demo for one day (2026-01-01) | `scripts/run_streaming_demo.py --start-kafka --stop-kafka` |

Model evaluation (validation and test) is in [`docs/evaluation.md`](../evaluation.md). Dashboard screenshots are in [`docs/images/`](../images/).
