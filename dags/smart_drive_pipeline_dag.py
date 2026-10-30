"""Airflow DAG that orchestrates the whole project, run by hand (no schedule).

    verify_bronze -> clean_silver -> build_analytics_and_health -> segment_drives_kmeans
                  -> build_features -> train_model -> score_predictions -> export_dashboard

The DAG only calls scripts that already exist (see pipeline/dag_spec.py); it contains no pipeline logic.
verify_bronze only CHECKS that Bronze is loaded: loading Bronze is the manual `scripts/upload_to_hdfs.py` step on
the host. train_model trains and evaluates on train/validation only; the test split is never touched.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta

sys.path.insert(0, "/opt/smart-drive")  # the repo mount; pipeline.dag_spec imports neither Airflow nor Spark

from airflow import DAG  # noqa: E402
from airflow.operators.bash import BashOperator  # noqa: E402

from pipeline.dag_spec import DAG_ID, POOL, TASKS, bash_command  # noqa: E402

with DAG(
    dag_id=DAG_ID,
    description="Bronze -> Silver -> analytics/health/K-Means -> features -> train -> score -> dashboard export",
    schedule=None,  # manual runs only (the `schedule` argument replaces the deprecated schedule_interval)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    dagrun_timeout=timedelta(minutes=240),
    default_args={"retries": 0},  # a retry would silently repeat a 5-15 minute Spark job
    tags=["smart-drive", "phase-7b"],
) as dag:
    previous = None
    for spec in TASKS:
        task = BashOperator(
            task_id=spec.task_id,
            bash_command=bash_command(spec),
            execution_timeout=timedelta(minutes=spec.timeout_minutes),
            pool=POOL if spec.uses_spark else "default_pool",
        )
        if previous is not None:
            previous >> task
        previous = task
