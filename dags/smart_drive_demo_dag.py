"""Airflow DAG that shows the course's nine steps running one after another, on the isolated demo area.

    prepare_hdfs -> upload_data -> read_spark -> clean_data -> spark_sql -> kmeans -> build_features
                 -> train_model -> score_days -> save_results -> kafka_produce -> kafka_consume -> finish_summary

Every task runs scripts/run_demo_step.py with config/demo.yaml (28 days of Q1 under /smart-drive-demo), so the real
Bronze/Silver/Gold, the official model and the real reports are never touched. Each task prints what it does and
writes evidence the dashboard page "Quy trình 9 bước" shows live. The DAG contains no pipeline logic: see
pipeline/demo_spec.py and pipeline/demo_steps.py. The Kafka tasks need Kafka running first:
`docker compose --profile streaming up -d zookeeper kafka`.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta

sys.path.insert(0, "/opt/smart-drive")  # the repo mount; pipeline.demo_spec imports neither Airflow nor Spark

from airflow import DAG  # noqa: E402
from airflow.operators.bash import BashOperator  # noqa: E402

from pipeline.demo_spec import DEMO_DAG_ID, DEMO_POOL, DEMO_STEPS, demo_command  # noqa: E402

with DAG(
    dag_id=DEMO_DAG_ID,
    description="Demo: the nine steps (HDFS, upload, Spark read, clean, SQL, K-Means and ML, save, Kafka, Airflow) on a small isolated dataset",
    schedule=None,  # manual runs only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    dagrun_timeout=timedelta(minutes=180),
    default_args={"retries": 0},  # a retry would silently repeat a Spark job in the middle of a demo
    tags=["smart-drive", "demo"],
) as dag:
    previous = None
    for step in DEMO_STEPS:
        task = BashOperator(
            task_id=step.task_id,
            bash_command=demo_command(step),
            execution_timeout=timedelta(minutes=step.timeout_minutes),
            pool=DEMO_POOL if step.uses_spark else "default_pool",
        )
        if previous is not None:
            previous >> task
        previous = task
