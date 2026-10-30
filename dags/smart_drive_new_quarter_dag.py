"""Airflow DAG for ONE new out-of-time quarter, run by hand with the quarter as a parameter (no schedule).

    verify_bronze_quarter -> profile_schema -> clean_silver_quarter -> build_quarter_features
                          -> evaluate_quarter -> score_all_days -> export_dashboard

    airflow dags trigger smart_drive_new_quarter --conf '{"quarter": "2026-Q3"}'

The quarter must already be declared in config/project.yaml (data.quarters). The DAG only calls scripts that already
exist (see pipeline/dag_spec.py); it contains no pipeline logic. verify_bronze_quarter only CHECKS that the quarter's
Bronze is loaded: loading it is the manual `scripts/upload_to_hdfs.py --quarter ...` step on the host. The frozen
model is never trained here, and a quarter that already has its evaluation report is not evaluated again.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta

sys.path.insert(0, "/opt/smart-drive")  # the repo mount; pipeline.dag_spec imports neither Airflow nor Spark

from airflow import DAG  # noqa: E402
from airflow.models.param import Param  # noqa: E402
from airflow.operators.bash import BashOperator  # noqa: E402

from pipeline.dag_spec import POOL, QUARTER_DAG_ID, QUARTER_TASKS, bash_command  # noqa: E402

with DAG(
    dag_id=QUARTER_DAG_ID,
    description="One new out-of-time quarter: Bronze check -> Silver -> features -> frozen-model evaluation -> daily scores -> dashboard export",
    schedule=None,  # manual runs only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    dagrun_timeout=timedelta(minutes=240),
    default_args={"retries": 0},  # a retry would silently repeat a Spark job
    params={"quarter": Param(type="string", pattern=r"^[0-9]{4}-Q[1-4]$", description="Quarter id declared in data.quarters, e.g. 2026-Q3")},
    tags=["smart-drive", "multi-quarter"],
) as dag:
    previous = None
    for spec in QUARTER_TASKS:
        task = BashOperator(
            task_id=spec.task_id,
            bash_command=bash_command(spec),
            execution_timeout=timedelta(minutes=spec.timeout_minutes),
            pool=POOL if spec.uses_spark else "default_pool",
        )
        if previous is not None:
            previous >> task
        previous = task
