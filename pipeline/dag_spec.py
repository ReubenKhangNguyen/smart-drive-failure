"""Definition of the Airflow DAG tasks, kept free of Airflow and Spark imports so it can be tested in the plain
`tests` container. dags/smart_drive_pipeline_dag.py only turns this list into BashOperators chained in order.

The DAG orchestrates; it never re-implements pipeline logic: every task calls an existing script. Spark tasks run
`spark-submit` from the Airflow container (client mode: standalone clusters do not support cluster mode for Python
applications), with the container's system python (/usr/bin/python3, the same 3.8 as the Spark cluster) and the
default executor size, exactly like the commands run by hand in docs/quick-start.md.
"""
from __future__ import annotations

from typing import List, NamedTuple

DAG_ID = "smart_drive_pipeline"
POOL = "spark_cluster"  # 1 slot: at most one Spark task of this DAG at a time
REPO = "/opt/smart-drive"
PYTHON = "/usr/bin/python3"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
SPARK_MASTER = "spark://spark-master:7077"


class TaskSpec(NamedTuple):
    task_id: str
    script: str  # path relative to the repo
    args: str  # extra command line arguments, "" when none
    uses_spark: bool
    timeout_minutes: int


# Order matters: each task depends on the previous one. Timeouts are generous multiples of the durations logged in
# artifacts/reports/pipeline_run_<date>.md (silver 269s, analytics+health 517s, features 373s, training 868s, ...).
TASKS: List[TaskSpec] = [
    TaskSpec("verify_bronze", "scripts/verify_bronze.py", "", False, 5),
    TaskSpec("clean_silver", "scripts/run_batch_pipeline.py", "--steps silver_etl", True, 30),
    TaskSpec("build_analytics_and_health", "scripts/run_batch_pipeline.py", "--steps analytics,health_status", True, 35),
    TaskSpec("segment_drives_kmeans", "analytics/kmeans_segmentation.py", "", True, 30),
    TaskSpec("build_features", "scripts/run_batch_pipeline.py", "--steps features", True, 30),
    # train + validation only: run_training_pipeline.py never touches the test split and does not overwrite HD3
    TaskSpec("train_model", "scripts/run_training_pipeline.py", "", True, 45),
    TaskSpec("score_predictions", "scripts/run_scoring_pipeline.py", "", True, 10),
    TaskSpec("export_dashboard", "analytics/export_dashboard.py", "", True, 15),
]


def bash_command(task: TaskSpec) -> str:
    """The bash command of one task. Spark tasks first check that no other Spark application is running."""
    script = "{}/{}".format(REPO, task.script)
    if not task.uses_spark:
        return "PYTHONPATH={repo} {python} {script}".format(repo=REPO, python=PYTHON, script=script)
    env = "PYTHONPATH={repo} PYSPARK_PYTHON={python} PYSPARK_DRIVER_PYTHON={python}".format(repo=REPO, python=PYTHON)
    guard = "PYTHONPATH={repo} {python} {repo}/scripts/check_spark_idle.py".format(repo=REPO, python=PYTHON)
    submit = (
        "{env} {submit} --master {master} "
        "--conf spark.driver.host=${{SPARK_DRIVER_HOST:-airflow-scheduler}} --conf spark.driver.bindAddress=0.0.0.0 {script}"
    ).format(env=env, submit=SPARK_SUBMIT, master=SPARK_MASTER, script=script)
    if task.args:
        submit += " " + task.args
    return "set -euo pipefail\n{}\n{}".format(guard, submit)
