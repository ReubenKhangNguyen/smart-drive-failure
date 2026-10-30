"""The demo run: the course's nine steps as Airflow tasks, on the isolated demo area (config/demo.yaml).

Kept free of Airflow and Spark imports (like pipeline/dag_spec.py) so it can be tested in the plain `tests`
container. dags/smart_drive_demo_dag.py only turns DEMO_STEPS into BashOperators chained in order; every task
calls scripts/run_demo_step.py, which runs one step, prints what it does and writes a JSON evidence file the
dashboard page reads (docs/contracts.md, HD12).
"""
from __future__ import annotations

from typing import Any, Dict, List, NamedTuple

from pipeline.dag_spec import PYTHON, REPO, SPARK_MASTER, SPARK_SUBMIT

DEMO_DAG_ID = "smart_drive_demo"
DEMO_POOL = "spark_cluster"  # the same single-slot pool as the real DAG: one Spark task at a time
DEMO_ROOT = "/smart-drive-demo"
DEMO_CONFIG = REPO + "/config/demo.yaml"
DEMO_ARTIFACTS = REPO + "/artifacts/demo"
DEMO_SOURCE_DIR = "/external_data_all/data_Q1_2026/data_Q1_2026"  # the CSV folder mounted into the Airflow containers
KAFKA_BOOTSTRAP = "kafka:29092"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"  # Scala 2.12, Spark 3.5.1 (as in pipeline/streaming_demo.py)
EVIDENCE_SUBDIR = "steps"


class DemoStep(NamedTuple):
    task_id: str
    step: int  # the course step 1-9
    label: str  # "6a": a course step that needs several tasks gets a letter
    title: str
    uses_spark: bool
    timeout_minutes: int
    spark_args: str = ""  # extra spark-submit arguments, e.g. --packages


# Order matters: each task depends on the previous one. Timeouts are generous multiples of the expected duration.
DEMO_STEPS: List[DemoStep] = [
    DemoStep("prepare_hdfs", 1, "1", "Chuẩn bị HDFS", False, 5),
    DemoStep("upload_data", 2, "2", "Upload dữ liệu lên HDFS", False, 20),
    DemoStep("read_spark", 3, "3", "Đọc bằng Spark", True, 15),
    DemoStep("clean_data", 4, "4", "Làm sạch dữ liệu", True, 20),
    DemoStep("spark_sql", 5, "5", "Spark SQL", True, 20),
    DemoStep("kmeans", 6, "6a", "Phân cụm K-Means", True, 30),
    DemoStep("build_features", 6, "6b", "Gán nhãn và tạo đặc trưng", True, 25),
    DemoStep("train_model", 6, "6c", "Huấn luyện và lưu mô hình", True, 30),
    DemoStep("score_days", 6, "6d", "Chấm điểm rủi ro", True, 20),
    DemoStep("save_results", 7, "7", "Lưu và xuất kết quả", True, 20),
    DemoStep("kafka_produce", 8, "8a", "Kafka: phát dữ liệu", False, 15),
    DemoStep("kafka_consume", 8, "8b", "Kafka: Spark nhận và xử lý", True, 20, "--conf spark.jars.ivy=/tmp/ivy_demo --packages " + KAFKA_PACKAGE),
    DemoStep("finish_summary", 9, "9", "Tổng kết luồng (Airflow DAG)", False, 5),
]


def step_by_id(task_id: str) -> DemoStep:
    for step in DEMO_STEPS:
        if step.task_id == task_id:
            return step
    raise KeyError("unknown demo step '{}'; valid: {}".format(task_id, ", ".join(s.task_id for s in DEMO_STEPS)))


def demo_env() -> str:
    """Environment of every demo task: the demo config, its own artifacts folder and the CSV source folder."""
    return "SMART_DRIVE_CONFIG={cfg} SMART_DRIVE_ARTIFACTS={art} DEMO_SOURCE_DIR={src}".format(
        cfg=DEMO_CONFIG, art=DEMO_ARTIFACTS, src=DEMO_SOURCE_DIR)


def demo_command(step: DemoStep) -> str:
    """The bash command of one demo task. Spark tasks first check that no other Spark application is running."""
    script = "{}/scripts/run_demo_step.py".format(REPO)
    env = demo_env()
    if not step.uses_spark:
        return "set -euo pipefail\n{env} PYTHONPATH={repo} {python} {script} --step {task}".format(
            env=env, repo=REPO, python=PYTHON, script=script, task=step.task_id)
    guard = "PYTHONPATH={repo} {python} {repo}/scripts/check_spark_idle.py".format(repo=REPO, python=PYTHON)
    submit = (
        "{env} PYTHONPATH={repo} PYSPARK_PYTHON={python} PYSPARK_DRIVER_PYTHON={python} {submit} --master {master} "
        "--conf spark.driver.host=${{SPARK_DRIVER_HOST:-airflow-scheduler}} --conf spark.driver.bindAddress=0.0.0.0 "
        "{extra}{script} --step {task}"
    ).format(env=env, repo=REPO, python=PYTHON, submit=SPARK_SUBMIT, master=SPARK_MASTER,
             extra=(step.spark_args + " ") if step.spark_args else "", script=script, task=step.task_id)
    return "set -euo pipefail\n{}\n{}".format(guard, submit)


def assert_demo_config(config: Dict[str, Any]) -> None:
    """Refuse to run a demo step with the real config: it would write into the real Bronze/Silver/Gold.

    Every task sets SMART_DRIVE_CONFIG itself, but a step started by hand without it must fail loudly instead of
    overwriting the real data."""
    offenders = [
        "{}={}".format(key, value)
        for key, value in config.get("hdfs", {}).items()
        if isinstance(value, str) and value.startswith("/") and not value.startswith(DEMO_ROOT + "/")
    ]
    if offenders:
        raise RuntimeError(
            "Cấu hình đang dùng KHÔNG phải cấu hình demo (đường dẫn ngoài {}): {}. Chạy qua DAG hoặc đặt "
            "SMART_DRIVE_CONFIG=config/demo.yaml.".format(DEMO_ROOT, "; ".join(offenders[:4])))
