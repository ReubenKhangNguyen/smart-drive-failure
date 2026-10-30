from __future__ import annotations

from pathlib import Path

import pytest

from config.settings import load_config
from pipeline.demo_spec import (
    DEMO_ARTIFACTS, DEMO_CONFIG, DEMO_DAG_ID, DEMO_POOL, DEMO_STEPS, KAFKA_PACKAGE, assert_demo_config, demo_command, demo_env, step_by_id,
)

ROOT = Path(__file__).resolve().parent.parent
EXPECTED = [
    "prepare_hdfs", "upload_data", "read_spark", "clean_data", "spark_sql", "kmeans", "build_features", "train_model",
    "score_days", "save_results", "kafka_produce", "kafka_consume", "finish_summary",
]


def test_the_demo_has_the_course_steps_in_order_with_unique_ids_and_labels():
    assert [s.task_id for s in DEMO_STEPS] == EXPECTED
    assert len({s.label for s in DEMO_STEPS}) == len(DEMO_STEPS)
    assert sorted({s.step for s in DEMO_STEPS}) == list(range(1, 10))  # every one of the nine steps has at least one task
    assert [s.step for s in DEMO_STEPS] == sorted(s.step for s in DEMO_STEPS)  # and the steps never go backwards
    assert DEMO_DAG_ID == "smart_drive_demo"


def test_every_task_runs_the_step_script_with_the_demo_environment():
    for step in DEMO_STEPS:
        command = demo_command(step)
        assert "scripts/run_demo_step.py --step {}".format(step.task_id) in command
        assert "SMART_DRIVE_CONFIG={}".format(DEMO_CONFIG) in command and "SMART_DRIVE_ARTIFACTS={}".format(DEMO_ARTIFACTS) in command
        assert not command.rstrip().endswith(".sh")
        assert "password" not in command.lower() and "AIRFLOW_ADMIN" not in command
    assert (ROOT / "scripts" / "run_demo_step.py").is_file() and (ROOT / "config" / "demo.yaml").is_file()


def test_spark_tasks_check_the_cluster_first_and_other_tasks_do_not_use_spark():
    for step in DEMO_STEPS:
        command = demo_command(step)
        if step.uses_spark:
            assert "spark-submit" in command and "check_spark_idle.py" in command
            assert command.index("check_spark_idle.py") < command.index("spark-submit")
        else:
            assert "spark-submit" not in command


def test_only_the_kafka_consumer_loads_the_kafka_package():
    for step in DEMO_STEPS:
        assert (KAFKA_PACKAGE in demo_command(step)) == (step.task_id == "kafka_consume")
    assert KAFKA_PACKAGE.endswith(":3.5.1") and "_2.12" in KAFKA_PACKAGE  # Scala 2.12 / Spark 3.5.1 of the cluster


def test_timeouts_are_sane_and_the_pool_is_the_single_slot_of_the_real_dag():
    assert all(0 < s.timeout_minutes <= 60 for s in DEMO_STEPS)
    assert DEMO_POOL == "spark_cluster"
    assert step_by_id("kmeans").timeout_minutes >= 20 and step_by_id("train_model").timeout_minutes >= 20


def test_unknown_step_is_rejected():
    with pytest.raises(KeyError, match="unknown demo step"):
        step_by_id("nope")


def test_every_task_has_a_function_and_no_function_is_orphaned():
    from pipeline.demo_steps import STEP_FUNCTIONS

    assert sorted(STEP_FUNCTIONS) == sorted(EXPECTED)


def test_the_demo_config_passes_the_guard_and_the_real_config_does_not():
    assert_demo_config(load_config(ROOT / "config" / "demo.yaml"))
    with pytest.raises(RuntimeError, match="KHÔNG phải cấu hình demo"):
        assert_demo_config(load_config(ROOT / "config" / "project.yaml"))


def test_a_single_real_path_is_enough_to_stop_the_run():
    config = load_config(ROOT / "config" / "demo.yaml")
    config["hdfs"]["features"] = "/smart-drive/gold/features"  # one forgotten path would overwrite real data

    with pytest.raises(RuntimeError, match="features=/smart-drive/gold/features"):
        assert_demo_config(config)


def test_the_environment_names_the_demo_config_artifacts_and_source():
    env = demo_env()

    assert "config/demo.yaml" in env and "artifacts/demo" in env and "DEMO_SOURCE_DIR=/external_data_all" in env


def test_the_dag_file_is_a_thin_wrapper_over_the_spec():
    source = (ROOT / "dags" / "smart_drive_demo_dag.py").read_text(encoding="utf-8")

    assert "schedule=None" in source and "max_active_runs=1" in source and '"retries": 0' in source
    assert "from pipeline.demo_spec import" in source and "DEMO_STEPS" in source
    assert "spark-submit" not in source and "pyspark" not in source


def test_the_airflow_containers_mount_the_csv_folder_read_only():
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

    assert 'C:/dataset_smart_drive_failure:/external_data_all:ro' in compose
    common = compose.split("x-airflow-common")[1].split("services:")[0] if "x-airflow-common" in compose else ""
    assert "external_data_all" in common or compose.count("external_data_all") >= 2
