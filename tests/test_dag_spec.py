from __future__ import annotations

import re
from pathlib import Path

from pipeline.batch_pipeline import STEP_NAMES, parse_steps
from pipeline.dag_spec import DAG_ID, POOL, REPO, SPARK_MASTER, TASKS, bash_command

ROOT = Path(__file__).resolve().parent.parent
EXPECTED_ORDER = [
    "verify_bronze", "clean_silver", "build_analytics_and_health", "segment_drives_kmeans",
    "build_features", "train_model", "score_predictions", "export_dashboard",
]


def _task(task_id):
    return next(t for t in TASKS if t.task_id == task_id)


def test_the_dag_has_the_eight_approved_tasks_in_order():
    assert DAG_ID == "smart_drive_pipeline"
    assert [t.task_id for t in TASKS] == EXPECTED_ORDER
    assert len({t.task_id for t in TASKS}) == len(TASKS)


def test_every_task_calls_an_existing_script_without_reimplementing_logic():
    for task in TASKS:
        assert (ROOT / task.script).is_file(), task.script
        assert "{}/{}".format(REPO, task.script) in bash_command(task)
    assert _task("verify_bronze").script == "scripts/verify_bronze.py"
    assert _task("segment_drives_kmeans").script == "analytics/kmeans_segmentation.py"
    assert _task("export_dashboard").script == "analytics/export_dashboard.py"
    assert _task("train_model").script == "scripts/run_training_pipeline.py"
    assert _task("score_predictions").script == "scripts/run_scoring_pipeline.py"


def test_batch_tasks_split_the_four_batch_steps_without_loss_or_overlap():
    batch = [t for t in TASKS if t.script == "scripts/run_batch_pipeline.py"]
    covered = []
    for task in batch:
        assert task.args.startswith("--steps ")
        covered += parse_steps(task.args.split(" ", 1)[1])  # valid names only (parse_steps raises on unknown ones)

    assert [t.task_id for t in batch] == ["clean_silver", "build_analytics_and_health", "build_features"]
    assert covered == list(STEP_NAMES)  # every step exactly once, in pipeline order


def test_kmeans_is_a_dag_task_only_not_part_of_the_batch_pipeline():
    assert "kmeans" not in (ROOT / "pipeline" / "batch_pipeline.py").read_text(encoding="utf-8").lower()
    assert "segment_drives_kmeans" in EXPECTED_ORDER
    assert EXPECTED_ORDER.index("build_analytics_and_health") + 1 == EXPECTED_ORDER.index("segment_drives_kmeans")
    assert EXPECTED_ORDER.index("segment_drives_kmeans") + 1 == EXPECTED_ORDER.index("build_features")


def test_spark_tasks_check_the_cluster_is_idle_first_and_use_the_cluster_python():
    for task in TASKS:
        command = bash_command(task)
        if not task.uses_spark:
            assert "spark-submit" not in command
            continue
        lines = command.splitlines()
        assert lines[0] == "set -euo pipefail"
        assert "scripts/check_spark_idle.py" in lines[1] and "spark-submit" not in lines[1]  # guard BEFORE the job
        submit = lines[2]
        assert "--master {}".format(SPARK_MASTER) in submit
        assert "PYSPARK_PYTHON=/usr/bin/python3" in submit and "PYSPARK_DRIVER_PYTHON=/usr/bin/python3" in submit
        assert "spark.driver.host=${SPARK_DRIVER_HOST:-airflow-scheduler}" in submit and "spark.driver.bindAddress=0.0.0.0" in submit
        assert "--deploy-mode" not in command  # standalone has no cluster mode for Python applications


def test_every_command_uses_the_system_python_not_the_airflow_venv():
    for task in TASKS:
        command = bash_command(task)
        assert "/usr/bin/python3" in command
        assert "airflow-venv" not in command
        assert not re.search(r"(?<![/\w])python3? ", command)  # no bare `python`/`python3` that could resolve to the venv


def test_no_task_changes_the_default_executor_size():
    # the default executor (1g) is the configuration that ran every Phase 6/7/9 job; a bigger one would overflow worker RAM
    for task in TASKS:
        command = bash_command(task)
        for flag in ("--executor-memory", "--driver-memory", "--total-executor-cores", "--executor-cores", "spark.executor.memory"):
            assert flag not in command, (task.task_id, flag)


def test_train_model_never_touches_the_test_split_and_does_not_overwrite_the_saved_model():
    command = bash_command(_task("train_model"))
    assert command.rstrip().endswith("scripts/run_training_pipeline.py")  # no extra flags such as a test evaluation

    for path in ("scripts/run_training_pipeline.py", "pipeline/training_pipeline.py"):
        source = (ROOT / path).read_text(encoding="utf-8")
        assert '== "test"' not in source and "== 'test'" not in source, path  # only the train and val splits are read
        assert ".save(" not in source and ".write." not in source, path  # no model or table is written: HD3 stays as it is


def test_spark_tasks_share_one_pool_slot_and_timeouts_are_sane():
    assert POOL == "spark_cluster"
    assert [t.task_id for t in TASKS if not t.uses_spark] == ["verify_bronze"]
    assert all(0 < t.timeout_minutes <= 60 for t in TASKS)
    assert _task("train_model").timeout_minutes >= 30  # the logged run took 868 s


def test_commands_are_not_mistaken_for_template_files_and_contain_no_secrets():
    for task in TASKS:
        command = bash_command(task)
        assert not command.rstrip().endswith(".sh")  # BashOperator would treat that as a template file path
        assert "password" not in command.lower() and "AIRFLOW_ADMIN" not in command


# ---------------------------------------------------------------- second DAG: one new quarter
from pipeline.dag_spec import QUARTER_DAG_ID, QUARTER_PARAM, QUARTER_TASKS  # noqa: E402

QUARTER_ORDER = [
    "verify_bronze_quarter", "profile_schema", "clean_silver_quarter", "build_quarter_features",
    "evaluate_quarter", "score_all_days", "export_dashboard",
]


def _qtask(task_id):
    return next(t for t in QUARTER_TASKS if t.task_id == task_id)


def test_the_new_quarter_dag_has_its_tasks_in_order_and_a_distinct_id():
    assert QUARTER_DAG_ID == "smart_drive_new_quarter" and QUARTER_DAG_ID != DAG_ID
    assert [t.task_id for t in QUARTER_TASKS] == QUARTER_ORDER
    assert len({t.task_id for t in QUARTER_TASKS}) == len(QUARTER_TASKS)


def test_every_quarter_task_calls_an_existing_script_that_understands_the_quarter_flag():
    for task in QUARTER_TASKS:
        assert (ROOT / task.script).is_file(), task.script
        assert "{}/{}".format(REPO, task.script) in bash_command(task)
        if "--quarter" in task.args:
            assert "--quarter" in (ROOT / task.script).read_text(encoding="utf-8"), task.script  # the script really takes it
    assert all("train" not in t.script for t in QUARTER_TASKS)  # the frozen model is never retrained here


def test_the_quarter_id_is_a_template_parameter_in_exactly_the_tasks_that_need_it():
    needs = {"verify_bronze_quarter", "profile_schema", "clean_silver_quarter", "build_quarter_features", "evaluate_quarter"}
    for task in QUARTER_TASKS:
        assert (QUARTER_PARAM in bash_command(task)) == (task.task_id in needs), task.task_id
    assert QUARTER_PARAM == "{{ params.quarter }}"


def test_non_spark_tasks_receive_their_arguments_too():
    command = bash_command(_qtask("verify_bronze_quarter"))

    assert command.endswith("scripts/verify_bronze.py --quarter {{ params.quarter }}")
    assert "spark-submit" not in command
    assert bash_command(next(t for t in TASKS if t.task_id == "verify_bronze")).endswith("scripts/verify_bronze.py")  # the old DAG is unchanged


def test_quarter_spark_tasks_check_the_cluster_first_and_share_the_single_slot():
    for task in QUARTER_TASKS:
        if task.uses_spark:
            command = bash_command(task)
            assert "check_spark_idle.py" in command and command.index("check_spark_idle.py") < command.index("spark-submit")
            assert SPARK_MASTER in command
    assert _qtask("verify_bronze_quarter").uses_spark is False
    assert all(0 < t.timeout_minutes <= 60 for t in QUARTER_TASKS)
    assert _qtask("build_quarter_features").timeout_minutes >= 30  # the logged run took 477 s


def test_quarter_commands_contain_no_secrets_and_no_shell_template_file():
    for task in QUARTER_TASKS:
        command = bash_command(task)
        assert not command.rstrip().endswith(".sh")
        assert "password" not in command.lower() and "AIRFLOW_ADMIN" not in command
