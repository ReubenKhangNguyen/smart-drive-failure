"""Run INSIDE the Airflow container (Airflow is not installed in the `tests` image, so this module is skipped there):

    docker compose --profile orchestration run --rm --no-deps airflow-scheduler \
        /opt/airflow-venv/bin/python -m pytest -q tests/test_dag.py
"""
from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("airflow", reason="Airflow only exists in the Airflow image; run this file there")

from airflow.models import DagBag  # noqa: E402

from pipeline.dag_spec import DAG_ID, POOL, TASKS  # noqa: E402

DAGS_DIR = Path(__file__).resolve().parent.parent / "dags"


@pytest.fixture(scope="module")
def dag_bag():
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_the_dag_file_imports_without_errors(dag_bag):
    assert dag_bag.import_errors == {}
    assert DAG_ID in dag_bag.dags


def test_dependencies_form_one_chain_in_the_approved_order(dag_bag):
    dag = dag_bag.dags[DAG_ID]
    order = [t.task_id for t in dag.topological_sort()]

    assert order == [t.task_id for t in TASKS]
    for upstream, downstream in zip(order, order[1:]):
        assert dag.get_task(upstream).downstream_task_ids == {downstream}
    assert dag.get_task(order[-1]).downstream_task_ids == set()


def test_the_dag_is_manual_single_run_and_never_retries(dag_bag):
    dag = dag_bag.dags[DAG_ID]

    assert dag.schedule_interval is None
    assert dag.max_active_runs == 1 and dag.catchup is False
    for task in dag.tasks:
        assert task.retries == 0
        assert task.execution_timeout is not None


def test_spark_tasks_use_the_single_slot_pool(dag_bag):
    dag = dag_bag.dags[DAG_ID]

    for spec in TASKS:
        assert dag.get_task(spec.task_id).pool == (POOL if spec.uses_spark else "default_pool")


# ---------------------------------------------------------------- second DAG: one new quarter
from pipeline.dag_spec import QUARTER_DAG_ID, QUARTER_TASKS  # noqa: E402


def test_the_new_quarter_dag_imports_and_is_a_single_chain(dag_bag):
    assert dag_bag.import_errors == {}
    dag = dag_bag.dags[QUARTER_DAG_ID]
    order = [t.task_id for t in dag.topological_sort()]

    assert order == [t.task_id for t in QUARTER_TASKS]
    for upstream, downstream in zip(order, order[1:]):
        assert dag.get_task(upstream).downstream_task_ids == {downstream}


def test_the_new_quarter_dag_requires_a_quarter_and_is_manual_single_run_without_retries(dag_bag):
    dag = dag_bag.dags[QUARTER_DAG_ID]

    param = dag.params.get_param("quarter")
    assert param.schema["type"] == "string" and "pattern" in param.schema
    assert dag.schedule_interval is None and dag.max_active_runs == 1 and dag.catchup is False
    for task in dag.tasks:
        assert task.retries == 0 and task.execution_timeout is not None


def test_the_new_quarter_dag_renders_the_quarter_into_the_commands(dag_bag):
    dag = dag_bag.dags[QUARTER_DAG_ID]
    task = dag.get_task("build_quarter_features")

    assert "{{ params.quarter }}" in task.bash_command
    assert dag.get_task("export_dashboard").pool == "spark_cluster"


def test_rendering_a_quarter_fills_the_command_with_that_quarter(dag_bag):
    dag = dag_bag.dags[QUARTER_DAG_ID]

    for task_id in ("verify_bronze_quarter", "profile_schema", "clean_silver_quarter", "build_quarter_features", "evaluate_quarter"):
        task = dag.get_task(task_id)
        rendered = task.render_template(task.bash_command, {"params": {"quarter": "2026-Q3"}})
        assert "--quarter 2026-Q3" in rendered and "{{" not in rendered, task_id
    export = dag.get_task("export_dashboard")
    assert "--quarter" not in export.render_template(export.bash_command, {"params": {"quarter": "2026-Q3"}})
