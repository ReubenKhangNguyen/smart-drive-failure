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
