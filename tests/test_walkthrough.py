from __future__ import annotations

from pipeline.demo_spec import DEMO_STEPS
from ui_dashboard import walkthrough


def test_page_tasks_match_the_real_dag_in_order_and_labels():
    assert [(t, l) for t, l, _ in walkthrough.TASKS] == [(s.task_id, s.label) for s in DEMO_STEPS]
    assert walkthrough.DAG_ID == "smart_drive_demo"


def test_every_task_is_explained_under_exactly_one_step():
    listed = [t for item in walkthrough.STEPS for t in item["tasks"]]
    assert sorted(listed) == sorted(s.task_id for s in DEMO_STEPS)
    assert [i["step"] for i in walkthrough.STEPS] == list(range(1, 10))
    step_of = {s.task_id: s.step for s in DEMO_STEPS}
    assert all(step_of[t] == item["step"] for item in walkthrough.STEPS for t in item["tasks"])


def test_every_step_has_all_the_explanation_parts():
    for item in walkthrough.STEPS:
        for key in ("goal", "theory", "prepare", "inside", "input", "output"):
            assert item[key], (item["step"], key)
