from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from pipeline.batch_pipeline import STEP_NAMES, parse_steps, run_steps, select_steps, write_run_report


def test_run_steps_runs_all_in_order_on_success():
    calls = []

    def step_a():
        calls.append("a")
        return {"ok": True}

    def step_b():
        calls.append("b")
        return {"ok": True}

    result = run_steps([("a", step_a), ("b", step_b)])

    assert calls == ["a", "b"]
    assert result["status"] == "ok"
    assert [s["step"] for s in result["steps"]] == ["a", "b"]
    assert all(s["status"] == "ok" for s in result["steps"])


def test_run_steps_stops_on_first_failure():
    calls = []

    def step_a():
        calls.append("a")
        return {"ok": True}

    def step_b():
        calls.append("b")
        raise RuntimeError("boom")

    def step_c():
        calls.append("c")
        return {"ok": True}

    result = run_steps([("a", step_a), ("b", step_b), ("c", step_c)])

    assert calls == ["a", "b"]  # step_c never runs
    assert result["status"] == "failed"
    assert result["failed_step"] == "b"
    assert len(result["steps"]) == 2
    assert result["steps"][-1]["status"] == "failed"
    assert "boom" in result["steps"][-1]["error"]


def test_run_steps_logs_elapsed_time_per_step():
    result = run_steps([("a", lambda: {"ok": True})])

    assert "elapsed_seconds" in result["steps"][0]
    assert isinstance(result["steps"][0]["elapsed_seconds"], float)


def test_write_run_report_lists_steps_with_rows_and_appends(tmp_path):
    result = run_steps([
        ("silver_etl", lambda: {"rows_in": 100, "rows_out": 98}),
        ("health_status", lambda: {"distribution": {"HEALTHY": 90, "WATCH": 8}}),
        ("train", lambda: {"val_metrics": {}}),
    ])

    path = write_run_report("batch_pipeline", result, report_dir=tmp_path, run_date="2026-10-02")
    write_run_report("scoring_pipeline", result, report_dir=tmp_path, run_date="2026-10-02")
    text = path.read_text(encoding="utf-8")

    assert path.name == "pipeline_run_2026-10-02.md"
    assert "| silver_etl |" in text and "| 100 | 98 |" in text
    assert "| health_status |" in text and "| không áp dụng | 98 |" in text
    assert "không áp dụng | không áp dụng |" in text  # train step reports no row counts
    assert text.count("## ") == 2  # two runs appended to the same daily file
    assert text.count("# Nhật ký chạy pipeline") == 1


def test_write_run_report_marks_failed_run(tmp_path):
    def boom():
        raise RuntimeError("hdfs down")

    result = run_steps([("a", boom)])
    text = write_run_report("batch_pipeline", result, report_dir=tmp_path, run_date="2026-10-02").read_text(encoding="utf-8")

    assert "THẤT BẠI tại bước a" in text
    assert "hdfs down" in text


def _fake_steps():
    return [(name, (lambda n=name: {"step": n})) for name in STEP_NAMES]


def test_step_names_match_the_real_pipeline_order():
    assert STEP_NAMES == ("silver_etl", "analytics", "health_status", "features")


def test_parse_steps_defaults_to_all_and_splits_commas():
    assert parse_steps(None) is None
    assert parse_steps("silver_etl") == ["silver_etl"]
    assert parse_steps(" analytics , health_status ") == ["analytics", "health_status"]


def test_parse_steps_rejects_unknown_and_empty_names():
    with pytest.raises(ValueError) as unknown:
        parse_steps("silver_etl,nope")
    assert "nope" in str(unknown.value) and "valid steps" in str(unknown.value)
    for empty in ("", " , "):
        with pytest.raises(ValueError):
            parse_steps(empty)


def test_select_steps_defaults_to_all_four_in_pipeline_order():
    selected = select_steps(_fake_steps())

    assert [name for name, _ in selected] == list(STEP_NAMES)


def test_select_steps_keeps_pipeline_order_whatever_the_request_order():
    selected = select_steps(_fake_steps(), ["features", "analytics"])

    assert [name for name, _ in selected] == ["analytics", "features"]
    assert [name for name, _ in select_steps(_fake_steps(), ["health_status"])] == ["health_status"]


def test_select_steps_rejects_a_name_the_pipeline_does_not_have():
    with pytest.raises(ValueError):
        select_steps([("only", lambda: {})], ["silver_etl"])


def test_selected_steps_run_only_those_and_still_stop_on_failure():
    calls = []

    def ok(name):
        return lambda: calls.append(name) or {"ok": True}

    def boom():
        calls.append("analytics")
        raise RuntimeError("boom")

    steps = [("silver_etl", ok("silver_etl")), ("analytics", boom), ("health_status", ok("health_status")), ("features", ok("features"))]

    result = run_steps(select_steps(steps, ["analytics", "health_status"]))

    assert calls == ["analytics"] and result["status"] == "failed" and result["failed_step"] == "analytics"


def test_run_batch_pipeline_cli_rejects_an_unknown_step_before_starting_spark():
    script = Path(__file__).resolve().parent.parent / "scripts" / "run_batch_pipeline.py"

    done = subprocess.run([sys.executable, str(script), "--steps", "silver_etl,bogus"], stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, universal_newlines=True, cwd=str(script.parent.parent))

    assert done.returncode == 2  # argparse error, no SparkSession was created
    assert "bogus" in done.stderr and "valid steps" in done.stderr

