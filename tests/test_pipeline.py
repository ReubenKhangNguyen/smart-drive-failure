from __future__ import annotations

from pipeline.batch_pipeline import run_steps, write_run_report


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
