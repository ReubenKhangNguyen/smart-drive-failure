from __future__ import annotations

from pipeline.batch_pipeline import run_steps


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
