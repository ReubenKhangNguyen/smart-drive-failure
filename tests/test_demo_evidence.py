from __future__ import annotations

import json

import pytest

from pipeline import demo_evidence as ev
from pipeline.demo_spec import DEMO_STEPS
from pipeline.demo_steps import StepContext, _guard, kafka_host_port
from scripts.run_demo_step import run_step


def _payload(task_id, status="ok", seconds=1.5, lines=None):
    return {"task_id": task_id, "status": status, "seconds": seconds, "lines": lines or ["a"], "metrics": {"n": 1}}


def test_write_and_read_roundtrip_leaves_no_temp_file(tmp_path):
    ev.write_step(tmp_path, "prepare_hdfs", _payload("prepare_hdfs", lines=["Việt Nam", "hai"]))

    assert ev.read_step(tmp_path, "prepare_hdfs")["lines"] == ["Việt Nam", "hai"]
    assert [p.name for p in (tmp_path / "steps").iterdir()] == ["prepare_hdfs.json"]
    assert ev.read_step(tmp_path, "upload_data") is None


def test_a_half_written_file_reads_as_none_instead_of_crashing_the_page(tmp_path):
    (tmp_path / "steps").mkdir()
    (tmp_path / "steps" / "prepare_hdfs.json").write_text('{"task_id": "prepa', encoding="utf-8")

    assert ev.read_step(tmp_path, "prepare_hdfs") is None


def test_init_run_marks_every_step_pending_and_records_the_run(tmp_path):
    ev.write_step(tmp_path, "clean_data", _payload("clean_data"))  # evidence of an older run

    ev.init_run(tmp_path, "20261008120000")

    everything = ev.read_all(tmp_path)
    assert list(everything) == [s.task_id for s in DEMO_STEPS]
    assert {p["status"] for p in everything.values()} == {"pending"} and all(p["run_id"] == "20261008120000" for p in everything.values())
    assert ev.read_run(tmp_path)["status"] == "running"


def test_very_long_logs_are_shortened_but_keep_the_start_and_the_end(tmp_path):
    lines = ["dòng {}".format(i) for i in range(1000)]

    ev.write_step(tmp_path, "read_spark", _payload("read_spark", lines=lines))

    saved = ev.read_step(tmp_path, "read_spark")["lines"]
    assert len(saved) <= ev.MAX_LINES and saved[0] == "dòng 0" and saved[-1] == "dòng 999" and any("lược bớt" in l for l in saved)


def test_summarize_counts_statuses_and_adds_the_seconds():
    all_steps = {"a": _payload("a", "ok", 10), "b": _payload("b", "failed", 2.5), "c": _payload("c", "pending", None)}

    summary = ev.summarize(all_steps)

    assert summary["counts"] == {"pending": 1, "running": 0, "ok": 1, "failed": 1} and summary["total_seconds"] == 12.5


def test_format_table_aligns_columns():
    lines = ev.format_table(["hãng", "AFR"], [["Seagate", "1.47%"], ["WDC", "0.64%"]])

    assert lines[0].startswith("hãng") and set(lines[1]) <= {"-", " "}
    assert lines[2].index("1.47%") == lines[3].index("0.64%") == len("Seagate") + 2


# ---------------------------------------------------------------- the step runner, with fake step functions
def _fake(result=None, fail=False):
    def function(ctx):
        ctx.say("đang làm việc")
        if fail:
            raise ValueError("hỏng giữa chừng")
        return result or {"rows": 7}
    return function


def test_a_successful_step_records_running_then_ok_with_its_lines_and_metrics(tmp_path):
    code = run_step("prepare_hdfs", {"hdfs": {}}, tmp_path, functions={"prepare_hdfs": _fake({"rows": 7})})

    saved = ev.read_step(tmp_path, "prepare_hdfs")
    assert code == 0 and saved["status"] == "ok" and saved["metrics"] == {"rows": 7} and "đang làm việc" in saved["lines"]
    assert saved["started_at"] and saved["finished_at"] and saved["seconds"] >= 0 and saved["error"] is None
    assert [s for s in ev.read_all(tmp_path).values() if s["status"] == "pending"]  # the other steps wait


def test_a_failing_step_records_the_error_and_marks_the_run_failed(tmp_path):
    code = run_step("prepare_hdfs", {"hdfs": {}}, tmp_path, functions={"prepare_hdfs": _fake(fail=True)})

    saved = ev.read_step(tmp_path, "prepare_hdfs")
    assert code == 1 and saved["status"] == "failed" and "ValueError: hỏng giữa chừng" in saved["error"]
    assert any(l.startswith("LỖI:") for l in saved["lines"])
    assert ev.read_run(tmp_path)["status"] == "failed" and ev.read_run(tmp_path)["failed_step"] == "prepare_hdfs"


def test_later_steps_reuse_the_run_id_and_the_last_step_closes_the_run(tmp_path):
    run_step("prepare_hdfs", {"hdfs": {}}, tmp_path, functions={"prepare_hdfs": _fake()})
    run_id = ev.read_run(tmp_path)["run_id"]

    run_step("upload_data", {"hdfs": {}}, tmp_path, functions={"upload_data": _fake()})
    assert ev.read_step(tmp_path, "upload_data")["run_id"] == run_id and ev.read_run(tmp_path)["status"] == "running"

    run_step("finish_summary", {"hdfs": {}}, tmp_path, functions={"finish_summary": _fake()})
    assert ev.read_run(tmp_path)["status"] == "ok" and ev.read_run(tmp_path)["finished_at"]


def test_starting_a_new_run_resets_the_old_evidence(tmp_path):
    run_step("prepare_hdfs", {"hdfs": {}}, tmp_path, functions={"prepare_hdfs": _fake()})
    run_step("upload_data", {"hdfs": {}}, tmp_path, functions={"upload_data": _fake()})

    run_step("prepare_hdfs", {"hdfs": {}}, tmp_path, functions={"prepare_hdfs": _fake()})

    assert ev.read_step(tmp_path, "upload_data")["status"] == "pending"  # not the old run's "ok"


# ---------------------------------------------------------------- context and helpers
def test_say_prints_keeps_the_line_and_notifies(tmp_path, capsys):
    seen = []
    ctx = StepContext({}, tmp_path, DEMO_STEPS[0], "r", on_update=lambda c: seen.append(len(c.lines)))

    ctx.say("xin chào")
    ctx.table(["a", "b"], [["1", "2"]])

    assert "xin chào" in capsys.readouterr().out and ctx.lines[0] == "xin chào" and len(ctx.lines) == 4 and seen == [1, 2, 3, 4]


def test_guard_allows_only_the_demo_area():
    assert _guard("/smart-drive-demo/bronze") == "/smart-drive-demo/bronze" and _guard("/smart-drive-demo") == "/smart-drive-demo"
    for bad in ("/smart-drive/bronze", "/smart-drive-demoX/a", "/", "/tmp"):
        with pytest.raises(RuntimeError, match="Từ chối ghi ngoài vùng demo"):
            _guard(bad)


def test_kafka_host_port_splits_the_bootstrap_address():
    assert kafka_host_port("kafka:29092") == ("kafka", 29092)
