from __future__ import annotations

import json
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

from ui_dashboard import data

PAGE = Path(__file__).resolve().parent.parent / "ui_dashboard" / "views" / "model_training.py"


@pytest.fixture
def empty_dirs(tmp_path, monkeypatch):
    dashboard, reports = tmp_path / "dashboard", tmp_path / "reports"
    dashboard.mkdir(), reports.mkdir()
    monkeypatch.setenv("SMART_DASHBOARD_DIR", str(dashboard))
    monkeypatch.setenv("SMART_REPORTS_DIR", str(reports))
    return dashboard, reports


def _texts(at):
    return "\n".join(str(el.value) for group in (at.markdown, at.caption, at.info, at.subheader, at.title) for el in group)


def test_loss_history_loads_only_a_usable_file(tmp_path):
    assert data.load_loss_history(tmp_path) is None  # missing
    (tmp_path / data.LOSS_FILE).write_text("{not json", encoding="utf-8")
    assert data.load_loss_history(tmp_path) is None  # half written
    (tmp_path / data.LOSS_FILE).write_text(json.dumps({"objective_history": [0.7]}), encoding="utf-8")
    assert data.load_loss_history(tmp_path) is None  # too short to draw
    (tmp_path / data.LOSS_FILE).write_text(json.dumps({"objective_history": [0.7, 0.5, 0.4], "max_iter": 2}), encoding="utf-8")
    assert data.load_loss_history(tmp_path)["objective_history"] == [0.7, 0.5, 0.4]


def test_page_without_data_shows_hints_instead_of_failing(empty_dirs):
    at = AppTest.from_file(str(PAGE), default_timeout=60).run()

    assert not at.exception
    text = _texts(at)
    assert "Chia tập theo thời gian" in text and "Chưa có lịch sử loss" in text


def test_page_draws_the_loss_with_its_honest_caveats(empty_dirs):
    _, reports = empty_dirs
    (reports / data.LOSS_FILE).write_text(json.dumps({"objective_history": [0.693, 0.5, 0.45], "max_iter": 2}), encoding="utf-8")

    at = AppTest.from_file(str(PAGE), default_timeout=60).run()

    assert not at.exception
    text = _texts(at)
    assert "dừng ở giới hạn 2 vòng" in text and "không phải loss của validation" in text and "huấn luyện lại" in text
    assert [m.value for m in at.metric][0] == "0.693"
