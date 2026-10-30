from __future__ import annotations

from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

from ui_dashboard import dictionary

PAGE = Path(__file__).resolve().parent.parent / "ui_dashboard" / "views" / "data_dictionary.py"


@pytest.fixture
def empty_dirs(tmp_path, monkeypatch):
    dashboard, reports = tmp_path / "dashboard", tmp_path / "reports"
    dashboard.mkdir(), reports.mkdir()
    monkeypatch.setenv("SMART_DASHBOARD_DIR", str(dashboard))
    monkeypatch.setenv("SMART_REPORTS_DIR", str(reports))


def test_page_renders_the_dictionary_even_without_exported_tables(empty_dirs):
    at = AppTest.from_file(str(PAGE), default_timeout=60).run()

    assert not at.exception
    assert len(at.table) == 3  # base columns, SMART attributes, derived columns
    assert "smart_197_raw" in at.table[1].value.index


def test_every_smart_attribute_has_all_the_explanation_parts():
    assert [row[0] for row in dictionary.SMART] == [
        "smart_5_raw", "smart_9_raw", "smart_187_raw", "smart_188_raw", "smart_194_raw", "smart_197_raw", "smart_198_raw", "smart_199_raw"]
    assert all(all(str(part).strip() for part in row) for row in dictionary.SMART + dictionary.BASE_COLUMNS)


def test_dictionary_attributes_match_the_features_the_project_uses():
    from features.build_features import SMART_COLUMNS

    assert [row[0] for row in dictionary.SMART] == list(SMART_COLUMNS)
