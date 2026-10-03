from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

PAGES_DIR = Path(__file__).resolve().parent.parent / "ui_dashboard" / "views"
PAGE_FILES = ["overview", "smart_analysis", "data_analytics", "failure_prediction", "cluster_performance"]


def _page(name):
    return AppTest.from_file(str(PAGES_DIR / (name + ".py")), default_timeout=60)


def _texts(at):
    parts = []
    for group in (at.markdown, at.caption, at.info, at.warning, at.error, at.success, at.subheader, at.title):
        parts += [str(el.value) for el in group]
    for frame in at.dataframe:
        parts.append(frame.value.to_string())
    return "\n".join(parts)


@pytest.fixture
def empty_dirs(tmp_path, monkeypatch):
    dashboard, reports = tmp_path / "dashboard", tmp_path / "reports"
    dashboard.mkdir(), reports.mkdir()
    monkeypatch.setenv("SMART_DASHBOARD_DIR", str(dashboard))
    monkeypatch.setenv("SMART_REPORTS_DIR", str(reports))
    return dashboard, reports


def _w(base, name, df):
    df.to_parquet(str(base / (name + ".parquet")), index=False)


def _fill(dashboard, reports):
    _w(dashboard, "dashboard_overview", pd.DataFrame([{
        "data_start": "2026-01-01", "data_end": "2026-03-31", "drive_days": 30597484, "drive_count": 351095,
        "failure_count": 1030, "afr": 0.0123, "model_version": "vtest", "k": 100, "scored_date": "2026-03-24", "scored_rows": 342662}]))
    _w(dashboard, "health_distribution", pd.DataFrame({
        "health_level": ["HEALTHY", "WATCH", "CRITICAL"], "drive_days": [90, 6, 4], "share": [0.9, 0.06, 0.04],
        "labeled_rows": [80, 5, 3], "failed_within_7d": [1, 2, 3], "failure_rate_7d": [0.0000620, 0.00047, 0.0046]}))
    _w(dashboard, "health_snapshot", pd.DataFrame({
        "date": ["2026-03-24"] * 2, "serial_number": ["SN_CRIT", "SN_WATCH"], "model": ["M1", "M2"],
        "health_level": ["CRITICAL", "WATCH"], "reasons": [np.array(["smart_5_raw >= 102"]), np.array(["smart_197_raw > 0"])]}))
    _w(dashboard, "predictions_topk", pd.DataFrame({
        "date": ["2026-03-24"] * 3, "risk_rank": [1, 2, 3], "serial_number": ["SN_HEALTHY", "SN_CRIT", "SN_WATCH"],
        "model": ["M1", "M1", "M2"], "risk_score": [0.9, 0.8, 0.7], "alert": [True] * 3,
        "health_level": ["HEALTHY", "CRITICAL", "WATCH"],
        "reasons": [np.array([], dtype=object), np.array(["smart_5_raw >= 102"]), np.array(["smart_197_raw > 0"])],
        "model_version": ["vtest"] * 3}))
    _w(dashboard, "smart_history_topk", pd.DataFrame({
        "serial_number": ["SN_HEALTHY"] * 2, "date": ["2026-03-23", "2026-03-24"], "smart_5_raw": [0, 0],
        "smart_187_raw": [None, None], "smart_197_raw": [0, 1], "smart_198_raw": [0, 0]}).astype(
        {"smart_187_raw": "float64"}))
    rows = []
    for model, normal in (("logistic_regression", 0.0555), ("random_forest", 0.0336), ("baseline_rules_v1", 0.0347)):
        base = {"model": model, "k": 100}
        rows.append({**base, "split": "val", "segment": "all", "pr_auc": 0.02, "roc_auc": 0.85, "recall_at_k": 0.09,
                     "precision_at_k": 0.1, "rows": None, "positives": None})
        rows.append({**base, "split": "test", "segment": "full", "pr_auc": 0.02, "roc_auc": 0.86, "recall_at_k": 0.4444,
                     "precision_at_k": 0.4341, "rows": 3435296, "positives": 963})
        rows.append({**base, "split": "test", "segment": "normal", "pr_auc": None, "roc_auc": None, "recall_at_k": normal,
                     "precision_at_k": 0.038, "rows": 3435008, "positives": 675})
        rows.append({**base, "split": "test", "segment": "tail", "pr_auc": None, "roc_auc": None, "recall_at_k": 1.0,
                     "precision_at_k": 1.0, "rows": 288, "positives": 288})
    _w(dashboard, "model_metrics", pd.DataFrame(rows))
    _w(dashboard, "model_feature_importance", pd.DataFrame({
        "model": ["logistic_regression", "logistic_regression", "random_forest"], "rank": [1, 2, 1],
        "feature": ["smart_5_raw", "smart_187_raw_is_missing", "smart_197_raw_max_7d"],
        "importance": [1.5, 1.2, 0.11],
        "source": ["LR vtest: |hệ số| × độ lệch chuẩn"] * 2 + ["RF, một lần chạy Phase 6, không tái lập"]}))
    _w(dashboard, "afr_by_model", pd.DataFrame({"model": ["M1", "M2"], "drive_days": [200000, 50], "failures": [3, 1], "afr": [0.0055, 7.3]}))
    _w(dashboard, "afr_by_manufacturer", pd.DataFrame({"manufacturer": ["HGST", None], "drive_days": [10, 5], "failures": [1, 0], "afr": [0.1, 0.0]}))
    _w(dashboard, "smart_distribution", pd.DataFrame({
        "smart_attribute": ["smart_5_raw"] * 2, "cohort": ["healthy", "pre_failure"], "n": [10, 5], "mean": [1.0, 100.0],
        "p50": [0.0, 8.0], "p90": [0.0, 400.0], "p99": [102.0, 5000.0]}))
    _w(dashboard, "pre_failure_signal", pd.DataFrame({
        "smart_attribute": ["smart_5_raw"] * 2, "window_days": [7, 30], "failed_serials": [1030, 1030],
        "signalled_serials": [562, 634], "signal_rate": [0.5456, 0.6155]}))
    _w(dashboard, "kmeans_clusters", pd.DataFrame({
        "cluster_type": ["zero_signal", "kmeans"], "cluster_id": [0, 1], "size": [100, 10], "healthy_count": [98, 6],
        "failed_count": [2, 4], "failed_rate": [0.02, 0.4], "mean_smart_5_raw": [0.0, 5.0], "mean_smart_187_raw": [0.0, 1.0],
        "mean_smart_197_raw": [0.0, 9.0], "mean_smart_198_raw": [0.0, 2.0], "smart_187_missing_share": [0.68, 0.5]}))
    _w(dashboard, "kmeans_k_selection", pd.DataFrame({"k": [3, 4], "silhouette": [0.68, 0.64], "wssse": [48490.0, 40976.0],
                                                        "is_selected": [True, False]}))
    _w(dashboard, "kmeans_health_crosstab", pd.DataFrame({"cluster_id": [0, 1], "health_level": ["HEALTHY", "CRITICAL"], "drives": [100, 10]}))
    _w(dashboard, "benchmark", pd.DataFrame({
        "experiment": ["format", "format", "pipeline_steps"], "variant": ["csv_7d", "silver_parquet_7d", "batch_pipeline/silver_etl"],
        "query": ["count+groupBy(model)", "count+groupBy(model)", "pipeline step"], "runs": [3, 3, 1],
        "median_seconds": [40.0, 4.0, 269.35], "min_seconds": [39.0, 3.5, 269.35], "max_seconds": [41.0, 4.5, 269.35],
        "size_bytes": [930 * 1048576, 20 * 1048576, None], "file_count": [7, 7, None], "input_partitions": [14, 7, None],
        "executors": [2, 2, None], "rows": [3000000, 3000000, None], "note": ["", "", "1 lần, không phải trung vị"]}).astype(
        {"size_bytes": "float64", "file_count": "float64", "input_partitions": "float64", "executors": "float64", "rows": "float64"}))
    _w(dashboard, "benchmark_environment", pd.DataFrame({"item": ["host_ram_gb", "docker_ncpu"], "value": ["15.2", "12"]}))
    (reports / "pipeline_run_2026-10-02.md").write_text("# Nhật ký chạy pipeline — 2026-10-02\n\n| Bước | Thời gian |\n|---|---|\n| score | 37.91 |\n", encoding="utf-8")


@pytest.mark.parametrize("page", PAGE_FILES)
def test_every_page_renders_without_data_and_shows_guidance(empty_dirs, page):
    at = _page(page).run()

    assert not at.exception, [e.value for e in at.exception]
    assert len(at.info) >= 1  # a how-to-create hint instead of an error


def test_missing_tables_hint_names_the_job_to_run(empty_dirs):
    at = _page("failure_prediction").run()

    hints = " ".join(str(i.value) for i in at.info)
    assert "analytics/export_dashboard.py" in hints and "predictions_topk" in hints


@pytest.mark.parametrize("page", PAGE_FILES)
def test_every_page_renders_with_data(empty_dirs, page):
    _fill(*empty_dirs)

    at = _page(page).run()

    assert not at.exception, [e.value for e in at.exception]


def test_failure_prediction_shows_risk_score_health_level_and_conflict_explanation(empty_dirs):
    _fill(*empty_dirs)

    at = _page("failure_prediction").run()
    text = _texts(at)

    assert not at.exception
    assert "Điểm rủi ro" in text and "Mức tình trạng" in text
    assert "không phải xác suất hỏng đã hiệu chỉnh" in text  # the fixed warning
    # the first Top-K drive is HEALTHY by the rules: the conflict explanation is shown and names the risk score
    assert any("Mâu thuẫn" in str(e.value) and "điểm rủi ro" in str(e.value).lower() for e in at.error)


def test_failure_prediction_leads_with_normal_segment_and_hides_full_test_recall(empty_dirs):
    _fill(*empty_dirs)

    at = _page("failure_prediction").run()
    text = _texts(at)

    assert "Đoạn normal" in text or "đoạn normal" in text
    assert "5.55" in text  # normal-segment recall@100 of the official model
    assert "44.44" not in text and "0.4444" not in text and "43.41" not in text  # full-test recall never shown
    assert "right-censoring" in text  # the tail explanation
    assert "không được hiển thị" in text  # visible note that the full-test recall is withheld
    assert "288" in text


def test_data_analytics_lookup_explains_scope_when_serial_is_not_listed(empty_dirs):
    _fill(*empty_dirs)

    at = _page("data_analytics").run()
    at.text_input(key="snap_query").set_value("NOT_A_SERIAL").run()

    assert not at.exception
    assert any("WATCH/CRITICAL và Top-K" in str(w.value) for w in at.warning)


def test_cluster_performance_renders_latest_pipeline_run(empty_dirs):
    _fill(*empty_dirs)

    at = _page("cluster_performance").run()

    assert not at.exception
    assert any("37.91" in str(m.value) for m in at.markdown)


def test_failure_prediction_warns_when_topk_scores_are_all_tied(empty_dirs):
    dashboard, reports = empty_dirs
    _fill(dashboard, reports)
    topk = pd.read_parquet(str(dashboard / "predictions_topk.parquet"))
    topk["risk_score"] = 1.0
    topk.to_parquet(str(dashboard / "predictions_topk.parquet"), index=False)

    at = _page("failure_prediction").run()

    assert not at.exception
    assert any("bão hòa" in str(w.value) and "margin" in str(w.value) for w in at.warning)


@pytest.mark.parametrize("page", PAGE_FILES)
def test_every_page_states_model_version_and_data_dates(empty_dirs, page):
    _fill(*empty_dirs)

    text = _texts(_page(page).run())

    assert "Mô hình vtest" in text and "2026-01-01" in text and "2026-03-31" in text and "2026-03-24" in text


def test_page_without_overview_says_provenance_is_unknown(empty_dirs):
    text = _texts(_page("smart_analysis").run())

    assert "không biết phiên bản mô hình" in text


def test_no_page_scripts_in_a_pages_directory_so_deep_links_go_through_app_py():
    # with a pages/ directory Streamlit runs a deep-linked page script directly, skipping app.py
    legacy = PAGES_DIR.parent / "pages"
    assert not legacy.exists() or not list(legacy.glob("*.py"))


def test_app_router_declares_all_five_views():
    source = (PAGES_DIR.parent / "app.py").read_text(encoding="utf-8")

    for name in PAGE_FILES:
        assert "views/{}.py".format(name) in source
        assert (PAGES_DIR / (name + ".py")).exists()


def test_failure_prediction_heading_has_no_placeholder_date_without_data(empty_dirs):
    at = _page("failure_prediction").run()

    assert not at.exception
    assert any(sh.value == "Top-100 ổ có điểm rủi ro cao nhất" for sh in at.subheader)


def test_smart_history_has_one_chart_per_indicator_and_skips_all_null_series(empty_dirs):
    _fill(*empty_dirs)

    at = _page("failure_prediction").run()
    text = _texts(at)

    assert not at.exception
    for name in ("smart_5_raw", "smart_187_raw", "smart_197_raw", "smart_198_raw"):
        assert "**{}**".format(name) in text
    # 3 history line charts (smart_187_raw is all null in the fixture: no chart for it) + 2 importance bar charts
    assert len(at.get("arrow_vega_lite_chart")) == 3 + 2
    assert "Không có giá trị (null) trong 30 ngày" in text


def test_data_analytics_tables_use_short_column_names(empty_dirs):
    _fill(*empty_dirs)

    at = _page("data_analytics").run()
    columns = {c for frame in at.dataframe for c in frame.value.columns}

    assert not at.exception
    assert "Gấp (lần)" in columns and "TB smart_198" in columns and "% null smart_187" in columns
    assert "Gấp bao nhiêu lần mức Khỏe" not in columns and "mean_smart_198_raw" not in columns


def test_failure_prediction_notes_that_missing_flags_may_reflect_drive_type(empty_dirs):
    _fill(*empty_dirs)

    text = _texts(_page("failure_prediction").run())

    assert "smart_187_raw_is_missing" in text and "loại hoặc dòng ổ" in text


def test_cluster_performance_shows_benchmark_tables_notes_and_environment(empty_dirs):
    _fill(*empty_dirs)

    at = _page("cluster_performance").run()
    text = _texts(at)
    columns = {c for frame in at.dataframe for c in frame.value.columns}

    assert not at.exception
    assert "Trung vị (s)" in columns and "Mục" in columns
    assert "Định dạng: CSV so với Parquet" in text and "csv_7d" in text and "host_ram_gb" in text
    assert "1 lần, không phải trung vị" in text and "bộ nhớ đệm" in text
    assert "930.0" in text  # size shown in MB
    assert len(at.get("arrow_vega_lite_chart")) == 1  # only the multi-row experiment (format) gets a chart


def test_cluster_performance_without_benchmark_points_to_the_runner(empty_dirs):
    at = _page("cluster_performance").run()

    hints = " ".join(str(i.value) for i in at.info)
    assert not at.exception and "scripts/run_benchmark.py" in hints

