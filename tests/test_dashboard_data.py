from __future__ import annotations

import numpy as np
import pandas as pd

from ui_dashboard import data
from ui_dashboard.explain import RISK_SCORE_NOTE, TAIL_NOTE, conflict_note, missing_flag_note, tie_note


def _write(base, name, df):
    df.to_parquet(str(base / (name + ".parquet")), index=False)


def test_load_table_missing_returns_hint_with_the_job_that_creates_it(tmp_path):
    df, hint = data.load_table("predictions_topk", tmp_path)

    assert df is None
    assert "predictions_topk" in hint and "analytics/export_dashboard.py" in hint


def test_load_table_reads_existing_parquet(tmp_path):
    _write(tmp_path, "afr_by_model", pd.DataFrame({"model": ["A"], "drive_days": [10], "failures": [1], "afr": [0.5]}))

    df, hint = data.load_table("afr_by_model", tmp_path)

    assert hint is None and list(df["model"]) == ["A"]


def test_load_table_corrupt_file_returns_hint_instead_of_raising(tmp_path):
    (tmp_path / "afr_by_model.parquet").write_text("not a parquet file", encoding="utf-8")

    df, hint = data.load_table("afr_by_model", tmp_path)

    assert df is None and "không đọc được" in hint


def test_table_status_lists_every_known_table_with_availability(tmp_path):
    _write(tmp_path, "afr_by_model", pd.DataFrame({"x": [1]}))

    status = data.table_status(tmp_path).set_index("Bảng")["Có dữ liệu"]

    assert set(status.index) == set(data.TABLES)
    assert bool(status["afr_by_model"]) and not bool(status["predictions_topk"])


def test_display_labels_never_call_risk_score_a_probability():
    for label in list(data.DISPLAY_COLUMNS.values()) + list(data.MODEL_LABELS.values()) + list(data.HEALTH_LABELS.values()):
        assert "xác suất" not in label.lower() and "probability" not in label.lower()
    assert data.DISPLAY_COLUMNS["risk_score"] == "Điểm rủi ro"


def test_reasons_text_handles_arrays_lists_and_null():
    assert data.reasons_text(np.array(["smart_5_raw > 0", "smart_197_raw > 0"])) == "smart_5_raw > 0; smart_197_raw > 0"
    assert data.reasons_text(["a"]) == "a"
    assert data.reasons_text(None) == ""


def _topk():
    return pd.DataFrame({
        "date": ["2026-03-24"] * 3,
        "risk_rank": [2, 1, 3],
        "serial_number": ["B", "A", "C"],
        "model": ["M1", "M1", "M2"],
        "risk_score": [0.5, 0.9, 0.3],
        "alert": [True, True, True],
        "health_level": ["HEALTHY", "CRITICAL", None],
        "reasons": [np.array([], dtype=object), np.array(["smart_5_raw >= 102"]), None],
        "model_version": ["v"] * 3,
    })


def test_topk_display_sorts_by_rank_and_uses_vietnamese_columns():
    out = data.topk_display(_topk(), {"B": "Mâu thuẫn"})

    assert list(out.columns) == ["Hạng", "Serial", "Model ổ", "Điểm rủi ro", "Mức tình trạng", "Lý do (luật rules_v1)", "Ghi chú"]
    assert list(out["Serial"]) == ["A", "B", "C"]
    assert list(out["Ghi chú"]) == ["", "Mâu thuẫn", ""]
    assert out["Lý do (luật rules_v1)"].iloc[0] == "smart_5_raw >= 102"


def test_conflict_summary_counts_critical_outside_topk():
    snapshot = pd.DataFrame({"serial_number": ["A", "X", "Y", "W"], "health_level": ["CRITICAL", "CRITICAL", "CRITICAL", "WATCH"]})

    summary = data.conflict_summary(_topk(), snapshot)

    assert summary["topk_total"] == 3
    assert summary["topk_by_level"] == {"HEALTHY": 1, "CRITICAL": 1, "(không có)": 1}
    assert (summary["critical_total"], summary["critical_in_topk"], summary["critical_outside_topk"]) == (3, 1, 2)


def _metrics():
    def row(split, segment, model, pr, roc, recall, precision, rows, pos):
        return {"split": split, "segment": segment, "model": model, "pr_auc": pr, "roc_auc": roc, "recall_at_k": recall,
                "precision_at_k": precision, "k": 100, "rows": rows, "positives": pos}

    rows = []
    for model, normal in (("logistic_regression", 0.0555), ("random_forest", 0.0336), ("baseline_rules_v1", 0.0347)):
        has_auc = model != "baseline_rules_v1"
        rows.append(row("val", "all", model, 0.02 if has_auc else None, 0.85 if has_auc else None, 0.09, 0.1, None, None))
        # a file that (wrongly) still carries the misleading full-test recall must not leak into any view
        rows.append(row("test", "full", model, 0.02 if has_auc else None, 0.86 if has_auc else None, 0.4444, 0.4341, 3435296, 963))
        rows.append(row("test", "normal", model, None, None, normal, 0.038, 3435008, 675))
        rows.append(row("test", "tail", model, None, None, 1.0, 1.0, 288, 288))
    return pd.DataFrame(rows)


def test_metrics_views_never_expose_full_test_recall_and_lead_with_normal():
    views = data.metrics_views(_metrics())

    for name, frame in views.items():
        values = frame.select_dtypes("number").to_numpy().ravel()
        assert not np.isclose(values, 44.44, atol=0.01).any(), name
        assert not np.isclose(values, 0.4444).any(), name
        assert not np.isclose(values, 43.41, atol=0.01).any(), name
    normal = views["test_normal"]
    assert list(normal.columns) == ["Model", "Recall@100 (%)", "Precision@100 (%)"]
    assert np.isclose(normal.loc[normal["Model"].str.startswith("Logistic"), "Recall@100 (%)"].iloc[0], 5.55)
    assert list(views["test_tail"]["Recall@100 (%)"]) == [100.0, 100.0, 100.0]
    assert (int(views["tail_size"]["rows"].iloc[0]), int(views["tail_size"]["positives"].iloc[0])) == (288, 288)
    assert list(views["test_auc"].columns) == ["Model", "PR-AUC (toàn test)", "ROC-AUC (toàn test)"]
    assert len(views["test_auc"]) == 2  # the baseline has no AUC


def test_conflict_notes_cover_each_combination_and_say_risk_score_not_probability():
    kinds = {
        ("CRITICAL", True): "agree",
        ("WATCH", True): "agree",
        ("HEALTHY", True): "conflict",
        ("CRITICAL", False): "conflict",
        ("WATCH", False): "info",
        ("HEALTHY", False): None,
        (None, True): "info",
    }
    for (level, alert), expected in kinds.items():
        kind, text = conflict_note(level, alert, 100)
        assert kind == expected, (level, alert)
        assert (text is None) == (expected is None)
        if text:
            assert "xác suất" not in text.lower()

    assert "điểm rủi ro" in conflict_note("HEALTHY", True, 100)[1].lower()
    assert "Top-100" in conflict_note("CRITICAL", False, 100)[1]
    assert "không phải xác suất" in RISK_SCORE_NOTE
    assert "right-censoring" in TAIL_NOTE and "100%" in TAIL_NOTE


def test_tie_note_warns_when_most_topk_scores_are_saturated_at_one():
    note = tie_note([1.0] * 9 + [0.99], 10)

    assert "9 trong 10" in note and "margin" in note and "điểm rủi ro" in note
    assert "thứ tự phụ" not in note  # the pre-fix wording (tie-break by serial only) is gone
    assert "xác suất" not in note.lower()
    assert tie_note([0.9, 0.8, 0.7, 1.0], 4) is None  # a single saturated score is not a tie problem
    assert tie_note([], 10) is None


def test_style_metrics_shows_a_dash_instead_of_none_or_nan():
    df = pd.DataFrame({"Model": ["LR", "Luật"], "PR-AUC": [0.02449, None], "Recall@100 (%)": [9.2072, 1.6373]})

    out = data.style_metrics(df)

    assert list(out["PR-AUC"]) == ["0.0245", "—"]
    assert list(out["Recall@100 (%)"]) == ["9.21", "1.64"]
    assert list(out["Model"]) == ["LR", "Luật"]
    assert not out.astype(str).apply(lambda col: col.str.contains("None|nan", case=False)).any().any()


def test_missing_flag_note_lists_the_flags_found_and_stays_cautious():
    note = missing_flag_note(["smart_197_raw", "smart_187_raw_is_missing", "smart_188_raw_is_missing"])

    assert "smart_187_raw_is_missing" in note and "smart_188_raw_is_missing" in note
    assert "loại hoặc dòng ổ" in note and "không bị sửa" in note
    assert missing_flag_note(["smart_197_raw", "smart_5_raw_max_7d"]) is None


def test_benchmark_views_format_numbers_and_use_a_dash_for_missing_values():
    df = pd.DataFrame({
        "experiment": ["pipeline_steps", "format"], "variant": ["batch/silver_etl", "csv_7d"], "query": ["pipeline step", "q"],
        "runs": [1, 3], "median_seconds": [269.35, 40.0], "min_seconds": [269.35, 39.0], "max_seconds": [269.35, 41.0],
        "size_bytes": [None, 2 * 1048576.0], "file_count": [None, 1234.0], "input_partitions": [None, 14.0],
        "executors": [None, 2.0], "rows": [None, 3000000.0], "note": ["1 lần, không phải trung vị", ""]})

    views = data.benchmark_views(df)

    assert list(views) == ["format", "pipeline_steps"]  # contract order, not row order
    # columns empty for the whole experiment are dropped: the format table has no notes, the pipeline table no sizes/rows
    assert list(views["format"].columns) == ["Biến thể", "Lần", "Trung vị (s)", "Min–Max (s)", "MB", "File", "Partition",
                                            "Executor", "Số dòng"]
    assert list(views["pipeline_steps"].columns) == ["Biến thể", "Lần", "Trung vị (s)", "Min–Max (s)", "Ghi chú"]
    fmt = views["format"].iloc[0]
    assert (fmt["Lần"], fmt["Trung vị (s)"], fmt["Min–Max (s)"], fmt["MB"], fmt["File"], fmt["Số dòng"]) == (
        "3", "40.00", "39.00–41.00", "2.0", "1,234", "3,000,000")
    step = views["pipeline_steps"].iloc[0]
    assert step["Ghi chú"] == "1 lần, không phải trung vị" and step["Min–Max (s)"] == "269.35–269.35"
    # an em dash is still shown for a missing value inside a column that has some values
    mixed = pd.DataFrame({
        "experiment": ["format", "format"], "variant": ["a", "b"], "query": ["q", "q"], "runs": [3, 3],
        "median_seconds": [1.0, 2.0], "min_seconds": [1.0, 2.0], "max_seconds": [1.0, 2.0], "size_bytes": [1048576.0, None],
        "file_count": [1.0, None], "input_partitions": [1.0, None], "executors": [2.0, 2.0], "rows": [5.0, 5.0], "note": ["", "n"]})
    assert list(data.benchmark_views(mixed)["format"]["MB"]) == ["1.0", "—"]
    assert "benchmark_environment" in data.TABLES and data.TABLES["benchmark"][1] == "HĐ9"


def test_benchmark_labels_keep_two_workloads_of_one_variant_apart():
    part = pd.DataFrame({
        "variant": ["cores_2_executors_1", "cores_2_executors_1", "csv_7d"],
        "query": ["count+groupBy(model) @ csv_7d", "count+groupBy(model) @ silver_q1", "count+groupBy(model)"],
        "median_seconds": [7.5, 2.6, 4.3]})

    labels = list(data.benchmark_chart_frame(part)["label"])

    assert labels == ["cores_2_executors_1 @ csv_7d", "cores_2_executors_1 @ silver_q1", "csv_7d"]
    assert len(set(labels)) == 3  # a bar chart keyed on this label cannot stack two measurements into one bar
    assert data.benchmark_label("batch_pipeline/silver_etl", "pipeline step") == "batch_pipeline/silver_etl"
