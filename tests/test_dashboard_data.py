from __future__ import annotations

import numpy as np
import pandas as pd

from ui_dashboard import data
from ui_dashboard.explain import RISK_SCORE_NOTE, TAIL_NOTE, conflict_note


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
