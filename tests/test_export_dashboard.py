from __future__ import annotations

import datetime as dt
import json
import random

import pandas as pd
import pytest
from pyspark.sql import functions as F

from analytics.export_dashboard import (
    build_health_distribution,
    build_health_snapshot,
    build_overview,
    build_predictions_topk,
    build_smart_history,
    lr_importance_rows,
    model_metrics_rows,
    rf_importance_rows,
    run,
)
from analytics.health_status import classify_health
from ml.train import add_class_weight, train_model

SILVER_SCHEMA = (
    "date date, serial_number string, model string, failure int, "
    "smart_5_raw bigint, smart_187_raw bigint, smart_197_raw bigint, smart_198_raw bigint"
)
PRED_SCHEMA = (
    "date date, serial_number string, model string, risk_score double, risk_rank int, alert boolean, model_version string"
)


def _day(n):
    return dt.date(2026, 1, n)


def _silver(spark):
    rows = []
    for d in range(1, 21):
        rows.append((_day(d), "H1", "ModelA", 0, 0, None, 0, 0))
        rows.append((_day(d), "H2", "ModelA", 0, 0, None, 0, 0))
        rows.append((_day(d), "W", "ModelB", 0, 0, None, 1 if d >= 15 else 0, 0))
    for d in range(1, 13):  # F fails on day 12, with big smart_5 (CRITICAL) on days 8-11
        rows.append((_day(d), "F", "ModelB", 1 if d == 12 else 0, 200 if 8 <= d <= 11 else 0, None, 0, 0))
    return spark.createDataFrame(rows, SILVER_SCHEMA)


def _predictions(spark):
    rows = [
        (_day(20), "W", "ModelB", 0.9, 1, True, "vtest"),
        (_day(20), "H1", "ModelA", 0.5, 2, True, "vtest"),
        (_day(20), "H2", "ModelA", 0.1, 3, False, "vtest"),
        (_day(19), "H2", "ModelA", 0.7, 1, True, "vtest"),  # an older scored day, must be ignored
    ]
    return spark.createDataFrame(rows, PRED_SCHEMA)


def _health(spark):
    return classify_health(_silver(spark))


def test_overview_is_one_row_with_afr_as_yearly_ratio(spark):
    rows = build_overview(_silver(spark), _predictions(spark), "vtest", 2).collect()
    assert len(rows) == 1
    r = rows[0].asDict()

    assert r["data_start"] == _day(1) and r["data_end"] == _day(20)
    assert r["drive_days"] == 60 + 12 and r["drive_count"] == 4 and r["failure_count"] == 1
    assert r["afr"] == pytest.approx(1 / (72 / 365.0))
    assert r["scored_date"] == _day(20) and r["scored_rows"] == 3
    assert r["model_version"] == "vtest" and r["k"] == 2


def test_health_distribution_shares_and_baseline(spark):
    df = build_health_distribution(_silver(spark), _health(spark))
    rows = {r["health_level"]: r.asDict() for r in df.collect()}

    assert df.columns == ["health_level", "drive_days", "share", "labeled_rows", "failed_within_7d", "failure_rate_7d"]
    assert set(rows) == {"HEALTHY", "WATCH", "CRITICAL"}
    assert sum(r["share"] for r in rows.values()) == pytest.approx(1.0)
    assert sum(r["drive_days"] for r in rows.values()) == 72
    # F is HEALTHY on days 5-7 (still failing within 7 days of day 12), CRITICAL on days 8-11
    assert rows["CRITICAL"]["failed_within_7d"] == 4 and rows["HEALTHY"]["failed_within_7d"] == 3
    assert rows["CRITICAL"]["failure_rate_7d"] > rows["HEALTHY"]["failure_rate_7d"]


def test_snapshot_lists_only_watch_and_critical_of_the_scored_day(spark):
    snap = build_health_snapshot(_health(spark), _day(20))

    assert snap.columns == ["date", "serial_number", "model", "health_level", "reasons"]
    assert [(r["serial_number"], r["health_level"]) for r in snap.collect()] == [("W", "WATCH")]


def test_predictions_topk_joins_health_and_keeps_only_alerts_of_scored_day(spark):
    top = build_predictions_topk(_predictions(spark), _health(spark), _day(20))
    rows = top.collect()

    assert top.columns == ["date", "risk_rank", "serial_number", "model", "risk_score", "alert",
                           "health_level", "reasons", "model_version"]
    assert [(r["risk_rank"], r["serial_number"], r["health_level"]) for r in rows] == [(1, "W", "WATCH"), (2, "H1", "HEALTHY")]
    assert all(r["alert"] for r in rows)


def test_predictions_topk_keeps_a_row_without_health_as_null(spark):
    preds = spark.createDataFrame([(_day(20), "UNKNOWN", "M", 0.4, 1, True, "vtest")], PRED_SCHEMA)
    row = build_predictions_topk(preds, _health(spark), _day(20)).collect()[0]

    assert row["serial_number"] == "UNKNOWN" and row["health_level"] is None and row["reasons"] is None


def test_smart_history_only_topk_serials_in_window_nulls_kept(spark):
    topk = build_predictions_topk(_predictions(spark), _health(spark), _day(20))
    hist = build_smart_history(_silver(spark), topk, _day(20), days=5)
    rows = hist.collect()

    assert hist.columns == ["serial_number", "date", "smart_5_raw", "smart_187_raw", "smart_197_raw", "smart_198_raw"]
    assert {r["serial_number"] for r in rows} == {"W", "H1"}
    assert min(r["date"] for r in rows) == _day(16) and max(r["date"] for r in rows) == _day(20)
    assert len(rows) == 2 * 5
    assert all(r["smart_187_raw"] is None for r in rows)  # null is not filled with 0


METRICS = {
    "K": 100,
    "val": {
        "logistic_regression": {"pr_auc": 0.0245, "roc_auc": 0.8545, "recall_at_k": 0.0921, "precision_at_k": 0.1027},
        "random_forest": {"pr_auc": 0.0349, "roc_auc": 0.9149, "recall_at_k": 0.0692, "precision_at_k": 0.0782},
        "baseline_rules_v1": {"recall_at_k": 0.0164, "precision_at_k": 0.0173},
    },
    "test": {
        "models": {
            "logistic_regression": {"pr_auc": 0.0204, "roc_auc": 0.8567, "recall_at_k": 0.4444, "precision_at_k": 0.4341},
            "random_forest": {"pr_auc": 0.0148, "roc_auc": 0.8863, "recall_at_k": 0.4315, "precision_at_k": 0.4253},
        },
        "baseline_rules_v1": {"recall_at_k": 0.4322, "precision_at_k": 0.4259},
    },
}


def _part(rows, positives, recall, precision):
    return {"rows": rows, "positives": positives, "recall_at_k": recall, "precision_at_k": precision}


def _breakdown():
    out = {}
    for model, normal in (("logistic_regression", 0.0555), ("random_forest", 0.0336), ("baseline_rules_v1", 0.0347)):
        out[model] = {
            "full_test": _part(3435296, 963, 0.44, 0.43),
            "normal_range": _part(3435008, 675, normal, 0.03),
            "tail_range": _part(288, 288, 1.0, 1.0),
        }
    return out


def test_model_metrics_never_publish_full_test_recall():
    rows = model_metrics_rows(METRICS, _breakdown())
    by_key = {(r[0], r[1], r[2]): r for r in rows}

    assert len(by_key) == len(rows)  # (split, segment, model) is unique
    for model in ("logistic_regression", "random_forest", "baseline_rules_v1"):
        full = by_key[("test", "full", model)]
        assert full[5] is None and full[6] is None  # recall/precision of the full test are withheld
        assert by_key[("test", "normal", model)][5] is not None
        tail = by_key[("test", "tail", model)]
        assert tail[8] == 288 and tail[9] == 288
    assert by_key[("test", "full", "logistic_regression")][3] == 0.0204  # AUCs of the full test are kept
    assert by_key[("test", "normal", "logistic_regression")][5] == 0.0555
    assert by_key[("val", "all", "baseline_rules_v1")][3] is None
    assert all(r[7] == 100 for r in rows)


def _lr_fixture(spark):
    rnd = random.Random(7)
    rows = []
    for i in range(300):
        driver = rnd.gauss(0, 1)
        noise = rnd.gauss(0, 1000.0)  # huge scale, no signal
        label = 1 if driver + rnd.gauss(0, 0.3) > 1.0 else 0
        rows.append(("S%d" % i, driver, noise, label))
    df = spark.createDataFrame(rows, "serial_number string, f_small double, f_big double, fail_within_7_days int")
    return df, train_model(df, ["f_small", "f_big"], "logistic_regression")


def test_lr_importance_uses_standardized_coefficients(spark):
    df, model = _lr_fixture(spark)
    rows = lr_importance_rows(model, df, "vtest", top_n=2)
    coef = dict(zip(["f_small", "f_big"], model.stages[1].coefficients.toArray()))

    assert [r[2] for r in rows] == ["f_small", "f_big"] and [r[1] for r in rows] == [1, 2]
    assert all(r[0] == "logistic_regression" and "vtest" in r[4] for r in rows)
    assert rows[0][3] > rows[1][3] >= 0.0

    # expected = |coef| x weighted unbiased std (Spark's Summarizer statistic)
    data = [(r["f_big"], r["class_weight"]) for r in add_class_weight(df).collect()]
    w_sum = sum(w for _, w in data)
    mean = sum(x * w for x, w in data) / w_sum
    var = sum(w * (x - mean) ** 2 for x, w in data) / (w_sum - sum(w * w for _, w in data) / w_sum)
    assert rows[1][3] == pytest.approx(abs(coef["f_big"]) * var ** 0.5, rel=1e-6)
    assert abs(coef["f_big"]) < abs(coef["f_small"])  # raw coefficients would understate the large-scale feature


def test_lr_importance_rejects_a_non_lr_model(spark):
    df, _ = _lr_fixture(spark)
    rf = train_model(df, ["f_small", "f_big"], "random_forest")

    with pytest.raises(ValueError):
        lr_importance_rows(rf, df, "vtest")


def test_rf_importance_reads_phase6_file_and_tolerates_absence(tmp_path):
    assert rf_importance_rows(tmp_path) == []
    payload = {"features": [{"rank": 2, "feature": "b", "importance": 0.05}, {"rank": 1, "feature": "a", "importance": 0.1}]}
    (tmp_path / "rf_feature_importance_phase6.json").write_text(json.dumps(payload), encoding="utf-8")

    rows = rf_importance_rows(tmp_path)
    assert [(r[0], r[1], r[2]) for r in rows] == [("random_forest", 1, "a"), ("random_forest", 2, "b")]
    assert all("không tái lập" in r[4] for r in rows)


def test_run_exports_all_hd8_tables(spark, tmp_path):
    names = ("silver", "health", "preds", "features", "model")
    paths = {n: (tmp_path / n).as_uri() for n in names}
    _silver(spark).write.parquet(paths["silver"])
    _health(spark).write.parquet(paths["health"])
    _predictions(spark).write.parquet(paths["preds"])
    train_df, model = _lr_fixture(spark)
    train_df.withColumn("split", F.lit("train")).write.parquet(paths["features"])
    model.save(paths["model"])
    reports = tmp_path / "reports"
    reports.mkdir()
    (reports / "metrics.json").write_text(json.dumps(METRICS), encoding="utf-8")
    (reports / "test_breakdown.json").write_text(json.dumps(_breakdown()), encoding="utf-8")
    (reports / "rf_feature_importance_phase6.json").write_text(
        json.dumps({"features": [{"rank": 1, "feature": "f_small", "importance": 0.2}]}), encoding="utf-8")

    stats = run(spark, paths["silver"], paths["health"], paths["preds"], paths["features"], paths["model"],
                "vtest", 2, reports_dir=reports, export_dir=tmp_path / "dashboard")

    assert set(stats["tables"]) == {
        "dashboard_overview", "health_distribution", "health_snapshot", "predictions_topk",
        "smart_history_topk", "model_metrics", "model_feature_importance",
    }
    assert stats["scored_date"] == "2026-01-20" and stats["skipped"] == []
    overview = pd.read_parquet(str(tmp_path / "dashboard" / "dashboard_overview.parquet"))
    assert len(overview) == 1 and int(overview["drive_days"][0]) == 72
    imp = pd.read_parquet(str(tmp_path / "dashboard" / "model_feature_importance.parquet"))
    assert set(imp["model"]) == {"logistic_regression", "random_forest"} and len(imp) == 3
    assert not imp.duplicated(["model", "rank"]).any()
    topk = pd.read_parquet(str(tmp_path / "dashboard" / "predictions_topk.parquet"))
    assert list(topk["risk_rank"]) == [1, 2]
