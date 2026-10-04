from __future__ import annotations

import datetime as dt
import json

from analytics.export_dashboard import (
    CATALOG_SCHEMA, QUARTER_METRICS_SCHEMA, QUARTER_MONTHLY_SCHEMA, catalog_rows, quarter_metric_rows,
)
from config.settings import quarters_from_data

DATA = {
    "start_date": "2026-01-01", "end_date": "2026-03-31", "warmup_end_date": "2026-01-30",
    "train_end_date": "2026-03-03", "val_end_date": "2026-03-14",
    "quarters": [
        {"id": "2026-Q2", "split": "oot", "start_date": "2026-04-01", "end_date": "2026-06-30", "report": "oot_metrics.json"},
        {"id": "2026-Q3", "split": "oot2", "start_date": "2026-07-01", "end_date": "2026-09-30"},
    ],
}


def _segment(rows, positives, lr, rules):
    return {"rows": rows, "positives": positives, "positive_rate": positives / rows, "logistic_regression": lr, "baseline_rules_v1": rules}


REPORT = {
    "k": 100, "normal_end_date": "2026-06-23", "model_version": "vtest", "run_date": "2026-10-04", "quarter": "2026-Q2", "split": "oot",
    "segments": {
        "normal": _segment(1000, 20, {"pr_auc": 0.0277, "roc_auc": 0.8485, "recall_at_k": 0.0823, "precision_at_k": 0.0932,
                                      "recall_at_k_margin_tiebreak": 0.0758, "precision_at_k_margin_tiebreak": 0.0861},
                           {"recall_at_k": 0.0212, "precision_at_k": 0.0268}),
        "tail": _segment(10, 10, {"pr_auc": None, "roc_auc": None, "recall_at_k": 1.0, "precision_at_k": 1.0,
                                  "recall_at_k_margin_tiebreak": 1.0, "precision_at_k_margin_tiebreak": 1.0},
                         {"recall_at_k": 1.0, "precision_at_k": 1.0}),
        "full": _segment(1010, 30, {"pr_auc": 0.0286, "roc_auc": 0.8489, "recall_at_k": 0.1529, "precision_at_k": 0.163,
                                    "recall_at_k_margin_tiebreak": 0.1469, "precision_at_k_margin_tiebreak": 0.1564},
                         {"recall_at_k": 0.0965, "precision_at_k": 0.1016}),
    },
    "monthly_normal": [
        {"month": "2026-04", "days": 30, "positives": 12, "logistic_regression": {"recall_at_k": 0.0853, "precision_at_k": 0.1213},
         "baseline_rules_v1": {"recall_at_k": 0.0367, "precision_at_k": 0.0523}},
        {"month": "2026-05", "days": 31, "positives": 8, "logistic_regression": {"recall_at_k": 0.0975, "precision_at_k": 0.101},
         "baseline_rules_v1": {"recall_at_k": 0.0132, "precision_at_k": 0.0152}},
    ],
}


def test_catalog_names_every_split_with_its_dates_and_whether_the_model_saw_it():
    rows = {r[0]: r for r in catalog_rows(DATA)}

    assert list(rows) == ["train", "val", "test", "oot", "oot2"]
    assert rows["train"][2:5] == ("tập huấn luyện", "analysis", True) and rows["train"][5:] == (dt.date(2026, 1, 31), dt.date(2026, 3, 3))
    assert rows["val"][5:] == (dt.date(2026, 3, 4), dt.date(2026, 3, 14)) and rows["val"][4] is True
    assert rows["test"][2] == "tập test Q1" and rows["test"][4] is False and rows["test"][6] == dt.date(2026, 3, 31)
    assert rows["oot"][1:5] == ("2026-Q2", "Q2/2026 ngoài thời gian", "out_of_time", False)
    assert rows["oot2"][2] == "Q3/2026 ngoài thời gian" and rows["oot2"][5:] == (dt.date(2026, 7, 1), dt.date(2026, 9, 30))


def test_quarter_metrics_skip_quarters_without_a_report_and_hide_full_recall(tmp_path):
    (tmp_path / "oot_metrics.json").write_text(json.dumps(REPORT), encoding="utf-8")  # Q3 has no report yet

    metric_rows, monthly_rows = quarter_metric_rows(quarters_from_data(DATA), tmp_path)

    assert {r[0] for r in metric_rows} == {"2026-Q2"}
    by_key = {(r[2], r[3]): r for r in metric_rows}
    assert set(by_key) == {(s, m) for s in ("normal", "tail", "full") for m in ("logistic_regression", "baseline_rules_v1")}
    normal_lr = by_key[("normal", "logistic_regression")]
    assert normal_lr[1] == "oot" and normal_lr[4:6] == (1000, 20) and normal_lr[6:10] == (0.0277, 0.8485, 0.0823, 0.0932) and normal_lr[12] == 100
    assert by_key[("normal", "baseline_rules_v1")][8:10] == (0.0212, 0.0268) and by_key[("normal", "baseline_rules_v1")][6:8] == (None, None)
    full_lr = by_key[("full", "logistic_regression")]
    assert full_lr[8:12] == (None, None, None, None)  # recall of the full segment is dominated by the censored tail: never published
    assert by_key[("tail", "logistic_regression")][8] == 1.0  # the tail is kept, flagged by its segment name
    assert len(monthly_rows) == 4 and {r[4] for r in monthly_rows} == {"logistic_regression", "baseline_rules_v1"}
    assert monthly_rows[0][:4] == ("2026-Q2", "2026-04", 30, 12)


def test_no_report_at_all_gives_no_rows(tmp_path):
    assert quarter_metric_rows(quarters_from_data(DATA), tmp_path) == ([], [])


def test_rows_fit_the_declared_spark_schemas(spark, tmp_path):
    (tmp_path / "oot_metrics.json").write_text(json.dumps(REPORT), encoding="utf-8")
    metric_rows, monthly_rows = quarter_metric_rows(quarters_from_data(DATA), tmp_path)

    catalog = spark.createDataFrame(catalog_rows(DATA), CATALOG_SCHEMA)
    metrics = spark.createDataFrame(metric_rows, QUARTER_METRICS_SCHEMA)
    monthly = spark.createDataFrame(monthly_rows, QUARTER_MONTHLY_SCHEMA)

    assert catalog.count() == 5 and metrics.count() == 6 and monthly.count() == 4
    assert metrics.where("segment = 'full' and recall_at_k is not null").count() == 0
