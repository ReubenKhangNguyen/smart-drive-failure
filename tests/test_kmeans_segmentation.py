from __future__ import annotations

import datetime as dt
from pathlib import Path

from analytics.kmeans_segmentation import build_snapshot, segment, run, split_zero_signal

COLS = ["smart_5_raw", "smart_187_raw", "smart_197_raw", "smart_198_raw"]
SCHEMA = "date date, serial_number string, failure int, smart_5_raw bigint, smart_187_raw bigint, smart_197_raw bigint, smart_198_raw bigint"
HEALTH_SCHEMA = "serial_number string, date date, health_level string"


def _day(n):
    return dt.date(2026, 1, n)


def _drive(serial, last_values, days=5, failure_day=None, null_187=False):
    """Zeros on every day but day 5, where the SMART values are `last_values` (day 5 is the last
    observed day of a healthy drive, and the day before failure_day=6 for a failed one)."""
    rows = []
    for d in range(1, days + 1):
        values = last_values if d == 5 else (0, 0, 0, 0)
        v187 = None if (null_187 and d == 5) else values[1]
        rows.append((_day(d), serial, 1 if failure_day == d else 0, values[0], v187, values[2], values[3]))
    return rows


def _silver(spark):
    rows = []
    for i in range(5):  # zero-signal healthy drives; two report no smart_187
        rows += _drive("Z%d" % i, (0, 0, 0, 0), null_187=i < 2)
    for i in range(12):  # three signal blobs of different magnitude
        rows += _drive("L%d" % i, (1 + i % 2, 0, 1 + i % 3, 0))
        rows += _drive("M%d" % i, (50 + i, 20 + i, 60 + i, 10 + i % 3))
        rows += _drive("H%d" % i, (5000 + 10 * i, 900 + i, 7000 + 10 * i, 400 + i))
    rows += _drive("FA", (5100, 910, 7100, 410), days=6, failure_day=6)  # fails on day 6 -> snapshot day 5
    rows += _drive("FB", (0, 0, 0, 0), days=6, failure_day=6)  # fails with no signal at all
    rows += [(_day(1), "FC", 1, 9, 9, 9, 9)]  # fails on its first day: no prior day, must be excluded
    return spark.createDataFrame(rows, SCHEMA)


def _health(spark, silver_df):
    snapshot = build_snapshot(silver_df, COLS).collect()
    rows = [(r["serial_number"], r["date"], "CRITICAL" if r["is_failed"] else "HEALTHY") for r in snapshot]
    return spark.createDataFrame(rows, HEALTH_SCHEMA)


def _segment(spark, k_min=2, k_max=4):
    silver = _silver(spark)
    return segment(silver, _health(spark, silver), COLS, k_min, k_max, seed=42, max_iter=30)


def test_snapshot_uses_last_day_for_healthy_and_day_before_failure_for_failed(spark):
    snapshot = {r["serial_number"]: r.asDict() for r in build_snapshot(_silver(spark), COLS).collect()}

    assert len(snapshot) == 5 + 36 + 2  # FC has no prior day -> excluded
    assert "FC" not in snapshot
    assert snapshot["H0"]["date"] == _day(5) and snapshot["H0"]["smart_5_raw"] == 5000.0
    assert snapshot["FA"]["is_failed"] is True and snapshot["FA"]["date"] == _day(5)
    assert snapshot["FA"]["smart_197_raw"] == 7100.0
    assert snapshot["Z0"]["smart_187_missing"] is True and snapshot["Z0"]["smart_187_raw"] == 0.0  # null filled with 0
    assert snapshot["Z3"]["smart_187_missing"] is False


def test_zero_signal_group_is_split_before_kmeans(spark):
    zero, with_signal = split_zero_signal(build_snapshot(_silver(spark), COLS), COLS)

    assert sorted(r["serial_number"] for r in zero.collect()) == ["FB", "Z0", "Z1", "Z2", "Z3", "Z4"]
    assert with_signal.count() == 37


def test_clusters_table_has_zero_signal_row_ordered_ids_and_consistent_totals(spark):
    result = _segment(spark)
    rows = {r["cluster_id"]: r.asDict() for r in result["clusters"].collect()}
    stats = result["stats"]

    assert result["clusters"].columns == [
        "cluster_type", "cluster_id", "size", "healthy_count", "failed_count", "failed_rate",
        "mean_smart_5_raw", "mean_smart_187_raw", "mean_smart_197_raw", "mean_smart_198_raw", "smart_187_missing_share",
    ]
    types = dict(result["clusters"].dtypes)
    assert types["cluster_id"] == "int" and types["size"] == "bigint" and types["failed_count"] == "bigint"
    assert all(types[c] == "double" for c in types if c.startswith("mean_") or c in ("failed_rate", "smart_187_missing_share"))
    assert dict(result["crosstab"].dtypes) == {"cluster_id": "int", "health_level": "string", "drives": "bigint"}
    assert dict(result["k_selection"].dtypes) == {"k": "int", "silhouette": "double", "wssse": "double", "is_selected": "boolean"}
    assert rows[0]["cluster_type"] == "zero_signal" and rows[0]["size"] == 6
    assert rows[0]["failed_count"] == 1 and rows[0]["failed_rate"] == 1 / 6.0
    assert rows[0]["smart_187_missing_share"] == 2 / 6.0
    kmeans_ids = sorted(i for i in rows if i != 0)
    assert kmeans_ids == list(range(1, stats["selected_k"] + 1)) and 2 <= stats["selected_k"] <= 4
    means_197 = [rows[i]["mean_smart_197_raw"] for i in kmeans_ids]
    assert means_197 == sorted(means_197)  # ids ordered by mean smart_197 ascending
    assert sum(r["size"] for r in rows.values()) == stats["drives_in_snapshot"] == 43
    assert all(r["healthy_count"] + r["failed_count"] == r["size"] for r in rows.values())
    assert stats["failed_in_zero_signal"] == 1 and stats["failed_excluded_no_prior_day"] == 1


def test_k_selection_table_marks_exactly_one_selected_k(spark):
    result = _segment(spark)
    rows = result["k_selection"].collect()

    assert result["k_selection"].columns == ["k", "silhouette", "wssse", "is_selected"]
    assert [r["k"] for r in rows] == [2, 3, 4]
    selected = [r for r in rows if r["is_selected"]]
    assert len(selected) == 1 and selected[0]["silhouette"] == max(r["silhouette"] for r in rows)


def test_health_crosstab_totals_match_cluster_sizes(spark):
    result = _segment(spark)
    sizes = {r["cluster_id"]: r["size"] for r in result["clusters"].collect()}
    totals = {}
    for r in result["crosstab"].collect():
        totals[r["cluster_id"]] = totals.get(r["cluster_id"], 0) + r["drives"]

    assert result["crosstab"].columns == ["cluster_id", "health_level", "drives"]
    assert totals == sizes


def test_same_seed_gives_same_clusters(spark):
    first = [r.asDict() for r in _segment(spark)["clusters"].collect()]
    second = [r.asDict() for r in _segment(spark)["clusters"].collect()]

    assert first == second


def test_run_writes_three_tables_exports_and_report(spark, tmp_path):
    silver = _silver(spark)
    silver_path, health_path = (tmp_path / "silver").as_uri(), (tmp_path / "health").as_uri()
    silver.write.parquet(silver_path)
    _health(spark, silver).write.parquet(health_path)
    analytics_path = (tmp_path / "analytics").as_uri()

    stats = run(spark, silver_path, health_path, analytics_path, COLS, 2, 4, 42, 30,
                export_dir=tmp_path / "dashboard", report_dir=tmp_path / "reports", run_date="2026-10-02")

    assert set(stats["tables"]) == {"kmeans_k_selection", "kmeans_clusters", "kmeans_health_crosstab"}
    for name, count in stats["tables"].items():
        assert spark.read.parquet((tmp_path / "dashboard" / (name + ".parquet")).as_uri()).count() == count
    report = (tmp_path / "reports" / "kmeans_segmentation.md").read_text(encoding="utf-8")
    assert "mô tả hồi cứu" in report.lower() and "recall@K" in report and "zero_signal" in report


def test_model_code_never_uses_cluster_ids():
    root = Path(__file__).resolve().parent.parent
    for folder in ("features", "ml"):
        for path in (root / folder).glob("*.py"):
            text = path.read_text(encoding="utf-8").lower()
            assert "kmeans" not in text and "cluster_id" not in text, path.name
