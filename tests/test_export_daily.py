from __future__ import annotations

from pyspark.sql import functions as F

from analytics.export_dashboard import build_daily_exports

SMART = ["smart_5_raw", "smart_187_raw", "smart_197_raw", "smart_198_raw"]


def _date(df):
    return df.withColumn("date", F.to_date("date"))


def _inputs(spark):
    silver_rows = []
    for day in range(1, 41):  # SN_A reports 40 days: 2026-02-01 .. 2026-03-12
        date = "2026-02-{:02d}".format(day) if day <= 28 else "2026-03-{:02d}".format(day - 28)
        silver_rows.append((date, "SN_A", 1.0, None, float(day), 0.0))
    silver_rows.append(("2026-03-12", "SN_B", 0.0, None, 0.0, 0.0))
    silver_rows.append(("2026-03-12", "SN_NOT_IN_TOPK", 9.0, None, 9.0, 9.0))
    silver = _date(spark.createDataFrame(silver_rows, "date string, serial_number string, smart_5_raw double, "
                                                     "smart_187_raw double, smart_197_raw double, smart_198_raw double"))

    health = _date(spark.createDataFrame(
        [("2026-03-12", "SN_A", "M", "CRITICAL", ["smart_5_raw >= 102"], "rules_v1"),
         ("2026-03-12", "SN_C", "M", "CRITICAL", ["smart_197_raw >= 16"], "rules_v1"),
         ("2026-03-12", "SN_B", "M", "HEALTHY", [], "rules_v1")],
        "date string, serial_number string, model string, health_level string, reasons array<string>, rules_version string"))

    topk = _date(spark.createDataFrame(
        [("2026-03-12", 1, "SN_A", "M", 0.9, 1, "v"), ("2026-03-12", 2, "SN_B", "M", 0.8, 0, "v"),
         ("2026-03-12", 3, "SN_NO_HEALTH", "M", 0.7, 0, "v")],
        "date string, risk_rank int, serial_number string, model string, risk_score double, failed_within_7d int, model_version string"))
    days = _date(spark.createDataFrame(
        [("2026-03-12", "val", 3000, 5, 1, "v"), ("2026-04-10", "oot", 3100, 6, 2, "v")],
        "date string, split string, scored_rows bigint, positives bigint, topk_hits bigint, model_version string"))
    return silver, health, topk, days


def test_topk_daily_joins_the_rules_level_and_keeps_missing_health_as_null(spark):
    out = build_daily_exports(*_inputs(spark))["predictions_topk_daily"]

    assert out.columns == ["score_date", "risk_rank", "serial_number", "model", "risk_score",
                           "failed_within_7d", "health_level", "reasons", "model_version"]
    rows = {r["serial_number"]: r for r in out.collect()}
    assert rows["SN_A"]["health_level"] == "CRITICAL" and rows["SN_B"]["health_level"] == "HEALTHY"
    assert rows["SN_NO_HEALTH"]["health_level"] is None  # no HD5 row for that drive: stays null, as in HD8


def test_scored_days_counts_critical_drives_and_those_inside_the_topk(spark):
    out = build_daily_exports(*_inputs(spark))["scored_days"]

    row = [r for r in out.collect() if r["score_date"].isoformat() == "2026-03-12"][0]
    assert row["score_date"].isoformat() == "2026-03-12" and row["split"] == "val"
    assert (row["scored_rows"], row["positives"], row["topk_hits"]) == (3000, 5, 1)
    assert row["critical_total"] == 2  # SN_A and SN_C are CRITICAL that day
    assert row["critical_in_topk"] == 1  # only SN_A is also in the Top-K


def test_history_has_30_days_up_to_the_score_date_for_topk_drives_only(spark):
    out = build_daily_exports(*_inputs(spark))["smart_history_daily_topk"]

    rows = out.collect()
    assert {r["serial_number"] for r in rows} == {"SN_A", "SN_B"}  # not SN_NOT_IN_TOPK
    a_dates = sorted(r["date"].isoformat() for r in rows if r["serial_number"] == "SN_A")
    assert len(a_dates) == 30 and a_dates[-1] == "2026-03-12" and a_dates[0] == "2026-02-11"
    a_187 = [r["smart_187_raw"] for r in rows if r["serial_number"] == "SN_A"]
    assert all(v is None for v in a_187)  # null stays null, never filled with 0


def test_critical_counts_are_null_not_zero_when_the_rules_never_saw_the_day(spark):
    out = build_daily_exports(*_inputs(spark))["scored_days"]

    row = [r for r in out.collect() if r["score_date"].isoformat() == "2026-04-10"][0]
    assert (row["scored_rows"], row["positives"], row["topk_hits"]) == (3100, 6, 2)  # day-level counts stay
    assert row["critical_total"] is None and row["critical_in_topk"] is None  # no HD5 rows that day
