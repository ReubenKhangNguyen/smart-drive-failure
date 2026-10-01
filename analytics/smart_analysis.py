from __future__ import annotations

from typing import Dict, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def failure_dates(df: DataFrame) -> DataFrame:
    """One row per serial that ever failed: (serial_number, failure_date)."""
    return (
        df.where(F.col("failure") == 1)
        .groupBy("serial_number")
        .agg(F.min("date").alias("failure_date"))
    )


def pre_failure_signal_rate(df: DataFrame, column: str, days_before: int) -> float:
    """Fraction of failed serials whose `column` was > 0 at least once in the
    [failure_date - days_before, failure_date - 1] window (evidence for health_rules.md)."""
    fd = failure_dates(df)
    total_failed = fd.count()
    if total_failed == 0:
        return 0.0

    joined = df.join(fd, "serial_number")
    window_df = joined.where(
        (F.col("date") >= F.date_sub(F.col("failure_date"), days_before))
        & (F.col("date") < F.col("failure_date"))
    )
    per_serial_max = window_df.groupBy("serial_number").agg(F.max(F.col(column)).alias("max_val"))
    with_signal = per_serial_max.where(F.col("max_val") > 0).count()
    return with_signal / total_failed


def healthy_vs_failed_snapshot(df: DataFrame, column: str) -> Dict[str, Dict[str, float]]:
    """Compare `column` between: the last observed day of never-failed serials ("healthy"),
    and the day right before failure of failed serials ("pre-failure")."""
    fd = failure_dates(df)
    failed_serials = fd.select("serial_number")

    healthy_df = df.join(failed_serials, "serial_number", "left_anti")
    healthy_last_day = (
        healthy_df.groupBy("serial_number")
        .agg(F.max("date").alias("date"))
        .join(healthy_df, ["serial_number", "date"])
    )

    pre_failure_day = (
        df.join(fd, "serial_number")
        .where(F.col("date") == F.date_sub(F.col("failure_date"), 1))
    )

    def _stats(sub_df: DataFrame) -> Dict[str, float]:
        row = sub_df.select(
            F.mean(column).alias("mean"),
            F.expr("percentile_approx({}, 0.5)".format(column)).alias("p50"),
            F.expr("percentile_approx({}, 0.99)".format(column)).alias("p99"),
        ).collect()[0]
        return {"mean": row["mean"], "p50": row["p50"], "p99": row["p99"]}

    return {"healthy": _stats(healthy_last_day), "pre_failure": _stats(pre_failure_day)}


def smart_percentiles(df: DataFrame, columns: List[str], percentiles: List[float]) -> Dict[str, List[float]]:
    return {c: df.approxQuantile(c, percentiles, 0.01) for c in columns}
