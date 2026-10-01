from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def duplicate_key_count(df: DataFrame) -> int:
    """Rows sharing the same (serial_number, date) key, counted before de-duplication."""
    return df.groupBy("serial_number", "date").count().where(F.col("count") > 1).count()


def serials_reappearing_after_failure(df: DataFrame) -> int:
    """Serials with a row dated after their own first failure=1 row.

    Phase 5 labeling drops these rows; this only reports how many exist so
    the decision is visible in the data-quality report.
    """
    failure_dates = (
        df.where(F.col("failure") == 1)
        .groupBy("serial_number")
        .agg(F.min("date").alias("failure_date"))
    )
    joined = df.join(failure_dates, on="serial_number", how="inner")
    return joined.where(F.col("date") > F.col("failure_date")).select("serial_number").distinct().count()


def implausible_value_count(df: DataFrame, column: str, min_value: float = 0.0) -> int:
    """Rows where `column` is non-null and below `min_value` (e.g. negative SMART raw counters)."""
    return df.where(F.col(column).isNotNull() & (F.col(column) < min_value)).count()


def missing_dates_per_serial(df: DataFrame, expected_day_count: int) -> int:
    """Serials with fewer distinct dates than expected_day_count (gaps in daily reporting)."""
    return (
        df.groupBy("serial_number")
        .agg(F.countDistinct("date").alias("day_count"))
        .where(F.col("day_count") < expected_day_count)
        .count()
    )
