from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from analytics.smart_analysis import failure_dates


def label_drive_days(df: DataFrame, dataset_end_date: str, horizon_days: int = 7) -> DataFrame:
    """Add label_status and fail_within_7_days to every Silver drive-day row.

    label_status:
    - POST_FAILURE: date is after this serial's own failure date (should not normally
      occur in Bronze/Silver, dropped defensively).
    - CENSORED: the (date, date+horizon_days] window extends past the dataset's last
      observed date, so the future outcome is unknown (right-censoring).
    - DISAPPEARED: the serial has no failure=1 anywhere, and stops reporting before
      this row's window closes, even though the dataset itself continues — left the
      fleet for an unknown reason, not a confirmed failure.
    - LABELED: fail_within_7_days is well-defined and safe to use for train/val/test.
    """
    fd = failure_dates(df)
    last_seen = df.groupBy("serial_number").agg(F.max("date").alias("last_seen_date"))

    joined = df.join(fd, "serial_number", "left").join(last_seen, "serial_number", "left")

    horizon_end = F.date_add(F.col("date"), horizon_days)
    end_date_lit = F.lit(dataset_end_date).cast("date")

    status = (
        F.when(F.col("failure_date").isNotNull() & (F.col("date") > F.col("failure_date")), F.lit("POST_FAILURE"))
        .when(F.col("failure_date").isNotNull(), F.lit("LABELED"))
        .when(horizon_end > end_date_lit, F.lit("CENSORED"))
        .when(F.col("last_seen_date") < horizon_end, F.lit("DISAPPEARED"))
        .otherwise(F.lit("LABELED"))
    )

    label = F.when(
        F.col("failure_date").isNotNull() & (F.col("failure_date") <= horizon_end),
        1,
    ).otherwise(0)

    result = joined.withColumn("label_status", status).withColumn("fail_within_7_days", label)
    return result.drop("failure_date", "last_seen_date")


def build_labeled_dataset(df: DataFrame, dataset_end_date: str, horizon_days: int = 7) -> DataFrame:
    """Only rows with a well-defined label (drops POST_FAILURE/CENSORED/DISAPPEARED)."""
    labeled = label_drive_days(df, dataset_end_date, horizon_days)
    return labeled.where(F.col("label_status") == "LABELED").drop("label_status")


def assign_split(df: DataFrame, warmup_end_date: str, train_end_date: str, val_end_date: str) -> DataFrame:
    """Drop the warmup period, then split chronologically: train < val < test by date.

    Chronological (not random) split so no row in val/test is ever from an earlier
    calendar date than a row in train — required by .claude/rules/ml-leakage.md.
    """
    usable = df.where(F.col("date") > F.lit(warmup_end_date).cast("date"))
    return usable.withColumn(
        "split",
        F.when(F.col("date") <= F.lit(train_end_date).cast("date"), F.lit("train"))
        .when(F.col("date") <= F.lit(val_end_date).cast("date"), F.lit("val"))
        .otherwise(F.lit("test")),
    )
