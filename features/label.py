"""Leakage-safe, forward-looking seven-day drive failure label generation."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def add_failure_within_horizon_label(frame: DataFrame, horizon_days: int) -> DataFrame:
    """Label failure strictly after a date and drop censored end-of-data rows."""
    serial_window = Window.partitionBy("serial_number")
    max_date = frame.agg(F.max("date").alias("max_date")).first()["max_date"]
    failure_date = F.min(F.when(F.col("failure") == 1, F.col("date"))).over(serial_window)
    labelled = frame.withColumn("failure_date", failure_date).withColumn("days_until_failure", F.datediff("failure_date", "date"))
    labelled = labelled.withColumn("fail_within_7_days", F.when(F.col("days_until_failure").between(1, horizon_days), F.lit(1)).otherwise(F.lit(0)))
    observed_before_failure = F.col("failure_date").isNull() | (F.col("date") < F.col("failure_date"))
    uncensored = F.col("failure_date").isNotNull() | (F.col("date") <= F.date_sub(F.lit(max_date), horizon_days))
    return labelled.filter(observed_before_failure & uncensored).drop("days_until_failure")
