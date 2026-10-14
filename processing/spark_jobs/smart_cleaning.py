"""Reusable schema alignment and data-quality functions for Backblaze CSV data."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


IDENTIFIER_COLUMNS = {"date", "serial_number", "model", "capacity_bytes", "failure"}


def normalize_column_names(frame: DataFrame) -> DataFrame:
    """Lowercase and normalize CSV headers to stable Spark-friendly names."""
    for column in frame.columns:
        normalized = column.strip().lower().replace(" ", "_").replace("-", "_")
        if normalized != column:
            frame = frame.withColumnRenamed(column, normalized)
    return frame


def align_and_cast(frame: DataFrame) -> DataFrame:
    """Cast mandatory fields and all SMART fields without a fixed release schema."""
    frame = normalize_column_names(frame)
    for required in IDENTIFIER_COLUMNS:
        if required not in frame.columns:
            frame = frame.withColumn(required, F.lit(None))
    frame = (frame.withColumn("date", F.to_date("date")).withColumn("serial_number", F.trim(F.col("serial_number"))).withColumn("model", F.trim(F.col("model"))).withColumn("capacity_bytes", F.col("capacity_bytes").cast("long")).withColumn("failure", F.col("failure").cast("int")))
    for column in frame.columns:
        if column.startswith("smart_"):
            frame = frame.withColumn(column, F.col(column).cast("double"))
    return frame


def quality_summary(frame: DataFrame) -> DataFrame:
    """Return null-key, invalid-failure and duplicate indicators in one row."""
    duplicate_rows = frame.groupBy("date", "serial_number").count().filter("count > 1").agg(F.sum("count").alias("duplicate_rows"))
    base = frame.agg(F.count("*").alias("input_rows"), F.sum(F.when(F.col("date").isNull() | F.col("serial_number").isNull() | (F.length("serial_number") == 0), 1).otherwise(0)).alias("invalid_key_rows"), F.sum(F.when(~F.col("failure").isin(0, 1) | F.col("failure").isNull(), 1).otherwise(0)).alias("invalid_failure_rows"))
    return base.crossJoin(duplicate_rows)
