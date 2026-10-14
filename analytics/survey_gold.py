"""Create descriptive Gold analytics from Silver Parquet for the project progress report.

This job intentionally performs no prediction, label creation, or model training.
It answers whether the current Backblaze subset is usable and which observed
failure and SMART patterns deserve later feature-engineering work.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from datetime import date, datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


SMART_COLUMNS = ["smart_5_raw", "smart_187_raw", "smart_197_raw", "smart_198_raw", "smart_199_raw"]


def json_value(value: object) -> object:
    """Convert Spark scalar values to values that can be stored in report JSON."""
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def write_output(frame: DataFrame, hdfs_path: str, report_path: Path) -> int:
    """Write the distributed Parquet result and a small human-readable JSON copy."""
    frame.write.mode("overwrite").parquet(hdfs_path)
    rows = [{key: json_value(value) for key, value in row.asDict(recursive=True).items()} for row in frame.collect()]
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    return len(rows)


def run(silver_path: str, gold_path: str, report_dir: str) -> dict[str, object]:
    """Profile Silver data and write five report-ready Gold analytics tables."""
    started = time.perf_counter()
    spark = SparkSession.builder.appName("smart-drive-data-survey").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    frame = spark.read.parquet(silver_path).cache()
    available_smart = [column for column in SMART_COLUMNS if column in frame.columns]
    reports = Path(report_dir)

    overview = frame.agg(
        F.count("*").alias("observations"),
        F.countDistinct("date").alias("days"),
        F.countDistinct("serial_number").alias("drives"),
        F.countDistinct("model").alias("models"),
        F.countDistinct("datacenter").alias("datacenters"),
        F.sum("failure").alias("recorded_failures"),
        F.min("date").alias("start_date"),
        F.max("date").alias("end_date"),
    )
    quality = frame.agg(
        F.sum(F.when(F.col("date").isNull(), 1).otherwise(0)).alias("null_date_rows"),
        F.sum(F.when(F.col("serial_number").isNull() | (F.length("serial_number") == 0), 1).otherwise(0)).alias("null_serial_rows"),
        F.sum(F.when(F.col("model").isNull() | (F.length("model") == 0), 1).otherwise(0)).alias("null_model_rows"),
        F.sum(F.when(~F.col("failure").isin(0, 1) | F.col("failure").isNull(), 1).otherwise(0)).alias("invalid_failure_rows"),
    )
    duplicate_rows = frame.groupBy("date", "serial_number").count().filter("count > 1").agg(F.coalesce(F.sum("count"), F.lit(0)).cast("long").alias("duplicate_key_rows"))
    quality = quality.crossJoin(duplicate_rows)
    failure_by_date = frame.groupBy("date").agg(F.count("*").alias("observations"), F.countDistinct("serial_number").alias("drives"), F.sum("failure").alias("failures")).withColumn("failure_rate", F.col("failures") / F.col("observations")).orderBy("date")
    failure_by_model = frame.groupBy("model").agg(F.count("*").alias("observations"), F.countDistinct("serial_number").alias("drives"), F.sum("failure").alias("failures")).withColumn("failure_rate", F.col("failures") / F.col("observations")).orderBy(F.desc("failures"), F.desc("failure_rate"), "model")
    smart_comparison = frame.groupBy("failure").agg(F.count("*").alias("observations"), *[F.avg(column).alias(f"avg_{column}") for column in available_smart]).orderBy("failure")

    paths = {
        "dataset_overview": overview,
        "data_quality": quality,
        "failure_by_date": failure_by_date,
        "failure_by_model": failure_by_model,
        "smart_comparison": smart_comparison,
    }
    output_rows = {name: write_output(table, f"{gold_path}/{name}", reports / f"{name}.json") for name, table in paths.items()}
    frame.unpersist()
    result = {"silver": silver_path, "gold": gold_path, "smart_columns": available_smart, "outputs": output_rows, "runtime_seconds": round(time.perf_counter() - started, 2)}
    print(f"SURVEY_RESULT {json.dumps(result)}", flush=True)
    spark.stop()
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver", default="hdfs://namenode:9000/smart-drive/silver/daily")
    parser.add_argument("--gold", default="hdfs://namenode:9000/smart-drive/gold/analytics")
    parser.add_argument("--report-dir", default="artifacts/reports/data_survey")
    args = parser.parse_args()
    run(args.silver, args.gold, args.report_dir)
