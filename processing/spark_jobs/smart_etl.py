"""Bronze CSV to Silver Parquet Spark job with an auditable quality log."""

from __future__ import annotations

import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import ProjectPaths
from processing.spark_jobs.smart_cleaning import align_and_cast, quality_summary


def build_spark(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).config("spark.sql.session.timeZone", "UTC").getOrCreate()


def run(input_path: str, output_path: str, quality_path: str) -> dict[str, object]:
    """Read Backblaze CSVs from Bronze, clean them, and overwrite Silver Parquet."""
    started = time.perf_counter()
    spark = build_spark("smart-drive-etl")
    raw = spark.read.option("header", True).option("mode", "PERMISSIVE").csv(input_path)
    cleaned = align_and_cast(raw)
    quality = quality_summary(cleaned)
    valid = cleaned.filter(F.col("date").isNotNull() & F.col("serial_number").isNotNull() & F.col("failure").isin(0, 1)).dropDuplicates(["date", "serial_number"])
    records = valid.count()
    valid.write.mode("overwrite").partitionBy("date").parquet(output_path)
    quality.withColumn("processed_at", F.current_timestamp()).write.mode("overwrite").json(quality_path)
    elapsed = round(time.perf_counter() - started, 2)
    result = {"input": input_path, "output": output_path, "records": records, "runtime_seconds": elapsed}
    print(f"ETL_RESULT {result}", flush=True)
    spark.stop()
    return result


if __name__ == "__main__":
    paths = ProjectPaths()
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=paths.bronze)
    parser.add_argument("--output", default=paths.silver)
    parser.add_argument("--quality-output", default=f"{paths.gold}/quality")
    args = parser.parse_args()
    run(args.input, args.output, args.quality_output)
