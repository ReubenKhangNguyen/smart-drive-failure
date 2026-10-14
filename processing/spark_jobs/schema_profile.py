"""Read a Bronze Backblaze CSV with Spark and print its schema and record count."""

from __future__ import annotations

import argparse
import json

from pyspark.sql import SparkSession

def run(input_path: str) -> dict[str, object]:
    """Return a lightweight, evidence-friendly profile before a full ETL run."""
    spark = SparkSession.builder.appName("smart-drive-schema-profile").getOrCreate()
    frame = spark.read.option("header", True).csv(input_path)
    result = {"input": input_path, "records": frame.count(), "columns": frame.columns}
    frame.printSchema()
    print(f"SCHEMA_PROFILE {json.dumps(result)}", flush=True)
    spark.stop()
    return result

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    args = parser.parse_args()
    run(args.input)
