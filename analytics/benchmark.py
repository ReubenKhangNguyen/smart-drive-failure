"""Benchmark CSV versus Parquet aggregation and export repeatable timing evidence."""

from __future__ import annotations

import argparse
import json
import statistics
import time
from pathlib import Path

from pyspark.sql import SparkSession

def elapsed(action) -> float:
    """Measure one materialized Spark aggregation."""
    started = time.perf_counter()
    action()
    return round(time.perf_counter() - started, 3)

def run(csv_path: str, parquet_path: str, dashboard_dir: str, repetitions: int = 3) -> list[dict[str, object]]:
    """Run an identical model aggregation three times and retain medians."""
    spark = SparkSession.builder.appName("smart-drive-benchmark").getOrCreate()
    workloads = {"csv": lambda: spark.read.option("header", True).csv(csv_path).groupBy("model").count().collect(), "parquet": lambda: spark.read.parquet(parquet_path).groupBy("model").count().collect()}
    report = []
    for storage, action in workloads.items():
        values = [elapsed(action) for _ in range(repetitions)]
        report.append({"storage": storage, "runs_seconds": values, "median_seconds": statistics.median(values), "workload": "group by drive model"})
    destination = Path(dashboard_dir) / "benchmark.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(report, indent=2), encoding="utf-8")
    spark.stop()
    return report

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True)
    parser.add_argument("--parquet", required=True)
    parser.add_argument("--dashboard-dir", default="artifacts/dashboard")
    args = parser.parse_args()
    print(json.dumps(run(args.csv, args.parquet, args.dashboard_dir), indent=2))
