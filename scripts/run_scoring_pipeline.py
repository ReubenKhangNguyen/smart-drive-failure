from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from config.settings import load_config
from pipeline.batch_pipeline import run_steps
from pipeline.scoring_pipeline import build_steps


def main() -> int:
    config = load_config()

    parser = argparse.ArgumentParser(description="Cham diem rui ro hong o cho mot ngay (HD4)")
    parser.add_argument("--date", default="2026-03-24")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("smart-scoring-pipeline").getOrCreate()
    steps = build_steps(spark, config, args.date)
    result = run_steps(steps)
    print("Scoring pipeline result:", result)
    spark.stop()
    return 0 if result["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
