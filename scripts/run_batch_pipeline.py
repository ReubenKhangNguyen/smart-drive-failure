from __future__ import annotations

from pyspark.sql import SparkSession

from config.settings import load_config
from pipeline.batch_pipeline import build_steps, run_steps, write_run_report


def main() -> int:
    config = load_config()

    spark = SparkSession.builder.appName("smart-batch-pipeline").getOrCreate()
    steps = build_steps(spark, config)
    result = run_steps(steps)
    print("Batch pipeline result:", result)
    print("Run report:", write_run_report("batch_pipeline", result))
    spark.stop()
    return 0 if result["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
