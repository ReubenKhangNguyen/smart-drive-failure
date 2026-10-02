from __future__ import annotations

from pyspark.sql import SparkSession

from config.settings import load_config
from pipeline.batch_pipeline import run_steps, write_run_report
from pipeline.training_pipeline import build_steps


def main() -> int:
    config = load_config()
    spark = SparkSession.builder.appName("smart-training-pipeline").getOrCreate()
    steps = build_steps(spark, config)
    result = run_steps(steps)
    print("Training pipeline result (train+val only, test never re-run):", result)
    print("Run report:", write_run_report("training_pipeline", result))
    spark.stop()
    return 0 if result["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
