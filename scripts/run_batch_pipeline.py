from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from config.settings import load_config
from pipeline.batch_pipeline import STEP_NAMES, build_steps, parse_steps, run_steps, select_steps, write_run_report


def main() -> int:
    parser = argparse.ArgumentParser(description="Chay Bronze->Silver->Gold (features+health_status) mot lenh")
    parser.add_argument("--steps", help="cac buoc can chay, cach nhau bang dau phay (mac dinh: tat ca): " + ", ".join(STEP_NAMES))
    args = parser.parse_args()
    try:
        names = parse_steps(args.steps)  # validate before starting Spark
    except ValueError as exc:
        parser.error(str(exc))

    config = load_config()

    spark = SparkSession.builder.appName("smart-batch-pipeline").getOrCreate()
    steps = select_steps(build_steps(spark, config), names)
    result = run_steps(steps)
    print("Batch pipeline result:", result)
    print("Run report:", write_run_report("batch_pipeline", result))
    spark.stop()
    return 0 if result["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
