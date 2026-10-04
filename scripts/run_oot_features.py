"""Gold features for the out-of-time quarter (Q2-2026), split = 'oot'. Only the Q2 date partitions are
written; Q1 Gold features are not touched.

    PYTHONPATH=/opt/smart-drive spark-submit scripts/run_oot_features.py
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from config.settings import load_config
from pipeline.batch_pipeline import build_oot_steps, run_steps, write_run_report


def main() -> int:
    config = load_config()
    spark = SparkSession.builder.appName("smart-oot-features").getOrCreate()
    result = run_steps(build_oot_steps(spark, config))
    print("OOT features result:", result)
    print("Run report:", write_run_report("oot_features_pipeline", result))
    spark.stop()
    return 0 if result["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
