"""Gold features for one out-of-time quarter (split 'oot', 'oot2', ... per data.quarters). Only that quarter's date
partitions are written; earlier Gold features are not touched.

    PYTHONPATH=/opt/smart-drive spark-submit scripts/run_oot_features.py --quarter 2026-Q2
    ... --quarter 2026-Q3 --dry-run     # checks Silver and the target partitions, writes nothing
"""
from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession

from config.settings import find_quarter, load_config
from pipeline.batch_pipeline import build_quarter_steps, run_steps, write_run_report


def main() -> int:
    parser = argparse.ArgumentParser(description="Tinh dac trung Gold cho mot quy ngoai thoi gian (data.quarters)")
    parser.add_argument("--quarter", help="id quy trong data.quarters, vd 2026-Q2 (bat buoc khi co nhieu quy)")
    parser.add_argument("--dry-run", action="store_true", help="chi kiem tra Silver va phan vung dich, khong ghi")
    args = parser.parse_args()

    config = load_config()
    try:
        find_quarter(config, args.quarter)  # fail fast, before Spark starts
    except ValueError as exc:
        parser.error(str(exc))

    spark = SparkSession.builder.appName("smart-quarter-features").getOrCreate()
    result = run_steps(build_quarter_steps(spark, config, args.quarter, args.dry_run))
    print("Quarter features result:", result)
    if not args.dry_run:
        print("Run report:", write_run_report("quarter_features_pipeline", result))
    spark.stop()
    return 0 if result["status"] == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
