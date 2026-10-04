"""Read-only check: recompute the features of one out-of-time quarter with the current code and compare them
with the Gold partitions already written (row count, positive labels, checksum of every column). Writes nothing.
Exit 0 when identical, 1 when they differ.

    PYTHONPATH=/opt/smart-drive spark-submit scripts/check_quarter_reproduction.py --quarter 2026-Q2
"""
from __future__ import annotations

import argparse
import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import find_quarter, hdfs_uri, load_config
from features.oot import build_quarter_features, quarter_fingerprint


def main() -> int:
    parser = argparse.ArgumentParser(description="Tinh lai dac trung cua mot quy va doi chieu voi Gold da ghi (chi doc)")
    parser.add_argument("--quarter", help="id quy trong data.quarters (bat buoc khi co nhieu quy)")
    args = parser.parse_args()

    config = load_config()
    try:
        quarter = find_quarter(config, args.quarter)
    except ValueError as exc:
        parser.error(str(exc))

    spark = SparkSession.builder.appName("smart-check-quarter-reproduction").getOrCreate()
    silver = spark.read.parquet(hdfs_uri(config, "silver"))
    stored = spark.read.parquet(hdfs_uri(config, "features")).where(F.col("split") == quarter["split"])
    recomputed = build_quarter_features(silver, config["data"], quarter, config["project"]["horizon_days"])

    old, new = quarter_fingerprint(stored), quarter_fingerprint(recomputed)
    print("stored    :", json.dumps(old))
    print("recomputed:", json.dumps(new))
    same = old == new
    print("RESULT:", "IDENTICAL" if same else "DIFFERENT")
    spark.stop()
    return 0 if same else 1


if __name__ == "__main__":
    sys.exit(main())
