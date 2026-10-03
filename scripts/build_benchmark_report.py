from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Dict

from pyspark.sql import SparkSession

from analytics import benchmark as bm


def _load_json(path: Path, fallback: Dict[str, str]) -> Dict[str, str]:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else fallback


def main() -> int:
    parser = argparse.ArgumentParser(description="Gop ket qua tho cua benchmark thanh benchmark.md va hai bang HD9")
    parser.add_argument("--raw-dir", default=str(bm.DEFAULT_RAW_DIR))
    parser.add_argument("--reports-dir", default=str(bm.DEFAULT_REPORTS_DIR))
    parser.add_argument("--export-dir", default=str(bm.DEFAULT_EXPORT_DIR))
    args = parser.parse_args()

    raw_dir = Path(args.raw_dir)
    host = _load_json(raw_dir / "environment_host.json", {"host": "chưa thu thập (chạy scripts/benchmark_env.py trên máy host)"})
    spark_env = _load_json(raw_dir / "environment_spark.json", {"spark": "chưa có (chạy scripts/run_benchmark.py)"})
    records = bm.load_raw_records(raw_dir)
    run_date = dt.date.today().isoformat()
    environment = bm.environment_items(host, spark_env, records, run_date)

    spark = SparkSession.builder.appName("smart-benchmark-report").getOrCreate()
    try:
        stats = bm.build_tables(spark, raw_dir, Path(args.reports_dir), Path(args.export_dir), environment, run_date)
    finally:
        spark.stop()
    print("Benchmark report stats:", stats)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
