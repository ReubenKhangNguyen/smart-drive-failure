"""Evaluate the FROZEN model on one out-of-time quarter (data.quarters: 'oot' = Q2-2026, 'oot2', ...) and write
artifacts/reports/<quarter report> (oot_metrics.json for Q2, oot_metrics_<id>.json for later quarters). No training,
no tuning. Like the Q1 test, each quarter is evaluated ONCE: if its report already exists the script stops
unless --allow-rerun is given.

    PYTHONPATH=/opt/smart-drive spark-submit scripts/evaluate_oot.py --quarter 2026-Q2
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import find_quarter, hdfs_uri, load_config
from ml.oot_eval import evaluate_frozen_on_oot

REPORT_DIR = Path(__file__).resolve().parent.parent / "artifacts" / "reports"


def main() -> int:
    config = load_config()
    parser = argparse.ArgumentParser(description="Danh gia model dong bang tren mot quy ngoai thoi gian, chay mot lan moi quy")
    parser.add_argument("--quarter", help="id quy trong data.quarters, vd 2026-Q2 (bat buoc khi co nhieu quy)")
    parser.add_argument("--model-version", default=config["ml"]["model_version"])
    parser.add_argument("--k", type=int, default=config["ml"]["k"])
    parser.add_argument("--allow-rerun", action="store_true", help="cho phep ghi de bao cao cua quy da danh gia")
    args = parser.parse_args()

    try:
        quarter = find_quarter(config, args.quarter)
    except ValueError as exc:
        parser.error(str(exc))
    report = REPORT_DIR / quarter["report"]
    if report.exists() and not args.allow_rerun:
        print("DUNG: {} da ton tai. Moi quy chi duoc danh gia mot lan (xem docs/decisions.md); dung --allow-rerun neu that su can.".format(report), file=sys.stderr)
        return 2

    horizon = config["project"]["horizon_days"]
    normal_end = (dt.date.fromisoformat(quarter["end_date"]) - dt.timedelta(days=horizon)).isoformat()

    spark = SparkSession.builder.appName("smart-evaluate-oot").getOrCreate()
    model = PipelineModel.load("{}/{}".format(hdfs_uri(config, "models_base"), args.model_version))
    oot_df = spark.read.parquet(hdfs_uri(config, "features")).where(F.col("split") == quarter["split"])

    result = evaluate_frozen_on_oot(model, oot_df, args.k, normal_end)
    result.update(quarter=quarter["id"], split=quarter["split"], model_version=args.model_version, run_date=dt.date.today().isoformat())
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    print("OOT metrics written to", report)
    print(json.dumps(result["segments"]["normal"], indent=2, default=str))
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
