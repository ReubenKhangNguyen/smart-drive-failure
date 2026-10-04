"""Evaluate the FROZEN model on the out-of-time quarter (split = 'oot', Q2-2026) and write
artifacts/reports/oot_metrics.json. No training, no tuning. Like the Q1 test, it runs ONCE:
if the report already exists the script stops unless --allow-rerun is given.

    PYTHONPATH=/opt/smart-drive spark-submit scripts/evaluate_oot.py
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

from config.settings import hdfs_uri, load_config
from ml.oot_eval import evaluate_frozen_on_oot

REPORT = Path(__file__).resolve().parent.parent / "artifacts" / "reports" / "oot_metrics.json"


def main() -> int:
    config = load_config()
    parser = argparse.ArgumentParser(description="Danh gia model dong bang tren quy Q2 (split=oot), chay mot lan")
    parser.add_argument("--model-version", default=config["ml"]["model_version"])
    parser.add_argument("--k", type=int, default=config["ml"]["k"])
    parser.add_argument("--allow-rerun", action="store_true", help="cho phep ghi de oot_metrics.json da co")
    args = parser.parse_args()

    if REPORT.exists() and not args.allow_rerun:
        print("DUNG: {} da ton tai. Danh gia oot chi chay mot lan (xem docs/decisions.md); dung --allow-rerun neu that su can.".format(REPORT), file=sys.stderr)
        return 2

    data_cfg = config["data"]
    horizon = config["project"]["horizon_days"]
    normal_end = (dt.date.fromisoformat(data_cfg["oot_end_date"]) - dt.timedelta(days=horizon)).isoformat()

    spark = SparkSession.builder.appName("smart-evaluate-oot").getOrCreate()
    model = PipelineModel.load("{}/{}".format(hdfs_uri(config, "models_base"), args.model_version))
    oot_df = spark.read.parquet(hdfs_uri(config, "features")).where(F.col("split") == "oot")

    result = evaluate_frozen_on_oot(model, oot_df, args.k, normal_end)
    result["model_version"] = args.model_version
    result["run_date"] = dt.date.today().isoformat()
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    print("OOT metrics written to", REPORT)
    print(json.dumps(result["segments"]["normal"], indent=2, default=str))
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
