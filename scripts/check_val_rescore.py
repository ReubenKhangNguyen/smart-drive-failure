from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import hdfs_uri, load_config
from ml.evaluate import extract_risk_score, macro_average_at_k, pr_auc, recall_precision_at_k_by_day, roc_auc

REPORTS_DIR = Path(__file__).resolve().parent.parent / "artifacts" / "reports"


def main() -> int:
    """Read-only check: score the VAL split with the saved HD3 model and compare with
    the metrics recorded in Phase 6 (val_metrics.json). Never reads the test split and
    never writes to Gold; the only output is a markdown report."""
    parser = argparse.ArgumentParser(description="Cham lai tap val bang model da luu va so voi val_metrics.json")
    parser.add_argument("--reference", default=str(REPORTS_DIR / "val_metrics.json"))
    args = parser.parse_args()

    config = load_config()
    k = config["ml"]["k"]
    version = config["ml"]["model_version"]
    chosen = config["ml"]["chosen_model"]

    spark = SparkSession.builder.appName("smart-check-val-rescore").getOrCreate()
    model = PipelineModel.load("{}/{}".format(hdfs_uri(config, "models_base"), version))
    val_df = spark.read.parquet(hdfs_uri(config, "features")).where(F.col("split") == "val")

    scored = extract_risk_score(model.transform(val_df))
    now = {"pr_auc": pr_auc(scored), "roc_auc": roc_auc(scored)}
    now.update(macro_average_at_k(recall_precision_at_k_by_day(scored, k=k)))
    now["val_rows"] = val_df.count()

    with open(args.reference, encoding="utf-8") as f:
        ref = json.load(f)["models"][chosen]

    lines = ["# Chấm lại tập val bằng model {} ({})\n".format(version, chosen),
             "Chỉ đọc: tập val, không đụng test, không ghi Gold. K = {}, số dòng val = {:,}.\n".format(k, now["val_rows"]),
             "| Chỉ số | Đã ghi ở Phase 6 | Chấm lại | Chênh lệch |", "|---|---|---|---|"]
    for key in ("pr_auc", "roc_auc", "recall_at_k", "precision_at_k"):
        lines.append("| {} | {:.6f} | {:.6f} | {:+.2e} |".format(key, ref[key], now[key], now[key] - ref[key]))
    report = REPORTS_DIR / "val_rescore_check.md"
    report.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print("Rescore val:", now)
    print("Reference  :", ref)
    print("Report:", report)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
