from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from config.settings import hdfs_uri, load_config
from ml.evaluate import extract_risk_score, macro_average_at_k, recall_precision_at_k_by_day

REPORTS_DIR = Path(__file__).resolve().parent.parent / "artifacts" / "reports"


def tie_stats_by_day(scored: DataFrame, k: int) -> DataFrame:
    """Per day: how many drives share the risk_score of the K-th drive (exact equality), and how many of
    them fall outside Top-K, i.e. are excluded only by the tie-break."""
    window = Window.partitionBy("date").orderBy(F.col("risk_score").desc(), F.col("_margin").desc(), F.col("serial_number"))
    ranked = scored.withColumn("_rank", F.row_number().over(window))
    cutoff = ranked.where(F.col("_rank") == k).select("date", F.col("risk_score").alias("cutoff_score"))
    tied = ranked.join(cutoff, "date").where(F.col("risk_score") == F.col("cutoff_score"))
    return tied.groupBy("date").agg(
        F.count("*").alias("tied_at_cutoff"),
        F.sum(F.when(F.col("_rank") > k, 1).otherwise(0)).alias("excluded_by_tiebreak"),
        F.max("cutoff_score").alias("cutoff_score"),
    ).orderBy("date")


def margin_ties_at_cutoff(scored: DataFrame, k: int) -> int:
    """Number of drives whose (risk_score, margin) equals that of the K-th drive: ties the margin cannot break."""
    window = Window.orderBy(F.col("risk_score").desc(), F.col("_margin").desc(), F.col("serial_number"))
    ranked = scored.withColumn("_rank", F.row_number().over(window))
    cut = ranked.where(F.col("_rank") == k).select("risk_score", "_margin").collect()[0]
    return ranked.where((F.col("risk_score") == cut["risk_score"]) & (F.col("_margin") == cut["_margin"])).count()


def main() -> int:
    """Read-only sensitivity check on the VAL split only (never reads test, never writes Gold): recall@K and
    precision@K of the official LR with ties broken by serial_number (the way Phase 6 published) versus by
    model margin; plus how many drives are tied at the Top-K cut-off. Also reports the tie situation of the
    scored day. Used as a sensitivity analysis, NOT to re-select the model."""
    parser = argparse.ArgumentParser(description="Do do nhay cua recall@K tren val theo cach chia hoa")
    parser.add_argument("--reference", default=str(REPORTS_DIR / "val_metrics.json"))
    parser.add_argument("--scored-date", default="2026-03-24")
    args = parser.parse_args()

    config = load_config()
    k, version, chosen = config["ml"]["k"], config["ml"]["model_version"], config["ml"]["chosen_model"]

    spark = SparkSession.builder.appName("smart-check-val-tiebreak").getOrCreate()
    model = PipelineModel.load("{}/{}".format(hdfs_uri(config, "models_base"), version))
    features = spark.read.parquet(hdfs_uri(config, "features"))

    def scored_of(df: DataFrame) -> DataFrame:
        # keep only the columns needed, before caching: val has ~3.8M rows x 95 feature columns
        scored = extract_risk_score(model.transform(df)).withColumn("_margin", vector_to_array("rawPrediction")[1])
        return scored.select("date", "serial_number", "fail_within_7_days", "risk_score", "_margin").cache()

    val = scored_of(features.where(F.col("split") == "val"))
    by_serial = macro_average_at_k(recall_precision_at_k_by_day(val, k=k))
    with_margin = val.withColumn("_tie_key", F.struct((-F.col("_margin")).alias("neg_margin"), F.col("serial_number")))
    by_margin = macro_average_at_k(recall_precision_at_k_by_day(with_margin, k=k, tie_break_col="_tie_key"))
    ties = tie_stats_by_day(val, k).collect()

    with open(args.reference, encoding="utf-8") as f:
        ref = json.load(f)["models"][chosen]

    scored_day = scored_of(features.where(F.col("date") == args.scored_date))
    day_total = scored_day.count()
    day_exactly_one = scored_day.where(F.col("risk_score") == 1.0).count()
    day_ties = tie_stats_by_day(scored_day, k).collect()
    day_margin_ties = margin_ties_at_cutoff(scored_day, k)

    result: Dict[str, Any] = {
        "recall_serial": by_serial["recall_at_k"], "precision_serial": by_serial["precision_at_k"],
        "recall_margin": by_margin["recall_at_k"], "precision_margin": by_margin["precision_at_k"],
        "ref_recall": ref["recall_at_k"], "ref_precision": ref["precision_at_k"],
        "val_days": len(ties), "val_days_with_excluded_ties": sum(1 for r in ties if r["excluded_by_tiebreak"] > 0),
        "val_excluded_total": int(sum(r["excluded_by_tiebreak"] for r in ties)),
        "val_tied_at_cutoff_max": int(max((r["tied_at_cutoff"] for r in ties), default=0)),
        "scored_date": args.scored_date, "scored_rows": day_total, "scored_exactly_one": day_exactly_one,
        "scored_tied_at_cutoff": int(day_ties[0]["tied_at_cutoff"]) if day_ties else 0,
        "scored_excluded_by_tiebreak": int(day_ties[0]["excluded_by_tiebreak"]) if day_ties else 0,
        "scored_margin_ties_at_cutoff": day_margin_ties,
    }

    lines = [
        "# Độ nhạy của recall@{k} trên val theo cách chia hòa — model {v} ({m})\n".format(k=k, v=version, m=chosen),
        "Chỉ đọc, chỉ tập val, không đụng test, không ghi Gold. Đây là phân tích độ nhạy, không dùng để chọn lại model.\n",
        "| Cách chia hòa | Recall@{k} | Precision@{k} |".format(k=k), "|---|---|---|",
        "| serial_number (đã công bố ở Phase 6) | {:.4%} | {:.4%} |".format(ref["recall_at_k"], ref["precision_at_k"]),
        "| serial_number (tính lại) | {:.4%} | {:.4%} |".format(result["recall_serial"], result["precision_serial"]),
        "| margin giảm dần, rồi serial | {:.4%} | {:.4%} |\n".format(result["recall_margin"], result["precision_margin"]),
        "- Số ngày val: {}; số ngày có ổ bị loại khỏi Top-{} chỉ vì chia hòa: {}; tổng số ổ bị loại như vậy: {}; số ổ hòa điểm lớn nhất tại ranh giới Top-{}: {}.".format(
            result["val_days"], k, result["val_days_with_excluded_ties"], result["val_excluded_total"], k, result["val_tied_at_cutoff_max"]),
        "- Ngày chấm điểm {}: {:,} ổ, {} ổ có điểm đúng 1.0; {} ổ hòa điểm tại ranh giới Top-{}, trong đó {} ổ bị loại chỉ vì chia hòa; {} ổ trùng cả điểm lẫn margin tại ranh giới.".format(
            args.scored_date, day_total, day_exactly_one, result["scored_tied_at_cutoff"], k,
            result["scored_excluded_by_tiebreak"], day_margin_ties),
    ]
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    (REPORTS_DIR / "val_tiebreak_sensitivity.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("TIEBREAK", json.dumps(result, default=str))
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
