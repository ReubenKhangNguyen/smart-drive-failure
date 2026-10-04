from __future__ import annotations

from typing import Any, Dict, List, Optional

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from analytics.health_status import rule_risk_score
from ml.evaluate import LABEL_COL, extract_risk_score, macro_average_at_k, pr_auc, recall_precision_at_k_by_day, roc_auc


def _segment_filters(normal_end_date: str) -> Dict[str, Optional[Any]]:
    end = F.lit(normal_end_date).cast("date")
    return {"normal": F.col("date") <= end, "tail": F.col("date") > end, "full": None}


def _daily_rows(daily: DataFrame) -> List[Dict[str, Any]]:
    return [r.asDict() for r in daily.orderBy("date").collect()]


def _monthly(daily_model: List[Dict[str, Any]], daily_rules: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Macro-average per calendar month (mean of the per-day values), as the headline number is."""
    rules_by_date = {r["date"]: r for r in daily_rules}
    months: Dict[str, Dict[str, List[float]]] = {}
    for row in daily_model:
        bucket = months.setdefault(row["date"].strftime("%Y-%m"), {"lr_r": [], "lr_p": [], "ru_r": [], "ru_p": [], "pos": []})
        if row["recall_at_k"] is not None:
            bucket["lr_r"].append(row["recall_at_k"])
            bucket["ru_r"].append(rules_by_date[row["date"]]["recall_at_k"])
        bucket["lr_p"].append(row["precision_at_k"])
        bucket["ru_p"].append(rules_by_date[row["date"]]["precision_at_k"])
        bucket["pos"].append(row["total_positives"])

    def mean(values: List[float]) -> Optional[float]:
        return sum(values) / len(values) if values else None

    return [
        {
            "month": month,
            "days": len(b["lr_p"]),
            "positives": int(sum(b["pos"])),
            "logistic_regression": {"recall_at_k": mean(b["lr_r"]), "precision_at_k": mean(b["lr_p"])},
            "baseline_rules_v1": {"recall_at_k": mean(b["ru_r"]), "precision_at_k": mean(b["ru_p"])},
        }
        for month, b in sorted(months.items())
    ]


def evaluate_frozen_on_oot(model: PipelineModel, oot_df: DataFrame, k: int, normal_end_date: str) -> Dict[str, Any]:
    """Score the out-of-time quarter with the FROZEN model (no fit, no tuning) and compare with rules_v1.

    Segments mirror the Q1 test breakdown: 'normal' (up to normal_end_date = last day with a full
    7-day horizon), 'tail' (the censored last days: only drives known to fail remain, so every row is
    positive and top-K is trivially 100% for ANY method) and 'full'. The headline number is 'normal';
    recall/precision@K for 'full' is reported but must not be quoted alone.
    """
    scored = extract_risk_score(model.transform(oot_df)).withColumn("_margin", vector_to_array("rawPrediction")[1])
    scored = rule_risk_score(scored).select(
        "date", "serial_number", LABEL_COL, "risk_score", "_margin", "rule_risk_score"
    ).persist(StorageLevel.DISK_ONLY)  # scalars only, on disk: 1 GB executors ran out of heap caching the vectors

    try:
        segments: Dict[str, Any] = {}
        daily_normal: Dict[str, List[Dict[str, Any]]] = {}
        for name, condition in _segment_filters(normal_end_date).items():
            part = scored if condition is None else scored.where(condition)
            counts = part.agg(F.count("*").alias("rows"), F.sum(LABEL_COL).alias("positives")).collect()[0]
            rows, positives = counts["rows"], int(counts["positives"] or 0)
            entry: Dict[str, Any] = {
                "rows": rows,
                "positives": positives,
                "positive_rate": (positives / rows) if rows else None,
            }

            lr_daily = recall_precision_at_k_by_day(part, k=k)
            lr_metrics = macro_average_at_k(lr_daily)
            rules_daily = recall_precision_at_k_by_day(part, k=k, score_col="rule_risk_score")
            rules_metrics = macro_average_at_k(rules_daily)

            # AUC needs both classes: the tail has only positives, so it is undefined there.
            has_both = 0 < positives < rows
            lr_entry = {
                "pr_auc": pr_auc(part, score_col="risk_score") if has_both else None,
                "roc_auc": roc_auc(part, score_col="risk_score") if has_both else None,
                "recall_at_k": lr_metrics["recall_at_k"],
                "precision_at_k": lr_metrics["precision_at_k"],
            }
            # risk_score saturates at 1.0, so ties at the top-K boundary matter: also report ties broken by model margin.
            by_margin = macro_average_at_k(
                recall_precision_at_k_by_day(part.withColumn("_tie_key", -F.col("_margin")), k=k, tie_break_col="_tie_key")
            )
            lr_entry["recall_at_k_margin_tiebreak"] = by_margin["recall_at_k"]
            lr_entry["precision_at_k_margin_tiebreak"] = by_margin["precision_at_k"]

            entry["logistic_regression"] = lr_entry
            entry["baseline_rules_v1"] = {
                "recall_at_k": rules_metrics["recall_at_k"],
                "precision_at_k": rules_metrics["precision_at_k"],
            }
            segments[name] = entry
            if name == "normal":
                daily_normal = {"lr": _daily_rows(lr_daily), "rules": _daily_rows(rules_daily)}

        return {
            "k": k,
            "normal_end_date": normal_end_date,
            "segments": segments,
            "monthly_normal": _monthly(daily_normal["lr"], daily_normal["rules"]),
        }
    finally:
        scored.unpersist()
