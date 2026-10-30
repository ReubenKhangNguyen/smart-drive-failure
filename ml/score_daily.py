from __future__ import annotations

import datetime as dt
from typing import Any, Callable, Dict, List, Tuple

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from config.settings import hdfs_uri, load_config, quarters_from_data
from ml.evaluate import extract_risk_score


def scorable_date_ranges(data_cfg: Dict[str, Any], horizon_days: int) -> List[Tuple[str, str]]:
    """Inclusive (first, last) date ranges worth scoring: every day whose full horizon is observed.

    One range for the analysis quarter and one per out-of-time quarter (data.quarters). The last `horizon_days` of
    each quarter are excluded on purpose: Gold features keep only drives already known to fail there
    (right-censoring), so a Top-K for those days would be trivially perfect and meaningless. The first day after
    the warmup is the first day with features.
    """
    gap = dt.timedelta(days=horizon_days)
    one = dt.timedelta(days=1)
    first = dt.date.fromisoformat(data_cfg["warmup_end_date"]) + one
    ranges = [(first.isoformat(), (dt.date.fromisoformat(data_cfg["end_date"]) - gap).isoformat())]
    for quarter in quarters_from_data(data_cfg):
        ranges.append((quarter["start_date"], (dt.date.fromisoformat(quarter["end_date"]) - gap).isoformat()))
    return ranges


def build_daily_scores(
    model: PipelineModel,
    features_df: DataFrame,
    k: int,
    model_version: str,
    ranges: List[Tuple[str, str]],
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """Score every day in `ranges` and keep, per day, the Top-K and a few day-level counts.

    Returns (topk, days, base). `base` is persisted on disk (scalars only) because both outputs
    read it; the caller must `base.unpersist()` once both are written/collected.

    topk: date, risk_rank, serial_number, model, risk_score, failed_within_7d, model_version.
    Ranking is identical to ml.score.score_day: risk_score, then model margin (risk_score saturates
    at 1.0), then serial_number. failed_within_7d is the known outcome, kept only so the dashboard
    can show in hindsight which listed drives did fail; it is never a model input.
    days: date, split, scored_rows, positives, topk_hits, model_version (one row per day).
    """
    condition = None
    for first, last in ranges:
        part = (F.col("date") >= F.lit(first).cast("date")) & (F.col("date") <= F.lit(last).cast("date"))
        condition = part if condition is None else (condition | part)

    scored = extract_risk_score(model.transform(features_df.where(condition)))
    scored = scored.withColumn("_margin", vector_to_array("rawPrediction")[1])
    base = scored.select(
        "date", "serial_number", "model", "split", "fail_within_7_days", "risk_score", "_margin"
    ).persist(StorageLevel.DISK_ONLY)

    window = Window.partitionBy("date").orderBy(F.col("risk_score").desc(), F.col("_margin").desc(), F.col("serial_number"))
    topk = (
        base.withColumn("risk_rank", F.row_number().over(window))
        .where(F.col("risk_rank") <= k)
        .select(
            "date", "risk_rank", "serial_number", "model", "risk_score",
            F.col("fail_within_7_days").cast("int").alias("failed_within_7d"),
            F.lit(model_version).alias("model_version"),
        )
    )
    hits = topk.groupBy("date").agg(F.sum("failed_within_7d").cast("bigint").alias("topk_hits"))
    days = (
        base.groupBy("date")
        .agg(
            F.max("split").alias("split"),
            F.count("*").cast("bigint").alias("scored_rows"),
            F.sum("fail_within_7_days").cast("bigint").alias("positives"),
        )
        .join(hits, "date", "left")
        .withColumn("model_version", F.lit(model_version))
        .select("date", "split", "scored_rows", "positives", "topk_hits", "model_version")
    )
    return topk, days, base


def run(
    spark: SparkSession,
    features_path: str,
    model_path: str,
    topk_path: str,
    days_path: str,
    ranges: List[Tuple[str, str]],
    k: int,
    model_version: str,
    release: Callable[[DataFrame], None] = lambda df: df.unpersist(),
) -> Dict[str, Any]:
    model = PipelineModel.load(model_path)
    features_df = spark.read.parquet(features_path)
    topk, days, base = build_daily_scores(model, features_df, k, model_version, ranges)
    try:
        # Small tables recomputed in full on every run, so a plain overwrite of their own directory is safe.
        topk.coalesce(1).write.mode("overwrite").parquet(topk_path)
        days.coalesce(1).write.mode("overwrite").parquet(days_path)
    finally:
        release(base)
    written_days = spark.read.parquet(days_path)
    return {
        "ranges": ranges,
        "days": written_days.count(),
        "topk_rows": spark.read.parquet(topk_path).count(),
        "k": k,
        "model_version": model_version,
    }


def main() -> int:
    import argparse

    config = load_config()
    parser = argparse.ArgumentParser(description="Cham diem Top-K cho moi ngay co du 7 ngay tuong lai (cho o chon ngay cua dashboard)")
    parser.add_argument("--model-version", default=config["ml"]["model_version"])
    parser.add_argument("--k", type=int, default=config["ml"]["k"])
    args = parser.parse_args()

    ranges = scorable_date_ranges(config["data"], config["project"]["horizon_days"])
    spark = SparkSession.builder.appName("smart-score-daily").getOrCreate()
    stats = run(
        spark,
        hdfs_uri(config, "features"),
        "{}/{}".format(hdfs_uri(config, "models_base"), args.model_version),
        hdfs_uri(config, "predictions_daily_topk"),
        hdfs_uri(config, "scored_days"),
        ranges,
        args.k,
        args.model_version,
    )
    print("Daily scoring stats:", stats)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
