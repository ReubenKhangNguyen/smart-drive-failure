from __future__ import annotations

import argparse
from typing import Any, Dict

from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from config.settings import enable_dynamic_overwrite, hdfs_uri, load_config
from ml.evaluate import extract_risk_score


def score_day(model: PipelineModel, features_df: DataFrame, score_date: str, k: int) -> DataFrame:
    """Score one day of Gold features -> HD4 schema (date, serial_number, model,
    risk_score, risk_rank, alert, model_version). risk_rank/alert are computed
    per-day, independent of any other day."""
    day_df = features_df.where(F.col("date") == score_date)
    predictions = extract_risk_score(model.transform(day_df))

    window = Window.orderBy(F.col("risk_score").desc(), F.col("serial_number"))
    ranked = predictions.withColumn("risk_rank", F.row_number().over(window))
    ranked = ranked.withColumn("alert", F.col("risk_rank") <= k)

    return ranked.select("date", "serial_number", "model", "risk_score", "risk_rank", "alert")


def run(
    spark: SparkSession,
    features_path: str,
    model_path: str,
    predictions_path: str,
    score_date: str,
    k: int,
    model_version: str,
) -> Dict[str, Any]:
    enable_dynamic_overwrite(spark)
    model = PipelineModel.load(model_path)
    features_df = spark.read.parquet(features_path)

    scored = score_day(model, features_df, score_date, k)
    scored = scored.withColumn("model_version", F.lit(model_version))

    scored.write.mode("overwrite").partitionBy("date").parquet(predictions_path)

    return {
        "score_date": score_date,
        "rows_scored": scored.count(),
        "alerts": scored.where(F.col("alert")).count(),
        "model_version": model_version,
    }


def main() -> int:
    config = load_config()

    parser = argparse.ArgumentParser(description="Cham diem rui ro hong o (HD4) tu model da luu (HD3)")
    parser.add_argument("--date", default="2026-03-24", help="Ngay chay diem (mac dinh 2026-03-24: ngay cuoi dataset 2026-03-31 chi con lai o da xac nhan hong, khong dai dien)")
    parser.add_argument("--features-path", default=hdfs_uri(config, "features"))
    parser.add_argument("--models-base", default=hdfs_uri(config, "models_base"))
    parser.add_argument("--model-version", default=config["ml"]["model_version"])
    parser.add_argument("--predictions-path", default=hdfs_uri(config, "predictions"))
    parser.add_argument("--k", type=int, default=config["ml"]["k"])
    args = parser.parse_args()

    model_path = "{}/{}".format(args.models_base, args.model_version)

    spark = SparkSession.builder.appName("smart-score").getOrCreate()
    stats = run(spark, args.features_path, model_path, args.predictions_path, args.date, args.k, args.model_version)
    print("Scoring stats:", stats)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
