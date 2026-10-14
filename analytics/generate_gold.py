"""Generate Gold tables and compact JSON exports from Silver Parquet with Spark."""

from __future__ import annotations

import argparse
import json
import math
import time
from datetime import date, datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from config.settings import FAILURE_HORIZON_DAYS, ProjectPaths
from features.build_features import build_feature_table, build_scoring_table
from features.label import add_failure_within_horizon_label


def json_value(value):
    """Convert Spark values to JSON-safe scalar values."""
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def export_json(frame: DataFrame, destination: Path, limit: int | None = None) -> None:
    """Collect small display aggregates only; raw and Silver data remain in HDFS."""
    if limit:
        frame = frame.limit(limit)
    rows = [{key: json_value(value) for key, value in row.asDict(recursive=True).items()} for row in frame.collect()]
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")


def run(silver: str, gold: str, dashboard_dir: str, horizon_days: int) -> dict[str, object]:
    """Create labelled feature table, analytic parquet output and JSON web exports."""
    started = time.perf_counter()
    spark = SparkSession.builder.appName("smart-drive-gold").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    silver_frame = spark.read.parquet(silver)
    labelled = add_failure_within_horizon_label(silver_frame, horizon_days).cache()
    feature_table = build_feature_table(labelled)
    feature_table.write.mode("overwrite").parquet(f"{gold}/features")
    build_scoring_table(silver_frame).write.mode("overwrite").parquet(f"{gold}/features_scoring")

    overview = labelled.agg(F.count("*").alias("observations"), F.countDistinct("serial_number").alias("drives"), F.sum("failure").alias("recorded_failures"), F.sum("fail_within_7_days").alias("positive_labels"), F.min("date").alias("start_date"), F.max("date").alias("end_date"))
    by_model = labelled.groupBy("model").agg(F.countDistinct("serial_number").alias("drives"), F.sum("failure").alias("failures"), F.avg("fail_within_7_days").alias("future_failure_rate")).orderBy(F.desc("failures"), "model")
    by_time = labelled.groupBy("date").agg(F.countDistinct("serial_number").alias("drives"), F.sum("failure").alias("failures"), F.sum("fail_within_7_days").alias("risk_labels")).orderBy("date")
    smart_candidates = [column for column in ["smart_5_raw", "smart_187_raw", "smart_197_current_pending_sector_count"] if column in labelled.columns]
    smart_comparison = labelled.groupBy("fail_within_7_days").agg(*[F.avg(column).alias(f"avg_{column}") for column in smart_candidates]).orderBy("fail_within_7_days")
    newest = labelled.withColumn("row_rank", F.row_number().over(Window.partitionBy("serial_number").orderBy(F.desc("date")))).filter("row_rank = 1").drop("row_rank")
    drive_list = newest.select("serial_number", "model", "date", "failure", "fail_within_7_days").orderBy("serial_number")

    overview.write.mode("overwrite").parquet(f"{gold}/analytics/overview")
    by_model.write.mode("overwrite").parquet(f"{gold}/analytics/failure_by_model")
    by_time.write.mode("overwrite").parquet(f"{gold}/analytics/failure_over_time")
    smart_comparison.write.mode("overwrite").parquet(f"{gold}/analytics/smart_comparison")
    dashboard = Path(dashboard_dir)
    export_json(overview, dashboard / "overview.json")
    export_json(by_model, dashboard / "failure_by_model.json", 100)
    export_json(by_time, dashboard / "failure_over_time.json", 120)
    export_json(smart_comparison, dashboard / "smart_comparison.json")
    export_json(drive_list, dashboard / "drive_list.json", 500)
    labelled.unpersist()
    result = {"silver": silver, "gold": gold, "feature_rows": feature_table.count(), "runtime_seconds": round(time.perf_counter() - started, 2)}
    print(f"GOLD_RESULT {result}", flush=True)
    spark.stop()
    return result


if __name__ == "__main__":
    paths = ProjectPaths()
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver", default=paths.silver)
    parser.add_argument("--gold", default=paths.gold)
    parser.add_argument("--dashboard-dir", default=paths.dashboard_dir)
    parser.add_argument("--horizon-days", type=int, default=FAILURE_HORIZON_DAYS)
    args = parser.parse_args()
    run(args.silver, args.gold, args.dashboard_dir, args.horizon_days)
