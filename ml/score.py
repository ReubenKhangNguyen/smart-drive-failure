"""Score the latest observation per drive and export model-based dashboard predictions."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from analytics.generate_gold import export_json
from config.settings import ProjectPaths

def run(features_path: str, model_path: str, predictions_path: str, dashboard_dir: str) -> int:
    """Apply the saved model to the newest observations and write HDFS/JSON outputs."""
    spark = SparkSession.builder.appName("smart-drive-scoring").getOrCreate()
    features = spark.read.parquet(features_path)
    latest = features.withColumn("row_rank", F.row_number().over(Window.partitionBy("serial_number").orderBy(F.desc("date")))).filter("row_rank = 1").drop("row_rank")
    scored = PipelineModel.load(model_path).transform(latest).select("serial_number", "model", "date", F.col("probability")[1].alias("failure_risk_7d")).orderBy(F.desc("failure_risk_7d"))
    scored.write.mode("overwrite").parquet(predictions_path)
    export_json(scored, Path(dashboard_dir) / "predictions.json", 500)
    count = scored.count()
    spark.stop()
    return count

if __name__ == "__main__":
    paths = ProjectPaths()
    parser = argparse.ArgumentParser()
    parser.add_argument("--features", default=f"{paths.gold}/features_scoring")
    parser.add_argument("--model", required=True)
    parser.add_argument("--predictions", default=f"{paths.gold}/predictions")
    parser.add_argument("--dashboard-dir", default=paths.dashboard_dir)
    args = parser.parse_args()
    print(json.dumps({"predictions": run(args.features, args.model, args.predictions, args.dashboard_dir)}))
