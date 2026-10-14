"""Cluster latest drive SMART observations into descriptive health segments with Spark MLlib."""

from __future__ import annotations

import argparse
import json

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import Imputer, StandardScaler, VectorAssembler
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


SMART_FEATURES = ["smart_5_raw", "smart_187_raw", "smart_197_raw", "smart_198_raw", "smart_199_raw"]


def run(silver_path: str, output_path: str, k: int) -> dict[str, object]:
    """Use the latest row per drive, fit K-Means, and write cluster assignments to Gold."""
    spark = SparkSession.builder.appName("smart-drive-kmeans-health").getOrCreate()
    source = spark.read.parquet(silver_path)
    feature_columns = [column for column in SMART_FEATURES if column in source.columns]
    if len(feature_columns) < 2:
        raise ValueError("At least two SMART columns are required for health clustering.")
    latest = (
        source.withColumn("row_number", F.row_number().over(Window.partitionBy("serial_number").orderBy(F.desc("date"))))
        .filter("row_number = 1")
        .drop("row_number")
        .select("serial_number", "model", "date", *feature_columns)
    )
    imputed = [f"{column}_imputed" for column in feature_columns]
    pipeline = Pipeline(
        stages=[
            Imputer(inputCols=feature_columns, outputCols=imputed, strategy="median"),
            VectorAssembler(inputCols=imputed, outputCol="raw_features"),
            StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True),
            KMeans(k=k, seed=42, featuresCol="features", predictionCol="health_cluster"),
        ]
    )
    model = pipeline.fit(latest)
    assignments = model.transform(latest).select("serial_number", "model", "date", *feature_columns, "health_cluster")
    assignments.write.mode("overwrite").partitionBy("health_cluster").parquet(f"{output_path}/assignments")
    summary = assignments.groupBy("health_cluster").agg(F.count("*").alias("drives"), *[F.avg(column).alias(f"avg_{column}") for column in feature_columns]).orderBy("health_cluster")
    summary.write.mode("overwrite").parquet(f"{output_path}/summary")
    result = {
        "input": silver_path,
        "output": output_path,
        "k": k,
        "feature_columns": feature_columns,
        "drives_clustered": assignments.count(),
        "training_cost": model.stages[-1].summary.trainingCost,
    }
    print(f"KMEANS_RESULT {json.dumps(result)}", flush=True)
    spark.stop()
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cluster latest SMART readings into health segments.")
    parser.add_argument("--silver", default="hdfs://namenode:9000/smart-drive/silver/daily")
    parser.add_argument("--output", default="hdfs://namenode:9000/smart-drive/gold/health-clusters")
    parser.add_argument("--k", type=int, default=3)
    args = parser.parse_args()
    run(args.silver, args.output, args.k)
