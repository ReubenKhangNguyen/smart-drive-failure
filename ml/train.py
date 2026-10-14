"""Train Logistic Regression and Random Forest with a temporal 70/15/15 split."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.feature import Imputer, StringIndexer, VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.settings import ProjectPaths
from features.build_features import selected_feature_columns
from ml.evaluate import choose_threshold, evaluate

def temporal_split(frame: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Split ordered observation dates into 70%, 15%, and 15% chronological sets."""
    dates = [row["date"] for row in frame.select("date").distinct().orderBy("date").collect()]
    if len(dates) < 7:
        raise ValueError("At least seven distinct dates are required for a temporal train/validation/test split.")
    train_end = dates[max(1, int(len(dates) * 0.70)) - 1]
    validation_end = dates[max(int(len(dates) * 0.85), int(len(dates) * 0.70) + 1) - 1]
    return frame.filter(F.col("date") <= F.lit(train_end)), frame.filter((F.col("date") > F.lit(train_end)) & (F.col("date") <= F.lit(validation_end))), frame.filter(F.col("date") > F.lit(validation_end))

def make_pipeline(feature_columns: list[str], classifier) -> Pipeline:
    """Create fit-on-train preprocessing without serial or failure fields."""
    numeric = [column for column in feature_columns if column != "model"]
    stages, assembled = [], list(numeric)
    if numeric:
        stages.append(Imputer(inputCols=numeric, outputCols=[f"{column}_imputed" for column in numeric], strategy="median"))
        assembled = [f"{column}_imputed" for column in numeric]
    if "model" in feature_columns:
        stages.append(StringIndexer(inputCol="model", outputCol="model_index", handleInvalid="keep"))
        assembled.append("model_index")
    stages += [VectorAssembler(inputCols=assembled, outputCol="features", handleInvalid="keep"), classifier]
    return Pipeline(stages=stages)

def add_class_weight(train: DataFrame) -> DataFrame:
    """Upweight positive rows from training counts only."""
    counts = {row["label"]: row["count"] for row in train.groupBy("label").count().collect()}
    positive, negative = counts.get(1.0, 0), counts.get(0.0, 0)
    if not positive or not negative:
        raise ValueError("The training period must include positive and negative labels.")
    return train.withColumn("class_weight", F.when(F.col("label") == 1.0, F.lit(negative / positive)).otherwise(F.lit(1.0)))

def run(features_path: str, models_path: str, metrics_path: str) -> dict[str, object]:
    """Fit both baselines, select by test PR-AUC, and save model plus actual metrics."""
    spark = SparkSession.builder.appName("smart-drive-training").getOrCreate()
    source = spark.read.parquet(features_path)
    feature_columns = selected_feature_columns(source)
    train, validation, test = temporal_split(source.withColumn("label", F.col("fail_within_7_days").cast("double")))
    weighted_train = add_class_weight(train)
    classifiers = {"logistic_regression": LogisticRegression(labelCol="label", featuresCol="features", weightCol="class_weight", maxIter=50, regParam=0.01), "random_forest": RandomForestClassifier(labelCol="label", featuresCol="features", weightCol="class_weight", numTrees=80, maxDepth=8, seed=42)}
    results = {}
    for name, classifier in classifiers.items():
        fitted = make_pipeline(feature_columns, classifier).fit(weighted_train)
        threshold_result = choose_threshold(fitted.transform(validation))
        metrics = evaluate(fitted.transform(test), threshold_result["threshold"])
        fitted.write().overwrite().save(f"{models_path}/{name}")
        results[name] = {"validation": threshold_result, "test": metrics}
    winner = max(results, key=lambda name: results[name]["test"]["pr_auc"])
    summary = {"selected_model": winner, "feature_columns": feature_columns, "split": "temporal 70/15/15", "models": results}
    Path(metrics_path).parent.mkdir(parents=True, exist_ok=True)
    Path(metrics_path).write_text(json.dumps(summary, indent=2), encoding="utf-8")
    spark.stop()
    return summary

if __name__ == "__main__":
    paths = ProjectPaths()
    parser = argparse.ArgumentParser()
    parser.add_argument("--features", default=f"{paths.gold}/features")
    parser.add_argument("--models", default=paths.models)
    parser.add_argument("--metrics", default="artifacts/metrics/model_metrics.json")
    args = parser.parse_args()
    print(json.dumps(run(args.features, args.models, args.metrics), indent=2))
