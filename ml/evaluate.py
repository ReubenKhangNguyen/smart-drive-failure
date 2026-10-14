"""Metrics and validation threshold selection for binary Spark MLlib predictions."""

from __future__ import annotations

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def metrics_at_threshold(predictions: DataFrame, threshold: float) -> dict[str, float]:
    """Calculate precision, recall, F1 and confusion-matrix counts in Spark."""
    scored = predictions.withColumn("positive_score", F.col("probability")[1]).withColumn("threshold_prediction", (F.col("positive_score") >= F.lit(threshold)).cast("int"))
    row = scored.agg(F.sum(F.when((F.col("label") == 1) & (F.col("threshold_prediction") == 1), 1).otherwise(0)).alias("tp"), F.sum(F.when((F.col("label") == 0) & (F.col("threshold_prediction") == 1), 1).otherwise(0)).alias("fp"), F.sum(F.when((F.col("label") == 1) & (F.col("threshold_prediction") == 0), 1).otherwise(0)).alias("fn"), F.sum(F.when((F.col("label") == 0) & (F.col("threshold_prediction") == 0), 1).otherwise(0)).alias("tn")).first().asDict()
    precision = row["tp"] / (row["tp"] + row["fp"]) if row["tp"] + row["fp"] else 0.0
    recall = row["tp"] / (row["tp"] + row["fn"]) if row["tp"] + row["fn"] else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {**{key: int(value or 0) for key, value in row.items()}, "threshold": threshold, "precision": precision, "recall": recall, "f1": f1}

def choose_threshold(validation_predictions: DataFrame) -> dict[str, float]:
    """Choose the validation threshold with maximum F1 from a transparent grid."""
    candidates = [metrics_at_threshold(validation_predictions, threshold) for threshold in [round(step / 100, 2) for step in range(5, 96, 5)]]
    return max(candidates, key=lambda item: item["f1"])

def evaluate(test_predictions: DataFrame, threshold: float) -> dict[str, float]:
    """Return PR-AUC plus threshold-dependent test metrics."""
    pr_auc = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderPR").evaluate(test_predictions)
    return {"pr_auc": pr_auc, **metrics_at_threshold(test_predictions, threshold)}
