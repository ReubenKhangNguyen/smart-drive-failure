from __future__ import annotations

from typing import Dict

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

LABEL_COL = "fail_within_7_days"


def pr_auc(predictions: DataFrame, label_col: str = LABEL_COL, score_col: str = "probability") -> float: # tinh AUC-PR (area under precision-recall curve) cho model binary classification
    """score_col may be the probability vector (default) or a numeric positive-class score such as risk_score."""
    evaluator = BinaryClassificationEvaluator(labelCol=label_col, rawPredictionCol=score_col, metricName="areaUnderPR")
    return evaluator.evaluate(predictions)


def roc_auc(predictions: DataFrame, label_col: str = LABEL_COL, score_col: str = "probability") -> float: # tinh AUC-ROC (area under receiver operating characteristic curve) cho model binary classification
    evaluator = BinaryClassificationEvaluator(labelCol=label_col, rawPredictionCol=score_col, metricName="areaUnderROC")
    return evaluator.evaluate(predictions)


def extract_risk_score(predictions: DataFrame, output_col: str = "risk_score") -> DataFrame: # tinh toan risk_score = probability cua class 1 (fail_within_7_days = 1) tu cot probability vector
    """Positive-class probability, no Python UDF (per .claude/rules/spark-hdfs.md)."""
    return predictions.withColumn(output_col, vector_to_array("probability")[1])


def recall_precision_at_k_by_day(
    df_with_score: DataFrame,
    k: int,
    label_col: str = LABEL_COL,
    score_col: str = "risk_score",
    date_col: str = "date",
    tie_break_col: str = "serial_number",
) -> DataFrame:
    """Per-day top-K precision/recall (K highest scores per day)."""
    window = Window.partitionBy(date_col).orderBy(F.col(score_col).desc(), F.col(tie_break_col)) # xep gia tri score tu cao den thap, neu bang nhau thi xep theo serial_number
    ranked = df_with_score.withColumn("risk_rank", F.row_number().over(window))
    topk = ranked.where(F.col("risk_rank") <= k)

    daily_positive_totals = df_with_score.groupBy(date_col).agg(F.sum(label_col).alias("total_positives"))
    daily_topk = topk.groupBy(date_col).agg(F.sum(label_col).alias("topk_hits"), F.count("*").alias("topk_count"))

    joined = daily_topk.join(daily_positive_totals, date_col)
    return joined.withColumn(
        "recall_at_k",
        F.when(F.col("total_positives") > 0, F.col("topk_hits") / F.col("total_positives")).otherwise(F.lit(None)),
    ).withColumn("precision_at_k", F.col("topk_hits") / F.col("topk_count")) #tinh precision = so luong du doan dung / tong so luong du doan


def macro_average_at_k(daily_df: DataFrame) -> Dict[str, float]: # so sánh trung bình macro của recall và precision trên tất cả các ngày
    row = daily_df.agg(
        F.avg("recall_at_k").alias("recall_at_k"), F.avg("precision_at_k").alias("precision_at_k")
    ).collect()[0]
    return {"recall_at_k": row["recall_at_k"], "precision_at_k": row["precision_at_k"]}
