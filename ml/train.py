from __future__ import annotations

from typing import List

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

LABEL_COL = "fail_within_7_days"
WEIGHT_COL = "class_weight"


def add_class_weight(df: DataFrame, label_col: str = LABEL_COL, weight_col: str = WEIGHT_COL) -> DataFrame:
    """weightCol = inverse class frequency, so MLlib doesn't collapse to always-predict-0
    given the ~0.02% positive rate (docs/charter.md: weightCol theo ty le lop)."""
    counts = {r[label_col]: r["count"] for r in df.groupBy(label_col).count().collect()}
    total = sum(counts.values())
    pos = counts.get(1, 1)
    neg = counts.get(0, 1)
    weight_expr = F.when(F.col(label_col) == 1, F.lit(total / (2.0 * pos))).otherwise(F.lit(total / (2.0 * neg)))
    return df.withColumn(weight_col, weight_expr)


def build_pipeline(feature_cols: List[str], model_type: str, label_col: str = LABEL_COL, weight_col: str = WEIGHT_COL) -> Pipeline:
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="keep")
    if model_type == "logistic_regression":
        classifier = LogisticRegression(
            featuresCol="features", labelCol=label_col, weightCol=weight_col, maxIter=50, standardization=True
        )
    elif model_type == "random_forest":
        classifier = RandomForestClassifier(
            featuresCol="features",
            labelCol=label_col,
            weightCol=weight_col,
            numTrees=30,
            maxDepth=8,
            seed=42,
            # model_index/manufacturer_index carry StringIndexer's nominal-attribute
            # metadata through VectorAssembler; maxBins must cover the largest
            # category count (model has ~100+ distinct values in Q1-2026) or MLlib
            # raises "DecisionTree requires maxBins to be at least as large as ...".
            maxBins=256,
        )
    else:
        raise ValueError("Unknown model_type: {}".format(model_type))
    return Pipeline(stages=[assembler, classifier])


def train_model(train_df: DataFrame, feature_cols: List[str], model_type: str) -> PipelineModel:
    weighted = add_class_weight(train_df)
    pipeline = build_pipeline(feature_cols, model_type)
    return pipeline.fit(weighted)
