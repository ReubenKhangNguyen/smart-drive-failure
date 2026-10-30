"""Refit the Logistic Regression on the TRAIN split only, with the same settings as the official model, and print the
loss per iteration (objectiveHistory) as one JSON line, so a convergence chart can be drawn for the report.

Why a refit: ml/train.py returns only the fitted PipelineModel and the saved model (docs/contracts.md, HD3) does not keep
the training summary, so the history of the loss cannot be read back from it. Nothing is written to HDFS: the saved model
v20260930-1 stays untouched. Validation and test are not read (.claude/rules/ml-leakage.md).

Run: see docs/quick-start.md (spark-submit on spark-master). The line starting with LOSS_JSON: carries the result.
"""
from __future__ import annotations

import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import hdfs_uri, load_config
from features.build_features import feature_columns
from ml.train import train_model


def main() -> int:
    config = load_config()
    spark = SparkSession.builder.appName("smart-loss-history").getOrCreate()
    df = spark.read.parquet(hdfs_uri(config, "features"))
    cols = [c for c in feature_columns(df) if c in df.columns]
    train_df = df.where(F.col("split") == "train")

    model = train_model(train_df, cols, "logistic_regression")
    summary = model.stages[-1].summary
    history = [float(x) for x in summary.objectiveHistory]
    result = {
        "model_type": "logistic_regression",
        "split": "train",
        "n_features": len(cols),
        "max_iter": 50,
        "total_iterations": int(summary.totalIterations),
        "objective_history": history,
    }
    print("LOSS_JSON:" + json.dumps(result))
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
