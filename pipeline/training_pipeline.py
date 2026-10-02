from __future__ import annotations

from typing import Any, Dict, List

from config.settings import hdfs_uri
from pipeline.batch_pipeline import PipelineStep


def build_steps(spark, config: Dict[str, Any]) -> List[PipelineStep]:
    """Train + evaluate on val ONLY — never touches the test split. The official
    test evaluation already ran exactly once (docs/decisions.md, 2026-09-30); this
    pipeline exists for re-running train/val model selection, not for re-deciding
    anything on test (.claude/rules/ml-leakage.md: test runs once, period)."""
    from pyspark.sql import functions as F

    from ml.evaluate import extract_risk_score, macro_average_at_k, pr_auc, recall_precision_at_k_by_day, roc_auc
    from ml.train import train_model
    from features.build_features import feature_columns

    features_path = hdfs_uri(config, "features")
    k = config["ml"]["k"]

    def step_train_and_evaluate() -> Dict[str, Any]:
        df = spark.read.parquet(features_path)
        cols = [c for c in feature_columns(df) if c in df.columns]

        train_df = df.where(F.col("split") == "train")
        val_df = df.where(F.col("split") == "val")

        results = {}
        for model_type in config["ml"]["models"]:
            model = train_model(train_df, cols, model_type)
            val_pred = extract_risk_score(model.transform(val_df))
            metrics = {"pr_auc": pr_auc(val_pred), "roc_auc": roc_auc(val_pred)}
            daily = recall_precision_at_k_by_day(val_pred, k=k)
            metrics.update(macro_average_at_k(daily))
            results[model_type] = metrics

        return {"val_metrics": results}

    return [("train_and_evaluate_on_val", step_train_and_evaluate)]
