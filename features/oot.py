from __future__ import annotations

from typing import Any, Dict, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from features.build_features import SMART_COLUMNS, build_features, feature_columns
from features.label import assign_split, build_labeled_dataset


def build_oot_features(
    silver_df: DataFrame,
    data_cfg: Dict[str, Any],
    horizon_days: int,
    columns: List[str] = SMART_COLUMNS,
) -> DataFrame:
    """Gold features for the out-of-time quarter only (rows with split = 'oot').

    silver_df holds BOTH quarters: the Q2 window features need the Q1 history of every
    serial, so features are computed on all of it and only the Q2 rows are returned.
    Q1 Gold features are never rebuilt, so the Q1 train/val/test numbers already
    reported stay reproducible.

    The StringIndexer is fit on the SAME train rows the frozen model was trained on
    (Q1 only, Q1 censoring end date). Fitting it on train rows labeled with the
    two-quarter Silver could shift category counts and reorder model_index, which would
    silently feed the frozen model different indexes.
    """
    q1_end = data_cfg["end_date"]
    q1_silver = silver_df.where(F.col("date") <= F.lit(q1_end).cast("date"))
    q1_labeled = build_labeled_dataset(q1_silver, q1_end, horizon_days)
    train_df = assign_split(
        q1_labeled, data_cfg["warmup_end_date"], data_cfg["train_end_date"], data_cfg["val_end_date"]
    ).where(F.col("split") == "train")

    labeled = build_labeled_dataset(silver_df, data_cfg["oot_end_date"], horizon_days)
    split_df = assign_split(
        labeled,
        data_cfg["warmup_end_date"],
        data_cfg["train_end_date"],
        data_cfg["val_end_date"],
        oot_start_date=data_cfg["oot_start_date"],
    )
    features_df = build_features(split_df, train_df, columns)
    cols = feature_columns(features_df)
    return features_df.where(F.col("split") == "oot").select(
        "date", "serial_number", "model", *cols, "fail_within_7_days", "split"
    )
