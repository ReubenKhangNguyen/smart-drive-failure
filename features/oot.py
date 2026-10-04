from __future__ import annotations

from typing import Any, Dict, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.settings import quarters_from_data
from features.build_features import SMART_COLUMNS, build_features, feature_columns
from features.label import assign_split, build_labeled_dataset


def build_quarter_features(
    silver_df: DataFrame,
    data_cfg: Dict[str, Any],
    quarter: Dict[str, Any],
    horizon_days: int,
    columns: List[str] = SMART_COLUMNS,
) -> DataFrame:
    """Gold features of one out-of-time quarter only (rows whose split is quarter['split']).

    silver_df may hold this quarter AND later ones; it is cut at the quarter's last day first, so the labels and
    features of a quarter never depend on quarters loaded after it. Adding a new quarter therefore cannot change
    a quarter already evaluated and published.

    The window features need the history of every earlier quarter, so they are computed on all Silver up to the
    quarter's end and only this quarter's rows are returned. Earlier Gold features are never rebuilt.

    The StringIndexer is fit on the SAME train rows the frozen model was trained on (analysis quarter only, its own
    censoring end date). Fitting it on train rows labeled with a longer Silver could shift category counts and
    reorder model_index, which would silently feed the frozen model different indexes.
    """
    q1_end = data_cfg["end_date"]
    analysis_silver = silver_df.where(F.col("date") <= F.lit(q1_end).cast("date"))
    analysis_labeled = build_labeled_dataset(analysis_silver, q1_end, horizon_days)
    train_df = assign_split(
        analysis_labeled, data_cfg["warmup_end_date"], data_cfg["train_end_date"], data_cfg["val_end_date"]
    ).where(F.col("split") == "train")

    upto_end = silver_df.where(F.col("date") <= F.lit(quarter["end_date"]).cast("date"))
    labeled = build_labeled_dataset(upto_end, quarter["end_date"], horizon_days)
    split_df = assign_split(
        labeled,
        data_cfg["warmup_end_date"],
        data_cfg["train_end_date"],
        data_cfg["val_end_date"],
        oot_start_date=quarter["start_date"],
        oot_label=quarter["split"],
    )
    features_df = build_features(split_df, train_df, columns)
    cols = feature_columns(features_df)
    return features_df.where(F.col("split") == quarter["split"]).select(
        "date", "serial_number", "model", *cols, "fail_within_7_days", "split"
    )


def build_oot_features(
    silver_df: DataFrame,
    data_cfg: Dict[str, Any],
    horizon_days: int,
    columns: List[str] = SMART_COLUMNS,
) -> DataFrame:
    """The first configured out-of-time quarter (Q2-2026 in the shipped config); kept for older callers."""
    quarter = quarters_from_data(data_cfg)[0]
    return build_quarter_features(silver_df, data_cfg, quarter, horizon_days, columns)


def quarter_fingerprint(df: DataFrame) -> Dict[str, Any]:
    """Row count, positive labels and a checksum over every column (order-independent): two DataFrames with the
    same fingerprint hold the same rows. Used to check a recomputed quarter against the Gold partitions already
    written, without writing anything."""
    cols = sorted(df.columns)
    row = df.select(*cols).agg(
        F.count("*").alias("rows"),
        F.sum("fail_within_7_days").alias("positives"),
        F.sum(F.xxhash64(*[F.col(c) for c in cols]).cast("decimal(38,0)")).alias("checksum"),
    ).collect()[0]
    return {"rows": row["rows"], "positives": int(row["positives"] or 0), "checksum": str(row["checksum"])}
