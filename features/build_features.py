"""Build the model table while excluding identifiers and future-information fields."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.settings import FORBIDDEN_FEATURE_COLUMNS

def selected_feature_columns(frame: DataFrame) -> list[str]:
    """Return stable safe SMART fields present in the current Silver schema."""
    candidates = ["model", "capacity_bytes", "smart_5_raw", "smart_5_normalized", "smart_187_raw", "smart_187_normalized", "smart_197_current_pending_sector_count", "smart_197_raw", "smart_198_raw", "smart_199_raw"]
    return [column for column in candidates if column in frame.columns and column not in FORBIDDEN_FEATURE_COLUMNS]

def build_feature_table(frame: DataFrame) -> DataFrame:
    """Return typed safe features; imputation is fitted later only on train data."""
    features = selected_feature_columns(frame)
    if not features:
        raise ValueError("No safe SMART feature columns are available in the Silver schema.")
    return frame.select("date", "serial_number", "fail_within_7_days", *features).filter(F.col("fail_within_7_days").isNotNull())

def build_scoring_table(frame: DataFrame) -> DataFrame:
    """Keep current safe fields for scoring; it intentionally contains no outcome label."""
    features = selected_feature_columns(frame)
    if not features:
        raise ValueError("No safe SMART feature columns are available in the Silver schema.")
    return frame.select("date", "serial_number", *features)
