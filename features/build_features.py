from __future__ import annotations

from typing import List, Tuple

from pyspark.ml.feature import StringIndexer
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

SMART_COLUMNS = [
    "smart_5_raw",
    "smart_9_raw",
    "smart_187_raw",
    "smart_188_raw",
    "smart_194_raw",
    "smart_197_raw",
    "smart_198_raw",
    "smart_199_raw",
]

WINDOWS = [7, 14, 30]

NON_FEATURE_COLUMNS = {
    "date",
    "serial_number",
    "model",
    "manufacturer",
    "failure",
    "fail_within_7_days",
    "split",
    # capacity_bytes can be null (docs/contracts.md HD1) and has no missing-flag/window
    # handling here; keep it out of the feature set until it gets that treatment,
    # rather than let a raw nullable column reach a VectorAssembler unannounced.
    "capacity_bytes",
}


def add_missing_flags(df: DataFrame, columns: List[str] = SMART_COLUMNS) -> DataFrame:
    for c in columns:
        df = df.withColumn(c + "_is_missing", F.col(c).isNull().cast("int"))
        df = df.withColumn(c, F.coalesce(F.col(c), F.lit(0.0)))
    return df


def add_window_features(df: DataFrame, columns: List[str] = SMART_COLUMNS, windows: List[int] = WINDOWS) -> DataFrame:
    """Rolling max, delta-vs-N-days-ago, and count of day-over-day increases.

    Every window is ORDER BY date, ROWS BETWEEN -(w-1) AND 0 (current + past rows
    only) — never looks at rows dated after the current one, per ml-leakage.md.
    """
    row_window = Window.partitionBy("serial_number").orderBy("date")

    increased_cols = []
    for c in columns:
        prev_value = F.lag(c, 1).over(row_window)
        col_name = c + "_increased_tmp"
        df = df.withColumn(col_name, F.when(F.col(c) > prev_value, 1).otherwise(0))
        increased_cols.append(col_name)

    for c, inc_col in zip(columns, increased_cols):
        for w in windows:
            win = row_window.rowsBetween(-(w - 1), 0)
            df = df.withColumn("{}_max_{}d".format(c, w), F.max(c).over(win))
            # lag(c, w) is null for a serial's first w rows (not enough history yet);
            # coalesce to 0 so VectorAssembler never sees a NaN downstream.
            delta = F.col(c) - F.coalesce(F.lag(c, w).over(row_window), F.col(c))
            df = df.withColumn("{}_delta_{}d".format(c, w), delta)
            df = df.withColumn("{}_increasing_days_{}d".format(c, w), F.sum(inc_col).over(win))

    return df.drop(*increased_cols)


def index_categorical(df: DataFrame, train_df: DataFrame, columns: Tuple[str, ...] = ("model", "manufacturer")) -> DataFrame:
    """Fit StringIndexer on train_df rows only, then transform the full df."""
    result = df
    for c in columns:
        indexer = StringIndexer(inputCol=c, outputCol=c + "_index", handleInvalid="keep")
        fitted = indexer.fit(train_df)
        result = fitted.transform(result)
    return result


def build_features(df: DataFrame, train_df: DataFrame, columns: List[str] = SMART_COLUMNS) -> DataFrame:
    df = add_missing_flags(df, columns)
    df = add_window_features(df, columns)
    df = index_categorical(df, train_df)
    return df


def feature_columns(df: DataFrame) -> List[str]:
    return [c for c in df.columns if c not in NON_FEATURE_COLUMNS]
