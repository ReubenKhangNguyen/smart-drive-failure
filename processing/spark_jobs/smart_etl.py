from __future__ import annotations

import argparse
import time
from typing import Any, Dict, Tuple

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType

REQUIRED_RAW_COLUMNS = [
    "date",
    "serial_number",
    "model",
    "capacity_bytes",
    "failure",
    "smart_5_raw",
    "smart_9_raw",
    "smart_187_raw",
    "smart_188_raw",
    "smart_194_raw",
    "smart_197_raw",
    "smart_198_raw",
    "smart_199_raw",
]

_DOUBLE_COLUMNS = [
    "smart_5_raw",
    "smart_9_raw",
    "smart_187_raw",
    "smart_188_raw",
    "smart_194_raw",
    "smart_197_raw",
    "smart_198_raw",
    "smart_199_raw",
]


def ensure_required_columns(df: DataFrame) -> DataFrame:
    """Add any required column missing from this quarter's Bronze schema as typed null
    (schema drift across quarters, per .claude/rules/spark-hdfs.md)."""
    string_typed = {"date", "serial_number", "model", "capacity_bytes", "failure"}
    for col_name in REQUIRED_RAW_COLUMNS:
        if col_name not in df.columns:
            cast_type = StringType() if col_name in string_typed else DoubleType()
            df = df.withColumn(col_name, F.lit(None).cast(cast_type))
    return df


def infer_manufacturer(df: DataFrame) -> DataFrame:
    model_upper = F.upper(F.coalesce(F.col("model"), F.lit("")))
    expr = (
        F.when(model_upper.startswith("ST"), F.lit("Seagate"))
        .when(model_upper.startswith("WDC"), F.lit("Western Digital"))
        .when(
            model_upper.startswith("HGST")
            | model_upper.startswith("HUH")
            | model_upper.startswith("HUS"),
            F.lit("HGST"),
        )
        .when(
            model_upper.startswith("TOSHIBA")
            | model_upper.startswith("MG")
            | model_upper.startswith("MQ"),
            F.lit("Toshiba"),
        )
        .when(model_upper.contains("SAMSUNG"), F.lit("Samsung"))
        .otherwise(F.lit(None).cast(StringType()))
    )
    return df.withColumn("manufacturer", expr)


def clean_and_align(df: DataFrame) -> Tuple[DataFrame, Dict[str, Any]]:
    """Select/cast Bronze raw columns to the HD1 Silver schema, null out negative
    capacity, infer manufacturer, and de-duplicate by (serial_number, date)."""
    stats: Dict[str, Any] = {"rows_in": df.count()}

    df = ensure_required_columns(df)
    df = df.select(*REQUIRED_RAW_COLUMNS)

    df = df.withColumn("date", F.to_date("date"))
    df = df.withColumn("capacity_bytes", F.col("capacity_bytes").cast(LongType()))
    df = df.withColumn(
        "capacity_bytes",
        F.when(F.col("capacity_bytes") < 0, None).otherwise(F.col("capacity_bytes")),
    )
    df = df.withColumn("failure", F.col("failure").cast(IntegerType()))
    for col_name in _DOUBLE_COLUMNS:
        df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))

    df = infer_manufacturer(df)

    window = Window.partitionBy("serial_number", "date").orderBy(F.lit(1))
    df = df.withColumn("_rn", F.row_number().over(window)).where(F.col("_rn") == 1).drop("_rn")

    stats["rows_out"] = df.count()
    stats["duplicates_removed"] = stats["rows_in"] - stats["rows_out"]

    column_order = [
        "date",
        "serial_number",
        "model",
        "manufacturer",
        "capacity_bytes",
        "failure",
        "smart_5_raw",
        "smart_9_raw",
        "smart_187_raw",
        "smart_188_raw",
        "smart_194_raw",
        "smart_197_raw",
        "smart_198_raw",
        "smart_199_raw",
    ]
    return df.select(*column_order), stats


def run(spark: SparkSession, bronze_path: str, silver_path: str) -> Dict[str, Any]:
    start = time.time()
    raw = spark.read.option("header", "true").csv(bronze_path)
    clean_df, stats = clean_and_align(raw)

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    (
        clean_df.repartition("date")
        .write.mode("overwrite")
        .partitionBy("date")
        .parquet(silver_path)
    )

    stats["elapsed_seconds"] = round(time.time() - start, 1)
    stats["bronze_path"] = bronze_path
    stats["silver_path"] = silver_path
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description="Bronze CSV -> Silver Parquet (HD1)")
    parser.add_argument("--bronze-path", required=True)
    parser.add_argument("--silver-path", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("smart-etl-bronze-to-silver").getOrCreate()
    stats = run(spark, args.bronze_path, args.silver_path)
    print("ETL stats:", stats)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
