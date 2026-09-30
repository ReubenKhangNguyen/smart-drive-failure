from __future__ import annotations

import argparse
import datetime as dt
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from analytics.smart_analysis import failure_dates

RULES_VERSION = "rules_v1"

WATCH_COLUMNS = ["smart_5_raw", "smart_187_raw", "smart_197_raw", "smart_198_raw"]

CRITICAL_THRESHOLDS: Dict[str, float] = {
    "smart_5_raw": 102,
    "smart_187_raw": 40,
    "smart_197_raw": 16,
    "smart_198_raw": 8,
}


def classify_health(df: DataFrame, rules_version: str = RULES_VERSION) -> DataFrame:
    """Apply docs/health_rules.md rules_v1 to Silver drive-day rows -> HD5 schema."""
    nonzero_flags = [
        F.when(F.coalesce(F.col(c), F.lit(0.0)) > 0, 1).otherwise(0) for c in WATCH_COLUMNS
    ]
    nonzero_count = nonzero_flags[0]
    for flag in nonzero_flags[1:]:
        nonzero_count = nonzero_count + flag

    severe = F.lit(False)
    for col_name, threshold in CRITICAL_THRESHOLDS.items():
        severe = severe | (F.coalesce(F.col(col_name), F.lit(0.0)) >= threshold)

    health_level = (
        F.when((nonzero_count >= 2) | severe, F.lit("CRITICAL"))
        .when(nonzero_count == 1, F.lit("WATCH"))
        .otherwise(F.lit("HEALTHY"))
    )

    reason_exprs = []
    for col_name, threshold in CRITICAL_THRESHOLDS.items():
        value = F.coalesce(F.col(col_name), F.lit(0.0))
        reason_exprs.append(
            F.when(
                value >= threshold,
                F.concat(F.lit(col_name + " >= " + str(threshold) + " (gia tri "), value.cast("string"), F.lit(")")),
            )
            .when(
                value > 0,
                F.concat(F.lit(col_name + " > 0 (gia tri "), value.cast("string"), F.lit(")")),
            )
            .otherwise(F.lit(None).cast("string"))
        )
    reasons = F.filter(F.array(*reason_exprs), lambda x: x.isNotNull())

    return df.select(
        "date",
        "serial_number",
        "model",
        health_level.alias("health_level"),
        reasons.alias("reasons"),
        F.lit(rules_version).alias("rules_version"),
    )


def failure_within_days(silver_df: DataFrame, days: int = 7) -> DataFrame:
    """For each drive-day row, whether this serial fails within (date, date + days]."""
    fd = failure_dates(silver_df)
    joined = silver_df.select("serial_number", "date").join(fd, "serial_number", "left")
    flag = F.when(
        F.col("failure_date").isNotNull()
        & (F.col("failure_date") > F.col("date"))
        & (F.col("failure_date") <= F.date_add(F.col("date"), days)),
        1,
    ).otherwise(0)
    return joined.withColumn("fail_within_days", flag).select("serial_number", "date", "fail_within_days")


def baseline_failure_rate_by_level(silver_df: DataFrame, health_df: DataFrame, days: int = 7) -> DataFrame:
    """Group health_status rows by level and report the real failure rate within the
    next `days`, excluding the last `days` of the dataset (right-censoring)."""
    max_date = silver_df.agg(F.max("date")).collect()[0][0]
    cutoff = max_date - dt.timedelta(days=days)

    labeled = failure_within_days(silver_df, days)
    joined = health_df.join(labeled, ["serial_number", "date"])
    joined = joined.where(F.col("date") <= F.lit(cutoff))

    return joined.groupBy("health_level").agg(
        F.count("*").alias("total_rows"),
        F.sum("fail_within_days").alias("failed_within_window"),
        (F.sum("fail_within_days") / F.count("*")).alias("failure_rate"),
    )


def run(spark: SparkSession, silver_path: str, health_status_path: str) -> Dict[str, object]:
    silver_df = spark.read.parquet(silver_path)
    health_df = classify_health(silver_df)

    health_df.repartition("date").write.mode("overwrite").partitionBy("date").parquet(health_status_path)

    distribution = {
        row["health_level"]: row["count"]
        for row in health_df.groupBy("health_level").count().collect()
    }
    baseline = {
        row["health_level"]: {
            "total_rows": row["total_rows"],
            "failed_within_window": row["failed_within_window"],
            "failure_rate": row["failure_rate"],
        }
        for row in baseline_failure_rate_by_level(silver_df, health_df).collect()
    }
    return {"distribution": distribution, "baseline_failure_rate_7d": baseline}


def main() -> int:
    parser = argparse.ArgumentParser(description="Silver -> Gold health_status (HD5, rules_v1)")
    parser.add_argument("--silver-path", required=True)
    parser.add_argument("--health-status-path", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("smart-health-status").getOrCreate()
    stats = run(spark, args.silver_path, args.health_status_path)
    print("Health status stats:", stats)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
