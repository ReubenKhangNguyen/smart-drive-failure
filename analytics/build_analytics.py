from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Any, Dict, List, Sequence

from pyspark.sql import DataFrame, SparkSession

from analytics.failure_analysis import afr_by_group
from config.settings import hdfs_uri, load_config

SILVER_VIEW = "silver_daily"
FAILED_VIEW = "failed_drives"
DEFAULT_EXPORT_DIR = Path(__file__).resolve().parent.parent / "artifacts" / "dashboard"
SIGNAL_WINDOWS = (7, 30)


def register_views(silver_df: DataFrame) -> None:
    """Temp views shared by every query: Silver, and one row per failed serial."""
    spark = silver_df.sparkSession
    silver_df.createOrReplaceTempView(SILVER_VIEW)
    spark.sql(
        "CREATE OR REPLACE TEMP VIEW {failed} AS "
        "SELECT serial_number, MIN(date) AS failure_date FROM {silver} WHERE failure = 1 GROUP BY serial_number".format(
            failed=FAILED_VIEW, silver=SILVER_VIEW
        )
    )


def build_afr(silver_df: DataFrame, group_col: str) -> DataFrame:
    """AFR = failures / (drive_days / 365), a yearly ratio (not x100), via Spark SQL (HD6)."""
    result = afr_by_group(silver_df, [group_col], view_name=SILVER_VIEW)
    return result.selectExpr(
        group_col,
        "CAST(drive_days AS BIGINT) AS drive_days",
        "CAST(failures AS BIGINT) AS failures",
        "CAST(afr AS DOUBLE) AS afr",
    )


def build_smart_distribution(spark: SparkSession, columns: Sequence[str]) -> DataFrame:
    """Healthy = last observed day of never-failed serials; pre_failure = the day before
    the failure of failed serials (same definition as smart_analysis.healthy_vs_failed_snapshot)."""
    cohorts = {
        "healthy": (
            "SELECT s.* FROM {silver} s "
            "LEFT ANTI JOIN {failed} f ON s.serial_number = f.serial_number "
            "JOIN (SELECT s2.serial_number, MAX(s2.date) AS last_date FROM {silver} s2 "
            "      LEFT ANTI JOIN {failed} f2 ON s2.serial_number = f2.serial_number "
            "      GROUP BY s2.serial_number) m "
            "  ON s.serial_number = m.serial_number AND s.date = m.last_date"
        ),
        "pre_failure": (
            "SELECT s.* FROM {silver} s JOIN {failed} f ON s.serial_number = f.serial_number "
            "WHERE s.date = date_sub(f.failure_date, 1)"
        ),
    }
    for name, query in cohorts.items():
        spark.sql(query.format(silver=SILVER_VIEW, failed=FAILED_VIEW)).createOrReplaceTempView("cohort_" + name)

    selects: List[str] = []
    for cohort in cohorts:
        for col in columns:
            selects.append(
                "SELECT '{col}' AS smart_attribute, '{cohort}' AS cohort, "
                "CAST(COUNT({col}) AS BIGINT) AS n, "
                "CAST(AVG({col}) AS DOUBLE) AS mean, "
                "CAST(percentile_approx({col}, 0.5) AS DOUBLE) AS p50, "
                "CAST(percentile_approx({col}, 0.9) AS DOUBLE) AS p90, "
                "CAST(percentile_approx({col}, 0.99) AS DOUBLE) AS p99 "
                "FROM cohort_{cohort}".format(col=col, cohort=cohort)
            )
    return spark.sql(" UNION ALL ".join(selects))


def build_pre_failure_signal(spark: SparkSession, columns: Sequence[str], windows: Sequence[int] = SIGNAL_WINDOWS) -> DataFrame:
    """Per failed serial: did the attribute exceed 0 at least once in
    [failure_date - window_days, failure_date - 1]? (same as smart_analysis.pre_failure_signal_rate)."""
    selects: List[str] = []
    for col in columns:
        for days in windows:
            selects.append(
                "SELECT '{col}' AS smart_attribute, CAST({days} AS INT) AS window_days, "
                "CAST((SELECT COUNT(*) FROM {failed}) AS BIGINT) AS failed_serials, "
                "CAST(COALESCE(SUM(CASE WHEN max_val > 0 THEN 1 ELSE 0 END), 0) AS BIGINT) AS signalled_serials "
                "FROM (SELECT f.serial_number, MAX(s.{col}) AS max_val "
                "      FROM {failed} f JOIN {silver} s ON s.serial_number = f.serial_number "
                "       AND s.date >= date_sub(f.failure_date, {days}) AND s.date < f.failure_date "
                "      GROUP BY f.serial_number)".format(col=col, days=days, failed=FAILED_VIEW, silver=SILVER_VIEW)
            )
    unioned = spark.sql(" UNION ALL ".join(selects))
    unioned.createOrReplaceTempView("pre_failure_signal_counts")
    return spark.sql(
        "SELECT smart_attribute, window_days, failed_serials, signalled_serials, "
        "CASE WHEN failed_serials > 0 THEN signalled_serials / failed_serials ELSE 0.0 END AS signal_rate "
        "FROM pre_failure_signal_counts ORDER BY smart_attribute, window_days"
    )


def build_all(silver_df: DataFrame, columns: Sequence[str]) -> Dict[str, DataFrame]:
    spark = silver_df.sparkSession
    register_views(silver_df)
    return {
        "afr_by_model": build_afr(silver_df, "model"),
        "afr_by_manufacturer": build_afr(silver_df, "manufacturer"),
        "smart_distribution": build_smart_distribution(spark, columns),
        "pre_failure_signal": build_pre_failure_signal(spark, columns),
    }


def run(
    spark: SparkSession,
    silver_path: str,
    analytics_path: str,
    columns: Sequence[str],
    export_dir: Any = None,
) -> Dict[str, Any]:
    """Write the HD6 tables to Gold analytics (one directory per table) and export each
    as a single-file Parquet for the dashboard. Returns row counts and elapsed time."""
    start = time.time()
    export_root = Path(export_dir) if export_dir else DEFAULT_EXPORT_DIR
    tables = build_all(spark.read.parquet(silver_path), columns)

    rows = {}  # type: Dict[str, int]
    for name, df in tables.items():
        df.write.mode("overwrite").parquet("{}/{}".format(analytics_path, name))
        written = spark.read.parquet("{}/{}".format(analytics_path, name))
        written.coalesce(1).write.mode("overwrite").parquet((export_root / (name + ".parquet")).as_uri())
        rows[name] = written.count()
    return {"rows_out": sum(rows.values()), "tables": rows, "elapsed_seconds": round(time.time() - start, 1)}


def main() -> int:
    config = load_config()
    smart_columns = [c for c in config["data"]["required_columns"] if c.startswith("smart_")]

    parser = argparse.ArgumentParser(description="Silver -> Gold analytics (HD6) + export artifacts/dashboard")
    parser.add_argument("--silver-path", default=hdfs_uri(config, "silver"))
    parser.add_argument("--analytics-path", default=hdfs_uri(config, "analytics"))
    parser.add_argument("--export-dir", default=str(DEFAULT_EXPORT_DIR))
    args = parser.parse_args()

    spark = SparkSession.builder.appName("smart-build-analytics").getOrCreate()
    stats = run(spark, args.silver_path, args.analytics_path, smart_columns, args.export_dir)
    print("Analytics stats:", stats)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
