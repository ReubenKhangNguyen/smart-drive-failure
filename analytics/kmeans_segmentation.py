from __future__ import annotations

import argparse
import datetime as dt
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from analytics.build_analytics import DEFAULT_EXPORT_DIR, FAILED_VIEW, SILVER_VIEW, register_views, write_table
from config.settings import hdfs_uri, load_config

SNAPSHOT_VIEW = "drive_snapshot"
CLUSTERS_VIEW = "drive_clusters"
HEALTH_VIEW = "health_status_view"
DEFAULT_REPORT_DIR = Path(__file__).resolve().parent.parent / "artifacts" / "reports"
MISSING_COLUMN = "smart_187_raw"  # the attribute whose null share is reported per cluster

LIMITATIONS = (
    "Phân tích mô tả hồi cứu, mẫu lệch ngày (ổ khỏe lấy ngày quan sát cuối quý, ổ hỏng lấy ngày liền trước "
    "ngày hỏng). Không so sánh trực tiếp với recall@K của LR; `failed_rate` không phải xác suất hỏng. "
    "Cluster id không được dùng làm đặc trưng của model dự đoán."
)


def build_snapshot(silver_df: DataFrame, columns: Sequence[str]) -> DataFrame:
    """One row per drive: healthy = last observed day of never-failed serials, failed = the day
    before `failure_date` (same cohorts as HD6 smart_distribution). Null SMART values are filled
    with 0 (assumption: a missing attribute ~ no signal); `smart_187_missing` keeps the null flag."""
    spark = silver_df.sparkSession
    register_views(silver_df)
    value_cols = ", ".join("CAST(COALESCE(s.{c}, 0) AS DOUBLE) AS {c}".format(c=c) for c in columns)
    healthy = (
        "SELECT s.serial_number, s.date, FALSE AS is_failed, {values}, "
        "(s.{missing} IS NULL) AS smart_187_missing "
        "FROM {silver} s "
        "JOIN (SELECT s2.serial_number, MAX(s2.date) AS last_date FROM {silver} s2 "
        "      LEFT ANTI JOIN {failed} f2 ON s2.serial_number = f2.serial_number "
        "      GROUP BY s2.serial_number) m "
        "  ON s.serial_number = m.serial_number AND s.date = m.last_date"
    )
    failed = (
        "SELECT s.serial_number, s.date, TRUE AS is_failed, {values}, "
        "(s.{missing} IS NULL) AS smart_187_missing "
        "FROM {silver} s JOIN {failed} f ON s.serial_number = f.serial_number "
        "WHERE s.date = date_sub(f.failure_date, 1)"
    )
    query = " UNION ALL ".join([healthy, failed]).format(
        values=value_cols, silver=SILVER_VIEW, failed=FAILED_VIEW, missing=MISSING_COLUMN
    )
    return spark.sql(query)


def split_zero_signal(snapshot: DataFrame, columns: Sequence[str]) -> Tuple[DataFrame, DataFrame]:
    """(zero_signal, with_signal): zero_signal = all clustering columns are 0."""
    all_zero = F.lit(True)
    for c in columns:
        all_zero = all_zero & (F.col(c) == 0)
    return snapshot.where(all_zero), snapshot.where(~all_zero)


def fit_and_select(
    with_signal: DataFrame,
    columns: Sequence[str],
    k_min: int,
    k_max: int,
    seed: int,
    max_iter: int,
) -> Tuple[DataFrame, List[Dict[str, Any]], int]:
    """log1p -> StandardScaler -> KMeans for K in [k_min, k_max]; pick the K with the highest
    silhouette (ties -> smaller K). Returns (rows with raw_cluster, per-K table, selected K)."""
    logged = with_signal.select("*", *[F.log1p(F.col(c)).alias("log_" + c) for c in columns])
    assembled = VectorAssembler(inputCols=["log_" + c for c in columns], outputCol="raw_features").transform(logged)
    scaled = (
        StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)
        .fit(assembled)
        .transform(assembled)
        .cache()
    )
    num_points = scaled.count()
    evaluator = ClusteringEvaluator(
        featuresCol="features", predictionCol="raw_cluster", metricName="silhouette", distanceMeasure="squaredEuclidean"
    )

    table = []  # type: List[Dict[str, Any]]
    best = None  # type: Optional[Tuple[float, int, DataFrame]]
    for k in range(k_min, k_max + 1):
        if num_points < k:
            break
        model = KMeans(k=k, seed=seed, maxIter=max_iter, featuresCol="features", predictionCol="raw_cluster").fit(scaled)
        predictions = model.transform(scaled)
        silhouette = float(evaluator.evaluate(predictions))
        table.append({"k": k, "silhouette": silhouette, "wssse": float(model.summary.trainingCost)})
        if best is None or silhouette > best[0]:
            best = (silhouette, k, predictions)
    if best is None:
        raise ValueError("Not enough drives with a signal ({}) for K >= {}".format(num_points, k_min))

    selected = best[1]
    for row in table:
        row["is_selected"] = row["k"] == selected
    return best[2].drop("raw_features", "features", *["log_" + c for c in columns]), table, selected


def relabel_clusters(clustered: DataFrame, columns: Sequence[str]) -> DataFrame:
    """Map raw KMeans ids to 1..K ordered by mean smart_197_raw ascending (ties: smart_5, smart_187,
    smart_198 means, then raw id) so cluster ids are stable between runs."""
    spark = clustered.sparkSession
    order_cols = [c for c in ("smart_197_raw", "smart_5_raw", "smart_187_raw", "smart_198_raw") if c in columns]
    means = clustered.groupBy("raw_cluster").agg(*[F.avg(c).alias(c) for c in order_cols]).collect()
    ranked = sorted(means, key=lambda r: tuple(round(r[c], 9) for c in order_cols) + (r["raw_cluster"],))
    mapping = spark.createDataFrame([(r["raw_cluster"], i + 1) for i, r in enumerate(ranked)], ["raw_cluster", "cluster_id"])
    return (
        clustered.join(F.broadcast(mapping), "raw_cluster")
        .drop("raw_cluster")
        .withColumn("cluster_type", F.lit("kmeans"))
    )


def build_drive_clusters(zero_signal: DataFrame, clustered: DataFrame) -> DataFrame:
    zero = zero_signal.withColumn("cluster_type", F.lit("zero_signal")).withColumn("cluster_id", F.lit(0))
    return zero.unionByName(clustered).select(
        "serial_number", "date", "is_failed", *[c for c in zero.columns if c.startswith("smart_")],
        "cluster_type", F.col("cluster_id").cast("int").alias("cluster_id"),
    )


def build_cluster_summary(drive_clusters: DataFrame, columns: Sequence[str]) -> DataFrame:
    spark = drive_clusters.sparkSession
    drive_clusters.createOrReplaceTempView(CLUSTERS_VIEW)
    means = ", ".join("AVG({c}) AS mean_{c}".format(c=c) for c in columns)
    return spark.sql(
        "SELECT cluster_type, cluster_id, CAST(COUNT(*) AS BIGINT) AS size, "
        "CAST(SUM(CASE WHEN is_failed THEN 0 ELSE 1 END) AS BIGINT) AS healthy_count, "
        "CAST(SUM(CASE WHEN is_failed THEN 1 ELSE 0 END) AS BIGINT) AS failed_count, "
        "CAST(SUM(CASE WHEN is_failed THEN 1 ELSE 0 END) / COUNT(*) AS DOUBLE) AS failed_rate, "
        "{means}, AVG(CASE WHEN smart_187_missing THEN CAST(1 AS DOUBLE) ELSE CAST(0 AS DOUBLE) END) AS smart_187_missing_share "
        "FROM {view} GROUP BY cluster_type, cluster_id ORDER BY cluster_id".format(means=means, view=CLUSTERS_VIEW)
    )


def build_health_crosstab(drive_clusters: DataFrame, health_df: DataFrame) -> DataFrame:
    spark = drive_clusters.sparkSession
    drive_clusters.createOrReplaceTempView(CLUSTERS_VIEW)
    health_df.createOrReplaceTempView(HEALTH_VIEW)
    return spark.sql(
        "SELECT c.cluster_id, h.health_level, CAST(COUNT(*) AS BIGINT) AS drives "
        "FROM {clusters} c JOIN {health} h ON c.serial_number = h.serial_number AND c.date = h.date "
        "GROUP BY c.cluster_id, h.health_level ORDER BY c.cluster_id, h.health_level".format(
            clusters=CLUSTERS_VIEW, health=HEALTH_VIEW
        )
    )


def segment(
    silver_df: DataFrame,
    health_df: DataFrame,
    columns: Sequence[str],
    k_min: int,
    k_max: int,
    seed: int,
    max_iter: int,
) -> Dict[str, Any]:
    """Pure pipeline (no writes). Returns the three HD7 DataFrames plus run stats."""
    spark = silver_df.sparkSession
    snapshot = build_snapshot(silver_df, columns).cache()
    zero_signal, with_signal = split_zero_signal(snapshot, columns)
    clustered, k_rows, selected_k = fit_and_select(with_signal, columns, k_min, k_max, seed, max_iter)
    drive_clusters = build_drive_clusters(zero_signal, relabel_clusters(clustered, columns)).cache()

    k_table = spark.createDataFrame(
        [(r["k"], r["silhouette"], r["wssse"], r["is_selected"]) for r in k_rows],
        "k int, silhouette double, wssse double, is_selected boolean",
    )
    summary = build_cluster_summary(drive_clusters, columns).cache()
    crosstab = build_health_crosstab(drive_clusters, health_df)

    total_failed = spark.table(FAILED_VIEW).count()
    summary_rows = summary.collect()
    snapshot_failed = sum(r["failed_count"] for r in summary_rows)
    zero_row = [r for r in summary_rows if r["cluster_type"] == "zero_signal"]
    stats = {
        "selected_k": selected_k,
        "drives_in_snapshot": sum(r["size"] for r in summary_rows),
        "failed_serials_total": total_failed,
        "failed_in_snapshot": snapshot_failed,
        "failed_excluded_no_prior_day": total_failed - snapshot_failed,
        "zero_signal_size": zero_row[0]["size"] if zero_row else 0,
        "failed_in_zero_signal": zero_row[0]["failed_count"] if zero_row else 0,
    }
    return {"k_selection": k_table, "clusters": summary, "crosstab": crosstab, "stats": stats, "k_rows": k_rows}


def write_report(result: Dict[str, Any], summary_rows: List[Any], report_dir: Path, run_date: str) -> Path:
    stats = result["stats"]
    lines = [
        "# Phân cụm K-Means ổ cứng — {}\n".format(run_date),
        "Giới hạn: {}\n".format(LIMITATIONS),
        "- K được chọn (silhouette lớn nhất trong khoảng thử): **{}**".format(stats["selected_k"]),
        "- Số ổ trong mẫu: {:,} (ổ hỏng {:,} / tổng ổ hỏng {:,}; **{:,} ổ hỏng bị loại** vì không có dòng ngày liền trước ngày hỏng)".format(
            stats["drives_in_snapshot"], stats["failed_in_snapshot"], stats["failed_serials_total"],
            stats["failed_excluded_no_prior_day"]),
        "- Nhóm `zero_signal` (cả 4 chỉ số = 0, không đưa vào K-Means): {:,} ổ, trong đó **{:,} ổ hỏng** (giới hạn của cách tiếp cận dựa trên 4 chỉ số này, dùng cho phần phân tích lỗi)\n".format(
            stats["zero_signal_size"], stats["failed_in_zero_signal"]),
        "| K | Silhouette | WSSSE | Chọn |", "|---|---|---|---|",
    ]
    for r in result["k_rows"]:
        lines.append("| {} | {:.4f} | {:.1f} | {} |".format(r["k"], r["silhouette"], r["wssse"], "x" if r["is_selected"] else ""))
    lines += ["", "| Cụm | Loại | Số ổ | Khỏe | Hỏng | failed_rate | smart_5 TB | smart_187 TB | smart_197 TB | smart_198 TB | smart_187 null |",
              "|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in summary_rows:
        lines.append("| {} | {} | {:,} | {:,} | {:,} | {:.4f} | {:.1f} | {:.1f} | {:.1f} | {:.1f} | {:.1%} |".format(
            r["cluster_id"], r["cluster_type"], r["size"], r["healthy_count"], r["failed_count"], r["failed_rate"],
            r["mean_smart_5_raw"], r["mean_smart_187_raw"], r["mean_smart_197_raw"], r["mean_smart_198_raw"],
            r["smart_187_missing_share"]))
    report_dir.mkdir(parents=True, exist_ok=True)
    path = report_dir / "kmeans_segmentation.md"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def run(
    spark: SparkSession,
    silver_path: str,
    health_status_path: str,
    analytics_path: str,
    columns: Sequence[str],
    k_min: int,
    k_max: int,
    seed: int,
    max_iter: int,
    export_dir: Any = None,
    report_dir: Any = None,
    run_date: Optional[str] = None,
) -> Dict[str, Any]:
    """Write the three HD7 tables to Gold analytics, export them for the dashboard, and write
    artifacts/reports/kmeans_segmentation.md. Descriptive only: never touches features/model."""
    start = time.time()
    export_root = Path(export_dir) if export_dir else DEFAULT_EXPORT_DIR
    result = segment(spark.read.parquet(silver_path), spark.read.parquet(health_status_path),
                     columns, k_min, k_max, seed, max_iter)
    rows = {
        "kmeans_k_selection": write_table(spark, result["k_selection"], analytics_path, "kmeans_k_selection", export_root),
        "kmeans_clusters": write_table(spark, result["clusters"], analytics_path, "kmeans_clusters", export_root),
        "kmeans_health_crosstab": write_table(spark, result["crosstab"], analytics_path, "kmeans_health_crosstab", export_root),
    }
    summary_rows = spark.read.parquet("{}/kmeans_clusters".format(analytics_path)).orderBy("cluster_id").collect()
    report = write_report(result, summary_rows, Path(report_dir) if report_dir else DEFAULT_REPORT_DIR,
                          run_date or dt.date.today().isoformat())
    stats = dict(result["stats"])
    stats.update({"tables": rows, "report": str(report), "elapsed_seconds": round(time.time() - start, 1)})
    return stats


def main() -> int:
    config = load_config()
    km = config["kmeans"]

    parser = argparse.ArgumentParser(description="Phan cum K-Means o cung (HD7), chi mo ta, khong dua vao model")
    parser.add_argument("--silver-path", default=hdfs_uri(config, "silver"))
    parser.add_argument("--health-status-path", default=hdfs_uri(config, "health_status"))
    parser.add_argument("--analytics-path", default=hdfs_uri(config, "analytics"))
    parser.add_argument("--export-dir", default=str(DEFAULT_EXPORT_DIR))
    args = parser.parse_args()

    spark = SparkSession.builder.appName("smart-kmeans-segmentation").getOrCreate()
    stats = run(spark, args.silver_path, args.health_status_path, args.analytics_path, km["columns"],
                km["k_min"], km["k_max"], config["project"]["random_seed"], km["max_iter"], args.export_dir)
    print("KMeans stats:", stats)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
