from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pyspark.ml import PipelineModel
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.feature import StandardScalerModel
from pyspark.ml.stat import Summarizer
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from analytics.build_analytics import DEFAULT_EXPORT_DIR
from analytics.health_status import WATCH_COLUMNS, baseline_failure_rate_by_level
from config.settings import hdfs_uri, load_config
from ml.train import add_class_weight

DEFAULT_REPORTS_DIR = Path(__file__).resolve().parent.parent / "artifacts" / "reports"
HISTORY_DAYS = 30
TOP_N_FEATURES = 15
RF_IMPORTANCE_FILE = "rf_feature_importance_phase6.json"
RF_SOURCE = "RF, một lần chạy Phase 6, không tái lập"

OVERVIEW_SCHEMA = (
    "data_start date, data_end date, drive_days bigint, drive_count bigint, failure_count bigint, afr double, "
    "model_version string, k int, scored_date date, scored_rows bigint"
)
METRICS_SCHEMA = (
    "split string, segment string, model string, pr_auc double, roc_auc double, recall_at_k double, "
    "precision_at_k double, k int, rows bigint, positives bigint"
)
IMPORTANCE_SCHEMA = "model string, rank int, feature string, importance double, source string"


# ---------- Spark-side tables ----------

def build_overview(silver_df: DataFrame, predictions_df: DataFrame, model_version: str, k: int) -> DataFrame:
    spark = silver_df.sparkSession
    silver_df.createOrReplaceTempView("silver_daily")
    row = spark.sql(
        "SELECT MIN(date) AS data_start, MAX(date) AS data_end, COUNT(*) AS drive_days, "
        "COUNT(DISTINCT serial_number) AS drive_count, CAST(SUM(failure) AS BIGINT) AS failure_count "
        "FROM silver_daily"
    ).collect()[0]
    scored_date = predictions_df.agg(F.max("date")).collect()[0][0]
    scored_rows = predictions_df.where(F.col("date") == scored_date).count()
    afr = row["failure_count"] / (row["drive_days"] / 365.0) if row["drive_days"] else 0.0
    return spark.createDataFrame(
        [(row["data_start"], row["data_end"], row["drive_days"], row["drive_count"], row["failure_count"], float(afr),
          model_version, int(k), scored_date, scored_rows)],
        OVERVIEW_SCHEMA,
    )


def build_health_distribution(silver_df: DataFrame, health_df: DataFrame) -> DataFrame:
    spark = health_df.sparkSession
    health_df.createOrReplaceTempView("health_status_all")
    counts = spark.sql(
        "SELECT health_level, CAST(COUNT(*) AS BIGINT) AS drive_days, "
        "CAST(COUNT(*) / SUM(COUNT(*)) OVER () AS DOUBLE) AS share "
        "FROM health_status_all GROUP BY health_level"
    )
    baseline = baseline_failure_rate_by_level(silver_df, health_df).select(
        "health_level",
        F.col("total_rows").cast("bigint").alias("labeled_rows"),
        F.col("failed_within_window").cast("bigint").alias("failed_within_7d"),
        F.col("failure_rate").cast("double").alias("failure_rate_7d"),
    )
    return counts.join(baseline, "health_level", "left").select(
        "health_level", "drive_days", "share", "labeled_rows", "failed_within_7d", "failure_rate_7d"
    ).orderBy("health_level")


def build_health_snapshot(health_df: DataFrame, scored_date: Any) -> DataFrame:
    return (
        health_df.where((F.col("date") == scored_date) & F.col("health_level").isin("WATCH", "CRITICAL"))
        .select("date", "serial_number", "model", "health_level", "reasons")
    )


def build_predictions_topk(predictions_df: DataFrame, health_df: DataFrame, scored_date: Any) -> DataFrame:
    top = predictions_df.where((F.col("date") == scored_date) & F.col("alert"))
    health = health_df.where(F.col("date") == scored_date).select("serial_number", "date", "health_level", "reasons")
    return (
        top.join(health, ["serial_number", "date"], "left")
        .select("date", "risk_rank", "serial_number", "model", "risk_score", "alert", "health_level", "reasons", "model_version")
        .orderBy("risk_rank")
    )


def build_smart_history(silver_df: DataFrame, topk_df: DataFrame, scored_date: Any, days: int = HISTORY_DAYS) -> DataFrame:
    serials = topk_df.select("serial_number").distinct()
    window = silver_df.where(
        (F.col("date") <= F.lit(scored_date)) & (F.col("date") > F.date_sub(F.lit(scored_date), days))
    )
    return (
        window.join(F.broadcast(serials), "serial_number")
        .select("serial_number", "date", *[F.col(c).cast("bigint").alias(c) for c in WATCH_COLUMNS])
        .orderBy("serial_number", "date")
    )


# ---------- pure-Python tables (metrics, importance) ----------

def model_metrics_rows(metrics: Dict[str, Any], breakdown: Dict[str, Any]) -> List[Tuple[Any, ...]]:
    """Rows for `model_metrics` (HD8). recall@K / precision@K of the full test are deliberately
    NOT published (null): the 'tail' segment (right-censoring, 288/288 positives) dominates them;
    the headline test number is the 'normal' segment."""
    k = int(metrics["K"])
    rows = []  # type: List[Tuple[Any, ...]]
    for model, vals in metrics["val"].items():
        rows.append(("val", "all", model, vals.get("pr_auc"), vals.get("roc_auc"),
                     vals.get("recall_at_k"), vals.get("precision_at_k"), k, None, None))
    for model, vals in metrics["test"]["models"].items():
        rows.append(("test", "full", model, vals.get("pr_auc"), vals.get("roc_auc"), None, None, k,
                     breakdown[model]["full_test"]["rows"], breakdown[model]["full_test"]["positives"]))
    rows.append(("test", "full", "baseline_rules_v1", None, None, None, None, k,
                 breakdown["baseline_rules_v1"]["full_test"]["rows"], breakdown["baseline_rules_v1"]["full_test"]["positives"]))
    for model, parts in breakdown.items():
        for key, segment in (("normal_range", "normal"), ("tail_range", "tail")):
            part = parts[key]
            rows.append(("test", segment, model, None, None, part["recall_at_k"], part["precision_at_k"], k,
                         part["rows"], part["positives"]))
    return rows


def lr_importance_rows(
    model: PipelineModel, train_df: DataFrame, model_version: str, top_n: int = TOP_N_FEATURES
) -> List[Tuple[Any, ...]]:
    """Top-N |coefficient| of the saved LR on STANDARDIZED features. The saved pipeline is
    [VectorAssembler, LogisticRegression] with standardization=True inside LR, so
    `coefficients` are on the original feature scale; multiply by the weighted std of each
    feature on the train split (same Summarizer statistic Spark uses) to make them comparable.
    If a scaler stage exists before LR the coefficients are already standardized."""
    assembler, classifier = model.stages[0], model.stages[-1]
    if not isinstance(classifier, LogisticRegressionModel):
        raise ValueError("Last stage is not a LogisticRegressionModel")
    names = assembler.getInputCols()
    coefficients = classifier.coefficients.toArray()

    has_scaler = any(isinstance(stage, StandardScalerModel) for stage in model.stages[:-1])
    if has_scaler:
        stds = [1.0] * len(names)
    else:
        weighted = add_class_weight(train_df)
        assembled = assembler.transform(weighted)
        summary = assembled.select(Summarizer.metrics("std").summary(F.col("features"), F.col("class_weight")).alias("s"))
        stds = [float(x) for x in summary.collect()[0]["s"]["std"].toArray()]

    scored = sorted(
        ((name, abs(float(coef) * std)) for name, coef, std in zip(names, coefficients, stds)),
        key=lambda item: (-item[1], item[0]),
    )[:top_n]
    source = "LR {}: |hệ số| × độ lệch chuẩn có trọng số trên train (đặc trưng chuẩn hóa)".format(model_version)
    return [("logistic_regression", i + 1, name, value, source) for i, (name, value) in enumerate(scored)]


def rf_importance_rows(reports_dir: Path, top_n: int = TOP_N_FEATURES) -> List[Tuple[Any, ...]]:
    """Phase 6 RF top features, read from artifacts/reports (transcribed once from docs/evaluation.md
    section 4 because the RF model itself was never saved). Missing file -> no RF rows."""
    path = Path(reports_dir) / RF_IMPORTANCE_FILE
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    items = sorted(data["features"], key=lambda item: item["rank"])[:top_n]
    return [("random_forest", int(i["rank"]), i["feature"], float(i["importance"]), RF_SOURCE) for i in items]


# ---------- writing ----------

def export_table(spark: SparkSession, df: DataFrame, name: str, export_root: Path) -> int:
    uri = (export_root / (name + ".parquet")).as_uri()
    df.coalesce(1).write.mode("overwrite").parquet(uri)
    return spark.read.parquet(uri).count()


def run(
    spark: SparkSession,
    silver_path: str,
    health_status_path: str,
    predictions_path: str,
    features_path: str,
    model_path: str,
    model_version: str,
    k: int,
    reports_dir: Any = None,
    export_dir: Any = None,
) -> Dict[str, Any]:
    start = time.time()
    reports = Path(reports_dir) if reports_dir else DEFAULT_REPORTS_DIR
    export_root = Path(export_dir) if export_dir else DEFAULT_EXPORT_DIR

    silver_df = spark.read.parquet(silver_path)
    health_df = spark.read.parquet(health_status_path)
    predictions_df = spark.read.parquet(predictions_path)
    scored_date = predictions_df.agg(F.max("date")).collect()[0][0]

    topk_df = build_predictions_topk(predictions_df, health_df, scored_date).cache()
    tables = {
        "dashboard_overview": build_overview(silver_df, predictions_df, model_version, k),
        "health_distribution": build_health_distribution(silver_df, health_df),
        "health_snapshot": build_health_snapshot(health_df, scored_date),
        "predictions_topk": topk_df,
        "smart_history_topk": build_smart_history(silver_df, topk_df, scored_date),
    }

    skipped = []  # type: List[str]
    metrics_path, breakdown_path = reports / "metrics.json", reports / "test_breakdown.json"
    if metrics_path.exists() and breakdown_path.exists():
        rows = model_metrics_rows(json.loads(metrics_path.read_text(encoding="utf-8")),
                                  json.loads(breakdown_path.read_text(encoding="utf-8")))
        tables["model_metrics"] = spark.createDataFrame(rows, METRICS_SCHEMA)
    else:
        skipped.append("model_metrics")

    train_df = spark.read.parquet(features_path).where(F.col("split") == "train")
    importance = lr_importance_rows(PipelineModel.load(model_path), train_df, model_version) + rf_importance_rows(reports)
    tables["model_feature_importance"] = spark.createDataFrame(importance, IMPORTANCE_SCHEMA)

    counts = {name: export_table(spark, df, name, export_root) for name, df in tables.items()}
    topk_df.unpersist()
    return {"scored_date": str(scored_date), "tables": counts, "skipped": skipped,
            "rf_rows": sum(1 for r in importance if r[0] == "random_forest"),
            "elapsed_seconds": round(time.time() - start, 1)}


def main() -> int:
    config = load_config()
    parser = argparse.ArgumentParser(description="Export cac bang nho cho dashboard (HD8) vao artifacts/dashboard")
    parser.add_argument("--export-dir", default=str(DEFAULT_EXPORT_DIR))
    parser.add_argument("--reports-dir", default=str(DEFAULT_REPORTS_DIR))
    args = parser.parse_args()

    version = config["ml"]["model_version"]
    spark = SparkSession.builder.appName("smart-export-dashboard").getOrCreate()
    stats = run(
        spark,
        hdfs_uri(config, "silver"),
        hdfs_uri(config, "health_status"),
        hdfs_uri(config, "predictions"),
        hdfs_uri(config, "features"),
        "{}/{}".format(hdfs_uri(config, "models_base"), version),
        version,
        config["ml"]["k"],
        args.reports_dir,
        args.export_dir,
    )
    print("Dashboard export stats:", stats)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
