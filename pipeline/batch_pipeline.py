from __future__ import annotations

import time
from typing import Any, Callable, Dict, List, Tuple

from config.settings import enable_dynamic_overwrite, hdfs_uri

PipelineStep = Tuple[str, Callable[[], Dict[str, Any]]]


def run_steps(steps: List[PipelineStep]) -> Dict[str, Any]:
    """Run (name, callable) steps in order, stop on the first exception, and log the
    elapsed time of every step that ran (including the one that failed)."""
    log: List[Dict[str, Any]] = []
    for name, step in steps:
        start = time.time()
        try:
            result = step()
        except Exception as exc:  # noqa: BLE001 - re-raised after logging which step failed
            log.append({"step": name, "elapsed_seconds": round(time.time() - start, 2), "status": "failed", "error": str(exc)})
            return {"steps": log, "status": "failed", "failed_step": name}
        log.append({"step": name, "elapsed_seconds": round(time.time() - start, 2), "status": "ok", "result": result})
    return {"steps": log, "status": "ok"}


def build_steps(spark, config: Dict[str, Any]) -> List[PipelineStep]:
    """Wire up the real Bronze->Silver->Gold steps (Phase 3-5). Never deletes
    existing Silver/Gold (CLAUDE.md luat 3): writes use mode("overwrite") with
    partitionOverwriteMode=dynamic, so re-running only overwrites the partitions
    this run touches. There is deliberately no sample mode: a partial run would
    write into the real Gold paths (see docs/decisions.md, 2026-10-02)."""
    from pyspark.sql import functions as F

    from analytics.health_status import run as run_health_status
    from features.build_features import build_features, feature_columns
    from features.label import assign_split, build_labeled_dataset
    from processing.spark_jobs.smart_etl import run as run_smart_etl

    data_cfg = config["data"]
    bronze_path = hdfs_uri(config, "bronze")
    silver_path = hdfs_uri(config, "silver")
    features_path = hdfs_uri(config, "features")
    health_status_path = hdfs_uri(config, "health_status")

    def step_silver():
        return run_smart_etl(spark, bronze_path, silver_path)

    def step_health_status():
        return run_health_status(spark, silver_path, health_status_path)

    def step_features():
        enable_dynamic_overwrite(spark)
        silver_df = spark.read.parquet(silver_path)

        labeled = build_labeled_dataset(silver_df, data_cfg["end_date"], config["project"]["horizon_days"])
        split_df = assign_split(
            labeled,
            warmup_end_date=data_cfg["warmup_end_date"],
            train_end_date=data_cfg["train_end_date"],
            val_end_date=data_cfg["val_end_date"],
        )
        train_df = split_df.where(F.col("split") == "train")
        features_df = build_features(split_df, train_df)

        cols = feature_columns(features_df)
        final_df = features_df.select("date", "serial_number", "model", *cols, "fail_within_7_days", "split")
        final_df.write.mode("overwrite").partitionBy("date").parquet(features_path)
        return {"rows": final_df.count()}

    return [
        ("silver_etl", step_silver),
        ("health_status", step_health_status),
        ("features", step_features),
    ]
