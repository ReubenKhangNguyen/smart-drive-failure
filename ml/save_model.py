from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession

RULES_VERSION = "rules_v1"


def bundle_files(
    feature_cols: List[str],
    model_version: str,
    model_type: str,
    k: int,
    horizon_days: int,
    metrics: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    """The small JSON files that travel with a saved model (docs/contracts.md, HD3), as {file name: text}.

    Same shape as the files of the official model: feature_list.json (the exact input columns, in order),
    threshold.json (K and which model) and, when given, metrics.json (what it scored on validation)."""
    files = {
        "feature_list.json": {"features": list(feature_cols), "horizon_days": horizon_days, "rules_version": RULES_VERSION},
        "threshold.json": {"k": k, "model_version": model_version, "model_type": model_type},
    }
    if metrics is not None:
        files["metrics.json"] = {"model_version": model_version, "chosen_model": model_type, "val": metrics}
    return {name: json.dumps(content, indent=2, ensure_ascii=False) for name, content in files.items()}


def write_text(spark: SparkSession, path: str, text: str) -> None:
    """Write one small text file to HDFS (or file://) through the Hadoop FileSystem the session already has."""
    jvm = spark._jvm
    target = jvm.org.apache.hadoop.fs.Path(path)
    fs = target.getFileSystem(spark._jsc.hadoopConfiguration())
    stream = fs.create(target, True)
    try:
        stream.write(bytearray(text.encode("utf-8")))
    finally:
        stream.close()


def save_model_bundle(
    spark: SparkSession,
    model: PipelineModel,
    feature_cols: List[str],
    path: str,
    model_version: str,
    model_type: str,
    k: int,
    horizon_days: int,
    metrics: Optional[Dict[str, Any]] = None,
) -> List[str]:
    """Save the fitted PipelineModel at `path` plus its JSON files; returns the JSON file names written.

    The caller chooses `path`, so a demo run (config/demo.yaml) saves under /smart-drive-demo and can never
    overwrite the official model."""
    model.write().overwrite().save(path)
    written = []
    for name, text in bundle_files(feature_cols, model_version, model_type, k, horizon_days, metrics).items():
        write_text(spark, "{}/{}".format(path.rstrip("/"), name), text)
        written.append(name)
    return written
