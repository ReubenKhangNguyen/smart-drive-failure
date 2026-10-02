from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "project.yaml"


def hdfs_uri(config: Dict[str, Any], key: str) -> str:
    """Full HDFS URI for a path in the `hdfs:` section (namenode_url + path)."""
    hdfs = config["hdfs"]
    return hdfs["namenode_url"] + hdfs[key]


def enable_dynamic_overwrite(spark: Any) -> None:
    """Make mode("overwrite") + partitionBy only replace the partitions being written
    (default is static, which would delete every other partition)."""
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


def load_config(path: Optional[Path] = None) -> Dict[str, Any]:
    """Load project.yaml (or SMART_DRIVE_CONFIG override) into a dict."""
    config_path = Path(path) if path else Path(os.environ.get("SMART_DRIVE_CONFIG", DEFAULT_CONFIG_PATH))
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
