from __future__ import annotations

from typing import Any, Dict, Optional

from config.settings import load_config


def build_spark_session_options(config: Optional[Dict[str, Any]] = None, app_name: str = "smart-drive-failure") -> Dict[str, str]:
    """Return SparkSession config options for cluster jobs (HDFS access, dynamic partition overwrite)."""
    cfg = config or load_config()
    return {
        "spark.app.name": app_name,
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.sql.session.timeZone": "UTC",
    }
