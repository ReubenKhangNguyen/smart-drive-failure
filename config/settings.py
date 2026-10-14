"""Central paths and constants shared by Spark jobs."""

from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class ProjectPaths:
    """HDFS layer locations and the local directory for compact web exports."""

    bronze: str = os.getenv("BRONZE_PATH", "/smart-drive/bronze/year=2026/quarter=Q1")
    silver: str = os.getenv("SILVER_PATH", "/smart-drive/silver/daily")
    gold: str = os.getenv("GOLD_PATH", "/smart-drive/gold")
    models: str = os.getenv("MODELS_PATH", "/smart-drive/models/v1")
    dashboard_dir: str = os.getenv("DASHBOARD_DIR", "artifacts/dashboard")


FAILURE_HORIZON_DAYS = int(os.getenv("FAILURE_HORIZON_DAYS", "7"))
PRIMARY_METRIC = "pr_auc"
FORBIDDEN_FEATURE_COLUMNS = {"serial_number", "failure", "fail_within_7_days", "failure_date"}
