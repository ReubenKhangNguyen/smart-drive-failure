from __future__ import annotations

from typing import Any, Dict, Optional

from config.settings import load_config


def get_hdfs_paths(config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Return the hdfs: section of project.yaml (Bronze/Silver/Gold paths and replication)."""
    cfg = config or load_config()
    return cfg["hdfs"]
