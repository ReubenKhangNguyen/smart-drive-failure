from __future__ import annotations

import datetime as dt
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

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


# ---------------------------------------------------------------- out-of-time quarters
# The analysis quarter (data.start_date .. data.end_date, Q1) trains and selects the model. Every later quarter is
# only used to check the frozen model once (split 'oot', 'oot2', ...) and must start the day after the previous one.
_SPLIT_PATTERN = re.compile(r"^oot\d*$")
_REQUIRED_QUARTER_KEYS = ("id", "split", "start_date", "end_date")


def _quarter_id(start_date: str) -> str:
    day = dt.date.fromisoformat(start_date)
    return "{}-Q{}".format(day.year, (day.month - 1) // 3 + 1)


def quarters_from_data(data_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """The out-of-time quarters of the `data:` section, validated and normalised, in time order.

    Reads `data.quarters`; when it is absent, falls back to the original single-quarter keys
    (oot_start_date / oot_end_date / oot_bronze) so older configs keep working. Returns [] when neither exists.
    """
    items = data_cfg.get("quarters")
    if not items:
        if not (data_cfg.get("oot_start_date") and data_cfg.get("oot_end_date")):
            return []
        items = [{
            "id": _quarter_id(data_cfg["oot_start_date"]), "split": "oot", "bronze": data_cfg.get("oot_bronze"),
            "start_date": data_cfg["oot_start_date"], "end_date": data_cfg["oot_end_date"],
        }]

    one_day = dt.timedelta(days=1)
    previous_end = dt.date.fromisoformat(data_cfg["end_date"])
    seen_ids, seen_splits, result = set(), set(), []
    for raw in items:
        quarter = dict(raw)
        for key in _REQUIRED_QUARTER_KEYS:
            if key not in quarter:
                raise ValueError("data.quarters: an entry has no '{}': {}".format(key, raw))
        if not _SPLIT_PATTERN.match(str(quarter["split"])):
            raise ValueError("data.quarters: split '{}' of {} must be 'oot', 'oot2', 'oot3', ...".format(quarter["split"], quarter["id"]))
        if quarter["id"] in seen_ids or quarter["split"] in seen_splits:
            raise ValueError("data.quarters: id or split of {} is used twice".format(quarter["id"]))
        start, end = dt.date.fromisoformat(quarter["start_date"]), dt.date.fromisoformat(quarter["end_date"])
        if end < start:
            raise ValueError("data.quarters: {} ends before it starts".format(quarter["id"]))
        if start != previous_end + one_day:
            raise ValueError(
                "data.quarters: {} starts on {} but the previous quarter ends on {}; quarters must be contiguous "
                "(feature windows need the previous days)".format(quarter["id"], start, previous_end))
        quarter.setdefault("bronze", None)
        quarter.setdefault("report", "oot_metrics.json" if quarter["split"] == "oot" else "oot_metrics_{}.json".format(quarter["id"]))
        seen_ids.add(quarter["id"]), seen_splits.add(quarter["split"])
        previous_end = end
        result.append(quarter)
    return result


def quarters(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    return quarters_from_data(config["data"])


def find_quarter(config: Dict[str, Any], quarter_id: Optional[str] = None) -> Dict[str, Any]:
    """One configured out-of-time quarter by id. Without an id it is only unambiguous when exactly one is configured."""
    available = quarters(config)
    if not available:
        raise ValueError("no out-of-time quarter is configured (data.quarters)")
    if quarter_id is None:
        if len(available) > 1:
            raise ValueError("--quarter is required; configured quarters: {}".format(", ".join(q["id"] for q in available)))
        return available[0]
    for quarter in available:
        if quarter["id"] == quarter_id:
            return quarter
    raise ValueError("unknown quarter '{}'; configured quarters: {}".format(quarter_id, ", ".join(q["id"] for q in available)))


def bronze_target(config: Dict[str, Any], quarter_id: Optional[str] = None) -> Dict[str, str]:
    """Where a quarter's raw CSV lives: its first and last day and its Bronze directory on HDFS.

    Without an id it is the analysis quarter (data.start_date .. data.end_date, hdfs.bronze); with an id it is
    one of data.quarters, which must declare its `bronze` path."""
    if quarter_id is None:
        data = config["data"]
        return {"id": "analysis", "start_date": data["start_date"], "end_date": data["end_date"], "path": config["hdfs"]["bronze"]}
    quarter = find_quarter(config, quarter_id)
    if not quarter.get("bronze"):
        raise ValueError("quarter {} has no 'bronze' path in data.quarters".format(quarter["id"]))
    return {"id": quarter["id"], "start_date": quarter["start_date"], "end_date": quarter["end_date"], "path": quarter["bronze"]}
