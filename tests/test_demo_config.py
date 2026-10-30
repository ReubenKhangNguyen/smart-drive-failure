from __future__ import annotations

from pathlib import Path

from config.settings import load_config, quarters

ROOT = Path(__file__).resolve().parent.parent
REAL = load_config(ROOT / "config" / "project.yaml")
DEMO = load_config(ROOT / "config" / "demo.yaml")


def test_every_hdfs_path_of_the_real_config_is_redirected_to_the_demo_area():
    """A path added to project.yaml but forgotten in demo.yaml would make a demo run write into the REAL data."""
    for key, value in REAL["hdfs"].items():
        if isinstance(value, str) and value.startswith("/smart-drive/"):
            assert DEMO["hdfs"][key].startswith("/smart-drive-demo/"), key
            assert DEMO["hdfs"][key] != value, key


def test_the_demo_does_not_share_a_model_name_with_the_official_model():
    assert DEMO["ml"]["model_version"] != REAL["ml"]["model_version"]
    assert DEMO["hdfs"]["models_base"] != REAL["hdfs"]["models_base"]


def test_the_demo_keeps_the_cluster_and_the_column_contract_of_the_real_config():
    assert DEMO["hdfs"]["namenode_url"] == REAL["hdfs"]["namenode_url"]
    assert DEMO["data"]["required_columns"] == REAL["data"]["required_columns"]
    assert DEMO["kmeans"]["columns"] == REAL["kmeans"]["columns"]
    assert DEMO["project"]["horizon_days"] == REAL["project"]["horizon_days"] and DEMO["project"]["random_seed"] == REAL["project"]["random_seed"]


def test_the_demo_calendar_is_ordered_and_has_no_out_of_time_quarter():
    d = DEMO["data"]
    assert d["start_date"] < d["warmup_end_date"] < d["train_end_date"] < d["val_end_date"] < d["end_date"]
    assert quarters(DEMO) == []  # the real config still lists Q2; the demo must not inherit it
    assert len(quarters(REAL)) >= 1


def test_loading_the_real_config_is_unchanged_by_the_base_mechanism():
    assert REAL["hdfs"]["bronze"] == "/smart-drive/bronze/year=2026/quarter=Q1" and "base" not in REAL
