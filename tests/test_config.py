from __future__ import annotations

from config.settings import enable_dynamic_overwrite, hdfs_uri, load_config

PARTITION_MODE = "spark.sql.sources.partitionOverwriteMode"


def test_hdfs_uri_joins_namenode_url_and_configured_path():
    config = {"hdfs": {"namenode_url": "hdfs://nn:9000", "silver": "/smart-drive/silver/daily"}}

    assert hdfs_uri(config, "silver") == "hdfs://nn:9000/smart-drive/silver/daily"


def test_project_yaml_has_namenode_url_and_no_sample_keys():
    config = load_config()

    assert config["hdfs"]["namenode_url"].startswith("hdfs://")
    assert "pipeline" not in config


def test_dynamic_overwrite_keeps_other_partitions(spark, tmp_path):
    path = (tmp_path / "out").as_uri()  # file:// — the test container defaults to HDFS otherwise
    day1 = spark.createDataFrame([("2026-01-01", 1)], ["date", "v"])
    day2 = spark.createDataFrame([("2026-01-02", 2)], ["date", "v"])

    enable_dynamic_overwrite(spark)
    try:
        day1.write.mode("overwrite").partitionBy("date").parquet(path)
        day2.write.mode("overwrite").partitionBy("date").parquet(path)
        dates = sorted(r["date"] for r in spark.read.parquet(path).select("date").distinct().collect())
    finally:
        spark.conf.unset(PARTITION_MODE)

    assert [str(d) for d in dates] == ["2026-01-01", "2026-01-02"]


# ---------------------------------------------------------------- out-of-time quarters
import pytest

from config.settings import find_quarter, quarters, quarters_from_data

BASE = {"start_date": "2026-01-01", "end_date": "2026-03-31"}
Q2 = {"id": "2026-Q2", "split": "oot", "start_date": "2026-04-01", "end_date": "2026-06-30"}
Q3 = {"id": "2026-Q3", "split": "oot2", "start_date": "2026-07-01", "end_date": "2026-09-30"}


def test_legacy_single_quarter_keys_still_work():
    data = dict(BASE, oot_start_date="2026-04-01", oot_end_date="2026-06-30", oot_bronze="/b/q2")

    result = quarters_from_data(data)

    assert [(q["id"], q["split"], q["bronze"], q["report"]) for q in result] == [("2026-Q2", "oot", "/b/q2", "oot_metrics.json")]


def test_no_out_of_time_quarter_is_an_empty_list():
    assert quarters_from_data(dict(BASE)) == []


def test_quarters_list_is_normalised_with_default_report_names():
    result = quarters_from_data(dict(BASE, quarters=[Q2, Q3]))

    assert [q["split"] for q in result] == ["oot", "oot2"]
    assert [q["report"] for q in result] == ["oot_metrics.json", "oot_metrics_2026-Q3.json"]
    assert result[1]["bronze"] is None


def test_a_gap_between_quarters_is_rejected():
    later = dict(Q3, start_date="2026-07-05")

    with pytest.raises(ValueError, match="contiguous"):
        quarters_from_data(dict(BASE, quarters=[Q2, later]))


def test_first_quarter_must_follow_the_analysis_quarter():
    with pytest.raises(ValueError, match="contiguous"):
        quarters_from_data(dict(BASE, quarters=[dict(Q2, start_date="2026-04-02")]))


@pytest.mark.parametrize("bad", [{"split": "test"}, {"split": "oot2x"}, {"id": "2026-Q2"}])
def test_invalid_or_duplicate_quarter_entries_are_rejected(bad):
    with pytest.raises(ValueError):
        quarters_from_data(dict(BASE, quarters=[Q2, dict(Q3, **bad)]))


def test_missing_required_key_is_rejected():
    broken = {k: v for k, v in Q2.items() if k != "end_date"}

    with pytest.raises(ValueError, match="end_date"):
        quarters_from_data(dict(BASE, quarters=[broken]))


def test_find_quarter_needs_an_id_only_when_ambiguous():
    one = {"data": dict(BASE, quarters=[Q2])}
    two = {"data": dict(BASE, quarters=[Q2, Q3])}

    assert find_quarter(one)["id"] == "2026-Q2"
    assert find_quarter(two, "2026-Q3")["split"] == "oot2"
    with pytest.raises(ValueError, match="--quarter is required"):
        find_quarter(two)
    with pytest.raises(ValueError, match="unknown quarter"):
        find_quarter(two, "2027-Q1")


def test_shipped_config_declares_q2_as_the_first_out_of_time_quarter():
    result = quarters(load_config())

    assert result[0]["id"] == "2026-Q2" and result[0]["split"] == "oot" and result[0]["report"] == "oot_metrics.json"


from config.settings import bronze_target

_CONFIG = {"hdfs": {"bronze": "/b/q1"}, "data": dict(BASE, quarters=[dict(Q2, bronze="/b/q2"), Q3])}


def test_bronze_target_defaults_to_the_analysis_quarter():
    assert bronze_target(_CONFIG) == {"id": "analysis", "start_date": "2026-01-01", "end_date": "2026-03-31", "path": "/b/q1"}


def test_bronze_target_of_a_configured_quarter():
    assert bronze_target(_CONFIG, "2026-Q2") == {"id": "2026-Q2", "start_date": "2026-04-01", "end_date": "2026-06-30", "path": "/b/q2"}


def test_bronze_target_needs_a_bronze_path_and_a_known_quarter():
    with pytest.raises(ValueError, match="no 'bronze' path"):
        bronze_target(_CONFIG, "2026-Q3")
    with pytest.raises(ValueError, match="unknown quarter"):
        bronze_target(_CONFIG, "2030-Q1")
