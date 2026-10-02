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
    path = str(tmp_path / "out")
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
