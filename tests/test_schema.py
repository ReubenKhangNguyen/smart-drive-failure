from __future__ import annotations

from pathlib import Path

from processing.spark_jobs.schema_profile import null_rate_report, profile_schema
from processing.spark_jobs.smart_etl import REQUIRED_RAW_COLUMNS, clean_and_align, ensure_required_columns


def _write_csv(path: Path, header: str, rows: list) -> None:
    path.write_text(header + "\n" + "\n".join(rows) + "\n", encoding="utf-8")


FULL_HEADER = "date,serial_number,model,capacity_bytes,failure,smart_5_raw,smart_9_raw,smart_187_raw,smart_188_raw,smart_194_raw,smart_197_raw,smart_198_raw,smart_199_raw"
DRIFTED_HEADER = "date,serial_number,model,capacity_bytes,failure,smart_5_raw,smart_9_raw,smart_197_raw,smart_198_raw,smart_199_raw"


def test_profile_schema_detects_drifted_file(tmp_path: Path, spark):
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()
    _write_csv(bronze_dir / "2026-01-01.csv", FULL_HEADER, ["2026-01-01,SN001,ST4000DM000,4000000000000,0,0,100,0,0,30,0,0,0"])
    _write_csv(bronze_dir / "2026-01-02.csv", DRIFTED_HEADER, ["2026-01-02,SN001,ST4000DM000,4000000000000,0,0,124,0,0,0"])

    profile = profile_schema(spark, "file://{}".format(bronze_dir))

    assert profile.file_count == 2
    assert len(profile.drifted_files) == 1


def test_ensure_required_columns_fills_missing_as_null(spark):
    df = spark.createDataFrame([("2026-01-01", "SN001")], ["date", "serial_number"])
    result = ensure_required_columns(df)

    for col_name in REQUIRED_RAW_COLUMNS:
        assert col_name in result.columns


def test_clean_and_align_produces_hd1_schema(spark):
    rows = [
        ("2026-01-01", "SN001", "ST4000DM000", "4000000000000", "0", "0", "100", "0", "0", "30", "0", "0", "0"),
        ("2026-01-01", "SN002", "WDC WD40", "-1", "0", "0", "50", "0", "0", "25", "0", "0", "0"),
    ]
    df = spark.createDataFrame(rows, REQUIRED_RAW_COLUMNS)

    clean_df, stats = clean_and_align(df)

    expected_columns = {
        "date", "serial_number", "model", "manufacturer", "capacity_bytes", "failure",
        "smart_5_raw", "smart_9_raw", "smart_187_raw", "smart_188_raw",
        "smart_194_raw", "smart_197_raw", "smart_198_raw", "smart_199_raw",
    }
    assert set(clean_df.columns) == expected_columns
    assert stats["rows_in"] == 2
    assert stats["rows_out"] == 2

    by_serial = {row["serial_number"]: row for row in clean_df.collect()}
    assert by_serial["SN001"]["manufacturer"] == "Seagate"
    assert by_serial["SN002"]["manufacturer"] == "Western Digital"
    assert by_serial["SN002"]["capacity_bytes"] is None  # negative capacity nulled out


def test_clean_and_align_deduplicates_by_serial_and_date(spark):
    rows = [
        ("2026-01-01", "SN001", "ST4000DM000", "4000000000000", "0", "0", "100", "0", "0", "30", "0", "0", "0"),
        ("2026-01-01", "SN001", "ST4000DM000", "4000000000000", "0", "0", "101", "0", "0", "30", "0", "0", "0"),
    ]
    df = spark.createDataFrame(rows, REQUIRED_RAW_COLUMNS)

    clean_df, stats = clean_and_align(df)

    assert stats["rows_in"] == 2
    assert stats["rows_out"] == 1
    assert stats["duplicates_removed"] == 1


def test_null_rate_report_computes_fraction(spark):
    from pyspark.sql.types import DoubleType, StructField, StructType

    schema = StructType(
        [
            StructField("smart_5_raw", DoubleType(), True),
            StructField("smart_9_raw", DoubleType(), True),
        ]
    )
    df = spark.createDataFrame(
        [(1.0, None), (None, None), (3.0, None)],
        schema=schema,
    )

    rates = null_rate_report(df, ["smart_5_raw", "smart_9_raw"])

    assert rates["smart_5_raw"] == 1 / 3
    assert rates["smart_9_raw"] == 1.0
