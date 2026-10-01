from __future__ import annotations

from pathlib import Path

from ingestion.dataset_validator import validate_source_dir

REQUIRED_COLUMNS_HEADER = (
    "date,serial_number,model,capacity_bytes,failure,"
    "smart_5_raw,smart_9_raw,smart_187_raw,smart_188_raw,"
    "smart_194_raw,smart_197_raw,smart_198_raw,smart_199_raw"
)

INCOMPLETE_HEADER = (
    "date,serial_number,model,capacity_bytes,failure,"
    "smart_5_raw,smart_9_raw,smart_197_raw,smart_198_raw,smart_199_raw"
)


def _write_csv(path: Path, header: str, row: str) -> None:
    path.write_text(header + "\n" + row + "\n", encoding="utf-8")


def _build_source_dir(tmp_path: Path) -> Path:
    source_dir = tmp_path / "bronze_sample"
    source_dir.mkdir()
    _write_csv(source_dir / "2026-01-01.csv", REQUIRED_COLUMNS_HEADER, "2026-01-01,SN001,ModelA,1000000000000,0,0,100,0,0,30,0,0,0")
    _write_csv(source_dir / "2026-01-02.csv", REQUIRED_COLUMNS_HEADER, "2026-01-02,SN001,ModelA,1000000000000,0,0,124,0,0,31,0,0,0")
    # 2026-01-03 intentionally missing to test gap detection.
    _write_csv(source_dir / "2026-01-04.csv", INCOMPLETE_HEADER, "2026-01-04,SN001,ModelA,1000000000000,1,3,172,1,0,0")
    return source_dir


def test_counts_files_and_detects_missing_date(tmp_path: Path):
    source_dir = _build_source_dir(tmp_path)
    report = validate_source_dir(source_dir, "2026-01-01", "2026-01-04")

    assert report.total_files == 3
    assert report.missing_dates == ["2026-01-03"]


def test_detects_bad_header_missing_required_columns(tmp_path: Path):
    source_dir = _build_source_dir(tmp_path)
    report = validate_source_dir(source_dir, "2026-01-01", "2026-01-04")

    assert "2026-01-04.csv" in report.bad_header_files
    assert set(report.bad_header_files["2026-01-04.csv"]) == {
        "smart_187_raw",
        "smart_188_raw",
        "smart_194_raw",
    }
    assert "2026-01-01.csv" not in report.bad_header_files


def test_is_valid_false_when_missing_dates_or_bad_headers(tmp_path: Path):
    source_dir = _build_source_dir(tmp_path)
    report = validate_source_dir(source_dir, "2026-01-01", "2026-01-04")
    assert report.is_valid is False


def test_is_valid_true_for_complete_clean_range(tmp_path: Path):
    source_dir = _build_source_dir(tmp_path)
    report = validate_source_dir(source_dir, "2026-01-01", "2026-01-02")
    assert report.is_valid is True
