from __future__ import annotations

import csv
import datetime as dt
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

REQUIRED_COLUMNS = [
    "date",
    "serial_number",
    "model",
    "capacity_bytes",
    "failure",
    "smart_5_raw",
    "smart_9_raw",
    "smart_187_raw",
    "smart_188_raw",
    "smart_194_raw",
    "smart_197_raw",
    "smart_198_raw",
    "smart_199_raw",
]


@dataclass
class ValidationReport:
    total_files: int
    dates_found: List[str]
    missing_dates: List[str]
    bad_header_files: Dict[str, List[str]] = field(default_factory=dict)

    @property
    def is_valid(self) -> bool:
        return not self.missing_dates and not self.bad_header_files


def _read_header(path: Path) -> List[str]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        return next(csv.reader(f))


def _expected_dates(start_date: str, end_date: str) -> List[str]:
    start = dt.date.fromisoformat(start_date)
    end = dt.date.fromisoformat(end_date)
    dates = []
    current = start
    while current <= end:
        dates.append(current.isoformat())
        current += dt.timedelta(days=1)
    return dates


def validate_source_dir(
    source_dir: Path,
    start_date: str,
    end_date: str,
    required_columns: Optional[List[str]] = None,
) -> ValidationReport:
    """Count CSV files by day, find missing days in [start_date, end_date], check header columns.

    Only checks that `required_columns` (per docs/contracts.md HĐ1) are present; extra
    SMART columns present in the source file are allowed (schema drift across quarters).
    """
    required = required_columns or REQUIRED_COLUMNS
    expected = set(_expected_dates(start_date, end_date))
    files = sorted(f for f in Path(source_dir).glob("*.csv") if f.stem in expected)

    dates_found: List[str] = []
    bad_header_files: Dict[str, List[str]] = {}

    for f in files:
        dates_found.append(f.stem)
        header = _read_header(f)
        missing_cols = [c for c in required if c not in header]
        if missing_cols:
            bad_header_files[f.name] = missing_cols

    missing_dates = sorted(expected - set(dates_found))

    return ValidationReport(
        total_files=len(files),
        dates_found=dates_found,
        missing_dates=missing_dates,
        bad_header_files=bad_header_files,
    )
