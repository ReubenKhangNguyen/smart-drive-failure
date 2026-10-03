"""Read-only check that Bronze holds one non-empty CSV per day of the configured range (WebHDFS LISTSTATUS).

This VERIFIES, it does not ingest: loading Bronze is the manual step `scripts/upload_to_hdfs.py` run on the host
(it needs the docker CLI and the host path of the CSV files). Standard library only.
"""
from __future__ import annotations

import datetime as dt
import json
import urllib.error
import urllib.request
from typing import Any, Callable, Dict, List, NamedTuple

UPLOAD_HINT = (
    "Bronze chưa đủ. DAG chỉ xác nhận, không nạp dữ liệu: nạp bằng bước thủ công `python scripts/upload_to_hdfs.py ...` "
    "chạy trên máy host (xem docs/quick-start.md)."
)


class BronzeCheck(NamedTuple):
    expected: int
    present: int
    missing: List[str]
    empty: List[str]

    @property
    def ok(self) -> bool:
        return not self.missing and not self.empty


def expected_dates(start: str, end: str) -> List[str]:
    first, last = dt.date.fromisoformat(start), dt.date.fromisoformat(end)
    return [(first + dt.timedelta(days=i)).isoformat() for i in range((last - first).days + 1)]


def parse_liststatus(payload: Dict[str, Any]) -> Dict[str, int]:
    """WebHDFS LISTSTATUS JSON -> {file name: length in bytes}, files only."""
    entries = payload.get("FileStatuses", {}).get("FileStatus", [])
    return {e["pathSuffix"]: int(e["length"]) for e in entries if e.get("type") == "FILE"}


def check_listing(sizes: Dict[str, int], dates: List[str]) -> BronzeCheck:
    missing = [d for d in dates if "{}.csv".format(d) not in sizes]
    empty = [d for d in dates if sizes.get("{}.csv".format(d), 1) == 0]
    return BronzeCheck(expected=len(dates), present=len(dates) - len(missing), missing=missing, empty=empty)


def list_status(web_url: str, path: str, timeout: float = 15.0, opener: Callable[..., Any] = urllib.request.urlopen) -> Dict[str, Any]:
    """WebHDFS LISTSTATUS. A missing directory (HTTP 404) gives an empty listing; other errors propagate."""
    url = "{}/webhdfs/v1{}?op=LISTSTATUS".format(web_url.rstrip("/"), path)
    try:
        with opener(url, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return {}
        raise


def describe(check: BronzeCheck) -> str:
    text = "Bronze: {}/{} ngày có file".format(check.present, check.expected)
    if check.missing:
        text += "; thiếu: {}".format(", ".join(check.missing[:10]) + (" ..." if len(check.missing) > 10 else ""))
    if check.empty:
        text += "; file rỗng: {}".format(", ".join(check.empty[:10]))
    return text
