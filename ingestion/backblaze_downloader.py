from __future__ import annotations

import time
from pathlib import Path
from typing import Callable, Optional

DEFAULT_BASE_URL = "https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data"


class DownloadError(Exception):
    pass


def quarter_zip_filename(quarter: str) -> str:
    """"2026-Q1" -> "data_Q1_2026.zip", matching Backblaze's archive naming."""
    year, q = quarter.split("-")
    return "data_{}_{}.zip".format(q, year)


def download_with_retry(
    url: str,
    dest: Path,
    fetch: Callable[[str, Path], None],
    max_retries: int = 5,
    backoff_seconds: float = 2.0,
    sleep: Callable[[float], None] = time.sleep,
) -> Path:
    """Call `fetch(url, dest)` with retry+backoff. Raises DownloadError with manual-download
    instructions if every attempt fails (network resets are common on this connection)."""
    last_error: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            fetch(url, dest)
            return dest
        except Exception as exc:  # noqa: BLE001 - any transport failure should trigger retry
            last_error = exc
            if attempt == max_retries:
                break
            sleep(backoff_seconds * attempt)

    raise DownloadError(
        "Tai that bai sau {} lan thu voi URL {}. Loi cuoi: {}. "
        "Huong dan tai tay: mo trinh duyet toi {} va luu file vao {}.".format(
            max_retries, url, last_error, url, dest
        )
    )
