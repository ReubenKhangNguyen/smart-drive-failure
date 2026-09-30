from __future__ import annotations

from pathlib import Path

import pytest

from ingestion.backblaze_downloader import DownloadError, download_with_retry, quarter_zip_filename


def test_quarter_zip_filename():
    assert quarter_zip_filename("2026-Q1") == "data_Q1_2026.zip"


def test_download_with_retry_succeeds_after_transient_failures(tmp_path: Path):
    attempts = {"count": 0}

    def flaky_fetch(url: str, dest: Path) -> None:
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise ConnectionError("simulated network reset")
        dest.write_text("ok")

    sleeps = []
    dest = tmp_path / "data_Q1_2026.zip"

    result = download_with_retry(
        "http://example.invalid/data_Q1_2026.zip",
        dest,
        fetch=flaky_fetch,
        max_retries=5,
        backoff_seconds=0.01,
        sleep=sleeps.append,
    )

    assert result == dest
    assert dest.read_text() == "ok"
    assert attempts["count"] == 3
    assert len(sleeps) == 2


def test_download_with_retry_raises_after_exhausting_attempts(tmp_path: Path):
    def always_fails(url: str, dest: Path) -> None:
        raise ConnectionError("simulated network reset")

    dest = tmp_path / "data_Q1_2026.zip"

    with pytest.raises(DownloadError) as exc_info:
        download_with_retry(
            "http://example.invalid/data_Q1_2026.zip",
            dest,
            fetch=always_fails,
            max_retries=3,
            backoff_seconds=0.01,
            sleep=lambda _: None,
        )

    assert "http://example.invalid/data_Q1_2026.zip" in str(exc_info.value)
