from __future__ import annotations

import argparse
import sys
from pathlib import Path

import requests

from config.settings import load_config
from ingestion.backblaze_downloader import (
    DEFAULT_BASE_URL,
    DownloadError,
    download_with_retry,
    quarter_zip_filename,
)


def _fetch(url: str, dest: Path) -> None:
    with requests.get(url, stream=True, timeout=30) as response:
        response.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)


def main() -> int:
    parser = argparse.ArgumentParser(description="Tai file zip Backblaze cho quy da cau hinh trong config/project.yaml")
    parser.add_argument("--quarter", default=None, help="quy can tai, vd 2026-Q3 (mac dinh: project.quarter trong config)")
    parser.add_argument("--dest-dir", default="dataset/raw")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    args = parser.parse_args()

    config = load_config()
    quarter = args.quarter or config["project"]["quarter"]
    zip_name = quarter_zip_filename(quarter)
    dest_dir = Path(args.dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / zip_name

    if dest.exists():
        print("Da co file {}, bo qua tai lai (xoa file neu muon tai lai).".format(dest))
        return 0

    url = "{}/{}".format(args.base_url, zip_name)
    print("Dang tai {} -> {}".format(url, dest))
    try:
        download_with_retry(url, dest, fetch=_fetch)
    except DownloadError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print("Da tai xong: {}".format(dest))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
