"""Exit 0 when Bronze has a non-empty CSV for every day of config data.start_date..end_date; 1 when it does not;
3 when the namenode cannot be reached. Read-only: it never loads, changes or deletes data.

    PYTHONPATH=/opt/smart-drive /usr/bin/python3 scripts/verify_bronze.py
"""
from __future__ import annotations

import argparse
import sys

from config.settings import load_config
from ingestion.bronze_verifier import UPLOAD_HINT, check_listing, describe, expected_dates, list_status, parse_liststatus


def main() -> int:
    config = load_config()
    parser = argparse.ArgumentParser(description="Xac nhan Bronze da du file theo ngay (chi doc, khong nap)")
    parser.add_argument("--web-url", default=config["hdfs"]["namenode_web_url"])
    parser.add_argument("--path", default=config["hdfs"]["bronze"])
    args = parser.parse_args()

    dates = expected_dates(config["data"]["start_date"], config["data"]["end_date"])
    try:
        sizes = parse_liststatus(list_status(args.web_url, args.path))
    except Exception as exc:  # noqa: BLE001 - cannot verify, so do not let the DAG continue
        print("ERROR: cannot list {} ({}: {})".format(args.path, type(exc).__name__, exc), file=sys.stderr)
        return 3
    check = check_listing(sizes, dates)
    print(describe(check))
    if not check.ok:
        print(UPLOAD_HINT, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
