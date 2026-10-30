from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from config.settings import bronze_target, load_config
from ingestion.dataset_validator import validate_source_dir
from ingestion.hdfs_loader import (
    hdfs_put_command,
    parse_hdfs_ls_sizes,
    plan_uploads,
    to_container_path,
)


def _run(cmd, check: bool = True):
    return subprocess.run(cmd, check=check, capture_output=True, text=True)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Nap CSV Backblaze vao HDFS Bronze, idempotent theo kich thuoc file."
    )
    parser.add_argument("--host-source-dir", required=True, help="Thu muc chua CSV tren host, vd C:/dataset_smart_drive_failure/data_Q1_2026/data_Q1_2026")
    parser.add_argument("--container-source-dir", required=True, help="Duong dan tuong ung trong container namenode, vd /external_data/data_Q1_2026")
    parser.add_argument("--namenode-service", default="namenode")
    parser.add_argument("--quarter", help="id quy trong data.quarters: lay khoang ngay va thu muc Bronze dich tu config")
    parser.add_argument("--start-date", default=None, help="mac dinh lay tu --quarter")
    parser.add_argument("--end-date", default=None, help="mac dinh lay tu --quarter")
    parser.add_argument("--skip-validation", action="store_true")
    parser.add_argument("--hdfs-target-dir", default=None, help="Thu muc Bronze tren HDFS; mac dinh lay tu config (hdfs.bronze). Dat khi nap quy khac, vd /smart-drive/bronze/year=2026/quarter=Q2")
    args = parser.parse_args()

    config = load_config()
    if args.quarter:
        try:
            target = bronze_target(config, args.quarter)
        except ValueError as exc:
            parser.error(str(exc))
        args.start_date = args.start_date or target["start_date"]
        args.end_date = args.end_date or target["end_date"]
        args.hdfs_target_dir = args.hdfs_target_dir or target["path"]
    if not (args.start_date and args.end_date):
        parser.error("can --quarter hoac ca --start-date va --end-date")
    hdfs_bronze = args.hdfs_target_dir or config["hdfs"]["bronze"]
    replication = config["hdfs"].get("bronze_replication", 1)

    host_source_dir = Path(args.host_source_dir)

    if not args.skip_validation:
        report = validate_source_dir(host_source_dir, args.start_date, args.end_date)
        if report.missing_dates:
            print("CANH BAO: thieu ngay: {}".format(report.missing_dates), file=sys.stderr)
        if report.bad_header_files:
            print("LOI: file thieu cot bat buoc: {}".format(report.bad_header_files), file=sys.stderr)
            return 1
        print("Validation OK: {} file, {} ngay thieu".format(report.total_files, len(report.missing_dates)))

    _run(["docker", "compose", "exec", "-T", args.namenode_service, "hdfs", "dfs", "-mkdir", "-p", hdfs_bronze])

    ls_result = _run(
        ["docker", "compose", "exec", "-T", args.namenode_service, "hdfs", "dfs", "-ls", hdfs_bronze],
        check=False,
    )
    existing_sizes = parse_hdfs_ls_sizes(ls_result.stdout) if ls_result.returncode == 0 else {}

    csv_files = sorted(
        f for f in host_source_dir.glob("*.csv") if args.start_date <= f.stem <= args.end_date
    )
    plan = plan_uploads(csv_files, existing_sizes)

    for f in plan.to_upload:
        container_path = to_container_path(f, host_source_dir, args.container_source_dir)
        cmd = hdfs_put_command(container_path, hdfs_bronze, f.name, replication)
        _run(["docker", "compose", "exec", "-T", args.namenode_service] + cmd)
        print("Uploaded", f.name)

    for f in plan.skipped_same_size:
        print("Skip (same size):", f.name)

    print(
        "Tong: {} file tai moi, {} file bo qua (da co, cung kich thuoc)".format(
            len(plan.to_upload), len(plan.skipped_same_size)
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
