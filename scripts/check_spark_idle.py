"""Exit 0 when the Spark master runs no application, 2 when it is busy, 3 when it cannot be reached.

Standard library only (no pyspark, no yaml): runs with /usr/bin/python3 inside the Airflow image.
    PYTHONPATH=/opt/smart-drive /usr/bin/python3 scripts/check_spark_idle.py
"""
from __future__ import annotations

import argparse
import sys

from pipeline.cluster_guard import ClusterBusyError, fetch_master_state, wait_until_idle


def main() -> int:
    parser = argparse.ArgumentParser(description="Dung neu Spark master dang chay ung dung khac")
    parser.add_argument("--master-http", default="http://spark-master:8080")
    parser.add_argument("--attempts", type=int, default=5)
    parser.add_argument("--delay", type=float, default=3.0)
    args = parser.parse_args()
    try:
        wait_until_idle(lambda: fetch_master_state(args.master_http), attempts=args.attempts, delay=args.delay)
    except ClusterBusyError as exc:
        print("BUSY:", exc, file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001 - cannot verify the cluster is idle, so do not start a job blind
        print("ERROR: cannot read Spark master state ({}: {})".format(type(exc).__name__, exc), file=sys.stderr)
        return 3
    print("Spark master is idle")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
