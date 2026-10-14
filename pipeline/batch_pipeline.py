"""Run the reproducible Bronze CSV to Silver Parquet to Gold batch pipeline."""

from __future__ import annotations

import argparse
import json

from analytics.generate_gold import run as run_gold
from config.settings import FAILURE_HORIZON_DAYS, ProjectPaths
from processing.spark_jobs.smart_etl import run as run_etl

def run(input_path: str, paths: ProjectPaths) -> dict[str, object]:
    """Execute ETL then Gold generation, retaining paths, counts and runtimes in logs."""
    etl = run_etl(input_path, paths.silver, f"{paths.gold}/quality")
    gold = run_gold(paths.silver, paths.gold, paths.dashboard_dir, FAILURE_HORIZON_DAYS)
    return {"etl": etl, "gold": gold}

if __name__ == "__main__":
    defaults = ProjectPaths()
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=defaults.bronze)
    args = parser.parse_args()
    print(json.dumps(run(args.input, defaults), indent=2))
