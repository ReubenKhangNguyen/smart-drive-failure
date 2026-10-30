from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

from pyspark.sql import SparkSession

from analytics import benchmark as bm
from config.settings import hdfs_uri, load_config

WORKER_LABELS = {"cores_2_executors_1": 1, "cores_4_executors_2": 2}
Variants = Dict[str, Callable[[], Any]]
Meta = Dict[str, Dict[str, Any]]


def days_from_start(config: Dict[str, Any], count: int) -> List[str]:
    start = dt.date.fromisoformat(config["data"]["start_date"])
    return [(start + dt.timedelta(days=i)).isoformat() for i in range(count)]


def _meta(spark: SparkSession, df: Any, paths: List[str]) -> Dict[str, Any]:
    size, files = bm.size_and_files(spark, paths)
    return {"size_bytes": size, "file_count": files, "input_partitions": bm.scan_partitions(df)}


def format_7d(spark: SparkSession, config: Dict[str, Any], days: int) -> Tuple[Variants, Meta]:
    dates = days_from_start(config, days)
    bronze = hdfs_uri(config, "bronze")
    silver = hdfs_uri(config, "silver")
    csv_paths = ["{}/{}.csv".format(bronze, d) for d in dates]
    parquet_path = "{}/parquet_all_columns_{}d".format(hdfs_uri(config, "benchmark_tmp"), days)
    created = bm.ensure_parquet_all_columns(spark, csv_paths, parquet_path)
    print("parquet_all_columns:", "created" if created else "reused (already existed)", parquet_path, flush=True)

    csv_df = bm.csv_dataframe(spark, csv_paths)
    parquet_df = spark.read.parquet(parquet_path)
    silver_df = bm.silver_dataframe(spark, silver, dates[0], dates[-1])
    silver_dirs = ["{}/date={}".format(silver, d) for d in dates]
    variants = {
        "csv_7d": lambda: bm.run_query(csv_df),
        "parquet_all_columns_7d": lambda: bm.run_query(parquet_df),
        "silver_parquet_7d": lambda: bm.run_query(silver_df),
    }
    meta = {
        "csv_7d": _meta(spark, csv_df, csv_paths),
        "parquet_all_columns_7d": _meta(spark, parquet_df, [parquet_path]),
        "silver_parquet_7d": _meta(spark, silver_df, silver_dirs),
    }
    return variants, meta


def format_q1(spark: SparkSession, config: Dict[str, Any]) -> Tuple[Variants, Meta]:
    bronze, silver = hdfs_uri(config, "bronze"), hdfs_uri(config, "silver")
    csv_df = bm.csv_dataframe(spark, [bronze])
    silver_df = spark.read.parquet(silver)
    variants = {"csv_q1": lambda: bm.run_query(csv_df), "silver_parquet_q1": lambda: bm.run_query(silver_df)}
    meta = {"csv_q1": _meta(spark, csv_df, [bronze]), "silver_parquet_q1": _meta(spark, silver_df, [silver])}
    return variants, meta


def small_files(spark: SparkSession, config: Dict[str, Any]) -> Tuple[Variants, Meta]:
    features = hdfs_uri(config, "features")
    coalesced = "{}/features_coalesced".format(hdfs_uri(config, "benchmark_tmp"))
    created = bm.ensure_features_coalesced(spark, features, coalesced)
    print("features_coalesced:", "created" if created else "reused (already existed)", coalesced, flush=True)
    as_is_df, coalesced_df = spark.read.parquet(features), spark.read.parquet(coalesced)
    variants = {"features_as_is": lambda: bm.run_query(as_is_df), "features_coalesced": lambda: bm.run_query(coalesced_df)}
    meta = {"features_as_is": _meta(spark, as_is_df, [features]), "features_coalesced": _meta(spark, coalesced_df, [coalesced])}
    return variants, meta


def workers(spark: SparkSession, config: Dict[str, Any], label: str, workload: str, days: int) -> Tuple[Variants, Meta]:
    if workload == "csv_7d":
        paths = ["{}/{}.csv".format(hdfs_uri(config, "bronze"), d) for d in days_from_start(config, days)]
        df, sources = bm.csv_dataframe(spark, paths), paths
    else:  # silver_q1
        silver = hdfs_uri(config, "silver")
        df, sources = spark.read.parquet(silver), [silver]
    return {label: lambda: bm.run_query(df)}, {label: _meta(spark, df, sources)}


def spark_environment(spark: SparkSession, master_state: Dict[str, Any]) -> Dict[str, str]:
    conf = spark.sparkContext.getConf()
    workers_info = master_state.get("workers", [])
    return {
        "spark_version": spark.version,
        "executor_memory": conf.get("spark.executor.memory", "1g (mặc định)"),
        "executor_cores": conf.get("spark.executor.cores", "(không đặt)"),
        "spark_cores_max": conf.get("spark.cores.max", "(không đặt)"),
        "driver_memory": conf.get("spark.driver.memory", "1g (mặc định)"),
        "spark_workers": "{} worker, mỗi worker {}".format(
            len(workers_info), ", ".join("{} core / {} MB".format(w.get("cores"), w.get("memory")) for w in workers_info[:1]) or "?"),
        "spark_master_total_cores": str(master_state.get("cores", "?")),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase 9 benchmark (HD9): ghi ket qua tho vao artifacts/reports/benchmark_raw")
    parser.add_argument("--experiment", required=True, choices=["format", "small_files", "workers"])
    parser.add_argument("--scope", choices=["7d", "q1"], default="7d", help="chi cho --experiment format")
    parser.add_argument("--label", choices=sorted(WORKER_LABELS), help="chi cho --experiment workers")
    parser.add_argument("--workload", choices=["csv_7d", "silver_q1"], default="csv_7d", help="chi cho --experiment workers")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--runs", type=int, default=bm.DEFAULT_RUNS)
    parser.add_argument("--warmup", type=int, default=bm.DEFAULT_WARMUP)
    parser.add_argument("--limit-seconds", type=float, default=bm.LIMIT_SECONDS)
    parser.add_argument("--master-http", default="http://spark-master:8080")
    parser.add_argument("--raw-dir", default=str(bm.DEFAULT_RAW_DIR))
    args = parser.parse_args()
    if args.experiment == "workers" and not args.label:
        parser.error("--experiment workers can --label")

    # Refuse to measure while another Spark application runs (it would distort the timings).
    try:
        master_state = bm.fetch_master_state(args.master_http)
        bm.assert_no_other_apps(master_state)
    except RuntimeError as exc:
        print("ABORT:", exc, file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001 - cannot verify the cluster is idle, so do not measure blind
        print("ABORT: cannot read Spark master state ({}: {})".format(type(exc).__name__, exc), file=sys.stderr)
        return 3

    config = load_config()
    spark = SparkSession.builder.appName("smart-benchmark").getOrCreate()
    try:
        if args.experiment == "format":
            variants, meta = format_7d(spark, config, args.days) if args.scope == "7d" else format_q1(spark, config)
            query = bm.QUERY_NAME
        elif args.experiment == "small_files":
            variants, meta = small_files(spark, config)
            query = bm.QUERY_NAME
        else:
            variants, meta = workers(spark, config, args.label, args.workload, args.days)
            query = "{} @ {}".format(bm.QUERY_NAME, args.workload)

        measured = bm.measure_variants(variants, runs=args.runs, warmup=args.warmup, limit_seconds=args.limit_seconds)
        executors = bm.executor_count(spark)
        raw_dir = Path(args.raw_dir)
        for name, result in measured.items():
            valid, note = True, ""
            if args.experiment == "workers" and executors != WORKER_LABELS[args.label]:
                valid, note = False, "không hợp lệ: đo được {} executor, cần {}".format(executors, WORKER_LABELS[args.label])
            record = bm.raw_record(args.experiment, name, query, result, meta[name], executors, valid, note)
            bm.append_jsonl(raw_dir, args.experiment, record)
            print("RESULT", json.dumps({k: record[k] for k in ("variant", "query", "seconds", "warmup_seconds", "rows", "executors", "valid", "note")},
                                       ensure_ascii=False), flush=True)
        env_path = raw_dir / "environment_spark.json"
        env_path.write_text(json.dumps(spark_environment(spark, master_state), ensure_ascii=False, indent=2), encoding="utf-8")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
