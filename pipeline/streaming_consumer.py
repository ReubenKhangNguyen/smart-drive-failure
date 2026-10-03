"""Phase 2.b consumer: Spark Structured Streaming reads Kafka `smart-events` JSON, applies the SAME cleaning as the batch
ETL (`cast_and_clean_columns`) and appends Parquet to /smart-drive/streaming_output/<run_id> (demo output, not a Gold
table). Stops itself once the feed has been quiet for --idle-seconds, then compares its output with the Silver partition
of the same day. Python 3.8 compatible (runs on the cluster)."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StringType, StructField, StructType

from config.settings import hdfs_uri, load_config
from processing.spark_jobs.smart_etl import REQUIRED_RAW_COLUMNS, cast_and_clean_columns

DEFAULT_SUMMARY = Path(__file__).resolve().parent.parent / "artifacts" / "reports" / "streaming_consumer_summary.json"


def build_raw_schema() -> StructType:
    return StructType([StructField(c, StringType(), True) for c in REQUIRED_RAW_COLUMNS])


def parse_kafka_messages(kafka_df: DataFrame) -> DataFrame:
    """JSON message value -> the same raw column shape smart_etl.py reads from CSV,
    so cast_and_clean_columns (Phase 3's cleaning logic) applies unchanged."""
    schema = build_raw_schema()
    parsed = kafka_df.select(F.from_json(F.col("value").cast("string"), schema).alias("data")).select("data.*")
    return cast_and_clean_columns(parsed)


def start_stream(
    spark: SparkSession,
    kafka_bootstrap: str,
    topic: str,
    output_path: str,
    checkpoint_path: str,
    max_offsets_per_trigger: Optional[int] = None,
) -> StreamingQuery:
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
    )
    if max_offsets_per_trigger:
        reader = reader.option("maxOffsetsPerTrigger", int(max_offsets_per_trigger))  # several visible micro-batches
    clean_df = parse_kafka_messages(reader.load())

    return (
        clean_df.writeStream.format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .start()
    )


def summarize_progress(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Micro-batches that read data, from StreamingQuery.recentProgress."""
    by_batch = {}  # type: Dict[Any, Dict[str, Any]]
    for event in events:
        rows = int(event.get("numInputRows") or 0)
        batch_id = event.get("batchId")
        if rows > 0 and batch_id is not None:
            by_batch[batch_id] = {
                "batchId": batch_id,
                "numInputRows": rows,
                "triggerExecutionMs": (event.get("durationMs") or {}).get("triggerExecution"),
                "processedRowsPerSecond": event.get("processedRowsPerSecond"),
            }
    batches = [by_batch[k] for k in sorted(by_batch)]
    return {"batches": batches, "total_input_rows": sum(b["numInputRows"] for b in batches)}


def wait_until_idle(
    get_progress: Callable[[], List[Dict[str, Any]]],
    idle_seconds: float,
    timeout_seconds: float,
    poll_seconds: float = 2.0,
    is_active: Callable[[], bool] = lambda: True,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> str:
    """Block until data was read and then none arrived for `idle_seconds` ("idle"), the overall `timeout_seconds`
    passes ("timeout"), or the query stopped by itself ("stopped")."""
    start = clock()
    seen = 0
    last_growth = None  # type: Optional[float]
    while True:
        total = summarize_progress(get_progress())["total_input_rows"]
        now = clock()
        if total > seen:
            seen, last_growth = total, now
        if last_growth is not None and now - last_growth >= idle_seconds:
            return "idle"
        if not is_active():
            return "stopped"
        if now - start >= timeout_seconds:
            return "timeout"
        sleep(poll_seconds)


def _stats(df: DataFrame) -> Dict[str, Any]:
    row = df.agg(
        F.count("*").alias("rows"),
        F.countDistinct("serial_number").alias("serials"),
        F.sum(F.col("failure").cast("long")).alias("failures"),
    ).collect()[0]
    return {"rows": int(row["rows"]), "serials": int(row["serials"]), "failures": int(row["failures"] or 0)}


def compare_with_silver(spark: SparkSession, output_path: str, silver_path: str, date: str) -> Dict[str, Any]:
    """Rows, distinct serials and failures of the streamed output versus the Silver partition of `date` (read-only)."""
    streamed = spark.read.parquet(output_path).where(F.col("date").cast("string") == date)
    silver = spark.read.parquet(silver_path).where(F.col("date").cast("string") == date)
    s_stats, b_stats = _stats(streamed), _stats(silver)
    return {"date": date, "streamed": s_stats, "silver": b_stats, "match": s_stats == b_stats}


def main() -> int:
    config = load_config()
    parser = argparse.ArgumentParser(description="Spark Structured Streaming: Kafka -> streaming_output/<run_id> (demo)")
    parser.add_argument("--kafka-bootstrap", default="kafka:29092")
    parser.add_argument("--topic", default="smart-events")
    parser.add_argument("--run-id", default=dt.datetime.utcnow().strftime("%Y%m%d%H%M%S"))
    parser.add_argument("--output-path", help="mac dinh: streaming_output trong config + /<run-id>")
    parser.add_argument("--checkpoint-path", help="mac dinh: streaming_checkpoint trong config + /<run-id>")
    parser.add_argument("--max-offsets-per-trigger", type=int, default=50000)
    parser.add_argument("--idle-seconds", type=float, default=30.0)
    parser.add_argument("--timeout-seconds", type=float, default=900.0)
    parser.add_argument("--compare-date", help="doi chieu voi phan vung Silver cua ngay nay, vd 2026-01-01")
    parser.add_argument("--silver-path", default=hdfs_uri(config, "silver"))
    parser.add_argument("--summary-json", default=str(DEFAULT_SUMMARY))
    args = parser.parse_args()

    output_path = args.output_path or "{}/{}".format(hdfs_uri(config, "streaming_output"), args.run_id)
    checkpoint_path = args.checkpoint_path or "{}/{}".format(hdfs_uri(config, "streaming_checkpoint"), args.run_id)

    spark = SparkSession.builder.appName("smart-streaming-consumer").getOrCreate()
    started = time.time()
    query = start_stream(spark, args.kafka_bootstrap, args.topic, output_path, checkpoint_path, args.max_offsets_per_trigger)
    print("STREAM_STARTED run_id={} topic={}".format(args.run_id, args.topic), flush=True)

    reason = wait_until_idle(lambda: list(query.recentProgress), args.idle_seconds, args.timeout_seconds,
                             is_active=lambda: query.isActive)
    progress = summarize_progress(list(query.recentProgress))
    query.stop()

    summary = {
        "run_id": args.run_id, "topic": args.topic, "stopped_because": reason, "output_path": output_path,
        "checkpoint_path": checkpoint_path, "max_offsets_per_trigger": args.max_offsets_per_trigger,
        "micro_batches": progress["batches"], "streamed_input_rows": progress["total_input_rows"],
        "output_rows": spark.read.parquet(output_path).count(), "seconds": round(time.time() - started, 1),
    }
    if args.compare_date:
        summary["silver_comparison"] = compare_with_silver(spark, output_path, args.silver_path, args.compare_date)
    Path(args.summary_json).write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print("CONSUMER_SUMMARY " + json.dumps(summary), flush=True)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
