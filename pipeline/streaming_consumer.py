from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StringType, StructField, StructType

from processing.spark_jobs.smart_etl import REQUIRED_RAW_COLUMNS, cast_and_clean_columns


def build_raw_schema() -> StructType:
    return StructType([StructField(c, StringType(), True) for c in REQUIRED_RAW_COLUMNS])


def parse_kafka_messages(kafka_df: DataFrame) -> DataFrame:
    """JSON message value -> the same raw column shape smart_etl.py reads from CSV,
    so cast_and_clean_columns (Phase 3's cleaning logic) applies unchanged."""
    schema = build_raw_schema()
    parsed = kafka_df.select(F.from_json(F.col("value").cast("string"), schema).alias("data")).select("data.*")
    return cast_and_clean_columns(parsed)


def run(
    spark: SparkSession,
    kafka_bootstrap: str,
    topic: str,
    output_path: str,
    checkpoint_path: str,
) -> StreamingQuery:
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )
    clean_df = parse_kafka_messages(kafka_df)

    return (
        clean_df.writeStream.format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .start()
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Spark Structured Streaming: Kafka smart-events -> streaming_output (demo)")
    parser.add_argument("--kafka-bootstrap", default="kafka:29092")
    parser.add_argument("--topic", default="smart-events")
    parser.add_argument("--output-path", default="hdfs://namenode:9000/smart-drive/streaming_output")
    parser.add_argument("--checkpoint-path", default="hdfs://namenode:9000/smart-drive/streaming_checkpoint")
    parser.add_argument("--timeout-seconds", type=int, default=90)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("smart-streaming-consumer").getOrCreate()
    query = run(spark, args.kafka_bootstrap, args.topic, args.output_path, args.checkpoint_path)
    query.awaitTermination(args.timeout_seconds)
    query.stop()
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
