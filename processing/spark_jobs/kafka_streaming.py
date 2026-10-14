"""Consume compact SMART JSON events from Kafka and persist them in HDFS Bronze."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, LongType, StringType, StructField, StructType


EVENT_SCHEMA = StructType(
    [
        StructField("date", DateType(), True),
        StructField("serial_number", StringType(), True),
        StructField("model", StringType(), True),
        StructField("capacity_bytes", LongType(), True),
        StructField("failure", LongType(), True),
        StructField("smart_5_raw", LongType(), True),
        StructField("smart_187_raw", LongType(), True),
        StructField("smart_197_raw", LongType(), True),
        StructField("smart_198_raw", LongType(), True),
        StructField("smart_199_raw", LongType(), True),
    ]
)


def run(bootstrap_servers: str, topic: str, output: str, checkpoint: str) -> None:
    """Run until interrupted; checkpointing permits a restart without rereading offsets."""
    spark = SparkSession.builder.appName("smart-drive-kafka-streaming").getOrCreate()
    events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
        .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event"), F.col("timestamp").alias("kafka_timestamp"))
        .select("event.*", "kafka_timestamp")
        .filter(F.col("serial_number").isNotNull() & F.col("date").isNotNull())
    )
    query = (
        events.writeStream.format("parquet")
        .option("path", output)
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .start()
    )
    print(f"KAFKA_STREAMING_STARTED topic={topic} output={output} checkpoint={checkpoint}", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Write Kafka SMART events as HDFS Parquet.")
    parser.add_argument("--bootstrap-servers", default="kafka:9092")
    parser.add_argument("--topic", default="smart-drive-events")
    parser.add_argument("--output", default="hdfs://namenode:9000/smart-drive/bronze/kafka-events")
    parser.add_argument("--checkpoint", default="hdfs://namenode:9000/smart-drive/checkpoints/kafka-events")
    args = parser.parse_args()
    run(args.bootstrap_servers, args.topic, args.output, args.checkpoint)
