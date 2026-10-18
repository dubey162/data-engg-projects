"""Aggregate Kafka store events into event-time windows using Spark Structured Streaming."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, from_json, sum as spark_sum, to_timestamp, window
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("region", StringType(), True),
    ]
)


def main() -> None:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    topic = os.getenv("KAFKA_TOPIC", "store-events")
    starting_offsets = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
    output_path = os.getenv("OUTPUT_PATH", "data/aggregates")
    checkpoint_path = os.getenv("CHECKPOINT_PATH", "data/checkpoints/store-analytics")

    spark = (
        SparkSession.builder.appName("StreamingStoreAnalytics")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )

    events = (
        kafka_events.select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("event"))
        .select("event.*")
        .withColumn("event_time", to_timestamp(col("event_time")))
        .filter(
            col("event_id").isNotNull()
            & col("event_time").isNotNull()
            & col("event_type").isNotNull()
            & col("region").isNotNull()
        )
    )

    aggregates = (
        events.withWatermark("event_time", "10 seconds")
        .groupBy(
            window(col("event_time"), "30 seconds"),
            col("region"),
            col("event_type"),
        )
        .agg(
            spark_sum("amount").alias("revenue"),
            count("*").alias("event_count"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("region"),
            col("event_type"),
            col("event_count"),
            col("revenue"),
        )
    )

    query = (
        aggregates.writeStream.outputMode("append")
        .format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .start()
    )
    print(
        f"Streaming topic '{topic}' from {bootstrap_servers}; "
        f"writing window aggregates to {output_path}",
        flush=True,
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
