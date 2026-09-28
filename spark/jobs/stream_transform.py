"""Streaming Transform + Load: consume traffictab23 events from Kafka,
apply the same transform_df() used by the batch pipeline, and append into
core.observations (PostgreSQL) via Spark Structured Streaming.

This is the streaming counterpart to spark/jobs/transform.py, built to
satisfy the course's streaming requirement (Kafka + Spark Structured
Streaming + database sink). The source is still traffictab23 replayed by
streaming/producer.py -- this is a replay simulation of historical data,
not a live sensor feed. The *pipeline* itself (Kafka topic/partition/
offset, Structured Streaming micro-batches, checkpointing) is genuinely
streaming, independent of where the underlying data originally came from.

transform_df() is reused unchanged from spark.jobs.transform -- the same
audited transformation logic (derived time columns, tod_mismatch/
dow_mismatch flags, incident_name mapping) applies whether the input
DataFrame came from a batch Parquet read or a streaming micro-batch.

Usage:
    python -m spark.jobs.stream_transform \
        --config /opt/project/config/incident_types.yaml \
        --topic traffic_topic \
        --bootstrap-servers kafka:9092 \
        --checkpoint-dir /opt/project/data/checkpoints/traffic_stream \
        --starting-offsets earliest
"""
import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from spark.common.schema import RAW_SCHEMA
from spark.common.spark_session import get_spark, write_to_postgres
from spark.jobs.transform import load_incident_names, transform_df

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

# Semua field mentah datang lewat JSON. Parse dulu sebagai string (aman
# terlepas dari bagaimana producer men-serialize angka), baru cast ke tipe
# asli dari RAW_SCHEMA -- ini setara dengan yang dilakukan ingest.py di
# jalur batch (schema eksplisit, bukan inferSchema).
WIRE_SCHEMA = StructType([StructField(f.name, StringType(), True) for f in RAW_SCHEMA.fields])


def parse_and_cast(raw: DataFrame) -> DataFrame:
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), WIRE_SCHEMA).alias("data")
    ).select("data.*")

    for field in RAW_SCHEMA.fields:
        parsed = parsed.withColumn(field.name, F.col(field.name).cast(field.dataType))

    return parsed


def make_batch_writer(target_table: str):
    """Tiap micro-batch streaming itu DataFrame biasa (bounded), jadi
    write_to_postgres() yang sudah ada dipakai tanpa perubahan, mode append."""

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        count = batch_df.count()
        if count == 0:
            return
        write_to_postgres(batch_df, target_table, mode="append")
        print(f"[batch {batch_id}] loaded {count} row(s) into {target_table}")

    return write_batch


def run(
    config_path: str,
    topic: str,
    bootstrap_servers: str,
    checkpoint_dir: str,
    target_table: str = "core.observations_stream",
    starting_offsets: str = "latest",
    trigger_seconds: int = 10,
) -> None:
    spark = get_spark("stream_transform_traffictab23", packages=KAFKA_PACKAGE)
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    incident_names = load_incident_names(config_path)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )

    typed = parse_and_cast(raw)
    transformed = transform_df(typed, incident_names)
    transformed = transformed.withColumn("observed_at", F.to_timestamp("observed_at"))

    query = (
        transformed.writeStream.foreachBatch(make_batch_writer(target_table))
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )

    print(f"Streaming query started (id={query.id}). Menunggu data...")
    query.awaitTermination()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--topic", default="traffic_topic")
    parser.add_argument("--bootstrap-servers", default="kafka:9092")
    parser.add_argument("--checkpoint-dir", default="/opt/project/data/checkpoints/traffic_stream")
    parser.add_argument("--starting-offsets", default="latest")
    parser.add_argument("--trigger-seconds", type=int, default=10)
    parser.add_argument("--target-table", default="core.observations_stream")
    args = parser.parse_args()

    run(
        config_path=args.config,
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        checkpoint_dir=args.checkpoint_dir,
        starting_offsets=args.starting_offsets,
        trigger_seconds=args.trigger_seconds,
    )


if __name__ == "__main__":
    main()