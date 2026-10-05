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

Delivery guarantee: the Kafka checkpoint makes each offset range commit
once, but foreachBatch + plain JDBC append is only at-least-once -- a batch
that crashes after writing is replayed on restart, and a producer run that
re-sends the same rows also lands them twice. The sink is therefore made
idempotent: each micro-batch goes to a staging table and is merged with
INSERT ... ON CONFLICT (observed_at) DO NOTHING (observed_at is unique per
row in traffictab23; see sql/schemas/14_core_observations_stream_unique.sql).
Replaying the same data any number of times leaves exactly one row per event.

Second query -- stateful hourly aggregation: the same stream is also grouped
per road_segment_id into 1-hour EVENT-TIME windows (watermark + per-window
state in Spark), with exactly the formulas of sql/mart/build_road_hourly_stats.sql,
and upserted into mart.road_hourly_stream. mart.road_current_status_live is
the live twin of mart.road_current_status built on top of it. Duplicates are
dropped inside the stream (dropDuplicatesWithinWatermark on observed_at)
before they reach the aggregation state, so a re-sent event is not counted
twice. Output mode is "update": every micro-batch emits the windows whose
totals changed, so the dashboard sees an hour fill up while it is still open.

Usage:
    python -m spark.jobs.stream_transform \
        --config /opt/project/config/incident_types.yaml \
        --topic traffic_topic \
        --bootstrap-servers kafka:9092 \
        --checkpoint-dir /opt/project/data/checkpoints/traffic_stream \
        --starting-offsets earliest
"""
import argparse
import os

import psycopg2
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


def get_pg_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def make_batch_writer(target_table: str):
    """Tiap micro-batch streaming itu DataFrame biasa (bounded). Ditulis dulu
    ke tabel staging lewat write_to_postgres() (JDBC), lalu digabung ke tabel
    target dengan ON CONFLICT DO NOTHING, sehingga batch yang diproses ulang
    (restart/crash) atau data yang dikirim ulang producer tidak jadi dobel."""
    staging_table = f"{target_table}_staging"

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        batch_df = batch_df.dropDuplicates(["observed_at"])
        count = batch_df.count()
        if count == 0:
            return
        write_to_postgres(batch_df, staging_table, mode="overwrite")

        cols = ", ".join(batch_df.columns)
        conn = get_pg_connection()
        try:
            with conn, conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO {target_table} ({cols}) "
                    f"SELECT {cols} FROM {staging_table} "
                    f"ON CONFLICT (observed_at) DO NOTHING"
                )
                inserted = cur.rowcount  # one statement, so this is the full count
        finally:
            conn.close()
        print(f"[batch {batch_id}] {count} row(s) received, {inserted} new, "
              f"{count - inserted} already in {target_table} (skipped)")

    return write_batch


HOURLY_KEY = ["road_segment_id", "obs_date", "obs_hour"]


def build_hourly(transformed: DataFrame, watermark: str) -> DataFrame:
    """Per segment per event-time hour; same columns and formulas as
    mart.road_hourly_stats (sql/mart/build_road_hourly_stats.sql)."""
    w = F.window("observed_at", "1 hour")
    return (
        transformed
        .withWatermark("observed_at", watermark)
        .dropDuplicatesWithinWatermark(["observed_at"])
        .groupBy(w.alias("w"), "road_segment_id")
        .agg(
            F.count("*").alias("n_observations"),
            F.avg("vehicle_count").alias("avg_vehicle_count"),
            F.avg("average_speed").alias("avg_average_speed"),
            F.avg("lane_occupancy_rate").alias("avg_lane_occupancy_rate"),
            F.avg("jam_density_index").alias("avg_jam_density_index"),
            F.sum(F.when(F.col("incident_type") != 0, 1).otherwise(0)).alias("incident_count"),
            F.avg(F.col("anomaly_label").cast("int")).alias("anomaly_rate"),
            F.max("incident_type").alias("max_incident_type"),
            F.avg("v2x_packet_loss_rate").alias("avg_v2x_packet_loss_rate"),
            F.avg("v2x_message_delay_avg").alias("avg_v2x_message_delay_avg"),
            F.max(F.coalesce(F.col("weather_condition").isin("Rain", "Snow"), F.lit(False)).cast("int"))
             .alias("bad_weather_i"),
            F.max(F.coalesce(F.col("road_surface_status") == "Wet", F.lit(False)).cast("int"))
             .alias("wet_surface_i"),
        )
        .select(
            "road_segment_id",
            F.to_date(F.col("w.start")).alias("obs_date"),
            F.hour(F.col("w.start")).cast("short").alias("obs_hour"),
            F.col("n_observations").cast("int"),
            "avg_vehicle_count", "avg_average_speed", "avg_lane_occupancy_rate", "avg_jam_density_index",
            F.col("incident_count").cast("int"),
            "anomaly_rate",
            F.col("max_incident_type").cast("short"),
            "avg_v2x_packet_loss_rate", "avg_v2x_message_delay_avg",
            (F.col("bad_weather_i") == 1).alias("any_bad_weather"),
            (F.col("wet_surface_i") == 1).alias("any_wet_surface"),
        )
    )


def make_upsert_writer(target_table: str, key: list):
    """foreachBatch writer for the aggregated stream: in "update" mode each
    micro-batch carries the CURRENT totals of every window that changed, so
    the row for that key is replaced (ON CONFLICT ... DO UPDATE). Replaying a
    batch writes the same totals again -- idempotent."""
    staging_table = f"{target_table}_staging"

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        count = batch_df.count()
        if count == 0:
            return
        write_to_postgres(batch_df, staging_table, mode="overwrite")
        cols = batch_df.columns
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in key)
        conn = get_pg_connection()
        try:
            with conn, conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO {target_table} ({', '.join(cols)}) "
                    f"SELECT {', '.join(cols)} FROM {staging_table} "
                    f"ON CONFLICT ({', '.join(key)}) DO UPDATE SET {updates}, updated_at = now()"
                )
        finally:
            conn.close()
        print(f"[hourly batch {batch_id}] {count} segment-hour window(s) upserted into {target_table}")

    return write_batch


def run(
    config_path: str,
    topic: str,
    bootstrap_servers: str,
    checkpoint_dir: str,
    target_table: str = "core.observations_stream",
    starting_offsets: str = "latest",
    trigger_seconds: int = 10,
    hourly_table: str = "mart.road_hourly_stream",
    watermark: str = "1 hour",
) -> None:
    spark = get_spark("stream_transform_traffictab23", packages=KAFKA_PACKAGE)
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

    # Second, stateful query on the same stream -- its own checkpoint, since
    # it also stores the open windows' aggregation state.
    hourly_query = (
        build_hourly(transformed, watermark).writeStream
        .outputMode("update")
        .foreachBatch(make_upsert_writer(hourly_table, HOURLY_KEY))
        .option("checkpointLocation", f"{checkpoint_dir}_hourly")
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )

    print(f"Streaming queries started (raw id={query.id}, hourly id={hourly_query.id}). Menunggu data...")
    spark.streams.awaitAnyTermination()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--topic", default="traffic_topic")
    parser.add_argument("--bootstrap-servers", default="kafka:9092")
    parser.add_argument("--checkpoint-dir", default="/opt/project/data/checkpoints/traffic_stream")
    parser.add_argument("--starting-offsets", default="latest")
    parser.add_argument("--trigger-seconds", type=int, default=10)
    parser.add_argument("--target-table", default="core.observations_stream")
    parser.add_argument("--hourly-table", default="mart.road_hourly_stream")
    parser.add_argument("--watermark", default="1 hour",
                        help="event-time lateness allowed before an hourly window's state is dropped")
    args = parser.parse_args()

    run(
        config_path=args.config,
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        checkpoint_dir=args.checkpoint_dir,
        target_table=args.target_table,  # was parsed but never passed on
        starting_offsets=args.starting_offsets,
        trigger_seconds=args.trigger_seconds,
        hourly_table=args.hourly_table,
        watermark=args.watermark,
    )


if __name__ == "__main__":
    main()