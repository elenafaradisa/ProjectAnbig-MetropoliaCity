"""Transform + Load: read ingested Parquet, clean, derive features, and load
into core.observations (PostgreSQL).

Design choices, and why:

- observed_at (parsed timestamp) is treated as the source of truth for time.
  Spot checks on the raw sample showed time_of_day and day_of_week
  disagreeing with the timestamp (e.g. 00:00:30 labelled "Morning"), so
  weekday_from_ts / obs_hour are derived fresh rather than trusting the raw
  columns. The raw values are kept as-is in time_of_day/day_of_week, and
  tod_mismatch/dow_mismatch record where they disagree, for the audit
  notebook.
- anomaly_label and incident_type are kept as given (not recomputed) but
  label_consistent flags the (rare, if any) row where the two disagree, so
  that inconsistency is visible rather than silently propagated.
- incident_name comes from config/incident_types.yaml, not hardcoded here,
  so the mapping only needs to change in one place.

transform_df() is the pure, testable core (DataFrame in, DataFrame out — no
I/O). run() wraps it with the Parquet read and Postgres write for the DAG.

Usage:
    python -m spark.jobs.transform --input /opt/project/data/parquet \
        --config /opt/project/config/incident_types.yaml
"""
import argparse

import yaml
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.common.schema import CORE_COLUMNS
from spark.common.spark_session import get_spark, write_to_postgres


def load_incident_names(config_path: str) -> dict:
    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    return {k: v["name"] for k, v in cfg["incident_types"].items()}


def transform_df(df: DataFrame, incident_names: dict) -> DataFrame:
    if "timestamp" in df.columns:
        df = df.withColumnRenamed("timestamp", "observed_at")

    mapping_expr = F.create_map(
        *[x for pair in incident_names.items() for x in (F.lit(pair[0]), F.lit(pair[1]))]
    )

    df = (
        df.withColumn("incident_name", mapping_expr[F.col("incident_type")])
        .withColumn("obs_date", F.to_date("observed_at"))
        # Derived here directly from observed_at rather than relying on
        # ingest's obs_year/obs_month partition columns: transform_df must
        # work standalone (e.g. in tests, or if input isn't partitioned).
        .withColumn("obs_year", F.year("observed_at"))
        .withColumn("obs_month", F.month("observed_at"))
        .withColumn("obs_hour", F.hour("observed_at"))
        .withColumn("weekday_from_ts", F.date_format("observed_at", "EEEE"))
        .withColumn("is_weekend", F.col("weekday_from_ts").isin("Saturday", "Sunday"))
        .withColumn(
            "window_15min",
            F.to_timestamp(F.floor(F.unix_timestamp("observed_at") / 900) * 900),
        )
    )

    # Derive a coarse time-of-day bucket from the hour to compare against the
    # raw time_of_day column (same buckets the raw file appears to use).
    df = df.withColumn(
        "tod_from_hour",
        F.when(F.col("obs_hour").between(5, 11), "Morning")
        .when(F.col("obs_hour").between(12, 16), "Afternoon")
        .when(F.col("obs_hour").between(17, 20), "Evening")
        .otherwise("Night"),
    )
    df = (
        df.withColumn("tod_mismatch", F.col("time_of_day") != F.col("tod_from_hour"))
        .drop("tod_from_hour")
        .withColumn("dow_mismatch", F.col("day_of_week") != F.col("weekday_from_ts"))
        .withColumn(
            "label_consistent",
            (F.col("anomaly_label") == 1) == (F.col("incident_type") != 0),
        )
    )

    return df.select(*CORE_COLUMNS)


def run(input_path: str, config_path: str, target_table: str = "core.observations", spark=None) -> int:
    owns_session = spark is None
    if owns_session:
        spark = get_spark("transform_traffictab23")

    df = spark.read.parquet(input_path)
    incident_names = load_incident_names(config_path)
    df = transform_df(df, incident_names)

    row_count = df.count()
    print(f"Transformed {row_count} rows.")

    inconsistent = df.filter(~F.col("label_consistent")).count()
    if inconsistent > 0:
        print(f"WARNING: {inconsistent} row(s) have anomaly_label/incident_type mismatch.")

    write_to_postgres(df, target_table, mode="overwrite")
    print(f"Loaded into {target_table}")

    if owns_session:
        spark.stop()
    return row_count


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    run(args.input, args.config)


if __name__ == "__main__":
    main()
