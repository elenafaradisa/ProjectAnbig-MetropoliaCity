"""Column definitions for the traffictab23 dataset.

Kept in one place so the ingest job (reads raw CSV), the transform job
(writes core.observations), and the SQL DDL (sql/schemas/02_core_observations.sql)
stay in sync. If a column is added or renamed, update all three.
"""
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    ShortType,
    TimestampType,
)

# Raw CSV schema, in file order. Read explicitly (not inferSchema) so a
# malformed row fails loudly instead of silently becoming null/garbage.
RAW_SCHEMA = StructType(
    [
        StructField("timestamp", StringType(), nullable=False),
        StructField("time_of_day", StringType(), nullable=True),
        StructField("day_of_week", StringType(), nullable=True),
        StructField("gps_latitude", DoubleType(), nullable=True),
        StructField("gps_longitude", DoubleType(), nullable=True),
        StructField("road_segment_id", IntegerType(), nullable=True),
        StructField("distance_to_intersection", DoubleType(), nullable=True),
        StructField("vehicle_count", IntegerType(), nullable=True),
        StructField("average_speed", DoubleType(), nullable=True),
        StructField("lane_occupancy_rate", DoubleType(), nullable=True),
        StructField("jam_density_index", DoubleType(), nullable=True),
        StructField("hard_braking_events", IntegerType(), nullable=True),
        StructField("rapid_acceleration_events", IntegerType(), nullable=True),
        StructField("lane_changes_per_minute", DoubleType(), nullable=True),
        StructField("stop_duration_avg", DoubleType(), nullable=True),
        StructField("weather_condition", StringType(), nullable=True),
        StructField("visibility_range", DoubleType(), nullable=True),
        StructField("road_surface_status", StringType(), nullable=True),
        StructField("v2x_packet_loss_rate", DoubleType(), nullable=True),
        StructField("v2v_beacon_interval_avg", DoubleType(), nullable=True),
        StructField("v2x_message_delay_avg", DoubleType(), nullable=True),
        StructField("anomaly_label", ShortType(), nullable=True),
        StructField("incident_type", ShortType(), nullable=True),
    ]
)

RAW_COLUMNS = [f.name for f in RAW_SCHEMA.fields]

# Columns added by the transform job, in the order they are appended.
DERIVED_COLUMNS = [
    "incident_name",
    "obs_date",
    "obs_year",
    "obs_month",
    "obs_hour",
    "weekday_from_ts",
    "is_weekend",
    "window_15min",
    "tod_mismatch",
    "dow_mismatch",
    "label_consistent",
]

# Full column order for core.observations, matching sql/schemas/02_core_observations.sql.
CORE_COLUMNS = ["observed_at"] + RAW_COLUMNS[1:] + DERIVED_COLUMNS

# Plausible-range bounds used by the quality job (spark/jobs/quality_checks.py).
# These are loose sanity bounds, not the definition of an "anomaly" — a row
# outside range is flagged for review, not silently dropped.
RANGE_CHECKS = {
    "gps_latitude": (33.0, 34.0),
    "gps_longitude": (72.5, 73.5),
    "vehicle_count": (0, 100),
    "average_speed": (0, 200),
    "lane_occupancy_rate": (0.0, 1.0),
    "v2x_packet_loss_rate": (0.0, 100.0),
    "anomaly_label": (0, 1),
    "incident_type": (0, 4),
}
