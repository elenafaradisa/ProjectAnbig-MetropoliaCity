from datetime import datetime

from pyspark.sql import functions as F

from spark.jobs.transform import transform_df

INCIDENT_NAMES = {0: "Normal", 1: "Minor collision", 2: "Major accident",
                   3: "Signal disruption", 4: "Road blockage"}

RAW_COLUMNS = [
    "timestamp", "time_of_day", "day_of_week", "gps_latitude", "gps_longitude",
    "road_segment_id", "distance_to_intersection", "vehicle_count", "average_speed",
    "lane_occupancy_rate", "jam_density_index", "hard_braking_events",
    "rapid_acceleration_events", "lane_changes_per_minute", "stop_duration_avg",
    "weather_condition", "visibility_range", "road_surface_status",
    "v2x_packet_loss_rate", "v2v_beacon_interval_avg", "v2x_message_delay_avg",
    "anomaly_label", "incident_type",
]


def make_row(**overrides):
    # timestamp is a string here (like the raw CSV) and parsed below via
    # to_timestamp, the same way ingest.py parses it — NOT a Python datetime
    # object, whose interpretation by Spark depends on the driver machine's
    # OS timezone and gave wrong weekday/hour results on a non-UTC sandbox.
    base = dict(
        timestamp="2022-01-01 00:03:00",           # hour 0 -> derived bucket "Night"
        time_of_day="Evening",                      # raw label disagrees -> mismatch expected
        day_of_week="Saturday",                      # matches weekday_from_ts (Sat) -> no mismatch
        gps_latitude=33.58, gps_longitude=72.98,
        road_segment_id=1020, distance_to_intersection=120.9,
        vehicle_count=12, average_speed=64.2,
        lane_occupancy_rate=0.96, jam_density_index=88.2,
        hard_braking_events=0, rapid_acceleration_events=0,
        lane_changes_per_minute=2.6, stop_duration_avg=60.1,
        weather_condition="Clear", visibility_range=2020.2,
        road_surface_status="Dry",
        v2x_packet_loss_rate=48.5, v2v_beacon_interval_avg=0.63,
        v2x_message_delay_avg=84.9,
        anomaly_label=1, incident_type=3,
    )
    base.update(overrides)
    return tuple(base[c] for c in RAW_COLUMNS)


def make_df(spark, **overrides):
    df = spark.createDataFrame([make_row(**overrides)], RAW_COLUMNS)
    return df.withColumn("timestamp", F.to_timestamp("timestamp"))


def test_transform_adds_all_derived_columns(spark):
    out = transform_df(make_df(spark), INCIDENT_NAMES)
    for col in ["incident_name", "obs_date", "obs_year", "obs_month", "obs_hour",
                "weekday_from_ts", "is_weekend", "window_15min", "tod_mismatch",
                "dow_mismatch", "label_consistent"]:
        assert col in out.columns


def test_incident_name_lookup(spark):
    out = transform_df(make_df(spark, incident_type=3), INCIDENT_NAMES)
    assert out.collect()[0]["incident_name"] == "Signal disruption"


def test_weekday_and_hour_derived_from_timestamp_not_raw_columns(spark):
    # 2022-01-01 is a Saturday; timestamp hour is 0.
    row = transform_df(make_df(spark), INCIDENT_NAMES).collect()[0]
    assert row["weekday_from_ts"] == "Saturday"
    assert row["obs_hour"] == 0
    assert row["obs_year"] == 2022
    assert row["obs_month"] == 1
    assert row["is_weekend"] is True


def test_tod_mismatch_flagged_when_raw_label_disagrees(spark):
    # raw time_of_day="Evening" but hour=0 derives to "Night" -> mismatch
    row = transform_df(make_df(spark, time_of_day="Evening"), INCIDENT_NAMES).collect()[0]
    assert row["tod_mismatch"] is True


def test_dow_mismatch_false_when_raw_label_agrees(spark):
    row = transform_df(make_df(spark, day_of_week="Saturday"), INCIDENT_NAMES).collect()[0]
    assert row["dow_mismatch"] is False


def test_label_consistent_true_when_anomaly_and_incident_type_agree(spark):
    df1 = make_df(spark, anomaly_label=1, incident_type=3)
    df2 = make_df(spark, anomaly_label=0, incident_type=0)
    rows = transform_df(df1.union(df2), INCIDENT_NAMES).collect()
    assert all(r["label_consistent"] for r in rows)


def test_label_consistent_false_when_anomaly_and_incident_type_disagree(spark):
    # anomaly_label says "no incident" but incident_type says otherwise -> leakage/error case
    row = transform_df(
        make_df(spark, anomaly_label=0, incident_type=2), INCIDENT_NAMES
    ).collect()[0]
    assert row["label_consistent"] is False


def test_window_15min_floors_to_quarter_hour(spark):
    # Compare as a formatted string, computed inside Spark. Pulling a
    # TimestampType into a Python datetime via collect() re-interprets it in
    # the *driver machine's* OS timezone (not spark.sql.session.timeZone),
    # so a naive Python-side datetime(...) comparison is host-dependent and
    # was flaky between UTC and non-UTC machines.
    out = transform_df(make_df(spark, timestamp="2022-01-01 08:07:30"), INCIDENT_NAMES)
    row = out.select(F.date_format("window_15min", "yyyy-MM-dd HH:mm:ss").alias("w")).collect()[0]
    assert row["w"] == "2022-01-01 08:00:00"
