# Data

## traffictab23 (required for this week's ETL scope)

1. Download from Kaggle: https://www.kaggle.com/datasets/datasetengineer/traffictab23
2. Place the CSV at `data/raw/traffictab23.csv` (this exact path — the DAG
   and `spark/jobs/ingest.py` both expect it).
3. Do not commit it. `data/raw/*` is gitignored except `.gitkeep`; the file
   is large and everyone can download their own copy.

## Columns (raw CSV, in order)

`timestamp, time_of_day, day_of_week, gps_latitude, gps_longitude,
road_segment_id, distance_to_intersection, vehicle_count, average_speed,
lane_occupancy_rate, jam_density_index, hard_braking_events,
rapid_acceleration_events, lane_changes_per_minute, stop_duration_avg,
weather_condition, visibility_range, road_surface_status,
v2x_packet_loss_rate, v2v_beacon_interval_avg, v2x_message_delay_avg,
anomaly_label, incident_type`

Size: ~2.1M rows (30-second interval, 2022-01-01 to 2024-01-01), ~101
distinct `road_segment_id` values.

Labels (documented by the dataset, confirmed — see `config/incident_types.yaml`):
- `anomaly_label`: 0 = normal, 1 = anomalous (an incident occurred)
- `incident_type`: 0 = normal, 1 = minor collision, 2 = major accident,
  3 = signal disruption, 4 = road blockage

## Known data-quality issues (found during manual audit, before this ETL was built)

These are why the transform step derives `weekday_from_ts`/`obs_hour` from
the timestamp instead of trusting `time_of_day`/`day_of_week`, and flags
mismatches (`tod_mismatch`, `dow_mismatch`) rather than silently
overwriting the raw columns:

- `time_of_day` and `day_of_week` do not reliably match the hour/weekday
  implied by `timestamp` in spot checks.
- `gps_latitude`/`gps_longitude` do not appear to be a stable per-
  `road_segment_id` location — the same segment ID shows up at different
  coordinates across rows. This matters for `meta.road_positions` (used by
  the dashboard map later): don't assume GPS-per-row implies a fixed
  segment location without checking (`std(lat/lon)` grouped by
  `road_segment_id`).
- Several numeric columns (e.g. `jam_density_index`, `lane_occupancy_rate`,
  the coordinates, several V2X fields) have suspiciously flat/uniform
  distributions — a sign the dataset may be synthetic. This does not block
  the ETL, but it does affect what dashboard features are safe to build
  later (see the project's `docs/keterbatasan.md` once written).

`dag_quality` / `spark/jobs/quality_checks.py` records null counts,
out-of-range values, and the `tod_mismatch`/`dow_mismatch`/label-leakage
rates into `meta.quality_results` on every run, so these don't need to be
re-discovered by hand each time.

## pollution dataset

Not part of this week's ETL scope (traffictab23 only, per the course
requirement). Kaggle: https://www.kaggle.com/datasets/hajramohsin/pakistan-air-quality-pollutant-concentrations
