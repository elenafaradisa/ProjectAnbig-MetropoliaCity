# Data

Data is **not stored on GitHub** (it is too large). Each team member downloads it and places it at
the paths below. The paths must match exactly, because the DAGs and Spark jobs read them.

| Data | Source | Location | Used by |
|---|---|---|---|
| traffictab23 | Kaggle | `data/raw/traffictab23.csv` | DAG `etl_traffic` |
| Islamabad air pollution | Kaggle | `data/raw/pollution` | DAG `etl_pollution` |
| Islamabad road geometry (OSM) | Overpass API | ⚠️ FILL IN: path of `islamabad_roads_overpass.json` | `geo/assign_osm_roads.py` |

Generated automatically by the pipelines (**no download needed**): `data/parquet/`,
`data/checkpoints/`, `data/models/` (pollution model, ~179 MB), `data/run_stats/`.
All of them are in `.gitignore`.

---

## 1. traffictab23

1. Download from Kaggle: https://www.kaggle.com/datasets/datasetengineer/traffictab23
2. Place the CSV at `data/raw/traffictab23.csv` (this exact name and path).
3. Do not commit it. `data/raw/*` is gitignored except `.gitkeep`.

Size: ~2.1M rows (30-second interval, 2022-01-01 to 2024-01-01), ~101 distinct `road_segment_id` values.

### Columns (raw CSV, in order)

`timestamp, time_of_day, day_of_week, gps_latitude, gps_longitude, road_segment_id,
distance_to_intersection, vehicle_count, average_speed, lane_occupancy_rate, jam_density_index,
hard_braking_events, rapid_acceleration_events, lane_changes_per_minute, stop_duration_avg,
weather_condition, visibility_range, road_surface_status, v2x_packet_loss_rate,
v2v_beacon_interval_avg, v2x_message_delay_avg, anomaly_label, incident_type`

### Labels (as documented by the dataset, see `config/incident_types.yaml`)

- `anomaly_label`: 0 = normal, 1 = anomalous (an incident occurred)
- `incident_type`: 0 = normal, 1 = minor collision, 2 = major accident, 3 = signal disruption, 4 = road blockage

### Known data-quality issues (found during audit)

These are why the transform step derives `weekday_from_ts` and `obs_hour` from the timestamp, and
flags mismatches (`tod_mismatch`, `dow_mismatch`) instead of silently overwriting the raw columns:

- `time_of_day` and `day_of_week` do not reliably match the hour/weekday implied by `timestamp`.
- `gps_latitude`/`gps_longitude` are **not a stable location** per `road_segment_id`: the same
  segment ID appears at different coordinates across rows. For this reason segment locations on the
  map are not taken from GPS but from a mapping to OSM roads (see section 3).
- Many numeric columns (`jam_density_index`, `lane_occupancy_rate`, the coordinates, several V2X
  fields) have flat/uniform distributions, a sign that the dataset is **synthetic**. Further
  findings: `jam_density_index` is uncorrelated with `average_speed` (r ≈ −0.002), and there is no
  rush-hour pattern. Details are in `docs/keterbatasan.md`.

On every run, `spark/jobs/quality_checks.py` records null counts, out-of-range values, and the
`tod_mismatch`/`dow_mismatch` rates into `meta.quality_results`, so these do not need to be
re-audited by hand. The results are shown in the dashboard (Data Quality page).

---

## 2. Air pollution data (Islamabad)

Air pollution data is already on the raw folder

Summary (from the latest quality-check run):

- ~28,990 hourly rows for Islamabad: 28,245 training and 745 testing.
- Time range 2021-08-24 to 2024-12-31, with 434 missing hours (not imputed; predictions for those
  hours are labelled `gap_fill` and excluded from MAE).
- Mean PM2.5 of the test data (~213.6) is much higher than that of the training data (~122.6):
  the distributions differ, so test-set MAE should be read with care.

Pipeline: the `etl_pollution` DAG loads the data into `core.pollution_hourly`, runs the quality
checks (`pollution/quality_checks.py`), trains a RandomForest model (`ml/train_pollution_model.py`),
and writes forecasts to `mart.pollution_forecast`. The model is saved in `data/models/`.

> Traffic and pollution are two independent datasets from different sources: each is processed
> by its own DAG and they are not merged.

---

## 3. Road geometry (OpenStreetMap)

`geo/assign_osm_roads.py` fetches named roads in Islamabad from the Overpass API and assigns each
`road_segment_id` to one road (random but deterministic, seed 42). The result is stored in
`meta.road_positions`.

- The main Overpass server often times out (504). The script uses mirror servers, but the safest
  option is to use the already downloaded file (`islamabad_roads_overpass.json`, ~1.3 MB) placed at
  ⚠️ FILL IN: JSON file path.
- **This mapping is illustrative.** The dataset does not say where the segments really are, so
  road names on the map are not the actual locations of the events.

---

## 4. Setup checklist on a new laptop

```powershell
copy .env.example .env                      # then fill in the passwords
# place the data as listed in the table above
docker compose up -d --build
docker compose exec airflow-scheduler airflow dags trigger etl_traffic
# wait until it succeeds (http://localhost:8080), then:
docker compose exec airflow-scheduler airflow dags trigger etl_pollution
```

Run the DAGs one at a time, not together (Docker memory is limited). The full guide is in the
`README.md` at the repository root.
