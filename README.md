# ProjectAnbig-MetropoliaCity

Smart-city big data course project: traffic congestion / incident analysis
from the traffictab23 dataset (Islamabad-Rawalpindi, 2022-2024), building
toward a dashboard for government analysts and field officers.

**Status: ETL, data-quality audit, and dashboard-ready `mart` tables are
complete.** The Streamlit dashboard itself has not been started yet — see
[Status](#status) below for the exact breakdown.

## Stack

- **PySpark** (local mode, no separate Spark cluster) — ETL (`ingest`,
  `transform_load`, `quality_checks`)
- **Airflow** — orchestrates the ETL DAG (`etl_traffic`)
- **PostgreSQL** — stores everything (`core`, `mart`, `meta` schemas)
- **Docker Compose** — runs Postgres + Airflow
- **Jupyter** (run locally, outside Docker) — the analytical audit notebook
- **DBeaver** (optional) — GUI client for browsing the database; see
  [Inspecting the database with DBeaver](#inspecting-the-database-with-dbeaver)

## Quickstart

```bash
cp .env.example .env
# edit .env: set real passwords, and AIRFLOW_UID to `id -u` on Linux

# put the dataset in place (see data/README.md)
mkdir -p data/raw
# copy traffictab23.csv into data/raw/

docker compose up -d --build
# Airflow UI: http://localhost:8080 (user/pass from .env)
# trigger the "etl_traffic" DAG from the UI, or:
docker compose exec airflow-scheduler airflow dags trigger etl_traffic
```

**Postgres port note:** if port `5432` on your machine is already taken by
a native PostgreSQL install (common on Windows — check with
`netstat -ano | findstr :5432`), the `postgres` service in
`docker-compose.yml` is mapped to host port **`5433`** instead
(`"5433:5432"`). Connect external tools (DBeaver, notebooks, etc.) to
`127.0.0.1:5433`, not `5432`. Commands run *inside* the containers
(`docker compose exec ...`) are unaffected — they always use the internal
port `5432`.

Check ETL results:

```bash
docker compose exec postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "select count(*) from core.observations;"
docker compose exec postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "select category, metric_name, value, detail from meta.quality_results order by run_ts desc limit 20;"
docker compose exec postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "select alert_level, count(*) from mart.road_current_status group by 1;"
```

## Data model

Three Postgres schemas, each with a different job:

### `core` — cleaned, granular data (ETL output)

- **`core.observations`** — one row per raw observation (~2.1M rows), the
  23 original columns plus columns derived during transform (`observed_at`,
  `weekday_from_ts`, `obs_hour`, `incident_name`, `is_weekend`,
  `window_15min`) and data-quality flags (`tod_mismatch`, `dow_mismatch`,
  `label_consistent`). See `spark/jobs/transform.py` for exactly what each
  derived column means and why it exists.

### `mart` — dashboard-ready aggregates, built from `core.observations`

Built with plain SQL (`sql/mart/*.sql`), not Spark — these are aggregations
over data already in Postgres, no need to re-read raw files. **Not yet
wired into the Airflow DAG** — rerun the build scripts manually after any
ETL rerun (see [Known limitation](#known-limitations--gaps) below).

| Table/View | Grain | Purpose |
|---|---|---|
| `mart.road_window_stats` | `road_segment_id` × 15-min window | Granular rollup for time-series drill-down / future modelling |
| `mart.road_hourly_stats` | `road_segment_id` × date × hour | Descriptive statistics per hour, for trend charts |
| `mart.hour_of_week_baseline` | `road_segment_id` × day-of-week × hour | Historical mean/std of `anomaly_rate`, used to compute z-scores |
| `mart.road_current_status` (view) | `road_segment_id` (101 rows) | Latest status per segment — `alert_level` (kritis/waspada/perhatian/normal) and `v2x_status` (kritis/waspada/normal), ready for map coloring |

To rebuild the mart tables after an ETL run:

```bash
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < sql/mart/build_road_window_stats.sql
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < sql/mart/build_road_hourly_stats.sql
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < sql/mart/build_hour_of_week_baseline.sql
```

(`mart.road_current_status` is a `VIEW`, so it updates automatically once
the tables above are refreshed — no separate rebuild step.)

### `meta` — data about the data

- **`meta.quality_results`** — one row per metric per ETL run (row counts,
  null counts, duplicate checks, mismatch rates, `anomaly_rate`). Written
  by `spark/jobs/quality_checks.py` on every DAG run.
- **`meta.road_positions`** — map coordinates per `road_segment_id`
  (101 rows). Raw GPS in the source data is **not** a stable per-segment
  location (~4km spread — see `notebooks/road_gps_stability.py`), so this
  uses `source = 'illustrative_grid'`: segments spread evenly across a
  bounding box around Islamabad-Rawalpindi, for map-plotting only — **not**
  real road locations. Built by `notebooks/populate_road_positions.py`.

## Known limitations / gaps

Full write-up: [`docs/keterbatasan.md`](docs/keterbatasan.md). Headline
findings from the six-test analytical audit
(`notebooks/01_audit_data.ipynb`):

- `time_of_day` (raw) does not reliably match the hour derived from
  `timestamp` — pattern is systematic (fixed-rate, not random noise), not
  just corrupted. `day_of_week` (raw), however, always matches.
- `anomaly_label`/`incident_type` are internally 100% consistent, but
  carry **no correlation** with traffic metrics (vehicle count, speed,
  hard-braking, etc.) — treat as recorded incidents, not
  data-derived/validated anomaly detection.
- Most numeric columns (GPS, `jam_density_index`, `lane_occupancy_rate`,
  most V2X fields) look synthetic (uniform-distribution KS-test).
  `v2x_packet_loss_rate` is the one exception — its distribution looks
  like real sensor data.
- **`mart` tables are not wired into the Airflow DAG.** If `etl_traffic`
  reruns with new data, `core.observations` refreshes automatically
  (`mode="overwrite"`), but `mart.*` will go stale until the build scripts
  above are rerun manually. Fixing this (adding a DAG task) is on the
  to-do list.

## Config

- **`config/incident_types.yaml`** — maps `incident_type` code (0-4) to a
  human-readable name and who to contact. Used by `transform.py`.
- **`config/thresholds.yaml`** — V2X status thresholds
  (`packet_loss_pct`, `message_delay_ms`, each with `normal_max`/
  `waspada_max`) and alert-level rules (`kritis`, `waspada`, `perhatian`).
  Values were filled from p50/p90 percentiles found in the audit notebook
  (2026-09-21) and are mirrored into the `CASE WHEN` logic inside
  `sql/schemas/06_mart_road_current_status.sql` — **the two must be kept
  in sync manually** if thresholds are revisited.

## Inspecting the database with DBeaver

Optional GUI alternative to `docker compose exec postgres psql ...`.

1. Install [DBeaver Community](https://dbeaver.io/download/).
2. New PostgreSQL connection: host `127.0.0.1`, port `5433` (see the port
   note under Quickstart), database/user/password from `.env`.
3. Browse `Schemas` → `core` / `mart` / `meta` → `Tables` (or `Views` for
   `mart.road_current_status`) → double-click a table to see its data.
4. Right-click a schema → **View Diagram** for an ER-style overview of its
   tables and columns (there are no formal `FOREIGN KEY` constraints
   between schemas in this project — relationships are implicit via
   `road_segment_id` — so the diagram will show unconnected tables, which
   is expected).

## Repo layout

## Repo layout

```
.
├── airflow/
│   ├── dags/
│   │   └── dag_etl_traffic.py       DAG: ingest -> transform_load -> quality_checks
│   └── logs/                        (gitignored, generated by Airflow)
│
├── spark/
│   ├── jobs/
│   │   ├── ingest.py                Extract: CSV -> partitioned Parquet (typed schema, drops malformed rows)
│   │   ├── transform.py             Transform+Load: Parquet -> core.observations (derives weekday_from_ts, obs_hour, tod_mismatch, dow_mismatch, label_consistent, etc.)
│   │   └── quality_checks.py        Load-time checks -> meta.quality_results (row counts, nulls, duplicates, mismatch rates, anomaly_rate)
│   ├── common/
│   │   ├── schema.py                RAW_SCHEMA, RAW_COLUMNS, CORE_COLUMNS, RANGE_CHECKS — single source of truth for column names/types
│   │   └── spark_session.py         get_spark(), read/write helpers for Postgres (JDBC)
│   └── tests/
│       ├── test_ingest.py           Unit tests for ingest.py (no Postgres needed)
│       └── test_transform.py        Unit tests for transform.py (no Postgres needed)
│
├── sql/
│   ├── schemas/                     DDL — runs automatically on first `docker compose up` (docker-entrypoint-initdb.d)
│   │   ├── 00_create_airflow_db.sql
│   │   ├── 01_schemas.sql           CREATE SCHEMA core, mart, meta
│   │   ├── 02_core_observations.sql DDL for core.observations
│   │   ├── 03_meta_quality.sql      DDL for meta.quality_results, meta.road_positions
│   │   ├── 04_mart_road_window_stats.sql
│   │   ├── 05_mart_road_hourly_stats.sql
│   │   ├── 06_mart_road_current_status.sql   VIEW: alert_level + v2x_status per segment
│   │   └── 07_mart_hour_of_week_baseline.sql
│   └── mart/                        Populate scripts — NOT auto-run, rerun manually after each ETL run
│       ├── build_road_window_stats.sql
│       ├── build_road_hourly_stats.sql
│       └── build_hour_of_week_baseline.sql
│
├── notebooks/                       Run locally (not in Docker) — connect via psycopg2, not Spark
│   ├── road_gps_stability.py        One-off: confirms GPS is not stable per road_segment_id (~4km spread)
│   ├── populate_road_positions.py   Fills meta.road_positions (illustrative grid)
│   └── 01_audit_data.ipynb          Six-test analytical audit (temporal consistency, label consistency, spatial stability, distributional realism, range plausibility, signal correlation)
│
├── config/
│   ├── incident_types.yaml          incident_type code -> name -> who to contact
│   └── thresholds.yaml              V2X status thresholds + alert-level rules (values filled from audit percentiles)
│
├── docs/
│   └── keterbatasan.md              Data & pipeline limitations, audit findings, per-item status
│
├── data/
│   ├── README.md                    Where to get traffictab23.csv, known data-quality issues
│   ├── raw/                         traffictab23.csv goes here (gitignored)
│   └── parquet/                     ingest.py output (gitignored)
│
├── docker/
│   └── airflow/
│       └── Dockerfile               Airflow + Java 17 + PySpark + Postgres JDBC driver + psycopg2-binary
│
├── docker-compose.yml                Postgres + Airflow (webserver/scheduler/init) services
├── .env.example                      Template for .env (copy, fill in real passwords)
├── .gitignore
├── requirements-dev.txt              For running tests/notebooks locally, outside Docker
└── README.md                         This file
```

## Running tests locally (without Docker)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
PYTHONPATH=. pytest spark/tests/ -v
```

These tests exercise `spark/jobs/ingest.py` and `spark/jobs/transform.py`
directly (a local Spark session, no Postgres), so anyone can run them
before opening a PR against `sigma`.

### Running the audit notebook

`notebooks/01_audit_data.ipynb` and the two `notebooks/*.py` scripts
connect to Postgres directly (`psycopg2`), not through Spark, and are
meant to be run **locally**, not inside Docker:

```powershell
pip install -r requirements-dev.txt
jupyter notebook notebooks/01_audit_data.ipynb
```

They read connection details from `.env` via `python-dotenv`
(`load_dotenv()`); if running from a subdirectory, use
`load_dotenv(find_dotenv(usecwd=True))` to make sure `.env` is found.
Remember the port note above — connect to `127.0.0.1:5433`, or whichever
port your `postgres` service is actually mapped to.

## Git workflow

Branch `sigma` is the integration branch. Create a feature branch from it
(`feat/<short-name>`), open a PR back into `sigma`, get one review.
Suggested folder ownership to reduce merge conflicts:

- `docker/`, `docker-compose.yml`, `airflow/` — infra
- `spark/`, `sql/`, `config/` — pipeline
- `notebooks/`, `docs/`, `tests/` — analysis & docs
- `dashboard/` — later stage, not part of this week's scope

## Status

- [x] Repo skeleton, Docker Compose (Postgres + Airflow), Postgres schemas (`core`, `mart`, `meta`)
- [x] ETL: `ingest` (CSV -> partitioned Parquet), `transform_load` (-> `core.observations`), `quality_checks` (-> `meta.quality_results`)
- [x] Unit tests for ingest/transform logic (11 passing, run without Postgres)
- [x] Road positions (`meta.road_positions`, illustrative grid — raw GPS confirmed unstable per segment)
- [x] Full six-test analytical audit (`notebooks/01_audit_data.ipynb`)
- [x] `docs/keterbatasan.md` — limitations documented from audit findings
- [x] `config/thresholds.yaml` — filled from audit percentiles (p50/p90)
- [x] `mart` schema: `road_window_stats`, `road_hourly_stats`, `hour_of_week_baseline`, `road_current_status` (with `alert_level` + `v2x_status`)
- [ ] Wire `mart` build scripts into the Airflow DAG (currently manual — see [Known limitations](#known-limitations--gaps))
- [ ] Dashboard (Streamlit) — not started