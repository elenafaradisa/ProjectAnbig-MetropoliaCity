# ProjectAnbig-MetropoliaCity

Smart-city big data course project: traffic congestion / incident analysis
from the traffictab23 dataset (Islamabad-Rawalpindi, 2022-2024), building
toward a dashboard for government analysts and field officers.

**This week's scope: ETL only** (extract CSV -> transform/clean -> load to
PostgreSQL, plus load-time quality checks). Dashboard, marts, and the full
six-test analytical audit come later, once ETL is reviewed.

## Stack

PySpark (local mode, no separate Spark cluster) orchestrated by Airflow,
loading into PostgreSQL, all run via Docker Compose.

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

Check results:

```bash
docker compose exec postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "select count(*) from core.observations;"
docker compose exec postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "select category, metric_name, value, detail from meta.quality_results order by run_ts desc limit 20;"
```

## Repo layout

```
airflow/dags/dag_etl_traffic.py   ingest -> transform_load -> quality_checks
spark/jobs/                        the three ETL jobs (Extract, Transform+Load, quality)
spark/common/                      shared schema, Spark session, Postgres helpers
spark/tests/                       pytest unit tests (no Postgres needed)
sql/schemas/                       Postgres DDL, run automatically on first `docker compose up`
config/                            incident_types.yaml (label -> name -> who to contact),
                                    thresholds.yaml (placeholder for the dashboard stage)
data/README.md                     where to get the dataset, known data-quality issues
docker/airflow/Dockerfile          Airflow + Java + PySpark + Postgres JDBC driver
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
- [ ] Full six-test analytical audit (notebooks/01_audit_data.ipynb) — next stage
- [ ] `mart` tables, dashboard (Streamlit) — later stage
