"""
pollution/quality_checks.py

Load-integrity checks for core.pollution_hourly -- did the ingest work
correctly? (Not: is the data realistic -- that's a separate, later concern.)
Mirrors spark/jobs/quality_checks.py's category scheme (integrity, nulls,
range, consistency, audit) and writes to the same meta.quality_results
table, so both pipelines' results sit side by side and are comparable.

Usage:
    python -m pollution.quality_checks --run-id 2026-09-30T04:42
"""
import argparse
import os
import uuid
import warnings
from datetime import datetime, timezone

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# pandas warns that a raw psycopg2 connection (vs. a SQLAlchemy engine) is
# "untested" for read_sql -- it works fine here, this just silences the noise.
warnings.filterwarnings(
    "ignore", message="pandas only supports SQLAlchemy", category=UserWarning
)

TABLE = "core.pollution_hourly"

# (column, low, high) -- physically/definitionally impossible outside this
# range; None means "no bound on that side". main_aqi is OpenWeatherMap's
# discrete 1-5 index; the rest are concentrations/measurements that can't
# be negative (except temperature, which isn't range-checked here).
RANGE_CHECKS = {
    "main_aqi": (1, 5),
    "components_co": (0, None),
    "components_no": (0, None),
    "components_no2": (0, None),
    "components_o3": (0, None),
    "components_so2": (0, None),
    "components_pm2_5": (0, None),
    "components_pm10": (0, None),
    "components_nh3": (0, None),
    "relative_humidity_2m": (0, 100),
    "wind_direction_10m": (0, 360),
    "wind_speed_10m": (0, None),
    "precipitation": (0, None),
}

NULLABLE_CHECK_COLUMNS = [
    "main_aqi", "components_co", "components_no", "components_no2",
    "components_o3", "components_so2", "components_pm2_5", "components_pm10",
    "components_nh3", "temperature_2m", "relative_humidity_2m", "dew_point_2m",
    "precipitation", "surface_pressure", "wind_speed_10m", "wind_direction_10m",
    "shortwave_radiation",
]

# ingest.py tags rows with os.path.basename(path) -- kept in sync with the
# filenames the etl_pollution DAG passes to --training/--testing.
EXPECTED_SOURCE_FILE = {
    "training": "islamabad_training.xlsx",
    "testing": "islamabad_testing.csv",
}


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "metropolia"),
        user=os.environ.get("POSTGRES_USER", "metropolia"),
        password=os.environ.get("POSTGRES_PASSWORD", "metropolia"),
    )


def write_results(conn, results: list) -> None:
    if not results:
        return
    rows = [(r["run_id"], r["category"], r["metric_name"], r["value"], r["detail"]) for r in results]
    with conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO meta.quality_results (run_id, category, metric_name, value, detail) VALUES %s",
            rows,
        )
    conn.commit()


def run(run_id: str, conn=None) -> list:
    owns_conn = conn is None
    if owns_conn:
        conn = get_connection()

    df = pd.read_sql(f"SELECT * FROM {TABLE}", conn)
    results = []

    def record(category, metric_name, value, detail=""):
        results.append({
            "run_id": run_id,
            "category": category,
            "metric_name": metric_name,
            "value": float(value) if value is not None else None,
            "detail": detail,
        })

    total = len(df)
    record("integrity", "row_count", total)

    for split in ("training", "testing"):
        n = int((df["data_split"] == split).sum())
        record("integrity", f"row_count.{split}", n)

    dup = int(df.duplicated(subset=["city", "observed_at"]).sum())
    record("integrity", "duplicate_city_observed_at", dup,
           "rows sharing (city, observed_at) -- should be 0, enforced by a unique constraint")

    invalid_split = int((~df["data_split"].isin(["training", "testing"])).sum())
    record("integrity", "invalid_data_split_values", invalid_split,
           "rows where data_split isn't 'training' or 'testing'")

    if total:
        full_range = pd.date_range(df["observed_at"].min(), df["observed_at"].max(), freq="h")
        missing_hours = len(full_range) - df["observed_at"].nunique()
        record("integrity", "missing_hourly_gaps", missing_hours,
               f"hours with no row between {df['observed_at'].min()} and {df['observed_at'].max()}")

    for col in NULLABLE_CHECK_COLUMNS:
        null_count = int(df[col].isnull().sum())
        if null_count > 0:
            record("nulls", f"null_count.{col}", null_count, f"{null_count}/{total} rows")

    for col, (lo, hi) in RANGE_CHECKS.items():
        mask = pd.Series(False, index=df.index)
        if lo is not None:
            mask = mask | (df[col] < lo)
        if hi is not None:
            mask = mask | (df[col] > hi)
        out_of_range = int(mask.sum())
        if out_of_range > 0:
            bound = f"[{lo if lo is not None else '-inf'}, {hi if hi is not None else 'inf'}]"
            record("range", f"out_of_range.{col}", out_of_range, f"expected {bound}")

    bad_provenance = int((
        ((df["data_split"] == "training") & (df["source_file"] != EXPECTED_SOURCE_FILE["training"])) |
        ((df["data_split"] == "testing") & (df["source_file"] != EXPECTED_SOURCE_FILE["testing"]))
    ).sum())
    record("consistency", "source_file_mismatch", bad_provenance,
           "rows whose source_file doesn't match its data_split's expected filename")

    for split in ("training", "testing"):
        subset = df.loc[df["data_split"] == split, "components_pm2_5"]
        if len(subset):
            record("audit", f"components_pm2_5_mean.{split}", subset.mean(),
                   f"n={len(subset)} -- sanity check that the held-out testing distribution "
                   "isn't wildly different from training")

    print(f"Quality check run {run_id}: {len(results)} metrics recorded.")
    for r in results:
        print(f"  [{r['category']}] {r['metric_name']} = {r['value']}  {r['detail']}")

    write_results(conn, results)

    if owns_conn:
        conn.close()
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()
    run_id = args.run_id or f"{datetime.now(timezone.utc).isoformat()}-{uuid.uuid4().hex[:8]}"
    run(run_id)


if __name__ == "__main__":
    main()