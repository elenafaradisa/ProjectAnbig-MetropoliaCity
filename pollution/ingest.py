"""
pollution/ingest.py

Reads the two Islamabad air-quality + weather source files, normalizes them
to core.pollution_hourly's schema, tags provenance (data_split, source_file),
and loads them into Postgres.

Overlap policy (the two batches share ~3,623 hours):
  1. Insert ALL training rows first (data_split='training').
  2. Insert testing rows with ON CONFLICT (city, observed_at) DO NOTHING --
     rows already covered by training are skipped, so only testing's
     genuinely new hours (mostly Dec 2024, plus a few gaps training was
     missing) land as data_split='testing'.
This gives a leakage-free held-out set: any row tagged 'testing' in the
table was NOT present in training.

Usage (run inside the airflow-scheduler container, same convention as
spark/jobs/ingest.py):
    python pollution/ingest.py \
        --training /opt/project/data/raw/pollution/islamabad_training.xlsx \
        --testing  /opt/project/data/raw/pollution/islamabad_testing.csv
"""
import argparse
import os
import sys

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

CITY = "Islamabad"

# must match core.pollution_hourly column order (minus id/ingested_at, which
# are auto-filled by the table's defaults)
TARGET_COLUMNS = [
    "city", "observed_at",
    "main_aqi",
    "components_co", "components_no", "components_no2", "components_o3",
    "components_so2", "components_pm2_5", "components_pm10", "components_nh3",
    "temperature_2m", "relative_humidity_2m", "dew_point_2m", "precipitation",
    "surface_pressure", "wind_speed_10m", "wind_direction_10m", "shortwave_radiation",
    "data_split", "source_file",
]


def load_training(path: str) -> pd.DataFrame:
    df = pd.read_excel(path)
    # training file uses dotted column names (main.aqi, components.co, ...)
    df.columns = [c.replace(".", "_") for c in df.columns]
    df["observed_at"] = pd.to_datetime(df["datetime"])
    df["data_split"] = "training"
    df["source_file"] = os.path.basename(path)
    return df


def load_testing(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    # testing file's datetime column mixes "d/m/Y H:M" and "d/m/Y H:M:S"
    df["observed_at"] = pd.to_datetime(df["datetime"], format="mixed", dayfirst=True)
    df["data_split"] = "testing"
    df["source_file"] = os.path.basename(path)
    return df


def finalize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["city"] = CITY
    df = df[TARGET_COLUMNS]
    # cast to object BEFORE replacing NaN with None: on a numeric dtype,
    # assigning None gets silently coerced back to float NaN, which
    # postgres would store as the float special value 'NaN' instead of
    # SQL NULL. object dtype holds None as a real Python None, and also
    # boxes numpy int64/float64 into plain Python int/float that psycopg2
    # knows how to adapt directly.
    df = df.astype(object)
    df = df.where(df.notnull(), None)
    return df


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "metropolia"),
        user=os.environ.get("POSTGRES_USER", "metropolia"),
        password=os.environ.get("POSTGRES_PASSWORD", "metropolia"),
    )


def count_by_split(conn, split: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM core.pollution_hourly WHERE data_split = %s", (split,)
        )
        return cur.fetchone()[0]


def upsert_training(conn, df: pd.DataFrame) -> None:
    # DO UPDATE (not DO NOTHING): training is the authoritative source, and
    # this keeps the script safe to re-run (Airflow retries, manual reruns
    # after fixing something upstream) without hitting the unique-constraint
    # error a plain INSERT would raise on the second run.
    update_cols = [c for c in TARGET_COLUMNS if c not in ("city", "observed_at")]
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    with conn.cursor() as cur:
        execute_values(
            cur,
            f"""INSERT INTO core.pollution_hourly ({', '.join(TARGET_COLUMNS)}) VALUES %s
                ON CONFLICT (city, observed_at) DO UPDATE SET {set_clause}""",
            list(df.itertuples(index=False, name=None)),
        )
    conn.commit()


def insert_testing(conn, df: pd.DataFrame) -> None:
    with conn.cursor() as cur:
        execute_values(
            cur,
            f"""INSERT INTO core.pollution_hourly ({', '.join(TARGET_COLUMNS)}) VALUES %s
                ON CONFLICT (city, observed_at) DO NOTHING""",
            list(df.itertuples(index=False, name=None)),
        )
    conn.commit()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--training", required=True, help="path to islamabad_training.xlsx")
    parser.add_argument("--testing", required=True, help="path to islamabad_testing.csv")
    args = parser.parse_args()

    train = finalize(load_training(args.training))
    test = finalize(load_testing(args.testing))

    print(f"Training: {len(train)} rows, {train['observed_at'].min()} .. {train['observed_at'].max()}")
    print(f"Testing:  {len(test)} rows, {test['observed_at'].min()} .. {test['observed_at'].max()}")

    conn = get_connection()
    try:
        before_train = count_by_split(conn, "training")
        before_test = count_by_split(conn, "testing")

        upsert_training(conn, train)
        insert_testing(conn, test)

        after_train = count_by_split(conn, "training")
        after_test = count_by_split(conn, "testing")
    finally:
        conn.close()

    print(f"core.pollution_hourly training rows: {after_train} ({after_train - before_train} new)")
    print(f"core.pollution_hourly testing rows:  {after_test} ({after_test - before_test} new)")

    if after_train == 0 and after_test == 0:
        sys.exit("Ingest produced 0 rows -- check --training/--testing paths.")


if __name__ == "__main__":
    main()