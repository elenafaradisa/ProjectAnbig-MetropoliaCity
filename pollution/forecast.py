"""
pollution/forecast.py

Loads the model saved by ml/train_pollution_model.py, scores it over all of
core.pollution_hourly (using the exact same feature logic -- see
pollution/features.py), and upserts predictions into mart.pollution_forecast.

This runs as an Airflow task (unlike training, which is manual -- see
ml/train_pollution_model.py's docstring for why). It writes abs_error
wherever the actual PM2.5 is known, which here is always, since this is
backtesting over historical data rather than a live unobserved future hour;
a real deployment would simply have actual_pm2_5 NULL for the newest row
until it's observed.

Usage:
    python -m pollution.forecast --model-dir /opt/project/data/models
"""
import argparse
import json
import os
import warnings

import pandas as pd
import psycopg2
from joblib import load
from psycopg2.extras import execute_values

from pollution.features import FEATURE_COLUMNS, build_feature_frame

warnings.filterwarnings(
    "ignore", message="pandas only supports SQLAlchemy", category=UserWarning
)

TABLE = "core.pollution_hourly"


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "metropolia"),
        user=os.environ.get("POSTGRES_USER", "metropolia"),
        password=os.environ.get("POSTGRES_PASSWORD", "metropolia"),
    )


def load_raw(conn) -> pd.DataFrame:
    return pd.read_sql(
        f"SELECT city, observed_at, data_split, components_pm2_5, "
        f"temperature_2m, relative_humidity_2m, wind_speed_10m, precipitation "
        f"FROM {TABLE} ORDER BY observed_at",
        conn,
    )


def upsert_forecast(conn, df: pd.DataFrame) -> None:
    cols = ["city", "observed_at", "data_split", "actual_pm2_5",
            "predicted_pm2_5", "abs_error", "model_version"]
    rows = list(df[cols].itertuples(index=False, name=None))
    with conn.cursor() as cur:
        execute_values(
            cur,
            f"""INSERT INTO mart.pollution_forecast ({', '.join(cols)}) VALUES %s
                ON CONFLICT (city, observed_at, model_version) DO UPDATE SET
                    data_split = EXCLUDED.data_split,
                    actual_pm2_5 = EXCLUDED.actual_pm2_5,
                    predicted_pm2_5 = EXCLUDED.predicted_pm2_5,
                    abs_error = EXCLUDED.abs_error,
                    predicted_at = now()""",
            rows,
        )
    conn.commit()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-dir", default=os.environ.get("MODEL_DIR", "data/models"))
    args = parser.parse_args()

    model_path = os.path.join(args.model_dir, "pollution_pm25_forecast.joblib")
    meta_path = os.path.join(args.model_dir, "pollution_pm25_forecast.meta.json")
    if not os.path.exists(model_path):
        raise SystemExit(
            f"No model found at {model_path}. Run `python -m ml.train_pollution_model` first."
        )

    model = load(model_path)
    with open(meta_path) as f:
        meta = json.load(f)
    model_version = meta["model_version"]

    conn = get_connection()
    try:
        raw = load_raw(conn)
        feat = build_feature_frame(raw)
        clean = feat.dropna(subset=FEATURE_COLUMNS)  # keep rows even if target_pm2_5 is NaN (future/live rows)

        predictions = model.predict(clean[FEATURE_COLUMNS])

        out = clean.reset_index()[["observed_at", "city", "data_split", "target_pm2_5"]].copy()
        out = out.rename(columns={"target_pm2_5": "actual_pm2_5"})
        # a gap hour (no real observation at all -- see quality_checks'
        # missing_hourly_gaps metric) has no data_split, since it was never
        # really 'training' or 'testing'. Label it honestly rather than
        # guessing: still predictable (lag features come from real
        # neighboring hours), just never observed.
        out["data_split"] = out["data_split"].fillna("gap_fill")
        out["predicted_pm2_5"] = predictions
        out["abs_error"] = (out["actual_pm2_5"] - out["predicted_pm2_5"]).abs()
        out["model_version"] = model_version
        out = out.astype(object).where(out.notnull(), None)

        upsert_forecast(conn, out)
    finally:
        conn.close()

    n_with_actual = int(out["actual_pm2_5"].notna().sum())
    print(f"Scored {len(out)} hours with model_version={model_version}")
    print(f"  {n_with_actual} had a known actual_pm2_5 (abs_error computed)")
    print(f"  {len(out) - n_with_actual} had no actual yet (predicted_pm2_5 only)")

    testing_rows = out[out["data_split"] == "testing"]
    if len(testing_rows):
        print(f"  Held-out testing MAE (sanity check, should match training run): "
              f"{testing_rows['abs_error'].mean():.2f} ug/m3")


if __name__ == "__main__":
    main()