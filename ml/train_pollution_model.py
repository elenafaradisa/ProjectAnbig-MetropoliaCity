"""
ml/train_pollution_model.py

Trains a next-hour PM2.5 forecaster for Islamabad: predicts
components_pm2_5 at hour t using only information available by t-1 (see
pollution/features.py for the exact feature set and why it's leakage-free).

Run manually (Option A: training is not part of the Airflow DAG -- the data
is historical and doesn't change on its own, so retraining on every DAG run
would just burn CPU for no benefit). pollution/forecast.py, which IS an
Airflow task, loads the model this script saves and writes predictions to
mart.pollution_forecast.

Train/evaluate split: trains on data_split='training' rows, evaluates on
data_split='testing' rows (the genuinely held-out Dec 2024 tail -- see
core.pollution_hourly's schema comment for why that split is leakage-free).
Lag features for testing rows are real historical values (often falling in
the training period), which is legitimate -- any real deployment would have
those actuals on hand too.

Usage:
    python -m ml.train_pollution_model
    python -m ml.train_pollution_model --model-dir /opt/project/data/models
"""
import argparse
import json
import os
import warnings
from datetime import datetime, timezone

import joblib
import numpy as np
import pandas as pd
import psycopg2
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error

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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-dir", default=os.environ.get("MODEL_DIR", "data/models"))
    args = parser.parse_args()

    conn = get_connection()
    try:
        raw = load_raw(conn)
    finally:
        conn.close()

    feat = build_feature_frame(raw)
    required = FEATURE_COLUMNS + ["target_pm2_5", "data_split"]
    clean = feat.dropna(subset=required)

    train = clean[clean["data_split"] == "training"]
    test = clean[clean["data_split"] == "testing"]

    print(f"Raw rows: {len(raw)}")
    print(f"Feature rows after dropping NaN (gaps, first 24h, reindexed gap-fillers): {len(clean)}")
    print(f"Train rows: {len(train)}  Test rows (held-out): {len(test)}")

    if len(train) == 0 or len(test) == 0:
        raise SystemExit("Not enough rows in one of the splits after feature engineering -- check the data.")

    X_train, y_train = train[FEATURE_COLUMNS], train["target_pm2_5"]
    X_test, y_test = test[FEATURE_COLUMNS], test["target_pm2_5"]

    model = RandomForestRegressor(
        n_estimators=300, max_depth=15, min_samples_leaf=2,
        random_state=42, n_jobs=-1,
    )
    model.fit(X_train, y_train)

    pred_test = model.predict(X_test)
    mae = mean_absolute_error(y_test, pred_test)
    rmse = np.sqrt(mean_squared_error(y_test, pred_test))

    # baseline for comparison: "next hour looks like this hour" (persistence model)
    baseline_mae = mean_absolute_error(y_test, test["lag_pm25_h1"])

    print(f"\nHeld-out test (data_split='testing', {len(test)} rows):")
    print(f"  MAE:  {mae:.2f} ug/m3")
    print(f"  RMSE: {rmse:.2f} ug/m3")
    print(f"  Persistence baseline MAE (predict = last hour's value): {baseline_mae:.2f} ug/m3")
    print(f"  (model should beat the baseline -- if it doesn't, it isn't learning anything lag_pm25_h1 doesn't already say)")

    importances = sorted(
        zip(FEATURE_COLUMNS, model.feature_importances_), key=lambda x: -x[1]
    )
    print("\nFeature importances:")
    for name, imp in importances:
        print(f"  {name}: {imp:.3f}")

    os.makedirs(args.model_dir, exist_ok=True)
    model_version = f"rf_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    model_path = os.path.join(args.model_dir, "pollution_pm25_forecast.joblib")
    meta_path = os.path.join(args.model_dir, "pollution_pm25_forecast.meta.json")

    joblib.dump(model, model_path)
    meta = {
        "model_version": model_version,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "features": FEATURE_COLUMNS,
        "n_train": len(train),
        "n_test": len(test),
        "mae_testing": mae,
        "rmse_testing": rmse,
        "baseline_mae_testing": baseline_mae,
    }
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)

    print(f"\nSaved model -> {model_path}")
    print(f"Saved metadata -> {meta_path}  (model_version={model_version})")


if __name__ == "__main__":
    main()