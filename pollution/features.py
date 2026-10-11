"""
pollution/features.py

Shared feature engineering for the PM2.5 next-hour forecaster. Used by both
ml/train_pollution_model.py (fits the model) and pollution/forecast.py
(scores it) -- kept in one place so the two can never silently drift apart
("train/serve skew").

All features are lagged (>= 1 hour behind the target hour) or calendar-based.
No same-hour co-pollutant or same-hour weather values are used: this is a
genuine forecast of the next hour, not an estimate from concurrent readings.
"""
import pandas as pd

FEATURE_COLUMNS = [
    "lag_pm25_h1", "lag_pm25_h2", "lag_pm25_h3", "lag_pm25_h24",
    "roll_mean_pm25_h6",
    "lag_temperature_h1", "lag_humidity_h1", "lag_windspeed_h1", "lag_precip_h1",
    "hour", "dow", "month", "is_weekend",
]


def build_feature_frame(df: pd.DataFrame) -> pd.DataFrame:
    """df must have columns: observed_at, data_split, city, components_pm2_5,
    temperature_2m, relative_humidity_2m, wind_speed_10m, precipitation.

    Returns one row per hour, reindexed to a complete hourly grid first so
    lag/rolling windows don't silently shift across missing-hour gaps in the
    source data (a gap just produces NaN features, which callers should
    drop, rather than quietly pulling the "wrong" hour's value).
    """
    df = df.sort_values("observed_at").set_index("observed_at")
    full_index = pd.date_range(df.index.min(), df.index.max(), freq="h")
    df = df.reindex(full_index)
    df.index.name = "observed_at"

    pm25 = df["components_pm2_5"]
    feat = pd.DataFrame(index=df.index)
    feat["lag_pm25_h1"] = pm25.shift(1)
    feat["lag_pm25_h2"] = pm25.shift(2)
    feat["lag_pm25_h3"] = pm25.shift(3)
    feat["lag_pm25_h24"] = pm25.shift(24)
    feat["roll_mean_pm25_h6"] = pm25.shift(1).rolling(6).mean()

    feat["lag_temperature_h1"] = df["temperature_2m"].shift(1)
    feat["lag_humidity_h1"] = df["relative_humidity_2m"].shift(1)
    feat["lag_windspeed_h1"] = df["wind_speed_10m"].shift(1)
    feat["lag_precip_h1"] = df["precipitation"].shift(1)

    feat["hour"] = feat.index.hour
    feat["dow"] = feat.index.dayofweek
    feat["month"] = feat.index.month
    feat["is_weekend"] = (feat.index.dayofweek >= 5).astype(int)

    feat["target_pm2_5"] = pm25
    feat["data_split"] = df["data_split"]
    # city is constant for this dataset; reindex() introduces NaN for it on
    # synthetic gap-filler rows (no real observation at that hour), but a
    # gap row can still get valid lag features from its real neighbors and
    # survive a dropna(subset=FEATURE_COLUMNS) filter downstream, so this
    # must never be left NaN -- core.pollution_hourly's city is NOT NULL.
    feat["city"] = df["city"].ffill().bfill()
    return feat