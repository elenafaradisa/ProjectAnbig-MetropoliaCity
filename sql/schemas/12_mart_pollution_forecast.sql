-- mart.pollution_forecast: model predictions for components_pm2_5, one row
-- per (city, hour, model_version). Joins back to core.pollution_hourly on
-- (city, observed_at) for the actual value and data_split, so the dashboard
-- can plot actual-vs-predicted and compute honest held-out error (filter
-- data_split='testing') without re-running the model.
--
-- model_version lets a retrained model write a fresh set of predictions
-- without overwriting or being blocked by the previous run's rows.
CREATE TABLE IF NOT EXISTS mart.pollution_forecast (
    id                BIGSERIAL PRIMARY KEY,
    city              TEXT NOT NULL DEFAULT 'Islamabad',
    observed_at       TIMESTAMP NOT NULL,
    data_split        TEXT NOT NULL,            -- 'training' | 'testing', copied from core.pollution_hourly
    actual_pm2_5      DOUBLE PRECISION,          -- core.pollution_hourly.components_pm2_5 at prediction time
    predicted_pm2_5   DOUBLE PRECISION NOT NULL,
    abs_error         DOUBLE PRECISION,          -- |actual - predicted|, NULL if actual_pm2_5 is NULL
    model_version     TEXT NOT NULL,             -- e.g. 'rf_2026-10-03'
    predicted_at      TIMESTAMP NOT NULL DEFAULT now(),

    CONSTRAINT uq_pollution_forecast UNIQUE (city, observed_at, model_version)
);

CREATE INDEX IF NOT EXISTS idx_pollution_forecast_observed_at
    ON mart.pollution_forecast (observed_at);

CREATE INDEX IF NOT EXISTS idx_pollution_forecast_model_version
    ON mart.pollution_forecast (model_version);