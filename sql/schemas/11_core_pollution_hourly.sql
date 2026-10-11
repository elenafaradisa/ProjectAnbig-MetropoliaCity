-- core.pollution_hourly: raw per-hour air-quality + weather readings, Islamabad.
--
-- One row = one city-hour (not per road segment, unlike core.observations).
-- Source: OpenWeatherMap-style Air Pollution API export, provided as two
-- CSV/XLSX batches ("Training" = Aug 2021-Dec 2024 historical, "Testing" =
-- Jul-Dec 2024, replayed as the "current data" feed for this demo). The two
-- batches overlap by ~3,623 hours -- data_split + source_file record which
-- batch a row came from so that overlap is auditable rather than silently
-- merged.
--
-- main_aqi is OpenWeatherMap's discrete 1-5 index (Good..Very Poor), kept
-- here for reference/audit only -- it is NOT the model's prediction target.
-- components_pm2_5 (continuous, ug/m3) is the target; see mart.pollution_forecast.
CREATE TABLE IF NOT EXISTS core.pollution_hourly (
    id                     BIGSERIAL PRIMARY KEY,
    city                   TEXT NOT NULL DEFAULT 'Islamabad',
    observed_at            TIMESTAMP NOT NULL,

    main_aqi               SMALLINT,
    components_co          DOUBLE PRECISION,
    components_no          DOUBLE PRECISION,
    components_no2         DOUBLE PRECISION,
    components_o3          DOUBLE PRECISION,
    components_so2         DOUBLE PRECISION,
    components_pm2_5       DOUBLE PRECISION,
    components_pm10        DOUBLE PRECISION,
    components_nh3         DOUBLE PRECISION,

    temperature_2m         DOUBLE PRECISION,
    relative_humidity_2m   DOUBLE PRECISION,
    dew_point_2m           DOUBLE PRECISION,
    precipitation          DOUBLE PRECISION,
    surface_pressure       DOUBLE PRECISION,
    wind_speed_10m         DOUBLE PRECISION,
    wind_direction_10m     DOUBLE PRECISION,
    shortwave_radiation    DOUBLE PRECISION,

    -- provenance, for the training/testing overlap audit noted above
    data_split             TEXT NOT NULL,   -- 'training' | 'testing'
    source_file            TEXT NOT NULL,

    ingested_at            TIMESTAMP NOT NULL DEFAULT now(),

    CONSTRAINT uq_pollution_hourly_city_time UNIQUE (city, observed_at)
);

CREATE INDEX IF NOT EXISTS idx_pollution_hourly_observed_at
    ON core.pollution_hourly (observed_at);

CREATE INDEX IF NOT EXISTS idx_pollution_hourly_city_observed_at
    ON core.pollution_hourly (city, observed_at);