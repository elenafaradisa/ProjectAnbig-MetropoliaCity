-- Citywide (semua segmen digabung) agregat hari-of-week x jam.
-- Beda dari mart.hour_of_week_baseline (per road_segment_id, dipakai
-- untuk anomaly_rate_zscore) — tabel ini untuk kebutuhan level-kota:
-- heatmap Supervisor dan estimasi kebutuhan petugas per shift.
CREATE TABLE IF NOT EXISTS mart.hour_of_week_city_summary (
    day_of_week SMALLINT NOT NULL,
    obs_hour SMALLINT NOT NULL,
    avg_anomaly_rate DOUBLE PRECISION NOT NULL,
    avg_incident_count DOUBLE PRECISION NOT NULL,
    n_segment_hours INTEGER NOT NULL,
    PRIMARY KEY (day_of_week, obs_hour)
);