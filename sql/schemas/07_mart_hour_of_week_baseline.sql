-- Historical baseline (mean + std of anomaly_rate) per road_segment_id,
-- day-of-week, and hour — built from mart.road_hourly_stats across all
-- ~104 weeks of history. Used to compute anomaly_rate_zscore in
-- mart.road_current_status (config/thresholds.yaml alert_levels.waspada:
-- anomaly_rate_zscore_min: 2.0).
CREATE TABLE IF NOT EXISTS mart.hour_of_week_baseline (
    road_segment_id              INTEGER          NOT NULL,
    day_of_week                   SMALLINT         NOT NULL,  -- extract(dow from date): 0=Sunday..6=Saturday
    obs_hour                      SMALLINT         NOT NULL,
    n_weeks                       INTEGER          NOT NULL,
    baseline_mean_anomaly_rate    DOUBLE PRECISION,
    baseline_std_anomaly_rate     DOUBLE PRECISION,
    PRIMARY KEY (road_segment_id, day_of_week, obs_hour)
);