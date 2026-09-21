-- Full refresh, rerun after every ETL run (like the other mart builds).
TRUNCATE TABLE mart.hour_of_week_baseline;

INSERT INTO mart.hour_of_week_baseline (
    road_segment_id, day_of_week, obs_hour, n_weeks,
    baseline_mean_anomaly_rate, baseline_std_anomaly_rate
)
SELECT
    road_segment_id,
    extract(dow from obs_date)::smallint    AS day_of_week,
    obs_hour,
    count(*)                                 AS n_weeks,
    avg(anomaly_rate)                        AS baseline_mean_anomaly_rate,
    stddev(anomaly_rate)                     AS baseline_std_anomaly_rate
FROM mart.road_hourly_stats
GROUP BY road_segment_id, extract(dow from obs_date), obs_hour;