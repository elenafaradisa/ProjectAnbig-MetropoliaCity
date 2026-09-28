-- Full-refresh: kosongkan lalu isi ulang dari mart.road_hourly_stats.
-- day_of_week dihitung dari obs_date (bukan dari kolom day_of_week
-- mentah yang tidak dipakai di sini), konsisten dengan pendekatan
-- di build_hour_of_week_baseline.sql.
TRUNCATE mart.hour_of_week_city_summary;

INSERT INTO mart.hour_of_week_city_summary (
    day_of_week, obs_hour, avg_anomaly_rate, avg_incident_count, n_segment_hours
)
SELECT
    extract(dow from obs_date)::smallint AS day_of_week,
    obs_hour,
    avg(anomaly_rate)   AS avg_anomaly_rate,
    avg(incident_count) AS avg_incident_count,
    count(*)            AS n_segment_hours
FROM mart.road_hourly_stats
GROUP BY 1, 2;