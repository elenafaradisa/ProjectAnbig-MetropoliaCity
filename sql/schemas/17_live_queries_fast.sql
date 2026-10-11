-- Keep the live dashboard fast however large the streaming tables grow
-- (a full replay of traffictab23 puts ~2.1M rows in core.observations_stream
-- and ~1.8M in mart.road_hourly_stream).
--
-- 1. Index for "latest hours" lookups on the hourly stream.
CREATE INDEX IF NOT EXISTS idx_road_hourly_stream_date_hour
    ON mart.road_hourly_stream (obs_date, obs_hour);

-- 2. The live status view only needs each segment's most recent hour; with
--    ~1.2 events per segment per event-hour every segment has rows within the
--    last two event-days, so DISTINCT ON runs over a few thousand rows
--    instead of the whole table. Same columns and rules as before.
CREATE OR REPLACE VIEW mart.road_current_status_live AS
SELECT DISTINCT ON (h.road_segment_id)
    h.road_segment_id,
    h.obs_date,
    h.obs_hour,
    h.n_observations,
    h.avg_vehicle_count,
    h.avg_average_speed,
    h.avg_lane_occupancy_rate,
    h.avg_jam_density_index,
    h.incident_count,
    h.anomaly_rate,
    h.max_incident_type,
    h.avg_v2x_packet_loss_rate,
    h.avg_v2x_message_delay_avg,
    h.any_bad_weather,
    h.any_wet_surface,
    CASE
        WHEN b.baseline_std_anomaly_rate IS NULL OR b.baseline_std_anomaly_rate = 0 THEN NULL
        ELSE (h.anomaly_rate - b.baseline_mean_anomaly_rate) / b.baseline_std_anomaly_rate
    END AS anomaly_rate_zscore,
    CASE
        WHEN h.max_incident_type IN (2, 4) THEN 'kritis'
        WHEN b.baseline_std_anomaly_rate IS NOT NULL
             AND b.baseline_std_anomaly_rate > 0
             AND (h.anomaly_rate - b.baseline_mean_anomaly_rate) / b.baseline_std_anomaly_rate >= 2.0
            THEN 'waspada'
        WHEN h.any_bad_weather OR h.any_wet_surface THEN 'perhatian'
        ELSE 'normal'
    END AS alert_level,
    CASE
        WHEN h.avg_v2x_packet_loss_rate > 31.02 OR h.avg_v2x_message_delay_avg > 451.01 THEN 'kritis'
        WHEN h.avg_v2x_packet_loss_rate > 14.79 OR h.avg_v2x_message_delay_avg > 255.13 THEN 'waspada'
        ELSE 'normal'
    END AS v2x_status,
    h.updated_at
FROM (
    SELECT *
    FROM mart.road_hourly_stream
    WHERE obs_date >= (SELECT max(obs_date) FROM mart.road_hourly_stream) - 1
) h
LEFT JOIN mart.hour_of_week_baseline b
    ON b.road_segment_id = h.road_segment_id
    AND b.day_of_week = extract(dow from h.obs_date)::smallint
    AND b.obs_hour = h.obs_hour
ORDER BY h.road_segment_id, h.obs_date DESC, h.obs_hour DESC;
