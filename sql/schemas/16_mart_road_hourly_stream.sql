-- Live (streaming) twin of mart.road_hourly_stats: same columns, key and
-- formulas, filled by the stateful hourly query in
-- spark/jobs/stream_transform.py (1-hour event-time windows, update mode,
-- upsert on the primary key). updated_at = when the row last changed.
CREATE TABLE IF NOT EXISTS mart.road_hourly_stream (
    LIKE mart.road_hourly_stats INCLUDING ALL
);
ALTER TABLE mart.road_hourly_stream
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- Live twin of mart.road_current_status (06_mart_road_current_status.sql):
-- identical logic -- z-score against the batch hour-of-week baseline,
-- alert_level and v2x_status rules -- only the source table differs.
-- Keep both views in sync when the rules change.
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
FROM mart.road_hourly_stream h
LEFT JOIN mart.hour_of_week_baseline b
    ON b.road_segment_id = h.road_segment_id
    AND b.day_of_week = extract(dow from h.obs_date)::smallint
    AND b.obs_hour = h.obs_hour
ORDER BY h.road_segment_id, h.obs_date DESC, h.obs_hour DESC;