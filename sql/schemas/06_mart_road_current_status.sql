-- Latest hourly stats per road segment — what the dashboard map's default
-- view should query (one row per road_segment_id, ~101 rows), instead of
-- scanning all of mart.road_hourly_stats. A VIEW, not a table: always
-- reflects the latest row in road_hourly_stats without a separate refresh
-- step (refresh road_hourly_stats via sql/mart/build_road_hourly_stats.sql
-- and this view updates automatically on next query).
CREATE OR REPLACE VIEW mart.road_current_status AS
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
    -- Traffic/incident alert level. Order matters: kritis beats waspada
    -- beats perhatian (config/thresholds.yaml alert_levels order).
    CASE
        WHEN h.max_incident_type IN (2, 4) THEN 'kritis'
        WHEN b.baseline_std_anomaly_rate IS NOT NULL
             AND b.baseline_std_anomaly_rate > 0
             AND (h.anomaly_rate - b.baseline_mean_anomaly_rate) / b.baseline_std_anomaly_rate >= 2.0
            THEN 'waspada'
        WHEN h.any_bad_weather OR h.any_wet_surface THEN 'perhatian'
        ELSE 'normal'
    END AS alert_level,
    -- V2X connectivity status, separate axis from traffic alert_level.
    -- Bounds mirrored from config/thresholds.yaml v2x_status (p50/p90 from
    -- notebooks/01_audit_data.ipynb, 2026-09-21) — keep in sync manually.
    CASE
        WHEN h.avg_v2x_packet_loss_rate > 31.02 OR h.avg_v2x_message_delay_avg > 451.01 THEN 'kritis'
        WHEN h.avg_v2x_packet_loss_rate > 14.79 OR h.avg_v2x_message_delay_avg > 255.13 THEN 'waspada'
        ELSE 'normal'
    END AS v2x_status
FROM mart.road_hourly_stats h
LEFT JOIN mart.hour_of_week_baseline b
    ON b.road_segment_id = h.road_segment_id
    AND b.day_of_week = extract(dow from h.obs_date)::smallint
    AND b.obs_hour = h.obs_hour
ORDER BY h.road_segment_id, h.obs_date DESC, h.obs_hour DESC;