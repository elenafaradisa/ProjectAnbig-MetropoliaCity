-- Latest hourly stats per road segment — what the dashboard map's default
-- view should query (one row per road_segment_id, ~101 rows), instead of
-- scanning all of mart.road_hourly_stats. A VIEW, not a table: always
-- reflects the latest row in road_hourly_stats without a separate refresh
-- step (refresh road_hourly_stats via sql/mart/build_road_hourly_stats.sql
-- and this view updates automatically on next query).
CREATE OR REPLACE VIEW mart.road_current_status AS
SELECT DISTINCT ON (road_segment_id)
    road_segment_id,
    obs_date,
    obs_hour,
    n_observations,
    avg_vehicle_count,
    avg_average_speed,
    avg_lane_occupancy_rate,
    avg_jam_density_index,
    incident_count,
    anomaly_rate,
    max_incident_type,
    avg_v2x_packet_loss_rate,
    avg_v2x_message_delay_avg
FROM mart.road_hourly_stats
ORDER BY road_segment_id, obs_date DESC, obs_hour DESC;