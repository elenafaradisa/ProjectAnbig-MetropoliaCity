-- Full refresh, same convention as build_road_window_stats.sql — rerun
-- after every ETL run.
TRUNCATE TABLE mart.road_hourly_stats;

INSERT INTO mart.road_hourly_stats (
    road_segment_id, obs_date, obs_hour, n_observations,
    avg_vehicle_count, avg_average_speed, avg_lane_occupancy_rate,
    avg_jam_density_index, incident_count, anomaly_rate, max_incident_type,
    avg_v2x_packet_loss_rate, avg_v2x_message_delay_avg
)
SELECT
    road_segment_id,
    obs_date,
    obs_hour,
    count(*)                                      AS n_observations,
    avg(vehicle_count)                            AS avg_vehicle_count,
    avg(average_speed)                            AS avg_average_speed,
    avg(lane_occupancy_rate)                      AS avg_lane_occupancy_rate,
    avg(jam_density_index)                        AS avg_jam_density_index,
    count(*) filter (where incident_type != 0)    AS incident_count,
    avg(anomaly_label::int)                       AS anomaly_rate,
    max(incident_type)                            AS max_incident_type,
    avg(v2x_packet_loss_rate)                     AS avg_v2x_packet_loss_rate,
    avg(v2x_message_delay_avg)                    AS avg_v2x_message_delay_avg
FROM core.observations
GROUP BY road_segment_id, obs_date, obs_hour;