-- Coarser rollup than mart.road_window_stats (15-min): per road segment
-- per calendar hour. Built for the dashboard's first-look summary/map,
-- where road_window_stats (barely smaller than core.observations, see
-- project discussion 2026-09-21) is too granular to be useful as an
-- overview.
CREATE TABLE IF NOT EXISTS mart.road_hourly_stats (
    road_segment_id            INTEGER          NOT NULL,
    obs_date                    DATE             NOT NULL,
    obs_hour                    SMALLINT         NOT NULL,
    n_observations               INTEGER          NOT NULL,
    avg_vehicle_count            DOUBLE PRECISION,
    avg_average_speed            DOUBLE PRECISION,
    avg_lane_occupancy_rate      DOUBLE PRECISION,
    avg_jam_density_index        DOUBLE PRECISION,
    incident_count                INTEGER          NOT NULL,
    anomaly_rate                  DOUBLE PRECISION NOT NULL,
    max_incident_type             SMALLINT         NOT NULL,
    avg_v2x_packet_loss_rate     DOUBLE PRECISION,
    avg_v2x_message_delay_avg    DOUBLE PRECISION,
    PRIMARY KEY (road_segment_id, obs_date, obs_hour)
);

CREATE INDEX IF NOT EXISTS idx_mart_hourly_date
    ON mart.road_hourly_stats (obs_date);
CREATE INDEX IF NOT EXISTS idx_mart_hourly_segment
    ON mart.road_hourly_stats (road_segment_id);