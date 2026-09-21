-- Dashboard-ready 15-minute rollup per road segment, built from
-- core.observations after ETL + audit. See notebooks/01_audit_data.ipynb
-- Test 6: anomaly_label / incident_type do NOT correlate with traffic
-- metrics in this dataset. Columns here are aggregates only — do not
-- build dashboard features that imply the incident/anomaly counts are
-- derived from or explained by the traffic metrics in the same row.
CREATE TABLE IF NOT EXISTS mart.road_window_stats (
    road_segment_id            INTEGER          NOT NULL,
    window_15min                TIMESTAMP        NOT NULL,
    obs_date                    DATE             NOT NULL,
    obs_hour                    SMALLINT         NOT NULL,
    n_observations               INTEGER          NOT NULL,
    avg_vehicle_count            DOUBLE PRECISION,
    avg_average_speed            DOUBLE PRECISION,
    avg_lane_occupancy_rate      DOUBLE PRECISION,
    avg_jam_density_index        DOUBLE PRECISION,
    incident_count                INTEGER          NOT NULL,  -- rows where incident_type != 0
    anomaly_rate                  DOUBLE PRECISION NOT NULL,  -- share where anomaly_label = 1
    max_incident_type             SMALLINT         NOT NULL,  -- worst incident in this window (0-4)
    avg_v2x_packet_loss_rate     DOUBLE PRECISION,
    avg_v2x_message_delay_avg    DOUBLE PRECISION,
    PRIMARY KEY (road_segment_id, window_15min)
);

CREATE INDEX IF NOT EXISTS idx_mart_road_window_date
    ON mart.road_window_stats (obs_date);
CREATE INDEX IF NOT EXISTS idx_mart_road_window_segment
    ON mart.road_window_stats (road_segment_id);