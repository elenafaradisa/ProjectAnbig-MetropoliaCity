-- One row per 30-second observation from traffictab23.
-- Column order matches CORE_COLUMNS in spark/common/schema.py.
CREATE TABLE IF NOT EXISTS core.observations (
    observed_at                TIMESTAMP        NOT NULL,
    time_of_day                TEXT,
    day_of_week                TEXT,
    gps_latitude                DOUBLE PRECISION,
    gps_longitude               DOUBLE PRECISION,
    road_segment_id             INTEGER,
    distance_to_intersection    DOUBLE PRECISION,
    vehicle_count                INTEGER,
    average_speed                DOUBLE PRECISION,
    lane_occupancy_rate          DOUBLE PRECISION,
    jam_density_index            DOUBLE PRECISION,
    hard_braking_events          INTEGER,
    rapid_acceleration_events    INTEGER,
    lane_changes_per_minute      DOUBLE PRECISION,
    stop_duration_avg            DOUBLE PRECISION,
    weather_condition            TEXT,
    visibility_range              DOUBLE PRECISION,
    road_surface_status           TEXT,
    v2x_packet_loss_rate          DOUBLE PRECISION,
    v2v_beacon_interval_avg       DOUBLE PRECISION,
    v2x_message_delay_avg         DOUBLE PRECISION,
    anomaly_label                  SMALLINT,
    incident_type                  SMALLINT,

    -- derived during the transform step, from observed_at (source of truth,
    -- since time_of_day/day_of_week in the raw file were found inconsistent
    -- with the timestamp in spot checks)
    incident_name                TEXT,           -- lookup of incident_type, see config/incident_types.yaml
    obs_date                     DATE,
    obs_year                     INTEGER,
    obs_month                    INTEGER,
    obs_hour                     INTEGER,
    weekday_from_ts               TEXT,
    is_weekend                    BOOLEAN,
    window_15min                  TIMESTAMP,      -- floor(observed_at, 15 min), for area-level aggregation

    -- data-quality flags, kept per row rather than discarded so the audit
    -- notebook and meta.quality_results can quantify them
    tod_mismatch                  BOOLEAN,        -- time_of_day (raw) != derived from observed_at hour
    dow_mismatch                  BOOLEAN,        -- day_of_week (raw) != weekday_from_ts
    label_consistent              BOOLEAN         -- (anomaly_label = 1) == (incident_type != 0)
);

CREATE INDEX IF NOT EXISTS idx_obs_observed_at ON core.observations (observed_at);
CREATE INDEX IF NOT EXISTS idx_obs_road        ON core.observations (road_segment_id);
CREATE INDEX IF NOT EXISTS idx_obs_anomaly     ON core.observations (anomaly_label, incident_type);
CREATE INDEX IF NOT EXISTS idx_obs_window15    ON core.observations (window_15min);
