-- Results of dag_quality / the audit notebook: one row per metric per run,
-- so results can be compared across runs instead of only keeping the latest.
CREATE TABLE IF NOT EXISTS meta.quality_results (
    id           BIGSERIAL PRIMARY KEY,
    run_id       TEXT             NOT NULL,
    run_ts       TIMESTAMPTZ      NOT NULL DEFAULT now(),
    category     TEXT             NOT NULL,   -- integrity | range | consistency | nulls | audit
    metric_name  TEXT             NOT NULL,
    value        DOUBLE PRECISION,
    detail       TEXT
);
CREATE INDEX IF NOT EXISTS idx_quality_run ON meta.quality_results (run_id);

-- Road-position lookup for the map feature (see project discussion: since
-- road_segment_id is not tied to a stable GPS point in the raw data, this
-- table is filled separately, either from a validated per-segment centroid
-- or from an OSM-road assignment, and is what the dashboard's map reads).
CREATE TABLE IF NOT EXISTS meta.road_positions (
    road_segment_id  INTEGER PRIMARY KEY,
    latitude          DOUBLE PRECISION NOT NULL,
    longitude         DOUBLE PRECISION NOT NULL,
    source            TEXT NOT NULL,   -- 'gps_median' | 'osm_assignment' | 'illustrative_grid'
    osm_way_id        BIGINT,
    note              TEXT
);
