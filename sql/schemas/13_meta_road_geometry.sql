-- Extends meta.road_positions (03_meta_quality.sql) with real road geometry
-- for the dashboard map: each road_segment_id is drawn as a polyline along a
-- real Islamabad road (OpenStreetMap way), and latitude/longitude now hold
-- that polyline's length-weighted midpoint -- where an incident icon goes.
--
-- The road_segment_id -> osm_way_id assignment is ARBITRARY (the raw
-- traffictab23 data has no road names/locations per segment); see
-- geo/assign_osm_roads.py and docs/keterbatasan.md.
ALTER TABLE meta.road_positions ADD COLUMN IF NOT EXISTS road_name TEXT;
ALTER TABLE meta.road_positions ADD COLUMN IF NOT EXISTS highway   TEXT;      -- OSM highway class
ALTER TABLE meta.road_positions ADD COLUMN IF NOT EXISTS length_m  DOUBLE PRECISION;
ALTER TABLE meta.road_positions ADD COLUMN IF NOT EXISTS path      JSONB;     -- [[lat, lon], ...]