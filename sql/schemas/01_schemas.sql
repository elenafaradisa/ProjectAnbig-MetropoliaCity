-- Project database layers.
-- Raw and lightly-typed data lives as Parquet files (data/parquet/), not in
-- Postgres, so there is no "staging" schema here.
CREATE SCHEMA IF NOT EXISTS core;   -- cleaned, one row per observation (ETL output)
CREATE SCHEMA IF NOT EXISTS mart;   -- dashboard-ready aggregates (built later, after audit)
CREATE SCHEMA IF NOT EXISTS meta;   -- data-quality results, road positions, thresholds
