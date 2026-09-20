-- Separate database for Airflow's own metadata (runs against the default
-- database on first container start, per Postgres init-script convention).
CREATE DATABASE airflow_meta;
