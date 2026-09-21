"""Check whether gps_latitude/gps_longitude are stable per road_segment_id,
to decide meta.road_positions.source (see sql/schemas/03_meta_quality.sql
and data/README.md's "Known data-quality issues").

Not part of the Airflow DAG — a one-off analysis script, run manually:

    docker compose exec airflow-scheduler python /opt/project/notebooks/road_gps_stability.py

or locally (pip install -r requirements-dev.txt first), against the
Postgres port exposed on the host:

    POSTGRES_HOST=localhost python notebooks/road_gps_stability.py

Decision rule this script exists to inform: if the 90th-percentile spread
(radius_p90_m) across road_segment_id is small relative to what "one road
segment" plausibly spans, gps_median is a reasonable source for
meta.road_positions. If spreads are large / segments turn up scattered
across the map, that confirms gps coordinates aren't a stable per-segment
location, and osm_assignment (manual/lookup to a real OSM way) is needed
instead.
"""
import os

import pandas as pd
import psycopg2
import pathlib

# Rough conversion: degrees -> meters, good enough for a sanity check (not
# for the actual road_positions values, which should use PostGIS or a
# proper geodesic calc if we go with gps_median).
METERS_PER_DEG_LAT = 111_000
METERS_PER_DEG_LON_AT_ISLAMABAD = 111_000 * 0.83  # cos(33.7 deg)


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        user=os.environ.get("POSTGRES_USER", "metropolia"),
        password=os.environ.get("POSTGRES_PASSWORD"),
        dbname=os.environ.get("POSTGRES_DB", "metropolia"),
    )


QUERY = """
    select
        road_segment_id,
        count(*)                    as n_rows,
        stddev_pop(gps_latitude)    as std_lat,
        stddev_pop(gps_longitude)   as std_lon,
        max(gps_latitude) - min(gps_latitude)  as range_lat,
        max(gps_longitude) - min(gps_longitude) as range_lon
    from core.observations
    group by road_segment_id
"""


def run() -> pd.DataFrame:
    with get_connection() as conn:
        df = pd.read_sql(QUERY, conn)

    df["radius_p90_m"] = (
        (df["std_lat"] * METERS_PER_DEG_LAT) ** 2
        + (df["std_lon"] * METERS_PER_DEG_LON_AT_ISLAMABAD) ** 2
    ) ** 0.5

    print(f"road_segment_id groups: {len(df)}")
    print()
    print("radius_p90_m distribution across road_segment_id (spread of GPS")
    print("points reported for the SAME segment, in meters):")
    print(df["radius_p90_m"].describe(percentiles=[0.5, 0.9, 0.99]).to_string())
    print()

    # A real road segment is at most a few hundred meters long. If most
    # segments spread well beyond that, gps_median is not defensible.
    threshold_m = 500
    over = (df["radius_p90_m"] > threshold_m).sum()
    print(f"segments with spread > {threshold_m}m: {over}/{len(df)} "
          f"({over / len(df):.1%})")

    return df


if __name__ == "__main__":
    result = run()
    output_path = pathlib.Path(__file__).parent / "road_gps_stability_output.csv"
    result.sort_values("radius_p90_m", ascending=False).to_csv(
        output_path, index=False
    )
    print(f"\nFull per-segment table written to {output_path}")

