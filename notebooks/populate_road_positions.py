"""Populate meta.road_positions with illustrative grid coordinates.

Decision (see notebooks/road_gps_stability.py output): gps_median is not
viable — road_segment_id groups show ~4km GPS spread, confirming the raw
GPS columns are not tied to a stable per-segment location. osm_assignment
is also not viable: the source data carries no road name / area reference
to match road_segment_id against real OSM ways, only an opaque integer ID.

So this uses source='illustrative_grid': road_segment_id values are spread
evenly across a bounding box around Islamabad-Rawalpindi, purely so the
dashboard map has *some* point to plot per segment. These are NOT real
road locations — do not present them as such. note on every row says so
explicitly.

Usage:
    docker compose exec airflow-scheduler python /opt/project/notebooks/populate_road_positions.py
"""
import math
import os

import psycopg2
import psycopg2.extras

# Rough bounding box for the Islamabad-Rawalpindi urban area. Chosen to be
# tighter than the RANGE_CHECKS sanity bounds in spark/common/schema.py
# (33.0-34.0, 72.5-73.5), which are loose plausibility limits, not a claim
# about where the twin cities actually are.
LAT_MIN, LAT_MAX = 33.45, 33.75
LON_MIN, LON_MAX = 72.85, 73.15

NOTE = (
    "Illustrative grid position, NOT the actual road location. Raw GPS in "
    "the source data is not stable per road_segment_id (~4km spread — see "
    "notebooks/road_gps_stability.py), and the dataset has no road name or "
    "area reference to assign a real OSM position. For map-plotting "
    "purposes only."
)


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        user=os.environ.get("POSTGRES_USER", "metropolia"),
        password=os.environ.get("POSTGRES_PASSWORD"),
        dbname=os.environ.get("POSTGRES_DB", "metropolia"),
    )


def grid_positions(segment_ids: list[int]) -> list[tuple]:
    n = len(segment_ids)
    cols = math.ceil(math.sqrt(n))
    rows = math.ceil(n / cols)

    lat_step = (LAT_MAX - LAT_MIN) / (rows - 1) if rows > 1 else 0
    lon_step = (LON_MAX - LON_MIN) / (cols - 1) if cols > 1 else 0

    rows_out = []
    for i, seg_id in enumerate(segment_ids):
        r, c = divmod(i, cols)
        lat = LAT_MIN + r * lat_step
        lon = LON_MIN + c * lon_step
        rows_out.append((seg_id, lat, lon, "illustrative_grid", None, NOTE))
    return rows_out


def run() -> int:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("select distinct road_segment_id from core.observations order by 1")
            segment_ids = [r[0] for r in cur.fetchall()]

        if not segment_ids:
            raise SystemExit("No road_segment_id found in core.observations — run the ETL DAG first.")

        rows = grid_positions(segment_ids)

        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                insert into meta.road_positions
                    (road_segment_id, latitude, longitude, source, osm_way_id, note)
                values %s
                on conflict (road_segment_id) do update set
                    latitude = excluded.latitude,
                    longitude = excluded.longitude,
                    source = excluded.source,
                    osm_way_id = excluded.osm_way_id,
                    note = excluded.note
                """,
                rows,
            )
        conn.commit()

    print(f"Upserted {len(rows)} rows into meta.road_positions (source=illustrative_grid).")
    return len(rows)


if __name__ == "__main__":
    run()