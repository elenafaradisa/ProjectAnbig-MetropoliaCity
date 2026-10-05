"""Assign each traffictab23 road_segment_id to a real Islamabad road
(OpenStreetMap way) and store its geometry in meta.road_positions.

Why this exists: road_segment_id in the raw data is just a number -- there is
no road name or stable location per segment. For the dashboard map we still
want congestion drawn along a REAL road and incident icons at that road's
midpoint, so this script:

  1. Fetches main roads (trunk/primary/secondary/tertiary) inside an
     Islamabad bounding box from the Overpass API, and caches the raw
     response to data/geo/ -- reruns read the cache, so the assignment stays
     stable even if OSM data changes later (delete the cache to refetch).
  2. Keeps ways at least --min-length-m long and picks them round-robin
     across road names, so segments spread over many different roads instead
     of piling onto one long highway split into many OSM ways.
  3. Assigns them to the sorted distinct road_segment_ids with a fixed seed
     -> same segment always maps to the same road.
  4. Computes each polyline's length-weighted midpoint (haversine), and
     upserts road_segment_id, midpoint lat/lon, osm_way_id, road_name,
     highway, length_m, path into meta.road_positions
     (source = 'osm_assignment').

The assignment is ARBITRARY by design (documented in docs/keterbatasan.md):
the road geometry is real, which road a given segment id sits on is not.

Run manually (one-off, like ml/train_pollution_model.py):
    docker compose exec airflow-scheduler bash -c "cd /opt/project && python -m geo.assign_osm_roads"
"""
import argparse
import json
import math
import os
import random
import sys
from pathlib import Path

import psycopg2
import requests
from psycopg2.extras import Json, execute_values

# Islamabad urban area (south, west, north, east)
DEFAULT_BBOX = (33.62, 72.98, 33.74, 73.17)
HIGHWAY_CLASSES = "trunk|primary|secondary|tertiary"
OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]
SEED = 42


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "metropolia"),
        user=os.environ.get("POSTGRES_USER", "metropolia"),
        password=os.environ.get("POSTGRES_PASSWORD", "metropolia"),
    )


def fetch_overpass(bbox, cache_path: Path) -> dict:
    if cache_path.exists():
        print(f"Using cached Overpass response: {cache_path}")
        return json.loads(cache_path.read_text(encoding="utf-8"))

    s, w, n, e = bbox
    query = (
        f'[out:json][timeout:90];'
        f'way["highway"~"^({HIGHWAY_CLASSES})$"]({s},{w},{n},{e});'
        f'out tags geom;'
    )
    headers = {"User-Agent": "MetropoliaCity-course-project/1.0"}
    last_err = None
    for url in OVERPASS_URLS:
        try:
            print(f"Querying {url} ...")
            r = requests.post(url, data={"data": query}, headers=headers, timeout=120)
            r.raise_for_status()
            data = r.json()
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(json.dumps(data), encoding="utf-8")
            print(f"Cached {len(data.get('elements', []))} ways -> {cache_path}")
            return data
        except Exception as err:  # try the next mirror
            print(f"  failed: {err}")
            last_err = err
    sys.exit(f"All Overpass endpoints failed: {last_err}")


def haversine_m(a, b) -> float:
    lat1, lon1, lat2, lon2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    h = (math.sin((lat2 - lat1) / 2) ** 2
         + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2)
    return 2 * 6_371_000 * math.asin(math.sqrt(h))


def polyline_length_and_midpoint(path):
    """Total length (m) and the point at 50% of that length along the line --
    not path[len//2], which is skewed when nodes are unevenly spaced."""
    seg = [haversine_m(path[i], path[i + 1]) for i in range(len(path) - 1)]
    total = sum(seg)
    half, run = total / 2, 0.0
    for i, d in enumerate(seg):
        if run + d >= half and d > 0:
            t = (half - run) / d
            a, b = path[i], path[i + 1]
            return total, (a[0] + t * (b[0] - a[0]), a[1] + t * (b[1] - a[1]))
        run += d
    return total, tuple(path[len(path) // 2])


def candidate_ways(data: dict, min_length_m: float) -> list:
    ways = []
    for el in data.get("elements", []):
        if el.get("type") != "way" or len(el.get("geometry", [])) < 2:
            continue
        path = [[round(p["lat"], 6), round(p["lon"], 6)] for p in el["geometry"]]
        length, mid = polyline_length_and_midpoint(path)
        if length < min_length_m:
            continue
        tags = el.get("tags", {})
        ways.append({
            "osm_way_id": el["id"],
            "road_name": tags.get("name:en") or tags.get("name"),
            "highway": tags.get("highway"),
            "length_m": round(length, 1),
            "mid": mid,
            "path": path,
        })
    return ways


def pick_spread(ways: list, n: int) -> list:
    """Round-robin across road names (longest way of each name first), so n
    segments land on as many distinct roads as possible."""
    groups = {}
    for w in ways:
        key = w["road_name"] or f"unnamed-{w['osm_way_id']}"
        groups.setdefault(key, []).append(w)
    for g in groups.values():
        g.sort(key=lambda w: (-w["length_m"], w["osm_way_id"]))
    order = sorted(groups, key=lambda k: (-groups[k][0]["length_m"], k))
    picked, rnd = [], 0
    while len(picked) < n:
        took = False
        for k in order:
            if rnd < len(groups[k]):
                picked.append(groups[k][rnd])
                took = True
                if len(picked) == n:
                    break
        if not took:
            break
        rnd += 1
    return picked


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", default="data/geo/islamabad_roads_overpass.json")
    ap.add_argument("--min-length-m", type=float, default=500.0)
    ap.add_argument("--dry-run", action="store_true", help="print the assignment, don't write")
    args = ap.parse_args()

    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT road_segment_id FROM core.observations "
                    "WHERE road_segment_id IS NOT NULL ORDER BY 1")
        segment_ids = [r[0] for r in cur.fetchall()]
    print(f"Distinct road_segment_id in core.observations: {len(segment_ids)}")
    if not segment_ids:
        sys.exit("No road_segment_id found -- run etl_traffic first.")

    data = fetch_overpass(DEFAULT_BBOX, Path(args.cache))
    ways = candidate_ways(data, args.min_length_m)
    print(f"OSM ways >= {args.min_length_m:.0f} m: {len(ways)}")
    if len(ways) < len(segment_ids):
        sys.exit(f"Only {len(ways)} ways for {len(segment_ids)} segments -- "
                 f"rerun with a smaller --min-length-m.")

    picked = pick_spread(ways, len(segment_ids))
    random.Random(SEED).shuffle(picked)

    rows = []
    for seg_id, w in zip(segment_ids, picked):
        rows.append((
            seg_id, w["mid"][0], w["mid"][1], "osm_assignment", w["osm_way_id"],
            "arbitrary id->road assignment, seed=42", w["road_name"], w["highway"],
            w["length_m"], Json(w["path"]),
        ))

    names = {r[6] for r in rows if r[6]}
    print(f"Assigned {len(rows)} segments to {len(names)} distinct named roads.")
    for r in rows[:5]:
        print(f"  segment {r[0]} -> way {r[4]} ({r[6] or 'unnamed'}, {r[7]}, "
              f"{r[8]:.0f} m), midpoint {r[1]:.5f},{r[2]:.5f}")

    if args.dry_run:
        print("Dry run -- nothing written.")
        return

    with conn, conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO meta.road_positions
                (road_segment_id, latitude, longitude, source, osm_way_id, note,
                 road_name, highway, length_m, path)
            VALUES %s
            ON CONFLICT (road_segment_id) DO UPDATE SET
                latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude,
                source = EXCLUDED.source, osm_way_id = EXCLUDED.osm_way_id,
                note = EXCLUDED.note, road_name = EXCLUDED.road_name,
                highway = EXCLUDED.highway, length_m = EXCLUDED.length_m,
                path = EXCLUDED.path
        """, rows)
        cur.execute("SELECT COUNT(*) FROM meta.road_positions WHERE source = 'osm_assignment'")
        print(f"meta.road_positions osm_assignment rows: {cur.fetchone()[0]}")
    conn.close()


if __name__ == "__main__":
    main()