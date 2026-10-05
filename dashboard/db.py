"""Database access for the dashboard.

The dashboard only READS finished mart.* / meta.* tables -- every heavy
computation stays in the Airflow pipelines. Connection settings come from the
project's .env (same file docker compose uses); POSTGRES_HOST defaults to
localhost because the dashboard runs on the host, where docker compose
publishes Postgres on port 5432.
"""
import json
import os
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")


@st.cache_resource
def get_engine():
    url = "postgresql+psycopg2://{u}:{p}@{h}:{port}/{db}".format(
        u=os.environ.get("POSTGRES_USER", "metropolia"),
        p=os.environ.get("POSTGRES_PASSWORD", "metropolia"),
        h=os.environ.get("DASHBOARD_POSTGRES_HOST", "localhost"),
        port=os.environ.get("DASHBOARD_POSTGRES_PORT", "5433"),
        db=os.environ.get("POSTGRES_DB", "metropolia"),
    )
    return create_engine(url, pool_pre_ping=True)


def query(sql: str, **params) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


CURRENT_STATUS_VIEW = {"batch": "mart.road_current_status", "live": "mart.road_current_status_live"}


def _current_status_sql(source: str) -> str:
    view = CURRENT_STATUS_VIEW[source]  # whitelisted, never user text
    extra = ", s.updated_at" if source == "live" else ""
    return f"""
        SELECT s.road_segment_id, s.obs_date, s.obs_hour, s.n_observations,
               s.avg_vehicle_count, s.avg_average_speed, s.avg_lane_occupancy_rate,
               s.avg_jam_density_index, s.incident_count, s.anomaly_rate,
               s.max_incident_type, s.anomaly_rate_zscore, s.alert_level, s.v2x_status{extra},
               p.road_name, p.highway, p.length_m, p.osm_way_id,
               p.latitude AS mid_lat, p.longitude AS mid_lon, p.path
        FROM {view} s
        JOIN meta.road_positions p USING (road_segment_id)
        WHERE p.source = 'osm_assignment'
        ORDER BY s.road_segment_id
    """


@st.cache_data(ttl=600)
def _current_status_batch() -> pd.DataFrame:
    return query(_current_status_sql("batch"))


@st.cache_data(ttl=10)
def _current_status_live() -> pd.DataFrame:
    return query(_current_status_sql("live"))


def road_current_status(source: str = "batch") -> pd.DataFrame:
    """One row per segment (its latest hour) joined with its OSM road geometry.
    source="batch": mart.road_current_status (batch pipeline);
    source="live":  mart.road_current_status_live (Spark Structured Streaming,
    same columns and rules, plus updated_at; looks at the last two event-days
    only, see 17_live_queries_fast.sql)."""
    return _current_status_live() if source == "live" else _current_status_batch()


@st.cache_data(ttl=5)
def segment_recent_windows(road_segment_id: int, hours: int = 2, source: str = "batch") -> pd.DataFrame:
    """15-minute windows in the last `hours` hours of this segment's data
    (oldest first). Windows with no observations simply have no row -- the
    chart's time axis keeps those gaps visible instead of hiding them.
    Live mode aggregates the streamed rows the same way build_road_window_stats.sql does."""
    if source == "live":
        sql = """
            SELECT window_15min, avg(average_speed) AS avg_average_speed,
                   avg(jam_density_index) AS avg_jam_density_index
            FROM core.observations_stream
            WHERE road_segment_id = :sid
              -- bounded through the observed_at index first, so this stays fast
              -- however long the replay has been running
              AND observed_at > (SELECT max(observed_at) FROM core.observations_stream) - interval '1 day'
              AND window_15min > (SELECT max(window_15min) FROM core.observations_stream
                                  WHERE road_segment_id = :sid
                                    AND observed_at > (SELECT max(observed_at) FROM core.observations_stream)
                                                      - interval '1 day') - make_interval(hours => :h)
            GROUP BY window_15min
            ORDER BY window_15min
        """
    else:
        sql = """
            SELECT window_15min, avg_average_speed, avg_jam_density_index
            FROM mart.road_window_stats
            WHERE road_segment_id = :sid
              AND window_15min > (SELECT max(window_15min) FROM mart.road_window_stats
                                  WHERE road_segment_id = :sid) - make_interval(hours => :h)
            ORDER BY window_15min
        """
    return query(sql, sid=int(road_segment_id), h=int(hours))


@st.cache_data(ttl=300)
def pollution_forecast() -> pd.DataFrame:
    """Hourly PM2.5 actual vs next-hour forecast for the current model version
    (the newest one written by the etl_pollution DAG's forecast task)."""
    return query("""
        SELECT observed_at, data_split, actual_pm2_5, predicted_pm2_5, abs_error, model_version
        FROM mart.pollution_forecast
        WHERE city = 'Islamabad'
          AND model_version = (SELECT model_version FROM mart.pollution_forecast
                               ORDER BY predicted_at DESC LIMIT 1)
        ORDER BY observed_at
    """)


@st.cache_data(ttl=300)
def pollution_model_meta() -> dict:
    """Training metadata saved next to the model by ml/train_pollution_model.py
    (includes the persistence-baseline MAE); empty dict if not found."""
    path = PROJECT_ROOT / "data" / "models" / "pollution_pm25_forecast.meta.json"
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


# ---------------------------------------------------------------- streaming
# Every live query below reads only a recent slice of the streaming tables
# through an index (ingested_at, observed_at), so its cost does not grow with
# the table: a full replay of the dataset (~2.1M rows) is as fast as 1,000 rows.

@st.cache_data(ttl=4)
def stream_summary() -> dict:
    df = query("""
        SELECT (SELECT reltuples::bigint FROM pg_class
                 WHERE oid = 'core.observations_stream'::regclass)               AS estimasi,
               (SELECT count(*) FROM core.observations_stream
                 WHERE ingested_at > now() - interval '60 seconds')              AS last_60s,
               (SELECT max(ingested_at) FROM core.observations_stream)          AS last_ingested_tz,
               (SELECT min(observed_at) FROM core.observations_stream)          AS first_event,
               (SELECT max(observed_at) FROM core.observations_stream)          AS last_event
    """)
    out = df.iloc[0].to_dict()
    # Exact count while the table is small (cheap); the planner's estimate
    # (kept current by autovacuum) once it is large, where count(*) is a scan.
    est = int(out["estimasi"] or 0)
    if est < 200_000:
        out["total"] = int(query("SELECT count(*) AS n FROM core.observations_stream").iloc[0]["n"])
        out["total_exact"] = True
    else:
        out["total"], out["total_exact"] = est, False
    last = out["last_ingested_tz"]
    if last is not None and not pd.isna(last):
        last = pd.Timestamp(last)
        out["last_ingested"] = last.tz_convert("Asia/Jakarta").tz_localize(None)
        out["seconds_since_last"] = (pd.Timestamp.now(tz="UTC") - last.tz_convert("UTC")).total_seconds()
    else:
        out["last_ingested"], out["seconds_since_last"] = None, None
    return out


@st.cache_data(ttl=4)
def stream_throughput(minutes: int = 15) -> pd.DataFrame:
    """Rows ingested per minute over the last `minutes` minutes (WIB), with
    empty minutes filled in as 0 so a stall is visible."""
    df = query("""
        SELECT date_trunc('minute', ingested_at) AT TIME ZONE 'Asia/Jakarta' AS menit, count(*) AS baris
        FROM core.observations_stream
        WHERE ingested_at >= date_trunc('minute', now()) - make_interval(mins => :mins - 1)
        GROUP BY 1
    """, mins=int(minutes))
    now_wib = pd.Timestamp.now(tz="Asia/Jakarta").tz_localize(None).floor("min")
    grid = pd.DataFrame({"menit": pd.date_range(end=now_wib, periods=int(minutes), freq="min")})
    df["menit"] = pd.to_datetime(df["menit"])
    return grid.merge(df, on="menit", how="left").fillna({"baris": 0})


@st.cache_data(ttl=4)
def stream_recent(n: int = 15) -> pd.DataFrame:
    return query("""
        SELECT s.ingested_at AT TIME ZONE 'Asia/Jakarta' AS masuk, s.observed_at,
               s.road_segment_id, p.road_name, s.average_speed, s.jam_density_index,
               s.vehicle_count, s.incident_name, s.anomaly_label
        FROM core.observations_stream s
        LEFT JOIN meta.road_positions p USING (road_segment_id)
        ORDER BY s.ingested_at DESC, s.observed_at DESC
        LIMIT :n
    """, n=int(n))


@st.cache_data(ttl=10)
def stream_vs_batch(minutes: int = 5) -> dict:
    """Events ingested in the last `minutes` minutes, matched against the batch
    table (same source file, same transform_df): same timestamp, segment and
    measured values. Bounded by the ingested_at index; checks the recent flow,
    which is where a new problem would show up."""
    df = query("""
        SELECT count(*)                         AS total,
               count(c.observed_at)             AS ada_di_batch,
               count(*) FILTER (WHERE c.observed_at IS NOT NULL AND (
                     s.road_segment_id IS DISTINCT FROM c.road_segment_id
                  OR s.obs_hour        IS DISTINCT FROM c.obs_hour
                  OR s.vehicle_count   IS DISTINCT FROM c.vehicle_count
                  OR abs(s.average_speed     - c.average_speed)     > 1e-9
                  OR abs(s.jam_density_index - c.jam_density_index) > 1e-9)) AS beda
        FROM core.observations_stream s
        LEFT JOIN core.observations c USING (observed_at)
        WHERE s.ingested_at > now() - make_interval(mins => :mins)
    """, mins=int(minutes))
    out = df.iloc[0].to_dict()
    out["minutes"] = minutes
    return out


# ---------------------------------------------------------------- trends (batch, historical)
# Built from the full two-year mart, which only changes when the batch
# pipeline reruns -- so a long cache is fine.

@st.cache_data(ttl=3600)
def hour_of_week_profile() -> pd.DataFrame:
    """Average per day-of-week x hour over all segments and weeks.
    day_of_week follows the marts: extract(dow) -> 0 = Minggu ... 6 = Sabtu."""
    return query("""
        SELECT extract(dow FROM obs_date)::int        AS day_of_week,
               obs_hour::int                          AS obs_hour,
               avg(avg_jam_density_index)             AS jam_density,
               avg(avg_average_speed)                 AS kecepatan,
               avg(anomaly_rate)                      AS anomaly_rate,
               avg(incident_count)                    AS insiden_per_jam,
               count(*)                               AS n_segment_jam
        FROM mart.road_hourly_stats
        GROUP BY 1, 2
        ORDER BY 1, 2
    """)


@st.cache_data(ttl=3600)
def daily_profile() -> pd.DataFrame:
    """Citywide daily values over the whole period."""
    return query("""
        SELECT obs_date,
               avg(avg_jam_density_index)  AS jam_density,
               avg(avg_average_speed)      AS kecepatan,
               sum(incident_count)         AS insiden,
               sum(n_observations)         AS observasi
        FROM mart.road_hourly_stats
        GROUP BY obs_date
        ORDER BY obs_date
    """)


@st.cache_data(ttl=3600)
def shift_summary() -> pd.DataFrame:
    return query("SELECT shift_name, avg_incident_per_hour, avg_anomaly_rate FROM mart.shift_summary")


# ---------------------------------------------------------------- incidents
# Same queries for both sources; only the table differs (whitelisted).
# Batch scans 2.1M rows and only changes when the pipeline reruns -> long
# cache. Live reads the small streaming table and must stay fresh.
OBS_TABLE = {"batch": "core.observations", "live": "core.observations_stream"}


LIVE_EVENT_HOURS = 24  # live incident view: last 24 hours of event time


def _incident_queries(source: str) -> dict:
    t = OBS_TABLE[source]
    bucket = "month" if source == "batch" else "hour"
    # Live: only the last LIVE_EVENT_HOURS of event time (an observed_at index
    # range of ~2,900 rows), so the page stays fast after a full replay.
    w = "" if source == "batch" else (
        f" AND observed_at > (SELECT max(observed_at) FROM {t}) - interval '{LIVE_EVENT_HOURS} hours'")
    wo = w.replace("observed_at >", "o.observed_at >")
    return {
        "summary": f"""
            SELECT count(*) AS observasi,
                   count(*) FILTER (WHERE incident_type <> 0) AS insiden,
                   min(observed_at) AS dari, max(observed_at) AS sampai
            FROM {t} WHERE true{w}""",
        "by_type": f"""
            SELECT incident_type, count(*) AS insiden
            FROM {t} WHERE incident_type <> 0{w}
            GROUP BY 1 ORDER BY 1""",
        "top_segments": f"""
            SELECT o.road_segment_id, p.road_name, count(*) AS insiden,
                   count(*) FILTER (WHERE o.incident_type IN (2, 4)) AS berat
            FROM {t} o LEFT JOIN meta.road_positions p USING (road_segment_id)
            WHERE o.incident_type <> 0{wo}
            GROUP BY 1, 2 ORDER BY 3 DESC, 1 LIMIT 10""",
        "trend": f"""
            SELECT date_trunc('{bucket}', observed_at) AS periode, incident_type, count(*) AS insiden
            FROM {t} WHERE incident_type <> 0{w}
            GROUP BY 1, 2 ORDER BY 1, 2""",
        "recent": f"""
            SELECT o.observed_at, o.road_segment_id, p.road_name, o.incident_type,
                   o.average_speed, o.jam_density_index, o.weather_condition
            FROM {t} o LEFT JOIN meta.road_positions p USING (road_segment_id)
            WHERE o.incident_type <> 0{wo}
            ORDER BY o.observed_at DESC LIMIT 30""",
    }


def _run_incident(source: str) -> dict:
    return {k: query(sql) for k, sql in _incident_queries(source).items()}


@st.cache_data(ttl=3600)
def _incidents_batch() -> dict:
    return _run_incident("batch")


@st.cache_data(ttl=10)
def _incidents_live() -> dict:
    return _run_incident("live")


def incidents(source: str = "batch") -> dict:
    return _incidents_live() if source == "live" else _incidents_batch()


# ---------------------------------------------------------------- data quality
@st.cache_data(ttl=60)
def quality_results() -> pd.DataFrame:
    return query("""
        SELECT id, run_id, run_ts, category, metric_name, value, detail
        FROM meta.quality_results
        ORDER BY run_ts, id
    """)


QUALITY_EVENT_HOURS = 6


@st.cache_data(ttl=15)
def stream_quality() -> dict:
    """Live integrity checks on the streaming sink, over the last
    QUALITY_EVENT_HOURS whole hours of event time. The window is computed first
    and passed as plain parameters, so every check is an index range scan
    (observed_at; the hourly table's (obs_date, obs_hour)) and stays fast at any
    table size. Whole hours only, so an hour still filling up is not counted
    as an aggregation mismatch."""
    lim = query(f"""
        SELECT date_trunc('hour', max(observed_at) - interval '{QUALITY_EVENT_HOURS} hours') + interval '1 hour' AS dari,
               date_trunc('hour', max(observed_at)) AS sampai
        FROM core.observations_stream
    """).iloc[0]
    if pd.isna(lim["dari"]):
        return {"total": 0, "hours": QUALITY_EVENT_HOURS}
    dari, sampai = pd.Timestamp(lim["dari"]), pd.Timestamp(lim["sampai"])
    df = query("""
        SELECT count(*)                                                      AS total,
               count(DISTINCT s.observed_at)                                 AS unik,
               count(*) FILTER (WHERE s.obs_hour <> extract(hour FROM s.observed_at)) AS jam_tidak_cocok,
               count(*) FILTER (WHERE c.observed_at IS NULL)                 AS tidak_ada_di_batch
        FROM core.observations_stream s
        LEFT JOIN core.observations c USING (observed_at)
        WHERE s.observed_at >= :dari AND s.observed_at < :sampai
    """, dari=dari.to_pydatetime(), sampai=sampai.to_pydatetime())
    agg = query("""
        SELECT coalesce(sum(n_observations), 0) AS n
        FROM mart.road_hourly_stream
        WHERE (obs_date, obs_hour) >= (:d1, :h1) AND (obs_date, obs_hour) < (:d2, :h2)
    """, d1=dari.date(), h1=int(dari.hour), d2=sampai.date(), h2=int(sampai.hour))
    out = df.iloc[0].to_dict()
    out.update(event_di_agregasi=int(agg.iloc[0]["n"]), dari=dari, sampai=sampai, hours=QUALITY_EVENT_HOURS)
    return out
