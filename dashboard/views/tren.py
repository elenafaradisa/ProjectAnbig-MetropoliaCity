"""Tren kepadatan: two years of batch data by day-of-week x hour, per day, and
per shift (mart.road_hourly_stats, mart.shift_summary).

The honest headline of this page is usually "no pattern": traffictab23's
columns are generated independently and uniformly (docs/keterbatasan.md), so
every hour looks like every other hour. The page is built so that this is
visible rather than hidden: the heatmap defaults to the metric's full scale
(where a flat pattern looks flat), and the zoomed-in scale -- which would
turn tiny noise into dramatic colours -- is opt-in and labelled as such.
"""
import altair as alt
import pandas as pd
import streamlit as st

from dashboard import db
from dashboard.theme import fmt, tgl

DAYS = {1: "Senin", 2: "Selasa", 3: "Rabu", 4: "Kamis", 5: "Jumat", 6: "Sabtu", 0: "Minggu"}  # extract(dow)
DAY_ORDER = list(DAYS.values())
METRICS = {
    # label: (column, unit, decimals, full-scale domain, higher-is-worse)
    "Jam density": ("jam_density", "", 1, (0, 100), True),
    "Kecepatan": ("kecepatan", " km/j", 1, (0, 100), False),
    "Insiden per jam": ("insiden_per_jam", "", 3, (0, 1), True),
    "Tingkat anomali": ("anomaly_rate", "", 3, (0, 1), True),
}
DAILY_COL = {"jam_density": "jam_density", "kecepatan": "kecepatan", "insiden_per_jam": "insiden", "anomaly_rate": None}
AXIS = dict(labelColor="#6E6E69", titleColor="#6E6E69", gridColor="#ECECE9")

st.markdown('<div class="mc-crumb">Analisis / Tren kepadatan</div>', unsafe_allow_html=True)
st.title("Tren kepadatan")

how = db.hour_of_week_profile()
daily = db.daily_profile()
if how.empty:
    st.warning("Belum ada data: mart.road_hourly_stats kosong. Jalankan pipeline batch terlebih dahulu.")
    st.stop()
daily["obs_date"] = pd.to_datetime(daily["obs_date"])
# The data ends at 2024-01-01 00:00, so the last "day" holds a single hour and
# its average is a misleading spike: drop days with under half the usual
# number of observations.
daily = daily[daily["observasi"] >= 0.5 * daily["observasi"].median()]
st.caption(
    f"Data historis (batch) · {tgl(daily['obs_date'].min()).rsplit(' ', 1)[0]} – "
    f"{tgl(daily['obs_date'].max()).rsplit(' ', 1)[0]} · rata-rata semua segmen per hari dan jam"
)

label = st.segmented_control("Metrik", list(METRICS), default="Jam density", key="tren_metric") or "Jam density"
col, unit, dec, full_domain, worse_high = METRICS[label]
how["hari"] = how["day_of_week"].map(DAYS)

# ---------------------------------------------------------------- KPIs
mean = how[col].mean()
hi = how.loc[how[col].idxmax()]
lo = how.loc[how[col].idxmin()]
spread = (hi[col] - lo[col]) / mean * 100 if mean else 0
k1, k2, k3, k4 = st.columns(4)
k1.metric(f"Rata-rata {label.lower()}", f"{fmt(mean, dec)}{unit}")
k2.metric("Jam tertinggi", f"{fmt(hi[col], dec)}{unit}", help=f"{hi['hari']}, {int(hi['obs_hour']):02d}:00")
k3.metric("Jam terendah", f"{fmt(lo[col], dec)}{unit}", help=f"{lo['hari']}, {int(lo['obs_hour']):02d}:00")
k4.metric("Selisih tertinggi–terendah", f"{fmt(spread, 1)}%",
          help="Selisih jam tertinggi dan terendah, dibagi rata-rata. Pada data lalu lintas nyata, "
               "jam sibuk biasanya berbeda puluhan persen dari jam sepi.")

if spread < 10:
    st.info(
        f"**Tidak ada pola jam sibuk yang berarti.** Dari 168 kombinasi hari × jam, nilai tertinggi dan terendah "
        f"hanya berselisih {fmt(spread, 1)}% dari rata-rata. Ini konsisten dengan temuan audit: kolom-kolom "
        "traffictab23 dibangkitkan secara acak dan seragam, sehingga setiap jam terlihat sama."
    )

# ---------------------------------------------------------------- heatmap
with st.container(border=True, key="card_heat"):
    h_left, h_right = st.columns([1.6, 1], vertical_alignment="center")
    h_left.markdown(f"**{label} per hari × jam**")
    zoom = h_right.toggle("Perbesar skala warna", key="tren_zoom",
                          help="Skala penuh memakai rentang wajar metrik ini, sehingga pola datar terlihat datar. "
                               "Skala diperbesar memakai rentang data saja: perbedaan kecil terlihat kontras, "
                               "jadi jangan dibaca sebagai pola besar.")
    domain = [float(how[col].min()), float(how[col].max())] if zoom else list(full_domain)
    scheme = ["#F7F3EE", "#E08A1E", "#C8322B"] if worse_high else ["#C8322B", "#E08A1E", "#F7F3EE"]
    heat = (
        alt.Chart(how)
        .mark_rect(stroke="#FFFFFF", strokeWidth=1)
        .encode(
            x=alt.X("obs_hour:O", title="Jam", axis=alt.Axis(labelAngle=0, **AXIS)),
            y=alt.Y("hari:N", sort=DAY_ORDER, title=None, axis=alt.Axis(labelOverlap=False, **AXIS)),
            color=alt.Color(f"{col}:Q", title=None,
                            scale=alt.Scale(domain=domain, range=scheme, clamp=True),
                            legend=alt.Legend(orient="bottom", gradientLength=260, labelColor="#6E6E69")),
            tooltip=[alt.Tooltip("hari:N", title="Hari"), alt.Tooltip("obs_hour:O", title="Jam"),
                     alt.Tooltip(f"{col}:Q", title=label, format=f".{dec}f"),
                     alt.Tooltip("n_segment_jam:Q", title="Segmen-jam", format=",")],
        )
        .properties(height=alt.Step(30))  # one 30px row per day, so all seven day labels fit
        .configure_view(stroke=None).configure(background="#FFFFFF")
    )
    st.altair_chart(heat, width="stretch")
    st.markdown(
        f'<div class="mc-muted">Skala warna: {"diperbesar ke rentang data (" + fmt(domain[0], dec) + "–" + fmt(domain[1], dec) + ")" if zoom else "penuh (" + fmt(domain[0], 0) + "–" + fmt(domain[1], 0) + ")"}. '
        'Hari dihitung dari tanggal observasi.</div>',
        unsafe_allow_html=True,
    )

# ---------------------------------------------------------------- daily trend + shifts
left, right = st.columns([1.5, 1], gap="medium")

with left, st.container(border=True, key="card_daily"):
    dcol = DAILY_COL[col]
    if dcol is None:
        st.markdown(f"**{label} per hari**")
        st.markdown('<div class="mc-muted">Tidak tersedia per hari untuk metrik ini.</div>', unsafe_allow_html=True)
    else:
        title = "Insiden per hari" if dcol == "insiden" else f"{label} per hari"
        st.markdown(f"**{title}** <span class='mc-muted'>· garis tebal = rata-rata 7 hari</span>",
                    unsafe_allow_html=True)
        d = daily[["obs_date", dcol]].rename(columns={dcol: "nilai"}).copy()
        d["rata7"] = d["nilai"].rolling(7, min_periods=1).mean()
        base = alt.Chart(d).encode(x=alt.X("obs_date:T", title=None,
                                           axis=alt.Axis(format="%m/%Y", tickCount={"interval": "month", "step": 3}, **AXIS)))
        chart = (
            alt.layer(
                base.mark_line(color="#C9C9C4", strokeWidth=1).encode(
                    y=alt.Y("nilai:Q", title=None, axis=alt.Axis(**AXIS), scale=alt.Scale(zero=dcol != "kecepatan"))),
                base.mark_line(color="#141414", strokeWidth=2).encode(
                    y="rata7:Q",
                    tooltip=[alt.Tooltip("obs_date:T", title="Tanggal", format="%d/%m/%Y"),
                             alt.Tooltip("nilai:Q", title="Nilai", format=".2f"),
                             alt.Tooltip("rata7:Q", title="Rata-rata 7 hari", format=".2f")]),
            )
            .properties(height=230)
            .configure_view(stroke=None).configure(background="#FFFFFF")
        )
        st.altair_chart(chart, width="stretch")

with right, st.container(border=True, key="card_shift"):
    st.markdown("**Insiden per jam, per shift petugas**")
    sh = db.shift_summary()
    if sh.empty:
        st.markdown('<div class="mc-muted">mart.shift_summary kosong.</div>', unsafe_allow_html=True)
    else:
        order = ["Pagi (05-10)", "Siang (10-15)", "Sore (15-20)", "Malam (20-05)"]
        bars = (
            alt.Chart(sh)
            .mark_bar(color="#141414", cornerRadiusEnd=3, height=20)
            .encode(
                y=alt.Y("shift_name:N", sort=order, title=None, axis=alt.Axis(domain=False, ticks=False, **AXIS)),
                x=alt.X("avg_incident_per_hour:Q", title="rata-rata insiden per segmen per jam",
                        scale=alt.Scale(domain=[0, max(0.2, float(sh["avg_incident_per_hour"].max()) * 1.25)]),
                        axis=alt.Axis(**AXIS)),
                tooltip=[alt.Tooltip("shift_name:N", title="Shift"),
                         alt.Tooltip("avg_incident_per_hour:Q", title="Insiden per jam", format=".4f"),
                         alt.Tooltip("avg_anomaly_rate:Q", title="Tingkat anomali", format=".4f")],
            )
            .properties(height=200)
            .configure_view(stroke=None).configure(background="#FFFFFF")
        )
        st.altair_chart(bars, width="stretch")
        lo_s, hi_s = sh["avg_incident_per_hour"].min(), sh["avg_incident_per_hour"].max()
        st.markdown(
            f'<div class="mc-muted">Antar-shift hanya berbeda {fmt((hi_s - lo_s) / lo_s * 100, 2)}%. '
            'Untuk dataset ini, kebutuhan petugas tidak bisa didasarkan pada perbedaan shift.</div>',
            unsafe_allow_html=True,
        )
