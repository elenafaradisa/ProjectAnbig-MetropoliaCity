"""Streaming langsung: what the Kafka -> Spark Structured Streaming -> Postgres
pipeline is writing into core.observations_stream right now.

Two clocks matter here and are kept apart on purpose:
  - ingested_at: when a row reached Postgres (processing time) -- drives the
    "is data flowing" status and the per-minute throughput chart;
  - observed_at: the replayed traffictab23 timestamp (event time, 2022-2023).

The live part runs in a fragment that re-runs every few seconds, so only this
section refreshes, not the whole page.
"""
import altair as alt
import pandas as pd
import streamlit as st

from dashboard import db
from dashboard.theme import STATUS, congestion_status, fmt, pill, tgl

REFRESH_SECONDS = 5
TRIGGER_SECONDS = 10  # stream_transform.py --trigger-seconds default

st.markdown('<div class="mc-crumb">Operasional / Streaming langsung</div>', unsafe_allow_html=True)
st.title("Streaming langsung")
st.caption(
    "Kafka (traffic_topic) → Spark Structured Streaming (micro-batch tiap "
    f"{TRIGGER_SECONDS} detik) → core.observations_stream · halaman diperbarui otomatis tiap "
    f"{REFRESH_SECONDS} detik. Data adalah replay traffictab23, bukan sensor langsung."
)


@st.fragment(run_every=REFRESH_SECONDS)
def live():
    s = db.stream_summary()
    total = int(s["total"] or 0)
    if total == 0:
        st.info("Belum ada data streaming. Jalankan Kafka, streaming job, lalu producer.")
        return

    since = float(s["seconds_since_last"] or 0)
    if since <= 3 * TRIGGER_SECONDS:
        state, bg, fg = "Mengalir", "#EAF4EE", "#2E6B4A"
    elif since <= 120:
        state, bg, fg = "Melambat", "#FDF1E1", "#8A5208"
    else:
        state, bg, fg = "Berhenti", "#FBE9E7", "#A3241D"
    ago = f"{since:.0f} detik lalu" if since < 120 else f"{since / 60:.0f} menit lalu"
    st.markdown(
        f'<div style="display:flex;gap:10px;align-items:center;margin-bottom:6px">{pill(state, bg, fg)}'
        f'<span class="mc-muted">batch terakhir masuk {ago} ({pd.Timestamp(s["last_ingested"]):%H:%M:%S} WIB)</span></div>',
        unsafe_allow_html=True,
    )

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Event tersimpan", ("" if s["total_exact"] else "≈ ") + f"{total:,}".replace(",", "."),
              help=None if s["total_exact"] else
              "Perkiraan dari statistik PostgreSQL. Menghitung persis jutaan baris setiap beberapa detik "
              "akan memperlambat dashboard.")
    k2.metric("Masuk 1 menit", int(s["last_60s"] or 0),
              help="Event yang masuk ke Postgres dalam 60 detik terakhir. Producer mengirim sekitar 1 event per detik (--interval 1.0).")
    last_ev = pd.Timestamp(s["last_event"])
    k3.metric("Event terakhir", f"{last_ev:%H:%M:%S}",
              help=f"{tgl(last_ev)} -- observed_at dari data traffictab23 yang di-replay (event time), bukan waktu sekarang.")
    v = db.stream_vs_batch()
    if int(v["total"]) == 0:
        k4.metric("Sama dengan batch", "—", help=f"Belum ada event dalam {v['minutes']} menit terakhir.")
    else:
        k4.metric("Sama dengan batch", f"{int(v['ada_di_batch']) - int(v['beda'])} / {int(v['total'])}",
                  help=f"Event yang masuk {v['minutes']} menit terakhir dibandingkan dengan baris yang sama "
                       "(observed_at) di core.observations: segmen, jam, jumlah kendaraan, kecepatan, dan jam "
                       "density harus identik. Ini membuktikan streaming memakai transformasi yang sama dengan batch.")

    left, right = st.columns([1.15, 1], gap="medium")
    with left, st.container(border=True, key="card_tp"):
        st.markdown("**Event masuk per menit** <span class='mc-muted'>· 15 menit terakhir, waktu WIB</span>",
                    unsafe_allow_html=True)
        tp = db.stream_throughput(15)
        tp["jam"] = pd.to_datetime(tp["menit"]).dt.strftime("%H:%M")  # one band per minute
        bars = (
            alt.Chart(tp)
            .mark_bar(color="#141414", cornerRadiusEnd=2)
            .encode(
                x=alt.X("jam:O", sort=None, title=None,
                        axis=alt.Axis(grid=False, labelColor="#6E6E69", labelAngle=0, labelOverlap="greedy")),
                y=alt.Y("baris:Q", title=None, axis=alt.Axis(gridColor="#ECECE9", labelColor="#6E6E69", tickCount=4)),
                tooltip=[alt.Tooltip("jam:O", title="Menit"), alt.Tooltip("baris:Q", title="Event")],
            )
            .properties(height=230)
            .configure_view(stroke=None).configure(background="#FFFFFF")
        )
        st.altair_chart(bars, width="stretch")
        st.markdown('<div class="mc-muted">Menit tanpa event ditampilkan sebagai 0, jadi jeda atau berhentinya '
                    'aliran langsung terlihat.</div>', unsafe_allow_html=True)

    with right, st.container(border=True, key="card_recent"):
        st.markdown("**Event terbaru**", unsafe_allow_html=True)
        rec = db.stream_recent(12)
        rec["Kepadatan"] = rec["jam_density_index"].map(lambda j: STATUS[congestion_status(j)]["label"])
        rec["Insiden"] = rec["incident_name"].where(rec["anomaly_label"] == 1, "—")
        st.dataframe(
            pd.DataFrame({
                "Masuk": pd.to_datetime(rec["masuk"]).dt.strftime("%H:%M:%S"),
                "Waktu event": pd.to_datetime(rec["observed_at"]).dt.strftime("%d/%m/%Y %H:%M:%S"),
                "Segmen": rec["road_segment_id"],
                "Jalan": rec["road_name"].fillna("Tanpa nama"),
                "Kepadatan": rec["Kepadatan"],
                "Kecepatan": rec["average_speed"],
                "Insiden": rec["Insiden"],
            }),
            hide_index=True, width="stretch", height=300,
            column_config={"Segmen": st.column_config.NumberColumn(format="%d"),
                           "Kecepatan": st.column_config.NumberColumn(format="%.1f km/j")},
        )


live()

with st.expander("Bagaimana pipeline ini menjamin tidak ada data dobel?"):
    st.markdown(
        f"""
- **Kafka + checkpoint**: Spark menyimpan offset Kafka yang sudah diproses di
  `data/checkpoints/traffic_stream`. Setelah restart, job melanjutkan dari offset terakhir.
- **Sink idempoten**: checkpoint saja hanya menjamin *at-least-once* untuk penulisan
  `foreachBatch` lewat JDBC. Batch yang crash setelah menulis akan diproses ulang, dan producer yang
  dijalankan ulang mengirim event yang sama lagi. Karena itu tiap micro-batch ditulis ke tabel staging
  lalu digabung dengan `INSERT … ON CONFLICT (observed_at) DO NOTHING`, dengan indeks unik pada
  `observed_at`. Event yang sama, berapa kali pun datang, tersimpan tepat satu kali.
- **Bukti**: kartu *Cocok dengan pipeline batch* membandingkan setiap event streaming dengan baris yang
  sama di `core.observations`. Log streaming job juga menampilkan jumlah baris baru dan yang dilewati
  per batch.
"""
    )
