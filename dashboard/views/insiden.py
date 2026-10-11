"""Insiden: incidents (incident_type != 0) by type, by segment, over time, and
the latest ones with who to contact -- from the batch table (two years) or the
streaming table (live).

incident_type follows config/incident_types.yaml (1 Minor collision,
2 Major accident, 3 Signal disruption, 4 Road blockage); types 2 and 4 are
the ones the alert rules treat as critical ("kritis").
"""
import altair as alt
import pandas as pd
import streamlit as st

from dashboard import db
from dashboard.theme import fmt, incident_types, tgl

LIVE_REFRESH = 15
TYPE_COLOR = {1: "#3B6EA8", 2: "#C8322B", 3: "#7E3F98", 4: "#E08A1E"}
AXIS = dict(labelColor="#6E6E69", titleColor="#6E6E69", gridColor="#ECECE9")

st.markdown('<div class="mc-crumb">Operasional / Insiden</div>', unsafe_allow_html=True)
st.title("Insiden")
src_label = st.segmented_control(
    "Sumber data", ["Live (streaming)", "Historis (batch)"], default="Historis (batch)", key="insiden_source",
    help="Live: core.observations_stream (data yang sedang di-replay lewat Kafka). "
         "Historis: core.observations, seluruh data dua tahun dari pipeline batch.",
) or "Historis (batch)"
SOURCE = "live" if src_label.startswith("Live") else "batch"

names = incident_types()
type_name = {k: v["name"] for k, v in names.items() if k != 0}
type_contact = {k: v.get("contact") or "—" for k, v in names.items() if k != 0}


def render(source: str):
    data = db.incidents(source)
    summ = data["summary"].iloc[0]
    if not summ["observasi"]:
        st.info("Belum ada data streaming. Jalankan Kafka, streaming job, lalu producer."
                if source == "live" else "core.observations kosong.")
        return

    total, obs = int(summ["insiden"]), int(summ["observasi"])
    by_type = data["by_type"].assign(
        jenis=lambda d: d["incident_type"].map(type_name),
        kontak=lambda d: d["incident_type"].map(type_contact),
        porsi=lambda d: d["insiden"] / max(total, 1),
    )
    top = data["top_segments"]
    st.caption(
        f"{'Live dari streaming · ' + str(db.LIVE_EVENT_HOURS) + ' jam waktu event terakhir' if source == 'live' else 'Historis (batch)'} · "
        f"{tgl(pd.Timestamp(summ['dari']))} – {tgl(pd.Timestamp(summ['sampai']))} · insiden = observasi dengan "
        f"incident_type ≠ 0" + (f" · diperbarui tiap {LIVE_REFRESH} detik" if source == "live" else "")
    )

    # ------------------------------------------------------------ KPIs
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total insiden", f"{total:,}".replace(",", "."))
    k2.metric("Porsi observasi", f"{fmt(total / obs * 100, 1)}%",
              help=f"{total:,} dari {obs:,} observasi mencatat insiden.".replace(",", "."))
    if not by_type.empty:
        most = by_type.loc[by_type["insiden"].idxmax()]
        k3.metric("Jenis terbanyak", most["jenis"], help=f"{fmt(most['porsi'] * 100, 1)}% dari semua insiden")
    severe = int(by_type.loc[by_type["incident_type"].isin([2, 4]), "insiden"].sum())
    k4.metric("Insiden berat", f"{severe:,}".replace(",", "."),
              help="Major accident dan Road blockage: dua jenis yang membuat alert_level segmen menjadi 'kritis'.")

    # ------------------------------------------------------------ by type + top segments
    left, right = st.columns([1, 1], gap="medium")
    with left, st.container(border=True, key="card_type"):
        st.markdown("**Insiden per jenis**")
        if by_type.empty:
            st.markdown('<div class="mc-muted">Belum ada insiden.</div>', unsafe_allow_html=True)
        else:
            order = [type_name[k] for k in sorted(type_name)]
            bars = (
                alt.Chart(by_type)
                .mark_bar(cornerRadiusEnd=3, height=22)
                .encode(
                    y=alt.Y("jenis:N", sort=order, title=None, axis=alt.Axis(domain=False, ticks=False, **AXIS)),
                    x=alt.X("insiden:Q", title=None, axis=alt.Axis(**AXIS)),
                    color=alt.Color("jenis:N", legend=None,
                                    scale=alt.Scale(domain=order, range=[TYPE_COLOR[k] for k in sorted(type_name)])),
                    tooltip=[alt.Tooltip("jenis:N", title="Jenis"), alt.Tooltip("insiden:Q", title="Insiden", format=","),
                             alt.Tooltip("porsi:Q", title="Porsi", format=".1%"), alt.Tooltip("kontak:N", title="Kontak")],
                )
                .properties(height=190)
                .configure_view(stroke=None).configure(background="#FFFFFF")
            )
            st.altair_chart(bars, width="stretch")
            st.dataframe(
                by_type[["jenis", "insiden", "porsi", "kontak"]].rename(
                    columns={"jenis": "Jenis", "insiden": "Insiden", "porsi": "Porsi", "kontak": "Kontak penanganan"}),
                hide_index=True, width="stretch",
                column_config={"Porsi": st.column_config.NumberColumn(format="percent"),
                               "Insiden": st.column_config.NumberColumn(format="localized")},
            )

    with right, st.container(border=True, key="card_topseg"):
        st.markdown("**Segmen dengan insiden terbanyak**")
        if top.empty:
            st.markdown('<div class="mc-muted">Belum ada insiden.</div>', unsafe_allow_html=True)
        else:
            st.dataframe(
                pd.DataFrame({
                    "Segmen": top["road_segment_id"],
                    "Jalan": top["road_name"].fillna("Tanpa nama"),
                    "Insiden": top["insiden"],
                    "Berat": top["berat"],
                }),
                hide_index=True, width="stretch", height=390,
                column_config={
                    "Segmen": st.column_config.NumberColumn(format="%d"),
                    "Insiden": st.column_config.ProgressColumn(min_value=0, max_value=int(top["insiden"].max()), format="%d"),
                    "Berat": st.column_config.NumberColumn(format="%d",
                                                           help="Major accident + Road blockage"),
                },
            )
            if source == "batch":
                spread = (top["insiden"].max() - top["insiden"].min()) / top["insiden"].max() * 100
                st.markdown(
                    f'<div class="mc-muted">Antara segmen ke-1 dan ke-10 hanya berselisih {fmt(spread, 1)}%: '
                    'insiden tersebar hampir merata di semua segmen, sejalan dengan temuan audit data.</div>',
                    unsafe_allow_html=True,
                )

    # ------------------------------------------------------------ trend
    with st.container(border=True, key="card_trend"):
        per = "bulan" if source == "batch" else "jam (waktu event)"
        st.markdown(f"**Insiden per {per}, per jenis**")
        tr = data["trend"]
        if tr.empty:
            st.markdown('<div class="mc-muted">Belum ada insiden.</div>', unsafe_allow_html=True)
        else:
            tr = tr.assign(jenis=tr["incident_type"].map(type_name), periode=pd.to_datetime(tr["periode"]))
            order = [type_name[k] for k in sorted(type_name)]
            fmt_x = "%m/%Y" if source == "batch" else "%d/%m %H:%M"
            chart = (
                alt.Chart(tr)
                .mark_bar()
                .encode(
                    x=alt.X("periode:T", title=None, timeUnit="yearmonth" if source == "batch" else "yearmonthdatehours",
                            axis=alt.Axis(format=fmt_x, labelAngle=0, **AXIS)),
                    y=alt.Y("insiden:Q", title=None, stack="zero", axis=alt.Axis(**AXIS)),
                    color=alt.Color("jenis:N", title=None, sort=order,
                                    scale=alt.Scale(domain=order, range=[TYPE_COLOR[k] for k in sorted(type_name)]),
                                    legend=alt.Legend(orient="top", labelColor="#141414")),
                    order=alt.Order("incident_type:Q"),
                    tooltip=[alt.Tooltip("periode:T", title="Periode", format=fmt_x),
                             alt.Tooltip("jenis:N", title="Jenis"), alt.Tooltip("insiden:Q", title="Insiden", format=",")],
                )
                .properties(height=240)
                .configure_view(stroke=None).configure(background="#FFFFFF")
            )
            st.altair_chart(chart, width="stretch")

    # ------------------------------------------------------------ recent
    with st.container(border=True, key="card_recent_inc"):
        st.markdown("**Insiden terbaru** <span class='mc-muted'>· 30 terakhir menurut waktu event</span>",
                    unsafe_allow_html=True)
        rec = data["recent"]
        if rec.empty:
            st.markdown('<div class="mc-muted">Belum ada insiden.</div>', unsafe_allow_html=True)
        else:
            st.dataframe(
                pd.DataFrame({
                    "Waktu event": pd.to_datetime(rec["observed_at"]).dt.strftime("%d/%m/%Y %H:%M:%S"),
                    "Segmen": rec["road_segment_id"],
                    "Jalan": rec["road_name"].fillna("Tanpa nama"),
                    "Jenis": rec["incident_type"].map(type_name),
                    "Kontak": rec["incident_type"].map(type_contact),
                    "Kecepatan": rec["average_speed"],
                    "Cuaca": rec["weather_condition"],
                }),
                hide_index=True, width="stretch", height=330,
                column_config={"Segmen": st.column_config.NumberColumn(format="%d"),
                               "Kecepatan": st.column_config.NumberColumn(format="%.1f km/j")},
            )


st.fragment(run_every=LIVE_REFRESH if SOURCE == "live" else None)(render)(SOURCE)
