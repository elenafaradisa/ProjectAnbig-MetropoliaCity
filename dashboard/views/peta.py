"""Peta lalu lintas: every road segment drawn along its real Islamabad road
(meta.road_positions, OSM geometry), colored by congestion status, with an
incident icon at the road's midpoint. Clicking a road opens its detail panel.
"""
import json
import re

import altair as alt
import folium
import pandas as pd
import streamlit as st
from streamlit_folium import st_folium

from dashboard import db
from dashboard.theme import ALERT, JDI_MACET, JDI_PADAT, STATUS, congestion_status, fmt, incident_types, pill, tgl

ISLAMABAD = (33.685, 73.065)
INCIDENT_ICON = """
<svg width="26" height="26" viewBox="-13 -13 26 26" xmlns="http://www.w3.org/2000/svg">
  <circle r="11" fill="#FFFFFF" stroke="#C8322B" stroke-width="2"/>
  <path d="M0 -6 L6 5 L-6 5 Z" fill="#C8322B" stroke="#C8322B" stroke-width="1.5" stroke-linejoin="round"/>
  <rect x="-0.8" y="-2.6" width="1.6" height="4.2" rx="0.8" fill="#FFFFFF"/>
  <circle cx="0" cy="3.3" r="0.9" fill="#FFFFFF"/>
</svg>"""
TOOLTIP_ID = re.compile(r"Segmen (\d+)")


LIVE_REFRESH = 30  # seconds; the Folium map is redrawn on refresh (zoom resets), so not too often

st.markdown('<div class="mc-crumb">Operasional / Peta lalu lintas</div>', unsafe_allow_html=True)
st.title("Peta lalu lintas")
src_label = st.segmented_control(
    "Sumber data", ["Live (streaming)", "Historis (batch)"], default="Historis (batch)", key="peta_source",
    help="Live: mart.road_current_status_live, dibangun Spark Structured Streaming (jendela 1 jam, watermark). "
         "Historis: mart.road_current_status dari pipeline batch.",
) or "Historis (batch)"
SOURCE = "live" if src_label.startswith("Live") else "batch"


def render(source: str):
    # ---------------------------------------------------------------- data
    df = db.road_current_status(source)
    if df.empty:
        st.warning("Belum ada data streaming. Jalankan Kafka, streaming job, dan producer." if source == "live" else
                   "Belum ada data: mart.road_current_status atau meta.road_positions (osm_assignment) kosong.")
        return

    df["status"] = df["avg_jam_density_index"].map(congestion_status)
    df["road_name"] = df["road_name"].fillna("Tanpa nama")  # some OSM ways have no name tag
    df["highway"] = df["highway"].fillna("")
    df["path"] = df["path"].map(lambda p: p if isinstance(p, list) else json.loads(p))
    df["obs_ts"] = pd.to_datetime(df["obs_date"]) + pd.to_timedelta(df["obs_hour"], unit="h")
    inc_names = incident_types()
    df["incident_name"] = df["max_incident_type"].map(lambda t: inc_names.get(int(t), {}).get("name") if pd.notna(t) else None)
    has_incident = df["incident_count"].fillna(0) > 0

    # ---------------------------------------------------------------- caption
    if source == "live":
        fresh = pd.to_datetime(df["updated_at"]).max().tz_convert("Asia/Jakarta")
        st.caption(
            f"Live dari streaming · jendela 1 jam per segmen (waktu event {tgl(df['obs_ts'].min())} – {tgl(df['obs_ts'].max())}) · "
            f"diperbarui {fresh:%H:%M:%S} WIB · peta dimuat ulang tiap {LIVE_REFRESH} detik · "
            f"kepadatan dari jam density index (tinggi ≥ {JDI_MACET:.0f}, sedang ≥ {JDI_PADAT:.0f})"
        )
    else:
        st.caption(
            f"Historis (batch) · kondisi terakhir tiap segmen · {tgl(df['obs_ts'].min())} – {tgl(df['obs_ts'].max())} · "
            f"kepadatan dari jam density index (tinggi ≥ {JDI_MACET:.0f}, sedang ≥ {JDI_PADAT:.0f})"
        )

    # ---------------------------------------------------------------- KPIs
    n_macet = int((df["status"] == "macet").sum())
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Kepadatan tinggi", f"{n_macet} / {len(df)}",
              help=f"Segmen dengan jam density index ≥ {JDI_MACET:.0f}. Ini mengukur kepadatan, bukan kecepatan: "
                   "di dataset ini jam density tidak berkorelasi dengan kecepatan (rata-rata 60 km/j di semua "
                   "tingkat kepadatan), jadi segmen berkepadatan tinggi bisa saja berkecepatan tinggi.")
    k2.metric("Segmen dengan insiden", int(has_incident.sum()))
    k3.metric("Kecepatan rata-rata", f"{fmt(df['avg_average_speed'].mean(), 1)} km/j")
    k4.metric("Jam density rata-rata", fmt(df["avg_jam_density_index"].mean(), 1))

    # ---------------------------------------------------------------- selection state
    if st.session_state.get("selected_segment") not in set(df["road_segment_id"]):
        ranked = df.assign(_inc=has_incident).sort_values(["_inc", "avg_jam_density_index"], ascending=False)
        st.session_state.selected_segment = int(ranked.iloc[0]["road_segment_id"])

    map_col, detail_col = st.columns([2.6, 1], gap="medium")

    # ---------------------------------------------------------------- map
    with map_col:
        with st.container(border=True, key="card_map"):
            head_l, head_r = st.columns([1, 1], vertical_alignment="center")
            head_l.markdown("**Status segmen**")
            view = head_r.segmented_control(
                "Filter peta", ["Semua", "Kepadatan tinggi", "Insiden"], default="Semua",
                label_visibility="collapsed", key="map_filter",
            ) or "Semua"

            shown = df
            if view == "Kepadatan tinggi":
                shown = df[df["status"] == "macet"]
            elif view == "Insiden":
                shown = df[has_incident]

            # Esri Light Gray Canvas: a muted basemap with English labels, made for
            # overlaying data (OSM standard tiles label Islamabad in Urdu; CARTO's
            # light basemap now needs an API key).
            m = folium.Map(location=ISLAMABAD, zoom_start=12, tiles="Esri.WorldGrayCanvas",
                           control_scale=True, prefer_canvas=True)
            m.get_root().header.add_child(folium.Element(
                "<style>.leaflet-control-scale-line:nth-child(2){display:none}</style>"))  # km only, no miles

            # draw lancar first so congested roads sit on top
            order = {"lancar": 0, "padat": 1, "macet": 2}
            for row in shown.sort_values("status", key=lambda s: s.map(order)).itertuples():
                st_ = STATUS[row.status]
                selected = row.road_segment_id == st.session_state.selected_segment
                tip = f"Segmen {row.road_segment_id} · {row.road_name} · {st_['long']}"
                if selected:  # dark halo under the selected road
                    folium.PolyLine(row.path, color="#141414", weight=st_["weight"] + 8, opacity=0.18).add_to(m)
                folium.PolyLine(row.path, color=st_["color"], weight=st_["weight"] + (2 if selected else 0),
                                opacity=0.95, tooltip=tip).add_to(m)

            for row in shown[has_incident.loc[shown.index]].itertuples():
                folium.Marker(
                    [row.mid_lat, row.mid_lon],
                    icon=folium.DivIcon(html=INCIDENT_ICON, icon_size=(26, 26), icon_anchor=(13, 13)),
                    tooltip=f"Segmen {row.road_segment_id} · {row.incident_name or 'Insiden'}",
                ).add_to(m)

            # one compact row at the bottom-left (above the scale bar), so it covers
            # as little of the road network as possible
            legend = "".join(
                f'<span style="display:inline-flex;align-items:center;gap:6px">'
                f'<span style="width:16px;height:4px;border-radius:2px;background:{v["color"]}"></span>'
                f'{v["label"]} <span style="color:#5F5F5B">{int((df.status == k).sum())}</span></span>'
                for k, v in (("macet", STATUS["macet"]), ("padat", STATUS["padat"]), ("lancar", STATUS["lancar"]))
            )
            legend = '<span style="color:#5F5F5B">Kepadatan:</span>' + legend
            m.get_root().html.add_child(folium.Element(
                '<div style="position:absolute;bottom:34px;left:10px;z-index:9999;background:rgba(255,255,255,0.94);'
                'border:1px solid #E2E2DE;border-radius:8px;padding:5px 10px;display:flex;gap:14px;align-items:center;'
                'font:12px Geist,system-ui,sans-serif;color:#141414;white-space:nowrap">'
                f'{legend}<span style="color:#5F5F5B">&#9888; insiden = titik tengah jalan</span></div>'
            ))

            out = st_folium(m, height=560, use_container_width=True, key="peta",
                            returned_objects=["last_object_clicked_tooltip"])
            # st_folium keeps returning the LAST click on every rerun, so only act
            # when it changes -- otherwise it would undo a pick made in the table
            clicked = (out or {}).get("last_object_clicked_tooltip")
            if clicked and clicked != st.session_state.get("last_map_click"):
                st.session_state.last_map_click = clicked
                hit = TOOLTIP_ID.search(clicked)
                if hit and int(hit.group(1)) != st.session_state.selected_segment:
                    st.session_state.selected_segment = int(hit.group(1))
                    st.rerun()

    # ---------------------------------------------------------------- detail panel
    with detail_col:
        sel = df[df["road_segment_id"] == st.session_state.selected_segment].iloc[0]
        s = STATUS[sel["status"]]
        with st.container(border=True, key="card_detail"):
            st.markdown(
                f'<div style="display:flex;justify-content:space-between;align-items:center">'
                f'<span class="mc-mono" style="color:#5F5F5B;font-size:13px">Segmen {sel.road_segment_id}</span>'
                f'{pill(s["long"], s["bg"], s["fg"])}</div>',
                unsafe_allow_html=True,
            )
            st.subheader(sel.road_name, anchor=False)
            st.markdown(
                f'<div class="mc-muted">{sel.highway.title()} · {fmt(sel.length_m / 1000, 2)} km · '
                f'OSM way {sel.osm_way_id} · data {tgl(sel.obs_ts)}</div>',
                unsafe_allow_html=True,
            )

            if sel.incident_count and sel.incident_count > 0:
                info = inc_names.get(int(sel.max_incident_type), {})
                st.markdown(
                    f'<div class="mc-incident"><b>{info.get("name", "Insiden")}</b><br>'
                    f'<span class="mc-muted">{int(sel.incident_count)} kejadian dalam jam ini · '
                    f'Kontak: {info.get("contact") or "—"}</span></div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown('<div class="mc-muted" style="margin:6px 0 12px">Tidak ada insiden pada jam ini.</div>',
                            unsafe_allow_html=True)

            st.markdown(
                '<div class="mc-grid">'
                f'<div><span>Kecepatan</span><span>{fmt(sel.avg_average_speed, 1)} km/j</span></div>'
                f'<div><span>Kendaraan</span><span>{fmt(sel.avg_vehicle_count, 1)}</span></div>'
                f'<div><span>Okupansi lajur</span><span>{fmt(sel.avg_lane_occupancy_rate, 2)}</span></div>'
                f'<div><span>Jam density</span><span>{fmt(sel.avg_jam_density_index, 1)}</span></div>'
                '</div>',
                unsafe_allow_html=True,
            )

            a_bg, a_fg = ALERT.get(sel.alert_level, ALERT["normal"])
            v_bg, v_fg = ALERT.get(sel.v2x_status, ALERT["normal"])
            st.markdown(
                f'<div class="mc-muted" style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:10px">'
                f'Anomali {pill(sel.alert_level or "—", a_bg, a_fg)} V2X {pill(sel.v2x_status or "—", v_bg, v_fg)}</div>',
                unsafe_allow_html=True,
            )

            hist = db.segment_recent_windows(int(sel.road_segment_id), source=source)
            if not hist.empty:
                st.markdown("**Kecepatan, 2 jam terakhir**" + (" (waktu event)" if source == "live" else ""))
                st.markdown(f'<div class="mc-muted" style="margin-top:-8px">per jendela 15 menit · '
                            f'{len(hist)} dari 8 jendela berisi data</div>', unsafe_allow_html=True)
                chart = (
                    alt.Chart(hist)
                    .mark_line(color="#141414", strokeWidth=1.8, point=alt.OverlayMarkDef(color="#141414", size=22))
                    .encode(
                        # temporal axis = spacing proportional to real time, so a
                        # missing window shows up as a longer gap between points
                        x=alt.X("window_15min:T", title=None,
                                axis=alt.Axis(format="%H:%M", grid=False, labelColor="#6E6E69", tickCount=5)),
                        y=alt.Y("avg_average_speed:Q", title=None, scale=alt.Scale(zero=False),
                                axis=alt.Axis(gridColor="#ECECE9", labelColor="#6E6E69", tickCount=4)),
                        tooltip=[alt.Tooltip("window_15min:T", title="Jam", format="%H:%M"),
                                 alt.Tooltip("avg_average_speed:Q", title="km/j", format=".1f")],
                    )
                    .properties(height=140)
                    .configure_view(stroke=None)
                    .configure(background="#FFFFFF")
                )
                st.altair_chart(chart, width="stretch")

    # ---------------------------------------------------------------- table
    with st.container(border=True, key="card_table"):
        t_left, t_right = st.columns([2.4, 1.6], vertical_alignment="center")
        t_left.markdown(f"**Segmen dengan kepadatan tertinggi** <span class='mc-muted'>· filter peta: {view}</span>",
                        unsafe_allow_html=True)
        show_all = t_right.toggle(f"Tampilkan semua {len(shown)} segmen", key="table_all")

        ranked = shown.sort_values("avg_jam_density_index", ascending=False)
        if not show_all:
            ranked = ranked.head(10)
        table = pd.DataFrame({
            "Segmen": ranked["road_segment_id"].astype(int),
            "Jalan": ranked["road_name"],
            "Kepadatan": ranked["status"].map(lambda k: STATUS[k]["label"]),
            "Jam density": ranked["avg_jam_density_index"],
            "Kecepatan": ranked["avg_average_speed"],
            "Kendaraan": ranked["avg_vehicle_count"],
            "Insiden": ranked["incident_name"].where(has_incident.loc[ranked.index], "—"),
            "Anomali": ranked["alert_level"].fillna("—"),
        }).reset_index(drop=True)

        event = st.dataframe(
            table,
            hide_index=True,
            width="stretch",
            on_select="rerun",
            selection_mode="single-row",
            key="tabel_segmen",
            column_config={
                "Segmen": st.column_config.NumberColumn(format="%d", width="small"),
                "Jam density": st.column_config.ProgressColumn(min_value=0, max_value=100, format="%.1f"),
                "Kecepatan": st.column_config.NumberColumn(format="%.1f km/j"),
                "Kendaraan": st.column_config.NumberColumn(format="%.1f"),
            },
        )
        st.markdown('<div class="mc-muted">Klik satu baris untuk membuka detail segmen dan menyorotnya di peta.</div>',
                    unsafe_allow_html=True)

        # same "act only on change" rule as the map click above
        rows = event.selection.rows if event else []
        picked = int(table.iloc[rows[0]]["Segmen"]) if rows else None
        if picked is not None and picked != st.session_state.get("last_table_pick"):
            st.session_state.last_table_pick = picked
            if picked != st.session_state.selected_segment:
                st.session_state.selected_segment = picked
                st.rerun()


# Live mode re-runs this whole section on a timer; batch mode renders once.
st.fragment(run_every=LIVE_REFRESH if SOURCE == "live" else None)(render)(SOURCE)
