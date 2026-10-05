"""Polusi udara: Islamabad hourly PM2.5, actual vs the RandomForest model's
next-hour forecast (mart.pollution_forecast, written by the etl_pollution DAG).

Only data_split = 'testing' hours are a fair accuracy check -- the model never
saw them during training. Training hours are in-sample, so their error is
optimistic; gap_fill hours have a prediction but no actual value.
"""
import altair as alt
import pandas as pd
import streamlit as st

from dashboard import db
from dashboard.theme import fmt, tgl

# US EPA PM2.5 AQI categories (2024 revision of the breakpoints). The EPA
# defines them on 24-hour averages; applied here to hourly values, so the
# category is indicative only.
AQI = [  # (upper bound µg/m³, label, color)
    (9.0, "Baik", "#3E9B5B"),
    (35.4, "Sedang", "#D9B600"),
    (55.4, "Tidak sehat (sensitif)", "#E08A1E"),
    (125.4, "Tidak sehat", "#C8322B"),
    (225.4, "Sangat tidak sehat", "#7E3F98"),
    (float("inf"), "Berbahaya", "#7A1F2B"),
]
SPLIT_LABEL = {"testing": "Data uji", "training": "Data latih", "gap_fill": "Jam kosong (gap_fill)"}
RANGES = {"72 jam": 72, "7 hari": 24 * 7, "30 hari": 24 * 30, "Semua": None}
# numeric date formats (Vega's default locale would print English month
# names) and one tick per natural interval, so labels never repeat
# ({"interval": "hour"} passes Altair's schema check but crashes the Vega
# renderer and leaves an empty chart, so 72 jam uses a plain tick count)
AXIS = {
    "72 jam": ("%d/%m %H:%M", 6),
    "7 hari": ("%d/%m", {"interval": "day", "step": 1}),
    "30 hari": ("%d/%m", {"interval": "day", "step": 5}),
    "Semua": ("%m/%Y", {"interval": "month", "step": 6}),
}


def aqi(v):
    for upper, label, color in AQI:
        if v <= upper:
            return label, color
    return AQI[-1][1], AQI[-1][2]


# ---------------------------------------------------------------- data
df = db.pollution_forecast()
if df.empty:
    st.warning("Belum ada data: mart.pollution_forecast kosong. Jalankan DAG etl_pollution terlebih dahulu.")
    st.stop()
df["observed_at"] = pd.to_datetime(df["observed_at"])
meta = db.pollution_model_meta()
test = df[df["data_split"] == "testing"].dropna(subset=["actual_pm2_5"])

# ---------------------------------------------------------------- header
st.markdown('<div class="mc-crumb">Lingkungan / Polusi udara</div>', unsafe_allow_html=True)
st.title("Polusi udara Islamabad")
st.caption(
    f"PM2.5 per jam dan prediksi satu jam ke depan · model {df['model_version'].iloc[0]} "
    f"(RandomForest, fitur lag) · data {tgl(df['observed_at'].min())} – {tgl(df['observed_at'].max())}"
)

# ---------------------------------------------------------------- KPIs
last = df.dropna(subset=["actual_pm2_5"]).iloc[-1]
cat, _ = aqi(last.actual_pm2_5)
mae_test = test["abs_error"].mean() if not test.empty else None
base = meta.get("baseline_mae_testing")

k1, k2, k3, k4 = st.columns(4)
k1.metric(f"PM2.5 terakhir · {cat}", f"{fmt(last.actual_pm2_5, 1)} µg/m³",
          help=f"Jam {tgl(last.observed_at)}. Kategori AQI US EPA (revisi 2024); EPA mendefinisikannya "
               "untuk rata-rata 24 jam, di sini dipakai pada nilai per jam, jadi bersifat indikatif.")
k2.metric("Prediksi model untuk jam itu", f"{fmt(last.predicted_pm2_5, 1)} µg/m³",
          delta=f"{fmt(last.predicted_pm2_5 - last.actual_pm2_5, 1)} µg/m³", delta_color="off",
          help="Panah naik: prediksi lebih tinggi dari nilai aktual; panah turun: lebih rendah.")
k3.metric("MAE data uji", f"{fmt(mae_test, 2)} µg/m³" if mae_test is not None else "—",
          help=f"Rata-rata kesalahan absolut pada {len(test)} jam data uji, yang tidak pernah dilihat model saat dilatih.")
if base and mae_test:
    k4.metric("Lebih baik dari baseline", f"{(1 - mae_test / base) * 100:.0f}%",
              help=f"Baseline persistence (MAE {fmt(base, 2)} µg/m³): menebak PM2.5 jam berikutnya sama dengan "
                   "jam ini. Model berguna hanya kalau kesalahannya lebih kecil dari baseline ini.")
else:
    k4.metric("Lebih baik dari baseline", "—", help="File data/models/pollution_pm25_forecast.meta.json tidak ditemukan.")

# ---------------------------------------------------------------- chart
with st.container(border=True, key="card_pm25"):
    c_left, c_right = st.columns([1.4, 1], vertical_alignment="center")
    c_left.markdown("**PM2.5 aktual vs prediksi**")
    rng = c_right.segmented_control("Rentang", list(RANGES), default="7 hari",
                                    label_visibility="collapsed", key="pm_range") or "7 hari"

    hours = RANGES[rng]
    view = df if hours is None else df[df["observed_at"] > df["observed_at"].max() - pd.Timedelta(hours=hours)]

    plot = view
    if hours is None:
        # ~28k hours is too dense to read (and slow to draw): show daily means;
        # a day counts as "Data uji" if any of its hours is held-out
        plot = (view.set_index("observed_at")
                .resample("D")
                .agg({"actual_pm2_5": "mean", "predicted_pm2_5": "mean",
                      "data_split": lambda s: "testing" if (s == "testing").any() else
                      (s.mode().iloc[0] if not s.empty else None)})
                .dropna(subset=["predicted_pm2_5"]).reset_index())
        c_left.caption("rata-rata harian")
    long = plot.melt(id_vars=["observed_at", "data_split"], value_vars=["actual_pm2_5", "predicted_pm2_5"],
                     var_name="seri", value_name="pm25").dropna(subset=["pm25"])
    long["seri"] = long["seri"].map({"actual_pm2_5": "Aktual", "predicted_pm2_5": "Prediksi"})
    long["bagian"] = long["data_split"].map(SPLIT_LABEL)

    lines = (
        alt.Chart(long)
        .mark_line(strokeWidth=1.6, interpolate="monotone")
        .encode(
            x=alt.X("observed_at:T", title=None,
                    axis=alt.Axis(format=AXIS[rng][0], tickCount=AXIS[rng][1],
                                  grid=False, labelColor="#6E6E69", labelAngle=0)),
            y=alt.Y("pm25:Q", title="µg/m³", axis=alt.Axis(gridColor="#ECECE9", labelColor="#6E6E69",
                                                       titleColor="#6E6E69")),
            color=alt.Color("seri:N", scale=alt.Scale(domain=["Aktual", "Prediksi"], range=["#141414", "#E08A1E"]),
                            legend=alt.Legend(orient="top-left", title=None, labelColor="#141414")),
            strokeDash=alt.StrokeDash("seri:N", scale=alt.Scale(domain=["Aktual", "Prediksi"],
                                                                range=[[1, 0], [5, 4]]), legend=None),
            tooltip=[alt.Tooltip("observed_at:T", title="Waktu", format="%d/%m/%Y %H:%M"),
                     alt.Tooltip("seri:N", title="Seri"),
                     alt.Tooltip("pm25:Q", title="µg/m³", format=".1f"),
                     alt.Tooltip("bagian:N", title="Bagian data")],
        )
    )
    layers = [lines]

    # mark where the held-out test period starts, if it is inside the range
    test_start = df.loc[df["data_split"] == "testing", "observed_at"].min()
    if pd.notna(test_start) and test_start >= plot["observed_at"].min():
        rule = pd.DataFrame({"t": [test_start], "label": ["mulai data uji"]})
        layers.append(alt.Chart(rule).mark_rule(color="#6E6E69", strokeDash=[2, 3]).encode(x="t:T"))
        layers.append(alt.Chart(rule).mark_text(align="left", dx=4, dy=-6, fontSize=11, color="#6E6E69")
                      .encode(x="t:T", y=alt.value(8), text="label:N"))

    chart = (alt.layer(*layers).properties(height=320)
             .configure_view(stroke=None).configure(background="#FFFFFF"))
    st.altair_chart(chart, width="stretch")
    st.markdown(
        '<div class="mc-muted">Prediksi tiap jam dibuat hanya dari data jam-jam sebelumnya (lag PM2.5 dan cuaca). '
        'Akurasi yang adil hanya terlihat pada <b>data uji</b>; di data latih model sudah pernah melihat jawabannya.</div>',
        unsafe_allow_html=True,
    )

# ---------------------------------------------------------------- accuracy + AQI distribution
left, right = st.columns([1, 1], gap="medium")

with left, st.container(border=True, key="card_acc"):
    st.markdown("**Akurasi per bagian data**")
    acc = (df.groupby("data_split")
           .agg(jam=("observed_at", "size"), mae=("abs_error", "mean"))
           .reindex(["testing", "training", "gap_fill"]).dropna(how="all").reset_index())
    acc["Bagian"] = acc["data_split"].map(SPLIT_LABEL)
    acc["mae"] = acc["mae"].map(lambda v: fmt(v, 2) if pd.notna(v) else "—")  # gap_fill has no actual
    st.dataframe(
        acc[["Bagian", "jam", "mae"]].rename(columns={"jam": "Jam", "mae": "MAE (µg/m³)"}),
        hide_index=True, width="stretch",
        column_config={"Jam": st.column_config.NumberColumn(format="%d")},
    )
    st.markdown(
        '<div class="mc-muted"><b>Data uji</b>: jam yang tidak pernah dilihat model, ukuran akurasi yang sebenarnya.<br>'
        '<b>Data latih</b>: in-sample, kesalahannya terlihat lebih kecil dari kenyataan.<br>'
        '<b>Jam kosong</b>: tidak ada observasi, jadi hanya ada prediksi tanpa MAE.</div>',
        unsafe_allow_html=True,
    )

with right, st.container(border=True, key="card_aqi"):
    st.markdown(f"**Jam per kategori AQI** <span class='mc-muted'>· aktual, rentang {rng}</span>",
                unsafe_allow_html=True)
    actual = view.dropna(subset=["actual_pm2_5"])
    cats = actual["actual_pm2_5"].map(lambda v: aqi(v)[0])
    dist = pd.DataFrame({"kategori": [a[1] for a in AQI], "warna": [a[2] for a in AQI]})
    dist["jam"] = dist["kategori"].map(cats.value_counts()).fillna(0).astype(int)
    dist["persen"] = dist["jam"] / max(len(actual), 1)
    bars = (
        alt.Chart(dist)
        .mark_bar(cornerRadiusEnd=3, height=16)
        .encode(
            y=alt.Y("kategori:N", sort=[a[1] for a in AQI], title=None,
                    axis=alt.Axis(labelColor="#141414", labelLimit=180, domain=False, ticks=False)),
            x=alt.X("jam:Q", title=None, axis=alt.Axis(gridColor="#ECECE9", labelColor="#6E6E69", tickCount=4)),
            color=alt.Color("warna:N", scale=None),
            tooltip=[alt.Tooltip("kategori:N", title="Kategori"), alt.Tooltip("jam:Q", title="Jam"),
                     alt.Tooltip("persen:Q", title="Porsi", format=".0%")],
        )
        .properties(height=210)
        .configure_view(stroke=None).configure(background="#FFFFFF")
    )
    st.altair_chart(bars, width="stretch")
    worst = dist.loc[dist["kategori"].isin(["Tidak sehat", "Sangat tidak sehat", "Berbahaya"]), "jam"].sum()
    st.markdown(
        f'<div class="mc-muted">{worst} dari {len(actual)} jam ({worst / max(len(actual), 1):.0%}) '
        f'berada di kategori tidak sehat atau lebih buruk.</div>',
        unsafe_allow_html=True,
    )