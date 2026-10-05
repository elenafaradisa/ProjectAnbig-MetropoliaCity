"""Kualitas data: the quality checks every pipeline run writes to
meta.quality_results (traffic: spark/jobs/quality_checks.py, pollution:
pollution/quality_checks.py), judged automatically, plus live integrity
checks on the streaming sink.

Judgement rules (kept here, next to the explanations shown to the reader):
  - checks that must be 0 (out-of-range, duplicates, mismatches) -> Lolos/Gagal
  - known audit findings that are expected and handled -> Temuan
  - everything else (counts, means) -> Info, row counts compared to the
    previous run of the same pipeline
"""
import pandas as pd
import streamlit as st

from dashboard import db
from dashboard.theme import fmt, pill, tgl

STATUS_STYLE = {
    "Lolos": ("#EAF4EE", "#2E6B4A"),
    "Gagal": ("#FBE9E7", "#A3241D"),
    "Temuan": ("#FDF1E1", "#8A5208"),
    "Info": ("#F2F2F0", "#4A4A46"),
}
ZERO_PREFIXES = ("out_of_range.", "duplicate_", "invalid_")
ZERO_NAMES = {"source_file_mismatch", "anomaly_incident_type_mismatch", "dow_mismatch_rate"}
FINDINGS = {
    "tod_mismatch_rate": "Kolom time_of_day mentah tidak cocok dengan jam pada timestamp. Karena itu pipeline "
                         "menghitung ulang jam dari observed_at (obs_hour) dan tidak memakai time_of_day.",
    "missing_hourly_gaps": "Jam tanpa data di sumber polusi. Lag fitur model menjadi kosong di jam itu; "
                           "prediksinya diberi label gap_fill dan tidak dihitung dalam MAE.",
}
DESCRIPTION = {
    "row_count": "Jumlah baris yang dimuat",
    "row_count.training": "Baris data latih",
    "row_count.testing": "Baris data uji (tidak tumpang-tindih dengan data latih)",
    "distinct_road_segment_id": "Jumlah segmen jalan unik",
    "duplicate_observed_at_road": "Baris dobel (observed_at, segmen)",
    "duplicate_city_observed_at": "Baris dobel (kota, jam)",
    "anomaly_incident_type_mismatch": "anomaly_label tidak sejalan dengan incident_type",
    "dow_mismatch_rate": "Porsi day_of_week mentah yang tidak cocok dengan tanggal",
    "tod_mismatch_rate": "Porsi time_of_day mentah yang tidak cocok dengan jam",
    "anomaly_rate": "Porsi observasi beranomali",
    "invalid_data_split_values": "Nilai data_split selain training/testing",
    "source_file_mismatch": "Baris dengan file sumber yang tidak sesuai data_split-nya",
    "missing_hourly_gaps": "Jam yang kosong dalam rentang data",
    "components_pm2_5_mean.training": "Rata-rata PM2.5 data latih",
    "components_pm2_5_mean.testing": "Rata-rata PM2.5 data uji",
}


def judge(name: str, value: float) -> str:
    if name in FINDINGS:
        return "Temuan"
    if name.startswith(ZERO_PREFIXES) or name in ZERO_NAMES:
        return "Lolos" if value == 0 else "Gagal"
    return "Info"


def describe(name: str, detail) -> str:
    if name.startswith("out_of_range."):
        col = name.split(".", 1)[1]
        return f"Nilai {col} di luar rentang wajar" + (f" ({detail})" if detail else "")
    return DESCRIPTION.get(name) or (detail or "")


def show_value(name: str, v: float) -> str:
    if name.endswith("_rate"):
        return f"{fmt(v * 100, 2)}%"
    if float(v).is_integer():
        return f"{int(v):,}".replace(",", ".")
    return fmt(v, 2)


st.markdown('<div class="mc-crumb">Sistem / Kualitas data</div>', unsafe_allow_html=True)
st.title("Kualitas data")
st.caption("Hasil quality check yang ditulis setiap run pipeline ke meta.quality_results, dinilai otomatis, "
           "ditambah pengecekan langsung atas tabel streaming.")

qr = db.quality_results()
if qr.empty:
    st.warning("meta.quality_results masih kosong. Jalankan DAG etl_traffic atau etl_pollution.")
    st.stop()

qr["pipeline"] = qr["run_id"].map(lambda r: "Polusi udara" if str(r).startswith("etl_pollution") else "Lalu lintas")
qr["run_ts"] = pd.to_datetime(qr["run_ts"], utc=True).dt.tz_convert("Asia/Jakarta")
qr["status"] = [judge(n, v) for n, v in zip(qr["metric_name"], qr["value"])]

tab_traffic, tab_pollution, tab_stream = st.tabs(["Lalu lintas (batch)", "Polusi udara", "Streaming (live)"])


def pipeline_tab(name: str):
    sub = qr[qr["pipeline"] == name]
    if sub.empty:
        st.info(f"Belum ada run untuk pipeline {name.lower()}.")
        return
    runs = (sub.groupby("run_id")
            .agg(waktu=("run_ts", "max"), cek=("metric_name", "size"),
                 gagal=("status", lambda s: int((s == "Gagal").sum())))
            .sort_values("waktu", ascending=False).reset_index())
    labels = {r.run_id: f"{r.waktu:%d/%m/%Y %H:%M} WIB · {r.run_id}" for r in runs.itertuples()}
    run_id = st.selectbox("Run", list(labels), format_func=labels.get, key=f"run_{name}")
    cur = sub[sub["run_id"] == run_id].drop_duplicates("metric_name", keep="last")

    # compare row counts with the previous run of the same pipeline
    older = runs[runs["waktu"] < runs.loc[runs["run_id"] == run_id, "waktu"].iloc[0]]
    prev = sub[sub["run_id"] == older.iloc[0]["run_id"]] if not older.empty else None

    n = cur["status"].value_counts()
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Lolos", int(n.get("Lolos", 0)))
    k2.metric("Gagal", int(n.get("Gagal", 0)))
    k3.metric("Temuan audit", int(n.get("Temuan", 0)),
              help="Masalah data mentah yang sudah diketahui dan sudah ditangani pipeline.")
    rc = cur.loc[cur["metric_name"] == "row_count", "value"]
    if not rc.empty:
        delta = None
        if prev is not None:
            prc = prev.loc[prev["metric_name"] == "row_count", "value"]
            if not prc.empty:
                d = int(rc.iloc[0] - prc.iloc[0])
                delta = "sama dengan run sebelumnya" if d == 0 else f"{d:+,} dari run sebelumnya".replace(",", ".")
        k4.metric("Baris dimuat", show_value("row_count", rc.iloc[0]),
                  help=(delta.capitalize() + ".") if delta else "Tidak ada run sebelumnya untuk dibandingkan.")

    failed = cur[cur["status"] == "Gagal"]
    if failed.empty:
        st.success(f"Semua pengecekan yang harus bernilai 0 lolos pada run ini ({tgl(cur['run_ts'].max())} WIB).")
    else:
        st.error(f"{len(failed)} pengecekan gagal: " + ", ".join(failed["metric_name"]))

    order = {"Gagal": 0, "Temuan": 1, "Lolos": 2, "Info": 3}
    cur = cur.assign(_o=cur["status"].map(order)).sort_values(["_o", "category", "metric_name"])
    table = pd.DataFrame({
        "Status": cur["status"],
        "Kategori": cur["category"],
        "Pengecekan": cur["metric_name"],
        "Nilai": [show_value(m, v) for m, v in zip(cur["metric_name"], cur["value"])],
        "Keterangan": [describe(m, d) for m, d in zip(cur["metric_name"], cur["detail"])],
    })
    st.dataframe(table, hide_index=True, width="stretch", height=36 * (len(table) + 1) + 4,  # all rows, no scroll
                 column_config={"Keterangan": st.column_config.TextColumn(width="large")})

    found = cur[cur["status"] == "Temuan"]
    for row in found.itertuples():
        bg, fg = STATUS_STYLE["Temuan"]
        st.markdown(
            f'<div class="mc-muted" style="margin:6px 0">{pill("Temuan", bg, fg)} <b>{row.metric_name}</b> = '
            f'{show_value(row.metric_name, row.value)} · {FINDINGS[row.metric_name]}</div>',
            unsafe_allow_html=True,
        )

    with st.expander(f"Riwayat run ({len(runs)})"):
        st.dataframe(
            runs.assign(waktu=runs["waktu"].dt.strftime("%d/%m/%Y %H:%M")).rename(
                columns={"run_id": "Run", "waktu": "Waktu (WIB)", "cek": "Pengecekan", "gagal": "Gagal"}),
            hide_index=True, width="stretch",
        )


with tab_traffic:
    pipeline_tab("Lalu lintas")

with tab_pollution:
    pipeline_tab("Polusi udara")

with tab_stream:
    @st.fragment(run_every=15)
    def stream_checks():
        q = db.stream_quality()
        total = int(q["total"] or 0)
        if total == 0:
            st.info("Tabel streaming masih kosong.")
            return
        checks = [
            ("Baris dobel", total - int(q["unik"]), "Indeks unik pada observed_at + sink idempoten (ON CONFLICT)."),
            ("Jam tergeser", int(q["jam_tidak_cocok"]),
             "obs_hour harus sama dengan jam observed_at; tidak sama berarti masalah zona waktu muncul lagi."),
            ("Tidak ada di batch", int(q["tidak_ada_di_batch"]),
             "Setiap event replay harus ada juga di core.observations (sumber file yang sama)."),
            ("Selisih agregasi", total - int(q["event_di_agregasi"]),
             "Jumlah event di mart.road_hourly_stream harus sama dengan event mentah. Selisih kecil sesaat "
             "wajar karena kedua query streaming menulis di micro-batch masing-masing."),
        ]
        k = st.columns(4)
        for col, (label, val, help_) in zip(k, checks):
            col.metric(label, f"{val:,}".replace(",", "."), help=help_)
        gap = checks[3][1]
        bad = [label for label, val, _ in checks[:3] if val != 0]
        if gap < 0:  # more events in the aggregate than raw rows: counted twice
            bad.append("Selisih agregasi (agregasi menghitung event lebih dari sekali)")
        n_fmt = f"{total:,}".replace(",", ".")
        if bad:
            st.error("Perlu diperiksa: " + ", ".join(bad))
        elif gap > 0:
            st.info(f"{n_fmt} event terakhir bersih: tanpa dobel, tanpa pergeseran jam, semuanya ada di data batch. "
                    f"Agregasi per jam masih tertinggal {gap} event; biasanya tersusul di micro-batch berikutnya. "
                    "Jika angkanya tidak berkurang setelah satu menit, periksa streaming job.")
        else:
            st.success(f"{n_fmt} event terakhir bersih: tanpa dobel, tanpa pergeseran jam, "
                       "semuanya ada di data batch, dan agregasi per jamnya lengkap.")
        st.caption(f"Diperiksa langsung dari database tiap 15 detik atas {q['hours']} jam waktu event terakhir "
                   f"({tgl(q['dari'])} – {tgl(q['sampai'])}), jam yang sudah lengkap saja; "
                   "tidak disimpan sebagai riwayat.")

    stream_checks()