# Keterbatasan Data & Pipeline

Dokumen ini mencatat keterbatasan yang sudah ditemukan pada dataset
`traffictab23` dan pipeline ETL-nya, supaya tidak perlu ditemukan ulang
tiap kali dan supaya jelas fitur dashboard mana yang aman dibangun di atas
data ini.

## 1. Kolom waktu mentah tidak reliabel

`time_of_day` dan `day_of_week` pada CSV sumber **tidak selalu cocok**
dengan hari/jam yang seharusnya berdasarkan `timestamp` (contoh: jam
00:00:30 berlabel `"Morning"`).

**Mitigasi:** tahap transform (`spark/jobs/transform.py`) menurunkan
`weekday_from_ts`/`obs_hour` langsung dari `timestamp`, bukan mempercayai
kolom mentah. Kolom mentah tetap disimpan apa adanya, dan dua flag
ditambahkan untuk transparansi: `tod_mismatch`, `dow_mismatch`.

**Skala masalah:** lihat `tod_mismatch_rate`/`dow_mismatch_rate` di
`meta.quality_results` (dihitung tiap run oleh `quality_checks.py`) untuk
angka terkini.

## 2. GPS tidak stabil per road_segment_id

`gps_latitude`/`gps_longitude` **bukan** lokasi tetap per
`road_segment_id` — segmen yang sama muncul di titik-titik yang tersebar
sampai kilometer-an jauhnya di baris yang berbeda.

**Dibuktikan lewat:** `notebooks/road_gps_stability.py` —
`radius_p90_m` (sebaran GPS per segmen, dikonversi ke meter) rata-rata
**~4.16 km**, minimum **~4.14 km**, di seluruh 101 `road_segment_id`
(100%). Bandingkan dengan panjang segmen jalan realistis yang cuma
ratusan meter — ini bukan noise kecil, GPS mentah tidak bisa dipakai
sebagai lokasi segmen sama sekali.

**Konsekuensi:** `meta.road_positions` **tidak** diisi dari GPS mentah
(`source = gps_median` ditolak) maupun dari assignment ke jalan OSM asli
(`source = osm_assignment` ditolak — dataset tidak punya nama jalan/area
referensi untuk dicocokkan). Diisi dengan `source = illustrative_grid`
(lihat `notebooks/populate_road_positions.py`): 101 segmen disebar merata
di grid buatan sekitar area Islamabad-Rawalpindi, **bukan lokasi asli**.

**Dampak ke dashboard:** peta yang menampilkan `road_segment_id` di posisi
dari `meta.road_positions` harus diberi label eksplisit sebagai ilustratif
(mis. "posisi indikatif, bukan lokasi jalan sebenarnya"), tidak boleh
ditampilkan seolah itu peta lokasi insiden yang akurat secara geografis.

## 3. Beberapa kolom numerik terlihat sintetis

Distribusi beberapa kolom (`jam_density_index`, `lane_occupancy_rate`,
koordinat GPS, beberapa kolom V2X) terlihat mencurigakan rata/uniform —
indikasi dataset ini disintesis, bukan hasil pengukuran sensor nyata.

**Dampak:** fitur dashboard yang mengasumsikan realisme statistik dari
kolom-kolom ini (mis. deteksi anomali berbasis distribusi, korelasi antar
V2X metric) perlu diperlakukan dengan hati-hati atau dihindari sampai
diverifikasi lebih lanjut lewat audit notebook (`notebooks/01_audit_data.ipynb`,
belum ditulis).

## 4. Threshold dashboard belum ditentukan dari data

`config/thresholds.yaml` (`v2x_status.packet_loss_pct`,
`message_delay_ms`) masih `null` — perlu diisi dari persentil (p50/p90)
`core.observations.v2x_packet_loss_rate`/`v2x_message_delay_avg` setelah
audit notebook selesai, bukan angka tebakan.

## Status per item

| Keterbatasan | Status |
|---|---|
| tod/dow mismatch | Ditangani (flag di `core.observations`) |
| GPS tidak stabil per segmen | Ditangani (grid ilustratif, ditandai jelas) |
| Kolom sintetis | Diketahui, belum diaudit penuh |
| Threshold V2X/alert | Belum diisi |

## 5. Hasil audit notebook (notebooks/01_audit_data.ipynb, 2026-09-21)

**Temporal consistency:** `dow_mismatch_rate = 0%` — day_of_week ternyata
SELALU cocok dengan turunan timestamp (klaim awal di bagian 1 bahwa
day_of_week juga bermasalah tidak terbukti; hanya time_of_day yang
bermasalah). `tod_mismatch_rate` tidak acak — berjenjang persis di batas
bucket jam (Night 90%, Morning 70%, Afternoon 60%, Evening 80%), dan
implied match rate-nya (10%+30%+40%+20%) = tepat 100%. Pola ini konsisten
dengan hipotesis: `time_of_day` mentah diisi acak dari distribusi tetap
(~10/30/40/20% Night/Morning/Afternoon/Evening), independen dari jam
sebenarnya — bukan cuma noise pengukuran.

**Label consistency:** `anomaly_label`/`incident_type` 100% konsisten
(0 dari 2.102.401 baris inkonsisten). `anomaly_rate` = 8.0%, sesuai
temuan awal.

**Distributional realism:** 6 dari 7 kolom yang dicurigai sintetis
terkonfirmasi mendekati distribusi uniform (KS-test, p-value tinggi):
`jam_density_index`, `lane_occupancy_rate`, `v2x_message_delay_avg`,
`gps_latitude`, `gps_longitude`, `v2v_beacon_interval_avg`. **Kecuali
`v2x_packet_loss_rate`** (KS-stat 0.51, jauh dari uniform) — satu-satunya
kolom numerik yang distribusinya terlihat seperti data nyata, bukan
sintetis acak.

**Signal correlation — temuan paling penting untuk scope dashboard:**
`anomaly_label` TIDAK berkorelasi dengan metrik lalu lintas manapun
(korelasi 0.0003–0.0025, praktis nol; rata-rata vehicle_count/speed/
hard_braking/rapid_accel/lane_occupancy nyaris identik antara baris
normal vs anomali). **Konsekuensi:** dashboard tidak boleh mengklaim
fitur "deteksi anomali" sebagai insight yang tervalidasi dari pola data
lalu lintas — `anomaly_label`/`incident_type` cuma boleh ditampilkan
sebagai catatan insiden yang tercatat (record), bukan hasil deteksi.

 | Keterbatasan | Status |
 |---|---|
 | tod/dow mismatch | Ditangani (flag di `core.observations`) |
 | GPS tidak stabil per segmen | Ditangani (grid ilustratif, ditandai jelas) |
-| Kolom sintetis | Diketahui, belum diaudit penuh |
+| Kolom sintetis | Diaudit — 6/7 kolom curiga terkonfirmasi uniform; v2x_packet_loss_rate terkecuali |
+| anomaly_label tanpa sinyal fisik | Dikonfirmasi — dashboard tidak boleh klaim "deteksi", hanya "catatan insiden" |
-| Threshold V2X/alert | Belum diisi |
+| Threshold V2X/alert | Terisi dari p50/p90 (lihat config/thresholds.yaml) |