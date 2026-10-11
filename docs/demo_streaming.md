# Demo streaming: ketahanan saat streaming job mati

Tujuan demo: menunjukkan bahwa pipeline Kafka → Spark Structured Streaming → PostgreSQL
tidak kehilangan data dan tidak menulis data dobel, walaupun streaming job dimatikan paksa
di tengah aliran data.

Tiga mekanisme yang didemokan:

| Mekanisme | Di mana | Yang dibuktikan |
|---|---|---|
| Kafka menyimpan pesan (durable log) | topic `traffic_topic` | pesan yang dikirim saat job mati tidak hilang |
| Checkpoint offset + state | `data/checkpoints/traffic_stream`, `..._hourly` | job melanjutkan dari offset terakhir, bukan dari awal atau dari akhir |
| Sink idempoten | `INSERT … ON CONFLICT (observed_at) DO NOTHING`, indeks unik | batch yang diproses ulang setelah crash tidak menjadi dobel |

## Persiapan (sebelum presentasi)

1. Pastikan container berjalan: `postgres`, `kafka`, `airflow-scheduler`.
   Jangan menjalankan DAG batch selama demo (memori Docker terbatas).
2. Jalankan Streamlit di host: `streamlit run dashboard/app.py`.
   Buka dua tab: **Streaming langsung** dan **Kualitas data → Streaming (live)**.
3. Siapkan tiga terminal PowerShell di folder project:
   - **T1**: streaming job
   - **T2**: producer
   - **T3**: pengecekan database

## Langkah demo

### 1. Jalankan streaming job (T1)

```powershell
docker compose exec airflow-scheduler bash -c "cd /opt/project && python -m spark.jobs.stream_transform --config /opt/project/config/incident_types.yaml --topic traffic_topic --bootstrap-servers kafka:9092 --checkpoint-dir /opt/project/data/checkpoints/traffic_stream --starting-offsets earliest"
```

Tunggu sampai muncul `Streaming queries started (raw id=…, hourly id=…)`.

### 2. Catat kondisi awal (T3)

```powershell
docker compose exec postgres psql -U metropolia -d metropolia -tA -c "SELECT count(*) FROM core.observations_stream"
```

Catat angkanya sebagai **N_awal**.

### 3. Jalankan producer (T2)

```powershell
powershell -ExecutionPolicy Bypass -File .\demo_producer.ps1 -Limit 180
```

Producer mengirim 180 event, 1 per detik (3 menit). Di halaman **Streaming langsung**,
status berubah menjadi **Mengalir** dan grafik per menit naik.

### 4. Matikan streaming job secara paksa (sekitar 1 menit setelah producer mulai)

Di **T3**:

```powershell
docker compose restart airflow-scheduler
```

Ini mematikan streaming job tanpa kesempatan menutup dengan rapi, seperti crash sungguhan.
T1 kembali ke prompt. **Producer di T2 tetap berjalan**: pesan terus masuk ke Kafka.

Yang terlihat di dashboard:
- **Streaming langsung**: status berubah menjadi **Melambat** lalu **Berhenti**; grafik per menit turun ke 0.
- Ini membuktikan dashboard benar-benar membaca database, bukan angka palsu.

### 5. Biarkan job mati sekitar 1 menit, lalu jalankan lagi (T1)

Jalankan perintah yang sama seperti langkah 1.

Yang terlihat di log T1:
- batch pertama berukuran besar, misalnya `[batch 31] 64 row(s) received, 64 new, 0 already in …`:
  semua event yang menumpuk di Kafka selama job mati diproses sekaligus;
- kalau crash terjadi di tengah penulisan sebuah batch, batch itu diproses ulang dan lognya
  menunjukkan `… already in core.observations_stream (skipped)`: inilah sink idempoten bekerja.

Di dashboard, status kembali **Mengalir** dan grafik per menit menunjukkan lonjakan saat
tumpukan pesan diproses.

### 6. Buktikan tidak ada data yang hilang atau dobel (T3, setelah producer selesai)

Tunggu sampai T2 menampilkan `Selesai mengirim semua baris.` dan satu atau dua batch berikutnya, lalu:

```powershell
docker compose exec postgres psql -U metropolia -d metropolia -P pager=off -c "SELECT count(*) AS total, count(DISTINCT observed_at) AS unik, count(*) FILTER (WHERE obs_hour <> extract(hour FROM observed_at)) AS jam_tergeser FROM core.observations_stream" -c "SELECT sum(n_observations) AS event_di_agregasi FROM mart.road_hourly_stream"
```

Hasil yang benar:

| Pengecekan | Nilai yang diharapkan | Artinya |
|---|---|---|
| `total` | **N_awal + 180** | tidak ada event yang hilang selama job mati |
| `unik` | sama dengan `total` | tidak ada event dobel |
| `jam_tergeser` | 0 | timestamp konsisten |
| `event_di_agregasi` | sama dengan `total` | state agregasi per jam pulih dari checkpoint |

Tab **Kualitas data → Streaming (live)** menampilkan hal yang sama: keempat angka 0 dan
kotak hijau "event streaming bersih".

## Penjelasan singkat untuk presentasi

- **Kafka** adalah log yang tahan lama: pesan tetap tersimpan di topic walaupun tidak ada
  consumer. Producer tidak perlu tahu bahwa streaming job sedang mati.
- **Checkpoint** Spark menyimpan offset Kafka yang sudah diproses dan state agregasi per jam.
  Saat dijalankan ulang, job melanjutkan dari offset terakhir yang tercatat, sehingga tidak ada
  pesan yang terlewat.
- **At-least-once + idempoten = efektif exactly-once.** Penulisan `foreachBatch` ke PostgreSQL
  bersifat *at-least-once*: batch yang sedang ditulis saat crash akan diproses ulang. Karena
  penulisan memakai `ON CONFLICT DO NOTHING` (data mentah) dan `ON CONFLICT DO UPDATE` dengan
  total terbaru (agregasi per jam), memproses ulang batch yang sama tidak mengubah hasil akhir.

## Kalau ada yang tidak sesuai

- **`total` kurang dari N_awal + 180**: cek apakah producer benar-benar selesai
  (`Selesai mengirim semua baris.`) dan apakah streaming job sudah memproses batch terakhir.
- **`unik` lebih kecil dari `total`**: indeks unik hilang atau versi lama `stream_transform.py`
  yang berjalan. Cek dengan
  `docker compose exec airflow-scheduler grep -c "ON CONFLICT" /opt/project/spark/jobs/stream_transform.py`.
- **`jam_tergeser` lebih dari 0**: ada streaming job versi lama yang masih hidup
  (lihat `docs/keterbatasan.md`, bagian pergeseran 5 jam).
