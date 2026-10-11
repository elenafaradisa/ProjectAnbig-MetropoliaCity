## Pergeseran 5 jam pada kolom timestamp (ditemukan dan diperbaiki, Oktober 2026)

**Gejala.** Semua kolom bertipe `TIMESTAMP` yang ditulis Spark ke Postgres (`core.observations.observed_at`, `core.observations.window_15min`, dan turunannya `mart.road_window_stats.window_15min`) tersimpan 5 jam lebih awal dari waktu di file mentah. Baris mentah `2022-01-01 00:03:00` tersimpan sebagai `2021-12-31 19:03:00`. Kolom hasil hitungan di dalam Spark (`obs_hour`, `obs_date`) tetap benar, sehingga terjadi ketidaksesuaian: baris dengan `obs_hour = 0` memiliki `observed_at` berjam 19.

**Penyebab.** `spark/common/spark_session.py` mengatur `spark.sql.session.timeZone = Asia/Karachi`, tetapi driver JDBC Postgres mengonversi timestamp memakai zona waktu default JVM, yaitu UTC di dalam container. Spark membaca waktu mentah sebagai waktu Karachi (UTC+5), lalu JDBC menuliskannya sebagai UTC, sehingga hasilnya mundur 5 jam. Mart per jam (`road_hourly_stats`, `road_current_status`) kebetulan benar karena dibangun dari `obs_date` + `obs_hour`.

**Perbaikan kode.** JVM driver dan executor dipaksa memakai zona waktu yang sama dengan sesi Spark:

```python
.config("spark.driver.extraJavaOptions", "-Duser.timezone=Asia/Karachi")
.config("spark.executor.extraJavaOptions", "-Duser.timezone=Asia/Karachi")
```

Perbaikan ini direproduksi dan diuji dengan PySpark 3.5.1 dan Postgres: dengan konfigurasi lama, input `2022-01-01 00:03:00` tersimpan sebagai `2021-12-31 19:03:00`; dengan konfigurasi baru tersimpan `2022-01-01 00:03:00`.

**Koreksi data yang sudah ada.** Karena run ulang `etl_traffic` gagal akibat keterbatasan memori laptop (lihat bagian berikut), data yang sudah tersimpan dikoreksi langsung di Postgres dengan menambahkan 5 jam. Pakistan tidak memakai jam musim panas sejak 2009, sehingga selisihnya konstan untuk seluruh data 2022–2023. Sebelum koreksi, dipastikan bahwa untuk ke-2.102.401 baris, `observed_at + 5 jam` cocok dengan `obs_hour` dan `obs_date` (0 baris tidak cocok). Setelah koreksi: 0 baris tergeser, rentang data `2022-01-01 00:00` sampai `2024-01-01 00:00`. Run pipeline berikutnya akan menghasilkan data yang benar tanpa koreksi manual.

## Keterbatasan memori lingkungan pengembangan

Docker Desktop di laptop pengembangan hanya mendapat sekitar 3,7 GB memori (bawaan WSL: 50% RAM). Dalam keadaan diam, Airflow, Postgres, dan Kafka sudah memakai sekitar 1,1 GB, sementara Spark meminta heap 3 GB. Akibatnya, task `ingest` pernah dimatikan sistem karena kehabisan memori (`oom_kill`). Saat pipeline batch dijalankan, Kafka dan producer streaming sebaiknya dimatikan, dan `SPARK_DRIVER_MEMORY` diturunkan.

Batas memori bersama (`/dev/shm`) bawaan Docker sebesar 64 MB juga membuat `VACUUM` paralel gagal. Batas ini dinaikkan dengan `shm_size: 256mb` pada service `postgres`.

## Jam density index tidak berkorelasi dengan kecepatan

Pada `mart.road_hourly_stats`, korelasi Pearson antara `avg_jam_density_index` dan `avg_average_speed` adalah −0,002, dan antara `avg_jam_density_index` dan `avg_lane_occupancy_rate` adalah −0,001. Keduanya praktis nol. Ini mengindikasikan bahwa kolom-kolom pada dataset traffictab23 dibangkitkan secara independen (ciri data sintetis), bukan hasil pengukuran lalu lintas yang saling terkait.

Akibatnya, status "macet/padat/lancar" di dashboard ditentukan **hanya dari jam density index** (macet ≥ 75, padat ≥ 50, lancar < 50). Kolom ini dipilih karena merupakan satu-satunya kolom yang secara definisi dimaksudkan sebagai indikator kemacetan. Konsekuensinya, sebuah segmen dapat berstatus "macet" dengan kecepatan rata-rata tinggi (contoh: segmen 1087, jam density 93,3 pada 75,3 km/j). Aturan gabungan seperti "jam density tinggi dan kecepatan rendah" sengaja tidak dipakai, karena akan menjadi aturan karangan dari dua kolom yang tidak berhubungan. Dasar penentuan status dituliskan langsung di dashboard.

## Posisi segmen jalan di peta bersifat ilustratif

Data mentah hanya berisi `road_segment_id` berupa angka, tanpa nama jalan atau lokasi yang stabil per segmen. Untuk menampilkan peta, setiap segmen dipasangkan dengan satu ruas jalan nyata di Islamabad dari OpenStreetMap (`geo/assign_osm_roads.py`, hasilnya disimpan di `meta.road_positions` dengan `source = 'osm_assignment'`):

- Geometri jalannya nyata: jalan trunk/primary/secondary/tertiary dengan panjang minimal 500 m, diambil dari Overpass API.
- **Pasangan segmen ke jalan bersifat arbitrer**: ruas dipilih bergiliran antar nama jalan lalu diacak dengan seed tetap (42), sehingga 101 segmen tersebar ke 73 jalan bernama dan pasangannya selalu sama di setiap run.
- Respons Overpass disimpan di `data/geo/islamabad_roads_overpass.json`, sehingga pasangan tidak berubah meskipun data OSM diperbarui.

Peta karena itu menunjukkan **pola** status antar segmen di atas geografi kota yang nyata, bukan lokasi sebenarnya dari setiap segmen dalam dataset.

## "Kondisi terkini" tiap segmen berasal dari jam yang berbeda

`mart.road_current_status` berisi satu baris per segmen, diambil dari jam terakhir yang memiliki observasi untuk segmen tersebut. Jam terakhir itu berbeda antar segmen (31 Des 2023 20:00 sampai 1 Jan 2024 00:00). Dashboard menampilkan rentang ini, bukan satu jam tunggal.

Observasi per segmen juga jarang: tidak setiap jendela 15 menit memiliki data. Grafik kecepatan per segmen memakai sumbu waktu proporsional, sehingga jendela kosong terlihat sebagai jarak yang lebih lebar, dan jumlah jendela yang berisi data ditampilkan.

## Kategori AQI pada halaman polusi bersifat indikatif

Kategori kualitas udara memakai batas PM2.5 US EPA (revisi 2024): Baik ≤ 9,0; Sedang ≤ 35,4; Tidak sehat bagi kelompok sensitif ≤ 55,4; Tidak sehat ≤ 125,4; Sangat tidak sehat ≤ 225,4; Berbahaya > 225,4 µg/m³. EPA mendefinisikan batas ini untuk rata-rata 24 jam, sedangkan dashboard menerapkannya pada nilai per jam. Kategori yang ditampilkan karena itu bersifat indikatif, bukan status AQI resmi.