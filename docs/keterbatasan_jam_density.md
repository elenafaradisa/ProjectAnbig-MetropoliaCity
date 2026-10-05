## Jam density index tidak berkorelasi dengan kecepatan

Pada `mart.road_hourly_stats`, korelasi Pearson antara `avg_jam_density_index` dan `avg_average_speed` adalah −0,002, dan antara `avg_jam_density_index` dan `avg_lane_occupancy_rate` adalah −0,001. Pemeriksaan per baris pada `core.observations` (2.102.401 baris) menunjukkan hal yang sama:

| Kelompok jam density | Baris | Rata-rata kecepatan | Kecepatan ≥ 60 km/j |
|---|---|---|---|
| Rendah (< 50) | 996.336 | 60,0 km/j | 50,1% |
| Sedang (50–75) | 553.196 | 60,0 km/j | 50,0% |
| Tinggi (≥ 75) | 552.869 | 60,0 km/j | 49,9% |

Ketiga kelompok memiliki rata-rata kecepatan yang identik, dan tepat separuh barisnya berkecepatan ≥ 60 km/j. Pada data lalu lintas nyata, segmen dengan kepadatan tinggi seharusnya jauh lebih lambat. Ini menunjukkan bahwa kolom-kolom traffictab23 dibangkitkan secara acak dan saling independen (ciri data sintetis). Sebarannya konsisten dengan distribusi seragam: jam density tersebar rata pada 0–100, dan kecepatan berpusat di 60 km/j.

Pipeline tidak mengubah kedua kolom ini: `transform_df()` meneruskannya apa adanya dari file mentah, dan pipeline batch serta streaming menghasilkan nilai yang identik.

**Konsekuensi pada dashboard.** Status segmen ditentukan dari `jam_density_index`, satu-satunya kolom yang secara definisi dimaksudkan sebagai indikator kepadatan, dengan batas tinggi ≥ 75, sedang ≥ 50, rendah < 50. Labelnya sengaja ditulis **"Kepadatan tinggi / sedang / rendah"**, bukan "macet / padat / lancar", karena label "macet" menjanjikan lalu lintas yang lambat, sedangkan data tidak menunjukkan hubungan itu. Sebuah segmen dapat berkepadatan tinggi dengan kecepatan rata-rata tinggi (contoh: segmen 1087, jam density 93,3 pada 75,3 km/j). Aturan gabungan seperti "kepadatan tinggi dan kecepatan rendah" sengaja tidak dipakai, karena hanya akan menyaring kebetulan acak dari dua kolom yang tidak berhubungan.
