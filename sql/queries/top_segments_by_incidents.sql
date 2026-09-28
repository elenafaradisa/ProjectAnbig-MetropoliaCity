-- Top N ruas dengan insiden terbanyak pada rentang tanggal tertentu.
-- Dipakai oleh halaman Operator (Top 5 Ruas Insiden Terbanyak) dan
-- Perencana. Bukan build script — ini template query yang dipanggil
-- langsung oleh layer aplikasi (Streamlit) dengan parameter diisi.
--
-- Params:
--   :tanggal_mulai, :tanggal_akhir -- rentang obs_date (inclusive)
--   :limit                        -- jumlah ruas teratas, default 5
SELECT road_segment_id, sum(incident_count) AS total_insiden
FROM mart.road_hourly_stats
WHERE obs_date BETWEEN :tanggal_mulai AND :tanggal_akhir
GROUP BY road_segment_id
ORDER BY total_insiden DESC
LIMIT :limit;