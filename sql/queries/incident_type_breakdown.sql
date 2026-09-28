-- Breakdown jumlah insiden per incident_type, untuk Kartu Ringkasan
-- di halaman Operator. Query langsung ke core.observations karena
-- mart.road_hourly_stats.incident_count tidak dipecah per jenis.
--
-- Params:
--   :periode_mulai, :periode_akhir -- rentang observed_at (inclusive)
--   :road_segment_id               -- NULL = semua ruas
SELECT
    incident_type,
    incident_name,
    count(*) AS jumlah
FROM core.observations
WHERE observed_at BETWEEN :periode_mulai AND :periode_akhir
  AND incident_type > 0
  AND (:road_segment_id IS NULL OR road_segment_id = :road_segment_id)
GROUP BY incident_type, incident_name
ORDER BY incident_type;