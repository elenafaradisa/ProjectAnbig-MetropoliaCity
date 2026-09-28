-- Daftar insiden individual (row-level), untuk panel "Daftar Insiden
-- Terbaru" di halaman Operator. Query langsung ke core.observations
-- karena butuh detail per baris, bukan agregat dari mart.
--
-- Params:
--   :periode_mulai, :periode_akhir -- rentang observed_at (inclusive)
--   :road_segment_id               -- NULL = semua ruas
--   :incident_type                 -- NULL = semua jenis (selain 0/normal)
--   :weather_condition              -- NULL = semua cuaca
--   :limit                          -- jumlah baris, default 20
SELECT
    observed_at,
    incident_type,
    incident_name,
    road_segment_id,
    weather_condition,
    road_surface_status,
    average_speed,
    v2x_message_delay_avg
FROM core.observations
WHERE observed_at BETWEEN :periode_mulai AND :periode_akhir
  AND incident_type > 0
  AND (:road_segment_id IS NULL OR road_segment_id = :road_segment_id)
  AND (:incident_type IS NULL OR incident_type = :incident_type)
  AND (:weather_condition IS NULL OR weather_condition = :weather_condition)
ORDER BY observed_at DESC
LIMIT :limit;