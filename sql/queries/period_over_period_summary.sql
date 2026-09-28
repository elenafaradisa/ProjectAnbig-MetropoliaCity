-- Ringkasan total insiden & rata-rata anomaly_rate untuk periode
-- terpilih, dibandingkan periode sebelumnya (panjang sama). Dipakai
-- oleh Kartu Ringkasan di halaman Operator ("+12% dari periode lalu").
--
-- Params:
--   :periode_mulai, :periode_akhir --  periode saat ini (inclusive)
--   :periode_sebelumnya_mulai, :periode_sebelumnya_akhir
--       -- dihitung di app: rentang sepanjang periode saat ini, mundur
--          persis sebelum :periode_mulai
WITH periode_ini AS (
    SELECT sum(incident_count) AS total_insiden, avg(anomaly_rate) AS avg_anomaly_rate
    FROM mart.road_hourly_stats
    WHERE obs_date BETWEEN :periode_mulai AND :periode_akhir
),
periode_lalu AS (
    SELECT sum(incident_count) AS total_insiden, avg(anomaly_rate) AS avg_anomaly_rate
    FROM mart.road_hourly_stats
    WHERE obs_date BETWEEN :periode_sebelumnya_mulai AND :periode_sebelumnya_akhir
)
SELECT
    periode_ini.total_insiden,
    periode_ini.avg_anomaly_rate,
    periode_lalu.total_insiden    AS total_insiden_lalu,
    periode_lalu.avg_anomaly_rate AS avg_anomaly_rate_lalu,
    CASE WHEN periode_lalu.total_insiden = 0 THEN NULL
         ELSE round(100.0 * (periode_ini.total_insiden - periode_lalu.total_insiden) / periode_lalu.total_insiden, 1)
    END AS pct_change_insiden,
    CASE WHEN periode_lalu.avg_anomaly_rate = 0 THEN NULL
         ELSE round(100.0 * (periode_ini.avg_anomaly_rate - periode_lalu.avg_anomaly_rate) / periode_lalu.avg_anomaly_rate, 1)
    END AS pct_change_anomaly
FROM periode_ini, periode_lalu;