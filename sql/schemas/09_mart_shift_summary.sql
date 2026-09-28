-- Ringkasan per shift (4 blok waktu), dari mart.hour_of_week_city_summary.
-- View, bukan tabel — otomatis ikut ter-update saat city_summary di-refresh,
-- tidak perlu build script terpisah.
CREATE OR REPLACE VIEW mart.shift_summary AS
SELECT
    CASE
        WHEN obs_hour >= 5  AND obs_hour < 10 THEN 'Pagi (05-10)'
        WHEN obs_hour >= 10 AND obs_hour < 15 THEN 'Siang (10-15)'
        WHEN obs_hour >= 15 AND obs_hour < 20 THEN 'Sore (15-20)'
        ELSE 'Malam (20-05)'
    END AS shift_name,
    avg(avg_incident_count) AS avg_incident_per_hour,
    avg(avg_anomaly_rate)   AS avg_anomaly_rate
FROM mart.hour_of_week_city_summary
GROUP BY 1;