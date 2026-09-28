-- Tabel terpisah untuk output pipeline streaming, supaya tidak
-- mencampuri core.observations (hasil batch yang sudah teraudit,
-- 2.102.401 baris terkonfirmasi). Struktur disalin persis dari
-- core.observations (kolom, tipe, index) lewat LIKE ... INCLUDING ALL,
-- supaya tidak perlu menulis ulang DDL secara manual dan berisiko beda.
CREATE TABLE IF NOT EXISTS core.observations_stream (
    LIKE core.observations INCLUDING ALL
);