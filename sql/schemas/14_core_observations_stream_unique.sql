-- One row per traffictab23 event in the streaming sink: observed_at is unique
-- per row in the source data (2,102,401 rows, 2,102,401 distinct timestamps),
-- so it is the idempotency key for spark/jobs/stream_transform.py, which
-- merges each micro-batch with INSERT ... ON CONFLICT (observed_at) DO NOTHING.
--
-- Existing rows must be de-duplicated (or the table emptied) before this
-- index can be created.
CREATE UNIQUE INDEX IF NOT EXISTS uq_observations_stream_observed_at
    ON core.observations_stream (observed_at);