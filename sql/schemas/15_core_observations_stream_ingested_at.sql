-- Processing time for the streaming sink. observed_at is EVENT time (the
-- replayed 2022-2023 timestamp from traffictab23); ingested_at is when the row
-- actually reached Postgres, which is what the dashboard's live view needs
-- (events per minute, time since the last micro-batch).
--
-- stream_transform.py inserts with an explicit column list that does not
-- include ingested_at, so the default fills it per micro-batch. Rows that
-- already exist get the time this migration runs.
ALTER TABLE core.observations_stream
    ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS idx_observations_stream_ingested_at
    ON core.observations_stream (ingested_at);
