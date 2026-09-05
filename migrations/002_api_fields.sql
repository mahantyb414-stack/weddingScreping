-- migrations/002_api_fields.sql
-- Safe to run on existing database — uses IF NOT EXISTS / ADD COLUMN IF NOT EXISTS.
-- Run: psql "$DATABASE_URL" -f migrations/002_api_fields.sql

ALTER TABLE scrape_jobs
    ADD COLUMN IF NOT EXISTS last_heartbeat TIMESTAMP,
    ADD COLUMN IF NOT EXISTS error_message  TEXT;

ALTER TABLE scrape_location_jobs
    ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMP;

-- Index to speed up "find running job" query used by the API
CREATE INDEX IF NOT EXISTS idx_scrape_jobs_status ON scrape_jobs(status);
