-- Distributed Job Scheduler — Database Schema
-- Run this once against a fresh PostgreSQL database to set up all required tables.

-- ============================================================
-- jobs — the schedule definitions ("the recipe")
-- ============================================================
CREATE TABLE jobs (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    cron_expr   TEXT NOT NULL,
    status      TEXT DEFAULT 'active',           -- active | paused | deleted
    next_run_at TIMESTAMP DEFAULT NOW(),
    job_type    TEXT DEFAULT 'default',           -- short | long | default — determines which queue a job is routed to
    retry_count INT DEFAULT 0,                    -- how many times the current run has failed
    max_retries INT DEFAULT 3                     -- failures allowed before moving to the dead letter queue
);

CREATE INDEX idx_jobs_next_run ON jobs(next_run_at) WHERE status = 'active';

-- ============================================================
-- job_runs — execution history ("the cooking log")
-- One job can have many runs over its lifetime.
-- ============================================================
CREATE TABLE job_runs (
    id          SERIAL PRIMARY KEY,
    job_id      INT REFERENCES jobs(id),
    status      TEXT DEFAULT 'pending',           -- pending | success | failed
    finished_at TIMESTAMP,
    error_msg   TEXT,
    created_at  TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- Seed data (optional) — useful for local testing
-- ============================================================
-- INSERT INTO jobs (name, cron_expr, job_type) VALUES ('my first job', '* * * * *', 'short');
-- INSERT INTO jobs (name, cron_expr, job_type) VALUES ('clean old logs', '0 * * * *', 'long');
