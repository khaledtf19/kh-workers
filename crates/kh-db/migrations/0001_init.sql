CREATE TABLE IF NOT EXISTS jobs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    kind TEXT NOT NULL,
    payload JSONB NOT NULL,
    attempts INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 3,
    run_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    claimed_at TIMESTAMPTZ,
    heartbeat_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    timeout INTERVAL NOT NULL DEFAULT INTERVAL '5 minutes',
    worker_id TEXT,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Claim path: pending jobs ordered by readiness.
CREATE INDEX IF NOT EXISTS idx_jobs_poll
ON jobs (run_at, created_at)
WHERE status = 'pending';

-- Reaper path: running jobs ordered by staleness.
CREATE INDEX IF NOT EXISTS idx_jobs_reaper
ON jobs (heartbeat_at)
WHERE status = 'running';
