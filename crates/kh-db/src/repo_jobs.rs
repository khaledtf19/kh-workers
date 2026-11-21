use chrono::{DateTime, Duration, Utc};
use kh_core::{Job, JobId, NewJob};
use sqlx::PgPool;
use sqlx::postgres::types::PgInterval;

use crate::error::DbError;
use crate::listener::JOBS_READY_CHANNEL;

/// PostgreSQL-backed job repository.
#[derive(Debug, Clone)]
pub struct PgJobs {
    pool: PgPool,
}

impl PgJobs {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Insert a new job and notify listeners once the insert commits.
    pub async fn enqueue(&self, job: &NewJob) -> Result<Job, DbError> {
        let mut tx = self.pool.begin().await?;

        let row = sqlx::query_as::<_, JobRow>(
            r#"
            INSERT INTO jobs (kind, payload, max_attempts, run_at, timeout)
            VALUES ($1, $2, $3, COALESCE($4, now()), $5)
            RETURNING *
            "#,
        )
        .bind(&job.kind)
        .bind(&job.payload)
        .bind(job.max_attempts)
        .bind(job.run_at)
        .bind(job.timeout)
        .fetch_one(&mut *tx)
        .await?;

        sqlx::query("SELECT pg_notify($1, '')")
            .bind(JOBS_READY_CHANNEL)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;

        Job::try_from(row)
    }

    /// Claim up to `limit` due jobs for `worker_id`.
    ///
    /// The claim is the first statement of an explicit transaction:
    ///
    /// 1. `SELECT ... FOR UPDATE SKIP LOCKED` locks a batch of due jobs. Any
    ///    row another worker already holds is skipped instead of blocking.
    /// 2. The locked rows are marked `running` in the same transaction, so no
    ///    other worker can take them before the commit.
    pub async fn claim(&self, worker_id: &str, limit: i64) -> Result<Vec<Job>, DbError> {
        if limit <= 0 {
            return Ok(Vec::new());
        }

        let mut tx = self.pool.begin().await?;

        let ids = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT id
            FROM jobs
            WHERE status = 'pending'
              AND run_at <= now()
            ORDER BY run_at, created_at
            FOR UPDATE SKIP LOCKED
            LIMIT $1
            "#,
        )
        .bind(limit)
        .fetch_all(&mut *tx)
        .await?;

        if ids.is_empty() {
            tx.rollback().await?;
            return Ok(Vec::new());
        }

        let rows = sqlx::query_as::<_, JobRow>(
            r#"
            UPDATE jobs
            SET status = 'running',
                worker_id = $1,
                claimed_at = now(),
                heartbeat_at = now(),
                attempts = attempts + 1
            WHERE id = ANY($2)
            RETURNING *
            "#,
        )
        .bind(worker_id)
        .bind(ids)
        .fetch_all(&mut *tx)
        .await?;

        tx.commit().await?;

        tracing::debug!(worker_id, claimed = rows.len(), "claimed jobs");

        rows.into_iter()
            .map(Job::try_from)
            .collect::<Result<Vec<_>, _>>()
    }

    /// Extend the lease of a running job owned by `worker_id`.
    ///
    /// Returns `false` if the worker no longer owns the job.
    pub async fn heartbeat(&self, id: JobId, worker_id: &str) -> Result<bool, DbError> {
        let result = sqlx::query(
            r#"
            UPDATE jobs
            SET heartbeat_at = now()
            WHERE id = $1
              AND status = 'running'
              AND worker_id = $2
            "#,
        )
        .bind(id.0)
        .bind(worker_id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    /// Mark a running job as completed.
    ///
    /// Returns `false` if the worker no longer owns the job.
    pub async fn complete(&self, id: JobId, worker_id: &str) -> Result<bool, DbError> {
        let result = sqlx::query(
            r#"
            UPDATE jobs
            SET status = 'completed',
                completed_at = now(),
                last_error = NULL
            WHERE id = $1
              AND status = 'running'
              AND worker_id = $2
            "#,
        )
        .bind(id.0)
        .bind(worker_id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    /// Return a running job to `pending`, scheduled at `run_at`.
    ///
    /// Returns `false` if the worker no longer owns the job.
    pub async fn retry(
        &self,
        id: JobId,
        worker_id: &str,
        run_at: DateTime<Utc>,
        error: &str,
    ) -> Result<bool, DbError> {
        let result = sqlx::query(
            r#"
            UPDATE jobs
            SET status = 'pending',
                run_at = $3,
                worker_id = NULL,
                claimed_at = NULL,
                heartbeat_at = NULL,
                last_error = $4
            WHERE id = $1
              AND status = 'running'
              AND worker_id = $2
            "#,
        )
        .bind(id.0)
        .bind(worker_id)
        .bind(run_at)
        .bind(error)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    /// Mark a running job as permanently failed.
    ///
    /// Returns `false` if the worker no longer owns the job.
    pub async fn fail(&self, id: JobId, worker_id: &str, error: &str) -> Result<bool, DbError> {
        let result = sqlx::query(
            r#"
            UPDATE jobs
            SET status = 'failed',
                completed_at = now(),
                last_error = $3
            WHERE id = $1
              AND status = 'running'
              AND worker_id = $2
            "#,
        )
        .bind(id.0)
        .bind(worker_id)
        .bind(error)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    /// Requeue or fail running jobs whose heartbeat is older than
    /// `stale_after`. Exhausted jobs move to `failed`; the rest go back to
    /// `pending`.
    pub async fn reap(&self, stale_after: Duration) -> Result<ReapStats, DbError> {
        let stats = sqlx::query_as::<_, ReapStats>(
            r#"
            WITH reaped AS (
                UPDATE jobs
                SET status = CASE
                        WHEN attempts >= max_attempts THEN 'failed'
                        ELSE 'pending'
                    END,
                    run_at = CASE
                        WHEN attempts >= max_attempts THEN run_at
                        ELSE now()
                    END,
                    completed_at = CASE
                        WHEN attempts >= max_attempts THEN now()
                        ELSE NULL
                    END,
                    worker_id = NULL,
                    claimed_at = NULL,
                    heartbeat_at = NULL,
                    last_error = 'worker heartbeat expired'
                WHERE status = 'running'
                  AND COALESCE(heartbeat_at, claimed_at) < now() - $1::interval
                RETURNING status
            )
            SELECT
                count(*) FILTER (WHERE status = 'pending') AS requeued,
                count(*) FILTER (WHERE status = 'failed') AS failed
            FROM reaped
            "#,
        )
        .bind(stale_after)
        .fetch_one(&self.pool)
        .await?;

        Ok(stats)
    }

    /// Fetch a single job by id.
    pub async fn get(&self, id: JobId) -> Result<Option<Job>, DbError> {
        let row = sqlx::query_as::<_, JobRow>("SELECT * FROM jobs WHERE id = $1")
            .bind(id.0)
            .fetch_optional(&self.pool)
            .await?;

        match row {
            Some(row) => Ok(Some(Job::try_from(row)?)),
            None => Ok(None),
        }
    }

    /// Count jobs by status.
    pub async fn stats(&self) -> Result<JobStats, DbError> {
        let stats = sqlx::query_as::<_, JobStats>(
            r#"
            SELECT
                count(*) FILTER (WHERE status = 'pending') AS pending,
                count(*) FILTER (WHERE status = 'running') AS running,
                count(*) FILTER (WHERE status = 'completed') AS completed,
                count(*) FILTER (WHERE status = 'failed') AS failed
            FROM jobs
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(stats)
    }
}

/// Counts returned by [`PgJobs::reap`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, sqlx::FromRow)]
pub struct ReapStats {
    pub requeued: i64,
    pub failed: i64,
}

/// Current job counts by status.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, sqlx::FromRow)]
pub struct JobStats {
    pub pending: i64,
    pub running: i64,
    pub completed: i64,
    pub failed: i64,
}

#[derive(sqlx::FromRow)]
struct JobRow {
    id: i64,
    status: String,
    kind: String,
    payload: serde_json::Value,
    attempts: i32,
    max_attempts: i32,
    run_at: DateTime<Utc>,
    claimed_at: Option<DateTime<Utc>>,
    heartbeat_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
    timeout: PgInterval,
    worker_id: Option<String>,
    last_error: Option<String>,
    created_at: DateTime<Utc>,
}

impl TryFrom<JobRow> for Job {
    type Error = DbError;

    fn try_from(row: JobRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: JobId(row.id),
            status: row.status.parse()?,
            kind: row.kind,
            payload: row.payload,
            attempts: row.attempts,
            max_attempts: row.max_attempts,
            run_at: row.run_at,
            claimed_at: row.claimed_at,
            heartbeat_at: row.heartbeat_at,
            completed_at: row.completed_at,
            timeout: interval_to_duration(row.timeout)?,
            worker_id: row.worker_id,
            last_error: row.last_error,
            created_at: row.created_at,
        })
    }
}

/// Convert a Postgres `INTERVAL` into a duration.
///
/// Job timeouts are stored as plain intervals (no months), so a month
/// component means the row was written by something we do not understand.
fn interval_to_duration(interval: PgInterval) -> Result<Duration, DbError> {
    if interval.months != 0 {
        return Err(DbError::UnsupportedInterval(format!(
            "{} months cannot be represented as a duration",
            interval.months
        )));
    }

    Duration::try_days(i64::from(interval.days))
        .and_then(|duration| duration.checked_add(&Duration::microseconds(interval.microseconds)))
        .ok_or_else(|| DbError::UnsupportedInterval("interval out of range".to_owned()))
}
