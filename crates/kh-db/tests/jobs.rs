use std::collections::HashSet;

use chrono::{Duration, Utc};
use kh_core::{Job, JobId, JobStatus, NewJob};
use kh_db::{Db, DbError, PgJobs, ReapStats};
use serde_json::json;
use sqlx::PgPool;

fn repo(pool: PgPool) -> PgJobs {
    Db::from_pool(pool).jobs()
}

async fn make_stale(pool: &PgPool, id: JobId) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE jobs SET heartbeat_at = now() - interval '5 minutes' WHERE id = $1")
        .bind(id.0)
        .execute(pool)
        .await?;
    Ok(())
}

#[sqlx::test]
async fn enqueue_persists_defaults(pool: PgPool) -> Result<(), DbError> {
    let jobs = repo(pool);

    let job = jobs
        .enqueue(&NewJob::new("send_email", json!({ "to": "a@b.c" })))
        .await?;

    assert_eq!(job.status, JobStatus::Pending);
    assert_eq!(job.kind, "send_email");
    assert_eq!(job.payload, json!({ "to": "a@b.c" }));
    assert_eq!(job.attempts, 0);
    assert_eq!(job.max_attempts, 3);
    assert_eq!(job.timeout, Duration::minutes(5));
    assert!(job.worker_id.is_none());
    assert!(job.completed_at.is_none());
    Ok(())
}

#[sqlx::test]
async fn claim_marks_running_and_increments_attempts(pool: PgPool) -> Result<(), DbError> {
    let jobs = repo(pool);
    let job = jobs.enqueue(&NewJob::new("work", json!({}))).await?;

    let claimed = jobs.claim("worker-a", 10).await?;

    assert_eq!(claimed.len(), 1);
    let claimed = &claimed[0] as &Job;
    assert_eq!(claimed.id, job.id);
    assert_eq!(claimed.status, JobStatus::Running);
    assert_eq!(claimed.attempts, 1);
    assert_eq!(claimed.worker_id.as_deref(), Some("worker-a"));
    assert!(claimed.claimed_at.is_some());
    assert!(claimed.heartbeat_at.is_some());
    Ok(())
}

#[sqlx::test]
async fn claim_skips_jobs_that_are_not_due(pool: PgPool) -> Result<(), DbError> {
    let jobs = repo(pool);

    jobs.enqueue(&NewJob::new("later", json!({})).delay(Duration::hours(1)))
        .await?;

    assert!(jobs.claim("worker-a", 10).await?.is_empty());
    Ok(())
}

#[sqlx::test]
async fn concurrent_claims_are_disjoint(pool: PgPool) -> Result<(), DbError> {
    let jobs = repo(pool);

    for i in 0..50 {
        jobs.enqueue(&NewJob::new("bulk", json!({ "i": i })))
            .await?;
    }

    let mut handles = Vec::new();
    for worker in 0..8 {
        let jobs = jobs.clone();
        handles.push(tokio::spawn(async move {
            jobs.claim(&format!("worker-{worker}"), 10).await
        }));
    }

    let mut ids = Vec::new();
    for handle in handles {
        let claimed = handle.await.expect("claim task panicked")?;
        ids.extend(claimed.into_iter().map(|job| job.id));
    }

    assert_eq!(ids.len(), 50);
    assert_eq!(ids.iter().copied().collect::<HashSet<_>>().len(), 50);
    Ok(())
}

#[sqlx::test]
async fn lease_is_owner_scoped(pool: PgPool) -> Result<(), DbError> {
    let jobs = repo(pool);
    let job = jobs.enqueue(&NewJob::new("work", json!({}))).await?;
    jobs.claim("worker-a", 1).await?;

    assert!(!jobs.heartbeat(job.id, "worker-b").await?);
    assert!(jobs.heartbeat(job.id, "worker-a").await?);

    assert!(!jobs.complete(job.id, "worker-b").await?);
    assert!(jobs.complete(job.id, "worker-a").await?);

    let stored = jobs.get(job.id).await?.expect("job exists");
    assert_eq!(stored.status, JobStatus::Completed);
    assert!(stored.completed_at.is_some());
    Ok(())
}

#[sqlx::test]
async fn retry_reschedules_and_clears_lease(pool: PgPool) -> Result<(), DbError> {
    let jobs = repo(pool);
    let job = jobs.enqueue(&NewJob::new("work", json!({}))).await?;
    jobs.claim("worker-a", 1).await?;

    let next_run = Utc::now() + Duration::hours(1);
    assert!(jobs.retry(job.id, "worker-a", next_run, "boom").await?);

    let stored = jobs.get(job.id).await?.expect("job exists");
    assert_eq!(stored.status, JobStatus::Pending);
    assert_eq!(stored.attempts, 1);
    assert!(stored.claimed_at.is_none());
    assert!(stored.heartbeat_at.is_none());
    assert!(stored.worker_id.is_none());
    assert_eq!(stored.last_error.as_deref(), Some("boom"));
    assert!(jobs.claim("worker-a", 10).await?.is_empty());
    Ok(())
}

#[sqlx::test]
async fn fail_marks_job_terminal(pool: PgPool) -> Result<(), DbError> {
    let jobs = repo(pool);
    let job = jobs.enqueue(&NewJob::new("work", json!({}))).await?;
    jobs.claim("worker-a", 1).await?;

    assert!(jobs.fail(job.id, "worker-a", "nope").await?);

    let stored = jobs.get(job.id).await?.expect("job exists");
    assert_eq!(stored.status, JobStatus::Failed);
    assert!(stored.completed_at.is_some());
    assert_eq!(stored.last_error.as_deref(), Some("nope"));
    Ok(())
}

#[sqlx::test]
async fn reap_requeues_stale_running_jobs(pool: PgPool) -> Result<(), DbError> {
    let jobs = repo(pool.clone());
    let job = jobs.enqueue(&NewJob::new("work", json!({}))).await?;
    jobs.claim("worker-a", 1).await?;

    make_stale(&pool, job.id).await?;

    let stats = jobs.reap(Duration::minutes(1)).await?;
    assert_eq!(
        stats,
        ReapStats {
            requeued: 1,
            failed: 0
        }
    );

    let stored = jobs.get(job.id).await?.expect("job exists");
    assert_eq!(stored.status, JobStatus::Pending);
    assert_eq!(stored.attempts, 1);
    Ok(())
}

#[sqlx::test]
async fn reap_fails_exhausted_jobs(pool: PgPool) -> Result<(), DbError> {
    let jobs = repo(pool.clone());
    let job = jobs
        .enqueue(&NewJob::new("work", json!({})).max_attempts(1))
        .await?;
    jobs.claim("worker-a", 1).await?;

    make_stale(&pool, job.id).await?;

    let stats = jobs.reap(Duration::minutes(1)).await?;
    assert_eq!(
        stats,
        ReapStats {
            requeued: 0,
            failed: 1
        }
    );

    let stored = jobs.get(job.id).await?.expect("job exists");
    assert_eq!(stored.status, JobStatus::Failed);
    assert!(stored.completed_at.is_some());
    assert_eq!(
        stored.last_error.as_deref(),
        Some("worker heartbeat expired")
    );
    Ok(())
}

#[sqlx::test]
async fn stats_counts_jobs_by_status(pool: PgPool) -> Result<(), DbError> {
    let jobs = repo(pool);

    jobs.enqueue(&NewJob::new("a", json!({}))).await?;
    jobs.enqueue(&NewJob::new("b", json!({}))).await?;

    let claimed = jobs.claim("worker-a", 1).await?;
    jobs.fail(claimed[0].id, "worker-a", "boom").await?;

    let stats = jobs.stats().await?;
    assert_eq!(stats.pending, 1);
    assert_eq!(stats.running, 0);
    assert_eq!(stats.completed, 0);
    assert_eq!(stats.failed, 1);
    Ok(())
}
