use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use kh_core::{Handler, HandlerError, Job, JobContext, JobId, JobStatus, NewJob};
use kh_db::{Db, PgJobs};
use kh_queue::{BackoffPolicy, QueueConfig, QueueError, Registry, Worker};
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Adapts a closure into a [`Handler`] so each test can define its behaviour
/// inline.
struct TestHandler<F> {
    f: F,
}

#[async_trait]
impl<F, Fut> Handler for TestHandler<F>
where
    F: Fn(JobContext, Value) -> Fut + Send + Sync,
    Fut: Future<Output = Result<(), HandlerError>> + Send,
{
    async fn handle(&self, ctx: &JobContext, payload: Value) -> Result<(), HandlerError> {
        (self.f)(ctx.clone(), payload).await
    }
}

fn jobs(pool: &PgPool) -> PgJobs {
    Db::from_pool(pool.clone()).jobs()
}

fn config() -> QueueConfig {
    QueueConfig {
        concurrency: 2,
        poll_interval: Duration::from_millis(50),
        shutdown_grace: Duration::from_secs(2),
        reaper_interval: Duration::from_secs(60),
        stale_after: Duration::from_secs(120),
        backoff: BackoffPolicy {
            base: Duration::from_millis(20),
            max: Duration::from_secs(1),
            jitter: 0.0,
        },
    }
}

fn start_worker(
    pool: &PgPool,
    registry: Registry,
    config: QueueConfig,
) -> (CancellationToken, JoinHandle<Result<(), QueueError>>) {
    let worker =
        Worker::new(Db::from_pool(pool.clone()), registry, config).with_worker_id("test-worker");
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(worker.run(shutdown.clone()));
    (shutdown, handle)
}

async fn stop_worker(shutdown: CancellationToken, handle: JoinHandle<Result<(), QueueError>>) {
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("worker did not stop in time")
        .expect("worker task panicked")
        .expect("worker returned an error");
}

async fn wait_for_status(jobs: &PgJobs, id: JobId, status: JobStatus) -> Job {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);

    loop {
        let job = jobs.get(id).await.expect("load job").expect("job exists");
        if job.status == status {
            return job;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "job {id} stayed {:?}, expected {status}",
            job.status
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[sqlx::test(migrations = "../kh-db/migrations")]
async fn completes_job(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(Mutex::new(None::<(i32, String, Value)>));

    let calls_handler = calls.clone();
    let seen_handler = seen.clone();
    let registry = Registry::new().with(
        "greet",
        TestHandler {
            f: move |ctx: JobContext, payload: Value| {
                let calls = calls_handler.clone();
                let seen = seen_handler.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    *seen.lock().unwrap() = Some((ctx.attempt, ctx.worker_id, payload));
                    Ok(())
                }
            },
        },
    );

    let jobs = jobs(&pool);
    let job = jobs
        .enqueue(&NewJob::new("greet", json!({ "name": "kh" })))
        .await?;

    let (shutdown, handle) = start_worker(&pool, registry, config());
    let stored = wait_for_status(&jobs, job.id, JobStatus::Completed).await;
    stop_worker(shutdown, handle).await;

    assert_eq!(stored.attempts, 1);
    assert!(stored.completed_at.is_some());
    assert!(stored.last_error.is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let (attempt, worker_id, payload) = seen.lock().unwrap().take().expect("handler ran");
    assert_eq!(attempt, 1);
    assert_eq!(worker_id, "test-worker");
    assert_eq!(payload, json!({ "name": "kh" }));
    Ok(())
}

#[sqlx::test(migrations = "../kh-db/migrations")]
async fn retries_until_success(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_handler = calls.clone();
    let registry = Registry::new().with(
        "flaky",
        TestHandler {
            f: move |_ctx: JobContext, _payload: Value| {
                let calls = calls_handler.clone();
                async move {
                    let attempt = calls.fetch_add(1, Ordering::SeqCst) + 1;
                    if attempt < 3 {
                        Err(HandlerError::retryable(format!("failure {attempt}")))
                    } else {
                        Ok(())
                    }
                }
            },
        },
    );

    let jobs = jobs(&pool);
    let job = jobs
        .enqueue(&NewJob::new("flaky", json!({})).max_attempts(3))
        .await?;

    let (shutdown, handle) = start_worker(&pool, registry, config());
    let stored = wait_for_status(&jobs, job.id, JobStatus::Completed).await;
    stop_worker(shutdown, handle).await;

    assert_eq!(stored.attempts, 3);
    assert!(stored.last_error.is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    Ok(())
}

#[sqlx::test(migrations = "../kh-db/migrations")]
async fn fatal_error_fails_immediately(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_handler = calls.clone();
    let registry = Registry::new().with(
        "bad",
        TestHandler {
            f: move |_ctx: JobContext, _payload: Value| {
                let calls = calls_handler.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err(HandlerError::fatal("invalid payload"))
                }
            },
        },
    );

    let jobs = jobs(&pool);
    let job = jobs.enqueue(&NewJob::new("bad", json!({}))).await?;

    let (shutdown, handle) = start_worker(&pool, registry, config());
    let stored = wait_for_status(&jobs, job.id, JobStatus::Failed).await;
    stop_worker(shutdown, handle).await;

    assert_eq!(stored.attempts, 1);
    assert!(stored.completed_at.is_some());
    assert_eq!(
        stored.last_error.as_deref(),
        Some("fatal job failure: invalid payload")
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[sqlx::test(migrations = "../kh-db/migrations")]
async fn retryable_error_fails_after_max_attempts(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_handler = calls.clone();
    let registry = Registry::new().with(
        "always_flaky",
        TestHandler {
            f: move |_ctx: JobContext, _payload: Value| {
                let calls = calls_handler.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err(HandlerError::retryable("boom"))
                }
            },
        },
    );

    let jobs = jobs(&pool);
    let job = jobs
        .enqueue(&NewJob::new("always_flaky", json!({})).max_attempts(2))
        .await?;

    let (shutdown, handle) = start_worker(&pool, registry, config());
    let stored = wait_for_status(&jobs, job.id, JobStatus::Failed).await;
    stop_worker(shutdown, handle).await;

    assert_eq!(stored.attempts, 2);
    assert!(stored.completed_at.is_some());
    assert_eq!(
        stored.last_error.as_deref(),
        Some("retryable job failure: boom")
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    Ok(())
}

#[sqlx::test(migrations = "../kh-db/migrations")]
async fn unknown_kind_is_failed(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let jobs = jobs(&pool);
    let job = jobs.enqueue(&NewJob::new("nobody", json!({}))).await?;

    let (shutdown, handle) = start_worker(&pool, Registry::new(), config());
    let stored = wait_for_status(&jobs, job.id, JobStatus::Failed).await;
    stop_worker(shutdown, handle).await;

    assert_eq!(stored.attempts, 1);
    assert_eq!(
        stored.last_error.as_deref(),
        Some("unknown job kind: nobody")
    );
    Ok(())
}

#[sqlx::test(migrations = "../kh-db/migrations")]
async fn shuts_down_when_idle(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let (shutdown, handle) = start_worker(&pool, Registry::new(), config());

    tokio::time::sleep(Duration::from_millis(200)).await;

    let started = tokio::time::Instant::now();
    stop_worker(shutdown, handle).await;
    assert!(started.elapsed() < Duration::from_secs(2));

    assert_eq!(jobs(&pool).stats().await?.pending, 0);
    Ok(())
}

#[sqlx::test(migrations = "../kh-db/migrations")]
async fn heartbeats_keep_long_jobs_alive(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_handler = calls.clone();
    let registry = Registry::new().with(
        "slow",
        TestHandler {
            f: move |_ctx: JobContext, _payload: Value| {
                let calls = calls_handler.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    Ok(())
                }
            },
        },
    );

    let jobs = jobs(&pool);
    let job = jobs
        .enqueue(&NewJob::new("slow", json!({})).timeout(chrono::Duration::seconds(6)))
        .await?;

    let mut config = config();
    config.stale_after = Duration::from_secs(3);

    let (shutdown, handle) = start_worker(&pool, registry, config);
    wait_for_status(&jobs, job.id, JobStatus::Running).await;

    // The heartbeat interval is timeout / 3 = 2s, so by 3.5s the lease must
    // have been renewed even though the handler is still running.
    tokio::time::sleep(Duration::from_millis(3_500)).await;

    let running = jobs.get(job.id).await?.expect("job exists");
    assert_eq!(
        running.status,
        JobStatus::Running,
        "job finished before the stale check"
    );

    let reaped = jobs.reap(chrono::Duration::seconds(3)).await?;
    assert_eq!(reaped.requeued, 0);
    assert_eq!(reaped.failed, 0);

    let stored = wait_for_status(&jobs, job.id, JobStatus::Completed).await;
    stop_worker(shutdown, handle).await;

    assert_eq!(stored.attempts, 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}
