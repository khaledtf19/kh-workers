use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures_util::FutureExt;
use kh_core::{Job, JobContext, JobId};
use kh_db::{Db, JobListener, PgJobs};
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::config::QueueConfig;
use crate::error::QueueError;
use crate::registry::Registry;

/// Runs jobs from Postgres.
#[derive(Clone)]
pub struct Worker {
    db: Db,
    registry: Registry,
    config: QueueConfig,
    worker_id: String,
}

impl Worker {
    pub fn new(db: Db, registry: Registry, config: QueueConfig) -> Self {
        Self {
            db,
            registry,
            config,
            worker_id: default_worker_id(),
        }
    }

    /// Override the identifier written to `jobs.worker_id`.
    pub fn with_worker_id(mut self, worker_id: impl Into<String>) -> Self {
        self.worker_id = worker_id.into();
        self
    }

    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Run until `shutdown` is cancelled.
    ///
    /// Workers stop claiming immediately. In-flight jobs get
    /// [`QueueConfig::shutdown_grace`] to finish; after that their tasks are
    /// aborted and the jobs are left for the reaper.
    pub async fn run(self, shutdown: CancellationToken) -> Result<(), QueueError> {
        let stop = shutdown.child_token();
        let jobs = self.db.jobs();
        let notify = Arc::new(Notify::new());

        let listener_task = tokio::spawn(listen_loop(
            self.db.listener(),
            notify.clone(),
            stop.clone(),
        ));

        let mut slots = JoinSet::new();
        for _ in 0..self.config.concurrency.max(1) {
            let jobs = jobs.clone();
            let registry = self.registry.clone();
            let config = self.config.clone();
            let worker_id = self.worker_id.clone();
            let notify = notify.clone();
            let stop = stop.clone();

            slots.spawn(async move {
                run_slot(jobs, registry, config, worker_id, notify, stop).await;
            });
        }

        let reaper_task = tokio::spawn(reaper_loop(
            jobs,
            self.config.reaper_interval.max(Duration::from_millis(1)),
            self.config.stale_after,
            stop.clone(),
        ));

        let mut failure = None;

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                joined = slots.join_next() => match joined {
                    Some(Ok(())) => {
                        if !stop.is_cancelled() {
                            tracing::warn!("worker slot exited unexpectedly");
                        }
                    }
                    Some(Err(error)) => {
                        failure = Some(QueueError::Join(error));
                        stop.cancel();
                        break;
                    }
                    None => break,
                },
            }
        }

        let grace = tokio::time::sleep(self.config.shutdown_grace);
        tokio::pin!(grace);

        loop {
            tokio::select! {
                _ = &mut grace => {
                    slots.abort_all();
                    break;
                }
                joined = slots.join_next() => match joined {
                    Some(Ok(())) => {}
                    Some(Err(error)) => {
                        if !error.is_cancelled() && failure.is_none() {
                            failure = Some(QueueError::Join(error));
                        }
                    }
                    None => break,
                },
            }
        }

        reaper_task.abort();
        listener_task.abort();

        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

async fn run_slot(
    jobs: PgJobs,
    registry: Registry,
    config: QueueConfig,
    worker_id: String,
    notify: Arc<Notify>,
    shutdown: CancellationToken,
) {
    let poll_interval = config.poll_interval.max(Duration::from_millis(1));

    while !shutdown.is_cancelled() {
        match jobs.claim(&worker_id, 1).await {
            Ok(claimed) if claimed.is_empty() => {
                wait_for_work(&notify, poll_interval, &shutdown).await;
            }
            Ok(claimed) => {
                for job in claimed {
                    process_job(&jobs, &registry, &config, &worker_id, job).await;
                }
            }
            Err(error) => {
                tracing::error!(%error, worker_id = %worker_id, "failed to claim jobs");
                tokio::select! {
                    _ = shutdown.cancelled() => {}
                    _ = tokio::time::sleep(poll_interval) => {}
                }
            }
        }
    }
}

async fn process_job(
    jobs: &PgJobs,
    registry: &Registry,
    config: &QueueConfig,
    worker_id: &str,
    job: Job,
) {
    let Job {
        id,
        kind,
        payload,
        attempts,
        max_attempts,
        timeout,
        ..
    } = job;

    let Some(handler) = registry.get(&kind) else {
        tracing::error!(job_id = %id, %kind, "no handler registered for job kind");
        mark_failed(jobs, id, worker_id, &format!("unknown job kind: {kind}")).await;
        return;
    };

    let ctx = JobContext {
        job_id: id,
        kind,
        attempt: attempts,
        max_attempts,
        worker_id: worker_id.to_owned(),
        timeout,
    };

    let mut ticker = tokio::time::interval(heartbeat_interval(timeout));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await;

    let handler_fut = AssertUnwindSafe(handler.handle(&ctx, payload)).catch_unwind();
    tokio::pin!(handler_fut);

    let outcome = loop {
        tokio::select! {
            _ = ticker.tick() => {
                match jobs.heartbeat(id, worker_id).await {
                    Ok(true) => {}
                    Ok(false) => {
                        tracing::warn!(job_id = %id, "lease lost; abandoning handler");
                        break None;
                    }
                    Err(error) => tracing::warn!(job_id = %id, %error, "heartbeat failed"),
                }
            }
            result = &mut handler_fut => break Some(result),
        }
    };

    let Some(outcome) = outcome else {
        return;
    };

    match outcome {
        Ok(Ok(())) => mark_completed(jobs, id, worker_id).await,
        Ok(Err(error)) => {
            let message = error.to_string();

            if error.is_retryable() && attempts < max_attempts {
                let delay = chrono::Duration::from_std(config.backoff.delay_for(attempts))
                    .unwrap_or_else(|_| chrono::Duration::minutes(5));
                let run_at = Utc::now() + delay;

                tracing::info!(job_id = %id, attempt = attempts, "job failed; scheduling retry");
                mark_retry(jobs, id, worker_id, run_at, &message).await;
            } else {
                tracing::error!(job_id = %id, attempt = attempts, %message, "job failed permanently");
                mark_failed(jobs, id, worker_id, &message).await;
            }
        }
        Err(panic) => {
            let message = format!("handler panicked: {}", panic_message(panic));
            tracing::error!(job_id = %id, %message);
            mark_failed(jobs, id, worker_id, &message).await;
        }
    }
}

async fn mark_completed(jobs: &PgJobs, id: JobId, worker_id: &str) {
    match jobs.complete(id, worker_id).await {
        Ok(true) => tracing::info!(job_id = %id, "job completed"),
        Ok(false) => tracing::warn!(job_id = %id, "lease lost before completion"),
        Err(error) => tracing::error!(job_id = %id, %error, "failed to mark job completed"),
    }
}

async fn mark_retry(jobs: &PgJobs, id: JobId, worker_id: &str, run_at: DateTime<Utc>, error: &str) {
    match jobs.retry(id, worker_id, run_at, error).await {
        Ok(true) => {}
        Ok(false) => tracing::warn!(job_id = %id, "lease lost before retry"),
        Err(error) => tracing::error!(job_id = %id, %error, "failed to schedule retry"),
    }
}

async fn mark_failed(jobs: &PgJobs, id: JobId, worker_id: &str, error: &str) {
    match jobs.fail(id, worker_id, error).await {
        Ok(true) => {}
        Ok(false) => tracing::warn!(job_id = %id, "lease lost before failing"),
        Err(error) => tracing::error!(job_id = %id, %error, "failed to mark job failed"),
    }
}

async fn wait_for_work(notify: &Notify, poll_interval: Duration, shutdown: &CancellationToken) {
    tokio::select! {
        _ = shutdown.cancelled() => {}
        _ = notify.notified() => {}
        _ = tokio::time::sleep(poll_interval) => {}
    }
}

async fn listen_loop(mut listener: JobListener, notify: Arc<Notify>, shutdown: CancellationToken) {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            signal = listener.recv() => match signal {
                Some(()) => notify.notify_waiters(),
                None => break,
            },
        }
    }
}

async fn reaper_loop(
    jobs: PgJobs,
    interval: Duration,
    stale_after: Duration,
    shutdown: CancellationToken,
) {
    let stale_after =
        chrono::Duration::from_std(stale_after).unwrap_or_else(|_| chrono::Duration::minutes(5));
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = ticker.tick() => {
                match jobs.reap(stale_after).await {
                    Ok(stats) if stats.requeued > 0 || stats.failed > 0 => {
                        tracing::warn!(
                            requeued = stats.requeued,
                            failed = stats.failed,
                            "reaped stale jobs"
                        );
                    }
                    Ok(_) => {}
                    Err(error) => tracing::error!(%error, "reaper failed"),
                }
            }
        }
    }
}

fn heartbeat_interval(timeout: chrono::Duration) -> Duration {
    let third = timeout.to_std().unwrap_or(Duration::from_secs(30)) / 3;

    third.clamp(Duration::from_secs(1), Duration::from_secs(30))
}

fn panic_message(panic: Box<dyn Any + Send>) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_owned()
    }
}

fn default_worker_id() -> String {
    format!(
        "worker-{}-{:08x}",
        std::process::id(),
        rand::random::<u32>()
    )
}
