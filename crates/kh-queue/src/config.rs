use std::time::Duration;

use crate::backoff::BackoffPolicy;

/// Runtime settings for a worker.
#[derive(Debug, Clone)]
pub struct QueueConfig {
    /// Number of concurrent job slots.
    pub concurrency: usize,

    /// Fallback poll interval when no notification arrives.
    pub poll_interval: Duration,

    /// How long in-flight jobs may finish after shutdown is requested.
    pub shutdown_grace: Duration,

    /// How often the reaper looks for stale running jobs.
    pub reaper_interval: Duration,

    /// A running job is stale when its heartbeat is older than this.
    pub stale_after: Duration,

    /// Retry policy used for retryable handler failures.
    pub backoff: BackoffPolicy,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            concurrency: 4,
            poll_interval: Duration::from_secs(5),
            shutdown_grace: Duration::from_secs(30),
            reaper_interval: Duration::from_secs(30),
            stale_after: Duration::from_secs(120),
            backoff: BackoffPolicy::default(),
        }
    }
}
