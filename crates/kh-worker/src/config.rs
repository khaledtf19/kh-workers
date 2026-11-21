use std::env;
use std::time::Duration;

use anyhow::{Context, Result};
use kh_db::DbConfig;
use kh_queue::{BackoffPolicy, QueueConfig};

/// Runtime configuration assembled from environment variables.
pub struct Config {
    pub db: DbConfig,
    pub queue: QueueConfig,
    /// Optional identity written to `jobs.worker_id`.
    pub worker_id: Option<String>,
}

impl Config {
    /// Read the worker configuration, applying defaults for optional values.
    pub fn from_env() -> Result<Self> {
        let url = env::var("DATABASE_URL").context("DATABASE_URL must be set")?;

        let db = DbConfig::new(url).max_connections(env_parse("DATABASE_MAX_CONNECTIONS", 10)?);

        let backoff = BackoffPolicy {
            base: Duration::from_millis(env_parse("BACKOFF_BASE_MS", 5_000)?),
            max: Duration::from_millis(env_parse("BACKOFF_MAX_MS", 300_000)?),
            jitter: env_parse("BACKOFF_JITTER", 0.2)?,
        };

        let queue = QueueConfig {
            concurrency: env_parse("WORKER_CONCURRENCY", 4)?,
            poll_interval: Duration::from_millis(env_parse("POLL_INTERVAL_MS", 5_000)?),
            shutdown_grace: Duration::from_secs(env_parse("SHUTDOWN_GRACE_SECS", 30)?),
            reaper_interval: Duration::from_secs(env_parse("REAPER_INTERVAL_SECS", 30)?),
            stale_after: Duration::from_secs(env_parse("STALE_AFTER_SECS", 120)?),
            backoff,
        };

        Ok(Self {
            db,
            queue,
            worker_id: env::var("WORKER_ID")
                .ok()
                .filter(|value| !value.trim().is_empty()),
        })
    }
}

fn env_parse<T>(key: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match env::var(key) {
        Ok(value) => value
            .parse()
            .map_err(|error| anyhow::anyhow!("{key} is not a valid value ({value}): {error}")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error).with_context(|| format!("failed to read {key}")),
    }
}
