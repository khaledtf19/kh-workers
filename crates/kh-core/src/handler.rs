use async_trait::async_trait;
use chrono::Duration;
use serde_json::Value;

use crate::error::HandlerError;
use crate::job::JobId;

/// Metadata about the job a handler is executing.
#[derive(Debug, Clone)]
pub struct JobContext {
    /// Identifier of the job being processed.
    pub job_id: JobId,

    /// The registered kind of the job.
    pub kind: String,

    /// Current attempt, starting at 1.
    pub attempt: i32,

    /// Total attempts the job is allowed.
    pub max_attempts: i32,

    /// Identifier of the worker executing this attempt.
    pub worker_id: String,

    /// Maximum execution time for this attempt.
    pub timeout: Duration,
}

impl JobContext {
    /// Whether this is the final allowed attempt.
    pub fn is_last_attempt(&self) -> bool {
        self.attempt >= self.max_attempts
    }
}

/// A unit of work that processes one kind of job.
///
/// Handlers are registered by kind in the queue's registry. Returning
/// [`HandlerError::Retryable`] lets the queue retry the job with backoff,
/// while [`HandlerError::Fatal`] sends it straight to the failed state.
#[async_trait]
pub trait Handler: Send + Sync {
    /// Execute one attempt of a job.
    async fn handle(&self, ctx: &JobContext, payload: Value) -> Result<(), HandlerError>;
}
