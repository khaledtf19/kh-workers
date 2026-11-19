use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::UnknownJobStatus;

/// Default number of attempts a job gets when [`NewJob`] does not override it.
pub const DEFAULT_MAX_ATTEMPTS: i32 = 3;

/// Default execution timeout, in seconds, applied to new jobs.
pub const DEFAULT_TIMEOUT_SECS: i64 = 5 * 60;

/// Identifier of a job, backed by the database identity column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JobId(pub i64);

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<i64> for JobId {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl From<JobId> for i64 {
    fn from(value: JobId) -> Self {
        value.0
    }
}

/// Lifecycle state of a job.
///
/// Retries move a job from [`Running`](Self::Running) back to
/// [`Pending`](Self::Pending); [`Failed`](Self::Failed) is terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

impl JobStatus {
    /// The database representation of the status.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    /// Whether the job is in a terminal state and will not run again.
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed)
    }
}

impl fmt::Display for JobStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for JobStatus {
    type Err = UnknownJobStatus;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            other => Err(UnknownJobStatus(other.to_owned())),
        }
    }
}

/// A job as stored in the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: JobId,
    pub status: JobStatus,

    /// Handler lookup key.
    pub kind: String,

    pub payload: Value,

    /// Number of attempts made so far, including the current one.
    pub attempts: i32,

    pub max_attempts: i32,

    /// Earliest time the job may be claimed.
    pub run_at: DateTime<Utc>,

    pub claimed_at: Option<DateTime<Utc>>,
    pub heartbeat_at: Option<DateTime<Utc>>,

    /// Set for both completed and failed jobs.
    pub completed_at: Option<DateTime<Utc>>,

    /// Maximum execution time for one attempt.
    pub timeout: Duration,

    pub worker_id: Option<String>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl Job {
    /// Whether the job has attempts left after the current one.
    pub fn can_retry(&self) -> bool {
        self.attempts < self.max_attempts
    }

    /// When the current attempt must finish, if the job is claimed.
    pub fn deadline(&self) -> Option<DateTime<Utc>> {
        self.claimed_at.map(|claimed_at| claimed_at + self.timeout)
    }
}

/// A job to enqueue. Build it with [`NewJob::new`] and the builder methods.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewJob {
    /// Handler lookup key.
    pub kind: String,

    pub payload: Value,

    /// Total attempts the job is allowed.
    pub max_attempts: i32,

    /// Earliest time the job may be claimed. `None` uses the database's
    /// `now()` default at insert time.
    pub run_at: Option<DateTime<Utc>>,

    /// Maximum execution time for one attempt.
    pub timeout: Duration,
}

impl NewJob {
    /// Create a job with the default retry and timeout policy.
    pub fn new(kind: impl Into<String>, payload: Value) -> Self {
        Self {
            kind: kind.into(),
            payload,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            run_at: None,
            timeout: Duration::seconds(DEFAULT_TIMEOUT_SECS),
        }
    }

    /// Set how many attempts the job gets in total.
    pub fn max_attempts(mut self, max_attempts: i32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Earliest time the job may be claimed.
    pub fn run_at(mut self, run_at: DateTime<Utc>) -> Self {
        self.run_at = Some(run_at);
        self
    }

    /// Run the job after `delay` from now.
    pub fn delay(mut self, delay: Duration) -> Self {
        self.run_at = Some(Utc::now() + delay);
        self
    }

    /// Maximum execution time for one attempt.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn status_roundtrips_through_string() {
        for status in [
            JobStatus::Pending,
            JobStatus::Running,
            JobStatus::Completed,
            JobStatus::Failed,
        ] {
            assert_eq!(status.as_str().parse::<JobStatus>().unwrap(), status);
        }
    }

    #[test]
    fn unknown_status_is_rejected() {
        assert_eq!(
            "dead".parse::<JobStatus>().unwrap_err(),
            UnknownJobStatus("dead".to_owned())
        );
    }

    #[test]
    fn status_serializes_lowercase() {
        assert_eq!(
            serde_json::to_string(&JobStatus::Running).unwrap(),
            "\"running\""
        );
    }

    #[test]
    fn job_id_serializes_as_number() {
        assert_eq!(serde_json::to_string(&JobId(42)).unwrap(), "42");
    }

    #[test]
    fn new_job_uses_defaults() {
        let job = NewJob::new("send_email", json!({"to": "a@b.c"}));

        assert_eq!(job.max_attempts, DEFAULT_MAX_ATTEMPTS);
        assert_eq!(job.timeout, Duration::seconds(DEFAULT_TIMEOUT_SECS));
        assert!(job.run_at.is_none());
    }

    #[test]
    fn new_job_builder_overrides_defaults() {
        let at = Utc::now();
        let job = NewJob::new("resize", json!({}))
            .max_attempts(7)
            .run_at(at)
            .timeout(Duration::minutes(2));

        assert_eq!(job.max_attempts, 7);
        assert_eq!(job.run_at, Some(at));
        assert_eq!(job.timeout, Duration::minutes(2));
    }

    fn running_job(attempts: i32, max_attempts: i32) -> Job {
        let now = Utc::now();
        Job {
            id: JobId(1),
            status: JobStatus::Running,
            kind: "test".to_owned(),
            payload: json!({}),
            attempts,
            max_attempts,
            run_at: now,
            claimed_at: Some(now),
            heartbeat_at: None,
            completed_at: None,
            timeout: Duration::minutes(5),
            worker_id: Some("worker-1".to_owned()),
            last_error: None,
            created_at: now,
        }
    }

    #[test]
    fn can_retry_until_max_attempts() {
        assert!(running_job(2, 3).can_retry());
        assert!(!running_job(3, 3).can_retry());
        assert!(!running_job(4, 3).can_retry());
    }

    #[test]
    fn deadline_is_claim_time_plus_timeout() {
        let job = running_job(1, 3);

        assert_eq!(
            job.deadline(),
            job.claimed_at
                .map(|claimed_at| claimed_at + Duration::minutes(5))
        );
    }
}
