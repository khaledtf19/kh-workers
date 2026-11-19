use thiserror::Error;

/// Error returned by a job [`Handler`](crate::Handler).
#[derive(Debug, Error)]
pub enum HandlerError {
    /// The attempt failed, but the job may be retried according to its policy.
    #[error("retryable job failure: {0}")]
    Retryable(String),

    /// The job failed permanently and must not be retried.
    #[error("fatal job failure: {0}")]
    Fatal(String),
}

impl HandlerError {
    /// Build a retryable failure from any displayable message.
    pub fn retryable(message: impl Into<String>) -> Self {
        Self::Retryable(message.into())
    }

    /// Build a permanent failure from any displayable message.
    pub fn fatal(message: impl Into<String>) -> Self {
        Self::Fatal(message.into())
    }

    /// Whether the queue may retry the job after this failure.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable(_))
    }
}

/// Error returned when parsing an unrecognised [`JobStatus`](crate::JobStatus).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("unknown job status: {0}")]
pub struct UnknownJobStatus(pub String);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_error_reports_retryable() {
        let error = HandlerError::retryable("connection reset");
        assert!(error.is_retryable());
        assert_eq!(error.to_string(), "retryable job failure: connection reset");
    }

    #[test]
    fn fatal_error_is_not_retryable() {
        let error = HandlerError::fatal("invalid payload");
        assert!(!error.is_retryable());
        assert_eq!(error.to_string(), "fatal job failure: invalid payload");
    }
}
