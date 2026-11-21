use thiserror::Error;

/// Error returned by the queue runtime.
#[derive(Debug, Error)]
pub enum QueueError {
    #[error(transparent)]
    Db(#[from] kh_db::DbError),

    #[error("worker task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}
