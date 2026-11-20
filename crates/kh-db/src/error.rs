use thiserror::Error;

/// Error returned by the database layer.
#[derive(Debug, Error)]
pub enum DbError {
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    #[error(transparent)]
    Migrate(#[from] sqlx::migrate::MigrateError),

    #[error(transparent)]
    UnknownStatus(#[from] kh_core::UnknownJobStatus),

    #[error("unsupported interval: {0}")]
    UnsupportedInterval(String),
}
