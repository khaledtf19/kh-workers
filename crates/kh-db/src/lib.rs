//! PostgreSQL persistence for the job queue: pool, migrations, and repositories.

#![forbid(unsafe_code)]

mod config;
mod error;
mod pool;
mod repo_jobs;

pub use config::DbConfig;
pub use error::DbError;
pub use pool::Db;
pub use repo_jobs::{JobStats, PgJobs, ReapStats};
