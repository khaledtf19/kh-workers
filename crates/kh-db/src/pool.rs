use sqlx::postgres::{PgPool, PgPoolOptions};

use crate::config::DbConfig;
use crate::error::DbError;
use crate::repo_jobs::PgJobs;

/// Handle to the Postgres database used by the queue.
#[derive(Debug, Clone)]
pub struct Db {
    pool: PgPool,
}

impl Db {
    /// Open a pool using `config`.
    pub async fn connect(config: &DbConfig) -> Result<Self, DbError> {
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .connect(&config.url)
            .await?;

        Ok(Self { pool })
    }

    /// Wrap an existing pool, for example in tests.
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Apply the migrations embedded in this crate.
    pub async fn migrate(&self) -> Result<(), DbError> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        tracing::info!("database migrations applied");
        Ok(())
    }

    /// Access the job repository.
    pub fn jobs(&self) -> PgJobs {
        PgJobs::new(self.pool.clone())
    }
}
