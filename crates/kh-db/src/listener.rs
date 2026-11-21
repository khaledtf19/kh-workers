use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::PgListener;
use tokio::sync::mpsc;

/// Channel used to wake workers when a job is enqueued.
pub const JOBS_READY_CHANNEL: &str = "kh_jobs_ready";

/// Receives wake-up signals from Postgres `LISTEN`/`NOTIFY`.
///
/// The listener reconnects transparently. Notifications are hints only: a
/// missed one is covered by the worker's polling fallback.
pub struct JobListener {
    rx: mpsc::Receiver<()>,
    task: tokio::task::JoinHandle<()>,
}

impl JobListener {
    pub(crate) fn spawn(pool: PgPool) -> Self {
        let (tx, rx) = mpsc::channel(64);
        let task = tokio::spawn(listen_loop(pool, tx));
        Self { rx, task }
    }

    /// Wait for the next notification. Returns `None` once the listener has
    /// stopped.
    pub async fn recv(&mut self) -> Option<()> {
        self.rx.recv().await
    }
}

impl Drop for JobListener {
    fn drop(&mut self) {
        // Abort the connection task so its pool connection is returned
        // instead of being held until the next notification or disconnect.
        self.task.abort();
    }
}

async fn listen_loop(pool: PgPool, tx: mpsc::Sender<()>) {
    loop {
        let mut listener = match PgListener::connect_with(&pool).await {
            Ok(listener) => listener,
            Err(error) => {
                tracing::warn!(%error, "job listener connection failed; retrying");
                tokio::time::sleep(Duration::from_secs(1)).await;
                if tx.is_closed() {
                    return;
                }
                continue;
            }
        };

        if let Err(error) = listener.listen(JOBS_READY_CHANNEL).await {
            tracing::warn!(%error, "job listener failed to LISTEN; retrying");
            tokio::time::sleep(Duration::from_secs(1)).await;
            if tx.is_closed() {
                return;
            }
            continue;
        }

        loop {
            match listener.recv().await {
                Ok(_) => {
                    if tx.send(()).await.is_err() {
                        return;
                    }
                }
                Err(error) => {
                    if tx.is_closed() {
                        return;
                    }
                    tracing::warn!(%error, "job listener disconnected; reconnecting");
                    break;
                }
            }
        }
    }
}
