mod config;
mod handlers;

use std::process::ExitCode;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use kh_core::{JobId, NewJob};
use kh_db::Db;
use kh_queue::{Registry, Worker};
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use crate::config::Config;

#[derive(Debug, Parser)]
#[command(
    name = "kh-worker",
    version,
    about = "PostgreSQL-backed job queue worker and admin CLI"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the worker until SIGINT or SIGTERM.
    Run {
        /// Do not apply pending migrations before starting.
        #[arg(long)]
        no_migrate: bool,
    },

    /// Apply pending database migrations and exit.
    Migrate,

    /// Insert a job into the queue.
    Enqueue {
        /// Handler kind, for example `echo` or `sleep`.
        kind: String,

        /// JSON payload.
        #[arg(default_value = "{}")]
        payload: String,

        /// Total attempts the job is allowed.
        #[arg(long, default_value_t = kh_core::DEFAULT_MAX_ATTEMPTS)]
        max_attempts: i32,

        /// Per-attempt timeout in seconds.
        #[arg(long)]
        timeout_secs: Option<i64>,

        /// Delay before the job becomes eligible, in seconds.
        #[arg(long, default_value_t = 0)]
        delay_secs: i64,
    },

    /// Print job counts by status.
    Stats,

    /// Print one job as JSON.
    Get {
        /// Job id.
        id: i64,
    },

    /// Move failed jobs back to pending.
    Requeue {
        /// Only requeue this job kind.
        #[arg(long)]
        kind: Option<String>,

        /// Maximum number of jobs to requeue.
        #[arg(long, default_value_t = 100)]
        limit: i64,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    dotenvy::dotenv().ok();
    init_tracing();

    let cli = Cli::parse();

    let config = match Config::from_env() {
        Ok(config) => config,
        Err(error) => {
            tracing::error!("{error:#}");
            return ExitCode::FAILURE;
        }
    };

    match run(cli.command, config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!("{error:#}");
            ExitCode::FAILURE
        }
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,kh_db=debug,kh_queue=debug"));

    tracing_subscriber::fmt().with_env_filter(filter).init();
}

async fn run(command: Command, config: Config) -> Result<()> {
    match command {
        Command::Run { no_migrate } => run_worker(config, no_migrate).await,
        Command::Migrate => {
            let db = connect(&config).await?;
            db.migrate().await?;
            println!("migrations applied");
            Ok(())
        }
        Command::Enqueue {
            kind,
            payload,
            max_attempts,
            timeout_secs,
            delay_secs,
        } => {
            enqueue(
                config,
                kind,
                payload,
                max_attempts,
                timeout_secs,
                delay_secs,
            )
            .await
        }
        Command::Stats => stats(config).await,
        Command::Get { id } => get(config, id).await,
        Command::Requeue { kind, limit } => requeue(config, kind, limit).await,
    }
}

async fn connect(config: &Config) -> Result<Db> {
    Db::connect(&config.db)
        .await
        .context("failed to connect to the database")
}

async fn run_worker(config: Config, no_migrate: bool) -> Result<()> {
    let db = connect(&config).await?;
    if !no_migrate {
        db.migrate().await?;
    }

    let mut registry = Registry::new();
    handlers::register_all(&mut registry);

    let worker = Worker::new(db, registry, config.queue.clone());
    let worker = match config.worker_id {
        Some(worker_id) => worker.with_worker_id(worker_id),
        None => worker,
    };

    let shutdown = CancellationToken::new();
    let signal = shutdown.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        signal.cancel();
    });

    let worker_id = worker.worker_id().to_owned();
    tracing::info!(
        worker_id,
        concurrency = config.queue.concurrency,
        "worker started"
    );

    worker.run(shutdown).await?;

    tracing::info!(worker_id, "worker stopped");
    Ok(())
}

async fn enqueue(
    config: Config,
    kind: String,
    payload: String,
    max_attempts: i32,
    timeout_secs: Option<i64>,
    delay_secs: i64,
) -> Result<()> {
    if max_attempts < 1 {
        anyhow::bail!("--max-attempts must be at least 1");
    }
    if matches!(timeout_secs, Some(secs) if secs <= 0) {
        anyhow::bail!("--timeout-secs must be positive");
    }
    if delay_secs < 0 {
        anyhow::bail!("--delay-secs must not be negative");
    }

    let payload: Value = serde_json::from_str(&payload).context("payload must be valid JSON")?;

    let mut job = NewJob::new(kind, payload).max_attempts(max_attempts);
    if let Some(secs) = timeout_secs {
        job = job.timeout(chrono::Duration::seconds(secs));
    }
    if delay_secs > 0 {
        job = job.delay(chrono::Duration::seconds(delay_secs));
    }

    let db = connect(&config).await?;
    let stored = db.jobs().enqueue(&job).await?;

    println!("enqueued job {}", stored.id);
    Ok(())
}

async fn stats(config: Config) -> Result<()> {
    let db = connect(&config).await?;
    let stats = db.jobs().stats().await?;

    println!("pending:   {}", stats.pending);
    println!("running:   {}", stats.running);
    println!("completed: {}", stats.completed);
    println!("failed:    {}", stats.failed);
    Ok(())
}

async fn get(config: Config, id: i64) -> Result<()> {
    let db = connect(&config).await?;

    match db.jobs().get(JobId(id)).await? {
        Some(job) => println!("{}", serde_json::to_string_pretty(&job)?),
        None => anyhow::bail!("job {id} not found"),
    }

    Ok(())
}

async fn requeue(config: Config, kind: Option<String>, limit: i64) -> Result<()> {
    if limit < 1 {
        anyhow::bail!("--limit must be at least 1");
    }

    let db = connect(&config).await?;
    let requeued = db.jobs().requeue_failed(kind.as_deref(), limit).await?;

    println!("requeued {requeued} job(s)");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}
