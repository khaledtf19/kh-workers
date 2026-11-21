//! Handlers wired into the worker binary.

use async_trait::async_trait;
use kh_core::{Handler, HandlerError, JobContext};
use kh_queue::Registry;
use serde_json::Value;

/// Register every handler this worker knows how to run.
pub fn register_all(registry: &mut Registry) {
    registry.register("echo", Echo);
    registry.register("sleep", Sleep);
}

/// Logs the payload and succeeds. Useful as a smoke test.
struct Echo;

#[async_trait]
impl Handler for Echo {
    async fn handle(&self, ctx: &JobContext, payload: Value) -> Result<(), HandlerError> {
        tracing::info!(
            job_id = %ctx.job_id,
            kind = %ctx.kind,
            attempt = ctx.attempt,
            %payload,
            "echo"
        );
        Ok(())
    }
}

/// Sleeps for `payload.ms` milliseconds to demonstrate a long-running job.
struct Sleep;

#[async_trait]
impl Handler for Sleep {
    async fn handle(&self, ctx: &JobContext, payload: Value) -> Result<(), HandlerError> {
        let ms = payload
            .get("ms")
            .and_then(Value::as_u64)
            .ok_or_else(|| HandlerError::fatal("sleep payload requires an integer `ms` field"))?;

        tracing::info!(job_id = %ctx.job_id, ms, "sleeping");
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use kh_core::JobId;
    use serde_json::json;

    fn context(kind: &str) -> JobContext {
        JobContext {
            job_id: JobId(1),
            kind: kind.to_owned(),
            attempt: 1,
            max_attempts: 3,
            worker_id: "test-worker".to_owned(),
            timeout: Duration::minutes(1),
        }
    }

    #[tokio::test]
    async fn echo_succeeds() {
        let mut registry = Registry::new();
        register_all(&mut registry);

        let handler = registry.get("echo").expect("echo is registered");
        handler
            .handle(&context("echo"), json!({ "ok": true }))
            .await
            .expect("echo succeeds");
    }

    #[tokio::test]
    async fn sleep_succeeds_with_ms() {
        let mut registry = Registry::new();
        register_all(&mut registry);

        let handler = registry.get("sleep").expect("sleep is registered");
        handler
            .handle(&context("sleep"), json!({ "ms": 1 }))
            .await
            .expect("sleep succeeds");
    }

    #[tokio::test]
    async fn sleep_requires_ms() {
        let mut registry = Registry::new();
        register_all(&mut registry);

        let handler = registry.get("sleep").expect("sleep is registered");
        let error = handler
            .handle(&context("sleep"), json!({}))
            .await
            .expect_err("missing ms is fatal");

        assert!(!error.is_retryable());
    }
}
