//! Queue runtime: worker loop, handler registry, backoff policy, and reaper.

#![forbid(unsafe_code)]

mod backoff;
mod config;
mod error;
mod registry;
mod worker;

pub use backoff::BackoffPolicy;
pub use config::QueueConfig;
pub use error::QueueError;
pub use registry::Registry;
pub use worker::Worker;
