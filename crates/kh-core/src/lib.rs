//! Domain types and contracts shared by the storage, queue, and worker crates.
//!
//! This crate is intentionally free of runtime and database dependencies: it
//! describes what a job is and how a handler processes one.

#![forbid(unsafe_code)]

mod error;
mod handler;
mod job;

pub use error::{HandlerError, UnknownJobStatus};
pub use handler::{Handler, JobContext};
pub use job::{DEFAULT_MAX_ATTEMPTS, DEFAULT_TIMEOUT_SECS, Job, JobId, JobStatus, NewJob};
