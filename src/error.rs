//! Error handling aliases shared by every module.
//!
//! The crate uses [`anyhow`] for application errors: callers add context at the
//! boundary where the failure is meaningful, and library-style helpers return
//! the same [`anyhow::Result`] so `?` works across module boundaries.

pub use anyhow::{anyhow, bail, Context, Result};

/// Result type used by fallible application code.
///
/// This is a thin alias so signatures read as `AppResult<T>` while remaining
/// interchangeable with [`anyhow::Result`].
pub type AppResult<T> = Result<T>;
