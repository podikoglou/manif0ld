//! Entry point for the Bluesky-to-Manifold trading bot.
//!
//! Phase 1 only wires configuration and storage. The ingestion, triage, belief,
//! trading, and Discord tasks are added by later modules.

// The module APIs are exercised by unit tests and consumed by the pipeline
// components added in later phases.
#![allow(dead_code)]

mod config;
mod db;
mod error;
mod extract;
mod ingest;
mod types;

use crate::config::Config;
use crate::db::Db;
use crate::error::AppResult;

fn main() -> AppResult<()> {
    let config = Config::from_env()?;
    let _db = Db::open(&config.db_path)?;
    Ok(())
}
