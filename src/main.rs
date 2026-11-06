//! Process entry point for the Bluesky-to-Manifold trading bot.
//!
//! Setup only: install the rustls crypto provider, configure `tracing`, load
//! [`config::Config`], then hand the process to [`app::App`]. The composition,
//! the broadcast subscriptions, and the task lifetime live in [`app`].

// Several module APIs are exercised by unit tests or reserved for the Discord
// layer that is wired in later.
#![allow(dead_code)]

mod app;
mod belief;
mod book;
mod config;
mod control;
mod copy;
mod db;
mod discord;
mod error;
mod extract;
mod ingest;
mod llm;
mod manifold_ws;
mod market_store;
mod onboarding;
mod onboarding_agent;
mod pipeline;
mod rate;
mod sieve;
mod snipe;
mod strategy_market;
mod trade;
mod traders;
mod triage;
mod types;

use crate::app::App;
use crate::config::Config;
use crate::error::{anyhow, AppResult};

#[tokio::main]
async fn main() -> AppResult<()> {
    // Serenity compiles with the rustls `ring` provider while rig's reqwest
    // stack adds `aws-lc-rs`, so rustls cannot pick one automatically. Pin
    // `ring` here, before any TLS handshake.
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|error| anyhow!("installing the default rustls crypto provider: {error:?}"))?;

    init_tracing();

    // Composition and task ownership live in `app`: `build` wires every service,
    // `spawn` subscribes every receiver before it starts any task.
    App::build(Config::from_env()?).await?.spawn().run().await
}

/// Configures `tracing` from `RUST_LOG`, defaulting to `info`.
fn init_tracing() {
    let filter = match tracing_subscriber::EnvFilter::try_from_default_env() {
        Ok(filter) => filter,
        Err(_) => tracing_subscriber::EnvFilter::new("info"),
    };
    tracing_subscriber::fmt().with_env_filter(filter).init();
}
