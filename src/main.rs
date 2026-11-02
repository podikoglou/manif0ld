//! Entry point for the Bluesky-to-Manifold trading bot.
//!
//! Startup is deliberately linear: load configuration, open the database,
//! compile one Sieve engine per active market, build the market cache, then
//! connect ingestion to the triage pipeline and the pipeline to the trade
//! executor. Discord is not started yet; [`pipeline::PipelineEvent`] is the
//! extension point a Discord consumer subscribes to.
//!
//! The process runs until Ctrl-C, then aborts its spawned tasks.

// Several module APIs are exercised by unit tests or reserved for the Discord
// layer that is wired in later.
#![allow(dead_code)]

mod belief;
mod config;
mod db;
mod discord;
mod error;
mod extract;
mod ingest;
mod onboarding;
mod pipeline;
mod sieve;
mod trade;
mod triage;
mod types;

use std::sync::Arc;

use tokio::sync::broadcast;

use crate::config::Config;
use crate::db::Db;
use crate::error::{anyhow, AppResult, Context};
use crate::onboarding::OnboardingService;
use crate::pipeline::{MarketCache, Pipeline, PipelineEvent};
use crate::trade::TradeExecutor;
use crate::types::MarketStatus;

#[tokio::main]
async fn main() -> AppResult<()> {
    // Every TLS stack in this binary (serenity, reqwest, tokio-tungstenite)
    // uses the rustls `ring` provider. Pin it here, before any handshake, so a
    // second rustls default cannot be chosen at runtime.
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|error| anyhow!("installing the default rustls crypto provider: {error:?}"))?;

    init_tracing();

    let config = Arc::new(Config::from_env()?);
    let db = Db::open(&config.db_path)?;

    let registry = Arc::new(sieve::EngineRegistry::new());
    for market in db.list_markets()? {
        if market.status != MarketStatus::Active {
            continue;
        }
        match sieve::compile(&market.sieve_source) {
            Ok(engine) => registry.register(market.id.clone(), engine),
            Err(error) => tracing::error!(
                market_id = %market.id,
                error = %error,
                "skipping market with an invalid Sieve rule"
            ),
        }
    }
    tracing::info!(
        markets = registry.len(),
        active = registry.market_ids().len(),
        "loaded Sieve engines"
    );

    let cache = Arc::new(MarketCache::load(&db)?);

    // Onboarding shares the registry; build it before the registry is moved
    // into the pipeline.
    let onboarding = Arc::new(OnboardingService::new(
        &config,
        db.clone(),
        Arc::clone(&registry),
        Arc::clone(&cache),
    )?);

    let pipeline = Arc::new(Pipeline::new(
        Arc::clone(&config),
        db.clone(),
        registry,
        Arc::clone(&cache),
    )?);
    let mut pipeline_events = pipeline.events().subscribe();
    // Subscribe before spawning the executor: the broadcast channel has no
    // history, so a subscriber created later would miss early signals.
    let trade_events = pipeline.events().subscribe();
    // Discord subscribes before either loop starts for the same reason: a
    // receiver created after the tasks are running may miss startup events.
    let discord_pipeline_events = pipeline.events().subscribe();

    // Resolving the bot identity here makes a bad API key a startup failure
    // instead of silently disabling trading.
    let executor = Arc::new(
        TradeExecutor::new(&config, db.clone(), Arc::clone(&cache))
            .await
            .context("initializing the trade executor")?,
    );
    tracing::info!(
        bot_user_id = executor.client().bot_user_id(),
        "Manifold client ready"
    );
    let discord_trade_events = executor.events().subscribe();

    let discord_state = discord::DiscordState {
        config: Arc::clone(&config),
        db: db.clone(),
        registry: Arc::clone(&pipeline.registry),
        cache: Arc::clone(&cache),
        pipeline: Arc::clone(&pipeline),
        onboarding,
        executor: Arc::clone(&executor),
    };

    let post_tx = ingest::channel();
    let post_rx = post_tx.subscribe();

    let pipeline_task = tokio::spawn({
        let pipeline = Arc::clone(&pipeline);
        async move {
            if let Err(error) = pipeline.run(post_rx).await {
                tracing::error!(error = %error, "pipeline stopped");
            }
        }
    });

    let trade_task = tokio::spawn({
        let executor = Arc::clone(&executor);
        async move {
            if let Err(error) = executor.run(trade_events).await {
                tracing::error!(error = %error, "trade executor stopped");
            }
        }
    });

    let ingest_task = tokio::spawn(async move {
        if let Err(error) = ingest::run((*config).clone(), db, post_tx).await {
            tracing::error!(error = %error, "ingest stopped");
        }
    });

    let discord_task = tokio::spawn(async move {
        if let Err(error) =
            discord::run(discord_state, discord_pipeline_events, discord_trade_events).await
        {
            tracing::error!(error = %error, "discord stopped");
        }
    });

    let event_task = tokio::spawn(async move {
        loop {
            match pipeline_events.recv().await {
                Ok(event) => log_pipeline_event(&event),
                Err(broadcast::error::RecvError::Lagged(dropped)) => {
                    tracing::warn!(dropped, "pipeline event consumer lagged");
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    tokio::signal::ctrl_c()
        .await
        .context("waiting for Ctrl-C")?;
    tracing::info!("shutting down");

    pipeline_task.abort();
    trade_task.abort();
    ingest_task.abort();
    event_task.abort();
    discord_task.abort();

    Ok(())
}

/// Configures `tracing` from `RUST_LOG`, defaulting to `info`.
fn init_tracing() {
    let filter = match tracing_subscriber::EnvFilter::try_from_default_env() {
        Ok(filter) => filter,
        Err(_) => tracing_subscriber::EnvFilter::new("info"),
    };
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

/// Prints pipeline events to stdout.
///
/// This is the Phase 2 milestone consumer: a material belief update is visible
/// on stdout. A later Discord consumer can subscribe to the same broadcast
/// channel without changing the pipeline.
fn log_pipeline_event(event: &PipelineEvent) {
    match event {
        PipelineEvent::Signal {
            market_id,
            question,
            post_uri,
            author_handle,
            author_did,
            text,
            direction,
            strength,
            rationale,
            material,
            impulse,
            p_before,
            p_after,
            p_market,
            is_vip,
            created_at,
        } => {
            if *material {
                tracing::info!(
                    market_id = %market_id,
                    question = %question,
                    post_uri = %post_uri,
                    author_handle = %author_handle,
                    author_did = %author_did,
                    post_text = %text,
                    direction = %direction,
                    strength = *strength,
                    rationale = %rationale,
                    impulse = *impulse,
                    p_before = *p_before,
                    p_after = *p_after,
                    p_market = *p_market,
                    is_vip = *is_vip,
                    created_at = *created_at,
                    "signal"
                );
            } else {
                tracing::debug!(
                    market_id = %market_id,
                    direction = %direction,
                    strength = *strength,
                    "non-material signal"
                );
            }
        }
        PipelineEvent::Error { market_id, message } => {
            tracing::error!(
                market_id = ?market_id,
                message = %message,
                "pipeline error"
            );
        }
    }
}
