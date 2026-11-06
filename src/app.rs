//! Composition root, broadcast subscriptions, and task lifetime.
//!
//! [`App::build`] constructs every shared `Arc` and service in dependency order.
//! [`App::spawn`] creates every broadcast receiver and then starts the long-running
//! tasks. [`Tasks::run`] waits for Ctrl-C and aborts them.
//!
//! # The rule this module exists to enforce
//!
//! `tokio::sync::broadcast` keeps no history: a receiver created after a sender
//! has published does not see that event. Every subscription must therefore exist
//! before the first task starts. `App::spawn` is the only production code that
//! subscribes to a broadcast channel, and it does so in one block before its
//! first spawn. A new channel adds its sender and its receiver in that same block.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::config::Config;
use crate::control::{ControlPlane, Onboarder};
use crate::db::Db;
use crate::discord::{self, DiscordState};
use crate::error::{AppResult, Context};
use crate::ingest;
use crate::manifold_ws;
use crate::market_store::MarketStore;
use crate::onboarding::OnboardingService;
use crate::pipeline::{Pipeline, PipelineEvent};
use crate::sieve::{self, EngineRegistry};
use crate::trade::TradeExecutor;
use crate::types::MarketStatus;

/// How often the INFO heartbeat reports throughput and active markets.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// The composed process state, built once in dependency order.
pub struct App {
    config: Arc<Config>,
    db: Db,
    store: Arc<MarketStore>,
    registry: Arc<EngineRegistry>,
    onboarding: Arc<OnboardingService>,
    pipeline: Arc<Pipeline>,
    executor: Arc<TradeExecutor>,
    control: Arc<ControlPlane>,
}

impl App {
    /// Builds every shared `Arc` and service, and loads the Sieve engines.
    ///
    /// Failure here aborts startup: a bad key, a missing database, or an
    /// unreachable Manifold must not leave a half-running process.
    pub async fn build(config: Config) -> AppResult<App> {
        let config = Arc::new(config);
        let db = Db::open(&config.db_path)?;
        let store = Arc::new(MarketStore::new(&db)?);

        let registry = Arc::new(EngineRegistry::new());
        for market in store.list()? {
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

        // Onboarding shares the registry; build it before the pipeline takes its
        // own clone.
        let onboarding = Arc::new(OnboardingService::new(
            &config,
            db.clone(),
            Arc::clone(&registry),
            Arc::clone(&store),
        )?);

        let pipeline = Arc::new(Pipeline::new(
            Arc::clone(&config),
            db.clone(),
            Arc::clone(&registry),
            Arc::clone(&store),
        )?);

        // Resolving the bot identity here makes a bad API key a startup failure
        // instead of silently disabling trading.
        let executor = Arc::new(
            TradeExecutor::new(&config, db.clone(), Arc::clone(&store))
                .await
                .context("initializing the trade executor")?,
        );
        tracing::info!(
            bot_user_id = executor.client().bot_user_id(),
            "Manifold client ready"
        );

        // The control plane owns every operator-driven lifecycle transition. The
        // `Arc` coercion keeps the dependency edge pointing at the service.
        let onboarder: Arc<dyn Onboarder> = onboarding.clone();
        let control = Arc::new(ControlPlane::new(
            db.clone(),
            Arc::clone(&store),
            Arc::clone(&registry),
            onboarder,
            executor.clone(),
        ));

        Ok(App {
            config,
            db,
            store,
            registry,
            onboarding,
            pipeline,
            executor,
            control,
        })
    }

    /// Subscribes every broadcast receiver, then starts the long-running tasks
    /// (six, plus the websocket client when `MANIFOLD_WS_ENABLED=1`).
    ///
    /// The two blocks are ordered on purpose and must stay in this function: see
    /// the module doc.
    pub fn spawn(self) -> Tasks {
        // `registry` and `onboarding` are the composition's retained references,
        // not runtime state: `Pipeline` and `ControlPlane` already hold their own
        // clones, so dropping these two handles here is unobservable.
        let App {
            config,
            db,
            store,
            pipeline,
            executor,
            control,
            ..
        } = self;

        // Every receiver is created here, before the first task below. `broadcast`
        // keeps no history, so a subscription made after a producer starts would
        // silently miss the events it already published. Keep this block whole and
        // keep its order. The manifold event channel is an mpsc (never-drop),
        // created here so the receiver exists before both the socket task and the
        // trade executor are spawned.
        let mut pipeline_events = pipeline.events().subscribe();
        let trade_events = pipeline.events().subscribe();
        let discord_pipeline_events = pipeline.events().subscribe();
        let discord_trade_events = executor.events().subscribe();
        let post_tx = ingest::channel();
        let post_rx = post_tx.subscribe();
        let (manifold_event_tx, manifold_event_rx) = manifold_ws::channel();

        let discord_state = DiscordState {
            config: Arc::clone(&config),
            db: db.clone(),
            store: Arc::clone(&store),
            pipeline: Arc::clone(&pipeline),
            control,
        };

        let pipeline_task = tokio::spawn({
            let pipeline = Arc::clone(&pipeline);
            async move {
                if let Err(error) = pipeline.run(post_rx).await {
                    tracing::error!(error = %error, "pipeline stopped");
                }
            }
        });

        let heartbeat_task = tokio::spawn({
            let pipeline = Arc::clone(&pipeline);
            let store = Arc::clone(&store);
            async move {
                let mut ticker = tokio::time::interval(HEARTBEAT_INTERVAL);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                // The first tick completes immediately; skip it so the first line
                // reports a full interval of throughput.
                ticker.tick().await;
                let mut last_count = pipeline.processed_posts();
                let mut last_at = Instant::now();
                loop {
                    ticker.tick().await;
                    let now = Instant::now();
                    let count = pipeline.processed_posts();
                    let elapsed = now.saturating_duration_since(last_at).as_secs_f64();
                    let per_sec = if elapsed > 0.0 {
                        count.saturating_sub(last_count) as f64 / elapsed
                    } else {
                        0.0
                    };
                    tracing::info!(
                        messages_per_sec = %format!("{per_sec:.1}"),
                        messages_processed = count,
                        active_markets = store.active_count(),
                        "heartbeat"
                    );
                    last_count = count;
                    last_at = now;
                }
            }
        });

        let trade_task = tokio::spawn({
            let executor = Arc::clone(&executor);
            async move {
                if let Err(error) = executor.run(trade_events, manifold_event_rx).await {
                    tracing::error!(error = %error, "trade executor stopped");
                }
            }
        });

        // The websocket client is part of the live-price path; the legacy
        // polling loop (`MANIFOLD_WS_ENABLED=0`) runs without it and the
        // unconsumed sender just drops.
        let manifold_ws_task = if config.manifold_ws_enabled {
            Some(tokio::spawn({
                let config = (*config).clone();
                async move {
                    if let Err(error) = manifold_ws::run(config, manifold_event_tx).await {
                        tracing::error!(error = %error, "manifold websocket stopped");
                    }
                }
            }))
        } else {
            drop(manifold_event_tx);
            None
        };

        let ingest_task = tokio::spawn(async move {
            if let Err(error) = ingest::run((*config).clone(), post_tx).await {
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

        Tasks {
            pipeline: pipeline_task,
            heartbeat: heartbeat_task,
            trade: trade_task,
            ingest: ingest_task,
            discord: discord_task,
            event: event_task,
            manifold_ws: manifold_ws_task,
        }
    }
}

/// The long-running tasks this process owns.
pub struct Tasks {
    pipeline: JoinHandle<()>,
    heartbeat: JoinHandle<()>,
    trade: JoinHandle<()>,
    ingest: JoinHandle<()>,
    discord: JoinHandle<()>,
    event: JoinHandle<()>,
    /// The websocket client task, absent while `MANIFOLD_WS_ENABLED=0` runs
    /// the legacy polling loop.
    manifold_ws: Option<JoinHandle<()>>,
}

impl Tasks {
    /// Runs until Ctrl-C, then aborts every task and returns.
    ///
    /// This is not a graceful shutdown: no handle is awaited, and in-flight work
    /// is discarded, exactly as before.
    pub async fn run(self) -> AppResult<()> {
        tokio::signal::ctrl_c()
            .await
            .context("waiting for Ctrl-C")?;
        tracing::info!("shutting down");

        self.pipeline.abort();
        self.heartbeat.abort();
        self.trade.abort();
        self.ingest.abort();
        self.event.abort();
        self.discord.abort();
        if let Some(manifold_ws) = self.manifold_ws {
            manifold_ws.abort();
        }

        Ok(())
    }
}

/// Prints pipeline events to stdout.
///
/// This is the Phase 2 milestone consumer: a material belief update is visible
/// on stdout. A later Discord consumer can subscribe to the same broadcast
/// channel without changing the pipeline.
fn log_pipeline_event(event: &PipelineEvent) {
    match event {
        PipelineEvent::PostMatched {
            market_id,
            post_uri,
            author_handle,
        } => {
            tracing::info!(
                market_id = %market_id,
                author_handle = %author_handle,
                post_uri = %post_uri,
                "post matched filter, sending to triage"
            );
        }
        PipelineEvent::Signal {
            market_id,
            question,
            post_uri,
            author_handle,
            author_did,
            text,
            answer,
            direction,
            p_evidence,
            p_target,
            strength,
            p_priced,
            material,
            impulse,
            p_before,
            p_after,
            p_market,
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
                    answer = ?answer,
                    direction = %direction,
                    p_evidence = *p_evidence,
                    p_target = *p_target,
                    strength = *strength,
                    p_priced = *p_priced,
                    impulse = *impulse,
                    p_before = *p_before,
                    p_after = *p_after,
                    p_market = *p_market,
                    created_at = *created_at,
                    "signal"
                );
            } else {
                tracing::debug!(
                    market_id = %market_id,
                    direction = %direction,
                    p_evidence = *p_evidence,
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
