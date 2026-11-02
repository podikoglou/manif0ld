//! Discord control plane: slash commands, autocomplete, and event embeds.
//!
//! The bot is a thin rendering and control layer. It never performs trading or
//! triage itself: commands call into [`OnboardingService`], the database, the
//! market cache, and [`TradeExecutor`], while background consumers render
//! [`PipelineEvent`] and [`TradeEvent`] messages into Discord threads.
//!
//! # Scope
//!
//! This module is the foundation from the control-plane milestone. Command
//! registration, operator gating, autocomplete plumbing, `/status`, and the
//! signal/trade/error consumers are complete. The remaining mutating commands
//! and the action buttons answer with an explicit "Not implemented yet"
//! response rather than being silently ignored, so an operator never mistakes a
//! missing feature for a successful action.
//!
//! # Authorization
//!
//! Every command and component interaction is checked against
//! `DISCORD_OPERATOR_USER_ID`. Unauthorized users receive an ephemeral
//! "Not authorized." message. Autocomplete only exposes market questions that
//! the operator already configured, so it is intentionally left ungated.
//!
//! # Locking
//!
//! The database handle and market cache use synchronous locks. Every handler
//! finishes its synchronous reads and drops the guard before the next `.await`;
//! no lock is held across a Discord request.

use std::collections::HashSet;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serenity::all::*;
use serenity::http::Http;
use tokio::sync::broadcast;

use crate::belief;
use crate::config::Config;
use crate::db::Db;
use crate::error::{bail, AppResult, Context as AnyhowContext};
use crate::onboarding::OnboardingService;
use crate::pipeline::{MarketCache, Pipeline, PipelineEvent};
use crate::sieve::EngineRegistry;
use crate::trade::{TradeEvent, TradeExecutor};
use crate::types::{available_budget, SignalRecord, TrackedMarket};

/// Maximum number of autocomplete choices Discord accepts.
const MAX_AUTOCOMPLETE_CHOICES: usize = 25;

/// Discord's limit for an autocomplete choice name.
const MAX_CHOICE_NAME_CHARS: usize = 100;

/// Discord's limit for an embed title.
const MAX_TITLE_CHARS: usize = 256;

/// Discord's limit for an embed field value and error description.
const MAX_FIELD_CHARS: usize = 1024;

/// Discord's limit for an embed description.
const MAX_DESCRIPTION_CHARS: usize = 4096;

/// Maximum number of market fields in the overview embed.
const MAX_STATUS_FIELDS: usize = 25;

/// Characters per Sieve source field.
const SIEVE_CHUNK_CHARS: usize = 1000;

/// Maximum Sieve source fields kept in a status embed.
///
/// The whole embed is capped at 6000 characters, so an arbitrarily long rule
/// cannot be shown in full. The last field notes how much was omitted.
const MAX_SIEVE_CHUNKS: usize = 3;

/// Number of recent signals shown for one market.
const MAX_SIGNAL_LINES: usize = 10;

/// Runtime dependencies shared by the gateway handler and event consumers.
///
/// `Db` is itself a cheap cloneable handle; every other dependency is behind an
/// `Arc` so the state can be cloned into each spawned consumer.
#[derive(Clone)]
pub struct DiscordState {
    pub config: Arc<Config>,
    pub db: Db,
    pub registry: Arc<EngineRegistry>,
    pub cache: Arc<MarketCache>,
    pub pipeline: Arc<Pipeline>,
    pub onboarding: Arc<OnboardingService>,
    pub executor: Arc<TradeExecutor>,
}

/// Builds the HTTP client, spawns the event consumers, and runs the gateway.
///
/// The consumers are spawned before the gateway connects so events published
/// during startup are still observed. `run` only returns when the gateway stops
/// or fails; the caller treats that as a fatal task exit.
pub async fn run(
    state: DiscordState,
    pipeline_events: broadcast::Receiver<PipelineEvent>,
    trade_events: broadcast::Receiver<TradeEvent>,
) -> AppResult<()> {
    let http = Arc::new(Http::new(&state.config.discord_bot_token));

    spawn_pipeline_consumer(
        Arc::clone(&http),
        state.db.clone(),
        Arc::clone(&state.config),
        pipeline_events,
    );
    spawn_trade_consumer(
        Arc::clone(&http),
        state.db.clone(),
        Arc::clone(&state.config),
        trade_events,
    );

    let mut client = Client::builder(&state.config.discord_bot_token, GatewayIntents::GUILDS)
        .event_handler(Handler { state })
        .await
        .context("building the Discord client")?;

    client.start().await.context("running the Discord gateway")
}

/// Serenity event handler.
struct Handler {
    state: DiscordState,
}

#[serenity::async_trait]
impl EventHandler for Handler {
    /// Registers the guild commands once the gateway is ready.
    ///
    /// Registration failure is logged and swallowed: the bot keeps running, but
    /// no operator command is available until the next reconnect.
    async fn ready(&self, ctx: Context, ready: Ready) {
        let guild = GuildId::new(self.state.config.discord_guild_id);
        match guild.set_commands(&ctx.http, command_definitions()).await {
            Ok(commands) => tracing::info!(
                guild_id = %guild,
                user = %ready.user.name,
                count = commands.len(),
                "registered Discord guild commands"
            ),
            Err(error) => tracing::error!(
                guild_id = %guild,
                error = %error,
                "failed to register Discord guild commands"
            ),
        }
    }

    /// Dispatches autocomplete, command, and component interactions.
    async fn interaction_create(&self, ctx: Context, interaction: Interaction) {
        match interaction {
            Interaction::Autocomplete(interaction) => {
                autocomplete(&self.state, &ctx, &interaction).await;
            }
            Interaction::Command(interaction) => {
                if !is_operator(&self.state, interaction.user.id) {
                    respond_ephemeral(&ctx, &interaction, "Not authorized.").await;
                    return;
                }

                match interaction.data.name.as_str() {
                    "status" => status(&self.state, &ctx, &interaction).await,
                    name => {
                        tracing::debug!(command = %name, "command is not implemented yet");
                        respond_ephemeral(
                            &ctx,
                            &interaction,
                            &format!("/{name} is not implemented yet."),
                        )
                        .await;
                    }
                }
            }
            Interaction::Component(interaction) => {
                if !is_operator(&self.state, interaction.user.id) {
                    respond_component_ephemeral(&ctx, &interaction, "Not authorized.").await;
                    return;
                }

                let custom_id = interaction.data.custom_id.as_str();
                tracing::info!(custom_id = %custom_id, "component interaction");
                respond_component_ephemeral(&ctx, &interaction, "Not implemented yet.").await;
            }
            _ => {}
        }
    }
}

/// Whether `user_id` is the configured operator.
fn is_operator(state: &DiscordState, user_id: UserId) -> bool {
    user_id.get() == state.config.discord_operator_user_id
}

// -----------------------------------------------------------------------------
// Command definitions
// -----------------------------------------------------------------------------

/// The seven guild commands from the specification.
///
/// Every command is guild-only and carries its full parameter list so the
/// Discord client performs type and range validation before the handler runs.
fn command_definitions() -> Vec<CreateCommand> {
    vec![
        CreateCommand::new("track")
            .description("Track a Manifold market and start listening for posts.")
            .dm_permission(false)
            .add_option(
                CreateCommandOption::new(CommandOptionType::String, "url", "Manifold market URL")
                    .required(true),
            )
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::Number,
                    "budget",
                    "Mana budget for this market (default from configuration)",
                )
                .required(false),
            )
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::Integer,
                    "initial_prob",
                    "Initial internal probability, 1-99",
                )
                .required(false)
                .min_int_value(1)
                .max_int_value(99),
            ),
        CreateCommand::new("untrack")
            .description("Stop tracking a market and optionally liquidate it.")
            .dm_permission(false)
            .add_option(market_option("Market to untrack", true))
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::Boolean,
                    "liquidate",
                    "Sell all shares before closing (default true)",
                )
                .required(false),
            ),
        CreateCommand::new("budget")
            .description("Change a market's mana budget.")
            .dm_permission(false)
            .add_option(market_option("Market to update", true))
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::Number,
                    "new_budget",
                    "New mana budget",
                )
                .required(true),
            ),
        CreateCommand::new("status")
            .description("Show all tracked markets or one market in detail.")
            .dm_permission(false)
            .add_option(market_option("Market to inspect (optional)", false)),
        CreateCommand::new("override")
            .description("Set a market's internal probability.")
            .dm_permission(false)
            .add_option(market_option("Market to override", true))
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::Integer,
                    "probability",
                    "New internal probability, 1-99",
                )
                .required(true)
                .min_int_value(1)
                .max_int_value(99),
            ),
        CreateCommand::new("vip")
            .description("Add a Bluesky author as a VIP for a market.")
            .dm_permission(false)
            .add_option(market_option("Market to update", true))
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::String,
                    "handle",
                    "Bluesky handle, for example alice.bsky.social",
                )
                .required(true),
            ),
        CreateCommand::new("panic")
            .description("Toggle the global buy kill switch.")
            .dm_permission(false)
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::Boolean,
                    "liquidate_all",
                    "Sell every position after enabling the kill switch",
                )
                .required(false),
            ),
    ]
}

/// Builds the repeated `market` option, optionally with autocomplete.
fn market_option(description: &str, required: bool) -> CreateCommandOption {
    CreateCommandOption::new(CommandOptionType::String, "market", description)
        .required(required)
        .set_autocomplete(true)
}

// -----------------------------------------------------------------------------
// Autocomplete
// -----------------------------------------------------------------------------

/// Answers the focused option with matching tracked markets.
///
/// The active cache is queried first for freshness, then every database market
/// is added so paused and closed markets stay addressable by commands such as
/// `/untrack`.
async fn autocomplete(state: &DiscordState, ctx: &Context, interaction: &CommandInteraction) {
    let query = interaction
        .data
        .autocomplete()
        .map(|option| option.value.trim().to_owned())
        .unwrap_or_default();

    let mut markets = state.cache.active_markets();
    match state.db.list_markets() {
        Ok(all) => markets.extend(all),
        Err(error) => tracing::warn!(error = %error, "autocomplete could not list all markets"),
    }

    let mut response = CreateAutocompleteResponse::new();
    for (name, value) in market_choices(&markets, &query) {
        response = response.add_string_choice(name, value);
    }

    if let Err(error) = interaction
        .create_response(&ctx.http, CreateInteractionResponse::Autocomplete(response))
        .await
    {
        tracing::warn!(error = %error, "failed to answer autocomplete");
    }
}

/// Filters markets by a case-insensitive substring and caps the result.
///
/// Matching covers the question, market ID, and slug. Duplicate IDs are removed
/// because callers concatenate the cache and database snapshots. Each choice
/// name has the form `"{question} ({id})"` and is truncated to Discord's limit.
pub(crate) fn market_choices(markets: &[TrackedMarket], query: &str) -> Vec<(String, String)> {
    let needle = query.trim().to_lowercase();
    let mut seen = HashSet::new();
    let mut choices = Vec::new();

    for market in markets {
        if !seen.insert(market.id.clone()) {
            continue;
        }
        if !needle.is_empty() {
            let matches = market.question.to_lowercase().contains(&needle)
                || market.id.to_lowercase().contains(&needle)
                || market.slug.to_lowercase().contains(&needle);
            if !matches {
                continue;
            }
        }

        let name = truncate_chars(
            &format!("{} ({})", market.question, market.id),
            MAX_CHOICE_NAME_CHARS,
        );
        choices.push((name, market.id.clone()));
        if choices.len() >= MAX_AUTOCOMPLETE_CHOICES {
            break;
        }
    }

    choices
}

// -----------------------------------------------------------------------------
// /status
// -----------------------------------------------------------------------------

/// Runs `/status`, deferring first so database and cache reads fit the
/// interaction deadline.
async fn status(state: &DiscordState, ctx: &Context, interaction: &CommandInteraction) {
    if let Err(error) = interaction.defer_ephemeral(&ctx.http).await {
        tracing::error!(error = %error, "failed to defer /status");
        return;
    }

    let query = string_option(interaction, "market")
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());

    let result = match query.as_deref() {
        Some(query) => status_for_market(state, query),
        None => status_overview(state),
    };

    let response = match result {
        Ok(embed) => CreateInteractionResponseFollowup::new().embed(embed),
        Err(error) => {
            tracing::warn!(error = %error, "failed to build /status response");
            CreateInteractionResponseFollowup::new()
                .content(truncate_chars(
                    &format!("Failed to load status: {error}"),
                    2000,
                ))
                .ephemeral(true)
        }
    };

    if let Err(error) = interaction.create_followup(&ctx.http, response).await {
        tracing::error!(error = %error, "failed to send /status response");
    }
}

/// Builds the multi-market overview embed.
fn status_overview(state: &DiscordState) -> AppResult<CreateEmbed> {
    let markets = state.db.list_markets()?;
    let mut embed = CreateEmbed::new().title("Tracked markets");

    if markets.is_empty() {
        return Ok(embed
            .description("No markets are tracked. Use `/track url:<manifold-url>` to add one."));
    }

    let now = Utc::now().timestamp();
    for market in markets.iter().take(MAX_STATUS_FIELDS) {
        let p_internal = internal_probability(state, market, now);
        let queue_depth = state.pipeline.queue_depth(&market.id);
        let last_signal = match state.db.latest_signal_time(&market.id) {
            Ok(Some(timestamp)) => discord_timestamp(timestamp),
            Ok(None) => "never".to_owned(),
            Err(error) => {
                tracing::warn!(
                    market_id = %market.id,
                    error = %error,
                    "failed to read the latest signal time"
                );
                "unknown".to_owned()
            }
        };

        let value = format!(
            "Budget {:.2} | Invested {:.2} | Realized PnL {:+.2} | Available {:.2}\n\
             P_int {:.3} vs P_mkt {:.3} | {}\n\
             Queue {} | Last signal {}",
            market.budget_mana,
            market.invested_mana,
            market.realized_pnl_mana,
            available_budget(market),
            p_internal,
            market.p_market_last,
            market.status,
            queue_depth,
            last_signal,
        );

        embed = embed.field(
            truncate_chars(&market.question, MAX_TITLE_CHARS),
            truncate_chars(&value, MAX_FIELD_CHARS),
            false,
        );
    }

    if markets.len() > MAX_STATUS_FIELDS {
        embed = embed.description(format!(
            "Showing the first {MAX_STATUS_FIELDS} of {} markets. Use `/status market:<id>` \
             for the rest.",
            markets.len()
        ));
    }

    Ok(embed)
}

/// Builds the full state embed for one market, matched by ID or unique slug.
fn status_for_market(state: &DiscordState, query: &str) -> AppResult<CreateEmbed> {
    let markets = state.db.list_markets()?;
    let Some(market) = resolve_market(&markets, query) else {
        bail!("no unique tracked market matches {query:?}");
    };

    let now = Utc::now().timestamp();
    let p_internal = internal_probability(state, market, now);
    let thread = if market.discord_thread_id.trim().is_empty() {
        "(none)".to_owned()
    } else {
        format!("<#{}>", market.discord_thread_id)
    };

    let mut embed = CreateEmbed::new()
        .title(truncate_chars(
            &format!("📈 {}", market.question),
            MAX_TITLE_CHARS,
        ))
        .field("ID", &market.id, true)
        .field("Slug", &market.slug, true)
        .field("Status", market.status.as_str(), true)
        .field(
            "Budget",
            format!(
                "Budget {:.2}\nInvested {:.2}\nRealized PnL {:+.2}\nAvailable {:.2}",
                market.budget_mana,
                market.invested_mana,
                market.realized_pnl_mana,
                available_budget(market),
            ),
            true,
        )
        .field(
            "Position",
            format!(
                "YES shares {:.4}\nNO shares {:.4}",
                market.yes_shares, market.no_shares
            ),
            true,
        )
        .field(
            "Probability",
            format!("P_int {p_internal:.3}\nP_mkt {:.3}", market.p_market_last),
            true,
        )
        .field("Thread", truncate_chars(&thread, MAX_FIELD_CHARS), false);

    // Sieve source is potentially large; split it into bounded fields rather
    // than letting the embed request fail validation.
    let chunks = chunk_chars(&market.sieve_source, SIEVE_CHUNK_CHARS);
    let shown = chunks.len().min(MAX_SIEVE_CHUNKS);
    let omitted = chunks.len().saturating_sub(shown);
    for (index, chunk) in chunks.iter().take(shown).enumerate() {
        let name = if index == 0 {
            "Sieve source".to_owned()
        } else {
            format!("Sieve source ({})", index + 1)
        };
        let mut value = chunk.clone();
        if index + 1 == shown && omitted > 0 {
            value.push_str(&format!(
                "\n… {omitted} more field(s) truncated; see the database for the full rule."
            ));
        }
        embed = embed.field(name, truncate_chars(&value, MAX_FIELD_CHARS), false);
    }

    let signals = state.db.recent_signals(&market.id, MAX_SIGNAL_LINES)?;
    if signals.is_empty() {
        embed = embed.field("Recent signals", "none", false);
    } else {
        let lines = signals
            .iter()
            .map(signal_line)
            .collect::<Vec<_>>()
            .join("\n");
        embed = embed.field(
            "Recent signals",
            truncate_chars(&lines, MAX_FIELD_CHARS),
            false,
        );
    }

    Ok(embed)
}

/// Decayed internal probability for one market.
fn internal_probability(state: &DiscordState, market: &TrackedMarket, now: i64) -> f64 {
    belief::current_p_internal(
        market.p_internal_log_odds,
        market.belief_updated_at,
        market.p_market_last,
        now,
        state.config.belief_halflife_hours,
    )
}

/// Resolves an operator-provided market reference.
///
/// The market ID or slug must match case-insensitively. A unique slug prefix is
/// accepted as a convenience. Ambiguous or missing references resolve to
/// `None` rather than guessing.
pub(crate) fn resolve_market<'a>(
    markets: &'a [TrackedMarket],
    query: &str,
) -> Option<&'a TrackedMarket> {
    let needle = query.trim().to_lowercase();
    if needle.is_empty() {
        return None;
    }

    let exact: Vec<&TrackedMarket> = markets
        .iter()
        .filter(|market| market.id.to_lowercase() == needle || market.slug.to_lowercase() == needle)
        .collect();
    if exact.len() == 1 {
        return exact.first().copied();
    }
    if !exact.is_empty() {
        return None;
    }

    let prefixes: Vec<&TrackedMarket> = markets
        .iter()
        .filter(|market| market.slug.to_lowercase().starts_with(&needle))
        .collect();
    if prefixes.len() == 1 {
        return prefixes.first().copied();
    }

    None
}

/// One compact line for the recent-signal history.
fn signal_line(signal: &SignalRecord) -> String {
    format!(
        "{} {} s{} {} @{} p_after {:.3}",
        discord_timestamp(signal.created_at),
        signal.direction.as_str(),
        signal.strength,
        if signal.material { "material" } else { "noise" },
        truncate_chars(&signal.author_handle, 40),
        signal.p_after,
    )
}

/// Formats a Unix timestamp as a Discord relative timestamp.
fn discord_timestamp(timestamp: i64) -> String {
    format!("<t:{timestamp}:R>")
}

// -----------------------------------------------------------------------------
// Interaction responses
// -----------------------------------------------------------------------------

/// Reads a string option by name, accepting normal and autocomplete values.
fn string_option(interaction: &CommandInteraction, name: &str) -> Option<String> {
    interaction
        .data
        .options
        .iter()
        .find(|option| option.name == name)
        .and_then(|option| match &option.value {
            CommandDataOptionValue::String(value)
            | CommandDataOptionValue::Autocomplete { value, .. } => Some(value.clone()),
            _ => None,
        })
}

/// Sends an ephemeral response for a command interaction.
async fn respond_ephemeral(ctx: &Context, interaction: &CommandInteraction, content: &str) {
    let response = CreateInteractionResponse::Message(
        CreateInteractionResponseMessage::new()
            .content(content)
            .ephemeral(true),
    );
    if let Err(error) = interaction.create_response(&ctx.http, response).await {
        tracing::error!(error = %error, "failed to send an ephemeral response");
    }
}

/// Sends an ephemeral response for a component interaction.
async fn respond_component_ephemeral(
    ctx: &Context,
    interaction: &ComponentInteraction,
    content: &str,
) {
    let response = CreateInteractionResponse::Message(
        CreateInteractionResponseMessage::new()
            .content(content)
            .ephemeral(true),
    );
    if let Err(error) = interaction.create_response(&ctx.http, response).await {
        tracing::error!(error = %error, "failed to send an ephemeral component response");
    }
}

// -----------------------------------------------------------------------------
// Event consumers
// -----------------------------------------------------------------------------

/// Spawns the pipeline-event renderer.
fn spawn_pipeline_consumer(
    http: Arc<Http>,
    db: Db,
    config: Arc<Config>,
    events: broadcast::Receiver<PipelineEvent>,
) {
    tokio::spawn(async move {
        consume_pipeline_events(http, db, config, events).await;
    });
}

/// Spawns the trade-event renderer.
fn spawn_trade_consumer(
    http: Arc<Http>,
    db: Db,
    config: Arc<Config>,
    events: broadcast::Receiver<TradeEvent>,
) {
    tokio::spawn(async move {
        consume_trade_events(http, db, config, events).await;
    });
}

/// Renders pipeline signals and errors into Discord messages.
async fn consume_pipeline_events(
    http: Arc<Http>,
    db: Db,
    config: Arc<Config>,
    mut events: broadcast::Receiver<PipelineEvent>,
) {
    loop {
        match events.recv().await {
            Ok(PipelineEvent::Signal {
                material: true,
                market_id,
                question,
                post_uri,
                author_handle,
                author_did,
                text,
                direction,
                strength,
                rationale,
                p_before,
                p_after,
                p_market,
                is_vip,
                ..
            }) => {
                let channel = channel_for_market(&db, &config, Some(&market_id));

                let mut embed = CreateEmbed::new()
                    .title(truncate_chars(&format!("📡 {question}"), MAX_TITLE_CHARS))
                    .description(truncate_chars(&text, MAX_DESCRIPTION_CHARS))
                    .field(
                        "Author",
                        truncate_chars(
                            &format!("@{author_handle} ({author_did})"),
                            MAX_FIELD_CHARS,
                        ),
                        false,
                    )
                    .field(
                        "Direction",
                        format!("{} (strength {strength})", direction.as_str()),
                        true,
                    )
                    .field(
                        "Rationale",
                        truncate_chars(&rationale, MAX_FIELD_CHARS),
                        false,
                    )
                    .field("P_internal", format!("{p_before:.3} → {p_after:.3}"), true)
                    .field("P_market", format!("{p_market:.3}"), true)
                    .field("VIP", if is_vip { "yes" } else { "no" }, true);

                match bsky_post_url(&post_uri) {
                    Some(url) => {
                        embed = embed.url(&url).field(
                            "Post",
                            format!("[Open on Bluesky]({url})"),
                            false,
                        );
                    }
                    None => {
                        embed =
                            embed.field("Post", truncate_chars(&post_uri, MAX_FIELD_CHARS), false);
                    }
                }

                send_embed(&http, channel, embed).await;
            }
            Ok(PipelineEvent::Signal {
                material: false,
                market_id,
                direction,
                strength,
                ..
            }) => {
                tracing::debug!(
                    market_id = %market_id,
                    direction = %direction,
                    strength,
                    "non-material signal not posted to Discord"
                );
            }
            Ok(PipelineEvent::Error { market_id, message }) => {
                tracing::error!(market_id = ?market_id, message = %message, "pipeline error");
                let channel = channel_for_market(&db, &config, market_id.as_deref());
                let mut embed = CreateEmbed::new()
                    .title("❌ [ERROR]")
                    .colour(Colour::RED)
                    .description(truncate_chars(&message, MAX_DESCRIPTION_CHARS));
                if let Some(market_id) = market_id {
                    embed = embed.field("Market", market_id, false);
                }
                send_embed(&http, channel, embed).await;
            }
            Err(broadcast::error::RecvError::Lagged(dropped)) => {
                tracing::warn!(dropped, "Discord pipeline consumer lagged");
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}

/// Renders trades and trade-loop errors into Discord messages.
async fn consume_trade_events(
    http: Arc<Http>,
    db: Db,
    config: Arc<Config>,
    mut events: broadcast::Receiver<TradeEvent>,
) {
    loop {
        match events.recv().await {
            Ok(TradeEvent::Trade {
                market_id,
                action,
                mana_amount,
                shares_delta,
                p_internal,
                p_market,
                available_budget,
                bet_id,
            }) => {
                let channel = channel_for_market(&db, &config, Some(&market_id));
                let embed = CreateEmbed::new()
                    .title(format!("💸 {action}"))
                    .field("Market", &market_id, false)
                    .field("Mana amount", format!("{mana_amount:.4}"), true)
                    .field("Shares delta", format!("{shares_delta:+.4}"), true)
                    .field(
                        "P_int vs P_mkt",
                        format!("{p_internal:.3} vs {p_market:.3}"),
                        true,
                    )
                    .field("Available budget", format!("{available_budget:.4}"), true)
                    .field(
                        "Bet ID",
                        bet_id.unwrap_or_else(|| "(not returned)".to_owned()),
                        true,
                    );
                send_embed(&http, channel, embed).await;
            }
            Ok(TradeEvent::Error { market_id, message }) => {
                tracing::error!(market_id = ?market_id, message = %message, "trade error");
                let channel = channel_for_market(&db, &config, market_id.as_deref());
                let mut embed = CreateEmbed::new()
                    .title("❌ [ERROR]")
                    .colour(Colour::RED)
                    .description(truncate_chars(&message, MAX_DESCRIPTION_CHARS));
                if let Some(market_id) = market_id {
                    embed = embed.field("Market", market_id, false);
                }
                send_embed(&http, channel, embed).await;
            }
            Err(broadcast::error::RecvError::Lagged(dropped)) => {
                tracing::warn!(dropped, "Discord trade consumer lagged");
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}

/// Chooses the market thread when it is a valid channel ID, otherwise the
/// control channel.
fn channel_for_market(db: &Db, config: &Config, market_id: Option<&str>) -> ChannelId {
    let thread = market_id
        .and_then(|id| match db.get_market(id) {
            Ok(market) => market,
            Err(error) => {
                tracing::warn!(
                    market_id = %id,
                    error = %error,
                    "failed to look up a market thread"
                );
                None
            }
        })
        .and_then(|market| market.discord_thread_id.trim().parse::<u64>().ok())
        .map(ChannelId::new);

    thread.unwrap_or_else(|| ChannelId::new(config.discord_control_channel_id))
}

/// Sends one embed, logging failures instead of propagating them.
async fn send_embed(http: &Http, channel: ChannelId, embed: CreateEmbed) {
    if let Err(error) = channel
        .send_message(http, CreateMessage::new().embed(embed))
        .await
    {
        tracing::error!(
            channel_id = channel.get(),
            error = %error,
            "failed to send a Discord message"
        );
    }
}

// -----------------------------------------------------------------------------
// Pure helpers
// -----------------------------------------------------------------------------

/// Converts a Bluesky AT URI into a public post URL.
///
/// Only `at://{did}/app.bsky.feed.post/{rkey}` is accepted. The DID and record
/// key must be non-empty and no extra path segments may be present.
pub(crate) fn bsky_post_url(uri: &str) -> Option<String> {
    let rest = uri.strip_prefix("at://")?;
    let mut parts = rest.split('/');
    let did = parts.next().filter(|part| !part.is_empty())?;
    let collection = parts.next()?;
    let rkey = parts.next().filter(|part| !part.is_empty())?;

    if collection != "app.bsky.feed.post" || parts.next().is_some() {
        return None;
    }

    Some(format!("https://bsky.app/profile/{did}/post/{rkey}"))
}

/// Truncates to at most `max` characters, appending `…` when content is lost.
///
/// The result never exceeds `max` characters and never splits a UTF-8 character.
pub(crate) fn truncate_chars(input: &str, max: usize) -> String {
    if max == 0 {
        return String::new();
    }
    if input.chars().count() <= max {
        return input.to_owned();
    }

    let mut truncated: String = input.chars().take(max - 1).collect();
    truncated.push('…');
    truncated
}

/// Splits text into chunks of at most `size` characters.
///
/// Used for embed fields, where a single field cannot exceed Discord's limit.
fn chunk_chars(input: &str, size: usize) -> Vec<String> {
    if size == 0 {
        return Vec::new();
    }

    let mut chunks = Vec::new();
    let mut current = String::new();
    let mut count = 0;

    for character in input.chars() {
        if count == size {
            chunks.push(std::mem::take(&mut current));
            count = 0;
        }
        current.push(character);
        count += 1;
    }
    if !current.is_empty() {
        chunks.push(current);
    }

    if chunks.is_empty() {
        chunks.push(String::new());
    }
    chunks
}

/// Builds a component custom ID in `action:market_id` form.
pub(crate) fn button_custom_id(action: &str, market_id: &str) -> String {
    format!("{action}:{market_id}")
}

/// Parses `action:market_id`, splitting only on the first colon.
///
/// Market IDs contain no colon, but splitting once keeps a malformed action
/// from discarding the rest of the custom ID.
pub(crate) fn parse_button_custom_id(id: &str) -> Option<(String, String)> {
    let (action, market_id) = id.split_once(':')?;
    if action.is_empty() || market_id.is_empty() {
        return None;
    }
    Some((action.to_owned(), market_id.to_owned()))
}

/// Formats a Unix timestamp for logs and non-Discord output.
///
/// Kept separate from [`discord_timestamp`] so callers can render absolute time
/// when a relative timestamp would be ambiguous.
#[allow(dead_code)]
fn format_time(timestamp: i64) -> String {
    DateTime::<Utc>::from_timestamp(timestamp, 0)
        .map(|time| time.format("%Y-%m-%d %H:%M UTC").to_string())
        .unwrap_or_else(|| timestamp.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::MarketStatus;

    fn market(id: &str, question: &str, slug: &str) -> TrackedMarket {
        TrackedMarket {
            id: id.to_owned(),
            slug: slug.to_owned(),
            question: question.to_owned(),
            description: String::new(),
            resolution_criteria: String::new(),
            close_time: None,
            status: MarketStatus::Active,
            budget_mana: 100.0,
            invested_mana: 0.0,
            realized_pnl_mana: 0.0,
            yes_shares: 0.0,
            no_shares: 0.0,
            p_internal_log_odds: 0.0,
            p_market_last: 0.5,
            belief_updated_at: 0,
            sieve_source: String::new(),
            discord_thread_id: String::new(),
            created_at: 0,
        }
    }

    // -- Bluesky URLs -------------------------------------------------------

    #[test]
    fn builds_a_bluesky_post_url_from_an_at_uri() {
        assert_eq!(
            bsky_post_url("at://did:plc:abc123/app.bsky.feed.post/3kxyz"),
            Some("https://bsky.app/profile/did:plc:abc123/post/3kxyz".to_owned())
        );
    }

    #[test]
    fn rejects_malformed_or_unknown_at_uris() {
        for uri in [
            "",
            "https://bsky.app/profile/alice/post/3kxyz",
            "at://did:plc:abc123/app.bsky.feed.post",
            "at://did:plc:abc123/app.bsky.feed.like/3kxyz",
            "at:///app.bsky.feed.post/3kxyz",
            "at://did:plc:abc123/app.bsky.feed.post/3kxyz/extra",
        ] {
            assert_eq!(bsky_post_url(uri), None, "uri: {uri}");
        }
    }

    // -- Truncation ---------------------------------------------------------

    #[test]
    fn truncate_keeps_short_input_unchanged() {
        assert_eq!(truncate_chars("hello", 10), "hello");
        assert_eq!(truncate_chars("hello", 5), "hello");
    }

    #[test]
    fn truncate_is_char_safe_and_never_exceeds_the_limit() {
        let truncated = truncate_chars("héllo wörld", 6);
        assert_eq!(truncated, "héllo…");
        assert_eq!(truncated.chars().count(), 6);

        let emoji = truncate_chars("😀😀😀😀", 3);
        assert_eq!(emoji.chars().count(), 3);
        assert!(emoji.ends_with('…'));
    }

    #[test]
    fn truncate_zero_returns_empty() {
        assert_eq!(truncate_chars("anything", 0), "");
    }

    #[test]
    fn chunk_chars_splits_on_character_boundaries() {
        let chunks = chunk_chars("abcdefg", 3);
        assert_eq!(chunks, vec!["abc", "def", "g"]);

        let unicode = chunk_chars("😀😀😀", 2);
        assert_eq!(unicode, vec!["😀😀", "😀"]);
    }

    // -- Autocomplete -------------------------------------------------------

    #[test]
    fn market_choices_filters_case_insensitively_and_deduplicates() {
        let markets = vec![
            market("M1", "Will it rain?", "will-it-rain"),
            market("M2", "Will the sun rise?", "will-the-sun-rise"),
            market("M1", "Will it rain?", "will-it-rain"),
            market("M3", "Something else", "something-else"),
        ];

        let choices = market_choices(&markets, "RAIN");
        assert_eq!(choices.len(), 1);
        assert_eq!(choices[0].1, "M1");
        assert_eq!(choices[0].0, "Will it rain? (M1)");

        // Slug substrings match too.
        let choices = market_choices(&markets, "sun-rise");
        assert_eq!(choices.len(), 1);
        assert_eq!(choices[0].1, "M2");
    }

    #[test]
    fn market_choices_caps_the_result_at_twenty_five() {
        let markets: Vec<TrackedMarket> = (0..30)
            .map(|index| market(&format!("M{index}"), "Question", "question"))
            .collect();

        let choices = market_choices(&markets, "");
        assert_eq!(choices.len(), MAX_AUTOCOMPLETE_CHOICES);
    }

    #[test]
    fn market_choices_truncates_long_names() {
        let long_question = "q".repeat(200);
        let markets = vec![market("M1", &long_question, "slug")];

        let choices = market_choices(&markets, "");
        assert_eq!(choices.len(), 1);
        assert!(choices[0].0.chars().count() <= MAX_CHOICE_NAME_CHARS);
        assert!(choices[0].0.ends_with('…'));
    }

    // -- Market resolution --------------------------------------------------

    #[test]
    fn resolves_a_market_by_case_insensitive_id_or_slug() {
        let markets = vec![
            market("AbC123", "First?", "first-question"),
            market("DeF456", "Second?", "second-question"),
        ];

        assert_eq!(
            resolve_market(&markets, "abc123").map(|market| market.id.as_str()),
            Some("AbC123")
        );
        assert_eq!(
            resolve_market(&markets, "SECOND-QUESTION").map(|market| market.id.as_str()),
            Some("DeF456")
        );
        assert_eq!(
            resolve_market(&markets, "second").map(|market| market.id.as_str()),
            Some("DeF456")
        );
        assert!(resolve_market(&markets, "missing").is_none());
        assert!(resolve_market(&markets, "").is_none());
    }

    #[test]
    fn ambiguous_slug_matches_resolve_to_none() {
        let markets = vec![
            market("M1", "First?", "shared-prefix-one"),
            market("M2", "Second?", "shared-prefix-two"),
        ];

        assert!(resolve_market(&markets, "shared").is_none());
    }

    // -- Buttons ------------------------------------------------------------

    #[test]
    fn button_custom_ids_roundtrip() {
        let id = button_custom_id("pause", "Market_123");
        assert_eq!(id, "pause:Market_123");
        assert_eq!(
            parse_button_custom_id(&id),
            Some(("pause".to_owned(), "Market_123".to_owned()))
        );
    }

    #[test]
    fn custom_id_parsing_splits_only_the_first_colon_and_rejects_empty_parts() {
        assert_eq!(
            parse_button_custom_id("liquidate:Market:extra"),
            Some(("liquidate".to_owned(), "Market:extra".to_owned()))
        );
        assert_eq!(parse_button_custom_id("no-separator"), None);
        assert_eq!(parse_button_custom_id(":Market"), None);
        assert_eq!(parse_button_custom_id("pause:"), None);
    }
}
