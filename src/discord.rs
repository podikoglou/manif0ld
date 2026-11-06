//! Discord adapter for the operator control plane: slash commands,
//! autocomplete, and event embeds.
//!
//! The bot is a thin rendering layer. It never performs trading, triage, or a
//! lifecycle transition itself: every mutating command calls one
//! [`ControlPlane`] method and renders the report it returns, while `/status`,
//! autocomplete, and the background consumers read the store, the database, and
//! the pipeline directly.
//!
//! # Scope
//!
//! Six slash commands (`/track`, `/untrack`, `/budget`, `/status`, `/override`,
//! `/panic`), the Pause / Liquidate action buttons on the ONBOARDED embed,
//! autocomplete, and the signal/trade/error consumers.
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
//! The database handle and market store use synchronous locks. Every handler
//! finishes its synchronous reads and drops the guard before the next `.await`;
//! no lock is held across a Discord request.

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serenity::all::*;
use serenity::http::Http;
use tokio::sync::{broadcast, mpsc};

use crate::belief;
use crate::config::Config;
use crate::control::{
    resolve_market, CloseReport, ControlPlane, PanicReport, Sale, ThreadId, Threads, UntrackReport,
};
use crate::db::Db;
use crate::error::{bail, AppResult, Context as AnyhowContext};
use crate::market_store::MarketStore;
use crate::onboarding::{OnboardUpdate, OnboardingDraft, ResearchStep, SieveAttempt};
use crate::pipeline::{Pipeline, PipelineEvent};
use crate::trade::TradeEvent;
use crate::types::{
    available_budget, MarketStatus, SignalRecord, TrackedAnswer, TrackedMarket, STRATEGY_SNIPE,
};

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

/// Discord's character limit for a message's `content`.
const DISCORD_CONTENT_LIMIT: usize = 2000;

/// Maximum number of market fields in the overview embed.
const MAX_STATUS_FIELDS: usize = 25;

/// Characters per Sieve source field.
const SIEVE_CHUNK_CHARS: usize = 900;

/// Maximum Sieve source fields kept in a status embed.
///
/// The whole embed is capped at 6000 characters, so an arbitrarily long rule
/// cannot be shown in full. The last field notes how much was omitted.
const MAX_SIEVE_CHUNKS: usize = 3;

/// Number of recent signals shown for one market.
const MAX_SIGNAL_LINES: usize = 10;

/// Number of answers shown in one multiple-choice status embed.
const MAX_ANSWER_LINES: usize = 6;

/// Timeout for one `/track` onboarding run.
///
/// Onboarding performs Manifold, LLM, and Bluesky lookups with up to three
/// synthesis attempts, which easily exceeds the 3-second interaction deadline;
/// the command defers first and reports a timeout followup when hit.
const ONBOARD_TIMEOUT: Duration = Duration::from_secs(600);

/// Minimum gap between edits of the `/track` progress message.
///
/// Onboarding can produce several updates within one second (a dry run, then a
/// submit). Discord rate-limits edits per message, so updates are coalesced into
/// one edit per interval and the newest state always wins.
const TRACK_EDIT_INTERVAL: Duration = Duration::from_millis(1200);

/// Maximum number of completed research actions kept in the `/track` message.
const TRACK_MAX_ACTIVITY_LINES: usize = 8;

/// Maximum characters of one research action line.
const TRACK_MAX_ACTIVITY_CHARS: usize = 110;

/// Space reserved for the latest Sieve attempt when an activity log exists.
///
/// The log never eats the whole message: the program and compiler error are the
/// most useful part of the report, so they keep room for their fences.
const TRACK_ATTEMPT_RESERVE: usize = 320;

/// Runtime dependencies shared by the gateway handler and event consumers.
///
/// `Db` is itself a cheap cloneable handle; every other dependency is behind an
/// `Arc` so the state can be cloned into each spawned consumer.
#[derive(Clone)]
pub struct DiscordState {
    pub config: Arc<Config>,
    pub db: Db,
    pub store: Arc<MarketStore>,
    pub pipeline: Arc<Pipeline>,
    pub control: Arc<ControlPlane>,
}

/// The Discord adapter for the control plane's thread port.
///
/// The adapter owns the Discord API and the thread name; the control plane owns
/// only the ordering (create → activate → archive on failure).
struct DiscordThreads {
    http: Arc<Http>,
    control_channel: ChannelId,
}

#[serenity::async_trait]
impl Threads for DiscordThreads {
    async fn create_for_market(&self, budget: f64, question: &str) -> AppResult<ThreadId> {
        let thread_name = format!("[M{budget}] {}", shorten_question(question, 50));
        let thread = self
            .control_channel
            .create_thread(
                &self.http,
                CreateThread::new(thread_name).kind(ChannelType::PublicThread),
            )
            .await
            .context("creating the market thread under the control channel")?;
        Ok(ThreadId(thread.id.get()))
    }

    async fn archive(&self, thread: ThreadId) {
        if let Err(error) = ChannelId::new(thread.0)
            .edit_thread(&self.http, EditThread::new().archived(true))
            .await
        {
            tracing::warn!(thread_id = thread.0, error = %error, "failed to archive the thread");
        }
    }
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
        Arc::clone(&state.store),
        Arc::clone(&state.config),
        pipeline_events,
    );
    spawn_trade_consumer(
        Arc::clone(&http),
        Arc::clone(&state.store),
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
                    "track" => track(&self.state, &ctx, &interaction).await,
                    "untrack" => untrack(&self.state, &ctx, &interaction).await,
                    "budget" => budget(&self.state, &ctx, &interaction).await,
                    "override" => override_belief(&self.state, &ctx, &interaction).await,
                    "panic" => panic_mode(&self.state, &ctx, &interaction).await,
                    name => {
                        tracing::debug!(command = %name, "unknown Discord command");
                        respond_ephemeral(&ctx, &interaction, &format!("Unknown command /{name}."))
                            .await;
                    }
                }
            }
            Interaction::Component(interaction) => {
                if !is_operator(&self.state, interaction.user.id) {
                    respond_component_ephemeral(&ctx, &interaction, "Not authorized.").await;
                    return;
                }

                component_action(&self.state, &ctx, interaction).await;
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

/// The six guild commands from the specification.
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

/// Keeps only the operator's own `/track`ed markets.
///
/// Snipe markets are internal: they stay in the executor's `active()` set and
/// in reconciliation, but `/status` and autocomplete are operator views and
/// must not list them. Their trade embeds still post, because
/// `channel_for_market` falls back to the control channel when the thread ID
/// is empty.
pub(crate) fn operator_markets(mut markets: Vec<TrackedMarket>) -> Vec<TrackedMarket> {
    markets.retain(|market| market.is_news());
    markets
}

/// Answers the focused option with matching tracked markets.
///
/// The active markets are queried first for freshness, then every database
/// market is added so paused and closed markets stay addressable by commands
/// such as `/untrack`.
async fn autocomplete(state: &DiscordState, ctx: &Context, interaction: &CommandInteraction) {
    let query = interaction
        .data
        .autocomplete()
        .map(|option| option.value.trim().to_owned())
        .unwrap_or_default();

    let mut markets = operator_markets(state.store.active());
    match state.store.list() {
        Ok(all) => markets.extend(operator_markets(all)),
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
/// because callers concatenate the active and full database listings. Each choice
/// name has the form `"{question} ({id})"` and is truncated to Discord's limit.
pub(crate) fn market_choices(markets: &[TrackedMarket], query: &str) -> Vec<(String, String)> {
    let trimmed = query.trim();
    let needle = trimmed.to_lowercase();
    let url_slug = crate::onboarding::parse_slug(trimmed)
        .or_else(|_| crate::onboarding::parse_slug(&format!("https://{trimmed}")))
        .ok()
        .map(|s| s.to_lowercase());
    let mut seen = HashSet::new();
    let mut choices = Vec::new();

    for market in markets {
        if !seen.insert(market.id.clone()) {
            continue;
        }
        if !needle.is_empty() {
            let matches = market.question.to_lowercase().contains(&needle)
                || market.id.to_lowercase().contains(&needle)
                || market.slug.to_lowercase().contains(&needle)
                || url_slug.as_deref().is_some_and(|slug| {
                    market.slug.to_lowercase().contains(slug)
                        || market.id.to_lowercase().contains(slug)
                });
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
    let markets = operator_markets(state.store.list()?);
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

        let probability_line = if market.is_multiple_choice() {
            let answer_count = state
                .db
                .list_answers(&market.id)
                .map(|rows| rows.len())
                .unwrap_or(0);
            format!(
                "{answer_count} answers | sum-to-one {}",
                if market.should_answers_sum_to_one {
                    "yes"
                } else {
                    "no"
                }
            )
        } else {
            format!("P_int {p_internal:.3} vs P_mkt {:.3}", market.p_market_last)
        };

        let value = format!(
            "Budget {:.2} | Invested {:.2} | Realized PnL {:+.2} | Available {:.2}\n\
             {}\n\
             {} | Queue {} | Last signal {}",
            market.budget_mana,
            market.invested_mana,
            market.realized_pnl_mana,
            available_budget(market),
            probability_line,
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
    let markets = state.store.list()?;
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
        .title(truncate_chars(&market.question, MAX_TITLE_CHARS))
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

    embed = with_sieve_source(embed, &market.sieve_source);

    if market.is_multiple_choice() {
        let answers = state.db.list_answers(&market.id).unwrap_or_default();
        let value = if answers.is_empty() {
            "no tracked answers".to_owned()
        } else {
            let mut lines = answer_lines(&answers, now, state.config.belief_halflife_hours);
            if answers.len() > MAX_ANSWER_LINES {
                lines.push(format!("({} answers total)", answers.len()));
            }
            lines.join("\n")
        };
        embed = embed.field("Answers", truncate_chars(&value, MAX_FIELD_CHARS), false);
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

/// Renders the answers that matter most for a multiple-choice market.
///
/// Open positions come first, then the largest gap between internal and market
/// probability. The line is compact because Discord caps one field at 1024
/// characters and a MEGA market has more than fifty answers.
fn answer_lines(answers: &[TrackedAnswer], now: i64, halflife_hours: f64) -> Vec<String> {
    let mut scored: Vec<(bool, f64, f64, &TrackedAnswer)> = answers
        .iter()
        .map(|answer| {
            let p_internal = belief::current_p_internal(
                answer.p_internal_log_odds,
                answer.belief_updated_at,
                answer.p_market_last,
                now,
                halflife_hours,
            );
            let held = answer.yes_shares > 0.0 || answer.no_shares > 0.0;
            let edge = p_internal - answer.p_market_last;
            (held, edge.abs(), p_internal, answer)
        })
        .collect();

    scored.sort_by(|left, right| {
        right
            .0
            .cmp(&left.0)
            .then_with(|| right.1.total_cmp(&left.1))
            .then_with(|| left.3.text.cmp(&right.3.text))
    });

    scored
        .into_iter()
        .take(MAX_ANSWER_LINES)
        .map(|(held, _, p_internal, answer)| {
            format!(
                "{} {} | Y {:.2} N {:.2} | {:.3} vs {:.3} ({:+.3})",
                if held { "+" } else { " " },
                truncate_chars(&answer.text, 64),
                answer.yes_shares,
                answer.no_shares,
                p_internal,
                answer.p_market_last,
                p_internal - answer.p_market_last,
            )
        })
        .collect()
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

/// Live state behind the `/track` progress message.
///
/// The message is a rolling log instead of a single frame. Every edit renders
/// the current step, the recent completed actions with their counts, and the
/// latest Sieve attempt. This keeps the result count of a fast search on
/// screen instead of overwriting it before the operator can read it.
struct TrackProgress {
    started: Instant,
    activity: VecDeque<String>,
    current: Option<String>,
    latest_attempt: Option<SieveAttempt>,
    searches: usize,
    results: usize,
    pages: usize,
    dry_runs: usize,
}

impl TrackProgress {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            activity: VecDeque::new(),
            current: None,
            latest_attempt: None,
            searches: 0,
            results: 0,
            pages: 0,
            dry_runs: 0,
        }
    }

    /// Folds one update into the report state.
    fn apply(&mut self, update: OnboardUpdate) {
        match update {
            OnboardUpdate::Phase(text) => self.current = Some(text),
            OnboardUpdate::Activity(step) => {
                self.current = None;
                self.record(step);
            }
            OnboardUpdate::Sieve(attempt) => {
                self.current = None;
                self.latest_attempt = Some(attempt);
            }
        }
    }

    /// Appends one completed research action and updates the counters.
    fn record(&mut self, step: ResearchStep) {
        let line = match step {
            ResearchStep::Search { query, results } => {
                self.searches += 1;
                self.results += results;
                format!(
                    "web search `{}` → {}",
                    truncate_chars(&query, 60),
                    plural(results, "result", "results")
                )
            }
            ResearchStep::Page { url, chars } => {
                self.pages += 1;
                format!(
                    "read page {} → {}",
                    truncate_chars(&url, 70),
                    format_char_count(chars)
                )
            }
            ResearchStep::DryRun {
                matched,
                total,
                warnings,
            } => {
                self.dry_runs += 1;
                let warning_text = match warnings {
                    0 => String::new(),
                    1 => ", 1 warning".to_owned(),
                    count => format!(", {count} warnings"),
                };
                format!("dry run → {matched}/{total} matched{warning_text}")
            }
            ResearchStep::DryRunFailed { error } => {
                self.dry_runs += 1;
                format!("dry run failed → {}", truncate_chars(&error, 70))
            }
            ResearchStep::Failed { tool, error } => {
                format!("{tool} failed → {}", truncate_chars(&error, 70))
            }
        };

        self.activity
            .push_back(truncate_chars(&line, TRACK_MAX_ACTIVITY_CHARS));
        while self.activity.len() > TRACK_MAX_ACTIVITY_LINES {
            self.activity.pop_front();
        }
    }

    /// Renders the whole report within Discord's content limit.
    fn render(&self) -> String {
        let mut out = String::from("Tracking market…\n");

        let mut counters = vec![format!(
            "{} elapsed",
            format_elapsed(self.started.elapsed())
        )];
        if self.searches > 0 {
            counters.push(plural(self.searches, "search", "searches"));
            counters.push(plural(self.results, "result", "results"));
        }
        if self.pages > 0 {
            counters.push(plural(self.pages, "page", "pages"));
        }
        if self.dry_runs > 0 {
            counters.push(plural(self.dry_runs, "dry run", "dry runs"));
        }
        out.push_str(&counters.join(" · "));

        if let Some(current) = &self.current {
            out.push_str("\n\n");
            out.push_str(&truncate_chars(current, TRACK_MAX_ACTIVITY_CHARS * 2));
        }

        if !self.activity.is_empty() {
            let reserve = if self.latest_attempt.is_some() {
                TRACK_ATTEMPT_RESERVE
            } else {
                0
            };
            let budget = DISCORD_CONTENT_LIMIT
                .saturating_sub(out.chars().count() + 2)
                .saturating_sub(reserve);

            // Keep the newest lines that fit; older ones roll off the top.
            let mut chosen: Vec<&String> = Vec::new();
            let mut used = 0;
            for line in self.activity.iter().rev() {
                let cost = line.chars().count() + 3; // "• " and a newline
                if used + cost > budget && !chosen.is_empty() {
                    break;
                }
                used += cost;
                chosen.push(line);
            }
            chosen.reverse();

            if !chosen.is_empty() {
                out.push_str("\n\n");
                for (index, line) in chosen.into_iter().enumerate() {
                    if index > 0 {
                        out.push('\n');
                    }
                    out.push_str("• ");
                    out.push_str(line);
                }
            }
        }

        if let Some(attempt) = &self.latest_attempt {
            let header = sieve_header(attempt);
            let budget = DISCORD_CONTENT_LIMIT.saturating_sub(out.chars().count() + 2);
            out.push_str("\n\n");
            out.push_str(&sieve_report_within(
                &header,
                &attempt.script,
                attempt.error.as_deref(),
                budget,
            ));
        }

        truncate_chars(&out, DISCORD_CONTENT_LIMIT)
    }
}

/// Formats an elapsed duration as `m:ss`.
fn format_elapsed(elapsed: Duration) -> String {
    let seconds = elapsed.as_secs();
    format!("{}:{:02}", seconds / 60, seconds % 60)
}

/// Formats a character count as `830 chars` or `12.4k chars`.
fn format_char_count(chars: usize) -> String {
    if chars >= 1000 {
        format!("{:.1}k chars", chars as f64 / 1000.0)
    } else {
        format!("{chars} chars")
    }
}

/// Returns `1 search` or `3 searches`.
fn plural(count: usize, singular: &str, many: &str) -> String {
    if count == 1 {
        format!("1 {singular}")
    } else {
        format!("{count} {many}")
    }
}

// -----------------------------------------------------------------------------
// /track
// -----------------------------------------------------------------------------

/// Runs `/track`, the onboarding flow.
///
/// The heavy Manifold + LLM + Bluesky lookups are wrapped in
/// [`ONBOARD_TIMEOUT`]. A progress message is posted after deferral so the
/// operator knows the command is running while the lookups complete. The
/// message is edited live from every [`OnboardUpdate`]: the current step, a
/// rolling log of searches and page reads with their result counts, and the
/// latest Sieve attempt with its program and compiler error. A failed synthesis
/// leaves the last attempt on screen under a final attempt count, so the
/// operator keeps a complete record without waiting for every try to run first.
async fn track(state: &DiscordState, ctx: &Context, interaction: &CommandInteraction) {
    let Some(url) = string_option(interaction, "url") else {
        respond_ephemeral(ctx, interaction, "Missing the required `url` option.").await;
        return;
    };
    let budget = number_option(interaction, "budget").unwrap_or(state.config.default_market_budget);
    if !budget.is_finite() || budget <= 0.0 {
        respond_ephemeral(
            ctx,
            interaction,
            &format!("The market budget must be a positive number, got {budget}."),
        )
        .await;
        return;
    }
    let initial_prob =
        integer_option(interaction, "initial_prob").map(|prob| prob.clamp(1, 99) as u8);

    if let Err(error) = interaction.defer_ephemeral(&ctx.http).await {
        tracing::error!(error = %error, "failed to defer /track");
        return;
    }
    let progress_id = match interaction
        .create_followup(
            &ctx.http,
            CreateInteractionResponseFollowup::new()
                .content("Tracking market…")
                .ephemeral(true),
        )
        .await
    {
        Ok(progress) => progress.id,
        Err(error) => {
            tracing::error!(error = %error, "failed to send the /track progress message");
            return;
        }
    };

    // Every update is folded into one live report: the current step, a rolling
    // log of completed research actions with their counts, and the latest Sieve
    // attempt. A failed attempt is remembered so a failed synthesis can be
    // closed out with the program and its error.
    let (updates_tx, mut updates_rx) = mpsc::channel::<OnboardUpdate>(16);
    let last_failure = Arc::new(Mutex::new(None));
    let update_ctx = ctx.clone();
    // The progress message is ephemeral. Discord does not keep ephemeral
    // messages in the channel, so `Message::edit` (a channel-message PATCH)
    // fails with "Unknown Message". Ephemeral followups can only be edited
    // through the interaction token, which is what `edit_followup` uses. The
    // token lives for 15 minutes, longer than [`ONBOARD_TIMEOUT`].
    let update_interaction = interaction.clone();
    let last_for_update = Arc::clone(&last_failure);
    let updater = tokio::spawn(async move {
        let mut progress = TrackProgress::new();
        let mut last_edit: Option<Instant> = None;
        let mut edit_failures = 0usize;

        while let Some(update) = updates_rx.recv().await {
            progress.apply(update);
            // Fold in anything already queued so one edit shows the newest
            // state instead of spending the rate limit on stale frames.
            while let Ok(update) = updates_rx.try_recv() {
                progress.apply(update);
            }

            if let Some(attempt) = progress.latest_attempt.as_ref() {
                if attempt.error.is_some() {
                    if let Ok(mut last) = last_for_update.lock() {
                        *last = Some(attempt.clone());
                    }
                }
            }

            if let Some(previous) = last_edit {
                let elapsed = previous.elapsed();
                if elapsed < TRACK_EDIT_INTERVAL {
                    tokio::time::sleep(TRACK_EDIT_INTERVAL - elapsed).await;
                    while let Ok(update) = updates_rx.try_recv() {
                        progress.apply(update);
                    }
                }
            }

            let content = progress.render();
            if let Err(error) = update_interaction
                .edit_followup(
                    &update_ctx.http,
                    progress_id,
                    CreateInteractionResponseFollowup::new().content(content),
                )
                .await
            {
                // One failed edit must not silence the rest of the run: log the
                // first failure, keep draining so senders never block, and let
                // the next update try again.
                edit_failures += 1;
                if edit_failures == 1 {
                    tracing::warn!(error = %error, "failed to update the /track progress message");
                } else {
                    tracing::debug!(error = %error, edit_failures, "failed to update the /track progress message");
                }
            }
            last_edit = Some(Instant::now());
        }
    });

    let result = tokio::time::timeout(
        ONBOARD_TIMEOUT,
        onboard(state, ctx, &url, budget, initial_prob, updates_tx),
    )
    .await;
    let _ = updater.await;

    match result {
        Err(_) => {
            finish_track_progress(
                ctx,
                interaction,
                progress_id,
                "Timed out after 600 seconds.",
            )
            .await;
        }
        Ok(Err(error)) => {
            tracing::warn!(error = ?error, "/track failed");
            // When synthesis itself failed, the progress message already shows
            // the last program and its error; only the header changes, so the
            // operator keeps the complete record in one message. Other failures
            // fall back to the plain error chain. The failure is extracted (and
            // the guard released) before any await.
            let failed = last_failure.lock().ok().and_then(|last| last.clone());
            let content = match failed {
                Some(attempt) => {
                    let header = "Agent stopped without a compiling rule.".to_owned();
                    sieve_report(&header, &attempt.script, attempt.error.as_deref())
                }
                None => {
                    // No attempt was ever reported: the model request itself
                    // failed before any program was written, or a pre-synthesis
                    // step failed. Either way the operator needs the whole error
                    // chain, which the legacy `{}` render hid behind the
                    // outermost context label.
                    let synthesis_failed = error
                        .chain()
                        .any(|cause| cause.to_string().contains("synthesizing a Sieve rule"));
                    if synthesis_failed {
                        format!("Failed to track market: model request failed.\n{error:#}")
                    } else {
                        format!("Failed to track market: {error:#}")
                    }
                }
            };
            finish_track_progress(ctx, interaction, progress_id, &content).await;
        }
        Ok(Ok(_)) => {
            if let Err(error) = interaction.delete_followup(&ctx.http, progress_id).await {
                tracing::debug!(error = %error, "failed to delete progress followup message");
            }
            if let Err(error) = interaction.delete_response(&ctx.http).await {
                tracing::debug!(error = %error, "failed to delete interaction response");
            }
        }
    }
}

/// Replaces the `/track` progress followup with its terminal content.
///
/// Editing keeps one run to a single message, so a timeout or a failure lands
/// where the operator is already looking instead of in another message. When
/// the edit fails (for example the message was deleted), the same content is
/// sent as a new followup so the result is never lost.
async fn finish_track_progress(
    ctx: &Context,
    interaction: &CommandInteraction,
    progress_id: MessageId,
    content: &str,
) {
    let trimmed = truncate_chars(content, DISCORD_CONTENT_LIMIT);
    let edit = interaction
        .edit_followup(
            &ctx.http,
            progress_id,
            CreateInteractionResponseFollowup::new().content(trimmed),
        )
        .await;
    if let Err(error) = edit {
        tracing::warn!(error = %error, "failed to edit the /track progress message");
        send_followup(ctx, interaction, content).await;
    }
}

/// Prepares, threads, and activates one market draft.
///
/// The thread is created under the control channel before activation so no
/// market goes live without a thread. If activation fails anyway, the created
/// thread is archived again so the operator does not see an empty thread
/// announcing a market that never went live.
async fn onboard(
    state: &DiscordState,
    ctx: &Context,
    url: &str,
    budget: f64,
    initial_prob: Option<u8>,
    updates: mpsc::Sender<OnboardUpdate>,
) -> AppResult<()> {
    let threads = DiscordThreads {
        http: Arc::clone(&ctx.http),
        control_channel: ChannelId::new(state.config.discord_control_channel_id),
    };
    let onboarded = state
        .control
        .track(url, budget, initial_prob, updates, &threads)
        .await?;

    let mut message = CreateMessage::new().embed(onboarded_embed(&onboarded.draft));
    for button in onboarded_buttons(&onboarded.draft.market.id) {
        message = message.button(button);
    }
    if let Err(error) = ChannelId::new(onboarded.thread.0)
        .send_message(&ctx.http, message)
        .await
    {
        tracing::error!(
            thread_id = onboarded.thread.0,
            error = %error,
            "failed to post the [ONBOARDED] embed"
        );
    }

    Ok(())
}

// -----------------------------------------------------------------------------
// /untrack
// -----------------------------------------------------------------------------

/// Runs `/untrack`, removing a market and optionally liquidating its position.
///
/// Deferral happens before the (potentially slow) liquidation. The default
/// behavior is to sell all shares, matching the specification.
async fn untrack(state: &DiscordState, ctx: &Context, interaction: &CommandInteraction) {
    let Some(query) = string_option(interaction, "market")
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    else {
        respond_ephemeral(ctx, interaction, "Missing the required `market` option.").await;
        return;
    };
    let liquidate = bool_option(interaction, "liquidate").unwrap_or(true);

    if let Err(error) = interaction.defer_ephemeral(&ctx.http).await {
        tracing::error!(error = %error, "failed to defer /untrack");
        return;
    }

    let threads = DiscordThreads {
        http: Arc::clone(&ctx.http),
        control_channel: ChannelId::new(state.config.discord_control_channel_id),
    };

    match state.control.untrack(&query, liquidate, &threads).await {
        Ok(report) => send_followup(ctx, interaction, &render_untrack(&report)).await,
        Err(error) => {
            tracing::warn!(error = %error, "/untrack failed");
            send_followup(
                ctx,
                interaction,
                &format!("Failed to untrack the market: {error}"),
            )
            .await;
        }
    }
}

/// Renders an `/untrack` report. Every string is copied from the pre-service
/// handler so the operator sees the same output.
fn render_untrack(report: &UntrackReport) -> String {
    match report {
        UntrackReport::AlreadyUntracked {
            question,
            market_id,
        } => format!(
            "**{question}** (`{market_id}`) is already untracked. Nothing was changed."
        ),
        UntrackReport::Untracked {
            question,
            market_id,
            sale,
        } => format!(
            "Untracked **{question}** (`{market_id}`).{} The row and its history were kept; the \
             thread was archived.",
            render_sale(sale),
        ),
    }
}

/// The position clause of an `/untrack` reply.
fn render_sale(sale: &Sale) -> String {
    match sale {
        Sale::Failed(error) => format!(
            " Untracking continued because liquidation failed: `{error}`. The \
             position was not sold; handle it on Manifold directly."
        ),
        Sale::Liquidated | Sale::Resolved | Sale::NoPosition => {
            " Positions were liquidated.".to_owned()
        }
        Sale::Untouched => " Positions were left untouched.".to_owned(),
        Sale::AlreadyClosed => " The position was already closed.".to_owned(),
    }
}

// -----------------------------------------------------------------------------
// /budget
// -----------------------------------------------------------------------------

/// Runs `/budget`, changing a market's mana budget.
async fn budget(state: &DiscordState, ctx: &Context, interaction: &CommandInteraction) {
    let Some(query) = string_option(interaction, "market")
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    else {
        respond_ephemeral(ctx, interaction, "Missing the required `market` option.").await;
        return;
    };
    let Some(new_budget) = number_option(interaction, "new_budget") else {
        respond_ephemeral(
            ctx,
            interaction,
            "Missing the required `new_budget` option.",
        )
        .await;
        return;
    };
    if !new_budget.is_finite() || new_budget <= 0.0 {
        respond_ephemeral(
            ctx,
            interaction,
            &format!("The new budget must be a positive number, got {new_budget}."),
        )
        .await;
        return;
    }

    let result = state.control.set_budget(&query, new_budget).map(|change| {
        format!(
            "Budget for **{}** (`{}`): {:.2} mana → {:.2} mana.",
            change.question, change.market_id, change.before, change.after
        )
    });

    finish_sync(ctx, interaction, result, "Failed to change the budget").await;
}

// -----------------------------------------------------------------------------
// /override
// -----------------------------------------------------------------------------

/// Runs `/override`, forcing a market's internal probability.
async fn override_belief(state: &DiscordState, ctx: &Context, interaction: &CommandInteraction) {
    let Some(query) = string_option(interaction, "market")
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    else {
        respond_ephemeral(ctx, interaction, "Missing the required `market` option.").await;
        return;
    };
    let Some(probability) = integer_option(interaction, "probability") else {
        respond_ephemeral(
            ctx,
            interaction,
            "Missing the required `probability` option.",
        )
        .await;
        return;
    };
    let probability = probability.clamp(1, 99) as f64 / 100.0;

    let result = (|| -> AppResult<String> {
        let change = state.control.set_probability(&query, probability)?;

        // Trigger one trade check off the interaction path: it makes network
        // calls and must not block the response.
        let control = Arc::clone(&state.control);
        let checked_id = change.market_id.clone();
        tokio::spawn(async move {
            control.check_now(&checked_id).await;
        });

        Ok(format!(
            "**{}** (`{}`) internal probability set to {:.3} (log-odds {:.4}). \
             One trade check was triggered.",
            change.question, change.market_id, change.clamped_p, change.log_odds
        ))
    })();

    finish_sync(
        ctx,
        interaction,
        result,
        "Failed to override the probability",
    )
    .await;
}

// -----------------------------------------------------------------------------
// /panic
// -----------------------------------------------------------------------------

/// Runs `/panic`, enabling the global buy kill switch.
///
/// There is deliberately no un-panic command: the switch is cleared by editing
/// the `global_kill_switch` kv entry directly, or by an operator hand-off that
/// writes the same value. Enabling it twice is therefore harmless. Sells are
/// never blocked by the switch.
async fn panic_mode(state: &DiscordState, ctx: &Context, interaction: &CommandInteraction) {
    let liquidate_all = bool_option(interaction, "liquidate_all").unwrap_or(false);

    if let Err(error) = interaction.defer_ephemeral(&ctx.http).await {
        tracing::error!(error = %error, "failed to defer /panic");
        return;
    }

    let result = (async || -> AppResult<String> {
        let report = state.control.panic(liquidate_all).await?;
        Ok(render_panic(&report))
    })()
    .await;

    match result {
        Ok(summary) => send_followup(ctx, interaction, &summary).await,
        Err(error) => {
            send_followup(
                ctx,
                interaction,
                &format!("Failed to enable the kill switch: {error}"),
            )
            .await;
        }
    }
}

/// Renders a `/panic` report. The prose and the kill-switch header are copied
/// from the pre-service handler; only the error text is truncated here.
fn render_panic(report: &PanicReport) -> String {
    let mut summary = String::from(
        "Global kill switch is **ON**. Future buys are blocked; sells remain allowed.\n\
         To disable it, edit the `global_kill_switch` kv entry (or hand off to an operator); \
         there is no un-panic command.",
    );

    if let Some(attempted) = report.attempted {
        summary.push_str(&format!(
            "\n\nLiquidate-all: {}/{attempted} market(s) liquidated.",
            report.liquidated
        ));
        for (market_id, error) in &report.errors {
            summary.push_str(&format!(
                "\n- `{market_id}`: {}",
                truncate_chars(error, 400)
            ));
        }
    }

    summary
}

// -----------------------------------------------------------------------------
// Interaction responses
// -----------------------------------------------------------------------------

/// Returns the value of a named command option, if present.
///
/// A missing optional option yields `None` rather than an error, so handlers
/// can apply their own defaults.
fn option_value<'a>(
    interaction: &'a CommandInteraction,
    name: &str,
) -> Option<&'a CommandDataOptionValue> {
    interaction
        .data
        .options
        .iter()
        .find(|option| option.name == name)
        .map(|option| &option.value)
}

/// Reads a string option by name, accepting normal and autocomplete values.
pub(crate) fn string_option(interaction: &CommandInteraction, name: &str) -> Option<String> {
    option_value(interaction, name).and_then(string_value)
}

/// Reads a floating-point option by name.
pub(crate) fn number_option(interaction: &CommandInteraction, name: &str) -> Option<f64> {
    option_value(interaction, name).and_then(number_value)
}

/// Reads an integer option by name.
pub(crate) fn integer_option(interaction: &CommandInteraction, name: &str) -> Option<i64> {
    option_value(interaction, name).and_then(integer_value)
}

/// Reads a boolean option by name.
pub(crate) fn bool_option(interaction: &CommandInteraction, name: &str) -> Option<bool> {
    option_value(interaction, name).and_then(boolean_value)
}

/// Extracts a string from one option value.
fn string_value(value: &CommandDataOptionValue) -> Option<String> {
    match value {
        CommandDataOptionValue::String(value)
        | CommandDataOptionValue::Autocomplete { value, .. } => Some(value.clone()),
        _ => None,
    }
}

/// Extracts a float from one option value.
fn number_value(value: &CommandDataOptionValue) -> Option<f64> {
    match value {
        CommandDataOptionValue::Number(value) => Some(*value),
        _ => None,
    }
}

/// Extracts an integer from one option value.
fn integer_value(value: &CommandDataOptionValue) -> Option<i64> {
    match value {
        CommandDataOptionValue::Integer(value) => Some(*value),
        _ => None,
    }
}

/// Extracts a boolean from one option value.
fn boolean_value(value: &CommandDataOptionValue) -> Option<bool> {
    match value {
        CommandDataOptionValue::Boolean(value) => Some(*value),
        _ => None,
    }
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

/// Sends an ephemeral followup (after the interaction was deferred).
///
/// Content is truncated to Discord's 2000-character message limit.
async fn send_followup(ctx: &Context, interaction: &CommandInteraction, content: &str) {
    let response = CreateInteractionResponseFollowup::new()
        .content(truncate_chars(content, DISCORD_CONTENT_LIMIT))
        .ephemeral(true);
    if let Err(error) = interaction.create_followup(&ctx.http, response).await {
        tracing::error!(error = %error, "failed to send an ephemeral followup");
    }
}

/// Renders one Sieve submission report for a message's `content`.
///
/// The program the agent submitted sits in its own `sieve` block and, when it
/// did not compile, the compiler error sits in a second block below it, so the
/// error can neither be mistaken for prose nor be read as part of the program.
/// A report without an error is the compiled candidate. Content stays within
/// Discord's character limit: the script is truncated first, and the error is
/// bounded too, so both blocks and the closing fences always survive.
fn sieve_report(header: &str, script: &str, error: Option<&str>) -> String {
    sieve_report_within(header, script, error, DISCORD_CONTENT_LIMIT)
}

/// Renders a Sieve report inside `limit` characters.
///
/// Used by the live `/track` message, where the activity log and the attempt
/// share one message. The error is capped first so the fence framing survives
/// even when the caller has little space left.
fn sieve_report_within(header: &str, script: &str, error: Option<&str>, limit: usize) -> String {
    const ERROR_LIMIT: usize = 700;
    const ERROR_FRAMING: usize = "\n\nError\n```text\n\n```".len();

    let opening = "```sieve\n";
    let closing = "\n```";
    let base = header.chars().count() + 1 + opening.chars().count() + closing.chars().count();
    let minimum = if error.is_some() {
        base + ERROR_FRAMING
    } else {
        base
    };

    if limit <= minimum {
        return truncate_chars(&format!("{header}\n{opening}"), limit);
    }

    let error_budget = limit.saturating_sub(base + ERROR_FRAMING).min(ERROR_LIMIT);
    let suffix = error
        .map(|error| {
            format!(
                "\n\nError\n```text\n{}\n```",
                truncate_chars(error, error_budget)
            )
        })
        .unwrap_or_default();
    let script = truncate_chars(script, limit.saturating_sub(base + suffix.chars().count()));
    format!("{header}\n{opening}{script}{closing}{suffix}")
}

/// The one-line header of a Sieve submission report.
fn sieve_header(attempt: &SieveAttempt) -> String {
    match attempt.error {
        Some(_) => format!("Attempt {} failed", attempt.attempt),
        None => format!("Attempt {} compiled", attempt.attempt),
    }
}

/// Responds to a synchronous command with its success message or truncated error.
///
/// These commands are pure local state changes, so they respond directly
/// instead of deferring and posting a followup.
async fn finish_sync(
    ctx: &Context,
    interaction: &CommandInteraction,
    result: AppResult<String>,
    error_prefix: &str,
) {
    match result {
        Ok(message) => respond_ephemeral(ctx, interaction, &message).await,
        Err(error) => {
            tracing::warn!(error = %error, "{error_prefix}");
            respond_ephemeral(
                ctx,
                interaction,
                &truncate_chars(&format!("{error_prefix}: {error}"), 2000),
            )
            .await;
        }
    }
}

// -----------------------------------------------------------------------------
// Action buttons
// -----------------------------------------------------------------------------

/// Dispatches a button press to its action handler.
///
/// The interaction is consumed here so the handler can edit the source message
/// through the owned [`ComponentInteraction`].
async fn component_action(state: &DiscordState, ctx: &Context, interaction: ComponentInteraction) {
    let Some((action, market_id)) = parse_button_custom_id(&interaction.data.custom_id) else {
        respond_component_ephemeral(ctx, &interaction, "Unknown button.").await;
        return;
    };
    tracing::debug!(action = %action, market_id = %market_id, "component action");

    match action.as_str() {
        "pause" => pause_button(state, ctx, interaction).await,
        "liquidate" => liquidate_button(state, ctx, interaction).await,
        _ => respond_component_ephemeral(ctx, &interaction, "Unknown button.").await,
    }
}

/// Toggles one market between ACTIVE and PAUSED from its ONBOARDED embed.
///
/// The Pause button label is swapped to Resume (and back) by editing the source
/// message; the Liquidate button is left untouched.
async fn pause_button(state: &DiscordState, ctx: &Context, mut interaction: ComponentInteraction) {
    let Some((_, market_id)) = parse_button_custom_id(&interaction.data.custom_id) else {
        respond_component_ephemeral(ctx, &interaction, "Unknown button.").await;
        return;
    };
    if let Err(error) = interaction.defer_ephemeral(&ctx.http).await {
        tracing::error!(error = %error, "failed to defer the pause button");
        return;
    }

    let result = state.control.set_paused(&market_id);

    match result {
        Ok(new_status) => {
            let paused = new_status == MarketStatus::Paused;
            let buttons = vec![
                CreateButton::new(button_custom_id("pause", &market_id))
                    .label(if paused { "Resume" } else { "Pause" })
                    .style(ButtonStyle::Secondary),
                CreateButton::new(button_custom_id("liquidate", &market_id))
                    .label("Liquidate & Close")
                    .style(ButtonStyle::Secondary),
            ];
            // Only the buttons are replaced; the embed is preserved.
            if let Err(error) = interaction
                .message
                .edit(
                    &ctx.http,
                    EditMessage::new().components(vec![CreateActionRow::Buttons(buttons)]),
                )
                .await
            {
                tracing::error!(error = %error, "failed to update the pause button label");
            }
            respond_component_followup(
                ctx,
                &interaction,
                &format!("Market is now {}.", new_status.as_str()),
            )
            .await;
        }
        Err(error) => {
            tracing::warn!(error = %error, "pause button failed");
            respond_component_followup(
                ctx,
                &interaction,
                &truncate_chars(&format!("Failed to toggle the market: {error}"), 2000),
            )
            .await;
        }
    }
}

/// Liquidates, closes, unregisters, and archives one market from its embed.
///
/// The market is validated before anything is sold. A closed or untracked
/// market is reported and the embed buttons are disabled, so a second press
/// cannot repeat the action.
async fn liquidate_button(
    state: &DiscordState,
    ctx: &Context,
    mut interaction: ComponentInteraction,
) {
    let Some((_, market_id)) = parse_button_custom_id(&interaction.data.custom_id) else {
        respond_component_ephemeral(ctx, &interaction, "Unknown button.").await;
        return;
    };
    if let Err(error) = interaction.defer_ephemeral(&ctx.http).await {
        tracing::error!(error = %error, "failed to defer the liquidate button");
        return;
    }

    let threads = DiscordThreads {
        http: Arc::clone(&ctx.http),
        control_channel: ChannelId::new(state.config.discord_control_channel_id),
    };

    match state.control.close(&market_id, &threads).await {
        Ok(report) => {
            // The market is closed or gone, so this embed is stale. Disable the
            // buttons before the followup so a second press does nothing.
            disable_action_buttons(ctx, &mut interaction.message, &market_id).await;
            respond_component_followup(ctx, &interaction, &render_close(&report)).await
        }
        Err(error) => {
            tracing::warn!(error = %error, "liquidate button failed");
            respond_component_followup(
                ctx,
                &interaction,
                &truncate_chars(&format!("Liquidation failed: {error}"), 2000),
            )
            .await;
        }
    }
}

/// Renders a Liquidate-button report. Every string is copied from the
/// pre-service handler.
fn render_close(report: &CloseReport) -> String {
    match report {
        CloseReport::NotTracked { market_id } => format!(
            "Market `{market_id}` is no longer tracked. Nothing was liquidated."
        ),
        CloseReport::AlreadyUntracked {
            question,
            market_id,
        } => format!(
            "**{question}** (`{market_id}`) was untracked already. Nothing was liquidated."
        ),
        CloseReport::AlreadyClosed {
            question,
            market_id,
        } => format!(
            "**{question}** (`{market_id}`) is already closed. Nothing was liquidated."
        ),
        CloseReport::ClosedByAnotherAction {
            question,
            market_id,
        } => format!(
            "**{question}** (`{market_id}`) was already closed by another action. Nothing was \
             liquidated."
        ),
        CloseReport::Closed {
            question,
            market_id,
            sale,
        } => match sale {
            Sale::Liquidated => format!(
                "**{question}** (`{market_id}`) liquidated, closed, and archived."
            ),
            Sale::Resolved => format!(
                "**{question}** (`{market_id}`) is resolved on Manifold, so its positions settled \
                 automatically. The market is now closed and archived."
            ),
            Sale::NoPosition => format!(
                "**{question}** (`{market_id}`) had no open position. The market is now closed and \
                 archived."
            ),
            Sale::AlreadyClosed => format!(
                "**{question}** (`{market_id}`) is already closed. Nothing was liquidated."
            ),
            Sale::Failed(error) => format!(
                "**{question}** (`{market_id}`) was closed and archived, but liquidation had \
                 errors: {error}"
            ),
            // The close path never asks for this end state.
            Sale::Untouched => format!(
                "**{question}** (`{market_id}`) is already closed. Nothing was liquidated."
            ),
        },
    }
}

/// Replaces the ONBOARDED action row with disabled buttons.
///
/// This is the final guard against a repeated destructive press on an embed
/// whose market is already closed.
async fn disable_action_buttons(ctx: &Context, message: &mut Message, market_id: &str) {
    let buttons = onboarded_buttons(market_id)
        .into_iter()
        .map(|button| button.disabled(true))
        .collect();
    if let Err(error) = message
        .edit(
            &ctx.http,
            EditMessage::new().components(vec![CreateActionRow::Buttons(buttons)]),
        )
        .await
    {
        tracing::warn!(market_id = %market_id, error = %error, "failed to disable the action buttons");
    }
}

/// Sends an ephemeral followup for a component interaction.
async fn respond_component_followup(
    ctx: &Context,
    interaction: &ComponentInteraction,
    content: &str,
) {
    let response = CreateInteractionResponseFollowup::new()
        .content(truncate_chars(content, 2000))
        .ephemeral(true);
    if let Err(error) = interaction.create_followup(&ctx.http, response).await {
        tracing::error!(error = %error, "failed to send a component followup");
    }
}

// -----------------------------------------------------------------------------
// Event consumers
// -----------------------------------------------------------------------------

/// Spawns the pipeline-event renderer.
fn spawn_pipeline_consumer(
    http: Arc<Http>,
    store: Arc<MarketStore>,
    config: Arc<Config>,
    events: broadcast::Receiver<PipelineEvent>,
) {
    tokio::spawn(async move {
        consume_pipeline_events(http, store, config, events).await;
    });
}

/// Spawns the trade-event renderer.
fn spawn_trade_consumer(
    http: Arc<Http>,
    store: Arc<MarketStore>,
    config: Arc<Config>,
    events: broadcast::Receiver<TradeEvent>,
) {
    tokio::spawn(async move {
        consume_trade_events(http, store, config, events).await;
    });
}

/// Renders pipeline signals and errors into Discord messages.
async fn consume_pipeline_events(
    http: Arc<Http>,
    store: Arc<MarketStore>,
    config: Arc<Config>,
    mut events: broadcast::Receiver<PipelineEvent>,
) {
    loop {
        match events.recv().await {
            Ok(PipelineEvent::PostMatched {
                market_id,
                post_uri,
                author_handle,
            }) => {
                let channel = channel_for_market(&store, &config, Some(&market_id));
                let url = bsky_post_url_with_handle(&post_uri, Some(&author_handle))
                    .unwrap_or_else(|| post_uri.clone());
                send_text_message(&http, channel, &url).await;
            }
            Ok(PipelineEvent::Signal {
                material: true,
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
                p_before,
                p_after,
                p_market,
                ..
            }) => {
                let channel = channel_for_market(&store, &config, Some(&market_id));

                let mut embed = CreateEmbed::new()
                    .title(truncate_chars(
                        &format!("[SIGNAL] {question}"),
                        MAX_TITLE_CHARS,
                    ))
                    .description(truncate_chars(&text, MAX_DESCRIPTION_CHARS))
                    .field(
                        "Author",
                        truncate_chars(
                            &format!("@{author_handle} ({author_did})"),
                            MAX_FIELD_CHARS,
                        ),
                        false,
                    );

                if let Some(answer) = answer {
                    embed = embed.field("Answer", truncate_chars(&answer, MAX_FIELD_CHARS), false);
                }

                embed = embed
                    .field("Evidence", format!("{p_evidence:.2}"), true)
                    .field(
                        "Direction",
                        format!("{} ({p_target:.2})", direction.as_str()),
                        true,
                    )
                    .field("Strength", format!("{strength:.1} / 3"), true)
                    .field("Priced", format!("{p_priced:.2}"), true)
                    .field("P_internal", format!("{p_before:.3} → {p_after:.3}"), true)
                    .field("P_market", format!("{p_market:.3}"), true);

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
                p_evidence,
                strength,
                ..
            }) => {
                tracing::debug!(
                    market_id = %market_id,
                    direction = %direction,
                    p_evidence,
                    strength,
                    "non-material signal not posted to Discord"
                );
            }
            Ok(PipelineEvent::Error { market_id, message }) => {
                tracing::error!(market_id = ?market_id, message = %message, "pipeline error");
                let channel = channel_for_market(&store, &config, market_id.as_deref());
                let mut embed = CreateEmbed::new()
                    .title("[ERROR]")
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
    store: Arc<MarketStore>,
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
                strategy,
            }) => {
                let channel = channel_for_market(&store, &config, Some(&market_id));
                let mut embed = CreateEmbed::new()
                    .title(format!("[TRADE] {action}"))
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
                // S2 provenance: `p_market` on a snipe entry is the creation
                // price, so the spread is the gap the snipe traded on. Snipe
                // rows have no thread; `channel_for_market` already falls back
                // to the control channel.
                if strategy == STRATEGY_SNIPE {
                    embed = embed.field(
                        "Strategy",
                        format!("⛏ SNIPE · gap {:+.3}", p_internal - p_market),
                        true,
                    );
                }
                send_embed(&http, channel, embed).await;
            }
            Ok(TradeEvent::Error { market_id, message }) => {
                tracing::error!(market_id = ?market_id, message = %message, "trade error");
                let channel = channel_for_market(&store, &config, market_id.as_deref());
                let mut embed = CreateEmbed::new()
                    .title("[ERROR]")
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
fn channel_for_market(store: &MarketStore, config: &Config, market_id: Option<&str>) -> ChannelId {
    let thread = market_id
        .and_then(|id| match store.load(id) {
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

/// Sends a text message, logging failures instead of propagating them.
async fn send_text_message(http: &Http, channel: ChannelId, content: &str) {
    if content.trim().is_empty() {
        return;
    }
    if let Err(error) = channel
        .send_message(http, CreateMessage::new().content(content))
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

/// Builds the `[ONBOARDED]` embed posted into a new market's thread.
///
/// The Sieve source is split into bounded fields exactly like the `/status`
/// market view, so an arbitrarily long rule cannot fail embed validation.
fn onboarded_embed(draft: &OnboardingDraft) -> CreateEmbed {
    let market = &draft.market;

    let embed = CreateEmbed::new()
        .title(truncate_chars(&market.question, MAX_TITLE_CHARS))
        .field(
            "Market",
            if market.market_url.trim().is_empty() {
                format!("https://manifold.markets/{}", market.slug)
            } else {
                market.market_url.clone()
            },
            true,
        )
        .field("Budget", format!("{:.2}", market.budget_mana), true)
        .field(
            "Initial P_market",
            format!("{:.3}", market.p_market_last),
            true,
        );

    with_sieve_source(embed, &draft.sieve_source)
}

/// Appends the Sieve source to an embed as bounded, numbered fields.
///
/// One embed field cannot exceed ~1024 characters and the whole embed is capped
/// at 6000, so a long rule is split into at most [`MAX_SIEVE_CHUNKS`] fields
/// named `Sieve source #1`, `#2`, … The last field notes how much was dropped.
/// Both the `/status` market view and the `[ONBOARDED]` embed use this, so the
/// two never disagree about how a rule is presented.
fn with_sieve_source(mut embed: CreateEmbed, source: &str) -> CreateEmbed {
    let chunks = chunk_chars(source, SIEVE_CHUNK_CHARS);
    let shown = chunks.len().min(MAX_SIEVE_CHUNKS);
    let omitted = chunks.len().saturating_sub(shown);
    for (index, chunk) in chunks.iter().take(shown).enumerate() {
        let mut value = format!("```sieve\n{chunk}\n```");
        if index + 1 == shown && omitted > 0 {
            value.push_str(&format!(
                "\n… {omitted} more field(s) truncated; see the database for the full rule."
            ));
        }
        embed = embed.field(
            sieve_field_name(index),
            truncate_chars(&value, MAX_FIELD_CHARS),
            false,
        );
    }
    embed
}

/// Names one Sieve source field, `#1`-based.
fn sieve_field_name(index: usize) -> String {
    format!("Sieve source #{}", index + 1)
}

/// The action buttons attached to the ONBOARDED embed.
fn onboarded_buttons(market_id: &str) -> Vec<CreateButton> {
    vec![
        CreateButton::new(button_custom_id("pause", market_id))
            .label("Pause")
            .style(ButtonStyle::Secondary),
        CreateButton::new(button_custom_id("liquidate", market_id))
            .label("Liquidate & Close")
            .style(ButtonStyle::Secondary),
    ]
}

/// Converts a Bluesky AT URI into a public post URL.
///
/// Only `at://{did}/app.bsky.feed.post/{rkey}` is accepted. The DID and record
/// key must be non-empty and no extra path segments may be present.
pub(crate) fn bsky_post_url(uri: &str) -> Option<String> {
    bsky_post_url_with_handle(uri, None)
}

/// Converts a Bluesky AT URI into a public post URL, preferring a human-readable handle when provided.
pub(crate) fn bsky_post_url_with_handle(uri: &str, handle: Option<&str>) -> Option<String> {
    let rest = uri.strip_prefix("at://")?;
    let mut parts = rest.split('/');
    let did = parts.next().filter(|part| !part.is_empty())?;
    let collection = parts.next()?;
    let rkey = parts.next().filter(|part| !part.is_empty())?;

    if collection != "app.bsky.feed.post" || parts.next().is_some() {
        return None;
    }

    let profile = match handle {
        Some(h) if !h.trim().is_empty() && !h.starts_with("did:") => h.trim(),
        _ => did,
    };

    Some(format!("https://bsky.app/profile/{profile}/post/{rkey}"))
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

/// Shortens a market question for use in a thread name.
///
/// Surrounding whitespace is removed and the result is truncated to at most
/// `max` characters without splitting a UTF-8 character, so thread names stay
/// within Discord's 100-character limit.
pub(crate) fn shorten_question(question: &str, max: usize) -> String {
    truncate_chars(question.trim(), max)
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
            market_url: String::new(),
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
            deleted_at: None,
            outcome_type: "BINARY".to_owned(),
            should_answers_sum_to_one: false,
            strategy: crate::types::STRATEGY_NEWS.to_owned(),
        }
    }

    // -- Bluesky URLs -------------------------------------------------------

    #[test]
    fn builds_a_bluesky_post_url_from_an_at_uri() {
        assert_eq!(
            bsky_post_url("at://did:plc:abc123/app.bsky.feed.post/3kxyz"),
            Some("https://bsky.app/profile/did:plc:abc123/post/3kxyz".to_owned())
        );
        assert_eq!(
            bsky_post_url_with_handle(
                "at://did:plc:abc123/app.bsky.feed.post/3kxyz",
                Some("alice.bsky.social")
            ),
            Some("https://bsky.app/profile/alice.bsky.social/post/3kxyz".to_owned())
        );
        assert_eq!(
            bsky_post_url_with_handle(
                "at://did:plc:abc123/app.bsky.feed.post/3kxyz",
                Some("did:plc:abc123")
            ),
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

    // -- Sieve submission report --------------------------------------------

    #[test]
    fn sieve_report_shows_script_and_error_in_separate_blocks() {
        let report = sieve_report(
            "Attempt 2 failed",
            "match Market_x { \"unterminated }",
            Some("unterminated string literal at 1:24"),
        );
        assert!(
            report.starts_with("Attempt 2 failed\n```sieve\n"),
            "report: {report}"
        );
        assert_eq!(
            report,
            "Attempt 2 failed\n\
             ```sieve\n\
             match Market_x { \"unterminated }\n\
             ```\n\n\
             Error\n\
             ```text\n\
             unterminated string literal at 1:24\n\
             ```",
            "report: {report}"
        );
        assert!(report.chars().count() <= DISCORD_CONTENT_LIMIT);
    }

    #[test]
    fn sieve_report_without_error_shows_only_the_script() {
        let report = sieve_report("Attempt 3 compiled", "match Market_x { \"launch\" }", None);
        assert_eq!(
            report, "Attempt 3 compiled\n```sieve\nmatch Market_x { \"launch\" }\n```",
            "report: {report}"
        );
        assert!(!report.contains("Error"));
        assert!(report.chars().count() <= DISCORD_CONTENT_LIMIT);
    }

    #[test]
    fn sieve_report_truncates_long_parts_but_keeps_both_fences() {
        let long_script: String = "x".repeat(3000);
        let long_error: String = "y".repeat(3000);
        let report = sieve_report("Attempt 1 failed", &long_script, Some(&long_error));
        assert!(report.chars().count() <= DISCORD_CONTENT_LIMIT);
        assert!(
            report.contains('…'),
            "truncation marker missing in: {report}"
        );
        assert_eq!(report.matches("```").count(), 4, "report: {report}");
        assert!(report.contains("Error\n```text\n"), "report: {report}");
        assert!(report.ends_with("```"), "report: {report}");
    }

    #[test]
    fn sieve_header_names_the_attempt_and_outcome() {
        let failed = SieveAttempt {
            attempt: 2,
            script: "x".to_owned(),
            error: Some("boom".to_owned()),
        };
        assert_eq!(sieve_header(&failed), "Attempt 2 failed");

        let compiled = SieveAttempt {
            error: None,
            ..failed
        };
        assert_eq!(sieve_header(&compiled), "Attempt 2 compiled");
    }

    // -- Live /track progress ----------------------------------------------

    #[test]
    fn track_progress_shows_search_counts_and_recent_actions() {
        let mut progress = TrackProgress::new();
        progress.apply(OnboardUpdate::Activity(ResearchStep::Search {
            query: "trump tariffs".to_owned(),
            results: 8,
        }));
        progress.apply(OnboardUpdate::Activity(ResearchStep::Search {
            query: "tariff court ruling".to_owned(),
            results: 0,
        }));
        progress.apply(OnboardUpdate::Activity(ResearchStep::Page {
            url: "https://example.com/story".to_owned(),
            chars: 12_400,
        }));
        progress.apply(OnboardUpdate::Activity(ResearchStep::DryRun {
            matched: 3,
            total: 8,
            warnings: 2,
        }));

        let report = progress.render();
        assert!(report.starts_with("Tracking market…\n"), "{report}");
        assert!(
            report.contains("2 searches · 8 results · 1 page · 1 dry run"),
            "{report}"
        );
        assert!(
            report.contains("web search `trump tariffs` → 8 results"),
            "{report}"
        );
        assert!(
            report.contains("web search `tariff court ruling` → 0 results"),
            "{report}"
        );
        assert!(
            report.contains("read page https://example.com/story → 12.4k chars"),
            "{report}"
        );
        assert!(
            report.contains("dry run → 3/8 matched, 2 warnings"),
            "{report}"
        );
    }

    #[test]
    fn track_progress_shows_the_running_phase_next_to_history() {
        let mut progress = TrackProgress::new();
        progress.apply(OnboardUpdate::Activity(ResearchStep::Search {
            query: "rates".to_owned(),
            results: 4,
        }));
        progress.apply(OnboardUpdate::Phase(
            "Researching: reading `https://example.com`…".to_owned(),
        ));

        let report = progress.render();
        assert!(report.contains("Researching: reading"), "{report}");
        assert!(
            report.contains("web search `rates` → 4 results"),
            "{report}"
        );
    }

    #[test]
    fn track_progress_rolls_old_actions_off_the_log() {
        let mut progress = TrackProgress::new();
        for index in 0..(TRACK_MAX_ACTIVITY_LINES + 2) {
            progress.apply(OnboardUpdate::Activity(ResearchStep::Search {
                query: format!("query-{index}"),
                results: index,
            }));
        }

        let report = progress.render();
        assert!(!report.contains("query-0"), "{report}");
        assert!(
            report.contains(&format!("query-{}", TRACK_MAX_ACTIVITY_LINES + 1)),
            "{report}"
        );
    }

    #[test]
    fn track_progress_keeps_the_attempt_report_within_the_content_limit() {
        let mut progress = TrackProgress::new();
        for index in 0..TRACK_MAX_ACTIVITY_LINES {
            progress.apply(OnboardUpdate::Activity(ResearchStep::Page {
                url: format!("https://example.com/very/long/path/{index}"),
                chars: 20_000,
            }));
        }
        progress.apply(OnboardUpdate::Sieve(SieveAttempt {
            attempt: 4,
            script: "match Market_x { \"launch\" }".repeat(200),
            error: Some("unexpected token".repeat(200)),
        }));

        let report = progress.render();
        assert!(
            report.chars().count() <= DISCORD_CONTENT_LIMIT,
            "{} chars",
            report.chars().count()
        );
        assert!(report.contains("Attempt 4 failed"), "{report}");
        assert!(report.contains("```sieve"), "{report}");
        assert!(report.contains("Error\n```text"), "{report}");
        // The closing fences of both blocks survive the shared budget.
        assert_eq!(report.matches("```").count(), 4, "{report}");
        assert!(report.ends_with("```"), "{report}");
    }

    #[test]
    fn chunk_chars_splits_on_character_boundaries() {
        let chunks = chunk_chars("abcdefg", 3);
        assert_eq!(chunks, vec!["abc", "def", "g"]);

        let unicode = chunk_chars("😀😀😀", 2);
        assert_eq!(unicode, vec!["😀😀", "😀"]);
    }

    // -- Thread-name shortening --------------------------------------------

    #[test]
    fn shorten_question_trims_whitespace_and_keeps_short_questions() {
        assert_eq!(shorten_question("  Will it rain?  ", 50), "Will it rain?");
        assert_eq!(shorten_question("Will it rain?", 50), "Will it rain?");
    }

    #[test]
    fn shorten_question_is_char_safe_and_never_exceeds_the_limit() {
        let shortened = shorten_question("Do we upgrade to 🚀 stellar engines tomorrow?", 10);
        assert!(shortened.chars().count() <= 10);
        assert!(shortened.ends_with('…'));
    }

    // -- Option parsing ----------------------------------------------------

    #[test]
    fn option_value_extraction_is_type_specific() {
        let string = CommandDataOptionValue::String("s".to_owned());
        assert_eq!(string_value(&string), Some("s".to_owned()));
        assert_eq!(number_value(&string), None);
        assert_eq!(integer_value(&string), None);
        assert_eq!(boolean_value(&string), None);

        let autocomplete = CommandDataOptionValue::Autocomplete {
            kind: CommandOptionType::String,
            value: "ac".to_owned(),
        };
        assert_eq!(string_value(&autocomplete), Some("ac".to_owned()));

        assert_eq!(
            number_value(&CommandDataOptionValue::Number(12.5)),
            Some(12.5)
        );
        assert_eq!(
            integer_value(&CommandDataOptionValue::Integer(-3)),
            Some(-3)
        );
        assert_eq!(
            boolean_value(&CommandDataOptionValue::Boolean(false)),
            Some(false)
        );
    }

    #[test]
    fn option_value_extraction_returns_none_for_wrong_variants() {
        assert_eq!(number_value(&CommandDataOptionValue::Boolean(true)), None);
        assert_eq!(integer_value(&CommandDataOptionValue::Number(1.0)), None);
        assert_eq!(boolean_value(&CommandDataOptionValue::Integer(1)), None);
    }

    // -- ONBOARDED embed ---------------------------------------------------

    #[test]
    fn onboarded_embed_carries_market_fields_and_sieve_source_chunks() {
        let mut market = market("M1", "Will it rain?", "will-it-rain");
        market.budget_mana = 25.0;
        market.p_market_last = 0.6;
        market.market_url = "https://manifold.markets/Alice/will-it-rain".to_owned();
        let draft = OnboardingDraft {
            market,
            sieve_source: "match Market_M1 { \"rain\" }".to_owned(),
            answers: Vec::new(),
        };

        let embed = onboarded_embed(&draft);
        // CreateEmbed keeps its fields private; inspect the serialized form.
        let value = serde_json::to_value(&embed).expect("embed serializes");
        let title = value["title"].as_str().unwrap_or_default();
        assert!(!title.contains("[ONBOARDED]"), "{title}");
        assert_eq!(title, "Will it rain?");

        let fields = value["fields"].as_array().expect("embed fields");
        let names: Vec<&str> = fields
            .iter()
            .filter_map(|field| field["name"].as_str())
            .collect();
        assert!(names.contains(&"Sieve source #1"), "{names:?}");
        assert!(!names.contains(&"VIP handles"), "{names:?}");

        let sieve_field = fields
            .iter()
            .find(|field| field["name"] == "Sieve source #1")
            .expect("sieve source field");
        assert!(
            sieve_field["value"]
                .as_str()
                .unwrap_or_default()
                .contains("```sieve\nmatch Market_M1"),
            "{:?}",
            sieve_field["value"]
        );

        let market_field = fields
            .iter()
            .find(|field| field["name"] == "Market")
            .expect("market field");
        assert_eq!(
            market_field["value"],
            "https://manifold.markets/Alice/will-it-rain"
        );
    }

    // -- Sieve source fields -----------------------------------------------

    /// Serializes an embed and returns its field names in order.
    fn field_names(embed: CreateEmbed) -> Vec<String> {
        let value = serde_json::to_value(embed).expect("embed serializes");
        value["fields"]
            .as_array()
            .expect("embed fields")
            .iter()
            .filter_map(|field| field["name"].as_str().map(str::to_owned))
            .collect()
    }

    /// Returns the value of the field with the given name.
    fn field_value(embed: CreateEmbed, name: &str) -> String {
        let value = serde_json::to_value(embed).expect("embed serializes");
        value["fields"]
            .as_array()
            .expect("embed fields")
            .iter()
            .find(|field| field["name"] == name)
            .and_then(|field| field["value"].as_str())
            .unwrap_or_default()
            .to_owned()
    }

    #[test]
    fn sieve_source_fields_are_numbered_from_one() {
        let source = "x".repeat(SIEVE_CHUNK_CHARS + 10);
        let names = field_names(with_sieve_source(CreateEmbed::new(), &source));

        assert_eq!(names, vec!["Sieve source #1", "Sieve source #2"]);
        // Nothing was dropped, so no field claims a truncation.
        let last = field_value(with_sieve_source(CreateEmbed::new(), &source), "Sieve source #2");
        assert!(!last.contains("truncated"), "{last}");
    }

    #[test]
    fn sieve_source_fields_stop_at_the_cap_and_note_omissions() {
        let source = "x".repeat(SIEVE_CHUNK_CHARS * 5);
        let embed = with_sieve_source(CreateEmbed::new(), &source);
        let names = field_names(embed.clone());

        assert_eq!(
            names,
            vec!["Sieve source #1", "Sieve source #2", "Sieve source #3"]
        );
        let last = field_value(embed, "Sieve source #3");
        assert!(last.contains("2 more field(s) truncated"), "{last}");
    }

    #[test]
    fn sieve_source_field_still_appears_for_an_empty_rule() {
        let names = field_names(with_sieve_source(CreateEmbed::new(), ""));

        assert_eq!(names, vec!["Sieve source #1"]);
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
    fn operator_markets_keeps_news_and_drops_snipe_rows() {
        let mut snipe = market("S1", "Snipe question?", "snipe-question");
        snipe.strategy = crate::types::STRATEGY_SNIPE.to_owned();
        let markets = vec![
            market("M1", "News question?", "news-question"),
            snipe,
        ];

        let visible = operator_markets(markets);
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].id, "M1");
        assert!(visible[0].is_news());
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

    // -- Multiple-choice answers -------------------------------------------

    fn tracked_answer(id: &str, text: &str, p_internal: f64, price: f64, yes: f64) -> TrackedAnswer {
        TrackedAnswer {
            id: id.to_owned(),
            market_id: "M1".to_owned(),
            text: text.to_owned(),
            p_internal_log_odds: belief::logit(p_internal),
            p_market_last: price,
            belief_updated_at: 0,
            yes_shares: yes,
            no_shares: 0.0,
            created_at: 0,
            invested_mana: 0.0,
        }
    }

    #[test]
    fn answer_lines_put_open_positions_first_and_cap_the_list() {
        let mut answers: Vec<TrackedAnswer> = (0..8)
            .map(|index| {
                tracked_answer(
                    &format!("a{index}"),
                    &format!("Answer {index}"),
                    0.5,
                    0.5,
                    0.0,
                )
            })
            .collect();
        // A small edge but an open position must outrank a larger bare edge.
        answers[5].yes_shares = 10.0;
        answers[5].p_internal_log_odds = belief::logit(0.52);
        answers[2].p_internal_log_odds = belief::logit(0.9);

        let lines = answer_lines(&answers, 0, 4.0);

        assert_eq!(lines.len(), MAX_ANSWER_LINES);
        assert!(lines[0].starts_with('+'), "line: {}", lines[0]);
        assert!(lines[0].contains("Answer 5"), "line: {}", lines[0]);
        assert!(lines[1].contains("Answer 2"), "line: {}", lines[1]);
    }
}
