//! Process configuration loaded from environment variables.
//!
//! `Config` is the single source of truth for runtime settings. Values are read
//! once at startup; downstream modules receive `&Config` rather than reading the
//! environment themselves. Secret values are never included in error messages.

use std::path::PathBuf;

use crate::error::{AppResult, Context, anyhow, bail};

const DEFAULT_JETSTREAM_URL: &str = "wss://jetstream2.us-east.bsky.network/subscribe";
const DEFAULT_DB_PATH: &str = "./data/manif0ld.db";
const DEFAULT_MARKET_BUDGET: f64 = 100.0;
const DEFAULT_KELLY_FRACTION: f64 = 0.25;
const DEFAULT_ENTRY_EDGE_THRESHOLD: f64 = 0.05;
const DEFAULT_EXIT_EDGE_THRESHOLD: f64 = 0.01;
const DEFAULT_MIN_TRADE_SIZE: f64 = 1.0;
/// Slack allowed beyond the bot's own fair price in one buy's fill limit
/// (`docs/ALGO.md` §S1): the capped `limitProb` becomes `p_internal − ε`.
const DEFAULT_LIMIT_EPSILON: f64 = 0.01;
/// Lifetime of the resting remainder of a fair-capped buy, sent as
/// `expiresMillisAfter` on `POST /v0/bet` (`docs/ALGO.md` §S1). The probe
/// verified that Manifold accepts the field, expires a past-due order
/// server-side at the top of the minute, and silently caps `amount` to what
/// the limit allows — so the remainder rests *by construction* and the only
/// choice is how long it may sit.
const DEFAULT_LIMIT_EXPIRY_SECS: usize = 60;
/// Reverse-drift cancel slack: how much worse than the day's fresh fair-capped
/// cap an own resting order may sit (YES/NO-sensitive; `docs/SPEC.md` §6.6)
/// before the executor cancels it.
const DEFAULT_LIMIT_DRIFT_CANCEL: f64 = 0.05;
const DEFAULT_BELIEF_KAPPA: f64 = 1.1;
const DEFAULT_BELIEF_HALFLIFE_HOURS: f64 = 4.0;
const DEFAULT_TRIAGE_MIN_EVIDENCE: f64 = 0.7;
/// Dry run is the safe default until the canary phase is approved.
const DEFAULT_TRADING_DRY_RUN: bool = true;
const DEFAULT_MC_MAX_POSITIONS: usize = 6;
const DEFAULT_MC_MAX_NEW_PER_TICK: usize = 2;
const DEFAULT_MC_MAX_ANSWER_FRACTION: f64 = 0.25;
/// Manifold allows 500 requests/minute per IP; the bucket starts at that.
const DEFAULT_RATE_LIMIT_PER_MIN: usize = 500;
/// S2 new-market sniping is off until its own canary phase is approved.
const DEFAULT_SNIPE_ENABLED: bool = false;
/// Separate global pool for snipes, so autonomous discovery cannot spend the
/// per-market budgets that `/track` allocated.
const DEFAULT_SNIPE_POOL_MANA: f64 = 100.0;
/// Per-snipe cap; new markets are thin, so one bad resolution stays small.
const DEFAULT_SNIPE_MAX_MANA: f64 = 10.0;
/// Markets below this liquidity cannot absorb even a minimum snipe.
const DEFAULT_SNIPE_MIN_LIQ: f64 = 100.0;
/// Smallest |p_model − p_initial| worth a trade; below this is noise.
const DEFAULT_SNIPE_MIN_GAP: f64 = 0.10;
/// Fractional Kelly for snipe sizing, smaller than the news default because
/// the model has one shot and the creator can resolve oddly.
const DEFAULT_SNIPE_KELLY_FRACTION: f64 = 0.25;
/// How long a new market stays eligible for a snipe.
const DEFAULT_SNIPE_TTL_SECS: usize = 3600;
/// Upper bound on the snipe eligibility window: one week. A longer TTL would
/// silently turn the "new market" strategy into a stale-price scanner.
pub const MAX_SNIPE_TTL_SECS: usize = 86_400 * 7;
/// Deadline for one fair-probability decision call; snipe speed is the edge.
const DEFAULT_SNIPE_DECISION_TIMEOUT_SECS: usize = 2;
/// The S3 leaderboard is read-only, so it can run without a canary phase;
/// copy execution has its own flag in the next task.
const DEFAULT_TRADERS_ENABLED: bool = true;
/// Target size of the scored watch list (`docs/ALGO.md` §3/S3 step 1).
const DEFAULT_TRADER_WATCH_SIZE: usize = 200;
/// Bets sampled per trader before the activity gate can pass; one lucky bet
/// must not buy a watch-list slot.
const DEFAULT_TRADER_MIN_BETS: usize = 25;
/// One refresh per hour: trader skill is a slow signal and each refresh spends
/// REST budget that trades may need.
const DEFAULT_TRADER_REFRESH_MIN: usize = 60;
/// Hard cap on Manifold requests one refresh may spend. Two per enriched
/// trader plus three for the candidate universe, so the default covers ~58
/// traders per pass; the remainder of the watch list keeps its last score.
const DEFAULT_TRADER_REQUEST_BUDGET: usize = 120;
/// S3b copy execution is off until its own canary phase is approved.
const DEFAULT_COPY_ENABLED: bool = false;
/// Smallest source bet worth considering; below it the bet is noise.
const DEFAULT_COPY_MIN_SIZE: f64 = 5.0;
/// Probability that their bet is informed, below which the copy is skipped.
const DEFAULT_COPY_MIN_INFORMED: f64 = 0.60;
/// Smallest residual edge after their move that still justifies a copy.
const DEFAULT_COPY_MIN_EDGE: f64 = 0.05;
/// Half-Kelly fraction for copy sizing: half of the news fraction, because a
/// copied trader's move is already reflected in the current price.
const DEFAULT_COPY_KELLY_FRACTION: f64 = 0.125;
/// Per-copy cap, clamped to the pool and daily caps at use time.
const DEFAULT_COPY_MAX_MANA: f64 = 10.0;
/// Global pool for copy positions, separate from every other strategy.
const DEFAULT_COPY_POOL_MANA: f64 = 100.0;
/// Rolling 24-hour deployment cap for copy buys.
const DEFAULT_COPY_DAILY_MANA: f64 = 30.0;
/// Largest accepted age of a source bet at evaluation time.
const DEFAULT_COPY_MAX_AGE_SECS: usize = 30;
/// Deadline for each of the two copy decision calls.
const DEFAULT_COPY_DECISION_TIMEOUT_SECS: usize = 2;

/// Runtime configuration for the bot.
///
/// Construct with [`Config::from_env`] in production. The `Debug` impl prints
/// every field verbatim, so never log a `Config` value: several fields hold
/// credentials.
#[derive(Debug, Clone)]
pub struct Config {
    pub jetstream_url: String,
    pub cloudflare_account_id: Option<String>,
    pub triage_model: String,
    pub onboarding_model: String,
    /// Firecrawl credential for the onboarding research agent.
    pub firecrawl_api_key: String,
    pub manifold_api_key: String,
    pub discord_bot_token: String,
    pub discord_guild_id: u64,
    pub discord_control_channel_id: u64,
    pub discord_operator_user_id: u64,
    pub default_market_budget: f64,
    pub kelly_fraction: f64,
    pub entry_edge_threshold: f64,
    pub exit_edge_threshold: f64,
    pub min_trade_size: f64,
    /// The epsilon in the fair-capped buy sweep `limitProb = p_internal − ε`
    /// (`docs/ALGO.md` §S1). Validated into the open interval `(0, 0.05)`:
    /// strictly positive so the cap always undercuts the fair price, and
    /// strictly below the *default* entry-edge threshold (0.05) so a
    /// threshold-sized edge still crosses the market price after the cap;
    /// an `ε ≥ ENTRY_EDGE_THRESHOLD` could only produce no-fill limit orders
    /// resting on the book instead of sweeps.
    pub limit_epsilon: f64,
    /// How long the resting remainder of one fair-capped buy may sit on the
    /// book before Manifold expires it, sent as `expiresMillisAfter` on every
    /// bot buy (seconds; the wire takes milliseconds). Bounded into
    /// `[1, 300]` seconds so a drifted edge can idle at most five minutes,
    /// and bot buys never need a manual cancel for a stale layer.
    pub limit_expiry_secs: usize,
    /// Reverse-drift cancel slack for the own resting layer (`docs/SPEC.md`
    /// §6.6): cancel an own order once its limit price is more than this far
    /// past the fresh fair-capped cap, on the outcome it buys. Must lie in the
    /// open interval `(0, 1)`.
    pub limit_drift_cancel: f64,
    pub belief_kappa: f64,
    pub belief_halflife_hours: f64,
    /// Probability of evidence a post must reach before it moves belief.
    pub triage_min_evidence: f64,
    /// When true, planned bets are sent with `dryRun: true` and nothing is
    /// persisted. Defaults to true until the canary phase is approved.
    pub trading_dry_run: bool,
    /// Maximum answers of one multiple-choice market that may hold shares.
    pub mc_max_positions: usize,
    /// Maximum new answer entries in one evaluation.
    pub mc_max_new_per_tick: usize,
    /// Maximum share of a market budget committed to one answer.
    pub mc_max_answer_fraction: f64,
    /// Manifold REST requests per minute shared by every consumer, refilled
    /// continuously; trade writes have priority over housekeeping.
    pub rate_limit_per_min: usize,
    /// Enables S2 new-market sniping (`docs/ALGO.md` §S2). Off by default.
    pub snipe_enabled: bool,
    /// Global mana pool for snipes, separate from every per-market budget.
    /// [`crate::db::Db::snipe_pool_invested`] reports what is committed.
    pub snipe_pool_mana: f64,
    /// Maximum mana for one snipe, validated only as positive here. It should
    /// stay at or below [`Self::snipe_pool_mana`]; a larger value is warned
    /// about at startup and clamped at use time.
    pub snipe_max_mana: f64,
    /// Minimum `totalLiquidity` a market needs before sniping it.
    pub snipe_min_liq: f64,
    /// Minimum absolute gap `|p_model − p_initial|` in the open interval
    /// `(0, 1)`.
    pub snipe_min_gap: f64,
    /// Fractional Kelly fraction for snipe sizing, in `(0, 1]`.
    pub snipe_kelly_fraction: f64,
    /// How long a new market stays eligible for a snipe, in seconds.
    ///
    /// Substrate only: the strategy owns the eligibility boundary check; this
    /// value is not enforced anywhere in this module.
    pub snipe_ttl_secs: usize,
    /// Deadline for one snipe decision call, in `[1, 30]` seconds.
    pub snipe_decision_timeout_secs: usize,
    /// Enables the S3 smart-money leaderboard (`docs/ALGO.md` §3/S3). The
    /// leaderboard is read-only, so this only gates the hourly refresh task;
    /// copy execution has its own flag in the next task.
    pub traders_enabled: bool,
    /// Target number of scored traders kept in `trader_scores`, in
    /// `[10, 1000]`.
    pub trader_watch_size: usize,
    /// Minimum sampled bet count before a trader may hold a watch-list slot.
    pub trader_min_bets: usize,
    /// Minutes between leaderboard refreshes, in `[5, 1440]`.
    pub trader_refresh_min: usize,
    /// Hard cap on Manifold requests one refresh may spend, at least 20.
    pub trader_request_budget: usize,
    /// Enables S3b smart-money copy execution (`docs/ALGO.md` §S3). Off by
    /// default; with the flag off the executor never evaluates a bet, never
    /// fetches a market, and never writes a `copy_ledger` row.
    pub copy_enabled: bool,
    /// Minimum mana size of a source bet before it may be copied.
    pub copy_min_size: f64,
    /// Minimum `is_informed_bet` probability, in `(0, 1)`.
    pub copy_min_informed: f64,
    /// Minimum residual edge after the source bet, in `(0, 1)`.
    pub copy_min_edge: f64,
    /// Fractional Kelly fraction for copy sizing, in `(0, 0.5]`.
    pub copy_kelly_fraction: f64,
    /// Per-copy cap in mana, clamped to the pool and daily caps at use time.
    pub copy_max_mana: f64,
    /// Global mana pool for copy positions, separate from every other
    /// strategy. [`crate::db::Db::copy_pool_invested`] reports what is open.
    pub copy_pool_mana: f64,
    /// Rolling 24-hour cap on executed copy buys, in mana.
    pub copy_daily_mana: f64,
    /// Largest accepted age of a source bet, in `[1, 300]` seconds.
    pub copy_max_age_secs: usize,
    /// Deadline for each copy decision call, in `[1, 30]` seconds.
    pub copy_decision_timeout_secs: usize,
    pub db_path: PathBuf,
}

impl Config {
    /// Loads configuration from the process environment.
    ///
    /// Also loads a `.env` file when present. A missing or unreadable `.env` is
    /// not an error: the process environment may be the source of truth.
    pub fn from_env() -> AppResult<Self> {
        dotenvy::dotenv().ok();
        let config = Self::from_lookup(|key| std::env::var(key).ok())?;
        if config.snipe_max_mana > config.snipe_pool_mana {
            tracing::warn!(
                snipe_max_mana = config.snipe_max_mana,
                snipe_pool_mana = config.snipe_pool_mana,
                "SNIPE_MAX_MANA exceeds SNIPE_POOL_MANA; the per-snipe cap will be clamped to \
                 the remaining pool at use time"
            );
        }
        if config.copy_max_mana > config.copy_pool_mana {
            tracing::warn!(
                copy_max_mana = config.copy_max_mana,
                copy_pool_mana = config.copy_pool_mana,
                "COPY_MAX_MANA exceeds COPY_POOL_MANA; the per-copy cap will be clamped to the \
                 remaining pool at use time"
            );
        }
        Ok(config)
    }

    /// Builds a configuration from an arbitrary key lookup.
    ///
    /// This is the testable core of [`Config::from_env`]; tests pass a closure
    /// over a map instead of mutating the process environment.
    fn from_lookup(get: impl Fn(&str) -> Option<String>) -> AppResult<Self> {
        let jetstream_url =
            optional(&get, "JETSTREAM_URL").unwrap_or_else(|| DEFAULT_JETSTREAM_URL.to_owned());

        // The rig cloudflare provider reads `CLOUDFLARE_API_TOKEN` and
        // `CLOUDFLARE_AI_GATEWAY_ID` from the process environment itself.
        // The rig openrouter provider reads `OPENROUTER_API_KEY`.
        let triage_model = required(&get, "TRIAGE_MODEL")?;
        let (triage_provider, _) = parse_model_spec(&triage_model)?;
        let onboarding_model = required(&get, "ONBOARDING_MODEL")?;
        let (onboarding_provider, _) = parse_model_spec(&onboarding_model)?;
        if onboarding_provider == ModelProvider::OpenRouterDecisions {
            bail!(
                "ONBOARDING_MODEL must be a chat model; '{onboarding_model}' uses the decisions API"
            );
        }

        let needs_cloudflare = triage_provider == ModelProvider::Cloudflare
            || onboarding_provider == ModelProvider::Cloudflare;
        let needs_openrouter = matches!(
            triage_provider,
            ModelProvider::OpenRouter | ModelProvider::OpenRouterDecisions
        ) || onboarding_provider == ModelProvider::OpenRouter;

        let cloudflare_account_id = if needs_cloudflare {
            let account_id = required(&get, "CLOUDFLARE_ACCOUNT_ID")?;
            validate_cloudflare_account_id(&account_id)?;
            Some(account_id)
        } else {
            match optional(&get, "CLOUDFLARE_ACCOUNT_ID") {
                Some(id) if id != "{account_id}" => {
                    validate_cloudflare_account_id(&id)?;
                    Some(id)
                }
                _ => None,
            }
        };

        if needs_openrouter {
            let _ = required(&get, "OPENROUTER_API_KEY")?;
        }

        let manifold_api_key = required(&get, "MANIFOLD_API_KEY")?;
        let firecrawl_api_key = required(&get, "FIRECRAWL_API_KEY")?;
        let discord_bot_token = required(&get, "DISCORD_BOT_TOKEN")?;
        let discord_guild_id = required_u64(&get, "DISCORD_GUILD_ID")?;
        let discord_control_channel_id = required_u64(&get, "DISCORD_CONTROL_CHANNEL_ID")?;
        let discord_operator_user_id = required_u64(&get, "DISCORD_OPERATOR_USER_ID")?;

        let default_market_budget = f64_or(&get, "DEFAULT_MARKET_BUDGET", DEFAULT_MARKET_BUDGET)?;
        let kelly_fraction = f64_or(&get, "KELLY_FRACTION", DEFAULT_KELLY_FRACTION)?;
        let entry_edge_threshold =
            f64_or(&get, "ENTRY_EDGE_THRESHOLD", DEFAULT_ENTRY_EDGE_THRESHOLD)?;
        let exit_edge_threshold = f64_or(&get, "EXIT_EDGE_THRESHOLD", DEFAULT_EXIT_EDGE_THRESHOLD)?;
        let min_trade_size = f64_or(&get, "MIN_TRADE_SIZE", DEFAULT_MIN_TRADE_SIZE)?;
        let limit_epsilon = f64_or(&get, "LIMIT_EPSILON", DEFAULT_LIMIT_EPSILON)?;
        // Open interval: ε must stay strictly below the default entry-edge
        // threshold so the fair-capped sweep (`p_internal − ε`) still crosses
        // the market price even at a threshold-sized edge (see the field doc).
        if !(limit_epsilon > 0.0 && limit_epsilon < 0.05) {
            bail!("LIMIT_EPSILON must be within (0, 0.05), got {limit_epsilon}");
        }
        let limit_expiry_secs = usize_or(&get, "LIMIT_EXPIRY_SECS", DEFAULT_LIMIT_EXPIRY_SECS)?;
        if !(1..=300).contains(&limit_expiry_secs) {
            bail!("LIMIT_EXPIRY_SECS must be within [1, 300] seconds, got {limit_expiry_secs}");
        }
        let limit_drift_cancel =
            f64_or(&get, "LIMIT_DRIFT_CANCEL", DEFAULT_LIMIT_DRIFT_CANCEL)?;
        if !(limit_drift_cancel > 0.0 && limit_drift_cancel < 1.0) {
            bail!("LIMIT_DRIFT_CANCEL must be within (0, 1), got {limit_drift_cancel}");
        }
        let belief_kappa = f64_or(&get, "BELIEF_KAPPA", DEFAULT_BELIEF_KAPPA)?;
        let belief_halflife_hours =
            f64_or(&get, "BELIEF_HALFLIFE_HOURS", DEFAULT_BELIEF_HALFLIFE_HOURS)?;
        let triage_min_evidence = f64_or(&get, "TRIAGE_MIN_EVIDENCE", DEFAULT_TRIAGE_MIN_EVIDENCE)?;
        let trading_dry_run = bool_or(&get, "TRADING_DRY_RUN", DEFAULT_TRADING_DRY_RUN)?;
        let mc_max_positions = usize_or(&get, "MC_MAX_POSITIONS", DEFAULT_MC_MAX_POSITIONS)?;
        let mc_max_new_per_tick =
            usize_or(&get, "MC_MAX_NEW_PER_TICK", DEFAULT_MC_MAX_NEW_PER_TICK)?;
        let mc_max_answer_fraction = f64_or(
            &get,
            "MC_MAX_ANSWER_FRACTION",
            DEFAULT_MC_MAX_ANSWER_FRACTION,
        )?;
        if !(mc_max_answer_fraction > 0.0 && mc_max_answer_fraction <= 1.0) {
            bail!("MC_MAX_ANSWER_FRACTION must be in (0, 1], got {mc_max_answer_fraction}");
        }

        let rate_limit_per_min = usize_or(&get, "RATE_LIMIT_PER_MIN", DEFAULT_RATE_LIMIT_PER_MIN)?;
        if rate_limit_per_min == 0 {
            bail!("RATE_LIMIT_PER_MIN must be a positive number of requests per minute");
        }

        let snipe_enabled = bool_or(&get, "SNIPE_ENABLED", DEFAULT_SNIPE_ENABLED)?;
        let snipe_pool_mana = f64_or(&get, "SNIPE_POOL_MANA", DEFAULT_SNIPE_POOL_MANA)?;
        if snipe_pool_mana <= 0.0 || !snipe_pool_mana.is_finite() {
            bail!("SNIPE_POOL_MANA must be a finite number greater than 0, got {snipe_pool_mana}");
        }
        let snipe_max_mana = f64_or(&get, "SNIPE_MAX_MANA", DEFAULT_SNIPE_MAX_MANA)?;
        if snipe_max_mana <= 0.0 || !snipe_max_mana.is_finite() {
            bail!("SNIPE_MAX_MANA must be a finite number greater than 0, got {snipe_max_mana}");
        }
        let snipe_min_liq = f64_or(&get, "SNIPE_MIN_LIQ", DEFAULT_SNIPE_MIN_LIQ)?;
        if snipe_min_liq < 0.0 || !snipe_min_liq.is_finite() {
            bail!(
                "SNIPE_MIN_LIQ must be a finite number greater than or equal to 0, got \
                 {snipe_min_liq}"
            );
        }
        let snipe_min_gap = f64_or(&get, "SNIPE_MIN_GAP", DEFAULT_SNIPE_MIN_GAP)?;
        if !(snipe_min_gap > 0.0 && snipe_min_gap < 1.0) {
            bail!("SNIPE_MIN_GAP must be within (0, 1), got {snipe_min_gap}");
        }
        let snipe_kelly_fraction =
            f64_or(&get, "SNIPE_KELLY_FRACTION", DEFAULT_SNIPE_KELLY_FRACTION)?;
        if !(snipe_kelly_fraction > 0.0 && snipe_kelly_fraction <= 1.0) {
            bail!("SNIPE_KELLY_FRACTION must be within (0, 1], got {snipe_kelly_fraction}");
        }
        let snipe_ttl_secs = usize_or(&get, "SNIPE_TTL_SECS", DEFAULT_SNIPE_TTL_SECS)?;
        if !(1..=MAX_SNIPE_TTL_SECS).contains(&snipe_ttl_secs) {
            bail!(
                "SNIPE_TTL_SECS must be within [1, {MAX_SNIPE_TTL_SECS}] seconds, got \
                 {snipe_ttl_secs}"
            );
        }
        let snipe_decision_timeout_secs = usize_or(
            &get,
            "SNIPE_DECISION_TIMEOUT_SECS",
            DEFAULT_SNIPE_DECISION_TIMEOUT_SECS,
        )?;
        if !(1..=30).contains(&snipe_decision_timeout_secs) {
            bail!(
                "SNIPE_DECISION_TIMEOUT_SECS must be within [1, 30] seconds, got \
                 {snipe_decision_timeout_secs}"
            );
        }

        let db_path = optional(&get, "DB_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_DB_PATH));

        let traders_enabled = bool_or(&get, "TRADERS_ENABLED", DEFAULT_TRADERS_ENABLED)?;
        let trader_watch_size =
            usize_or(&get, "TRADER_WATCH_SIZE", DEFAULT_TRADER_WATCH_SIZE)?;
        if !(10..=1000).contains(&trader_watch_size) {
            bail!(
                "TRADER_WATCH_SIZE must be within [10, 1000] traders, got {trader_watch_size}"
            );
        }
        let trader_min_bets = usize_or(&get, "TRADER_MIN_BETS", DEFAULT_TRADER_MIN_BETS)?;
        if trader_min_bets == 0 {
            bail!("TRADER_MIN_BETS must be at least 1");
        }
        let trader_refresh_min =
            usize_or(&get, "TRADER_REFRESH_MIN", DEFAULT_TRADER_REFRESH_MIN)?;
        if !(5..=1440).contains(&trader_refresh_min) {
            bail!(
                "TRADER_REFRESH_MIN must be within [5, 1440] minutes, got {trader_refresh_min}"
            );
        }
        let trader_request_budget =
            usize_or(&get, "TRADER_REQUEST_BUDGET", DEFAULT_TRADER_REQUEST_BUDGET)?;
        if trader_request_budget < 20 {
            bail!(
                "TRADER_REQUEST_BUDGET must be at least 20 requests per refresh, got \
                 {trader_request_budget}"
            );
        }

        let copy_enabled = bool_or(&get, "COPY_ENABLED", DEFAULT_COPY_ENABLED)?;
        let copy_min_size = f64_or(&get, "COPY_MIN_SIZE", DEFAULT_COPY_MIN_SIZE)?;
        if copy_min_size < 0.0 || !copy_min_size.is_finite() {
            bail!(
                "COPY_MIN_SIZE must be a finite number greater than or equal to 0, got \
                 {copy_min_size}"
            );
        }
        let copy_min_informed = f64_or(&get, "COPY_MIN_INFORMED", DEFAULT_COPY_MIN_INFORMED)?;
        if !(copy_min_informed > 0.0 && copy_min_informed < 1.0) {
            bail!("COPY_MIN_INFORMED must be within (0, 1), got {copy_min_informed}");
        }
        let copy_min_edge = f64_or(&get, "COPY_MIN_EDGE", DEFAULT_COPY_MIN_EDGE)?;
        if !(copy_min_edge > 0.0 && copy_min_edge < 1.0) {
            bail!("COPY_MIN_EDGE must be within (0, 1), got {copy_min_edge}");
        }
        let copy_kelly_fraction =
            f64_or(&get, "COPY_KELLY_FRACTION", DEFAULT_COPY_KELLY_FRACTION)?;
        if !(copy_kelly_fraction > 0.0 && copy_kelly_fraction <= 0.5) {
            bail!("COPY_KELLY_FRACTION must be within (0, 0.5], got {copy_kelly_fraction}");
        }
        let copy_max_mana = f64_or(&get, "COPY_MAX_MANA", DEFAULT_COPY_MAX_MANA)?;
        if copy_max_mana <= 0.0 || !copy_max_mana.is_finite() {
            bail!("COPY_MAX_MANA must be a finite number greater than 0, got {copy_max_mana}");
        }
        let copy_pool_mana = f64_or(&get, "COPY_POOL_MANA", DEFAULT_COPY_POOL_MANA)?;
        if copy_pool_mana <= 0.0 || !copy_pool_mana.is_finite() {
            bail!("COPY_POOL_MANA must be a finite number greater than 0, got {copy_pool_mana}");
        }
        let copy_daily_mana = f64_or(&get, "COPY_DAILY_MANA", DEFAULT_COPY_DAILY_MANA)?;
        if copy_daily_mana <= 0.0 || !copy_daily_mana.is_finite() {
            bail!("COPY_DAILY_MANA must be a finite number greater than 0, got {copy_daily_mana}");
        }
        let copy_max_age_secs = usize_or(&get, "COPY_MAX_AGE_SECS", DEFAULT_COPY_MAX_AGE_SECS)?;
        if !(1..=300).contains(&copy_max_age_secs) {
            bail!("COPY_MAX_AGE_SECS must be within [1, 300] seconds, got {copy_max_age_secs}");
        }
        let copy_decision_timeout_secs = usize_or(
            &get,
            "COPY_DECISION_TIMEOUT_SECS",
            DEFAULT_COPY_DECISION_TIMEOUT_SECS,
        )?;
        if !(1..=30).contains(&copy_decision_timeout_secs) {
            bail!(
                "COPY_DECISION_TIMEOUT_SECS must be within [1, 30] seconds, got \
                 {copy_decision_timeout_secs}"
            );
        }

        let config = Self {
            jetstream_url,
            cloudflare_account_id,
            triage_model,
            onboarding_model,
            firecrawl_api_key,
            manifold_api_key,
            discord_bot_token,
            discord_guild_id,
            discord_control_channel_id,
            discord_operator_user_id,
            default_market_budget,
            kelly_fraction,
            entry_edge_threshold,
            exit_edge_threshold,
            min_trade_size,
            limit_epsilon,
            limit_expiry_secs,
            limit_drift_cancel,
            belief_kappa,
            belief_halflife_hours,
            triage_min_evidence,
            trading_dry_run,
            mc_max_positions,
            mc_max_new_per_tick,
            mc_max_answer_fraction,
            rate_limit_per_min,
            snipe_enabled,
            snipe_pool_mana,
            snipe_max_mana,
            snipe_min_liq,
            snipe_min_gap,
            snipe_kelly_fraction,
            snipe_ttl_secs,
            snipe_decision_timeout_secs,
            traders_enabled,
            trader_watch_size,
            trader_min_bets,
            trader_refresh_min,
            trader_request_budget,
            copy_enabled,
            copy_min_size,
            copy_min_informed,
            copy_min_edge,
            copy_kelly_fraction,
            copy_max_mana,
            copy_pool_mana,
            copy_daily_mana,
            copy_max_age_secs,
            copy_decision_timeout_secs,
            db_path,
        };

        Ok(config)
    }
}

/// Provider selected by a model name prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelProvider {
    Cloudflare,
    OpenRouter,
    /// OpenRouter's decisions endpoint, which serves System One models.
    OpenRouterDecisions,
}

impl ModelProvider {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cloudflare => "cloudflare",
            Self::OpenRouter => "openrouter",
            Self::OpenRouterDecisions => "openrouter-decisions",
        }
    }
}

/// Matches an ASCII prefix without splitting a multi-byte character.
fn strip_prefix_ci<'a>(text: &'a str, prefix: &str) -> Option<&'a str> {
    let head = text.get(..prefix.len())?;
    head.eq_ignore_ascii_case(prefix)
        .then(|| &text[prefix.len()..])
}

/// Parses a model name prefix to determine the provider and underlying model name.
///
/// Supported prefixes:
/// - `cloudflare/` -> Cloudflare AI Gateway
/// - `openrouter/` -> OpenRouter
/// - `openrouter-decisions/` -> OpenRouter's decisions endpoint
///
/// Models without a recognized provider prefix are rejected.
pub fn parse_model_spec(model: &str) -> AppResult<(ModelProvider, &str)> {
    let trimmed = model.trim();
    if trimmed.is_empty() {
        bail!("model name cannot be empty");
    }

    for (prefix, provider) in [
        ("openrouter-decisions/", ModelProvider::OpenRouterDecisions),
        ("openrouter/", ModelProvider::OpenRouter),
        ("cloudflare/", ModelProvider::Cloudflare),
    ] {
        if let Some(rest) = strip_prefix_ci(trimmed, prefix) {
            let rest = rest.trim();
            if rest.is_empty() {
                bail!("model name after '{prefix}' prefix cannot be empty");
            }
            return Ok((provider, rest));
        }
    }

    bail!(
        "model '{trimmed}' must begin with 'cloudflare/', 'openrouter/', or \
         'openrouter-decisions/' prefix"
    )
}

/// Rejects an account id no URL can carry.
///
/// `CLOUDFLARE_ACCOUNT_ID` is interpolated into the endpoint URL by the rig
/// cloudflare provider, which already refuses anything that could not live in
/// a path segment. The extra guard catches the literal `{account_id}`
/// placeholder from `.env.example` (previously `CF_AI_GATEWAY_URL` carried it)
/// so a setup error fails at config load rather than as a confusing 404 on the
/// first LLM call.
fn validate_cloudflare_account_id(account_id: &str) -> AppResult<()> {
    if account_id.starts_with('{')
        || account_id.contains('/')
        || account_id.chars().any(char::is_whitespace)
    {
        bail!(
            "CLOUDFLARE_ACCOUNT_ID must be a non-empty account id with no `/`, braces, or \
             whitespace; replace the `{{account_id}}` placeholder in .env.example"
        );
    }
    Ok(())
}

/// Returns a trimmed, non-empty value, or `None` when the variable is absent or
/// blank. Blank values are treated as absent so that a default applies.
fn optional(get: &impl Fn(&str) -> Option<String>, key: &str) -> Option<String> {
    get(key)
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

/// Reads a required variable, reporting only its name on failure.
fn required(get: &impl Fn(&str) -> Option<String>, key: &str) -> AppResult<String> {
    match optional(get, key) {
        Some(value) => Ok(value),
        None => Err(anyhow!(
            "missing required environment variable {key}; see .env.example"
        )),
    }
}

/// Reads a required unsigned integer, used for Discord snowflake IDs.
fn required_u64(get: &impl Fn(&str) -> Option<String>, key: &str) -> AppResult<u64> {
    let raw = required(get, key)?;
    raw.parse::<u64>()
        .with_context(|| format!("{key} must be an unsigned 64-bit integer"))
}

/// Reads an optional floating-point variable, falling back to `default`.
fn f64_or(get: &impl Fn(&str) -> Option<String>, key: &str, default: f64) -> AppResult<f64> {
    match optional(get, key) {
        None => Ok(default),
        Some(raw) => raw
            .parse::<f64>()
            .with_context(|| format!("{key} must be a number")),
    }
}

/// Reads an optional boolean variable, falling back to `default`.
///
/// Accepts `true`/`false` in any case and the digits `1`/`0`.
fn bool_or(get: &impl Fn(&str) -> Option<String>, key: &str, default: bool) -> AppResult<bool> {
    match optional(get, key) {
        None => Ok(default),
        Some(raw) => match raw.to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Ok(true),
            "false" | "0" | "no" | "off" => Ok(false),
            _ => Err(anyhow!("{key} must be true or false")),
        },
    }
}

/// Reads an optional non-negative integer variable, falling back to `default`.
fn usize_or(get: &impl Fn(&str) -> Option<String>, key: &str, default: usize) -> AppResult<usize> {
    match optional(get, key) {
        None => Ok(default),
        Some(raw) => raw
            .parse::<usize>()
            .with_context(|| format!("{key} must be a non-negative integer")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Required variables with placeholder values; numeric and path variables
    /// are intentionally omitted so defaults can be asserted.
    fn required_vars() -> Vec<(&'static str, &'static str)> {
        vec![
            ("CLOUDFLARE_ACCOUNT_ID", "test-account"),
            ("TRIAGE_MODEL", "cloudflare/@cf/google/gemini-test"),
            ("ONBOARDING_MODEL", "cloudflare/@cf/deepseek/test"),
            ("FIRECRAWL_API_KEY", "fc-test"),
            ("MANIFOLD_API_KEY", "manifold-key"),
            ("DISCORD_BOT_TOKEN", "discord-token"),
            ("DISCORD_GUILD_ID", "123456789012345678"),
            ("DISCORD_CONTROL_CHANNEL_ID", "223456789012345678"),
            ("DISCORD_OPERATOR_USER_ID", "323456789012345678"),
        ]
    }

    fn config_from(vars: &[(&str, &str)]) -> AppResult<Config> {
        Config::from_lookup(|key| {
            vars.iter()
                .find(|(name, _)| *name == key)
                .map(|(_, value)| (*value).to_owned())
        })
    }

    #[test]
    fn applies_defaults_for_optional_values() {
        let config = config_from(&required_vars()).expect("config should load");

        assert_eq!(config.jetstream_url, DEFAULT_JETSTREAM_URL);
        assert_eq!(config.default_market_budget, 100.0);
        assert_eq!(config.kelly_fraction, 0.25);
        assert_eq!(config.entry_edge_threshold, 0.05);
        assert_eq!(config.exit_edge_threshold, 0.01);
        assert_eq!(config.min_trade_size, 1.0);
        assert_eq!(config.limit_epsilon, 0.01);
        assert_eq!(config.limit_expiry_secs, 60);
        assert_eq!(config.limit_drift_cancel, 0.05);
        assert_eq!(config.belief_kappa, 1.1);
        assert_eq!(config.belief_halflife_hours, 4.0);
        assert_eq!(config.triage_min_evidence, 0.7);
        assert!(config.trading_dry_run, "dry run must default to true");
        assert_eq!(config.mc_max_positions, 6);
        assert_eq!(config.mc_max_new_per_tick, 2);
        assert_eq!(config.mc_max_answer_fraction, 0.25);
        assert_eq!(config.rate_limit_per_min, DEFAULT_RATE_LIMIT_PER_MIN);
        assert!(!config.snipe_enabled, "sniping must default to off");
        assert_eq!(config.snipe_pool_mana, 100.0);
        assert_eq!(config.snipe_max_mana, 10.0);
        assert_eq!(config.snipe_min_liq, 100.0);
        assert_eq!(config.snipe_min_gap, 0.10);
        assert_eq!(config.snipe_kelly_fraction, 0.25);
        assert_eq!(config.snipe_ttl_secs, 3600);
        assert_eq!(config.snipe_decision_timeout_secs, 2);
        assert!(config.traders_enabled, "the read-only leaderboard defaults on");
        assert_eq!(config.trader_watch_size, 200);
        assert_eq!(config.trader_min_bets, 25);
        assert_eq!(config.trader_refresh_min, 60);
        assert_eq!(config.trader_request_budget, 120);
        assert!(!config.copy_enabled, "copy execution must default to off");
        assert_eq!(config.copy_min_size, 5.0);
        assert_eq!(config.copy_min_informed, 0.60);
        assert_eq!(config.copy_min_edge, 0.05);
        assert_eq!(config.copy_kelly_fraction, 0.125);
        assert_eq!(config.copy_max_mana, 10.0);
        assert_eq!(config.copy_pool_mana, 100.0);
        assert_eq!(config.copy_daily_mana, 30.0);
        assert_eq!(config.copy_max_age_secs, 30);
        assert_eq!(config.copy_decision_timeout_secs, 2);
        assert_eq!(config.db_path, PathBuf::from("./data/manif0ld.db"));
    }

    #[test]
    fn parses_explicit_values() {
        let mut vars = required_vars();
        vars.extend([
            ("JETSTREAM_URL", "wss://jetstream.example/subscribe"),
            ("DEFAULT_MARKET_BUDGET", "42.5"),
            ("KELLY_FRACTION", "0.5"),
            ("ENTRY_EDGE_THRESHOLD", "0.1"),
            ("EXIT_EDGE_THRESHOLD", "0.02"),
            ("MIN_TRADE_SIZE", "2.5"),
            ("LIMIT_EPSILON", "0.02"),
            ("LIMIT_EXPIRY_SECS", "120"),
            ("LIMIT_DRIFT_CANCEL", "0.08"),
            ("BELIEF_KAPPA", "2.0"),
            ("BELIEF_HALFLIFE_HOURS", "6.5"),
            ("TRADING_DRY_RUN", "false"),
            ("MC_MAX_POSITIONS", "3"),
            ("MC_MAX_NEW_PER_TICK", "1"),
            ("MC_MAX_ANSWER_FRACTION", "0.5"),
            ("RATE_LIMIT_PER_MIN", "400"),
            ("SNIPE_ENABLED", "true"),
            ("SNIPE_POOL_MANA", "50"),
            ("SNIPE_MAX_MANA", "5"),
            ("SNIPE_MIN_LIQ", "250"),
            ("SNIPE_MIN_GAP", "0.2"),
            ("SNIPE_KELLY_FRACTION", "0.5"),
            ("SNIPE_TTL_SECS", "120"),
            ("SNIPE_DECISION_TIMEOUT_SECS", "5"),
            ("TRADERS_ENABLED", "false"),
            ("TRADER_WATCH_SIZE", "50"),
            ("TRADER_MIN_BETS", "10"),
            ("TRADER_REFRESH_MIN", "15"),
            ("TRADER_REQUEST_BUDGET", "40"),
            ("COPY_ENABLED", "true"),
            ("COPY_MIN_SIZE", "7.5"),
            ("COPY_MIN_INFORMED", "0.7"),
            ("COPY_MIN_EDGE", "0.08"),
            ("COPY_KELLY_FRACTION", "0.2"),
            ("COPY_MAX_MANA", "6"),
            ("COPY_POOL_MANA", "80"),
            ("COPY_DAILY_MANA", "25"),
            ("COPY_MAX_AGE_SECS", "45"),
            ("COPY_DECISION_TIMEOUT_SECS", "4"),
            ("DB_PATH", "/tmp/test.db"),
        ]);

        let config = config_from(&vars).expect("config should load");

        assert_eq!(config.jetstream_url, "wss://jetstream.example/subscribe");
        assert_eq!(config.discord_guild_id, 123_456_789_012_345_678);
        assert_eq!(config.discord_control_channel_id, 223_456_789_012_345_678);
        assert_eq!(config.discord_operator_user_id, 323_456_789_012_345_678);
        assert_eq!(config.default_market_budget, 42.5);
        assert_eq!(config.kelly_fraction, 0.5);
        assert_eq!(config.entry_edge_threshold, 0.1);
        assert_eq!(config.exit_edge_threshold, 0.02);
        assert_eq!(config.min_trade_size, 2.5);
        assert_eq!(config.limit_epsilon, 0.02);
        assert_eq!(config.limit_expiry_secs, 120);
        assert_eq!(config.limit_drift_cancel, 0.08);
        assert_eq!(config.belief_kappa, 2.0);
        assert_eq!(config.belief_halflife_hours, 6.5);
        assert!(!config.trading_dry_run);
        assert_eq!(config.mc_max_positions, 3);
        assert_eq!(config.mc_max_new_per_tick, 1);
        assert_eq!(config.mc_max_answer_fraction, 0.5);
        assert_eq!(config.rate_limit_per_min, 400);
        assert!(config.snipe_enabled);
        assert_eq!(config.snipe_pool_mana, 50.0);
        assert_eq!(config.snipe_max_mana, 5.0);
        assert_eq!(config.snipe_min_liq, 250.0);
        assert_eq!(config.snipe_min_gap, 0.2);
        assert_eq!(config.snipe_kelly_fraction, 0.5);
        assert_eq!(config.snipe_ttl_secs, 120);
        assert_eq!(config.snipe_decision_timeout_secs, 5);
        assert!(!config.traders_enabled);
        assert_eq!(config.trader_watch_size, 50);
        assert_eq!(config.trader_min_bets, 10);
        assert_eq!(config.trader_refresh_min, 15);
        assert_eq!(config.trader_request_budget, 40);
        assert!(config.copy_enabled);
        assert_eq!(config.copy_min_size, 7.5);
        assert_eq!(config.copy_min_informed, 0.7);
        assert_eq!(config.copy_min_edge, 0.08);
        assert_eq!(config.copy_kelly_fraction, 0.2);
        assert_eq!(config.copy_max_mana, 6.0);
        assert_eq!(config.copy_pool_mana, 80.0);
        assert_eq!(config.copy_daily_mana, 25.0);
        assert_eq!(config.copy_max_age_secs, 45);
        assert_eq!(config.copy_decision_timeout_secs, 4);
        assert_eq!(config.db_path, PathBuf::from("/tmp/test.db"));
    }

    #[test]
    fn reports_missing_required_variable_by_name() {
        let mut vars = required_vars();
        vars.retain(|(name, _)| *name != "MANIFOLD_API_KEY");

        let error = config_from(&vars).expect_err("missing secret should fail");
        let message = error.to_string();

        assert!(message.contains("MANIFOLD_API_KEY"), "{message}");
    }

    #[test]
    fn treats_blank_required_value_as_missing() {
        let mut vars = required_vars();
        vars.retain(|(name, _)| *name != "DISCORD_BOT_TOKEN");
        vars.push(("DISCORD_BOT_TOKEN", "   "));

        let error = config_from(&vars).expect_err("blank secret should fail");
        assert!(error.to_string().contains("DISCORD_BOT_TOKEN"));
    }

    #[test]
    fn rejects_malformed_discord_id() {
        let mut vars = required_vars();
        vars.retain(|(name, _)| *name != "DISCORD_GUILD_ID");
        vars.push(("DISCORD_GUILD_ID", "not-a-snowflake"));

        let error = config_from(&vars).expect_err("malformed ID should fail");
        let message = error.to_string();
        assert!(message.contains("DISCORD_GUILD_ID"), "{message}");
        // The offending value must not leak into the error text.
        assert!(!message.contains("not-a-snowflake"), "{message}");
    }

    #[test]
    fn rejects_malformed_numeric_value() {
        let mut vars = required_vars();
        vars.push(("KELLY_FRACTION", "a quarter"));

        let error = config_from(&vars).expect_err("malformed number should fail");
        let message = error.to_string();
        assert!(message.contains("KELLY_FRACTION"), "{message}");
        assert!(!message.contains("a quarter"), "{message}");
    }

    #[test]
    fn rejects_a_zero_rate_limit() {
        let mut vars = required_vars();
        vars.push(("RATE_LIMIT_PER_MIN", "0"));

        let error = config_from(&vars).expect_err("zero budget should fail");
        assert!(error.to_string().contains("RATE_LIMIT_PER_MIN"), "{error}");
    }

    #[test]
    fn rejects_a_limit_epsilon_outside_the_open_unit_interval() {
        // The interval is open: 0 would not undercut fair value, 0.05 and
        // above sit at or past the default entry-edge threshold where a
        // capped sweep can no longer cross the market price (see the field
        // doc). NaN fails the `0 < ε < 0.05` check by construction.
        for value in ["0", "-0.01", "0.05", "0.5", "NaN"] {
            let mut vars = required_vars();
            vars.push(("LIMIT_EPSILON", value));

            let error = config_from(&vars).expect_err("out-of-range epsilon should fail");
            assert!(error.to_string().contains("LIMIT_EPSILON"), "{error}");
        }
    }

    #[test]
    fn rejects_a_limit_expiry_outside_the_one_to_three_hundred_second_band() {
        for value in ["0", "301", "-2"] {
            let mut vars = required_vars();
            vars.push(("LIMIT_EXPIRY_SECS", value));

            let error = config_from(&vars).expect_err("out-of-range expiry should fail");
            assert!(error.to_string().contains("LIMIT_EXPIRY_SECS"), "{error}");
        }
    }

    #[test]
    fn rejects_a_limit_drift_cancel_outside_the_open_unit_interval() {
        for value in ["0", "-0.05", "1", "1.5", "NaN"] {
            let mut vars = required_vars();
            vars.push(("LIMIT_DRIFT_CANCEL", value));

            let error = config_from(&vars).expect_err("out-of-range slack should fail");
            assert!(error.to_string().contains("LIMIT_DRIFT_CANCEL"), "{error}");
        }
    }

    #[test]
    fn rejects_a_malformed_dry_run_flag() {
        let mut vars = required_vars();
        vars.push(("TRADING_DRY_RUN", "sometimes"));

        let error = config_from(&vars).expect_err("malformed flag should fail");
        assert!(error.to_string().contains("TRADING_DRY_RUN"));
    }

    #[test]
    fn rejects_an_answer_fraction_outside_the_unit_interval() {
        for value in ["0", "-0.1", "1.5"] {
            let mut vars = required_vars();
            vars.push(("MC_MAX_ANSWER_FRACTION", value));

            let error = config_from(&vars).expect_err("out-of-range fraction should fail");
            assert!(
                error.to_string().contains("MC_MAX_ANSWER_FRACTION"),
                "{error}"
            );
        }
    }

    #[test]
    fn rejects_a_non_positive_snipe_pool() {
        for value in ["0", "-1", "NaN"] {
            let mut vars = required_vars();
            vars.push(("SNIPE_POOL_MANA", value));

            let error = config_from(&vars).expect_err("non-positive pool should fail");
            assert!(error.to_string().contains("SNIPE_POOL_MANA"), "{error}");
        }
    }

    #[test]
    fn rejects_a_non_positive_snipe_max() {
        for value in ["0", "-1", "NaN"] {
            let mut vars = required_vars();
            vars.push(("SNIPE_MAX_MANA", value));

            let error = config_from(&vars).expect_err("non-positive max should fail");
            assert!(error.to_string().contains("SNIPE_MAX_MANA"), "{error}");
        }
    }

    #[test]
    fn rejects_a_negative_snipe_min_liquidity() {
        for value in ["-1", "-0.01", "NaN"] {
            let mut vars = required_vars();
            vars.push(("SNIPE_MIN_LIQ", value));

            let error = config_from(&vars).expect_err("negative liquidity should fail");
            assert!(error.to_string().contains("SNIPE_MIN_LIQ"), "{error}");
        }
        // Zero is allowed: it only disables the liquidity gate.
        let mut vars = required_vars();
        vars.push(("SNIPE_MIN_LIQ", "0"));
        assert_eq!(
            config_from(&vars).expect("zero liquidity is valid").snipe_min_liq,
            0.0
        );
    }

    #[test]
    fn rejects_a_snipe_min_gap_outside_the_open_unit_interval() {
        for value in ["0", "-0.1", "1", "1.1", "NaN"] {
            let mut vars = required_vars();
            vars.push(("SNIPE_MIN_GAP", value));

            let error = config_from(&vars).expect_err("out-of-range gap should fail");
            assert!(error.to_string().contains("SNIPE_MIN_GAP"), "{error}");
        }
    }

    #[test]
    fn rejects_a_snipe_kelly_fraction_outside_the_half_open_unit_interval() {
        for value in ["0", "-0.1", "1.1", "NaN"] {
            let mut vars = required_vars();
            vars.push(("SNIPE_KELLY_FRACTION", value));

            let error = config_from(&vars).expect_err("out-of-range fraction should fail");
            assert!(
                error.to_string().contains("SNIPE_KELLY_FRACTION"),
                "{error}"
            );
        }
        // 1.0 is the closed upper bound and is valid.
        let mut vars = required_vars();
        vars.push(("SNIPE_KELLY_FRACTION", "1"));
        assert_eq!(
            config_from(&vars).expect("full Kelly is valid").snipe_kelly_fraction,
            1.0
        );
    }

    #[test]
    fn rejects_a_zero_snipe_ttl() {
        let mut vars = required_vars();
        vars.push(("SNIPE_TTL_SECS", "0"));

        let error = config_from(&vars).expect_err("zero TTL should fail");
        assert!(error.to_string().contains("SNIPE_TTL_SECS"), "{error}");
    }

    #[test]
    fn rejects_a_snipe_ttl_above_one_week() {
        let mut vars = required_vars();
        vars.push(("SNIPE_TTL_SECS", "604801"));

        let error = config_from(&vars).expect_err("a week plus a second should fail");
        assert!(error.to_string().contains("SNIPE_TTL_SECS"), "{error}");

        vars.pop();
        vars.push(("SNIPE_TTL_SECS", "604800"));
        assert_eq!(
            config_from(&vars).expect("one week is the ceiling").snipe_ttl_secs,
            604800
        );
    }

    #[test]
    fn rejects_a_snipe_decision_timeout_outside_the_one_to_thirty_second_band() {
        for value in ["0", "31", "-2"] {
            let mut vars = required_vars();
            vars.push(("SNIPE_DECISION_TIMEOUT_SECS", value));

            let error = config_from(&vars).expect_err("out-of-range timeout should fail");
            assert!(
                error.to_string().contains("SNIPE_DECISION_TIMEOUT_SECS"),
                "{error}"
            );
        }
    }

    #[test]
    fn rejects_a_malformed_snipe_enabled_flag() {
        let mut vars = required_vars();
        vars.push(("SNIPE_ENABLED", "sometimes"));

        let error = config_from(&vars).expect_err("malformed flag should fail");
        assert!(error.to_string().contains("SNIPE_ENABLED"), "{error}");
    }

    #[test]
    fn rejects_a_trader_watch_size_outside_the_ten_to_thousand_band() {
        for value in ["9", "1001"] {
            let mut vars = required_vars();
            vars.push(("TRADER_WATCH_SIZE", value));

            let error = config_from(&vars).expect_err("out-of-range watch size should fail");
            assert!(error.to_string().contains("TRADER_WATCH_SIZE"), "{error}");
        }
    }

    #[test]
    fn accepts_the_trader_watch_size_band_edges() {
        for value in ["10", "1000"] {
            let mut vars = required_vars();
            vars.push(("TRADER_WATCH_SIZE", value));

            let config = config_from(&vars).expect("band edge is valid");
            assert_eq!(config.trader_watch_size, value.parse::<usize>().unwrap());
        }
    }

    #[test]
    fn rejects_a_zero_trader_min_bets() {
        let mut vars = required_vars();
        vars.push(("TRADER_MIN_BETS", "0"));

        let error = config_from(&vars).expect_err("zero bets should fail");
        assert!(error.to_string().contains("TRADER_MIN_BETS"), "{error}");
    }

    #[test]
    fn rejects_a_trader_refresh_interval_outside_the_five_minute_to_one_day_band() {
        for value in ["4", "1441"] {
            let mut vars = required_vars();
            vars.push(("TRADER_REFRESH_MIN", value));

            let error = config_from(&vars).expect_err("out-of-range interval should fail");
            assert!(error.to_string().contains("TRADER_REFRESH_MIN"), "{error}");
        }
    }

    #[test]
    fn rejects_a_trader_request_budget_below_the_universe_cost() {
        let mut vars = required_vars();
        vars.push(("TRADER_REQUEST_BUDGET", "19"));

        let error = config_from(&vars).expect_err("budget below 20 should fail");
        assert!(error.to_string().contains("TRADER_REQUEST_BUDGET"), "{error}");
    }

    #[test]
    fn rejects_a_malformed_traders_enabled_flag() {
        let mut vars = required_vars();
        vars.push(("TRADERS_ENABLED", "sometimes"));

        let error = config_from(&vars).expect_err("malformed flag should fail");
        assert!(error.to_string().contains("TRADERS_ENABLED"), "{error}");
    }

    #[test]
    fn rejects_copy_thresholds_outside_their_intervals() {
        for (key, value) in [
            ("COPY_MIN_INFORMED", "0"),
            ("COPY_MIN_INFORMED", "1"),
            ("COPY_MIN_INFORMED", "NaN"),
            ("COPY_MIN_EDGE", "0"),
            ("COPY_MIN_EDGE", "1"),
            ("COPY_MIN_EDGE", "NaN"),
            ("COPY_KELLY_FRACTION", "0"),
            ("COPY_KELLY_FRACTION", "0.51"),
            ("COPY_KELLY_FRACTION", "NaN"),
        ] {
            let mut vars = required_vars();
            vars.push((key, value));

            let error = config_from(&vars).expect_err("out-of-range copy threshold should fail");
            assert!(error.to_string().contains(key), "{error}");
        }

        // The half-open upper bound of the Kelly fraction is valid.
        let mut vars = required_vars();
        vars.push(("COPY_KELLY_FRACTION", "0.5"));
        assert_eq!(
            config_from(&vars)
                .expect("half Kelly is valid")
                .copy_kelly_fraction,
            0.5
        );
    }

    #[test]
    fn rejects_non_positive_copy_budgets() {
        for key in ["COPY_MAX_MANA", "COPY_POOL_MANA", "COPY_DAILY_MANA"] {
            for value in ["0", "-1", "NaN"] {
                let mut vars = required_vars();
                vars.push((key, value));

                let error = config_from(&vars).expect_err("non-positive budget should fail");
                assert!(error.to_string().contains(key), "{error}");
            }
        }

        let mut vars = required_vars();
        vars.push(("COPY_MIN_SIZE", "-1"));
        let error = config_from(&vars).expect_err("negative minimum size should fail");
        assert!(error.to_string().contains("COPY_MIN_SIZE"), "{error}");
    }

    #[test]
    fn rejects_copy_timing_values_outside_their_bands() {
        for value in ["0", "301", "-2"] {
            let mut vars = required_vars();
            vars.push(("COPY_MAX_AGE_SECS", value));

            let error = config_from(&vars).expect_err("out-of-range age should fail");
            assert!(error.to_string().contains("COPY_MAX_AGE_SECS"), "{error}");
        }
        for value in ["0", "31", "-2"] {
            let mut vars = required_vars();
            vars.push(("COPY_DECISION_TIMEOUT_SECS", value));

            let error = config_from(&vars).expect_err("out-of-range timeout should fail");
            assert!(
                error.to_string().contains("COPY_DECISION_TIMEOUT_SECS"),
                "{error}"
            );
        }
    }

    #[test]
    fn rejects_a_malformed_copy_enabled_flag() {
        let mut vars = required_vars();
        vars.push(("COPY_ENABLED", "sometimes"));

        let error = config_from(&vars).expect_err("malformed flag should fail");
        assert!(error.to_string().contains("COPY_ENABLED"), "{error}");
    }

    #[test]
    fn stores_cloudflare_account_id_and_models() {
        let config = config_from(&required_vars()).expect("config should load");

        assert_eq!(
            config.cloudflare_account_id.as_deref(),
            Some("test-account")
        );
        assert_eq!(config.triage_model, "cloudflare/@cf/google/gemini-test");
        assert_eq!(config.onboarding_model, "cloudflare/@cf/deepseek/test");
    }

    #[test]
    fn parses_openrouter_configuration() {
        let mut vars = required_vars();
        vars.retain(|(name, _)| {
            *name != "CLOUDFLARE_ACCOUNT_ID"
                && *name != "TRIAGE_MODEL"
                && *name != "ONBOARDING_MODEL"
        });
        vars.push(("TRIAGE_MODEL", "openrouter/google/gemini-2.0-flash-001"));
        vars.push(("ONBOARDING_MODEL", "openrouter/anthropic/claude-3.5-sonnet"));
        vars.push(("OPENROUTER_API_KEY", "sk-or-test-key"));

        let config = config_from(&vars).expect("openrouter config should load");
        assert_eq!(config.cloudflare_account_id, None);
        assert_eq!(
            config.triage_model,
            "openrouter/google/gemini-2.0-flash-001"
        );
        assert_eq!(
            config.onboarding_model,
            "openrouter/anthropic/claude-3.5-sonnet"
        );
    }

    #[test]
    fn rejects_missing_openrouter_key() {
        let mut vars = required_vars();
        vars.retain(|(name, _)| *name != "TRIAGE_MODEL");
        vars.push(("TRIAGE_MODEL", "openrouter/google/gemini-2.0-flash-001"));

        let error = config_from(&vars).expect_err("missing OPENROUTER_API_KEY should fail");
        assert!(error.to_string().contains("OPENROUTER_API_KEY"));
    }

    #[test]
    fn rejects_missing_firecrawl_key() {
        let mut vars = required_vars();
        vars.retain(|(name, _)| *name != "FIRECRAWL_API_KEY");

        let error = config_from(&vars).expect_err("missing Firecrawl key should fail");
        assert!(error.to_string().contains("FIRECRAWL_API_KEY"));
    }

    #[test]
    fn parses_model_prefixes() {
        let (p, m) = parse_model_spec("openrouter/anthropic/claude").unwrap();
        assert_eq!(p, ModelProvider::OpenRouter);
        assert_eq!(m, "anthropic/claude");

        let (p, m) = parse_model_spec("cloudflare/@cf/meta/llama").unwrap();
        assert_eq!(p, ModelProvider::Cloudflare);
        assert_eq!(m, "@cf/meta/llama");

        let (p, m) = parse_model_spec("openrouter-decisions/baseten/joey-1").unwrap();
        assert_eq!(p, ModelProvider::OpenRouterDecisions);
        assert_eq!(m, "baseten/joey-1");

        // Models without a prefix must error
        assert!(parse_model_spec("@cf/meta/llama").is_err());
        assert!(parse_model_spec("gpt-4o").is_err());
        assert!(parse_model_spec("openrouter/").is_err());
        assert!(parse_model_spec("cloudflare/").is_err());
        assert!(parse_model_spec("openrouter-decisions/").is_err());
        assert!(parse_model_spec("").is_err());
    }

    #[test]
    fn parses_decisions_triage_configuration() {
        let mut vars = required_vars();
        vars.retain(|(name, _)| *name != "TRIAGE_MODEL");
        vars.push(("TRIAGE_MODEL", "openrouter-decisions/baseten/joey-1"));
        vars.push(("OPENROUTER_API_KEY", "sk-or-test-key"));

        let config = config_from(&vars).expect("decisions triage config should load");
        assert_eq!(config.triage_model, "openrouter-decisions/baseten/joey-1");
    }

    #[test]
    fn rejects_decisions_onboarding_model() {
        let mut vars = required_vars();
        vars.retain(|(name, _)| *name != "ONBOARDING_MODEL");
        vars.push(("ONBOARDING_MODEL", "openrouter-decisions/baseten/joey-1"));
        vars.push(("OPENROUTER_API_KEY", "sk-or-test-key"));

        let error = config_from(&vars).expect_err("decisions onboarding should fail");
        assert!(error.to_string().contains("ONBOARDING_MODEL"), "{error}");
    }

    #[test]
    fn rejects_missing_openrouter_key_for_decisions_triage() {
        let mut vars = required_vars();
        vars.retain(|(name, _)| *name != "TRIAGE_MODEL");
        vars.push(("TRIAGE_MODEL", "openrouter-decisions/baseten/joey-1"));

        let error = config_from(&vars).expect_err("missing OPENROUTER_API_KEY should fail");
        assert!(error.to_string().contains("OPENROUTER_API_KEY"), "{error}");
    }

    #[test]
    fn rejects_placeholder_account_id() {
        let mut vars = required_vars();
        vars.retain(|(name, _)| *name != "CLOUDFLARE_ACCOUNT_ID");
        vars.push(("CLOUDFLARE_ACCOUNT_ID", "{account_id}"));

        let error = config_from(&vars).expect_err("placeholder should fail");
        assert!(
            error.to_string().contains("CLOUDFLARE_ACCOUNT_ID"),
            "{error}"
        );
    }

    #[test]
    fn rejects_account_id_that_cannot_live_in_a_url() {
        for bad in ["has whitespace", "has/slash"] {
            let mut vars = required_vars();
            vars.retain(|(name, _)| *name != "CLOUDFLARE_ACCOUNT_ID");
            vars.push(("CLOUDFLARE_ACCOUNT_ID", bad));

            let error = config_from(&vars).expect_err("unusable account id should fail");
            assert!(
                error.to_string().contains("CLOUDFLARE_ACCOUNT_ID"),
                "{bad}: {error}"
            );
        }
    }

    #[test]
    fn negative_discord_id_is_rejected() {
        let mut vars = required_vars();
        vars.retain(|(name, _)| *name != "DISCORD_OPERATOR_USER_ID");
        vars.push(("DISCORD_OPERATOR_USER_ID", "-1"));

        let error = config_from(&vars).expect_err("negative snowflake should fail");
        assert!(error.to_string().contains("DISCORD_OPERATOR_USER_ID"));
    }
}
