//! Manifold trading: HTTP client, Kelly sizing, decisions, and execution.
//!
//! The module has three layers, each testable on its own:
//!
//! 1. [`ManifoldClient`] wraps the REST API (`/v0/me`, `/v0/market/{id}`,
//!    `/v0/market/{id}/positions`, `/v0/bet`, `/v0/market/{id}/sell`).
//! 2. Pure functions: [`kelly_target_mana`], [`decide_trade`], [`apply_buy`],
//!    and [`apply_sell`]. They take owned state and return decisions or mutate a
//!    [`TrackedMarket`] in place, so they need no network, database, or clock.
//! 3. [`TradeExecutor`] wires the two together: it reconciles the local
//!    position against Manifold, computes the belief-derived edge, executes at
//!    most one action, and persists the result.
//!
//! # Positions, not bets, for reconciliation
//!
//! Position sync uses `GET /v0/market/{id}/positions?userId=...` and not the
//! bet history. A bet response cannot reliably distinguish shares that are
//! still held from shares sold later (sell rows carry negative deltas and the
//! history endpoint is paginated), so summing `/v0/bets` rows drifts after the
//! first exit. Manifold's `ContractMetric` reports `totalShares`, `invested`,
//! and `profit` for the current position directly; that is the authoritative
//! source, and any mismatch is resolved in Manifold's favour with a `warn!`.
//!
//! # Timestamps
//!
//! Manifold reports every time in Unix milliseconds; the database stores Unix
//! seconds. [`millis_to_seconds`] is the single conversion point at this
//! boundary.
//!
//! # Secrets
//!
//! The API key is stored in [`ManifoldClient`] and sent in the `Authorization`
//! header. It never appears in an error, a log line, or the `Debug` output.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::{broadcast, Mutex};

use crate::belief;
use crate::config::Config;
use crate::db::Db;
use crate::error::{bail, AppResult, Context};
use crate::pipeline::{MarketCache, PipelineEvent};
use crate::types::{
    available_budget, MarketId, MarketStatus, TrackedMarket, TradeAction, TradeDecision,
    TradeRecord,
};

/// Public Manifold API base URL.
pub const MANIFOLD_BASE_URL: &str = "https://api.manifold.markets";

/// Timeout for one Manifold request.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);

/// Longest response snippet included in an error message.
const ERROR_SNIPPET_CHARS: usize = 500;

/// How often [`TradeExecutor::run`] reconciles every active market.
const SYNC_INTERVAL: Duration = Duration::from_secs(60);

/// Key in the `kv` table holding the panic switch.
///
/// Published so the Discord `/panic` command can write the same key the trading
/// layer reads. The switch is enabled only by writing the literal `true`; there
/// is deliberately no disable command, operators clear it by editing the entry.
pub(crate) const KILL_SWITCH_KEY: &str = "global_kill_switch";

/// Capacity of the trade event stream consumed by the Discord layer.
const EVENT_CAPACITY: usize = 256;

/// Limit price that emulates a market buy of YES shares.
const YES_LIMIT_PROB: f64 = 0.99;

/// Limit price that emulates a market buy of NO shares.
const NO_LIMIT_PROB: f64 = 0.01;

/// Kelly fractions are capped at half of the available budget.
const MAX_KELLY_FRACTION: f64 = 0.5;

/// Probability bounds used by the sizing and decision math.
///
/// A market probability of exactly 0 or 1 makes Kelly division and log-odds
/// undefined, so the trading layer treats the probability as bounded away from
/// those endpoints.
const MIN_TRADING_PROB: f64 = 0.01;
const MAX_TRADING_PROB: f64 = 0.99;

/// Tolerance for the budget invariant, in mana. Floating-point sums of budgets,
/// PnL, and invested amounts can differ by a few ULPs from zero.
const BUDGET_EPSILON: f64 = 1e-9;

/// Difference between local and remote share counts that is still considered
/// equal. Manifold reports fractional shares, so an exact comparison would warn
/// on every tick.
const POSITION_EPSILON: f64 = 1e-6;

// -----------------------------------------------------------------------------
// HTTP client
// -----------------------------------------------------------------------------

/// Minimal authenticated client for the Manifold REST API.
///
/// Cloneable so the executor can hand it to helpers without borrowing `self`.
/// The underlying `reqwest::Client` pools connections across clones.
#[derive(Clone)]
pub struct ManifoldClient {
    http: reqwest::Client,
    base_url: String,
    api_key: String,
    bot_user_id: String,
}

/// Redacts the API key so `tracing` and error chains can never leak it.
impl fmt::Debug for ManifoldClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManifoldClient")
            .field("base_url", &self.base_url)
            .field("bot_user_id", &self.bot_user_id)
            .field("api_key", &"<redacted>")
            .finish()
    }
}

/// Response shape of `GET /v0/me`; only the stable fields are read.
#[derive(Debug, Deserialize)]
struct MeResponse {
    id: String,
}

impl ManifoldClient {
    /// Builds a client and resolves the bot's user ID from `GET /v0/me`.
    ///
    /// Startup fails here rather than on the first trade if the API key is
    /// rejected or Manifold is unreachable. The error includes the HTTP status
    /// and a short body excerpt, never the key.
    pub async fn new(config: &Config) -> AppResult<Self> {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .context("building Manifold HTTP client")?;

        let client = Self {
            http,
            base_url: MANIFOLD_BASE_URL.to_owned(),
            api_key: config.manifold_api_key.clone(),
            bot_user_id: String::new(),
        };

        let me: MeResponse = client
            .send(client.http.get(client.url("/v0/me")), "GET /v0/me")
            .await
            .context("fetching the Manifold bot identity from /v0/me")?;

        if me.id.trim().is_empty() {
            bail!("Manifold /v0/me returned an empty user id");
        }

        Ok(Self {
            bot_user_id: me.id,
            ..client
        })
    }

    /// The authenticated Manifold user ID, used for position queries.
    pub fn bot_user_id(&self) -> &str {
        &self.bot_user_id
    }

    /// Fetches the current state of one market.
    pub async fn market(&self, market_id: &str) -> AppResult<MarketSnapshot> {
        let path = format!("/v0/market/{}", market_id);
        let request = self.http.get(self.url(&path));
        self.send(request, "GET /v0/market/{id}").await
    }

    /// Fetches the bot's position in one market.
    ///
    /// A missing array or a missing entry means the bot holds nothing, which is
    /// represented as a zero [`PositionSnapshot`] rather than an error.
    pub async fn position(&self, market_id: &str) -> AppResult<PositionSnapshot> {
        let path = format!(
            "/v0/market/{}/positions?userId={}",
            market_id, self.bot_user_id
        );
        let request = self.http.get(self.url(&path));
        let value: Value = self.send(request, "GET /v0/market/{id}/positions").await?;
        Ok(parse_position(&value, &self.bot_user_id))
    }

    /// Buys `amount` mana of `outcome` shares at the best available price.
    ///
    /// `limitProb` is placed far beyond the current price so the order behaves
    /// like a market buy: `0.99` for YES and `0.01` for NO. Manifold interprets
    /// the limit as a probability of YES, so a NO buy needs the low limit.
    pub async fn buy(
        &self,
        market_id: &str,
        outcome: &str,
        amount: f64,
        answer_id: Option<&str>,
    ) -> AppResult<BetResult> {
        let outcome = validate_outcome(outcome)?;
        if !amount.is_finite() || amount <= 0.0 {
            bail!("buy amount must be a positive finite mana value, got {amount}");
        }

        let limit_prob = match outcome {
            "YES" => YES_LIMIT_PROB,
            _ => NO_LIMIT_PROB,
        };
        let mut body = json!({
            "contractId": market_id,
            "amount": amount,
            "outcome": outcome,
            "limitProb": limit_prob,
        });
        if let Some(aid) = answer_id {
            body["answerId"] = json!(aid);
        }
        let request = self
            .http
            .post(self.url("/v0/bet"))
            .header(CONTENT_TYPE, "application/json")
            .body(body.to_string());

        self.send(request, "POST /v0/bet").await
    }

    /// Sells `shares` of `outcome`.
    pub async fn sell(
        &self,
        market_id: &str,
        outcome: &str,
        shares: f64,
        answer_id: Option<&str>,
    ) -> AppResult<BetResult> {
        let outcome = validate_outcome(outcome)?;
        if !shares.is_finite() || shares <= 0.0 {
            bail!("sell shares must be a positive finite number, got {shares}");
        }

        let mut body = json!({ "outcome": outcome, "shares": shares });
        if let Some(aid) = answer_id {
            body["answerId"] = json!(aid);
        }
        let path = format!("/v0/market/{}/sell", market_id);
        let request = self
            .http
            .post(self.url(&path))
            .header(CONTENT_TYPE, "application/json")
            .body(body.to_string());

        self.send(request, "POST /v0/market/{id}/sell").await
    }

    /// Returns raw bet history for the bot in one market.
    ///
    /// This is a diagnostic helper only. Reconciliation uses
    /// [`ManifoldClient::position`] because bet rows cannot distinguish held
    /// shares from shares sold later.
    pub async fn bets(&self, market_id: &str) -> AppResult<Vec<Value>> {
        let path = format!(
            "/v0/bets?contractId={}&userId={}",
            market_id, self.bot_user_id
        );
        let request = self.http.get(self.url(&path));
        let value: Value = self.send(request, "GET /v0/bets").await?;

        match value {
            Value::Array(items) => Ok(items),
            Value::Object(mut map) => match map.remove("bets") {
                Some(Value::Array(items)) => Ok(items),
                _ => bail!("Manifold GET /v0/bets response did not contain a bet array"),
            },
            _ => bail!("Manifold GET /v0/bets response was not an array"),
        }
    }

    /// Joins a path onto the configured base URL.
    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url.trim_end_matches('/'), path)
    }

    /// Sends an authenticated request and deserializes a successful response.
    ///
    /// Non-success responses become errors that carry the status and a truncated
    /// body. The body is included because Manifold explains rejections there;
    /// it never contains our credentials.
    async fn send<T: DeserializeOwned>(
        &self,
        request: reqwest::RequestBuilder,
        description: &str,
    ) -> AppResult<T> {
        let response = request
            .header(AUTHORIZATION, format!("Key {}", self.api_key))
            .send()
            .await
            .with_context(|| format!("sending {description} to Manifold"))?;

        let status = response.status();
        let raw = response
            .text()
            .await
            .with_context(|| format!("reading the Manifold response body for {description}"))?;

        if !status.is_success() {
            bail!(
                "Manifold {description} failed with HTTP {status}: {}",
                snippet(&raw)
            );
        }

        serde_json::from_str(&raw).with_context(|| {
            format!(
                "parsing the Manifold {description} response as JSON: {}",
                snippet(&raw)
            )
        })
    }
}

/// Rejects outcomes other than `YES` and `NO`.
fn validate_outcome(outcome: &str) -> AppResult<&str> {
    match outcome {
        "YES" | "NO" => Ok(outcome),
        other => bail!("Manifold outcome must be YES or NO, got {other:?}"),
    }
}

// -----------------------------------------------------------------------------
// Wire types and position parsing
// -----------------------------------------------------------------------------

/// Subset of a Manifold market that the trading layer reads.
///
/// `close_time_ms` is in Unix milliseconds; convert with [`millis_to_seconds`]
/// before storing it.
///
/// `probability` is absent on non-binary outcome types. All tracked markets are
/// validated as `BINARY` at onboarding, so `None` here would indicate an
/// unexpected API change; the trading layer falls back to the last cached
/// probability in that case.
#[derive(Debug, Clone, Deserialize)]
pub struct MarketSnapshot {
    pub id: String,
    pub question: String,
    /// `None` on non-BINARY markets (e.g. `MULTIPLE_CHOICE`). Tracked markets
    /// are always BINARY, but the field is optional for API robustness.
    #[serde(default)]
    pub probability: Option<f64>,
    #[serde(rename = "outcomeType")]
    pub outcome_type: String,
    #[serde(rename = "isResolved")]
    pub is_resolved: bool,
    #[serde(rename = "closeTime")]
    pub close_time_ms: Option<i64>,
    #[serde(rename = "textDescription", default)]
    pub text_description: String,
    #[serde(default)]
    pub answers: Option<Vec<MarketAnswerSnapshot>>,
}

/// One answer from a multiple choice market snapshot.
#[derive(Debug, Clone, Deserialize)]
pub struct MarketAnswerSnapshot {
    pub id: String,
    pub text: String,
    pub probability: Option<f64>,
}

/// The bot's current position in one market.
///
/// `invested` is Manifold's cost basis for the shares still held. `profit` is
/// Manifold's total profit (realized plus unrealized), so it is diagnostic
/// only: the local `realized_pnl_mana` is not overwritten from it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PositionSnapshot {
    pub yes_shares: f64,
    pub no_shares: f64,
    pub invested: f64,
    pub profit: f64,
}

/// Fees charged on a bet, in mana.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct BetFees {
    #[serde(rename = "creatorFee", default)]
    pub creator_fee: f64,
    #[serde(rename = "platformFee", default)]
    pub platform_fee: f64,
    #[serde(rename = "liquidityFee", default)]
    pub liquidity_fee: f64,
}

impl BetFees {
    /// Sum of all fee components.
    fn total(&self) -> f64 {
        self.creator_fee + self.platform_fee + self.liquidity_fee
    }
}

/// Response of `POST /v0/bet` and `POST /v0/market/{id}/sell`.
///
/// Manifold returns the bet identifier as `id` on single bets and as `betId` on
/// bet-like objects (for example the multi-bet payload). Both are accepted;
/// [`BetResult::bet_id`] picks whichever is present.
#[derive(Debug, Clone, Deserialize)]
pub struct BetResult {
    #[serde(default)]
    pub id: String,
    #[serde(default, rename = "betId")]
    pub bet_id: String,
    pub amount: f64,
    pub shares: f64,
    #[serde(rename = "probBefore", default)]
    pub prob_before: Option<f64>,
    #[serde(rename = "probAfter", default)]
    pub prob_after: Option<f64>,
    #[serde(rename = "isFilled", default)]
    pub is_filled: bool,
    #[serde(rename = "isCancelled", default)]
    pub is_cancelled: bool,
    #[serde(default)]
    pub fees: Option<BetFees>,
}

impl BetResult {
    /// The bet identifier, preferring `id` over `betId`.
    pub fn bet_id(&self) -> &str {
        if self.id.is_empty() {
            &self.bet_id
        } else {
            &self.id
        }
    }

    /// Total mana paid by the bot, including fees, as a positive number.
    ///
    /// For a buy, `amount` is the stake and the fees are added. For a sell
    /// Manifold may report the returned mana as a negative `amount`; the
    /// absolute value keeps the result a magnitude either way.
    pub fn total_cost(&self) -> f64 {
        (self.amount + self.fees_total()).abs()
    }

    /// Mana received from a sell, net of fees.
    ///
    /// Sell responses report the mana delta in `amount`, which may be negative.
    /// Fees reduce what the bot actually keeps, so they are subtracted here.
    pub fn net_proceeds(&self) -> f64 {
        (self.amount.abs() - self.fees_total()).max(0.0)
    }

    fn fees_total(&self) -> f64 {
        self.fees.as_ref().map_or(0.0, BetFees::total)
    }
}

/// Parses the `GET /v0/market/{id}/positions` payload for one user.
///
/// The endpoint returns an array of `ContractMetric`, one entry per bettor. The
/// entry whose `userId` matches `bot_user_id` and (if specified) `answerId` is used.
/// For binary or root market positions, entries with `answerId: null` or absent are selected.
pub(crate) fn parse_position(value: &Value, bot_user_id: &str) -> PositionSnapshot {
    parse_answer_position(value, bot_user_id, None)
}

/// Parses the position for a specific answer (or the root contract if `target_answer_id` is None).
pub(crate) fn parse_answer_position(
    value: &Value,
    bot_user_id: &str,
    target_answer_id: Option<&str>,
) -> PositionSnapshot {
    let entries: &[Value] = match value {
        Value::Array(items) => items.as_slice(),
        Value::Object(map) => map
            .get("positions")
            .and_then(Value::as_array)
            .map(|items| items.as_slice())
            .unwrap_or(&[]),
        _ => &[],
    };

    let selected = entries.iter().find(|entry| {
        let user_matches = entry.get("userId").and_then(Value::as_str) == Some(bot_user_id)
            || (entries.len() == 1 && entry.get("userId").is_none());
        if !user_matches {
            return false;
        }
        let answer_id = entry.get("answerId").and_then(Value::as_str);
        match target_answer_id {
            Some(target) => answer_id == Some(target),
            None => answer_id.is_none(),
        }
    }).or_else(|| {
        // Fallback for single-entry responses without answerId
        if target_answer_id.is_none() && entries.len() == 1 {
            entries.first()
        } else {
            None
        }
    });

    let Some(entry) = selected else {
        return PositionSnapshot::default();
    };

    PositionSnapshot {
        yes_shares: number_at(entry.pointer("/totalShares/YES")),
        no_shares: number_at(entry.pointer("/totalShares/NO")),
        invested: number_at(entry.get("invested")),
        profit: number_at(entry.get("profit")),
    }
}

/// Reads a JSON number, defaulting to zero for missing or non-numeric fields.
fn number_at(value: Option<&Value>) -> f64 {
    value.and_then(Value::as_f64).unwrap_or(0.0)
}

/// Converts Unix milliseconds to Unix seconds, flooring toward negative
/// infinity so pre-epoch timestamps stay ordered.
pub fn millis_to_seconds(millis: i64) -> i64 {
    millis.div_euclid(1_000)
}

/// Converts an optional Unix-millisecond timestamp to Unix seconds.
pub fn optional_millis_to_seconds(millis: Option<i64>) -> Option<i64> {
    millis.map(millis_to_seconds)
}

// -----------------------------------------------------------------------------
// Sizing and decisions
// -----------------------------------------------------------------------------

/// Trading parameters, usually sourced from [`Config`].
#[derive(Debug, Clone, Copy)]
pub struct TradeParams {
    pub kelly_fraction: f64,
    pub entry_edge_threshold: f64,
    pub exit_edge_threshold: f64,
    pub min_trade_size: f64,
}

impl TradeParams {
    /// Reads the trading parameters from process configuration.
    pub fn from_config(config: &Config) -> Self {
        Self {
            kelly_fraction: config.kelly_fraction,
            entry_edge_threshold: config.entry_edge_threshold,
            exit_edge_threshold: config.exit_edge_threshold,
            min_trade_size: config.min_trade_size,
        }
    }
}

/// Fractional-Kelly target size in mana for one entry.
///
/// Follows spec §6.6:
///
/// ```text
/// bullish YES: f = lambda * edge / (1 - p_market)
/// bullish NO:  f = lambda * |edge| / p_market
/// f = clamp(f, 0, 0.5);  target = f * available_budget
/// ```
///
/// Returns zero when the edge does not reach the entry threshold, when the
/// budget is non-positive, or when the result would not be finite. `p_market`
/// is clamped to `[0.01, 0.99]` to keep the denominators away from zero.
pub fn kelly_target_mana(
    edge: f64,
    p_market: f64,
    available_budget: f64,
    params: TradeParams,
) -> f64 {
    if !available_budget.is_finite() || available_budget <= 0.0 {
        return 0.0;
    }
    if !edge.is_finite() {
        return 0.0;
    }
    let p_market = p_market.clamp(MIN_TRADING_PROB, MAX_TRADING_PROB);

    let fraction = if edge >= params.entry_edge_threshold {
        params.kelly_fraction * edge / (1.0 - p_market)
    } else if edge <= -params.entry_edge_threshold {
        params.kelly_fraction * edge.abs() / p_market
    } else {
        return 0.0;
    };

    if !fraction.is_finite() {
        return 0.0;
    }

    fraction.clamp(0.0, MAX_KELLY_FRACTION) * available_budget
}

/// Chooses at most one trade for a market, following spec §6.6.
///
/// Exits are evaluated first and are all-or-nothing. Entries happen only when
/// both share counts are zero, so a position is never scaled into and never
/// held in both directions. A sell decision carries `shares: Some(count)`; a
/// buy decision carries the Kelly target in `amount_mana` and `shares: None`.
///
/// Exit `amount_mana` is zero because the proceeds are unknown until Manifold
/// fills the sell; the executed trade record stores the real proceeds.
pub fn decide_trade(
    market: &TrackedMarket,
    p_internal: f64,
    p_market: f64,
    params: TradeParams,
) -> Option<TradeDecision> {
    let edge = p_internal - p_market;

    if market.yes_shares > 0.0 && edge < params.exit_edge_threshold {
        return Some(TradeDecision {
            action: TradeAction::SellYes,
            amount_mana: 0.0,
            shares: Some(market.yes_shares),
            answer_id: None,
        });
    }

    if market.no_shares > 0.0 && edge > -params.exit_edge_threshold {
        return Some(TradeDecision {
            action: TradeAction::SellNo,
            amount_mana: 0.0,
            shares: Some(market.no_shares),
            answer_id: None,
        });
    }

    // A position that did not exit stays open. No scale-in.
    if market.yes_shares > 0.0 || market.no_shares > 0.0 {
        return None;
    }

    let target = kelly_target_mana(edge, p_market, available_budget(market), params);
    if target < params.min_trade_size {
        return None;
    }

    if edge > params.entry_edge_threshold {
        Some(TradeDecision {
            action: TradeAction::BuyYes,
            amount_mana: target,
            shares: None,
            answer_id: None,
        })
    } else if edge < -params.entry_edge_threshold {
        Some(TradeDecision {
            action: TradeAction::BuyNo,
            amount_mana: target,
            shares: None,
            answer_id: None,
        })
    } else {
        None
    }
}

/// Chooses at most one trade for a specific answer in a multiple-choice market.
pub fn decide_trade_for_answer(
    answer: &crate::types::TrackedAnswer,
    p_internal: f64,
    p_market: f64,
    available_budget: f64,
    params: TradeParams,
) -> Option<TradeDecision> {
    let edge = p_internal - p_market;

    if answer.yes_shares > 0.0 && edge < params.exit_edge_threshold {
        return Some(TradeDecision {
            action: TradeAction::SellYes,
            amount_mana: 0.0,
            shares: Some(answer.yes_shares),
            answer_id: Some(answer.id.clone()),
        });
    }

    if answer.no_shares > 0.0 && edge > -params.exit_edge_threshold {
        return Some(TradeDecision {
            action: TradeAction::SellNo,
            amount_mana: 0.0,
            shares: Some(answer.no_shares),
            answer_id: Some(answer.id.clone()),
        });
    }

    // A position that did not exit stays open. No scale-in.
    if answer.yes_shares > 0.0 || answer.no_shares > 0.0 {
        return None;
    }

    let target = kelly_target_mana(edge, p_market, available_budget, params);
    if target < params.min_trade_size {
        return None;
    }

    if edge > params.entry_edge_threshold {
        Some(TradeDecision {
            action: TradeAction::BuyYes,
            amount_mana: target,
            shares: None,
            answer_id: Some(answer.id.clone()),
        })
    } else if edge < -params.entry_edge_threshold {
        Some(TradeDecision {
            action: TradeAction::BuyNo,
            amount_mana: target,
            shares: None,
            answer_id: Some(answer.id.clone()),
        })
    } else {
        None
    }
}

// -----------------------------------------------------------------------------
// Position accounting
// -----------------------------------------------------------------------------

/// Records a filled buy on the local market snapshot.
///
/// Adds the mana actually paid (fees included) to `invested_mana` and the
/// filled share count to the matching side. Rejects sell actions, which must go
/// through [`apply_sell`].
pub fn apply_buy(
    market: &mut TrackedMarket,
    action: TradeAction,
    cost: f64,
    shares: f64,
) -> AppResult<()> {
    match action {
        TradeAction::BuyYes => market.yes_shares += shares,
        TradeAction::BuyNo => market.no_shares += shares,
        other => bail!("apply_buy called with non-buy action {other}"),
    }
    market.invested_mana += cost;
    Ok(())
}

/// Records a filled sell on the local market snapshot.
///
/// The cost basis is proportional: with `sold` of `held` shares sold,
/// `basis = invested * sold / held`. Realized PnL grows by `proceeds - basis`,
/// `invested_mana` shrinks by `basis`, and the matching share count falls by
/// `sold`. Every value is clamped at zero so rounding can never produce a
/// negative position.
///
/// Rejects buy actions and rejects a sell when the market holds no shares of
/// that side, which indicates a decision bug rather than a market condition.
pub fn apply_sell(
    market: &mut TrackedMarket,
    action: TradeAction,
    shares_sold: f64,
    proceeds: f64,
) -> AppResult<()> {
    let held = match action {
        TradeAction::SellYes => market.yes_shares,
        TradeAction::SellNo => market.no_shares,
        other => bail!("apply_sell called with non-sell action {other}"),
    };

    if !shares_sold.is_finite() || shares_sold <= 0.0 {
        bail!("sell must dispose of a positive finite share count, got {shares_sold}");
    }
    if held <= 0.0 {
        bail!("cannot {action}: the market holds no shares on that side");
    }

    let sold = shares_sold.min(held);
    let basis = (market.invested_mana * sold / held).max(0.0);

    market.realized_pnl_mana += proceeds - basis;
    market.invested_mana = (market.invested_mana - basis).max(0.0);
    match action {
        TradeAction::SellYes => market.yes_shares = (held - sold).max(0.0),
        _ => market.no_shares = (held - sold).max(0.0),
    }
    Ok(())
}

// -----------------------------------------------------------------------------
// Executor
// -----------------------------------------------------------------------------

/// Events published by the trading layer for downstream consumers.
///
/// The Discord layer renders these as `[TRADE]` and `[ERROR]` embeds. Events
/// are emitted only for executed trades and for failures the trade loop
/// observes; a no-op decision stays silent because there is nothing to report.
#[derive(Debug, Clone)]
pub enum TradeEvent {
    /// One buy or sell was filled and persisted.
    Trade {
        market_id: MarketId,
        action: TradeAction,
        /// Mana paid for a buy or received for a sell, fees included.
        mana_amount: f64,
        /// Signed share change: positive for a buy, negative for a sell.
        shares_delta: f64,
        p_internal: f64,
        p_market: f64,
        /// Budget still deployable after the trade.
        available_budget: f64,
        bet_id: Option<String>,
    },
    /// A trade check or execution failed.
    Error {
        market_id: Option<MarketId>,
        message: String,
    },
}

/// Executes trades and keeps local state in step with Manifold.
///
/// All state-changing operations ([`TradeExecutor::evaluate_market`],
/// [`TradeExecutor::sync_market`], [`TradeExecutor::liquidate`]) serialize on
/// `execution_lock`. Without that lock two concurrent signal evaluations could
/// both read a flat position and each buy, violating "one entry per lifecycle".
pub struct TradeExecutor {
    client: ManifoldClient,
    db: Db,
    cache: Arc<MarketCache>,
    params: TradeParams,
    halflife_hours: f64,
    execution_lock: Mutex<()>,
    events: broadcast::Sender<TradeEvent>,
}

impl TradeExecutor {
    /// Builds the executor and resolves the Manifold bot identity.
    ///
    /// This performs one authenticated `GET /v0/me`. A failure is fatal at
    /// startup: silently disabling trading would hide a broken API key.
    pub async fn new(config: &Config, db: Db, cache: Arc<MarketCache>) -> AppResult<Self> {
        let client = ManifoldClient::new(config)
            .await
            .context("initializing the Manifold client")?;
        let (events, _) = broadcast::channel(EVENT_CAPACITY);

        Ok(Self {
            client,
            db,
            cache,
            params: TradeParams::from_config(config),
            halflife_hours: config.belief_halflife_hours,
            execution_lock: Mutex::new(()),
            events,
        })
    }

    /// Returns a sender that can be subscribed to for trade events.
    pub fn events(&self) -> broadcast::Sender<TradeEvent> {
        self.events.clone()
    }

    /// The underlying Manifold client, used for direct queries by callers such
    /// as onboarding and the Discord status command.
    pub fn client(&self) -> &ManifoldClient {
        &self.client
    }

    /// Runs one trade check for a market.
    ///
    /// Steps: reload from the database, skip non-active markets, fetch a fresh
    /// market probability and the authoritative Manifold position, reconcile,
    /// compute the decayed internal probability, decide, execute at most one
    /// action, then persist the ledger row and the updated position.
    ///
    /// Returns `Ok(None)` when no trade happens. A resolved market, a paused
    /// market, an edge inside the thresholds, and a buy blocked by the kill
    /// switch all take that path.
    pub async fn evaluate_market(
        &self,
        market_id: &str,
        trigger_post_uri: Option<&str>,
    ) -> AppResult<Option<TradeRecord>> {
        let _guard = self.execution_lock.lock().await;

        let Some(mut market) = self.db.get_market(market_id)? else {
            bail!("market {market_id} is not tracked");
        };
        if market.status != MarketStatus::Active {
            return Ok(None);
        }

        let kill_switch = self.kill_switch_on()?;

        let snapshot = self
            .client
            .market(market_id)
            .await
            .with_context(|| format!("fetching Manifold market {market_id}"))?;
        if snapshot.is_resolved {
            tracing::info!(market_id = %market_id, "market is resolved; skipping trade check");
            return Ok(None);
        }

        let p_market = snapshot
            .probability
            .unwrap_or(market.p_market_last)
            .clamp(MIN_TRADING_PROB, MAX_TRADING_PROB);
        let position = self
            .client
            .position(market_id)
            .await
            .with_context(|| format!("fetching the Manifold position for {market_id}"))?;
        reconcile_position(&mut market, &position, market_id);

        let now = Utc::now().timestamp();
        market.p_market_last = p_market;
        let p_internal = belief::current_p_internal(
            market.p_internal_log_odds,
            market.belief_updated_at,
            p_market,
            now,
            self.halflife_hours,
        );

        // Persist reconciliation and the fresh market probability even when no
        // trade follows, so the cache and the next tick agree with Manifold.
        self.persist_market(&market)?;

        let Some(decision) = decide_trade(&market, p_internal, p_market, self.params) else {
            return Ok(None);
        };

        if is_buy(decision.action) && kill_switch {
            tracing::warn!(
                market_id = %market_id,
                "global kill switch is on; refusing to buy"
            );
            return Ok(None);
        }

        let record = match decision.action {
            TradeAction::BuyYes | TradeAction::BuyNo => {
                self.execute_buy(
                    &mut market,
                    &decision,
                    p_internal,
                    p_market,
                    trigger_post_uri,
                    now,
                )
                .await?
            }
            TradeAction::SellYes | TradeAction::SellNo => {
                self.execute_sell(
                    &mut market,
                    &decision,
                    p_internal,
                    p_market,
                    trigger_post_uri,
                    now,
                )
                .await?
            }
        };

        self.persist_market(&market)?;
        self.emit_trade(&market, &record);
        Ok(Some(record))
    }

    /// Reconciles one market with Manifold without trading.
    ///
    /// Updates `p_market_last`, shares, and invested mana from the remote
    /// snapshot, warns on a mismatch, writes the database row, and refreshes
    /// the cache.
    pub async fn sync_market(&self, market_id: &str) -> AppResult<TrackedMarket> {
        let _guard = self.execution_lock.lock().await;

        let Some(mut market) = self.db.get_market(market_id)? else {
            bail!("market {market_id} is not tracked");
        };

        let snapshot = self
            .client
            .market(market_id)
            .await
            .with_context(|| format!("fetching Manifold market {market_id}"))?;
        let position = self
            .client
            .position(market_id)
            .await
            .with_context(|| format!("fetching the Manifold position for {market_id}"))?;

        reconcile_position(&mut market, &position, market_id);
        market.p_market_last = snapshot
            .probability
            .unwrap_or(market.p_market_last)
            .clamp(MIN_TRADING_PROB, MAX_TRADING_PROB);
        self.persist_market(&market)?;
        Ok(market)
    }

    /// Syncs and evaluates every active market once.
    ///
    /// This is the 60-second trade check. Failures are collected per market so
    /// one broken market cannot stop the tick for the others.
    pub async fn sync_all(&self) -> Vec<(MarketId, anyhow::Error)> {
        let markets = match self.db.list_markets_by_status(MarketStatus::Active) {
            Ok(markets) => markets,
            Err(error) => return vec![(String::new(), error)],
        };

        let mut errors = Vec::new();
        for market in markets {
            let market_id = market.id.clone();
            if let Err(error) = self.sync_market(&market_id).await {
                errors.push((market_id, error));
                continue;
            }
            if let Err(error) = self.evaluate_market(&market_id, None).await {
                errors.push((market_id, error));
            }
        }
        errors
    }

    /// Sells every YES and NO share in one market.
    ///
    /// The remote position is fetched first, so shares already sold manually or
    /// by a previous partial liquidation are not sold twice. Each side is
    /// recorded and persisted before the next side is attempted.
    pub async fn liquidate(&self, market_id: &str) -> AppResult<()> {
        let _guard = self.execution_lock.lock().await;

        let Some(mut market) = self.db.get_market(market_id)? else {
            bail!("market {market_id} is not tracked");
        };

        let snapshot = self
            .client
            .market(market_id)
            .await
            .with_context(|| format!("fetching Manifold market {market_id} for liquidation"))?;
        let position = self
            .client
            .position(market_id)
            .await
            .with_context(|| format!("fetching the Manifold position for {market_id}"))?;

        reconcile_position(&mut market, &position, market_id);
        let p_market = snapshot
            .probability
            .unwrap_or(market.p_market_last)
            .clamp(MIN_TRADING_PROB, MAX_TRADING_PROB);
        market.p_market_last = p_market;

        let now = Utc::now().timestamp();
        let p_internal = belief::current_p_internal(
            market.p_internal_log_odds,
            market.belief_updated_at,
            p_market,
            now,
            self.halflife_hours,
        );
        self.persist_market(&market)?;

        for action in [TradeAction::SellYes, TradeAction::SellNo] {
            let held = match action {
                TradeAction::SellYes => market.yes_shares,
                _ => market.no_shares,
            };
            if held <= 0.0 {
                continue;
            }

            let decision = TradeDecision {
                action,
                amount_mana: 0.0,
                shares: Some(held),
                answer_id: None,
            };
            let record = self
                .execute_sell(&mut market, &decision, p_internal, p_market, None, now)
                .await
                .with_context(|| format!("liquidating {action} on market {market_id}"))?;
            self.persist_market(&market)?;
            self.emit_trade(&market, &record);
        }

        Ok(())
    }

    /// Liquidates every market that is not already closed.
    ///
    /// Paused markets are included because `/panic` must flatten what the bot
    /// would resume holding. Errors are collected rather than aborting the
    /// sweep, so one unsellable market does not strand the others.
    pub async fn liquidate_all(&self) -> Vec<(MarketId, anyhow::Error)> {
        let markets = match self.db.list_markets() {
            Ok(markets) => markets,
            Err(error) => return vec![(String::new(), error)],
        };

        let mut errors = Vec::new();
        for market in markets {
            if market.status == MarketStatus::Closed {
                continue;
            }
            if let Err(error) = self.liquidate(&market.id).await {
                errors.push((market.id, error));
            }
        }
        errors
    }

    /// Publishes a filled trade after the database and cache were updated.
    ///
    /// A receiver that is not running yet is normal: the broadcast channel has
    /// no history and events are best-effort.
    fn emit_trade(&self, market: &TrackedMarket, record: &TradeRecord) {
        let _ = self.events.send(trade_event(market, record));
    }

    /// Publishes a trade-loop failure.
    fn emit_error(&self, market_id: Option<MarketId>, message: String) {
        let _ = self.events.send(TradeEvent::Error { market_id, message });
    }

    /// Consumes pipeline signals and runs a 60-second sync ticker.
    ///
    /// Material signals trigger an immediate trade check for their market. The
    /// ticker reconciles every active market and then evaluates it, which is
    /// what catches exits caused by belief decay between posts.
    ///
    /// No single failure stops the loop: trade errors are logged, broadcast lag
    /// is logged, and a closed channel ends the loop.
    pub async fn run(
        self: Arc<Self>,
        mut events: broadcast::Receiver<PipelineEvent>,
    ) -> AppResult<()> {
        let mut ticker = tokio::time::interval(SYNC_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The first interval tick completes immediately. Consume it so startup
        // does not race the first real 60-second reconciliation.
        ticker.tick().await;

        loop {
            tokio::select! {
                event = events.recv() => {
                    match event {
                        Ok(PipelineEvent::Signal {
                            material: true,
                            market_id,
                            post_uri,
                            ..
                        }) => {
                            if let Err(error) = self
                                .evaluate_market(&market_id, Some(&post_uri))
                                .await
                            {
                                tracing::error!(
                                    market_id = %market_id,
                                    error = %error,
                                    "trade check failed after a material signal"
                                );
                                self.emit_error(Some(market_id), error.to_string());
                            }
                        }
                        Ok(_) => {}
                        Err(broadcast::error::RecvError::Lagged(dropped)) => {
                            tracing::warn!(
                                dropped,
                                "trade event consumer lagged; some signals were skipped"
                            );
                        }
                        Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    }
                }
                _ = ticker.tick() => {
                    for (market_id, error) in self.sync_all().await {
                        tracing::error!(
                            market_id = %market_id,
                            error = %error,
                            "scheduled market sync failed"
                        );
                        self.emit_error(
                            Some(market_id).filter(|id| !id.is_empty()),
                            error.to_string(),
                        );
                    }
                }
            }
        }
    }

    /// Performs one buy and writes its ledger row.
    async fn execute_buy(
        &self,
        market: &mut TrackedMarket,
        decision: &TradeDecision,
        p_internal: f64,
        p_market: f64,
        trigger_post_uri: Option<&str>,
        now: i64,
    ) -> AppResult<TradeRecord> {
        let amount = decision.amount_mana;
        if !amount.is_finite() || amount <= 0.0 {
            bail!("buy amount must be a positive finite mana value, got {amount}");
        }

        let available = available_budget(market);
        if amount > available + BUDGET_EPSILON {
            bail!(
                "budget invariant violated for market {}: buy of {amount} mana exceeds \
                 available budget {available}",
                market.id
            );
        }
        let committed = market.invested_mana + amount;
        let ceiling = market.budget_mana + market.realized_pnl_mana;
        if committed > ceiling + BUDGET_EPSILON {
            bail!(
                "budget invariant violated for market {}: invested {committed} would exceed \
                 budget plus realized PnL {ceiling}",
                market.id
            );
        }

        let outcome = decision.action.outcome();
        let result = self
            .client
            .buy(&market.id, outcome, amount, decision.answer_id.as_deref())
            .await
            .with_context(|| format!("buying {outcome} on Manifold market {}", market.id))?;
        ensure_filled(&result, &market.id)?;
        if !result.shares.is_finite() || result.shares <= 0.0 {
            bail!(
                "Manifold bet {} on market {} returned no shares",
                result.bet_id(),
                market.id
            );
        }

        let cost = result.total_cost();
        apply_buy(market, decision.action, cost, result.shares)?;

        let record = TradeRecord {
            id: 0,
            market_id: market.id.clone(),
            answer_id: decision.answer_id.clone(),
            action: decision.action,
            mana_amount: cost,
            shares_delta: result.shares,
            p_internal,
            p_market,
            trigger_post_uri: trigger_post_uri.map(str::to_owned),
            manifold_bet_id: optional_id(result.bet_id()),
            created_at: now,
        };
        let id = self.db.insert_trade(&record)?;

        Ok(TradeRecord { id, ..record })
    }

    /// Performs one sell and writes its ledger row.
    async fn execute_sell(
        &self,
        market: &mut TrackedMarket,
        decision: &TradeDecision,
        p_internal: f64,
        p_market: f64,
        trigger_post_uri: Option<&str>,
        now: i64,
    ) -> AppResult<TradeRecord> {
        let requested = decision.shares.unwrap_or(0.0);
        if !requested.is_finite() || requested <= 0.0 {
            bail!("sell must dispose of a positive finite share count, got {requested}");
        }

        let outcome = decision.action.outcome();
        let result = self
            .client
            .sell(&market.id, outcome, requested, decision.answer_id.as_deref())
            .await
            .with_context(|| format!("selling {outcome} on Manifold market {}", market.id))?;
        ensure_filled(&result, &market.id)?;

        // Prefer the filled share count from Manifold; fall back to the request
        // when the response omits it. apply_sell clamps to the held amount.
        let sold = if result.shares.is_finite() && result.shares.abs() > 0.0 {
            result.shares.abs()
        } else {
            requested
        };
        let proceeds = result.net_proceeds();
        apply_sell(market, decision.action, sold, proceeds)?;

        let record = TradeRecord {
            id: 0,
            market_id: market.id.clone(),
            answer_id: decision.answer_id.clone(),
            action: decision.action,
            mana_amount: proceeds,
            shares_delta: -sold,
            p_internal,
            p_market,
            trigger_post_uri: trigger_post_uri.map(str::to_owned),
            manifold_bet_id: optional_id(result.bet_id()),
            created_at: now,
        };
        let id = self.db.insert_trade(&record)?;

        Ok(TradeRecord { id, ..record })
    }

    /// Reads the panic switch. Only the literal `true` blocks buys.
    fn kill_switch_on(&self) -> AppResult<bool> {
        Ok(self
            .db
            .get_kv(KILL_SWITCH_KEY)?
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("true")))
    }

    /// Writes the position, belief, and probability fields and refreshes the
    /// cache with the same values.
    fn persist_market(&self, market: &TrackedMarket) -> AppResult<()> {
        self.db.update_position(
            &market.id,
            market.invested_mana,
            market.realized_pnl_mana,
            market.yes_shares,
            market.no_shares,
        )?;
        self.db.update_belief(
            &market.id,
            market.p_internal_log_odds,
            market.p_market_last,
            market.belief_updated_at,
        )?;
        self.cache.update_market(market.clone());
        Ok(())
    }
}

/// Whether an action opens a position.
fn is_buy(action: TradeAction) -> bool {
    matches!(action, TradeAction::BuyYes | TradeAction::BuyNo)
}

/// Builds the event for a persisted trade.
///
/// The available budget is read from the already-updated market snapshot, so it
/// reflects the post-trade position rather than the size that was requested.
fn trade_event(market: &TrackedMarket, record: &TradeRecord) -> TradeEvent {
    TradeEvent::Trade {
        market_id: record.market_id.clone(),
        action: record.action,
        mana_amount: record.mana_amount,
        shares_delta: record.shares_delta,
        p_internal: record.p_internal,
        p_market: record.p_market,
        available_budget: available_budget(market),
        bet_id: record.manifold_bet_id.clone(),
    }
}

/// Replaces the local position fields with Manifold's and warns when they
/// differed.
///
/// `realized_pnl_mana` is deliberately not overwritten: Manifold's `profit`
/// includes unrealized gains, while the bot's budget accounting treats realized
/// PnL as the only source of recycled budget.
fn reconcile_position(market: &mut TrackedMarket, remote: &PositionSnapshot, market_id: &str) {
    let differs = (market.yes_shares - remote.yes_shares).abs() > POSITION_EPSILON
        || (market.no_shares - remote.no_shares).abs() > POSITION_EPSILON
        || (market.invested_mana - remote.invested).abs() > POSITION_EPSILON;

    if differs {
        tracing::warn!(
            market_id = %market_id,
            local_yes_shares = market.yes_shares,
            remote_yes_shares = remote.yes_shares,
            local_no_shares = market.no_shares,
            remote_no_shares = remote.no_shares,
            local_invested = market.invested_mana,
            remote_invested = remote.invested,
            "local position differs from Manifold; trusting Manifold"
        );
    }

    market.yes_shares = remote.yes_shares.max(0.0);
    market.no_shares = remote.no_shares.max(0.0);
    market.invested_mana = remote.invested.max(0.0);
}

/// Rejects a bet response that Manifold did not fill.
fn ensure_filled(result: &BetResult, market_id: &str) -> AppResult<()> {
    if result.is_cancelled {
        bail!(
            "Manifold bet {} on market {market_id} was cancelled",
            result.bet_id()
        );
    }
    if !result.is_filled {
        bail!(
            "Manifold bet {} on market {market_id} was not filled",
            result.bet_id()
        );
    }
    Ok(())
}

/// Turns an empty bet identifier into `None`.
fn optional_id(id: &str) -> Option<String> {
    Some(id.trim())
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
}

/// Truncates response text for an error message without splitting a character.
fn snippet(text: &str) -> String {
    let mut out: String = text.chars().take(ERROR_SNIPPET_CHARS).collect();
    if text.chars().count() > ERROR_SNIPPET_CHARS {
        out.push('…');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn params() -> TradeParams {
        TradeParams {
            kelly_fraction: 0.25,
            entry_edge_threshold: 0.05,
            exit_edge_threshold: 0.01,
            min_trade_size: 1.0,
        }
    }

    fn market_with(budget: f64, invested: f64, pnl: f64, yes: f64, no: f64) -> TrackedMarket {
        TrackedMarket {
            id: "Market_test".to_owned(),
            slug: "will-the-thing-happen".to_owned(),
            question: "Will the thing happen?".to_owned(),
            description: String::new(),
            resolution_criteria: String::new(),
            close_time: None,
            status: MarketStatus::Active,
            budget_mana: budget,
            invested_mana: invested,
            realized_pnl_mana: pnl,
            yes_shares: yes,
            no_shares: no,
            p_internal_log_odds: 0.0,
            p_market_last: 0.5,
            belief_updated_at: 0,
            sieve_source: String::new(),
            discord_thread_id: String::new(),
            created_at: 0,
        }
    }

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-9,
            "expected {expected}, got {actual}"
        );
    }

    // -- Kelly sizing -------------------------------------------------------

    #[test]
    fn kelly_uses_the_yes_formula_for_a_bullish_edge() {
        // f = 0.25 * 0.1 / (1 - 0.5) = 0.05 -> 5 mana of a 100 mana budget.
        let target = kelly_target_mana(0.1, 0.5, 100.0, params());
        assert_close(target, 5.0);
    }

    #[test]
    fn kelly_uses_the_no_formula_for_a_bearish_edge() {
        // f = 0.25 * 0.1 / 0.4 = 0.0625 -> 6.25 mana of a 100 mana budget.
        let target = kelly_target_mana(-0.1, 0.4, 100.0, params());
        assert_close(target, 6.25);
    }

    #[test]
    fn kelly_caps_the_fraction_at_half_the_budget() {
        let aggressive = TradeParams {
            kelly_fraction: 2.0,
            ..params()
        };
        // f = 2.0 * 0.4 / 0.5 = 1.6, clamped to 0.5 -> 50 mana of 100.
        let target = kelly_target_mana(0.4, 0.5, 100.0, aggressive);
        assert_close(target, 50.0);
    }

    #[test]
    fn kelly_returns_zero_below_the_entry_threshold() {
        assert_eq!(kelly_target_mana(0.049, 0.5, 100.0, params()), 0.0);
        assert_eq!(kelly_target_mana(-0.049, 0.5, 100.0, params()), 0.0);
        assert_eq!(kelly_target_mana(0.0, 0.5, 100.0, params()), 0.0);
    }

    #[test]
    fn kelly_accepts_an_edge_exactly_at_the_threshold() {
        // f = 0.25 * 0.05 / 0.5 = 0.025 -> 2.5 mana.
        assert_close(kelly_target_mana(0.05, 0.5, 100.0, params()), 2.5);
        assert_close(kelly_target_mana(-0.05, 0.5, 100.0, params()), 2.5);
    }

    #[test]
    fn kelly_clamps_market_probability_at_the_endpoints() {
        // p_market 0 -> treated as 0.01: f = 0.25 * 0.5 / 0.99.
        let yes = kelly_target_mana(0.5, 0.0, 100.0, params());
        assert!(yes.is_finite());
        assert_close(yes, 0.25 * 0.5 / 0.99 * 100.0);

        // p_market 1 -> treated as 0.99: f = 0.25 * 0.5 / 0.99.
        let no = kelly_target_mana(-0.5, 1.0, 100.0, params());
        assert!(no.is_finite());
        assert_close(no, 0.25 * 0.5 / 0.99 * 100.0);
    }

    #[test]
    fn kelly_returns_zero_for_a_zero_or_negative_budget() {
        assert_eq!(kelly_target_mana(0.5, 0.5, 0.0, params()), 0.0);
        assert_eq!(kelly_target_mana(0.5, 0.5, -10.0, params()), 0.0);
    }

    // -- Decisions ----------------------------------------------------------

    #[test]
    fn flat_market_with_a_bullish_edge_buys_yes() {
        let market = market_with(100.0, 0.0, 0.0, 0.0, 0.0);
        let decision = decide_trade(&market, 0.6, 0.5, params()).expect("buy YES");
        assert_eq!(decision.action, TradeAction::BuyYes);
        assert_close(decision.amount_mana, 5.0);
        assert_eq!(decision.shares, None);
    }

    #[test]
    fn flat_market_with_a_bearish_edge_buys_no() {
        let market = market_with(100.0, 0.0, 0.0, 0.0, 0.0);
        let decision = decide_trade(&market, 0.4, 0.5, params()).expect("buy NO");
        assert_eq!(decision.action, TradeAction::BuyNo);
        assert_close(decision.amount_mana, 5.0);
        assert_eq!(decision.shares, None);
    }

    #[test]
    fn edge_inside_the_entry_band_does_not_trade() {
        let market = market_with(100.0, 0.0, 0.0, 0.0, 0.0);
        assert!(decide_trade(&market, 0.54, 0.5, params()).is_none());
        assert!(decide_trade(&market, 0.46, 0.5, params()).is_none());
        assert!(decide_trade(&market, 0.5, 0.5, params()).is_none());
    }

    #[test]
    fn holding_yes_exits_when_the_edge_drops_below_the_exit_threshold() {
        let market = market_with(100.0, 10.0, 0.0, 20.0, 0.0);
        let decision = decide_trade(&market, 0.505, 0.5, params()).expect("sell YES");
        assert_eq!(decision.action, TradeAction::SellYes);
        assert_eq!(decision.shares, Some(20.0));
    }

    #[test]
    fn holding_no_exits_when_the_edge_rises_above_the_exit_threshold() {
        let market = market_with(100.0, 10.0, 0.0, 0.0, 15.0);
        let decision = decide_trade(&market, 0.495, 0.5, params()).expect("sell NO");
        assert_eq!(decision.action, TradeAction::SellNo);
        assert_eq!(decision.shares, Some(15.0));
    }

    #[test]
    fn holding_yes_does_not_exit_inside_the_band() {
        let market = market_with(100.0, 10.0, 0.0, 20.0, 0.0);
        // edge = 0.02, above EXIT_EDGE_THRESHOLD, but below the entry threshold.
        assert!(decide_trade(&market, 0.52, 0.5, params()).is_none());
        // A large edge does not scale the position in either.
        assert!(decide_trade(&market, 0.7, 0.5, params()).is_none());
    }

    #[test]
    fn holding_no_does_not_exit_or_add_inside_the_band() {
        let market = market_with(100.0, 10.0, 0.0, 0.0, 15.0);
        assert!(decide_trade(&market, 0.48, 0.5, params()).is_none());
        assert!(decide_trade(&market, 0.3, 0.5, params()).is_none());
    }

    #[test]
    fn entries_below_the_minimum_trade_size_are_skipped() {
        // A 10 mana budget with a 0.1 edge sizes to 0.5 mana, below the 1 mana
        // minimum.
        let market = market_with(10.0, 0.0, 0.0, 0.0, 0.0);
        assert!(decide_trade(&market, 0.6, 0.5, params()).is_none());

        // Increasing the edge makes the same budget tradable.
        let decision = decide_trade(&market, 0.8, 0.5, params()).expect("large edge trades");
        assert_close(decision.amount_mana, 1.5);
    }

    #[test]
    fn entries_use_only_the_available_budget() {
        // budget 100 + pnl 20 - invested 50 = 70 available; f = 0.05.
        let market = market_with(100.0, 50.0, 20.0, 0.0, 0.0);
        let decision = decide_trade(&market, 0.6, 0.5, params()).expect("buy YES");
        assert_close(decision.amount_mana, 3.5);
    }

    #[test]
    fn decision_never_sells_a_zero_share_position() {
        let market = market_with(100.0, 0.0, 0.0, 0.0, 0.0);
        // A strongly bearish edge with no shares is an entry, never a sell.
        let decision = decide_trade(&market, 0.1, 0.5, params()).expect("entry");
        assert_eq!(decision.action, TradeAction::BuyNo);
    }

    // -- Position accounting ------------------------------------------------

    #[test]
    fn apply_buy_adds_cost_and_shares_to_the_matching_side() {
        let mut market = market_with(100.0, 0.0, 0.0, 0.0, 0.0);
        apply_buy(&mut market, TradeAction::BuyYes, 10.0, 20.0).expect("buy YES");
        assert_close(market.invested_mana, 10.0);
        assert_close(market.yes_shares, 20.0);
        assert_close(market.no_shares, 0.0);

        apply_buy(&mut market, TradeAction::BuyNo, 5.0, 10.0).expect("buy NO");
        assert_close(market.invested_mana, 15.0);
        assert_close(market.no_shares, 10.0);
    }

    #[test]
    fn apply_buy_rejects_sell_actions() {
        let mut market = market_with(100.0, 0.0, 0.0, 0.0, 0.0);
        assert!(apply_buy(&mut market, TradeAction::SellYes, 1.0, 1.0).is_err());
        assert!(apply_buy(&mut market, TradeAction::SellNo, 1.0, 1.0).is_err());
    }

    #[test]
    fn profitable_sell_increases_realized_pnl_and_available_budget() {
        let mut market = market_with(100.0, 10.0, 0.0, 20.0, 0.0);
        let before = available_budget(&market);

        apply_sell(&mut market, TradeAction::SellYes, 20.0, 12.0).expect("sell YES");

        assert_close(market.realized_pnl_mana, 2.0);
        assert_close(market.invested_mana, 0.0);
        assert_close(market.yes_shares, 0.0);
        // The released cost basis and the profit both return to the budget.
        assert_close(available_budget(&market), before + 12.0);
    }

    #[test]
    fn partial_sell_uses_a_proportional_cost_basis() {
        let mut market = market_with(100.0, 10.0, 0.0, 20.0, 0.0);
        let before = available_budget(&market);

        apply_sell(&mut market, TradeAction::SellYes, 10.0, 6.0).expect("partial sell");

        assert_close(market.realized_pnl_mana, 1.0); // 6 - 10 * 10/20
        assert_close(market.invested_mana, 5.0);
        assert_close(market.yes_shares, 10.0);
        assert_close(available_budget(&market), before + 6.0);
    }

    #[test]
    fn losing_sell_reduces_available_budget() {
        let mut market = market_with(100.0, 10.0, 0.0, 20.0, 0.0);
        let before = available_budget(&market);

        apply_sell(&mut market, TradeAction::SellYes, 20.0, 3.0).expect("sell YES");

        assert_close(market.realized_pnl_mana, -7.0);
        // The released basis minus the loss equals the actual proceeds.
        assert_close(available_budget(&market), before + 3.0);
    }

    #[test]
    fn sell_of_more_shares_than_held_is_clamped() {
        let mut market = market_with(100.0, 10.0, 0.0, 20.0, 0.0);

        apply_sell(&mut market, TradeAction::SellYes, 50.0, 12.0).expect("clamped sell");

        assert_close(market.yes_shares, 0.0);
        assert_close(market.invested_mana, 0.0);
        assert_close(market.realized_pnl_mana, 2.0);
    }

    #[test]
    fn apply_sell_rejects_buy_actions_and_empty_positions() {
        let mut market = market_with(100.0, 10.0, 0.0, 20.0, 0.0);
        assert!(apply_sell(&mut market, TradeAction::BuyYes, 1.0, 1.0).is_err());
        assert!(apply_sell(&mut market, TradeAction::BuyNo, 1.0, 1.0).is_err());
        assert!(apply_sell(&mut market, TradeAction::SellNo, 1.0, 1.0).is_err());
        assert!(apply_sell(&mut market, TradeAction::SellYes, 0.0, 1.0).is_err());
    }

    // -- Wire format --------------------------------------------------------

    #[test]
    fn market_snapshot_deserializes_manifold_fields() {
        let raw = json!({
            "id": "abc123",
            "question": "Will the thing happen?",
            "probability": 0.63,
            "outcomeType": "BINARY",
            "isResolved": false,
            "closeTime": 1_700_000_000_000_i64,
            "textDescription": "Resolves YES if the thing happens."
        });
        let snapshot: MarketSnapshot =
            serde_json::from_value(raw).expect("market snapshot should deserialize");

        assert_eq!(snapshot.id, "abc123");
        assert_eq!(snapshot.question, "Will the thing happen?");
        assert_close(snapshot.probability.unwrap(), 0.63);
        assert_eq!(snapshot.outcome_type, "BINARY");
        assert!(!snapshot.is_resolved);
        assert_eq!(snapshot.close_time_ms, Some(1_700_000_000_000));
        assert_eq!(
            optional_millis_to_seconds(snapshot.close_time_ms),
            Some(1_700_000_000)
        );
        assert_eq!(
            snapshot.text_description,
            "Resolves YES if the thing happens."
        );
    }

    #[test]
    fn market_snapshot_probability_is_none_when_absent() {
        // MULTIPLE_CHOICE and other non-binary markets omit the probability
        // field. The snapshot must still deserialize successfully.
        let raw = json!({
            "id": "mc-market",
            "question": "Who wins?",
            "outcomeType": "MULTIPLE_CHOICE",
            "isResolved": false
        });
        let snapshot: MarketSnapshot =
            serde_json::from_value(raw).expect("MC market should deserialize without probability");

        assert_eq!(snapshot.probability, None);
    }

    #[test]
    fn market_snapshot_tolerates_missing_optional_fields() {
        let raw = json!({
            "id": "abc123",
            "question": "Q?",
            "probability": 0.5,
            "outcomeType": "BINARY",
            "isResolved": true
        });
        let snapshot: MarketSnapshot =
            serde_json::from_value(raw).expect("optional fields may be absent");

        assert_eq!(snapshot.close_time_ms, None);
        assert!(snapshot.is_resolved);
        assert!(snapshot.text_description.is_empty());
    }

    #[test]
    fn bet_result_reads_id_fees_and_cost() {
        let raw = json!({
            "id": "bet-1",
            "amount": 10.0,
            "shares": 20.0,
            "probBefore": 0.50,
            "probAfter": 0.55,
            "isFilled": true,
            "isCancelled": false,
            "fees": {"creatorFee": 0.1, "platformFee": 0.2, "liquidityFee": 0.3}
        });
        let bet: BetResult = serde_json::from_value(raw).expect("bet should deserialize");

        assert_eq!(bet.bet_id(), "bet-1");
        assert_close(bet.total_cost(), 10.6);
        assert_eq!(bet.prob_before, Some(0.5));
        assert!(bet.is_filled);
        assert!(!bet.is_cancelled);
    }

    #[test]
    fn bet_result_falls_back_to_bet_id_and_handles_missing_fields() {
        let raw = json!({
            "betId": "bet-2",
            "amount": 5.0,
            "shares": 9.5
        });
        let bet: BetResult = serde_json::from_value(raw).expect("betId shape should deserialize");

        assert_eq!(bet.id, "");
        assert_eq!(bet.bet_id(), "bet-2");
        assert_close(bet.total_cost(), 5.0);
        assert!(bet.fees.is_none());
        assert!(!bet.is_filled, "missing isFilled defaults to false");
    }

    #[test]
    fn sell_bet_proceeds_are_net_of_fees() {
        // Manifold reports a sale as a negative mana delta.
        let raw = json!({
            "id": "sell-1",
            "amount": -12.0,
            "shares": -20.0,
            "isFilled": true,
            "fees": {"creatorFee": 0.1, "platformFee": 0.2, "liquidityFee": 0.3}
        });
        let bet: BetResult = serde_json::from_value(raw).expect("sell should deserialize");

        // Both helpers return a magnitude: `total_cost` takes the absolute
        // value of the signed delta, while `net_proceeds` removes fees.
        assert_close(bet.total_cost(), 11.4);
        assert_close(bet.net_proceeds(), 11.4);
    }

    #[test]
    fn position_parser_reads_a_yes_only_position() {
        let raw = json!([{
            "userId": "bot-1",
            "totalShares": {"YES": 12.5},
            "invested": 8.0,
            "profit": 4.5,
            "hasYesShares": true,
            "hasNoShares": false
        }]);

        let position = parse_position(&raw, "bot-1");

        assert_close(position.yes_shares, 12.5);
        assert_close(position.no_shares, 0.0);
        assert_close(position.invested, 8.0);
        assert_close(position.profit, 4.5);
    }

    #[test]
    fn position_parser_reads_a_no_only_position() {
        let raw = json!([{
            "userId": "bot-1",
            "totalShares": {"NO": 3.25},
            "invested": 2.0,
            "profit": -0.5
        }]);

        let position = parse_position(&raw, "bot-1");

        assert_close(position.yes_shares, 0.0);
        assert_close(position.no_shares, 3.25);
        assert_close(position.invested, 2.0);
        assert_close(position.profit, -0.5);
    }

    #[test]
    fn position_parser_selects_the_bot_entry_and_accepts_single_entries() {
        let raw = json!([
            {"userId": "someone-else", "totalShares": {"YES": 99.0}, "invested": 50.0},
            {"userId": "bot-1", "totalShares": {"NO": 7.0}, "invested": 4.0, "profit": 1.0}
        ]);
        let position = parse_position(&raw, "bot-1");
        assert_close(position.no_shares, 7.0);
        assert_close(position.invested, 4.0);

        // Can also parse for a specific answerId
        let multi_raw = json!([
            {"userId": "bot-1", "answerId": "ans-a", "totalShares": {"YES": 15.0}, "invested": 10.0},
            {"userId": "bot-1", "answerId": "ans-b", "totalShares": {"NO": 8.0}, "invested": 5.0}
        ]);
        let pos_a = parse_answer_position(&multi_raw, "bot-1", Some("ans-a"));
        assert_close(pos_a.yes_shares, 15.0);
        assert_close(pos_a.invested, 10.0);
        let pos_b = parse_answer_position(&multi_raw, "bot-1", Some("ans-b"));
        assert_close(pos_b.no_shares, 8.0);
        assert_close(pos_b.invested, 5.0);

        // The server filters by userId; a single anonymous entry is still ours.
        let single = json!([{"totalShares": {"YES": 2.0}, "invested": 1.0}]);
        let position = parse_position(&single, "bot-1");
        assert_close(position.yes_shares, 2.0);
    }

    #[test]
    fn position_parser_returns_zero_for_empty_or_missing_payloads() {
        for raw in [
            json!([]),
            json!({}),
            json!({"positions": []}),
            json!([
                {"userId": "other", "totalShares": {"YES": 1.0}},
                {"userId": "another", "totalShares": {"NO": 2.0}}
            ]),
        ] {
            assert_eq!(
                parse_position(&raw, "bot-1"),
                PositionSnapshot::default(),
                "payload: {raw}"
            );
        }
    }

    #[test]
    fn position_parser_accepts_a_positions_wrapper() {
        let raw = json!({
            "positions": [
                {"userId": "bot-1", "totalShares": {"YES": 5.0}, "invested": 3.0, "profit": 2.0}
            ]
        });
        let position = parse_position(&raw, "bot-1");
        assert_close(position.yes_shares, 5.0);
    }

    #[test]
    fn trade_event_reports_the_post_trade_available_budget() {
        // After a successful sell the released basis and profit are available
        // again, so the event must not report the pre-trade budget.
        let market = market_with(100.0, 10.0, 2.0, 20.0, 0.0);
        let record = TradeRecord {
            id: 7,
            market_id: market.id.clone(),
            answer_id: None,
            action: TradeAction::SellYes,
            mana_amount: 12.0,
            shares_delta: -20.0,
            p_internal: 0.55,
            p_market: 0.5,
            trigger_post_uri: None,
            manifold_bet_id: Some("bet-9".to_owned()),
            created_at: 1_700_000_000,
        };

        match trade_event(&market, &record) {
            TradeEvent::Trade {
                market_id,
                action,
                mana_amount,
                shares_delta,
                available_budget,
                bet_id,
                ..
            } => {
                assert_eq!(market_id, "Market_test");
                assert_eq!(action, TradeAction::SellYes);
                assert_close(mana_amount, 12.0);
                assert_close(shares_delta, -20.0);
                // 100 budget + 2 realized PnL - 10 still invested.
                assert_close(available_budget, 92.0);
                assert_eq!(bet_id.as_deref(), Some("bet-9"));
            }
            other => panic!("expected a trade event, got {other:?}"),
        }
    }

    #[test]
    fn no_op_decision_has_no_trade_to_report() {
        // The event is emitted only after a filled trade; a flat market whose
        // edge sits inside the entry band produces no decision at all.
        let market = market_with(100.0, 0.0, 0.0, 0.0, 0.0);
        assert!(decide_trade(&market, 0.54, 0.5, params()).is_none());
        assert!(decide_trade(&market, 0.46, 0.5, params()).is_none());

        let (sender, mut receiver) = broadcast::channel::<TradeEvent>(4);
        assert!(receiver.try_recv().is_err(), "no event may be queued");
        assert_eq!(sender.receiver_count(), 1);
    }

    #[test]
    fn millis_to_seconds_floors_to_whole_seconds() {
        assert_eq!(millis_to_seconds(1_700_000_000_000), 1_700_000_000);
        assert_eq!(millis_to_seconds(1_700_000_000_999), 1_700_000_000);
        assert_eq!(millis_to_seconds(999), 0);
        assert_eq!(millis_to_seconds(0), 0);
        assert_eq!(millis_to_seconds(-1), -1);
        assert_eq!(optional_millis_to_seconds(None), None);
    }

    #[test]
    fn debug_output_never_contains_the_api_key() {
        let client = ManifoldClient {
            http: reqwest::Client::new(),
            base_url: MANIFOLD_BASE_URL.to_owned(),
            api_key: "super-secret-key".to_owned(),
            bot_user_id: "bot-1".to_owned(),
        };
        let debug = format!("{client:?}");
        assert!(!debug.contains("super-secret-key"), "{debug}");
        assert!(debug.contains("bot-1"), "{debug}");
    }
}
