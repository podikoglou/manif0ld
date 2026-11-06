//! Manifold transport and trade execution.
//!
//! The module has two layers:
//!
//! 1. [`ManifoldClient`] wraps the REST API (`/v0/me`, `/v0/market/{id}`,
//!    `/v0/market/{id}/positions`, `/v0/market-probs`,
//!    `/v0/get-user-contract-metrics-with-contracts`, `/v0/bet`,
//!    `/v0/market/{id}/sell`).
//! 2. [`TradeExecutor`] is the control flow: it keeps the price columns
//!    current from two observation sources — the broadcast websocket
//!    ([`crate::manifold_ws`], live) and a batched REST refresh (the slow
//!    reconcile backstop) — then runs the same book-plan-execute cycle the
//!    old 60-second ticker ran, per price update instead of per minute.
//!
//! Prices arrive live; REST is demoted to housekeeping (`docs/ALGO.md` §1):
//! a 5-minute `Priority::Reconcile` pass chunks `GET /v0/market-probs` and
//! replaces the old per-market `/positions` sync with one paginated portfolio
//! snapshot. `MANIFOLD_WS_ENABLED=0` restores the legacy polling loop for the
//! duration of the canary — see [`TradeExecutor::run`].
//!
//! Positions, accounting, and the trading policy live in [`crate::book`]. The
//! dependency edge is one-way: `trade` uses `book`, and `book` never imports
//! this module. Positioning the layer boundary here is what makes the two
//! market shapes share one control flow.
//!
//! # Positions, not bets, for reconciliation
//!
//! Position sync uses the authoritative `ContractMetric` rows and not the
//! bet history. A bet response cannot reliably distinguish shares that are
//! still held from shares sold later (sell rows carry negative deltas and the
//! history endpoint is paginated), so summing `/v0/bets` rows drifts after the
//! first exit. Manifold's `ContractMetric` reports `totalShares`, `invested`,
//! and `profit` for the current position directly; that is the authoritative
//! source, and any mismatch is resolved in Manifold's favour with a `warn!`.
//! Since the reconcile swap the whole portfolio is one paginated call to
//! `GET /v0/get-user-contract-metrics-with-contracts`; the legacy per-market
//! `/positions` call only survives in `evaluate_market`/`sync_market`, which
//! the legacy loop and liquidation still drive.
//!
//! # Timestamps
//!
//! Manifold reports every time in Unix milliseconds; the database stores Unix
//! seconds. [`millis_to_seconds`] is the single conversion point at this
//! boundary.
//!
//! # Rate budget
//!
//! Every outgoing write sits in the global token bucket in [`crate::rate`]: a
//! buy, a sell, and a dry-run buy (which sends a real `dryRun: true` request)
//! each spend one Trade-class token before the call. When the bucket cannot
//! spare the trade floor, the write is skipped exactly like a kill-switch
//! refusal — the next material signal or price update re-runs the decision.
//! Live prices are free (websocket); REST reads in the reconcile backstop
//! spend Reconcile-class tokens and yield to trades below the housekeeping
//! reserve (`docs/ALGO.md` §2).
//!
//! # Secrets
//!
//! The API key is stored in [`ManifoldClient`] and sent in the `Authorization`
//! header. It never appears in an error, a log line, or the `Debug` output.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::sync::{Mutex, broadcast, mpsc};

use crate::book::{
    AnswerObservation, BUDGET_EPSILON, Book, MAX_TRADING_PROB, MIN_TRADING_PROB, MarketPositions,
    PlannedTrade, Position, Shape, TradePolicy,
};
use crate::config::Config;
use crate::db::Db;
use crate::error::{AppResult, Context, anyhow, bail};
use crate::manifold_ws::{ContractProbs, ManifoldEvent};
use crate::market_store::MarketStore;
use crate::pipeline::PipelineEvent;
use crate::rate::{Priority, RateLimiter, SystemClock, WRITE_COST_TOKENS};
use crate::types::{
    MarketId, MarketStatus, TrackedMarket, TradeAction, TradeDecision, TradeRecord,
    available_budget,
};

/// Public Manifold API base URL.
pub const MANIFOLD_BASE_URL: &str = "https://api.manifold.markets";

/// Timeout for one Manifold request.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);

/// Longest response snippet included in an error message.
const ERROR_SNIPPET_CHARS: usize = 500;

/// LEGACY: the 60-second per-market REST trade check of the polling loop.
/// `MANIFOLD_WS_ENABLED=0` runs it in `TradeExecutor::run_legacy`; delete it
/// after the websocket canary has proved out.
const LEGACY_SYNC_INTERVAL: Duration = Duration::from_secs(60);

/// Interval of the slow REST reconcile backstop that replaces the legacy
/// ticker's freshness role: batched probabilities, one portfolio position
/// snapshot, and a single summary line (`docs/ALGO.md` §1, §2).
const RECONCILE_INTERVAL: Duration = Duration::from_secs(300);

/// Markets per `GET /v0/market-probs` call. Manifold documents a maximum of
/// 100 IDs per request.
const PROB_CHUNK_SIZE: usize = 100;

/// Contracts per page of `GET /v0/get-user-contract-metrics-with-contracts`.
/// The portfolio of a bot is small; one page is the steady state, more pages
/// are the pagination safety valve.
const PORTFOLIO_PAGE_SIZE: usize = 500;

/// Hard cap on portfolio pages per reconcile pass. With `limit=500`, one
/// rounded pass covers far more contracts than a bot can accumulate; the cap
/// only exists so a server-side pagination bug cannot loop forever.
const PORTFOLIO_MAX_PAGES: usize = 20;

/// Coalescing window for websocket price updates, in seconds.
///
/// The live probe (`docs/ALGO.md` §6.1) measured `global/updated-contract` at
/// ~0.5 msg/s average with bursts to ~1.7/s, and bursts of bets can fire
/// several updates for one market within a few hundred milliseconds. Folding
/// two seconds of updates into one evaluation still offers ~5× the decision
/// opportunities of the 60-second ticker it replaces, and bounds the
/// Book::load + plan churn per market instead of tracking every burst tick.
const WS_DEBOUNCE_SECS: u64 = 2;

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
    /// represented as a zero [`Position`] rather than an error. This is the
    /// root summary row of [`ManifoldClient::positions`], the single parser for
    /// the `/positions` payload.
    pub async fn position(&self, market_id: &str) -> AppResult<Position> {
        Ok(self.positions(market_id).await?.root)
    }

    /// Fetches the root summary and every per-answer position in one request.
    ///
    /// [`MarketPositions::checksum_ok`] tells the caller whether the response
    /// was complete.
    pub async fn positions(&self, market_id: &str) -> AppResult<MarketPositions> {
        let path = format!(
            "/v0/market/{}/positions?userId={}",
            market_id, self.bot_user_id
        );
        let request = self.http.get(self.url(&path));
        let value: Value = self.send(request, "GET /v0/market/{id}/positions").await?;
        Ok(parse_market_positions(&value, &self.bot_user_id))
    }

    /// Fetches probabilities for a batch of markets in one request
    /// (`GET /v0/market-probs?ids=a&ids=b…`).
    ///
    /// The caller keeps each batch at [`PROB_CHUNK_SIZE`] IDs, the documented
    /// per-request maximum. The endpoint has a cache of up to 1 s, so even the
    /// largest call is one token of the batch budget.
    ///
    /// Markets missing from the map (unknown ID, closed, or a shape the parser
    /// does not recognize) are simply absent: the caller refreshes what it
    /// gets and relies on the next pass or the websocket for the rest.
    pub async fn market_probs(
        &self,
        market_ids: &[String],
    ) -> AppResult<HashMap<MarketId, MarketProbs>> {
        let ids: Vec<(&str, &str)> = market_ids.iter().map(|id| ("ids", id.as_str())).collect();
        let request = self.http.get(self.url("/v0/market-probs")).query(&ids);
        let value: Value = self.send(request, "GET /v0/market-probs").await?;
        Ok(parse_market_probs(&value))
    }

    /// Fetches one page of the bot's portfolio as the Manifold frontend does
    /// (`GET /v0/get-user-contract-metrics-with-contracts`).
    ///
    /// One entry per held position: a root row without an `answerId` plus one
    /// row per answer of a multiple-choice market, exactly the row shapes the
    /// per-market `/positions` parser accepts. `complete` is false whenever
    /// the page came back full, meaning callers must not infer "no metrics for
    /// a market" as "the bot holds nothing there".
    pub async fn portfolio_page(
        &self,
        offset: usize,
    ) -> AppResult<(HashMap<MarketId, Vec<Value>>, bool)> {
        let path = format!(
            "/v0/get-user-contract-metrics-with-contracts?userId={}&perAnswer=true&limit={}&offset={}",
            self.bot_user_id, PORTFOLIO_PAGE_SIZE, offset
        );
        let request = self.http.get(self.url(&path));
        let value: Value = self
            .send(request, "GET /v0/get-user-contract-metrics-with-contracts")
            .await?;

        let by_contract = parse_metrics_by_contract(&value);
        let entries: usize = by_contract.values().map(Vec::len).sum();
        Ok((by_contract, entries < PORTFOLIO_PAGE_SIZE))
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
        dry_run: bool,
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
            "dryRun": dry_run,
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
    /// `None` on non-BINARY markets (e.g. `MULTIPLE_CHOICE`).
    #[serde(default)]
    pub probability: Option<f64>,
    #[serde(rename = "outcomeType")]
    pub outcome_type: String,
    /// True when exactly one answer can win and answer prices sum to one.
    #[serde(rename = "shouldAnswersSumToOne", default)]
    pub should_answers_sum_to_one: bool,
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

/// One market's probability payload from `GET /v0/market-probs`.
///
/// The same two shapes the websocket patch parser detects structurally: a
/// bare `prob` is a binary market, an `answerProbs` dictionary is a
/// multiple-choice one.
#[derive(Debug, Clone, PartialEq)]
pub enum MarketProbs {
    Binary {
        /// The binary market's consensus probability.
        prob: f64,
    },
    Answers {
        /// `(answer_id, probability)` pairs.
        answers: Vec<(MarketId, f64)>,
    },
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
    /// A dry run returns `id: null`, so the field must accept null.
    #[serde(default)]
    pub id: Option<String>,
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
    ///
    /// A dry run reports `betId: "dry-run"` and a null `id`.
    pub fn bet_id(&self) -> &str {
        self.id
            .as_deref()
            .filter(|id| !id.is_empty())
            .unwrap_or(&self.bet_id)
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

/// One row of a `ContractMetric` as a [`Position`].
fn position_from_entry(entry: &Value) -> Position {
    Position {
        yes_shares: number_at(entry.pointer("/totalShares/YES")),
        no_shares: number_at(entry.pointer("/totalShares/NO")),
        invested: number_at(entry.get("invested")),
        profit: number_at(entry.get("profit")),
    }
}

/// Parses `GET /v0/market/{id}/positions` into root and per-answer positions.
///
/// Entries for other users are ignored. When the payload carries no `userId`
/// at all (a test fixture), every entry is accepted.
pub(crate) fn parse_market_positions(value: &Value, bot_user_id: &str) -> MarketPositions {
    let entries: &[Value] = match value {
        Value::Array(items) => items.as_slice(),
        Value::Object(map) => map
            .get("positions")
            .and_then(Value::as_array)
            .map(|items| items.as_slice())
            .unwrap_or(&[]),
        _ => &[],
    };

    parse_position_entries(entries, bot_user_id)
}

/// Builds a [`MarketPositions`] from raw `ContractMetric` entry values.
///
/// Shared by the per-market `/positions` parser above and the portfolio page
/// parser: both carry the same row shapes — a root summary without an
/// `answerId` plus one row per answer, attributed to a user.
fn parse_position_entries(entries: &[Value], bot_user_id: &str) -> MarketPositions {
    let has_user_ids = entries.iter().any(|entry| entry.get("userId").is_some());
    let mut positions = MarketPositions::default();

    for entry in entries {
        let user_matches = entry.get("userId").and_then(Value::as_str) == Some(bot_user_id);
        if has_user_ids && !user_matches {
            continue;
        }

        match entry.get("answerId").and_then(Value::as_str) {
            Some(answer_id) => {
                positions
                    .answers
                    .entry(answer_id.to_owned())
                    .or_insert_with(|| position_from_entry(entry));
            }
            None => {
                positions.root = position_from_entry(entry);
                positions.saw_root = true;
            }
        }
    }

    positions
}

/// Parses `GET /v0/market-probs`: a map of market ID to its probability
/// payload (`{"prob": …}` for a binary market, `{"answerProbs": {…}}` for a
/// multiple-choice one).
///
/// Unrecognized shapes are skipped rather than rejected: the batched refresh
/// must tolerate a new response variant without losing the whole pass.
fn parse_market_probs(value: &Value) -> HashMap<MarketId, MarketProbs> {
    let mut probs = HashMap::new();
    let Some(by_market) = value.as_object() else {
        return probs;
    };

    for (market_id, entry) in by_market {
        let parsed = if let Some(prob) = entry.get("prob").and_then(Value::as_f64) {
            MarketProbs::Binary { prob }
        } else if let Some(answers) = entry.get("answerProbs").and_then(Value::as_object) {
            let answers: Vec<(MarketId, f64)> = answers
                .iter()
                .filter_map(|(id, prob)| prob.as_f64().map(|prob| (id.clone(), prob)))
                .collect();
            if answers.is_empty() {
                continue;
            }
            MarketProbs::Answers { answers }
        } else {
            continue;
        };

        probs.insert(market_id.clone(), parsed);
    }

    probs
}

/// Parses `metricsByContract` out of one portfolio page.
///
/// `{"metricsByContract": {"<contractId>": [<ContractMetric>…]}, …}` — the
/// entries keep their raw JSON because the shared position parser below reads
/// them straight off.
fn parse_metrics_by_contract(value: &Value) -> HashMap<MarketId, Vec<Value>> {
    let mut by_contract = HashMap::new();
    let Some(metrics) = value.get("metricsByContract").and_then(Value::as_object) else {
        return by_contract;
    };

    for (contract_id, entries) in metrics {
        let Some(entries) = entries.as_array() else {
            continue;
        };
        if !entries.is_empty() {
            by_contract.insert(contract_id.clone(), entries.clone());
        }
    }

    by_contract
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
// Executor
// -----------------------------------------------------------------------------

/// A probability observation in the price-mirror's terms.
///
/// Both observation channels (websocket patch and batched REST refresh)
/// convert into this before writing, so [`TradeExecutor`]'s mirroring code has
/// exactly one shape to handle.
#[derive(Debug, Clone, PartialEq)]
struct ProbPatch {
    /// Root probability for a binary market; `None` for multiple-choice
    /// markets, where the per-answer rows are the price.
    root: Option<f64>,
    /// `(answer_id, probability)` pairs for multiple-choice markets.
    answers: Vec<(MarketId, f64)>,
}

impl From<ContractProbs> for ProbPatch {
    fn from(probs: ContractProbs) -> Self {
        match probs {
            ContractProbs::Binary { prob } => Self {
                root: Some(prob),
                answers: Vec::new(),
            },
            ContractProbs::Multi { answers } => Self {
                root: None,
                answers: answers.into_iter().map(|a| (a.id, a.prob)).collect(),
            },
        }
    }
}

impl From<MarketProbs> for ProbPatch {
    fn from(probs: MarketProbs) -> Self {
        match probs {
            MarketProbs::Binary { prob } => Self {
                root: Some(prob),
                answers: Vec::new(),
            },
            MarketProbs::Answers { answers } => Self {
                root: None,
                answers,
            },
        }
    }
}

/// Whether every probability in one patch sits exactly at an extreme.
///
/// A CPMM market cannot pin at exactly 0 or 1 while it is open, so an
/// all-extreme patch is the practical signature of a resolution broadcast.
/// Price patches carry no `isResolved` flag, so the legacy loop's snapshot
/// check (`evaluate_market`) is replaced by this guard: a resolved market
/// must not trade — its positions settle automatically — and the evaluation
/// resumes by itself only if the probability ever moves off the extreme.
fn patch_is_terminal(patch: &ProbPatch) -> bool {
    let at_extreme = |prob: f64| prob <= 0.0 || prob >= 1.0;
    if let Some(root) = patch.root {
        return at_extreme(root);
    }
    !patch.answers.is_empty() && patch.answers.iter().all(|(_, prob)| at_extreme(*prob))
}

/// Per-market evaluation throttle for websocket price bursts.
///
/// A market evaluated inside the window is skipped: its price has already
/// been mirrored, so the next due evaluation still sees it fresh — the price
/// mirror and the evaluation throttle are deliberately kept one map, not a
/// queue (no per-market buffers, no extra tasks; `docs/ALGO.md` phase 1).
#[derive(Debug)]
struct EvaluationDebouncer {
    window: Duration,
    last_evaluated: HashMap<MarketId, Instant>,
}

impl EvaluationDebouncer {
    fn new(window: Duration) -> Self {
        Self {
            window,
            last_evaluated: HashMap::new(),
        }
    }

    /// Reports whether `key` is due for evaluation now, recording a fresh
    /// timestamp exactly when it is.
    fn due(&mut self, now: Instant, key: &MarketId) -> bool {
        let fresh = self
            .last_evaluated
            .get(key)
            .is_some_and(|last| now.saturating_duration_since(*last) < self.window);
        if !fresh {
            self.last_evaluated.insert(key.clone(), now);
        }
        !fresh
    }
}

/// Accumulated counters of one reconcile pass.
#[derive(Debug, Default, Clone, Copy)]
struct ReconcileSummary {
    /// Tracked markets the pass attempted to refresh.
    markets_seen: usize,
    /// Position mismatches reconciled in Manifold's favour plus shape drifts
    /// that needed the full per-market sync.
    mismatches: usize,
    /// Reconcile-class tokens spent.
    tokens_spent: u64,
    /// Read calls skipped because the bucket was at the housekeeping reserve.
    tokens_skipped: u64,
}

impl ReconcileSummary {
    /// Spends one Reconcile-class token, recording grant or skip.
    fn acquire(&mut self, rate: &RateLimiter, label: &str) -> bool {
        match rate.try_acquire(Priority::Reconcile, WRITE_COST_TOKENS) {
            Ok(_) => {
                self.tokens_spent += 1;
                true
            }
            Err(denied) => {
                self.tokens_skipped += 1;
                tracing::warn!(
                    label,
                    remaining_tokens = denied.remaining,
                    reserve_tokens = denied.threshold,
                    "rate bucket is at the housekeeping reserve; skipping a reconcile read"
                );
                false
            }
        }
    }
}

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

/// What a liquidation request actually did.
///
/// Liquidation is idempotent: asking twice must not sell twice, and a request
/// on a market that cannot trade must do nothing. The outcome lets the caller
/// report the truth instead of a generic success.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiquidationOutcome {
    /// The local row was already `Closed`. No Manifold call was made.
    AlreadyClosed,
    /// Manifold already resolved the market. Resolved positions settle
    /// automatically, so the executor sold nothing.
    AlreadyResolved,
    /// The position held no YES and no NO shares.
    NoPosition,
    /// At least one side was sold.
    Sold,
}

/// The fill of one action, as the ledger records it.
struct Fill<'a> {
    /// Mana paid for a buy or received for a sell, fees included.
    mana_amount: f64,
    /// Signed share change: positive for a buy, negative for a sell.
    shares_delta: f64,
    /// The response the fill came from.
    result: &'a BetResult,
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
    store: Arc<MarketStore>,
    policy: TradePolicy,
    dry_run: bool,
    halflife_hours: f64,
    /// Global Manifold token bucket; writes spend Trade-class tokens, the
    /// reconcile backstop spends Reconcile-class tokens.
    rate: RateLimiter,
    execution_lock: Mutex<()>,
    events: broadcast::Sender<TradeEvent>,
    /// Whether live prices stream from the websocket. `false` selects the
    /// legacy 60-second REST loop in [`TradeExecutor::run`].
    ws_enabled: bool,
}

impl TradeExecutor {
    /// Builds the executor and resolves the Manifold bot identity.
    ///
    /// This performs one authenticated `GET /v0/me`. A failure is fatal at
    /// startup: silently disabling trading would hide a broken API key.
    pub async fn new(config: &Config, db: Db, store: Arc<MarketStore>) -> AppResult<Self> {
        let client = ManifoldClient::new(config)
            .await
            .context("initializing the Manifold client")?;
        let (events, _) = broadcast::channel(EVENT_CAPACITY);

        Ok(Self {
            client,
            db,
            store,
            policy: TradePolicy::from_config(config),
            dry_run: config.trading_dry_run,
            halflife_hours: config.belief_halflife_hours,
            rate: RateLimiter::per_minute(config.rate_limit_per_min, Box::new(SystemClock)),
            execution_lock: Mutex::new(()),
            events,
            ws_enabled: config.manifold_ws_enabled,
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

    /// Runs one trade check for a market, with a fresh REST snapshot first.
    ///
    /// Steps: load the book from the database, skip non-active markets, fetch a
    /// fresh market snapshot, prepare the book (kind, positions, prices, and
    /// reconciliation), plan one decision cycle, then execute and persist
    /// every action it returned.
    ///
    /// This is the legacy path (the 60-second ticker, and the operator's
    /// explicit recheck), and it is what the live operate mode keeps for
    /// liquidation. The live-price loop calls [`TradeExecutor::evaluate_cached`]
    /// instead because the price columns are already mirrored.
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

        let mut book = self.load_book(market_id)?;
        if book.status() != MarketStatus::Active {
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

        let now = Utc::now().timestamp();
        self.prepare(&mut book, &snapshot, now).await?;

        self.decide(&mut book, trigger_post_uri, kill_switch, now)
            .await
    }

    /// Runs one trade check against the already-mirrored state: no REST.
    ///
    /// The websocket and the slow reconcile write the price columns through
    /// `MarketStore::market_prices_observed` and the position columns through
    /// `Book::reconcile`, so loading the book is enough to decide on. The
    /// gating is exactly the legacy path's: status `ACTIVE`, kill switch,
    /// budget invariant (asserted per planned buy), and `TRADING_DRY_RUN`.
    ///
    /// Kind changes and brand-new answers are not caught here by design — the
    /// slow reconcile detects them (`docs/ALGO.md` phase 1).
    async fn evaluate_cached(
        &self,
        market_id: &str,
        trigger_post_uri: Option<&str>,
    ) -> AppResult<Option<TradeRecord>> {
        let _guard = self.execution_lock.lock().await;

        let mut book = self.load_book(market_id)?;
        if book.status() != MarketStatus::Active {
            return Ok(None);
        }

        let kill_switch = self.kill_switch_on()?;
        let now = Utc::now().timestamp();
        self.decide(&mut book, trigger_post_uri, kill_switch, now)
            .await
    }

    /// Plans one decision cycle over a prepared book and executes every
    /// action it produced.
    ///
    /// Shared by [`TradeExecutor::evaluate_market`] and
    /// [`TradeExecutor::evaluate_cached`]; the decision logic is not forked.
    async fn decide(
        &self,
        book: &mut Book,
        trigger_post_uri: Option<&str>,
        kill_switch: bool,
        now: i64,
    ) -> AppResult<Option<TradeRecord>> {
        let market_id = book.id().to_owned();

        let planned = book.plan(now, self.halflife_hours, &self.policy);
        let mut first: Option<TradeRecord> = None;
        for planned_trade in &planned {
            if is_buy(planned_trade.decision.action) && kill_switch {
                tracing::warn!(
                    market_id = %market_id,
                    action = %planned_trade.decision.action,
                    "global kill switch is on; refusing to buy"
                );
                continue;
            }

            // Log before execution so a dry run reports every planned action,
            // not only the one that would have been sent for real.
            if self.dry_run {
                tracing::info!(
                    market_id = %market_id,
                    answer = %book.leg(planned_trade.leg).text,
                    action = %planned_trade.decision.action,
                    edge = planned_trade.p_internal - planned_trade.p_market,
                    amount_mana = planned_trade.decision.amount_mana,
                    shares = ?planned_trade.decision.shares,
                    "planned trade"
                );
            }

            let Some(record) = self
                .execute(book, planned_trade, trigger_post_uri, now, self.dry_run)
                .await?
            else {
                continue;
            };

            book.persist(&self.store, &self.db)?;
            self.emit_trade(book, &record);
            first.get_or_insert(record);
        }

        Ok(first)
    }

    /// Loads the book, failing when the market is not tracked.
    fn load_book(&self, market_id: &str) -> AppResult<Book> {
        Book::load(&self.store, &self.db, market_id)?
            .ok_or_else(|| anyhow!("market {market_id} is not tracked"))
    }

    /// Brings the book in step with Manifold before a decision.
    ///
    /// Refreshes the market kind, applies fresh prices (which may discover new
    /// answers), reconciles the position from the authoritative `/positions`
    /// response, and persists the result. Belief columns stay with triage; this
    /// writes prices, text, positions, and the root aggregate only.
    async fn prepare(&self, book: &mut Book, snapshot: &MarketSnapshot, now: i64) -> AppResult<()> {
        book.refresh_kind(
            &self.store,
            &self.db,
            &snapshot.outcome_type,
            snapshot.should_answers_sum_to_one,
        )?;

        let positions = self
            .client
            .positions(book.id())
            .await
            .with_context(|| format!("fetching the Manifold positions for {}", book.id()))?;

        let observations: Vec<AnswerObservation<'_>> = snapshot
            .answers
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .map(|answer| AnswerObservation {
                id: &answer.id,
                text: &answer.text,
                probability: answer.probability,
            })
            .collect();

        book.observe_prices(&self.db, now, snapshot.probability, &observations)?;
        book.reconcile(&positions);
        book.persist(&self.store, &self.db)
    }

    /// Reconciles one market with Manifold without trading.
    ///
    /// Updates `p_market_last`, shares, and invested mana from the remote
    /// snapshot, warns on a mismatch, writes the database row, and refreshes
    /// the cache.
    pub async fn sync_market(&self, market_id: &str) -> AppResult<TrackedMarket> {
        let _guard = self.execution_lock.lock().await;

        let mut book = self.load_book(market_id)?;

        let snapshot = self
            .client
            .market(market_id)
            .await
            .with_context(|| format!("fetching Manifold market {market_id}"))?;

        let now = Utc::now().timestamp();
        self.prepare(&mut book, &snapshot, now).await?;

        if book.shape() == Shape::Answers {
            tracing::debug!(
                market_id = %market_id,
                answers = book.legs().len(),
                invested = book.aggregate().invested,
                "synced multiple-choice answers"
            );
        }

        Ok(book.market())
    }

    /// Syncs and evaluates every active market once.
    ///
    /// This is the 60-second trade check. `evaluate_market` performs the
    /// reconciliation itself, so the ticker makes one price and one position
    /// request per market. Failures are collected per market so one broken
    /// market cannot stop the tick for the others. The active set comes from
    /// the store's snapshot, so a paused market is skipped without a query.
    pub async fn sync_all(&self) -> Vec<(MarketId, anyhow::Error)> {
        let markets = self.store.active();

        let mut errors = Vec::new();
        for market in markets {
            let market_id = market.id.clone();
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
    ///
    /// The request is idempotent. A locally closed market makes no HTTP call.
    /// A resolved market is not sold because Manifold settles it automatically.
    /// The returned [`LiquidationOutcome`] tells the caller what happened.
    pub async fn liquidate(&self, market_id: &str) -> AppResult<LiquidationOutcome> {
        let _guard = self.execution_lock.lock().await;

        let mut book = self.load_book(market_id)?;
        if book.status() == MarketStatus::Closed {
            tracing::info!(market_id = %market_id, "market is already closed; skipping liquidation");
            return Ok(LiquidationOutcome::AlreadyClosed);
        }

        let snapshot = self
            .client
            .market(market_id)
            .await
            .with_context(|| format!("fetching Manifold market {market_id} for liquidation"))?;

        let now = Utc::now().timestamp();
        self.prepare(&mut book, &snapshot, now).await?;

        if snapshot.is_resolved {
            tracing::info!(
                market_id = %market_id,
                "market is resolved on Manifold; positions settle automatically"
            );
            return Ok(LiquidationOutcome::AlreadyResolved);
        }

        let sides = book.sell_sides(false);
        if sides.is_empty() {
            tracing::info!(market_id = %market_id, "no shares to liquidate");
            return Ok(LiquidationOutcome::NoPosition);
        }

        for (leg, action, held) in sides {
            let view = book.view(leg, now, self.halflife_hours);
            let planned = PlannedTrade {
                leg,
                decision: TradeDecision {
                    action,
                    amount_mana: 0.0,
                    shares: Some(held),
                    answer_id: book.leg(leg).answer_id.clone(),
                },
                p_internal: view.p_internal,
                p_market: view.p_market,
            };

            let record = self
                .execute(&mut book, &planned, None, now, false)
                .await
                .with_context(|| format!("liquidating {action} on market {market_id}"))?;
            let Some(record) = record else {
                continue;
            };

            book.persist(&self.store, &self.db)?;
            self.emit_trade(&book, &record);
        }

        Ok(LiquidationOutcome::Sold)
    }

    /// Liquidates every market that is not already closed.
    ///
    /// Paused markets are included because `/panic` must flatten what the bot
    /// would resume holding. Errors are collected rather than aborting the
    /// sweep, so one unsellable market does not strand the others.
    pub async fn liquidate_all(&self) -> Vec<(MarketId, anyhow::Error)> {
        let markets = match self.store.list() {
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
    /// no history and events are best-effort. The available budget comes from
    /// the book's aggregate, so it reflects the post-trade position.
    fn emit_trade(&self, book: &Book, record: &TradeRecord) {
        let _ = self.events.send(trade_event(&book.market(), record));
    }

    /// Publishes a trade-loop failure.
    fn emit_error(&self, market_id: Option<MarketId>, message: String) {
        let _ = self.events.send(TradeEvent::Error { market_id, message });
    }

    /// Runs the trade executor.
    ///
    /// With `MANIFOLD_WS_ENABLED` (the default) the loop has three arms:
    /// material pipeline signals (a trade check against the mirrored prices),
    /// websocket `ManifoldEvent`s (price mirror + debounced trade check), and
    /// a 5-minute reconcile ticker (batched REST backstop). The legacy branch
    /// below restores the old polling loop when the flag is `0`.
    ///
    /// No single failure stops the loop: trade errors are logged, broadcast
    /// lag is logged, and a closed pipeline channel ends the loop.
    pub async fn run(
        self: Arc<Self>,
        mut events: broadcast::Receiver<PipelineEvent>,
        mut manifold_events: mpsc::Receiver<ManifoldEvent>,
    ) -> AppResult<()> {
        if !self.ws_enabled {
            // LEGACY: delete after the websocket canary has proved out.
            tracing::info!("MANIFOLD_WS_ENABLED=0; running the legacy 60-second REST loop");
            drop(manifold_events);
            return self.run_legacy(events).await;
        }

        tracing::info!("MANIFOLD_WS_ENABLED=1; live prices from the websocket");

        let mut reconcile = tokio::time::interval(RECONCILE_INTERVAL);
        reconcile.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The first tick completes immediately and is deliberately kept: the
        // boot pass re-establishes price truth right after startup, which
        // matters because the websocket never replays missed events.
        let mut debouncer = EvaluationDebouncer::new(Duration::from_secs(WS_DEBOUNCE_SECS));
        let mut ws_open = true;
        let mut ws_closed_at: Option<Instant> = None;
        let mut ws_down_warned = false;

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
                                .evaluate_cached(&market_id, Some(&post_uri))
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
                maybe = manifold_events.recv(), if ws_open => {
                    match maybe {
                        Some(ManifoldEvent::ContractUpdate(update)) => {
                            // Untracked markets are broadcast to everyone at
                            // once; mirroring is only meaningful for ours.
                            if self.store.cached(&update.contract_id).is_none() {
                                continue;
                            }

                            let patch = ProbPatch::from(update.probs);
                            if let Err(error) =
                                self.mirror_prices(&update.contract_id, &patch)
                            {
                                tracing::error!(
                                    market_id = %update.contract_id,
                                    error = %error,
                                    "mirroring a websocket price update failed"
                                );
                                continue;
                            }

                            // The resolution signature (see `patch_is_terminal`):
                            // settled positions resolve automatically, so any
                            // trade attempt would fail against Manifold.
                            if patch_is_terminal(&patch) {
                                tracing::debug!(
                                    market_id = %update.contract_id,
                                    "probability patch is at an outcome extreme; \
                                     skipping evaluation"
                                );
                                continue;
                            }

                            // Paused or closed markets keep their fresh
                            // mirrored price but are never evaluated.
                            if self
                                .store
                                .cached(&update.contract_id)
                                .is_some_and(|market| market.status != MarketStatus::Active)
                            {
                                continue;
                            }

                            if !debouncer.due(Instant::now(), &update.contract_id) {
                                continue;
                            }
                            // Circular-safety: a fill moves the market toward
                            // our belief, so re-evaluating right after our own
                            // buy sees the edge collapse. The planner's exit
                            // branch no-ops unless edge < EXIT threshold and
                            // entry needs |edge| ≥ ENTRY; belief only moves on
                            // triage impulses (never on our bets), so the
                            // sequence converges and at most one action fires
                            // per evaluation. No other feedback state was
                            // added.
                            tracing::debug!(
                                market_id = %update.contract_id,
                                "evaluating after a websocket price update"
                            );
                            let market_id = update.contract_id;
                            if let Err(error) = self.evaluate_cached(&market_id, None).await {
                                tracing::error!(
                                    market_id = %market_id,
                                    error = %error,
                                    "trade check failed after a price update"
                                );
                                self.emit_error(Some(market_id), error.to_string());
                            }
                        }
                        // S2 (new-market sniping) and S3 (copy-trading) are
                        // later phases of `docs/ALGO.md`; both events are
                        // parsed but intentionally inert here.
                        Some(ManifoldEvent::NewContract(event)) => {
                            tracing::debug!(
                                contract_id = %event.contract.id,
                                slug = ?event.contract.slug,
                                outcome_type = ?event.contract.outcome_type,
                                "new contract broadcast; sniping is a later phase"
                            );
                        }
                        Some(ManifoldEvent::Bet(bet)) => {
                            tracing::debug!(
                                contract_id = %bet.contract_id,
                                "bet broadcast; copy-trading is a later phase"
                            );
                        }
                        // The channel closes only when the socket task is
                        // gone (all senders dropped). From here the 5-minute
                        // reconcile alone keeps the bot conservative.
                        None => {
                            ws_open = false;
                            ws_closed_at = Some(Instant::now());
                            tracing::info!(
                                "manifold event stream closed; running on the reconcile backstop"
                            );
                        }
                    }
                }
                _ = reconcile.tick() => {
                    // One-line warning once the websocket has been gone for a
                    // full reconcile cycle. Reconnects are invisible here (the
                    // channel just starts delivering again), which is fine:
                    // mirror prices resume with the next event.
                    if !ws_down_warned {
                        if let Some(closed_at) = ws_closed_at {
                            if closed_at.elapsed() >= RECONCILE_INTERVAL {
                                tracing::warn!(
                                    "manifold websocket has been closed for a reconcile cycle; \
                                     running on the 5-minute REST backstop"
                                );
                                ws_down_warned = true;
                            }
                        }
                    }

                    let summary = self.reconcile_pass().await;
                    tracing::info!(
                        markets_seen = summary.markets_seen,
                        mismatches = summary.mismatches,
                        tokens_spent = summary.tokens_spent,
                        tokens_skipped = summary.tokens_skipped,
                        "reconcile pass complete"
                    );
                    if summary.tokens_skipped > 0 {
                        tracing::warn!(
                            skipped = summary.tokens_skipped,
                            "some reconcile reads were skipped for the trade reserve"
                        );
                    }
                }
            }
        }
    }

    /// Consumes pipeline signals and runs a 60-second REST sync ticker.
    ///
    /// LEGACY: the pre-websocket control flow, selected by
    /// `MANIFOLD_WS_ENABLED=0` while the websocket path canaries. Material
    /// signals trigger an immediate trade check for their market. The ticker
    /// reconciles every active market and then evaluates it, which is what
    /// catches exits caused by belief decay between posts. Delete it with the
    /// flag after canary.
    async fn run_legacy(
        self: Arc<Self>,
        mut events: broadcast::Receiver<PipelineEvent>,
    ) -> AppResult<()> {
        let mut ticker = tokio::time::interval(LEGACY_SYNC_INTERVAL);
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

    /// Mirrors one observed probability patch into the store (cache + rows).
    ///
    /// Probabilities are clamped into the trading range exactly as
    /// [`Book::observe_prices`] clamps a REST snapshot, so both observation
    /// channels store comparable values.
    fn mirror_prices(&self, market_id: &str, patch: &ProbPatch) -> AppResult<()> {
        let answers: Vec<(MarketId, f64)> = patch
            .answers
            .iter()
            .map(|(answer_id, prob)| {
                (
                    answer_id.clone(),
                    prob.clamp(MIN_TRADING_PROB, MAX_TRADING_PROB),
                )
            })
            .collect();
        self.store.market_prices_observed(
            market_id,
            patch
                .root
                .map(|prob| prob.clamp(MIN_TRADING_PROB, MAX_TRADING_PROB)),
            &answers,
        )
    }

    /// The slow REST reconcile backstop (every [`RECONCILE_INTERVAL`]).
    ///
    /// Three parts, each of which may skip on a rate refusal:
    ///
    /// 1. **Batched probabilities** — `GET /v0/market-probs` in 100-ID chunks
    ///    for every active market, written through the same price mirror as
    ///    websocket updates. A response whose shape disagrees with the stored
    ///    market kind, or announces an answer we have never recorded, runs
    ///    the full per-market sync for that one market (kind refresh, answer
    ///    discovery with text).
    /// 2. **One portfolio snapshot** — the paginated
    ///    `get-user-contract-metrics-with-contracts` call replaces the legacy
    ///    per-market `/positions` sync. Every tracked market present in the
    ///    snapshot is reconciled through [`Book::reconcile`] (the same
    ///    warn-on-mismatch-trust-remote comparison), and when the pagination
    ///    is complete, tracked markets *absent* from it are treated as flat.
    /// 3. A single summary line, logged by the caller of this method.
    ///
    /// Per-market failures are logged and emitted so one broken market cannot
    /// stall the pass.
    async fn reconcile_pass(&self) -> ReconcileSummary {
        let mut summary = ReconcileSummary::default();
        let markets = self.store.active();
        summary.markets_seen = markets.len();

        // 1. Batched probability refresh.
        for chunk in markets.chunks(PROB_CHUNK_SIZE) {
            let ids: Vec<String> = chunk.iter().map(|market| market.id.clone()).collect();
            if !summary.acquire(&self.rate, "market-probs chunk") {
                continue;
            }
            for (market_id, probs) in match self.client.market_probs(&ids).await {
                Ok(probs) => probs,
                Err(error) => {
                    tracing::error!(error = %error, "batched probability refresh failed");
                    continue;
                }
            } {
                if self.store.cached(&market_id).is_none() {
                    continue;
                }
                if self.price_sync_needed(&market_id, &probs).await {
                    summary.mismatches += 1;
                    // The full sync spends one token per REST request it
                    // makes (market snapshot + positions).
                    if !summary.acquire(&self.rate, "full state sync (market)") {
                        continue;
                    }
                    if !summary.acquire(&self.rate, "full state sync (positions)") {
                        continue;
                    }
                    if let Err(error) = self.sync_market(&market_id).await {
                        tracing::error!(
                            market_id = %market_id,
                            error = %error,
                            "full state sync failed"
                        );
                        self.emit_error(Some(market_id.clone()), error.to_string());
                        continue;
                    }
                    continue;
                }

                let patch = ProbPatch::from(probs);
                if let Err(error) = self.mirror_prices(&market_id, &patch) {
                    tracing::error!(
                        market_id = %market_id,
                        error = %error,
                        "writing reconciled probabilities failed"
                    );
                    self.emit_error(Some(market_id.clone()), error.to_string());
                }
            }
        }

        // 2. One portfolio snapshot for position truth.
        let mut by_contract: HashMap<MarketId, Vec<Value>> = HashMap::new();
        let mut offset = 0;
        let mut complete = false;
        let mut pages = 0;
        while pages < PORTFOLIO_MAX_PAGES {
            if pages == 0 && !summary.acquire(&self.rate, "portfolio") {
                break;
            }
            let (page, final_page) = match self.client.portfolio_page(offset).await {
                Ok((page, final_page)) => (page, final_page),
                Err(error) => {
                    tracing::error!(error = %error, "portfolio snapshot fetch failed");
                    break;
                }
            };
            let page_size: usize = page.values().map(Vec::len).sum();
            by_contract.extend(page);
            complete = final_page;
            offset += page_size;
            pages += 1;
            if final_page {
                break;
            }
            if !summary.acquire(&self.rate, "portfolio page") {
                break;
            }
        }
        if pages == PORTFOLIO_MAX_PAGES && !complete {
            tracing::warn!(
                pages,
                "portfolio pagination hit the page cap; positions stay as-is"
            );
        }

        for (market_id, entries) in &by_contract {
            if self.store.cached(market_id).is_none() {
                continue;
            }
            let positions = parse_position_entries(entries.as_slice(), self.client.bot_user_id());
            if let Err(error) = self
                .reconcile_book_position(market_id, &positions, &mut summary.mismatches)
                .await
            {
                tracing::error!(
                    market_id = %market_id,
                    error = %error,
                    "portfolio position reconciliation failed"
                );
                self.emit_error(Some(market_id.clone()), error.to_string());
            }
        }

        // Only a fully paginated snapshot may declare a tracked market flat:
        // a truncated one would wrongly zero real positions.
        if complete {
            for market in &markets {
                if by_contract.contains_key(&market.id) {
                    continue;
                }
                let positions = MarketPositions::default();
                if let Err(error) = self
                    .reconcile_book_position(&market.id, &positions, &mut summary.mismatches)
                    .await
                {
                    tracing::error!(
                        market_id = %market.id,
                        error = %error,
                        "position reconciliation for a flat market failed"
                    );
                    self.emit_error(Some(market.id.clone()), error.to_string());
                }
            }
        } else if pages > 0 {
            tracing::warn!("portfolio snapshot did not finish; reconciled what was fetched only");
        }

        summary
    }

    /// Reconciles one market's position through the book, counting mismatches.
    async fn reconcile_book_position(
        &self,
        market_id: &str,
        positions: &MarketPositions,
        mismatches: &mut usize,
    ) -> AppResult<()> {
        let mut book = self.load_book(market_id)?;
        if book.status() != MarketStatus::Active {
            return Ok(());
        }

        *mismatches += book.reconcile(positions);
        book.persist(&self.store, &self.db)
    }

    /// Whether one batched price reading disagrees with the tracked market's
    /// stored shape: a multiple-choice market with a bare probability, a
    /// binary market with answer probabilities, or an answer ID we have never
    /// recorded (a brand-new answer, which needs its text from the full
    /// snapshot).
    ///
    /// These need the full per-market sync, which refreshes the kind,
    /// discovers new answers with their text, and re-syncs positions.
    async fn price_sync_needed(&self, market_id: &str, probs: &MarketProbs) -> bool {
        let is_multiple_choice = self
            .store
            .cached(market_id)
            .is_some_and(|market| market.is_multiple_choice());

        match (probs, is_multiple_choice) {
            (MarketProbs::Binary { .. }, _) if !is_multiple_choice => false,
            (MarketProbs::Answers { answers }, true) => {
                let known: Vec<MarketId> = match self.db.list_answers(market_id) {
                    Ok(list) => list.into_iter().map(|answer| answer.id).collect(),
                    Err(_) => return true,
                };
                answers
                    .iter()
                    .any(|(answer_id, _)| !known.iter().any(|id| id == answer_id))
            }
            _ => true,
        }
    }

    /// Performs one planned action and writes its ledger row.
    ///
    /// A buy keeps the budget invariant checks, sends `dryRun` when asked, and
    /// records the fill on its leg. A sell sends the requested share count and
    /// lets the book release the proportional cost basis. Neither branch writes
    /// a position column: the caller calls [`Book::persist`] afterwards.
    async fn execute(
        &self,
        book: &mut Book,
        planned: &PlannedTrade,
        trigger_post_uri: Option<&str>,
        now: i64,
        dry_run: bool,
    ) -> AppResult<Option<TradeRecord>> {
        let decision = &planned.decision;

        if is_buy(decision.action) {
            let amount = decision.amount_mana;
            if !amount.is_finite() || amount <= 0.0 {
                bail!("buy amount must be a positive finite mana value, got {amount}");
            }

            let available = book.available_budget();
            if amount > available + BUDGET_EPSILON {
                bail!(
                    "budget invariant violated for market {}: buy of {amount} mana exceeds \
                     available budget {available}",
                    book.id()
                );
            }
            let market = book.market();
            let committed = market.invested_mana + amount;
            let ceiling = market.budget_mana + market.realized_pnl_mana;
            if committed > ceiling + BUDGET_EPSILON {
                bail!(
                    "budget invariant violated for market {}: invested {committed} would exceed \
                     budget plus realized PnL {ceiling}",
                    book.id()
                );
            }

            if !self.acquire_write_token(book, decision.action) {
                return Ok(None);
            }

            let outcome = decision.action.outcome();
            let result = self
                .client
                .buy(
                    book.id(),
                    outcome,
                    amount,
                    decision.answer_id.as_deref(),
                    dry_run,
                )
                .await
                .with_context(|| format!("buying {outcome} on Manifold market {}", book.id()))?;
            ensure_filled(&result, book.id())?;
            if !result.shares.is_finite() || result.shares <= 0.0 {
                bail!(
                    "Manifold bet {} on market {} returned no shares",
                    result.bet_id(),
                    book.id()
                );
            }

            if dry_run {
                tracing::info!(
                    market_id = %book.id(),
                    answer_id = ?decision.answer_id,
                    action = %decision.action,
                    amount,
                    shares = result.shares,
                    prob_before = ?result.prob_before,
                    prob_after = ?result.prob_after,
                    "dry run: simulated buy, nothing persisted"
                );
                return Ok(None);
            }

            let cost = result.total_cost();
            book.apply_buy(planned.leg, decision.action, cost, result.shares)?;

            return self.record_trade(
                book,
                planned,
                trigger_post_uri,
                now,
                Fill {
                    mana_amount: cost,
                    shares_delta: result.shares,
                    result: &result,
                },
            );
        }

        let requested = decision.shares.unwrap_or(0.0);
        if !requested.is_finite() || requested <= 0.0 {
            bail!("sell must dispose of a positive finite share count, got {requested}");
        }

        if dry_run {
            tracing::info!(
                market_id = %book.id(),
                answer_id = ?decision.answer_id,
                action = %decision.action,
                shares = requested,
                "dry run: would sell, nothing persisted"
            );
            return Ok(None);
        }

        if !self.acquire_write_token(book, decision.action) {
            return Ok(None);
        }

        let outcome = decision.action.outcome();
        let result = self
            .client
            .sell(book.id(), outcome, requested, decision.answer_id.as_deref())
            .await
            .with_context(|| format!("selling {outcome} on Manifold market {}", book.id()))?;
        ensure_filled(&result, book.id())?;

        // Prefer the filled share count from Manifold; fall back to the request
        // when the response omits it. The settlement clamps to the held amount.
        let sold = if result.shares.is_finite() && result.shares.abs() > 0.0 {
            result.shares.abs()
        } else {
            requested
        };
        let proceeds = result.net_proceeds();
        let settlement = book.apply_sell(planned.leg, decision.action, sold, proceeds)?;

        self.record_trade(
            book,
            planned,
            trigger_post_uri,
            now,
            Fill {
                mana_amount: proceeds,
                shares_delta: -settlement.sold,
                result: &result,
            },
        )
    }

    /// Writes the ledger row for a filled action.
    fn record_trade(
        &self,
        book: &Book,
        planned: &PlannedTrade,
        trigger_post_uri: Option<&str>,
        now: i64,
        fill: Fill<'_>,
    ) -> AppResult<Option<TradeRecord>> {
        let record = TradeRecord {
            id: 0,
            market_id: book.id().to_owned(),
            answer_id: planned.decision.answer_id.clone(),
            action: planned.decision.action,
            mana_amount: fill.mana_amount,
            shares_delta: fill.shares_delta,
            p_internal: planned.p_internal,
            p_market: planned.p_market,
            trigger_post_uri: trigger_post_uri.map(str::to_owned),
            manifold_bet_id: optional_id(fill.result.bet_id()),
            created_at: now,
        };
        let id = self.db.insert_trade(&record)?;

        Ok(Some(TradeRecord { id, ..record }))
    }

    /// Reads the panic switch. Only the literal `true` blocks buys.
    fn kill_switch_on(&self) -> AppResult<bool> {
        Ok(self
            .db
            .get_kv(KILL_SWITCH_KEY)?
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("true")))
    }

    /// Spends one Trade-class token for an upcoming Manifold write.
    ///
    /// Every write costs the bucket one token, matching Manifold's per-request
    /// limit. When the bucket is at the trade floor the write is refused here,
    /// the same shape as a kill-switch buy refusal: the caller skips the REST
    /// call and the next material signal or 60-second tick re-runs it.
    /// Returning `false` instead of an error keeps a rate refusal out of the
    /// per-market error path, where it would be indistinguishable from a
    /// broken market.
    fn acquire_write_token(&self, book: &Book, action: TradeAction) -> bool {
        match self.rate.try_acquire(Priority::Trade, WRITE_COST_TOKENS) {
            Ok(_) => true,
            Err(denied) => {
                tracing::warn!(
                    market_id = %book.id(),
                    action = %action,
                    remaining_tokens = denied.remaining,
                    floor_tokens = denied.threshold,
                    "Manifold rate bucket is at the trade floor; skipping write"
                );
                false
            }
        }
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

    use crate::belief;
    use crate::book::{MAX_TRADING_PROB, MIN_TRADING_PROB, TradeLimits, TradeParams};

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
            market_url: String::new(),
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
            deleted_at: None,
            outcome_type: "BINARY".to_owned(),
            should_answers_sum_to_one: false,
        }
    }

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-9,
            "expected {expected}, got {actual}"
        );
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

        assert!(bet.id.is_none());
        assert_eq!(bet.bet_id(), "bet-2");
        assert_close(bet.total_cost(), 5.0);
        assert!(bet.fees.is_none());
        assert!(!bet.is_filled, "missing isFilled defaults to false");
    }

    #[test]
    fn bet_result_accepts_a_dry_run_null_id() {
        let raw = json!({
            "id": null,
            "betId": "dry-run",
            "amount": 1.0,
            "shares": 3.3,
            "isFilled": true
        });

        let bet: BetResult = serde_json::from_value(raw).expect("dry-run shape should deserialize");

        assert_eq!(bet.bet_id(), "dry-run");
        assert!(bet.is_filled);
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

        let position = parse_market_positions(&raw, "bot-1").root;

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

        let position = parse_market_positions(&raw, "bot-1").root;

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
        let position = parse_market_positions(&raw, "bot-1").root;
        assert_close(position.no_shares, 7.0);
        assert_close(position.invested, 4.0);

        // Can also parse for a specific answerId
        let multi_raw = json!([
            {"userId": "bot-1", "answerId": "ans-a", "totalShares": {"YES": 15.0}, "invested": 10.0},
            {"userId": "bot-1", "answerId": "ans-b", "totalShares": {"NO": 8.0}, "invested": 5.0}
        ]);
        let positions = parse_market_positions(&multi_raw, "bot-1");
        let pos_a = positions.answer("ans-a");
        assert_close(pos_a.yes_shares, 15.0);
        assert_close(pos_a.invested, 10.0);
        let pos_b = positions.answer("ans-b");
        assert_close(pos_b.no_shares, 8.0);
        assert_close(pos_b.invested, 5.0);

        // The server filters by userId; a single anonymous entry is still ours.
        let single = json!([{"totalShares": {"YES": 2.0}, "invested": 1.0}]);
        let position = parse_market_positions(&single, "bot-1").root;
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
                parse_market_positions(&raw, "bot-1").root,
                Position::default(),
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
        let position = parse_market_positions(&raw, "bot-1").root;
        assert_close(position.yes_shares, 5.0);
    }

    #[test]
    fn market_positions_split_the_root_row_from_the_answers() {
        let raw = json!([
            {
                "userId": "bot-1",
                "answerId": null,
                "totalShares": {"YES": 10.0, "NO": 7.0},
                "invested": 11.0
            },
            {"userId": "bot-1", "answerId": "ans-a", "totalShares": {"YES": 10.0}, "invested": 6.0},
            {"userId": "bot-1", "answerId": "ans-b", "totalShares": {"NO": 7.0}, "invested": 5.0},
            {"userId": "other", "answerId": "ans-a", "totalShares": {"YES": 99.0}, "invested": 50.0}
        ]);

        let positions = parse_market_positions(&raw, "bot-1");

        assert!(positions.saw_root);
        assert!(positions.checksum_ok());
        assert_eq!(positions.answers.len(), 2);
        assert_close(positions.answer("ans-a").yes_shares, 10.0);
        assert_close(positions.answer("ans-b").no_shares, 7.0);
        assert_close(positions.totals().invested, 11.0);
        assert_close(positions.answer("missing").invested, 0.0);
    }

    #[test]
    fn market_positions_checksum_detects_a_truncated_response() {
        let raw = json!([
            {"userId": "bot-1", "totalShares": {"YES": 18.0}, "invested": 11.0},
            {"userId": "bot-1", "answerId": "ans-a", "totalShares": {"YES": 10.0}, "invested": 6.0}
        ]);

        let positions = parse_market_positions(&raw, "bot-1");

        assert!(positions.saw_root);
        assert!(!positions.checksum_ok());
    }

    #[test]
    fn market_positions_without_a_root_row_are_consistent() {
        let raw = json!([
            {"userId": "bot-1", "answerId": "ans-a", "totalShares": {"YES": 10.0}, "invested": 6.0}
        ]);

        let positions = parse_market_positions(&raw, "bot-1");

        assert!(!positions.saw_root);
        assert!(positions.checksum_ok());
        assert_close(positions.totals().invested, 6.0);
    }

    #[test]
    fn market_snapshot_reads_answers_and_the_sum_to_one_flag() {
        let raw = json!({
            "id": "mc-market",
            "question": "Who wins?",
            "outcomeType": "MULTIPLE_CHOICE",
            "shouldAnswersSumToOne": true,
            "isResolved": false,
            "answers": [
                {"id": "a1", "text": "Candidate A", "probability": 0.6},
                {"id": "a2", "text": "Candidate B", "probability": 0.4}
            ]
        });

        let snapshot: MarketSnapshot =
            serde_json::from_value(raw).expect("MC snapshot should deserialize");

        assert_eq!(snapshot.outcome_type, "MULTIPLE_CHOICE");
        assert!(snapshot.should_answers_sum_to_one);
        let answers = snapshot.answers.expect("answers are present");
        assert_eq!(answers.len(), 2);
        assert_close(answers[0].probability.expect("price"), 0.6);
    }

    // -- Parsers ------------------------------------------------------------

    #[test]
    fn market_probs_response_parses_both_shapes() {
        let raw = json!({
            "binary-market": {"prob": 0.62},
            "multi-market": {"answerProbs": {"a1": 0.67, "a2": 0.33}}
        });

        let probs = parse_market_probs(&raw);

        assert_eq!(probs.len(), 2);
        assert_eq!(
            probs.get("binary-market"),
            Some(&MarketProbs::Binary { prob: 0.62 })
        );
        assert_eq!(
            probs.get("multi-market"),
            Some(&MarketProbs::Answers {
                answers: vec![("a1".to_owned(), 0.67), ("a2".to_owned(), 0.33)]
            })
        );
    }

    #[test]
    fn market_probs_response_skips_unrecognized_and_empty_shapes() {
        // A shape we do not know (or a market without probabilities) must be
        // skipped, tolerated, and never lose the whole pass.
        let raw = json!({
            "known": {"prob": 0.1},
            "broken": {"weird": 1},
            "empty-answers": {"answerProbs": {}},
            "null-prob": {"prob": null}
        });

        let probs = parse_market_probs(&raw);
        assert_eq!(
            probs.get("known"),
            Some(&MarketProbs::Binary { prob: 0.1 }),
            "the valid entry survives"
        );
        assert_eq!(probs.get("broken"), None);
        assert_eq!(probs.get("empty-answers"), None);
        assert_eq!(probs.get("null-prob"), None);
        assert!(parse_market_probs(&json!([1, 2, 3])).is_empty());
        assert!(parse_market_probs(&json!("nope")).is_empty());
    }

    #[test]
    fn metrics_by_contract_extracts_rentered_pages() {
        let raw = json!({
            "metricsByContract": {
                "bin": [
                    {"userId": "bot-1", "totalShares": {"YES": 3.0}, "invested": 2.0, "profit": 1.0}
                ],
                "mc": [
                    {"userId": "bot-1", "totalShares": {"YES": 10.0, "NO": 4.0}, "invested": 8.0},
                    {"userId": "bot-1", "answerId": "a1", "totalShares": {"YES": 10.0}, "invested": 6.0},
                    {"userId": "bot-1", "answerId": "a2", "totalShares": {"NO": 4.0}, "invested": 2.0}
                ],
                "skipped": []
            }
        });

        let by_contract = parse_metrics_by_contract(&raw);
        assert_eq!(by_contract.len(), 2, "empty metric lists are dropped");

        let positions = parse_position_entries(&by_contract["mc"], "bot-1");
        assert!(positions.saw_root);
        assert!(positions.checksum_ok());
        assert_close(positions.root.yes_shares, 10.0);
        assert_close(positions.root.invested, 8.0);
        assert_close(positions.answer("a1").yes_shares, 10.0);
        assert_close(positions.answer("a2").no_shares, 4.0);

        let binary = parse_position_entries(&by_contract["bin"], "bot-1");
        assert_close(binary.root.yes_shares, 3.0);
    }

    #[test]
    fn metrics_by_contract_tolerates_a_missing_or_shapeless_key() {
        assert!(parse_metrics_by_contract(&json!({})).is_empty());
        assert!(parse_metrics_by_contract(&json!({"metricsByContract": null})).is_empty());
        assert!(parse_metrics_by_contract(&json!({"metricsByContract": {"c": "no"}})).is_empty());
    }

    #[test]
    fn terminal_patches_are_detected_across_both_shapes() {
        // Binary at 0 or 1: the resolution signature of the legacy isResolved
        // check, which a price patch cannot carry.
        assert!(patch_is_terminal(&ProbPatch {
            root: Some(1.0),
            answers: Vec::new()
        }));
        assert!(patch_is_terminal(&ProbPatch {
            root: Some(0.0),
            answers: Vec::new()
        }));
        assert!(!patch_is_terminal(&ProbPatch {
            root: Some(0.5),
            answers: Vec::new()
        }));
        assert!(!patch_is_terminal(&ProbPatch {
            root: Some(0.99),
            answers: Vec::new()
        }));

        // A multiple-choice market counts only when every answer is settled:
        // some answers of an open market may legitimately sit at the extremes.
        assert!(patch_is_terminal(&ProbPatch {
            root: None,
            answers: vec![("a".to_owned(), 1.0), ("b".to_owned(), 0.0)]
        }));
        assert!(!patch_is_terminal(&ProbPatch {
            root: None,
            answers: vec![("a".to_owned(), 1.0), ("b".to_owned(), 0.3)]
        }));
        assert!(
            !patch_is_terminal(&ProbPatch {
                root: None,
                answers: Vec::new()
            }),
            "an empty patch has no observation, hence no verdict"
        );
    }

    // -- Debounce -----------------------------------------------------------

    #[test]
    fn the_debouncer_coalesces_a_burst_into_one_evaluation() {
        let window = Duration::from_millis(2_000);
        let mut debouncer = EvaluationDebouncer::new(window);
        let t0 = Instant::now();

        assert!(debouncer.due(t0, &"m1".to_owned()));
        // Every update inside the window is folded into the mirror only.
        assert!(!debouncer.due(t0 + Duration::from_millis(300), &"m1".to_owned()));
        assert!(!debouncer.due(t0 + Duration::from_millis(1_999), &"m1".to_owned()));
        // Just past the window the evaluation becomes due again.
        assert!(debouncer.due(t0 + Duration::from_millis(2_001), &"m1".to_owned()));
    }

    #[test]
    fn the_debouncer_throttles_markets_independently() {
        let window = Duration::from_secs(1);
        let mut debouncer = EvaluationDebouncer::new(window);
        let t0 = Instant::now();

        assert!(debouncer.due(t0, &"m1".to_owned()));
        assert!(debouncer.due(t0 + Duration::from_millis(1), &"m2".to_owned()));
        // m1 still throttled by its own timestamp, whatever m2 did.
        assert!(!debouncer.due(t0 + Duration::from_millis(2), &"m1".to_owned()));
    }

    #[test]
    fn probs_patch_converts_websocket_and_rest_shapes_the_same_way() {
        let from_ws = ProbPatch::from(ContractProbs::Binary { prob: 0.7 });
        assert_eq!(
            from_ws,
            ProbPatch {
                root: Some(0.7),
                answers: Vec::new()
            }
        );

        let from_ws = ProbPatch::from(ContractProbs::Multi {
            answers: vec![
                crate::manifold_ws::AnswerProb {
                    id: "a".to_owned(),
                    prob: 0.6,
                },
                crate::manifold_ws::AnswerProb {
                    id: "b".to_owned(),
                    prob: 0.4,
                },
            ],
        });
        assert_eq!(from_ws.root, None);
        assert_eq!(
            from_ws.answers,
            vec![("a".to_owned(), 0.6), ("b".to_owned(), 0.4)]
        );

        let from_rest = ProbPatch::from(MarketProbs::Answers {
            answers: vec![("a".to_owned(), 0.6)],
        });
        assert_eq!(from_rest.root, None);
        assert_eq!(from_rest.answers, vec![("a".to_owned(), 0.6)]);
    }

    // -------------------------------------------------------------------
    // Live calls. Run with `cargo test -- --ignored --nocapture`
    // -------------------------------------------------------------------

    fn live_config() -> Config {
        dotenvy::dotenv().ok();
        let key = std::env::var("MANIFOLD_API_KEY")
            .ok()
            .filter(|key| !key.is_empty())
            .expect("MANIFOLD_API_KEY must be set for the live probe");
        Config {
            jetstream_url: "wss://jetstream2.us-east.bsky.network/subscribe".to_owned(),
            cloudflare_account_id: None,
            triage_model: "openrouter-decisions/baseten/joey-1".to_owned(),
            onboarding_model: "openrouter/anthropic/claude".to_owned(),
            firecrawl_api_key: "fc-".to_owned(),
            manifold_api_key: key,
            discord_bot_token: "d-".to_owned(),
            discord_guild_id: 1,
            discord_control_channel_id: 1,
            discord_operator_user_id: 1,
            default_market_budget: 100.0,
            kelly_fraction: 0.25,
            entry_edge_threshold: 0.05,
            exit_edge_threshold: 0.01,
            min_trade_size: 1.0,
            belief_kappa: 1.1,
            belief_halflife_hours: 4.0,
            triage_min_evidence: 0.7,
            trading_dry_run: true,
            mc_max_positions: 6,
            mc_max_new_per_tick: 2,
            mc_max_answer_fraction: 0.25,
            rate_limit_per_min: 500,
            manifold_ws_enabled: true,
            db_path: std::path::PathBuf::from("/tmp/opencode/manif0ld-mcq-live.db"),
        }
    }

    fn live_mc_market(id: &str, question: &str) -> TrackedMarket {
        TrackedMarket {
            id: id.to_owned(),
            slug: id.to_owned(),
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
            outcome_type: "MULTIPLE_CHOICE".to_owned(),
            should_answers_sum_to_one: false,
        }
    }

    #[test]
    #[ignore = "network call; needs MANIFOLD_API_KEY"]
    fn live_dry_run_places_a_simulated_answer_bet() {
        let config = live_config();
        let db = Db::open_in_memory().expect("in-memory DB");
        let store = Arc::new(MarketStore::new(&db).expect("store loads"));
        store
            .insert(&live_mc_market(
                "thlctZn5PU",
                "Israel election: Who will be the next prime minister?",
            ))
            .expect("insert market");
        let runtime = tokio::runtime::Runtime::new().expect("runtime builds");
        let executor = runtime
            .block_on(TradeExecutor::new(&config, db.clone(), Arc::clone(&store)))
            .expect("executor builds");

        runtime
            .block_on(executor.sync_market("thlctZn5PU"))
            .expect("initial sync");

        // Give one mid-priced answer a confident belief so the planner produces
        // an entry. Everything else stays anchored at its price.
        let answers = db.list_answers("thlctZn5PU").expect("answers");
        let target = answers
            .iter()
            .filter(|answer| answer.p_market_last > 0.05 && answer.p_market_last < 0.5)
            .max_by(|left, right| left.p_market_last.total_cmp(&right.p_market_last))
            .expect("a mid-priced answer");
        let now = Utc::now().timestamp();
        db.update_answer_belief(
            "thlctZn5PU",
            &target.id,
            belief::logit(0.9),
            target.p_market_last,
            now,
        )
        .expect("belief update");

        let record = runtime
            .block_on(executor.evaluate_market("thlctZn5PU", None))
            .expect("dry-run evaluation should succeed");

        assert!(record.is_none(), "a dry run must not persist a trade");
        assert!(
            db.recent_trades("thlctZn5PU", 10)
                .expect("trades")
                .is_empty(),
            "a dry run must not write a ledger row"
        );
        let after = db
            .get_answer("thlctZn5PU", &target.id)
            .expect("answer")
            .expect("tracked");
        assert_eq!(after.yes_shares, 0.0);
        assert_eq!(after.invested_mana, 0.0);
    }

    #[test]
    #[ignore = "network call; needs MANIFOLD_API_KEY"]
    fn live_sync_reads_every_answer_of_both_mechanisms() {
        let config = live_config();
        let db = Db::open_in_memory().expect("in-memory DB");
        let store = Arc::new(MarketStore::new(&db).expect("store loads"));
        store
            .insert(&live_mc_market(
                "thlctZn5PU",
                "Israel election: Who will be the next prime minister?",
            ))
            .expect("insert sum-to-one market");
        store
            .insert(&live_mc_market("hEpuZQpgR2", "US Midterms MEGA Market"))
            .expect("insert independent market");
        let runtime = tokio::runtime::Runtime::new().expect("runtime builds");
        let executor = runtime
            .block_on(TradeExecutor::new(&config, db.clone(), Arc::clone(&store)))
            .expect("executor builds");

        let sum_to_one = runtime
            .block_on(executor.sync_market("thlctZn5PU"))
            .expect("sum-to-one sync");
        let independent = runtime
            .block_on(executor.sync_market("hEpuZQpgR2"))
            .expect("independent sync");

        assert_eq!(sum_to_one.outcome_type, "MULTIPLE_CHOICE");
        assert!(sum_to_one.should_answers_sum_to_one);
        assert_eq!(independent.outcome_type, "MULTIPLE_CHOICE");
        assert!(!independent.should_answers_sum_to_one);

        let sum_answers = db.list_answers("thlctZn5PU").expect("answers");
        let mega_answers = db.list_answers("hEpuZQpgR2").expect("answers");
        assert_eq!(sum_answers.len(), 7);
        assert_eq!(mega_answers.len(), 53);

        let total: f64 = sum_answers.iter().map(|answer| answer.p_market_last).sum();
        assert!(
            (total - 1.0).abs() < 0.02,
            "sum-to-one prices must add up, got {total}"
        );
        assert!(
            mega_answers
                .iter()
                .all(|answer| (MIN_TRADING_PROB..=MAX_TRADING_PROB).contains(&answer.p_market_last)),
            "every answer price must be clamped into the trading range"
        );
        assert!(
            mega_answers
                .iter()
                .any(|answer| answer.p_market_last == MIN_TRADING_PROB
                    || answer.p_market_last == MAX_TRADING_PROB),
            "the MEGA market has resolved answers at 0 or 1"
        );
        assert_eq!(sum_to_one.invested_mana, 0.0);
        assert_eq!(independent.invested_mana, 0.0);
        assert!(sum_answers.iter().all(|answer| answer.invested_mana == 0.0));
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
        let db = Db::open_in_memory().expect("in-memory DB");
        let store = MarketStore::new(&db).expect("store loads");
        store.insert(&market).expect("insert market");
        let book = Book::load(&store, &db, "Market_test")
            .expect("load book")
            .expect("tracked");
        let policy = TradePolicy {
            params: params(),
            multiple_choice: TradeLimits::binary(),
        };
        assert!(
            book.plan(Utc::now().timestamp(), 4.0, &policy).is_empty(),
            "a flat market at its own price must not trade"
        );

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
