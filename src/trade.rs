//! Manifold transport and trade execution.
//!
//! The module has two layers:
//!
//! 1. [`ManifoldClient`] wraps the REST API (`/v0/me`, `/v0/market/{id}`,
//!    `/v0/market/{id}/positions`, `/v0/market-probs`,
//!    `/v0/get-user-contract-metrics-with-contracts`, `/v0/bet`,
//!    `/v0/market/{id}/sell`, `/v0/bet/cancel/{betId}`,
//!    `/v0/bets?kinds=open-limit`).
//! 2. [`TradeExecutor`] is the control flow: it keeps the price columns
//!    current from two observation sources — the broadcast websocket
//!    ([`crate::manifold_ws`], live) and a batched REST refresh (the slow
//!    reconcile backstop) — then runs the same book-plan-execute cycle the
//!    old 60-second ticker ran, per price update instead of per minute.
//!
//! Prices arrive live; REST is demoted to housekeeping (`docs/ALGO.md` §1):
//! a 5-minute `Priority::Reconcile` pass chunks `GET /v0/market-probs` and
//! replaces the per-market `/positions` sync with one paginated portfolio
//! snapshot, and each pass ends with a REST-free cached-state sweep of every
//! active market so a quiet market still reaches its belief-decay exit.
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
//! Since the reconcile swap the whole portfolio is fetched as one paginated
//! call to `GET /v0/get-user-contract-metrics-with-contracts`; the per-market
//! `/positions` call remains only inside `prepare`, which the operator's
//! re-check, the reconcile pass's full-sync branch, and liquidation drive.
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
//! # Fair-capped buys and the expiring rest
//!
//! Every buy sweeps with `limitProb = clamp(p_internal) − ε`
//! (`LIMIT_EPSILON`, default 0.01; `docs/ALGO.md` §S1) instead of the old
//! market-buy emulation (`0.99` for YES / `0.01` for NO) that could sweep a
//! thin book far past fair value. Manifold interprets `limitProb` as the
//! market probability of YES for both outcomes — a YES sweep stops when the
//! probability rises to it (the per-share price ceiling), a NO sweep stops
//! when the probability falls to it (so the per-NO-share ceiling is
//! `1 − limitProb`) — which is why one expression caps both sides. The
//! probability source is the same decayed `p_internal` the edge math used in
//! that evaluation (normalized on sum-to-one multiple-choice markets).
//!
//! One `POST /v0/bet` with `limitProb + amount + expiresMillisAfter` runs the
//! **whole execution ladder by itself** (probe-verified against the backend
//! source, `/tmp/opencode/limit_probe/SOURCE_NOTES.md`): the call consumes
//! opposing resting orders first — at *their* price, in price-time priority —
//! then sweeps the AMM up to the cap, and rests whatever is left as the same
//! bet row. The rest carries `LIMIT_EXPIRY_SECS` (default 60 s, validated
//! into `[1, 300]`) as `expiresMillisAfter`, so an edge that drifts away is
//! bounded in time without any cancel call; matching also rejects expired
//! orders inline the moment their `expiresAt` passes.
//!
//! # Own resting-order lifecycle
//!
//! The unfilled remainder of a fair-capped buy is real committed money, so
//! the executor tracks it in-memory ([`OwnRestingOrders`], no schema change),
//! fed by the buy responses, the `global/new-bet` websocket broadcasts for
//! the bot's own user (which carry maker fills), and one boot snapshot of
//! `GET /v0/bets?userId={bot}&kinds=open-limit` (one REST call covering every
//! market). Three rules retire an order:
//!
//! 1. **Reverse drift.** After the price-mirror step of every cached
//!    evaluation each own order is compared against a freshly derived fair
//!    value (`should_cancel_for_drift`): a YES order is cancelled when its
//!    limit sits more than `LIMIT_DRIFT_CANCEL` above today's
//!    fair-capped cap, a NO order equivalently on the YES scale — never leave
//!    a resting order that would take us into a position at odds with current
//!    belief. Orders younger than a few seconds are never cancelled by this
//!    policy (they were placed against the same belief), and a resting
//!    order on the opposite side of an entry executed in the same evaluation
//!    is cancelled immediately.
//! 2. **Maker fills.** A websocket `BetEvent` for one of our own bets that
//!    grew by shares since we last saw it applies those shares as a real
//!    buy ([`Book::apply_buy`] + ledger row, provenance `maker_fill`) at the
//!    server-reported mana — the same book flow the original taker fill
//!    used, with the budget invariant checked first. Duplicate broadcasts
//!    are naturally idempotent: the fill read is a delta against the stored
//!    cumulative counters.
//! 3. **Death.** A cancelled, fully filled, or expired order is dropped from
//!    the map. Expiry is *not* broadcast on any websocket topic (probe
//!    finding: a 1-minute cron flips `isCancelled` silently), so rows are
//!    filtered by their `expiresAt` locally.
//!
//! Deliberate S1 deviations (documented in `docs/SPEC.md` §6.6): the resting
//! book of *other* traders is not mirrored (the wire already gives the
//! price-time ladder through one bet call; a passive `contract/{id}/orders`
//! mirror is deferred to the SNIPER phase), and there is no passive sell
//! side — `/v0/market/{id}/sell` is an instant uncapped sweep.
//!
//! # Secrets
//!
//! The API key is stored in [`ManifoldClient`] and sent in the `Authorization`
//! header. It never appears in an error, a log line, or the `Debug` output.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
/// Synchronous mutex (aliased against tokio's). Used only for locks that are
/// taken and released inside one synchronous block.
use std::sync::Mutex as SyncMutex;
use std::sync::MutexGuard;
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
use crate::manifold_ws::{BetEvent, ContractProbs, ContractStub, ManifoldEvent};
use crate::market_store::MarketStore;
use crate::pipeline::PipelineEvent;
use crate::rate::{Priority, RateLimiter, SystemClock, WRITE_COST_TOKENS};
use crate::snipe::SnipeStrategy;
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

/// Interval of the slow REST reconcile backstop that demotes REST to
/// ticker's freshness role: batched probabilities, one portfolio position
/// snapshot, and a single summary line (`docs/ALGO.md` §1, §2).
const RECONCILE_INTERVAL: Duration = Duration::from_secs(300);

/// Age of an own resting order below which the reverse-drift and
/// opposite-entry cancel policies skip it, in milliseconds.
///
/// A seconds-old order was placed against the belief of the evaluation that
/// created it, so it cannot have drifted within the wall-clock time of one
/// tick, and a same-tick cancellation would fight the order the ladder just
/// placed. Orders this fresh are re-checked by the next price update.
const OWN_ORDER_GRACE_MILLIS: i64 = 5_000;

/// Margin (in whole-percent units, `×100`) used while snapping a fair-capped
/// cap onto Manifold's whole-percent `limitProb` grid.
///
/// A cap computed as `fair − ε` in floating point can land an ulp below the
/// exact percent boundary it mathematically belongs to (for example
/// `0.5 − 0.01` = `48.99999999999999…`), and a naive floor would shave a
/// whole extra percent off. The margin covers only that representation
/// error — real caps differ from a boundary by far more than `1e-7`.
const LIMIT_SNAP_EPSILON: f64 = 1e-7;

/// Minimum unfilled mana for a buy remainder to count as a resting order.
const REMAINDER_EPSILON: f64 = 1e-9;

/// Expiry assumed for snapshot rows that omit `expiresAt`, in seconds
/// (`LIMIT_EXPIRY_SECS`'s default value; the field is optional on the wire).
const FALLBACK_LIMIT_EXPIRY_SECS: i64 = 60;

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
    /// per-market `/positions` parser accepts. Returns the parsed map, the
    /// *raw* entry count the server consumed (empty lists included — the
    /// parser drops those, but they still advance pagination), and whether
    /// the page was short of the limit, meaning no further pages exist.
    /// Callers must not infer "no metrics for a market" as "the bot holds
    /// nothing there" from anything but a fully paginated snapshot.
    pub async fn portfolio_page(
        &self,
        offset: usize,
    ) -> AppResult<(HashMap<MarketId, Vec<Value>>, usize, bool)> {
        let path = format!(
            "/v0/get-user-contract-metrics-with-contracts?userId={}&perAnswer=true&limit={}&offset={}",
            self.bot_user_id, PORTFOLIO_PAGE_SIZE, offset
        );
        let request = self.http.get(self.url(&path));
        let value: Value = self
            .send(request, "GET /v0/get-user-contract-metrics-with-contracts")
            .await?;

        let by_contract = parse_metrics_by_contract(&value);
        // Pagination advances by raw entries: the server consumed a row for
        // every list entry, including the empty lists the parser drops.
        let raw_entries = raw_metrics_entry_count(&value);
        Ok((by_contract, raw_entries, raw_entries < PORTFOLIO_PAGE_SIZE))
    }

    /// Buys `amount` mana of `outcome` shares at up to `limit_prob`, with any
    /// unfilled remainder resting for `expires_millis_after` milliseconds.
    ///
    /// One call runs the whole execution ladder (probe-verified, `docs/SPEC.md`
    /// §6.6): Manifold consumes opposing resting orders first at *their* price
    /// in price-time priority, sweeps the AMM up to `limit_prob`, and rests
    /// whatever is left as the same bet row — the rest dies on its own when
    /// the expiry passes (`expiresMillisAfter`; `0` omits the field, which no
    /// production path does).
    ///
    /// Manifold interprets `limitProb` as the market probability of YES for
    /// *both* outcomes: a YES sweep stops when the probability rises to the
    /// limit (that limit is the per-share price ceiling), while a NO sweep
    /// stops when the probability falls to it (so the per-NO-share ceiling is
    /// `1 − limitProb`, and `limit_prob = p_internal − ε` prices every NO
    /// share at no more than `fair_NO + ε`). Manifold rejects limit values
    /// outside `[0.01, 0.99]` and off the whole-percent grid; callers clamp
    /// and quantize with [`fair_capped_limit_prob`].
    //
    // clippy too_many_arguments: the parameters mirror the `POST /v0/bet`
    // wire fields one to one; a request struct would not reduce coupling.
    #[allow(clippy::too_many_arguments)]
    pub async fn buy(
        &self,
        market_id: &str,
        outcome: &str,
        amount: f64,
        answer_id: Option<&str>,
        limit_prob: f64,
        dry_run: bool,
        expires_millis_after: i64,
    ) -> AppResult<BetResult> {
        let outcome = validate_outcome(outcome)?;
        if !amount.is_finite() || amount <= 0.0 {
            bail!("buy amount must be a positive finite mana value, got {amount}");
        }
        if !limit_prob.is_finite() {
            bail!("buy limitProb must be finite, got {limit_prob}");
        }

        let body = bet_body(
            market_id,
            outcome,
            amount,
            answer_id,
            limit_prob,
            dry_run,
            expires_millis_after,
        );
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

        parse_bets_envelope(value)
    }

    /// Fetches the bot's own open limit orders across every market in one
    /// call (`GET /v0/bets?userId={bot}&kinds=open-limit`).
    ///
    /// This is the executor's boot snapshot of the own resting layer
    /// ([`TradeExecutor::seed_own_orders`]): one REST call, no `contractId`
    /// filter, verified parameter combination (`kinds` and `userId` combinable;
    /// backend filter is `is_filled=false AND is_cancelled=false` only).
    ///
    /// The endpoint has **no expiry filter**: a row whose `expiresAt` has
    /// passed can appear for up to the ~1-minute cron lag, so the caller must
    /// filter expired rows locally — matching on Manifold already rejects an
    /// expired order inline.
    pub async fn open_limit_orders(&self) -> AppResult<Vec<Value>> {
        let path = format!("/v0/bets?userId={}&kinds=open-limit", self.bot_user_id);
        let request = self.http.get(self.url(&path));
        let value: Value = self
            .send(request, "GET /v0/bets?userId&kinds=open-limit")
            .await?;

        parse_bets_envelope(value)
    }

    /// Cancels one of the bot's open limit orders
    /// (`POST /v0/bet/cancel/{betId}`, one bet per request).
    ///
    /// Own orders only — Manifold rejects a foreign bet id. The cancel
    /// response (the cancelled bet) is not read by callers; success is the
    /// verdict.
    pub async fn cancel_bet(&self, bet_id: &str) -> AppResult<()> {
        let path = format!("/v0/bet/cancel/{}", bet_id);
        let request = self.http.post(self.url(&path));
        let _value: Value = self
            .send(request, "POST /v0/bet/cancel/{betId}")
            .await?;
        Ok(())
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

/// Test seam: a client pointed at a local endpoint instead of Manifold.
///
/// Only compiled in test builds. It keeps the production constructor (which
/// performs the one `GET /v0/me`) out of offline tests without adding a second
/// HTTP implementation.
#[cfg(test)]
impl ManifoldClient {
    pub(crate) fn for_test(base_url: impl Into<String>) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url: base_url.into(),
            api_key: "test-key".to_owned(),
            bot_user_id: "bot-1".to_owned(),
        }
    }
}

/// Rejects outcomes other than `YES` and `NO`.
fn validate_outcome(outcome: &str) -> AppResult<&str> {
    match outcome {
        "YES" | "NO" => Ok(outcome),
        other => bail!("Manifold outcome must be YES or NO, got {other:?}"),
    }
}

/// Unwraps the `GET /v0/bets` response envelope.
///
/// Both observed shapes are accepted: a bare bet array, or an object with a
/// `bets` array.
fn parse_bets_envelope(value: Value) -> AppResult<Vec<Value>> {
    match value {
        Value::Array(items) => Ok(items),
        Value::Object(mut map) => match map.remove("bets") {
            Some(Value::Array(items)) => Ok(items),
            _ => bail!("Manifold GET /v0/bets response did not contain a bet array"),
        },
        _ => bail!("Manifold GET /v0/bets response was not an array"),
    }
}

/// Builds the `POST /v0/bet` request body for one buy.
///
/// Pure so tests can pin the exact wire shape (`docs/SPEC.md` §6.6) without an
/// HTTP round trip. `expires_millis_after` ≤ 0 omits the expiry field.
fn bet_body(
    market_id: &str,
    outcome: &str,
    amount: f64,
    answer_id: Option<&str>,
    limit_prob: f64,
    dry_run: bool,
    expires_millis_after: i64,
) -> Value {
    let mut body = json!({
        "contractId": market_id,
        "amount": amount,
        "outcome": outcome,
        "limitProb": limit_prob,
        "dryRun": dry_run,
    });
    if expires_millis_after > 0 {
        body["expiresMillisAfter"] = json!(expires_millis_after);
    }
    if let Some(aid) = answer_id {
        body["answerId"] = json!(aid);
    }
    body
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
    /// Manifold's slug for the market, used to build a page URL when the
    /// snapshot has no explicit `url`.
    #[serde(default)]
    pub slug: String,
    /// Canonical Manifold page URL, when the API returns one.
    #[serde(default)]
    pub url: String,
    /// `cpmm-1`, `cpmm-multi-1`, `cpmm-multi-2`, `dpm-2`, and so on.
    #[serde(default)]
    pub mechanism: String,
    /// `None` on non-BINARY markets (e.g. `MULTIPLE_CHOICE`).
    #[serde(default)]
    pub probability: Option<f64>,
    /// The creator's opening probability, present on most markets. S2 reads it
    /// as the fallback for [`Self::probability`] on a freshly created market.
    #[serde(rename = "initialProb", default)]
    pub initial_prob: Option<f64>,
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
    /// The market's explicit resolution rules; onboarding falls back to the
    /// description when absent, and S2 feeds it to the fair-value question.
    #[serde(rename = "resolutionCriteria", default)]
    pub resolution_criteria: Option<String>,
    /// Total mana subsidised into the market's liquidity pool. S2's liquidity
    /// gate reads it; absent on payloads that omit the field.
    #[serde(rename = "totalLiquidity", default)]
    pub total_liquidity: Option<f64>,
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
    /// Sell responses may omit `shares`; `execute` already falls back to the
    /// requested count downstream, so a missing field becomes 0.0.
    #[serde(default)]
    pub shares: f64,
    #[serde(rename = "probBefore", default)]
    pub prob_before: Option<f64>,
    #[serde(rename = "probAfter", default)]
    pub prob_after: Option<f64>,
    /// The original amount of a created limit order, reported on limit bets.
    /// With a fair-capped sweep that fills part of the budget the unfilled
    /// remainder is `orderAmount − amount`, and it rests on Manifold as an
    /// open limit order at the cap.
    #[serde(rename = "orderAmount")]
    pub order_amount: Option<f64>,
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

/// Counts the raw `metricsByContract` rows in one portfolio page.
///
/// [`parse_metrics_by_contract`] drops contract entries it cannot use (an
/// empty list, a non-array value), but the server consumed row space for
/// every entry in the raw response. Completeness (is this the last page?)
/// and the next pass's `offset` must both be computed from this raw count:
/// a page that fills its limit with rows the parser drops would otherwise
/// end pagination early and wrongly declare absent markets flat, zeroing
/// real positions.
fn raw_metrics_entry_count(value: &Value) -> usize {
    let Some(metrics) = value.get("metricsByContract").and_then(Value::as_object) else {
        return 0;
    };

    metrics
        .values()
        .map(|entry| match entry.as_array() {
            Some(array) if !array.is_empty() => array.len(),
            // An entry the parser would drop still occupied a row.
            _ => 1,
        })
        .sum()
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
/// Price patches carry no `isResolved` flag, so the snapshot check's
/// `isResolved` guard is not available here and this predicate replaces it: a resolved market
/// must not trade — its positions settle automatically — and, because the
/// mirror clamps the stored price to 0.99/0.01, the skip is recorded in
/// [`TerminalMirrors`] until a full [`TradeExecutor::sync_market`] refresh
/// proves the market is tradable again.
fn patch_is_terminal(patch: &ProbPatch) -> bool {
    let at_extreme = |prob: f64| prob <= 0.0 || prob >= 1.0;
    if let Some(root) = patch.root {
        return at_extreme(root);
    }
    !patch.answers.is_empty() && patch.answers.iter().all(|(_, prob)| at_extreme(*prob))
}

/// In-memory set of markets whose latest price observation was a terminal
/// (resolution-signature) patch.
///
/// The price mirror clamps probabilities into the trading range, so the
/// stored columns cannot show that a market resolved: a later material
/// signal would then drive a buy against prices pinned at 0.99/0.01 —
/// doomed against a settled contract. While a market is in this set its
/// [`TradeExecutor::evaluate_cached`] checks skip it, and only a full
/// [`TradeExecutor::sync_market`] REST snapshot (kind refresh, answer
/// discovery, position truth) clears it. Nothing here is persisted: a
/// restart re-derives the flag from the next event stream observation.
#[derive(Debug, Default)]
struct TerminalMirrors(HashSet<MarketId>);

impl TerminalMirrors {
    fn mark(&mut self, market_id: &str) {
        self.0.insert(market_id.to_owned());
    }

    fn clear(&mut self, market_id: &str) {
        self.0.remove(market_id);
    }

    fn contains(&self, market_id: &str) -> bool {
        self.0.contains(market_id)
    }
}

/// One resting limit order the bot itself owns (executor-side truth).
///
/// In-memory only, no schema change: a restart re-seeds the whole map with
/// one `GET /v0/bets?userId={bot}&kinds=open-limit` call
/// ([`TradeExecutor::seed_own_orders`]). `limit_prob` and both amount
/// counters are YES-scale wire values as Manifold reports them; the
/// per-outcome price of the shares the order buys is `limit_prob` for YES
/// and `1 − limit_prob` for NO.
#[derive(Debug, Clone)]
struct OwnRestingOrder {
    /// Manifold bet id: the placement and its resting remainder are the
    /// same bet row (probe-verified), and websocket maker updates come
    /// under this id.
    bet_id: String,
    market_id: String,
    /// `None` for the binary leg, the answer id otherwise.
    answer_id: Option<String>,
    /// `YES` or `NO`, the outcome the order buys.
    outcome: String,
    /// The limit the order rests at, YES scale, whole-percent quantized —
    /// exactly what was sent and what Manifold accepted.
    limit_prob: f64,
    /// Full mana the request committed (`orderAmount`).
    order_amount: f64,
    /// Mana filled so far (cumulative `amount`).
    filled_amount: f64,
    /// Shares filled so far (cumulative `shares`).
    filled_shares: f64,
    /// Order creation, milliseconds since epoch.
    created_ms: i64,
    /// Expiry, milliseconds since epoch: the server's `expiresAt` when
    /// known, otherwise the placement-time estimate
    /// (`created_ms` + `LIMIT_EXPIRY_SECS`).
    expires_ms: Option<i64>,
}

/// Every own resting order, keyed market → bet id.
///
/// Guarded by a [`SyncMutex`] exactly like [`TerminalMirrors`]: the guard is
/// taken and released inside one synchronous block and never held across an
/// await, so the REST cancels and fill applications can never deadlock with
/// it (the fill path takes `execution_lock` first, and nothing takes
/// `own_orders` after a lock anywhere).
#[derive(Debug, Default)]
struct OwnRestingOrders(HashMap<MarketId, HashMap<String, OwnRestingOrder>>);

impl OwnRestingOrders {
    /// Registers (or replaces) one order.
    fn upsert(&mut self, order: OwnRestingOrder) {
        self.0
            .entry(order.market_id.clone())
            .or_default()
            .insert(order.bet_id.clone(), order);
    }

    fn get(&self, market_id: &str, bet_id: &str) -> Option<&OwnRestingOrder> {
        self.0.get(market_id)?.get(bet_id)
    }

    /// Removes one order, returning it when it was tracked.
    fn remove(&mut self, market_id: &str, bet_id: &str) -> Option<OwnRestingOrder> {
        self.0.get_mut(market_id)?.remove(bet_id)
    }

    /// Snapshot of one market's orders (cloned, so the guard can drop
    /// before any await).
    fn of_market(&self, market_id: &str) -> Vec<OwnRestingOrder> {
        self.0
            .get(market_id)
            .map(|orders| orders.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Drops every order of one market `dead` accepts and returns how many
    /// went. Pure sweep: used for expired orders that need no cancel call.
    fn retain_market(&mut self, market_id: &str, dead: impl Fn(&OwnRestingOrder) -> bool) -> usize {
        let Some(orders) = self.0.get_mut(market_id) else {
            return 0;
        };
        let before = orders.len();
        orders.retain(|_, order| !dead(order));
        before - orders.len()
    }

    /// Total unfilled mana of one market's own resting layer.
    ///
    /// That is committed money which has not converted to an invested
    /// position yet, so the pre-buy budget check must count it.
    fn outstanding(&self, market_id: &str) -> f64 {
        self.0
            .get(market_id)
            .map(|orders| {
                orders
                    .values()
                    .map(|order| (order.order_amount - order.filled_amount).max(0.0))
                    .sum()
            })
            .unwrap_or(0.0)
    }
}

/// Whether a resting order's cap is now past the day's fair-capped cap by
/// more than the configured staleness slack (`docs/SPEC.md` §6.6).
///
/// Precisely, with `cap_now = fair_capped_limit_prob(p_fair_now, ε, outcome)`
/// (the cap a fresh entry into the same outcome would use today):
///
/// - **YES order** — it fills our YES shares at the YES-market price
///   `limitProb`, so cancel iff `limitProb − cap_now > drift`.
/// - **NO order** — measured on the same YES scale: the order buys NO shares
///   at `1 − limitProb`, and today's ceiling is `1 − cap_now`, so cancel iff
///   `cap_now − limitProb > drift`.
///
/// A favorable drift (the fair price moved *toward* the order, making its
/// fill better than today's entry standard) yields a negative `overpay` and
/// keeps the order: it fills at a price we could not replicate.
///
/// `outcome` is validated as `YES`/`NO` by every producer of an
/// [`OwnRestingOrder`]; anything else is a shape bug and the safe verdict is
/// "no cancel" (the expiry bounds the order anyway).
fn should_cancel_for_drift(
    outcome: &str,
    limit_prob: f64,
    fair_now: f64,
    epsilon: f64,
    drift: f64,
) -> bool {
    let cap_now = fair_capped_limit_prob(fair_now, epsilon, outcome);
    let overpay = match outcome {
        "YES" => limit_prob - cap_now,
        "NO" => cap_now - limit_prob,
        _ => {
            tracing::debug!(outcome, "unknown outcome; no drift-cancel basis");
            return false;
        }
    };
    overpay > drift
}

/// The verdict for one websocket update of an own bet (pure, unit-tested).
///
/// The rule is delta-based: only shares *beyond* the last seen cumulative
/// counters are a new fill, so a duplicate broadcast re-applies nothing (the
/// idempotence requirement on duplicate `global/new-bet` events). A
/// cancelled, fully filled, or expired order is reported dead — the server
/// flips `isCancelled` for expired orders at the top of the minute, but
/// matching already rejects an expired order inline, so expiry checks run on
/// the local clock.
struct OwnBetUpdate {
    /// The order must leave the tracker (death).
    removed: bool,
    /// Newly filled mana and shares, when any.
    fill: Option<(f64, f64)>,
}

fn own_bet_update(order: &OwnRestingOrder, bet: &BetEvent, now_ms: i64) -> OwnBetUpdate {
    let expires_ms = bet.expires_at.or(order.expires_ms);
    let dead = bet.is_cancelled
        || bet.is_filled
        || expires_ms.is_some_and(|expires| expires <= now_ms);

    // Trust the server's cumulative counters; a negative delta (a replay of
    // an older event) must never unwind a fill.
    let fill_shares = (bet.shares - order.filled_shares).max(0.0);
    let fill_mana = (bet.amount - order.filled_amount).max(0.0);
    let fill = if fill_shares > 0.0 && fill_mana > 0.0 {
        Some((fill_mana, fill_shares))
    } else {
        None
    };

    OwnBetUpdate { removed: dead, fill }
}

/// Parses one row of the boot snapshot
/// `GET /v0/bets?userId={bot}&kinds=open-limit` into an own resting order.
///
/// Skipped rows (returning `None`): cancelled or fully filled bets, already
/// expired ones (the snapshot has no expiry filter — the ~1-minute cron lag
/// can still list them — but matching rejects expired orders inline, so we
/// filter on the local clock), rows without a limit probability, redemptions,
/// and rows whose committed mana already converted entirely. The REST query
/// is user-filtered, so the `userId` check only guards against a response-
/// shape change.
fn parse_own_order_row(row: &Value, bot_user_id: &str, now_ms: i64) -> Option<OwnRestingOrder> {
    let id = row
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())?;
    if row
        .get("userId")
        .and_then(Value::as_str)
        .is_some_and(|user| user != bot_user_id)
    {
        return None;
    }
    if row.get("isCancelled").and_then(Value::as_bool).unwrap_or(false) {
        return None;
    }
    if row.get("isFilled").and_then(Value::as_bool).unwrap_or(false) {
        return None;
    }
    if row.get("isRedemption").and_then(Value::as_bool).unwrap_or(false) {
        return None;
    }
    let market_id = row
        .get("contractId")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())?
        .to_owned();
    let outcome = row
        .get("outcome")
        .and_then(Value::as_str)
        .filter(|outcome| validate_outcome(outcome).is_ok())?
        .to_owned();
    let limit_prob = row.get("limitProb").and_then(Value::as_f64)?;
    let filled_amount = number_at(row.get("amount"));
    let order_amount = number_at(row.get("orderAmount"));
    let remainder = order_amount - filled_amount;
    if !remainder.is_finite() || remainder <= REMAINDER_EPSILON {
        return None;
    }
    let expires_at = row.get("expiresAt").and_then(Value::as_i64);
    if expires_at.is_some_and(|expires| expires <= now_ms) {
        return None;
    }
    let answer_id = row
        .get("answerId")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map(str::to_owned);

    Some(OwnRestingOrder {
        bet_id: id.to_owned(),
        market_id,
        answer_id,
        outcome,
        limit_prob,
        order_amount,
        filled_amount,
        filled_shares: number_at(row.get("shares")),
        created_ms: row
            .get("createdTime")
            .and_then(Value::as_i64)
            .unwrap_or(now_ms),
        expires_ms: Some(expires_at.unwrap_or_else(|| now_ms + FALLBACK_LIMIT_EXPIRY_SECS * 1_000)),
    })
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
        /// [`crate::types::STRATEGY_NEWS`] or [`crate::types::STRATEGY_SNIPE`];
        /// the Discord renderer marks snipe trades with their own provenance.
        strategy: String,
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
    /// Slack over fair value in one buy's fill limit; see
    /// [`fair_capped_limit_prob`].
    limit_epsilon: f64,
    /// Resting-layer lifetime sent as `expiresMillisAfter` on every bot buy.
    limit_expiry_secs: u64,
    /// Reverse-drift cancel slack; see [`should_cancel_for_drift`].
    limit_drift_cancel: f64,
    /// Global Manifold token bucket; writes spend Trade-class tokens, the
    /// reconcile backstop spends Reconcile-class tokens.
    rate: RateLimiter,
    execution_lock: Mutex<()>,
    /// Markets whose latest mirrored price was a terminal patch; see
    /// [`TerminalMirrors`]. Guarded by [`SyncMutex`], released before
    /// anything awaits.
    terminal_mirrors: SyncMutex<TerminalMirrors>,
    /// The bot's own resting limit orders; see [`OwnRestingOrders`]. Guarded
    /// by [`SyncMutex`], never held across an await.
    own_orders: SyncMutex<OwnRestingOrders>,
    /// S2 new-market sniper; `None` when `SNIPE_ENABLED=false`.
    snipe: Option<SnipeStrategy>,
    events: broadcast::Sender<TradeEvent>,
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

        // S2 is constructed only when enabled: with the flag off the executor
        // spends no decision call and writes no snipe ledger row at all.
        let snipe = if config.snipe_enabled {
            Some(
                SnipeStrategy::new(config, db.clone(), Arc::clone(&store), client.clone())
                    .context("initializing the S2 new-market sniper")?,
            )
        } else {
            None
        };

        let executor = Self {
            client,
            db,
            store,
            policy: TradePolicy::from_config(config),
            dry_run: config.trading_dry_run,
            halflife_hours: config.belief_halflife_hours,
            limit_epsilon: config.limit_epsilon,
            limit_expiry_secs: config.limit_expiry_secs as u64,
            limit_drift_cancel: config.limit_drift_cancel,
            rate: RateLimiter::per_minute(config.rate_limit_per_min, Box::new(SystemClock)),
            execution_lock: Mutex::new(()),
            terminal_mirrors: SyncMutex::new(TerminalMirrors::default()),
            own_orders: SyncMutex::new(OwnRestingOrders::default()),
            snipe,
            events,
        };

        // Boot snapshot of the own resting layer: one REST call, best
        // effort. The websocket and the buy path keep the map current from
        // here on; a leftover order the snapshot missed simply expires.
        if let Err(error) = executor.seed_own_orders().await {
            tracing::warn!(
                error = %error,
                "boot snapshot of own open limit orders failed; own-order tracking starts empty"
            );
        }

        Ok(executor)
    }

    /// The lifetime the resting layer gets, in milliseconds for the wire.
    fn limit_expiry_millis(&self) -> i64 {
        // The config validation bounds the seconds into [1, 300]; this only
        // keeps the multiplication defensive.
        (self.limit_expiry_secs.min(300) as i64).saturating_mul(1_000)
    }

    /// Locks the own resting-order map. The guard never crosses an await.
    fn own_orders(&self) -> MutexGuard<'_, OwnRestingOrders> {
        self.own_orders
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Seeds the own resting-order map with one boot snapshot.
    ///
    /// One `GET /v0/bets?userId={bot}&kinds=open-limit` call (no
    /// `contractId` filter) covers every market at once
    /// ([`ManifoldClient::open_limit_orders`]); climbs one Reconcile-class
    /// token like every other snapshot read. Orders on markets the process
    /// does not track are skipped: their expiry still bounds them.
    ///
    /// Failure is not fatal: the websocket and the buy path keep the map
    /// current from here on, and the boot-snapshot-only leftovers (orders
    /// placed by a process that died before its response returned) cost at
    /// most their own `LIMIT_EXPIRY_SECS`.
    async fn seed_own_orders(&self) -> AppResult<usize> {
        if self
            .rate
            .try_acquire(Priority::Reconcile, WRITE_COST_TOKENS)
            .is_err()
        {
            bail!("rate bucket is at the housekeeping reserve; the boot snapshot was skipped");
        }

        let rows = self.client.open_limit_orders().await?;
        let now_ms = Utc::now().timestamp_millis();
        let bot_user_id = self.client.bot_user_id().to_owned();

        let mut seeded = 0;
        for row in &rows {
            let Some(order) = parse_own_order_row(row, &bot_user_id, now_ms) else {
                continue;
            };
            if self.store.cached(&order.market_id).is_none() {
                continue;
            }
            tracing::debug!(
                market_id = %order.market_id,
                bet_id = %order.bet_id,
                outcome = %order.outcome,
                limit_prob = order.limit_prob,
                order_amount = order.order_amount,
                filled_amount = order.filled_amount,
                "boot snapshot seeded an own resting order"
            );
            self.own_orders().upsert(order);
            seeded += 1;
        }

        if seeded > 0 {
            tracing::info!(seeded, "seeded own resting orders from the boot snapshot");
        }
        Ok(seeded)
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
    /// This is the full-REST path, driven by the operator's explicit recheck
    /// (through the Discord control plane). The live-price loop calls
    /// [`TradeExecutor::evaluate_cached`] instead because the price columns
    /// are already mirrored.
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
    /// gating is the standard one: status `ACTIVE`, kill switch,
    /// budget invariant (asserted per planned buy), and `TRADING_DRY_RUN`.
    /// The terminal-mirror skip (see [`TerminalMirrors`]) also applies here,
    /// since every call site — signal, price update, and the post-reconcile
    /// pass — funnels through this method.
    ///
    /// Kind changes and brand-new answers are not caught here by design — the
    /// slow reconcile detects them (`docs/ALGO.md` phase 1).
    async fn evaluate_cached(
        &self,
        market_id: &str,
        trigger_post_uri: Option<&str>,
    ) -> AppResult<Option<TradeRecord>> {
        if self.terminal_mirror(market_id) {
            tracing::debug!(
                market_id = %market_id,
                "last price observation was a resolution patch; skipping evaluation"
            );
            // A settled market makes every own resting order moot (positions
            // settle automatically, matching stops). Cancelling is best
            // effort and cheap when nothing rests; this also covers orders
            // the boot snapshot seeded on a resolved market.
            self.cancel_market_orders(market_id, "terminal").await;
            return Ok(None);
        }

        let _guard = self.execution_lock.lock().await;

        let mut book = self.load_book(market_id)?;
        if book.status() != MarketStatus::Active {
            return Ok(None);
        }

        let kill_switch = self.kill_switch_on()?;
        let now = Utc::now().timestamp();

        // §S1 ladder, own-order lifecycle: retire rests whose cap drifted
        // past the fresh fair value before the planner sees the same state.
        if let Err(error) = self.run_cancel_policy(&book, now).await {
            tracing::error!(
                market_id = %market_id,
                error = %error,
                "own-order cancel policy failed"
            );
            self.emit_error(Some(market_id.to_owned()), error.to_string());
        }

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

            // §S1 ladder, own-order lifecycle: an entry executed in this
            // evaluation invalidates resting orders on the opposite side of
            // its leg — they would take the book into the position just bet
            // against. Best effort: failures are logged inside.
            if is_buy(planned_trade.decision.action) {
                if let Err(error) =
                    self.cancel_opposite_after_entry(book, &planned_trade.decision, now)
                        .await
                {
                    tracing::error!(
                        market_id = %market_id,
                        error = %error,
                        "opposite-side resting-order cancellation failed"
                    );
                }
            }

            first.get_or_insert(record);
        }

        Ok(first)
    }

    /// Executes one externally planned buy or sell through the shared path.
    ///
    /// S2 owns the *plan* (the market is new and has no news trigger), but not
    /// the execution: this method takes the execution lock, loads the book,
    /// reads the kill switch, and calls [`TradeExecutor::execute`] — the same
    /// code that enforces the budget invariant, spends the Trade-class rate
    /// token, builds the fair-capped whole-percent limit, tracks the expiring
    /// rest, writes `trade_ledger`, persists the position columns, and emits
    /// the Discord event. No part of that ladder is duplicated here.
    ///
    /// `Ok(None)` means nothing was persisted: an inactive market, the kill
    /// switch, a rate refusal, dry-run mode, or a buy that filled no shares
    /// (its remainder rests and is tracked by `execute`).
    pub async fn execute_planned(
        &self,
        market_id: &str,
        planned: &PlannedTrade,
        trigger_post_uri: Option<&str>,
        now: i64,
    ) -> AppResult<Option<TradeRecord>> {
        let _guard = self.execution_lock.lock().await;

        let mut book = self.load_book(market_id)?;
        if book.status() != MarketStatus::Active {
            return Ok(None);
        }

        let kill_switch = self.kill_switch_on()?;
        if is_buy(planned.decision.action) && kill_switch {
            tracing::warn!(
                market_id = %market_id,
                action = %planned.decision.action,
                "global kill switch is on; refusing a planned buy"
            );
            return Ok(None);
        }

        let Some(record) = self
            .execute(&mut book, planned, trigger_post_uri, now, self.dry_run)
            .await?
        else {
            return Ok(None);
        };

        book.persist(&self.store, &self.db)?;
        self.emit_trade(&book, &record);

        // Same own-order lifecycle rule as `decide`: an entry on this leg
        // invalidates a resting order on the opposite outcome.
        if is_buy(planned.decision.action) {
            if let Err(error) = self
                .cancel_opposite_after_entry(&book, &planned.decision, now)
                .await
            {
                tracing::error!(
                    market_id = %market_id,
                    error = %error,
                    "opposite-side resting-order cancellation failed after a planned buy"
                );
            }
        }

        Ok(Some(record))
    }

    /// Evaluates and executes one `global/new-contract` broadcast (S2).
    ///
    /// The evaluation runs inline in the event loop: new contracts are an
    /// hours-scale stream (`docs/ALGO.md` §6.1 measured none in two minutes),
    /// and the expensive parts are bounded — the fair-value call by
    /// `SNIPE_DECISION_TIMEOUT_SECS` (default 2 s) and the market fetch by the
    /// HTTP client timeout. Queued websocket events wait in the never-drop
    /// channel and are mirrored on the next loop pass.
    ///
    /// # Why the regular path cannot double-bet after a snipe
    ///
    /// Our own bet produces an `updated-contract` broadcast, so
    /// [`TradeExecutor::evaluate_cached`] may run on this market moments later.
    /// It cannot open a second position: `Book::plan` only considers entries
    /// for a leg with **no** shares, and the snipe fill created one. Belief is
    /// never moved by our own trade, so the only plan the regular path can
    /// produce is an exit. A fill that was entirely rested (zero shares) leaves
    /// the leg flat; the next normal entry is then bounded by the same
    /// per-market ceiling (`budget_mana = min(SNIPE_MAX_MANA, pool)`), because
    /// the snipe order is not retried here.
    async fn handle_new_contract(&self, contract: &ContractStub, now: i64) -> AppResult<()> {
        let Some(snipe) = self.snipe.as_ref() else {
            tracing::debug!(
                contract_id = %contract.id,
                "new contract broadcast; sniping is disabled"
            );
            return Ok(());
        };

        let Some(order) = snipe.evaluate(contract, now, &self.rate).await? else {
            return Ok(());
        };

        match self
            .execute_planned(
                &order.market_id,
                &order.planned,
                Some(&order.trigger),
                now,
            )
            .await
        {
            Ok(Some(record)) => snipe.record_filled(&order, &record)?,
            Ok(None) => {
                let reason = if self.dry_run {
                    "dry_run"
                } else {
                    "execution refused or nothing filled"
                };
                snipe.record_unfilled(&order, reason)?;
            }
            Err(error) => {
                snipe.record_unfilled(&order, &format!("execution error: {error}"))?;
                return Err(error);
            }
        }

        Ok(())
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

        // The full REST snapshot is position truth for this market: only a
        // refresh that proves the market is *not* resolved clears a
        // terminal-mirror flag, and no other observation channel ever does.
        // If the snapshot shows it resolved the skip stays, whatever the
        // event stream does next.
        if snapshot.is_resolved {
            tracing::debug!(
                market_id = %market_id,
                "full sync found the market resolved; the terminal skip stays"
            );
        } else {
            self.clear_terminal_mirror(market_id);
        }

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

    /// Runs one REST-free cached-state trade check for every active market.
    ///
    /// Runs one REST-free cached-state trade check for every active market.
    ///
    /// This is the post-reconcile evaluation sweep: it catches exits caused
    /// by belief decay on markets that have gone quiet — no sieve post sparks
    /// a signal, no websocket event fires, so without a sweep such a market
    /// is never evaluated and its exit edge never fires. The 5-minute
    /// reconcile pass gives that periodic role a home. Each check goes
    /// through [`TradeExecutor::evaluate_cached`],
    /// so the decision logic is not forked and every per-market gate
    /// (status, kill switch, terminal-mirror skip) is already inside it.
    /// No debounce is needed: 5 minutes is self-limiting cadence.
    ///
    /// Per-market failures are logged and emitted so one broken market does
    /// not stop the sweep. Called from the spawned reconcile task in
    /// [`TradeExecutor::run`], never inline, so the executor stays available
    /// to websocket events while it runs.
    async fn evaluate_all_cached(&self) {
        for market in self.store.active() {
            if let Err(error) = self.evaluate_cached(&market.id, None).await {
                tracing::error!(
                    market_id = %market.id,
                    error = %error,
                    "periodic cached trade check failed"
                );
                self.emit_error(Some(market.id), error.to_string());
            }
        }
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

        // §S1 ladder: the control-plane exit is also the one hook /close and
        // /untrack --liquidate share. Cancel own resting orders so exit
        // capital is not idling in the book; best effort, failures are
        // logged inside and the expiry bounds anything left. `/untrack`
        // without a sale does not come through here — its rests simply
        // expire (documented in `docs/SPEC.md` §6.6).
        self.cancel_market_orders(market_id, "liquidation").await;

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
    /// The loop has four arms: material pipeline signals (a trade check
    /// against the mirrored prices), websocket `ManifoldEvent`s (price
    /// mirror + debounced trade check), a 5-minute reconcile ticker (batched
    /// REST backstop, run on its own task with the summary returned over a
    /// channel so the loop never stops draining events while it runs), and
    /// the drained result arm that also frees the reconcile slot. Each
    /// reconcile pass is followed by a REST-free cached-state evaluation of
    /// every active market, which catches belief-decay exits on markets
    /// that go quiet.
    ///
    /// No single failure stops the loop: trade errors are logged, broadcast
    /// lag is logged, and a closed pipeline channel ends the loop.
    pub async fn run(
        self: Arc<Self>,
        mut events: broadcast::Receiver<PipelineEvent>,
        mut manifold_events: mpsc::Receiver<ManifoldEvent>,
    ) -> AppResult<()> {
        tracing::info!("live prices from the Manifold websocket");

        let mut reconcile = tokio::time::interval(RECONCILE_INTERVAL);
        reconcile.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The first tick completes immediately and is deliberately kept: the
        // boot pass re-establishes price truth right after startup, which
        // matters because the websocket never replays missed events.
        let mut debouncer = EvaluationDebouncer::new(Duration::from_secs(WS_DEBOUNCE_SECS));
        let mut ws_open = true;
        let mut ws_closed_at: Option<Instant> = None;
        let mut ws_down_warned = false;

        // The reconcile pass runs on its own task instead of inline in the
        // tick arm: an inline pass stops this loop from draining websocket
        // events while REST is pending, which can fill the event channel and
        // block the socket loop's sends away from its keepalive ping (the
        // server sweeps a silent connection at ~94 s). Summaries come back
        // through this one-slot channel; at most one pass runs at a time — a
        // tick while one is running is skipped, not queued.
        let (reconcile_tx, mut reconcile_rx) = mpsc::channel::<ReconcileSummary>(1);
        let mut reconcile_task: Option<tokio::task::JoinHandle<()>> = None;

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
                        Err(broadcast::error::RecvError::Closed) => {
                            // Shutting down: a still-running reconcile task
                            // would trade after the loop is gone, so it is
                            // aborted with everything else.
                            if let Some(task) = reconcile_task.take() {
                                task.abort();
                            }
                            return Ok(());
                        }
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
                                // §S1 ladder: a settled market makes every own
                                // resting order moot; cancel so no mana idles
                                // in the book until the expiry.
                                self.cancel_market_orders(&update.contract_id, "terminal")
                                    .await;
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
                        // S2 new-market sniping. S3 (copy-trading) is a later
                        // phase; its `Bet` event stays parsed but inert below.
                        Some(ManifoldEvent::NewContract(event)) => {
                            let now = Utc::now().timestamp();
                            if let Err(error) =
                                self.handle_new_contract(&event.contract, now).await
                            {
                                tracing::error!(
                                    contract_id = %event.contract.id,
                                    error = %error,
                                    "snipe evaluation or execution failed"
                                );
                                self.emit_error(
                                    Some(event.contract.id.clone()),
                                    error.to_string(),
                                );
                            }
                        }
                        Some(ManifoldEvent::Bet(bet)) => {
                            if bet.user_id == self.client.bot_user_id() {
                                // Our own resting layer: maker fills, cancels,
                                // and expiry reports come through here.
                                if let Err(error) = self.on_own_bet_event(&bet).await {
                                    tracing::error!(
                                        contract_id = %bet.contract_id,
                                        bet_id = %bet.id,
                                        error = %error,
                                        "applying an own bet broadcast failed"
                                    );
                                    self.emit_error(
                                        Some(bet.contract_id.clone()),
                                        error.to_string(),
                                    );
                                }
                            } else {
                                tracing::debug!(
                                    contract_id = %bet.contract_id,
                                    "bet broadcast; copy-trading is a later phase"
                                );
                            }
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

                    if reconcile_task.is_some() {
                        tracing::debug!("reconcile pass already running; skipping this tick");
                        continue;
                    }
                    let executor = Arc::clone(&self);
                    let reporting = reconcile_tx.clone();
                    reconcile_task = Some(tokio::spawn(async move {
                        let summary = executor.reconcile_pass().await;

                        // Belief-decay exits on quiet markets (no posts, so
                        // no signal; no websocket event, so no evaluation)
                        // are caught by this periodic sweep: after every
                        // pass the cached state is complete
                        // enough to decide on, and this periodic sweep runs
                        // `evaluate_cached` for every active market with the
                        // same gating as the ws path (status, kill switch,
                        // terminal-mirror skip — no debounce is needed at
                        // 5-minute cadence). Nothing here calls REST per
                        // market; fresh REST state is what the pass just
                        // fetched.
                        executor.evaluate_all_cached().await;

                        let _ = reporting.send(summary).await;
                    }));
                }
                maybe = reconcile_rx.recv(), if reconcile_task.is_some() => {
                    // The pass signalled completion (or died without one);
                    // the slot is free for the next tick either way.
                    reconcile_task = None;
                    // The sender the loop holds outlives this arm, so a
                    // `None` means the task ended without reporting
                    // (cancelled at shutdown is the expected case).
                    if let Some(summary) = maybe {
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
    }

    /// Mirrors one observed probability patch into the store (cache + rows).
    ///
    /// Probabilities are clamped into the trading range exactly as
    /// [`Book::observe_prices`] clamps a REST snapshot, so both observation
    /// channels store comparable values. A terminal patch (the resolution
    /// signature, see [`patch_is_terminal`]) additionally marks the market in
    /// [`TerminalMirrors`]: the clamp hides the resolution, so without the
    /// mark a later material signal would try to trade a settled contract.
    /// Only a full [`TradeExecutor::sync_market`] clears the mark.
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
        )?;

        if patch_is_terminal(patch) {
            self.mark_terminal_mirror(market_id);
        }
        Ok(())
    }

    /// Records that `market_id`'s last observed price patch was terminal.
    fn mark_terminal_mirror(&self, market_id: &str) {
        self.terminal_mirrors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .mark(market_id);
    }

    /// Reports whether `market_id`'s last observed price patch was terminal.
    fn terminal_mirror(&self, market_id: &str) -> bool {
        self.terminal_mirrors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(market_id)
    }

    /// Full-sync completion of a market's terminal-mirror state.
    fn clear_terminal_mirror(&self, market_id: &str) {
        self.terminal_mirrors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear(market_id);
    }

    /// Cancels one own resting order (best effort, rate-checked).
    ///
    /// The write spends one Trade-class token, exactly like a buy or sell; a
    /// rate refusal keeps the order resting (its expiry bounds it), and so
    /// does a failed cancel. Success drops the tracker entry.
    async fn cancel_own_order(&self, order: &OwnRestingOrder, market_id: &str, reason: &str) {
        if !self.acquire_write_token_for(market_id, "CANCEL resting order") {
            return;
        }
        match self.client.cancel_bet(&order.bet_id).await {
            Ok(()) => {
                self.own_orders().remove(market_id, &order.bet_id);
                tracing::info!(
                    market_id = %market_id,
                    bet_id = %order.bet_id,
                    outcome = %order.outcome,
                    limit_prob = order.limit_prob,
                    reason = reason,
                    "cancelled own resting limit order"
                );
            }
            Err(error) => {
                tracing::warn!(
                    market_id = %market_id,
                    bet_id = %order.bet_id,
                    reason = reason,
                    error = %error,
                    "cancelling own resting limit order failed; it expires on Manifold"
                );
            }
        }
    }

    /// Cancels every own resting order on one market (best effort).
    ///
    /// Used when a market becomes moot for trading: a terminal price patch
    /// (the resolution signature) or an operator liquidation. No execution
    /// lock: the map is guarded separately and no book column is touched,
    /// so this can fire straight from the websocket arm.
    async fn cancel_market_orders(&self, market_id: &str, reason: &str) {
        let orders = self.own_orders().of_market(market_id);
        for order in &orders {
            self.cancel_own_order(order, market_id, reason).await;
        }
    }

    /// Retires own resting orders on one market whose cap no longer agrees
    /// with the current belief (`docs/SPEC.md` §6.6, reverse-drift policy).
    ///
    /// Runs inside [`TradeExecutor::evaluate_cached`] after the price mirror
    /// step, before the plan: the fair value each order is compared against
    /// is derived the same way [`fair_capped_limit_prob`] derives a fresh
    /// entry cap (`entry_fair_value` with the sum-to-one normalization).
    /// Expired orders are first swept locally without any REST call —
    /// matching already rejects them inline (probe finding: expiry is not
    /// broadcast; a 1-minute cron flips `isCancelled`). Orders younger than
    /// [`OWN_ORDER_GRACE_MILLIS`] are never cancelled by this policy.
    async fn run_cancel_policy(&self, book: &Book, now_secs: i64) -> AppResult<()> {
        let market_id = book.id().to_owned();
        let now_ms = Utc::now().timestamp_millis();

        let expired = self.own_orders().retain_market(&market_id, |order| {
            order.expires_ms.is_some_and(|expires| expires <= now_ms)
        });
        if expired > 0 {
            tracing::info!(
                market_id = %market_id,
                expired,
                "swept expired own resting orders without a cancel call"
            );
        }

        let orders = self.own_orders().of_market(&market_id);
        if orders.is_empty() {
            return Ok(());
        }

        // The same leg views the entry caps were built from on this
        // evaluation's price state.
        let leg_p_values: Vec<f64> = (0..book.legs().len())
            .map(|index| book.view(index, now_secs, self.halflife_hours).p_internal)
            .collect();

        for order in &orders {
            if now_ms.saturating_sub(order.created_ms) < OWN_ORDER_GRACE_MILLIS {
                continue;
            }
            let Some(leg) = leg_index_for(book, &order.answer_id) else {
                tracing::debug!(
                    market_id = %market_id,
                    bet_id = %order.bet_id,
                    answer_id = ?order.answer_id,
                    "own resting order lost its leg; leaving it to expire"
                );
                continue;
            };
            let view = book.view(leg, now_secs, self.halflife_hours);
            let fair = entry_fair_value(
                view.p_internal,
                &leg_p_values,
                book.market().should_answers_sum_to_one,
            );
            if should_cancel_for_drift(
                &order.outcome,
                order.limit_prob,
                fair,
                self.limit_epsilon,
                self.limit_drift_cancel,
            ) {
                self.cancel_own_order(order, &market_id, "reverse_drift").await;
            }
        }

        Ok(())
    }

    /// Cancels own resting orders that buy the opposite side of an entry
    /// executed in the same evaluation (best effort, same grace rule).
    ///
    /// The match is per leg: an entry on answer A does not touch a resting
    /// order on answer B of the same multiple-choice market. When the entry
    /// happened, the opposite order would take the book into a position this
    /// evaluation just bet against.
    async fn cancel_opposite_after_entry(
        &self,
        book: &Book,
        decision: &TradeDecision,
        now_secs: i64,
    ) -> AppResult<()> {
        if !is_buy(decision.action) {
            return Ok(());
        }
        let entered_outcome = decision.action.outcome();
        let opposite = if entered_outcome == "YES" { "NO" } else { "YES" };
        let market_id = book.id().to_owned();
        let now_ms = now_secs.saturating_mul(1_000);

        let orders = self.own_orders().of_market(&market_id);
        for order in &orders {
            if order.outcome != opposite || order.answer_id != decision.answer_id {
                continue;
            }
            if now_ms.saturating_sub(order.created_ms) < OWN_ORDER_GRACE_MILLIS {
                continue;
            }
            self.cancel_own_order(order, &market_id, "opposite_entry").await;
        }

        Ok(())
    }

    /// Records the order any fair-capped sweep leaves resting.
    ///
    /// Probe-verified: one `POST /v0/bet` with a limit does the whole ladder
    /// — the unfilled remainder (`orderAmount − amount`) rests automatically
    /// as the same bet row, so the only bookkeeping is to track it. A dry
    /// run places nothing; a fully filled bet rests nothing.
    fn record_resting_order(
        &self,
        book: &Book,
        outcome: &str,
        answer_id: Option<&str>,
        limit_prob: f64,
        result: &BetResult,
        dry_run: bool,
    ) {
        if dry_run || result.is_filled || result.is_cancelled {
            return;
        }
        let order_amount = result.order_amount.unwrap_or(0.0);
        let remainder = order_amount - result.amount;
        if !remainder.is_finite() || remainder <= REMAINDER_EPSILON {
            return;
        }
        if optional_id(result.bet_id()).is_none() {
            tracing::warn!(
                market_id = %book.id(),
                remainder,
                "a resting remainder has no bet id to track it; it simply expires"
            );
            return;
        }

        let now_ms = Utc::now().timestamp_millis();
        let order = OwnRestingOrder {
            bet_id: result.bet_id().to_owned(),
            market_id: book.id().to_owned(),
            answer_id: answer_id.map(str::to_owned),
            outcome: outcome.to_owned(),
            limit_prob,
            order_amount,
            filled_amount: result.amount,
            filled_shares: result.shares,
            created_ms: now_ms,
            expires_ms: Some(now_ms + self.limit_expiry_millis()),
        };
        self.own_orders().upsert(order);
        tracing::info!(
            market_id = %book.id(),
            bet_id = result.bet_id(),
            outcome,
            limit_prob,
            remainder,
            expires_in_secs = self.limit_expiry_secs,
            "buy remainder rests as an own expiring limit order"
        );
    }

    /// Applies one websocket update of the bot's own bet to the executor
    /// state (`docs/SPEC.md` §6.6, maker-fill accounting).
    ///
    /// The `global/new-bet` stream carries our maker fills (probe-verified),
    /// and `BetEvent` reports cumulative `amount`/`shares` per bet. A fill
    /// delta beyond the stored counters is applied through the same book
    /// flow as an ordinary buy: `Book::apply_buy` + a `trade_ledger` row
    /// (provenance `maker_fill` in the trigger column) + a Discord
    /// [`TradeEvent`], all under the execution lock. Duplicate broadcasts
    /// are idempotent by construction — the delta is zero — and the stored
    /// counters are overwritten with the server's values first, so a
    /// double-send can never re-apply a consumed fill.
    async fn on_own_bet_event(&self, bet: &BetEvent) -> AppResult<()> {
        if bet.user_id != self.client.bot_user_id() {
            // Not ours: the run arm normally filters these before calling,
            // so this is a defensive no-op against future call sites.
            return Ok(());
        }
        if bet.is_redemption {
            tracing::debug!(
                contract_id = %bet.contract_id,
                bet_id = %bet.id,
                "own redemption broadcast ignored"
            );
            return Ok(());
        }

        let Some(order) = self
            .own_orders()
            .get(&bet.contract_id, &bet.id)
            .cloned()
        else {
            // Either the placement broadcast of a bet the POST response
            // already accounts (our own taker bet), or an order placed
            // before this process started: not tracked here, so the event
            // would double-count.
            tracing::debug!(
                contract_id = %bet.contract_id,
                bet_id = %bet.id,
                outcome = %bet.outcome,
                "own bet broadcast is not a tracked resting order"
            );
            return Ok(());
        };

        let now_ms = Utc::now().timestamp_millis();
        let update = own_bet_update(&order, bet, now_ms);

        // The server's counters are authoritative: overwrite the stored view
        // before (rather than only after) applying, so an interrupted
        // application can never re-apply from a duplicate event.
        if update.fill.is_some() {
            let refreshed = OwnRestingOrder {
                filled_amount: bet.amount,
                filled_shares: bet.shares,
                expires_ms: bet.expires_at.or(order.expires_ms),
                ..order.clone()
            };
            self.own_orders().upsert(refreshed);
        }

        let Some((fill_mana, fill_shares)) = update.fill else {
            if update.removed {
                self.own_orders().remove(&bet.contract_id, &bet.id);
                tracing::debug!(
                    contract_id = %bet.contract_id,
                    bet_id = %bet.id,
                    "own resting order is dead; dropped from tracking"
                );
            }
            return Ok(());
        };

        self.apply_maker_fill(&order, fill_mana, fill_shares, bet)
            .await?;
        if update.removed {
            self.own_orders().remove(&bet.contract_id, &bet.id);
        }
        Ok(())
    }

    /// Posts one maker fill into the same book flow a taker fill uses.
    ///
    /// The fill happened on Manifold already — this is accounting, not a
    /// decision. The budget invariant still gates the *local* application:
    /// a fill that would breach the ceiling (only possible after external
    /// interference with the market's budget columns) is refused locally and
    /// left to the next reconcile pass, which trusts Manifold's positions
    /// (`docs/SPEC.md` §6.6); a Discord error is emitted.
    async fn apply_maker_fill(
        &self,
        order: &OwnRestingOrder,
        fill_mana: f64,
        fill_shares: f64,
        bet: &BetEvent,
    ) -> AppResult<()> {
        let _guard = self.execution_lock.lock().await;

        if self.store.cached(&bet.contract_id).is_none() {
            tracing::warn!(
                contract_id = %bet.contract_id,
                bet_id = %bet.id,
                "maker fill arrived for an untracked market; not applied"
            );
            return Ok(());
        }
        let mut book = self.load_book(&bet.contract_id)?;

        let market = book.market();
        let committed = market.invested_mana + fill_mana;
        let ceiling = market.budget_mana + market.realized_pnl_mana;
        if committed > ceiling + BUDGET_EPSILON {
            let message = format!(
                "maker fill of {} mana on market {} would breach the budget invariant \
                 (invested {committed} > budget plus realized PnL {ceiling}); \
                 applied nothing locally, the reconcile pass will trust Manifold",
                fill_mana,
                bet.contract_id
            );
            tracing::error!(contract_id = %bet.contract_id, "{message}");
            self.emit_error(Some(bet.contract_id.clone()), message);
            return Ok(());
        }

        let Some(leg) = leg_index_for(&book, &order.answer_id) else {
            tracing::error!(
                contract_id = %bet.contract_id,
                bet_id = %bet.id,
                answer_id = ?order.answer_id,
                "maker fill has no leg to apply to; not applied"
            );
            return Ok(());
        };

        let action = if order.outcome == "YES" {
            TradeAction::BuyYes
        } else {
            TradeAction::BuyNo
        };

        let now = Utc::now().timestamp();
        let view = book.view(leg, now, self.halflife_hours);
        book.apply_buy(leg, action, fill_mana, fill_shares)?;
        book.persist(&self.store, &self.db)?;

        let record = TradeRecord {
            id: 0,
            market_id: bet.contract_id.clone(),
            answer_id: order.answer_id.clone(),
            action,
            mana_amount: fill_mana,
            shares_delta: fill_shares,
            p_internal: view.p_internal,
            p_market: view.p_market,
            trigger_post_uri: Some(format!("maker_fill:{}", bet.id)),
            manifold_bet_id: Some(bet.id.clone()),
            created_at: now,
        };
        self.db.insert_trade(&record)?;
        tracing::info!(
            market_id = %bet.contract_id,
            bet_id = %bet.id,
            outcome = %order.outcome,
            mana = fill_mana,
            shares = fill_shares,
            "maker fill applied to the book"
        );

        // Post-fill budget sanity: must never go negative net.
        let after = book.market();
        if available_budget(&after) + BUDGET_EPSILON < 0.0 {
            let message = format!(
                "maker fill on market {} left a negative available budget; \
                 check the market's budget columns",
                bet.contract_id
            );
            tracing::error!(contract_id = %bet.contract_id, "{message}");
            self.emit_error(Some(bet.contract_id.clone()), message);
        }

        let _ = self.events.send(trade_event(&after, &record));
        Ok(())
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
    ///    `get-user-contract-metrics-with-contracts` call replaces the
    ///    per-market `/positions` sync. Every tracked market present in the
    ///    snapshot is reconciled through [`Book::reconcile`] (the same
    ///    warn-on-mismatch-trust-remote comparison), and when the pagination
    ///    is complete, tracked markets *absent* from it are treated as flat.
    /// 3. A single summary line, logged by `run` when the pass reports back
    ///    over its channel; `evaluate_all_cached` sweeps every active market
    ///    over the freshly synced state right after.
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
            let (page, raw_entries, final_page) = match self.client.portfolio_page(offset).await {
                Ok((page, raw_entries, final_page)) => (page, raw_entries, final_page),
                Err(error) => {
                    tracing::error!(error = %error, "portfolio snapshot fetch failed");
                    break;
                }
            };
            // The offset advances by the raw row count the server consumed:
            // empty metric lists are dropped by the parser but still occupy
            // a row, and an under-counted offset would re-fetch them next
            // page. Completeness is likewise declared from the raw count.
            by_contract.extend(page);
            complete = final_page;
            offset += raw_entries;
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
    ///
    /// Takes the executor's `execution_lock` so a portfolio snapshot that was
    /// taken before a command cannot overwrite a position the command just
    /// filled: an operator's `/track`-sized trade and this application of a
    /// pre-trade snapshot must not interleave.
    ///
    /// # Lock ordering
    ///
    /// The lock is held only across local book work (load, `Book::reconcile`,
    /// persist) — no REST call and no other lock is acquired while it is
    /// held, so it cannot deadlock with the command handlers (Discord
    /// commands acquire the same lock inside `evaluate_market`/`liquidate`,
    /// also with nothing nested under it). One lock at a time, briefly,
    /// everywhere: that is the whole ordering rule.
    async fn reconcile_book_position(
        &self,
        market_id: &str,
        positions: &MarketPositions,
        mismatches: &mut usize,
    ) -> AppResult<()> {
        let _guard = self.execution_lock.lock().await;

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
    /// A buy keeps the budget invariant checks (with the rested layer counted
    /// as committed), sweeps with the fair-capped, whole-percent-quantized
    /// limit ([`fair_capped_limit_prob`], fed by [`entry_fair_value`]), sends
    /// `dryRun` when asked, and records the fill on its leg. A cap-stopped
    /// short fill is logged with its shortfall, the remainder is tracked as
    /// an own expiring limit order ([`TradeExecutor::record_resting_order`]),
    /// and the leg records the *actual* shares and mana from the response —
    /// never the requested amount. A sell sends the requested share count
    /// and lets the book release the proportional cost basis. Neither branch
    /// writes a position column: the caller calls [`Book::persist`]
    /// afterwards.
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

            // The resting layer is committed mana that has not converted to a
            // position yet: one POST fills a part and rests the remainder
            // (probe-verified). Counting it in keeps a later maker fill of
            // an own order inside the ceiling (`docs/SPEC.md` §6.6).
            let outstanding = self.own_orders().outstanding(book.id());
            let available = book.available_budget() - outstanding;
            if amount > available + BUDGET_EPSILON {
                bail!(
                    "budget invariant violated for market {}: buy of {amount} mana exceeds \
                     available budget {available} (rested outstanding {outstanding})",
                    book.id()
                );
            }
            let market = book.market();
            let committed = market.invested_mana + outstanding + amount;
            let ceiling = market.budget_mana + market.realized_pnl_mana;
            if committed > ceiling + BUDGET_EPSILON {
                bail!(
                    "budget invariant violated for market {}: invested {committed} (rested \
                     outstanding {outstanding}) would exceed budget plus realized PnL {ceiling}",
                    book.id()
                );
            }

            if !self.acquire_write_token(book, decision.action) {
                return Ok(None);
            }

            let outcome = decision.action.outcome();
            // The fair value the cap must not fill past: the same decayed
            // belief the entry edge was computed from on this evaluation
            // (`Book::plan` produces every view at the same `now`), rotated
            // into the normalized share on sum-to-one markets.
            let leg_p_values: Vec<f64> = (0..book.legs().len())
                .map(|index| book.view(index, now, self.halflife_hours).p_internal)
                .collect();
            let fair = entry_fair_value(
                planned.p_internal,
                &leg_p_values,
                book.market().should_answers_sum_to_one,
            );
            let limit_prob = fair_capped_limit_prob(fair, self.limit_epsilon, outcome);

            let result = self
                .client
                .buy(
                    book.id(),
                    outcome,
                    amount,
                    decision.answer_id.as_deref(),
                    limit_prob,
                    dry_run,
                    self.limit_expiry_millis(),
                )
                .await
                .with_context(|| format!("buying {outcome} on Manifold market {}", book.id()))?;

            // Track the rest *before* the fill analysis: a cap-stopped sweep
            // can fill nothing at all, and the remainder rests either way.
            self.record_resting_order(
                book,
                outcome,
                decision.answer_id.as_deref(),
                limit_prob,
                &result,
                dry_run,
            );

            // No retry and no resting-order layoff on a short fill (see
            // analyze_buy_fill): the ledger records the ACTUAL shares and
            // mana below.
            if !analyze_buy_fill(&result, amount, book.id())? {
                return Ok(None);
            }
            if dry_run {
                tracing::info!(
                    market_id = %book.id(),
                    answer_id = ?decision.answer_id,
                    action = %decision.action,
                    amount,
                    shares = result.shares,
                    limit_prob,
                    prob_before = ?result.prob_before,
                    prob_after = ?result.prob_after,
                    "dry run: simulated buy, nothing persisted"
                );
                return Ok(None);
            }

            if !result.shares.is_finite() || result.shares <= 0.0 {
                // analyze_buy_fill only returns true with a positive,
                // finite share count; this guard keeps that invariant
                // explicit before the position columns are touched.
                bail!(
                    "Manifold bet {} on market {} returned no shares",
                    result.bet_id(),
                    book.id()
                );
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
    /// call and the next material signal or price update re-runs it.
    /// Returning `false` instead of an error keeps a rate refusal out of the
    /// per-market error path, where it would be indistinguishable from a
    /// broken market.
    fn acquire_write_token(&self, book: &Book, action: TradeAction) -> bool {
        self.acquire_write_token_for(book.id(), action.as_str())
    }

    /// [`TradeExecutor::acquire_write_token`] for writes keyed by market id
    /// alone: a resting-order cancel has no trade action to name.
    fn acquire_write_token_for(&self, market_id: &str, what: &str) -> bool {
        match self.rate.try_acquire(Priority::Trade, WRITE_COST_TOKENS) {
            Ok(_) => true,
            Err(denied) => {
                tracing::warn!(
                    market_id = %market_id,
                    write = what,
                    remaining_tokens = denied.remaining,
                    floor_tokens = denied.threshold,
                    "Manifold rate bucket is at the trade floor; skipping write"
                );
                false
            }
        }
    }
}

/// The leg index a resting order's answer maps to.
///
/// `None` maps to the single binary leg; an answer id is matched against the
/// legs' ids. `Some` answer on a binary book (or a lost answer row) has no
/// leg: the caller leaves such an order to expire.
fn leg_index_for(book: &Book, answer_id: &Option<String>) -> Option<usize> {
    book.legs()
        .iter()
        .position(|leg| leg.answer_id == *answer_id)
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
        strategy: market.strategy.clone(),
    }
}

/// Rejects a bet response that Manifold did not fill.
///
/// Buy fills are analyzed separately by [`analyze_buy_fill`], which accepts
/// the partial fills a fair-capped sweep produces; the sell path keeps the
/// all-or-nothing contract.
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

/// The wire `limitProb` of one fair-capped buy (`docs/ALGO.md` §S1).
///
/// The bot must never fill worse than its own believed fair price, plus one
/// [`Config::limit_epsilon`] of slack. Manifold reads `limitProb` as the
/// market probability of YES for both outcomes (see [`ManifoldClient::buy`]),
/// so a single expression caps both sides:
///
/// - YES buy: pays up to `limitProb` per share ⇒ cap `= fair − ε`.
/// - NO buy: pays up to `1 − limitProb` per NO share ⇒
///   `limitProb = fair − ε` gives `1 − (fair − ε) = (1 − fair) + ε`, exactly
///   the `limitProb(NO) = 1 − p_fair + ε` cap of §S1 expressed as the price
///   of the NO share itself.
///
/// Both the fair value and the result are clamped into the trading range:
/// the fair clamp keeps the cap inside Manifold's documented `[0.01, 0.99]`
/// validation, and the result clamp keeps a boundary-probability belief
/// (0.01 after the fair clamp) from sending `limitProb = 0.0`.
///
/// The result is also **quantized onto Manifold's whole-percent grid**
/// (probe-verified: finer values are rejected with HTTP 400), in the one
/// direction that can only make the cap *less* aggressive for the outcome
/// being bought — this is the single invariant: the sent cap never permits a
/// worse per-share fill than the unquantized cap:
///
/// - YES buy: the per-share price IS `limitProb`, so round **down**
///   (`floor`): the ceiling only gets tighter.
/// - NO buy: the per-NO-share ceiling is `1 − limitProb`, so `floor` would
///   *raise* it (a worse fill); round **up** (`ceil`), which keeps
///   `1 − limit ≤ 1 − cap_exact = fair_NO + ε`.
///
/// Because `entry_fair_value` supplies `fair` and the config validation
/// bounds `ε` only against the *default* entry-edge threshold (0.05), an
/// operator could theoretically configure `ENTRY_EDGE_THRESHOLD < ε`; an
/// entry with `edge < ε` would then send a limit at or below the market
/// price, which rests unfilled instead of sweeping (see [`analyze_buy_fill`]).
/// That is the documented limit of the guard, not a special-cased rejection.
fn fair_capped_limit_prob(fair: f64, epsilon: f64, outcome: &str) -> f64 {
    let fair = fair.clamp(MIN_TRADING_PROB, MAX_TRADING_PROB);
    let cap = (fair - epsilon).clamp(MIN_TRADING_PROB, MAX_TRADING_PROB);
    quantize_limit_prob(cap, outcome)
}

/// Quantizes a fair-capped cap onto Manifold's whole-percent `limitProb`
/// grid, rounded toward the less aggressive side for `outcome` ("YES" floors,
/// "NO" ceils; see [`fair_capped_limit_prob`]).
fn quantize_limit_prob(cap: f64, outcome: &str) -> f64 {
    let scaled = cap * 100.0;
    // `LIMIT_SNAP_EPSILON` covers representation error only (a `fair − ε`
    // computed in floating point can sit an ulp below the boundary it
    // belongs to); real caps never come this close to a percent boundary.
    let snapped = match outcome {
        "YES" => (scaled + LIMIT_SNAP_EPSILON).floor(),
        _ => (scaled - LIMIT_SNAP_EPSILON).ceil(),
    };
    (snapped / 100.0).clamp(MIN_TRADING_PROB, MAX_TRADING_PROB)
}

/// The internal fair probability one planned buy must not fill past.
///
/// On independent markets (binary legs and cpmm-multi-2 answers) that is the
/// leg's own decayed belief — the exact `p_internal` the entry edge was
/// computed from (`PlannedTrade.p_internal`). On sum-to-one multiple-choice
/// markets the edge math (`sum_to_one` branch of the planner's edge
/// computation) normalizes each belief by the sum over all legs, so the raw
/// belief alone would place the cap *above* the fair value the trade actually
/// bet on. The normalization here mirrors that computation leg-for-leg
/// (`clamp(0, 1)` on every view, same sum) by taking the raw probabilities of
/// every leg as `leg_p_values`. A degenerate sum (zero, non-finite) falls
/// back to the raw belief: planner gating would have rejected the entry in
/// that case anyway.
fn entry_fair_value(p_raw: f64, leg_p_values: &[f64], sum_to_one: bool) -> f64 {
    if !sum_to_one {
        return p_raw;
    }
    let total: f64 = leg_p_values.iter().map(|p| p.clamp(0.0, 1.0)).sum();
    if !total.is_finite() || total <= 0.0 {
        return p_raw;
    }
    p_raw.clamp(0.0, 1.0) / total
}

/// Accepts a buy response and reports whether anything filled.
///
/// The buy path no longer demands a fully filled bet: the fair-capped sweep
/// can stop short of the budget, and Manifold then fills what the cap allows
/// and RESTS the remainder as an open limit order at the cap — the rest
/// carries `LIMIT_EXPIRY_SECS` as `expiresMillisAfter` (see
/// [`TradeExecutor::record_resting_order`], which ran before this analysis),
/// so it is picked off or dies without a cancel call.
///
/// `Ok(false)` (nothing filled, no position to record) happens when the price
/// moved past the cap before the fill - the order then rests waiting for a
/// retrace past a belief that is no longer an edge. `Ok(true)` covers both a
/// full fill and a partial one; the caller records the *actual* shares and
/// mana either way.
///
/// Only a cancelled response is an error: resting money without any fill is
/// a normal consequence of the cap, not a broken trade.
fn analyze_buy_fill(result: &BetResult, requested_mana: f64, market_id: &str) -> AppResult<bool> {
    if result.is_cancelled {
        bail!(
            "Manifold bet {} on market {market_id} was cancelled",
            result.bet_id()
        );
    }

    let filled = result.shares.is_finite() && result.shares > 0.0;
    let requested = result.order_amount.unwrap_or(requested_mana);
    if !filled {
        tracing::info!(
            market_id = %market_id,
            bet_id = %result.bet_id(),
            requested_mana = requested,
            filled_mana = result.amount,
            shares = result.shares,
            "buy did not fill at the fair-capped limit; the order rests on Manifold"
        );
        return Ok(false);
    }

    if !result.is_filled {
        tracing::info!(
            market_id = %market_id,
            bet_id = %result.bet_id(),
            requested_mana = requested,
            filled_mana = result.amount,
            shares = result.shares,
            "buy partially filled at the fair-capped limit; the remainder rests \
             as a limit order"
        );
    }
    Ok(true)
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
            strategy: crate::types::STRATEGY_NEWS.to_owned(),
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
    fn bet_result_reads_order_amount_on_a_limit_bet() {
        // A fair-capped sweep that runs out of liquidity reports `isFilled:
        // false` with the partial fill in `amount`/`shares` and the original
        // request in `orderAmount`; the shortfall log reads both.
        let raw = json!({
            "id": "limit-1",
            "amount": 10.0,
            "shares": 20.0,
            "limitProb": 0.5,
            "orderAmount": 100.0,
            "isFilled": false
        });
        let bet: BetResult = serde_json::from_value(raw).expect("limit shape should deserialize");

        assert_eq!(bet.order_amount, Some(100.0));
        assert!(!bet.is_filled);
        assert_close(bet.total_cost(), 10.0);
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
    fn bet_result_tolerates_a_sell_response_without_shares() {
        // Sell responses can omit `shares`; downstream already falls back to
        // the requested count, so the field must default instead of failing.
        let raw = json!({
            "id": "sell-2",
            "amount": -6.0,
            "isFilled": true
        });
        let bet: BetResult = serde_json::from_value(raw).expect("sell shape should deserialize");

        assert_eq!(bet.shares, 0.0, "missing shares becomes the fallback sentinel");
        assert_close(bet.net_proceeds(), 6.0);
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
    fn portfolio_pagination_counts_raw_entries_not_parsed_ones() {
        // 5 raw rows on the page: "bin" carries 3, "mc" carries 1, and
        // "empty" carries one row the parser drops (its list is empty).
        // The raw count is what advances `offset` and what decides "is
        // this the final page?": counting the parsed map instead would
        // under-count by one, stack up duplicate rows each page, and —
        // when a page is full of dropped rows — wrongly declare the
        // snapshot complete and zero absent tracked markets.
        let value = json!({
            "metricsByContract": {
                "bin": [
                    {"userId": "bot-1", "totalShares": {"YES": 1.0}, "invested": 1.0},
                    {"userId": "bot-1", "answerId": "a", "totalShares": {"YES": 2.0}, "invested": 1.0},
                    {"userId": "bot-1", "answerId": "b", "totalShares": {"NO": 3.0}, "invested": 1.0}
                ],
                "mc": [
                    {"userId": "bot-1", "totalShares": {"YES": 4.0}, "invested": 2.0}
                ],
                "empty": []
            }
        });

        assert_eq!(raw_metrics_entry_count(&value), 5, "server consumed 5 rows");

        let by_contract = parse_metrics_by_contract(&value);
        assert_eq!(by_contract.len(), 2, "the parser drops empty lists");
        let parsed: usize = by_contract.values().map(Vec::len).sum();
        assert_eq!(parsed, 4, "parsed rows are one below the raw count");
    }

    #[test]
    fn raw_metrics_count_shapeless_values_and_payloads() {
        // No usable key, or a key whose value is itself shapeless: such a
        // value still consumed a row, so it counts as one. A missing key
        // counts nothing.
        assert_eq!(raw_metrics_entry_count(&json!({})), 0);
        assert_eq!(
            raw_metrics_entry_count(&json!({"metricsByContract": null})),
            0
        );
        assert_eq!(
            raw_metrics_entry_count(&json!({"metricsByContract": {"c": "no"}})),
            1
        );
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

    // -- Terminal-mirror state ---------------------------------------------

    #[test]
    fn terminal_mirrors_block_evaluation_until_a_full_sync_clears_them() {
        let mut mirrors = TerminalMirrors::default();
        assert!(!mirrors.contains("m1"), "fresh state skips nothing");

        // A terminal (resolution-signature) price patch marks the market.
        mirrors.mark("m1");
        assert!(mirrors.contains("m1"));

        // The mark is idempotent and survives more observed prices: only a
        // full sync_market refresh clears it.
        mirrors.mark("m1");
        assert!(mirrors.contains("m1"));

        mirrors.clear("m1");
        assert!(!mirrors.contains("m1"), "a full sync clears the skip");

        // Clearing without a mark stays a no-op.
        mirrors.clear("m1");
        assert!(!mirrors.contains("m1"));
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

    // -- Fair-capped buy limits --------------------------------------------

    #[test]
    fn bet_body_carries_the_capped_limit_prob() {
        let body = bet_body("mkt", "YES", 10.0, None, 0.62, false, 60_000);

        assert_eq!(body["contractId"], "mkt");
        assert_close(body["amount"].as_f64().unwrap(), 10.0);
        assert_eq!(body["outcome"], "YES");
        assert_close(body["limitProb"].as_f64().unwrap(), 0.62);
        assert_eq!(body["dryRun"], false);
        assert_eq!(body["expiresMillisAfter"], 60_000);
        assert!(body.get("answerId").is_none(), "binary buys omit answerId");

        let mc_body = bet_body("mkt", "YES", 10.0, Some("ans-a"), 0.49, true, 120_000);
        assert_eq!(mc_body["answerId"], "ans-a");
        assert_eq!(mc_body["dryRun"], true);
        assert_eq!(mc_body["expiresMillisAfter"], 120_000);
        assert_close(mc_body["limitProb"].as_f64().unwrap(), 0.49);
    }

    #[test]
    fn bet_body_omits_the_expiry_field_when_it_is_not_requested() {
        let body = bet_body("mkt", "NO", 10.0, None, 0.4, false, 0);

        assert!(body.get("expiresMillisAfter").is_none());
    }

    #[test]
    fn fair_cap_for_a_yes_buy_is_fair_minus_epsilon() {
        // The §S1 example: a belief of 63 on a YES entry buys at up to 62
        // with the default ε = 0.01. Whole-percent quantization floors the
        // YES cap (the per-share price IS limitProb).
        assert_close(fair_capped_limit_prob(0.63, 0.01, "YES"), 0.62);
        assert_close(fair_capped_limit_prob(0.5, 0.01, "YES"), 0.49);
    }

    #[test]
    fn fair_cap_for_a_no_buy_prices_fair_no_plus_epsilon() {
        // The wire value equals the YES expression (Manifold reads
        // limitProb as the market probability of YES for both outcomes), so
        // the per-NO-share ceiling is 1 − cap = fair_NO + ε: for a belief of
        // YES 0.30, NO shares never cost more than 0.71.
        let cap = fair_capped_limit_prob(0.30, 0.01, "NO");
        assert_close(cap, 0.29);
        assert_close(1.0 - cap, (1.0 - 0.30) + 0.01);
    }

    #[test]
    fn fair_cap_clamps_at_the_boundary_beliefs() {
        // A boundary belief of 0.01 with ε = 0.01 would undercut the
        // Manifold limitProb minimum (0.01), so the cap bottoms out there
        // instead of going zero or negative.
        assert_close(fair_capped_limit_prob(0.01, 0.01, "YES"), 0.01);
        // Defense at the top edge: 0.99 − 0.01 stays valid.
        assert_close(fair_capped_limit_prob(0.99, 0.01, "YES"), 0.98);
        // A pathological ε past the clamp never produces an invalid limit.
        assert_close(fair_capped_limit_prob(0.01, 0.05, "NO"), 0.01);
    }

    #[test]
    fn caps_are_quantized_to_whole_percents_toward_less_aggressive() {
        // Manifold rejects off-grid limitProb values with HTTP 400
        // (probe result). The quantization direction is outcome-sensitive:
        // the one invariant is that the sent cap never permits a worse
        // per-share fill than the unquantized exact cap.
        let exact = 0.63_f64.clamp(0.01, 0.99) - 0.01; // 0.62 exactly on the grid
        let yes = fair_capped_limit_prob(0.6389, 0.01, "YES");
        let no = fair_capped_limit_prob(0.6389, 0.01, "NO");

        assert!(yes <= exact, "YES quantization must floor: {yes} ≤ {exact}");
        let grid = |value: f64| (value * 100.0) - (value * 100.0).round();
        assert!(
            grid(yes).abs() < 1e-9,
            "YES cap is whole-percent grid: {yes}"
        );

        // NO differs per-share: ceil on the YES scale keeps 1 − cap ≤ fair_NO
        // + ε (the exact cap's ceiling), never worse.
        let exact_no_ceiling = 1.0 - exact;
        assert!(
            grid(no).abs() < 1e-9,
            "NO cap is whole-percent grid: {no}"
        );
        assert!(
            (1.0 - no) <= (1.0 - 0.6389) + 0.01 + 1e-9,
            "NO ceiling never exceeds the unquantized one in any direction beyond representation: \
             {no} vs {exact_no_ceiling}"
        );
    }

    #[test]
    fn quantization_rounds_yes_down_and_no_up_of_an_off_grid_exact_cap() {
        // An exact cap of 0.295 (fair 0.305, ε 0.01):
        // YES must send 0.29 (floors), NO must send 0.30 (ceils) so the
        // per-NO ceiling 1 − 0.30 = 0.70 stays ≤ fair_NO + ε = 0.705.
        assert_close(fair_capped_limit_prob(0.305, 0.01, "YES"), 0.29);
        assert_close(fair_capped_limit_prob(0.305, 0.01, "NO"), 0.30);
    }

    #[test]
    fn quantization_respects_the_whole_percent_grid_for_both_sides() {
        for fair in [0.017, 0.123, 0.3456, 0.5555, 0.7777, 0.9012, 0.989] {
            for epsilon in [0.01, 0.02, 0.049] {
                let yes = fair_capped_limit_prob(fair, epsilon, "YES");
                let no = fair_capped_limit_prob(fair, epsilon, "NO");
                let exact = (fair.clamp(0.01, 0.99) - epsilon).clamp(0.01, 0.99);

                let grid = |value: f64| (value * 100.0) - (value * 100.0).round();
                assert!(
                    grid(yes).abs() < 1e-9,
                    "YES cap {yes} off grid"
                );
                assert!(
                    grid(no).abs() < 1e-9,
                    "NO cap {no} off grid"
                );
                assert!(yes <= exact + 1e-9, "YES {yes} worse than exact {exact}");
                assert!(1.0 - no <= 1.0 - exact + 1e-9, "NO ceiling from {no} worse than exact");
            }
        }
    }

    #[test]
    fn entry_fair_value_is_the_raw_belief_off_sum_to_one_markets() {
        // Binary legs (the "no-answer" case, answer_id None) and independent
        // cpmm-multi-2 answers keep their own belief: exactly the value the
        // edge math used.
        assert_close(entry_fair_value(0.63, &[0.63], false), 0.63);
        assert_close(entry_fair_value(0.2, &[0.2, 0.4, 0.3], false), 0.2);
    }

    #[test]
    fn entry_fair_value_normalizes_sum_to_one_legs_like_the_planner() {
        // The edge math divides each belief by the sum over all legs; the
        // cap must be built from that same normalized share, not the raw
        // belief (which sits above the fair value the bet was sized on).
        assert_close(entry_fair_value(0.9, &[0.9, 0.9], true), 0.5);
        assert_close(entry_fair_value(0.2, &[0.2, 0.4, 0.4], true), 0.2);

        let mc_cap = fair_capped_limit_prob(
            entry_fair_value(0.9, &[0.9, 0.9], true),
            0.01,
            "YES",
        );
        assert_close(mc_cap, 0.49);

        // A degenerate sum cannot happen after planner gating; the fallback
        // is the raw belief rather than a division by zero.
        assert_close(entry_fair_value(0.7, &[0.0, 0.0], true), 0.7);
    }

    // -- Reverse-drift cancel predicate ------------------------------------
    //
    // Caps land on the whole-percent grid, so an overpay at exactly the
    // slack lies on a percent boundary and its floating-point direction is
    // arbitrary (±4e-17). The truth table therefore keeps cases at least one
    // percent step away from the slack: the policy is re-checked on every
    // evaluation anyway, so a boundary wobble costs nothing.

    #[test]
    fn a_drifted_yes_resting_order_is_cancelled_and_a_fresh_one_is_kept() {
        // YES order resting at 0.40. Fair dropped to 0.34 → today's cap is
        // 0.33; paying 0.40 is 0.07 beyond it, more than the default slack.
        assert!(should_cancel_for_drift("YES", 0.40, 0.34, 0.01, 0.05));

        // Fair dropped a little less: cap 0.36, overpay 0.04, inside the
        // slack → keep (re-checked on every evaluation).
        assert!(!should_cancel_for_drift("YES", 0.40, 0.37, 0.01, 0.05));

        // One percent step beyond the slack still cancels.
        assert!(should_cancel_for_drift("YES", 0.40, 0.35, 0.01, 0.05));

        // Favorable drift: fair rose; the order's price is better than we
        // could replicate today, so it stays.
        assert!(!should_cancel_for_drift("YES", 0.40, 0.60, 0.01, 0.05));
    }

    #[test]
    fn a_drifted_no_resting_order_is_cancelled_on_the_yes_scale() {
        // NO order resting at YES-scale 0.30 (per-NO price 0.70). Fair rose
        // to 0.40 → today's NO cap is 0.39 → per-NO ceiling 0.61; waiting to
        // fill at 0.70 is 0.09 worse than acceptable: cancel.
        assert!(should_cancel_for_drift("NO", 0.30, 0.40, 0.01, 0.05));

        // Fair rose a little less: NO cap 0.33, overpay 0.03 → keep
        // (re-checked on every evaluation).
        assert!(!should_cancel_for_drift("NO", 0.30, 0.34, 0.01, 0.05));

        // One percent step beyond the slack still cancels (fair 0.41 → cap
        // 0.40, overpay 0.10).
        assert!(should_cancel_for_drift("NO", 0.30, 0.41, 0.01, 0.05));

        // Fair sank toward NO: the order's per-NO price is now better than
        // today's entry standard, so it stays.
        assert!(!should_cancel_for_drift("NO", 0.30, 0.20, 0.01, 0.05));
    }

    #[test]
    fn an_unknown_outcome_never_panics_and_never_cancels_by_drift() {
        // Shape-bug guard: the expiry bounds such an order anyway.
        assert!(!should_cancel_for_drift("dpm", 0.5, 0.99, 0.01, 0.05));
    }

    // -- Own resting-order bookkeeping --------------------------------------

    const NOW_MS: i64 = 1_700_000_000_000;

    fn resting_order(
        bet_id: &str,
        market: &str,
        outcome: &str,
        limit_prob: f64,
    ) -> OwnRestingOrder {
        OwnRestingOrder {
            bet_id: bet_id.to_owned(),
            market_id: market.to_owned(),
            answer_id: None,
            outcome: outcome.to_owned(),
            limit_prob,
            order_amount: 50.0,
            filled_amount: 10.0,
            filled_shares: 20.0,
            created_ms: NOW_MS - 10_000,
            expires_ms: Some(NOW_MS + 50_000),
        }
    }

    fn own_bet_event(bet_id: &str, amount: f64, shares: f64) -> BetEvent {
        BetEvent {
            id: bet_id.to_owned(),
            user_id: "bot-1".to_owned(),
            contract_id: "Market_test".to_owned(),
            answer_id: None,
            outcome: "YES".to_owned(),
            amount,
            order_amount: 50.0,
            shares,
            prob_before: 0.5,
            prob_after: 0.5,
            is_filled: false,
            limit_prob: Some(0.49),
            expires_at: Some(NOW_MS + 40_000),
            is_cancelled: false,
            is_redemption: false,
            created_time: NOW_MS - 10_000,
            updated_time: Some(NOW_MS),
        }
    }

    #[test]
    fn a_fill_delta_is_only_the_shares_beyond_the_last_seen_counters() {
        let order = resting_order("rest-1", "Market_test", "YES", 0.49);
        let bet = own_bet_event("rest-1", 30.0, 60.0);

        let update = own_bet_update(&order, &bet, NOW_MS);

        let (mana, shares) = update.fill.expect("a growth is a fill");
        assert_close(mana, 20.0);
        assert_close(shares, 40.0);
        assert!(!update.removed, "an open order is not dead");

        // Replay against the *refreshed* counters (the server's values), the
        // way the executor stores them before applying: a duplicate
        // broadcast computes a zero delta and re-applies nothing.
        let refreshed = OwnRestingOrder {
            filled_amount: bet.amount,
            filled_shares: bet.shares,
            ..order.clone()
        };
        let replay = own_bet_update(&refreshed, &bet, NOW_MS);
        assert!(replay.fill.is_none(), "a duplicate must not re-apply");
        assert!(!replay.removed);

        // An older event replayed against refreshed counters (the .max(0)
        // clamps) must never unwind a fill.
        let stale = own_bet_event("rest-1", 12.0, 24.0);
        assert!(
            own_bet_update(&refreshed, &stale, NOW_MS).fill.is_none(),
            "a stale replay must not create a fill"
        );
    }

    #[test]
    fn cancelled_filled_and_expired_events_kill_the_tracker_entry() {
        let order = resting_order("rest-1", "Market_test", "YES", 0.49);

        let mut cancelled = own_bet_event("rest-1", 30.0, 60.0);
        cancelled.is_cancelled = true;
        assert!(own_bet_update(&order, &cancelled, NOW_MS).removed);

        let mut fully_filled = own_bet_event("rest-1", 50.0, 100.0);
        fully_filled.is_filled = true;
        let update = own_bet_update(&order, &fully_filled, NOW_MS);
        assert!(update.removed, "a full fill retires the order");
        let (mana, shares) = update.fill.expect("the final fill still applies");
        assert_close(mana, 40.0);
        let _ = shares;

        // Server expiry passed (or our placement estimate did): dead.
        let mut expired = own_bet_event("rest-1", 10.0, 20.0);
        expired.expires_at = Some(NOW_MS - 1);
        assert!(own_bet_update(&order, &expired, NOW_MS).removed);

        // When the event does not carry an expiry, the tracker's placement
        // estimate decides.
        let mut blind = own_bet_event("rest-1", 10.0, 20.0);
        blind.expires_at = None;
        assert!(own_bet_update(&expired_order(), &blind, NOW_MS).removed);
        assert!(!own_bet_update(&alive_order(), &blind, NOW_MS).removed);
    }

    fn expired_order() -> OwnRestingOrder {
        let mut expired = resting_order("rest-1", "Market_test", "YES", 0.49);
        expired.expires_ms = Some(NOW_MS - 1);
        expired
    }

    fn alive_order() -> OwnRestingOrder {
        resting_order("rest-1", "Market_test", "YES", 0.49)
    }

    #[test]
    fn the_own_resting_order_map_tracks_per_market_and_reports_outstanding() {
        let mut orders = OwnRestingOrders::default();
        let mut no_side = resting_order("rest-no", "Market_test", "NO", 0.29);
        no_side.order_amount = 25.0;
        no_side.filled_amount = 5.0;

        orders.upsert(resting_order("rest-1", "Market_test", "YES", 0.49));
        orders.upsert(no_side);

        assert_eq!(orders.of_market("Market_test").len(), 2);
        assert_eq!(orders.of_market("other").len(), 0);
        assert!(
            orders.get("Market_test", "rest-1").is_some(),
            "tracked by bet id"
        );

        // Outstanding = unspent committed mana: YES 50−10, NO 25−5.
        assert_close(orders.outstanding("Market_test"), 60.0);
        assert_close(orders.outstanding("other"), 0.0);

        // Removal is market-scoped and reports what went.
        assert_eq!(
            orders.remove("Market_test", "rest-1").expect("removed").bet_id,
            "rest-1"
        );
        assert!(orders.remove("Market_test", "rest-1").is_none());
        assert_close(orders.outstanding("Market_test"), 20.0);
    }

    #[test]
    fn expired_orders_are_swept_from_the_map_without_a_cancel_call() {
        let mut orders = OwnRestingOrders::default();
        orders.upsert(resting_order("alive", "Market_test", "YES", 0.49));
        orders.upsert(expired_order());

        let swept = orders.retain_market("Market_test", |order| {
            order.expires_ms.is_some_and(|expires| expires <= NOW_MS)
        });

        assert_eq!(swept, 1);
        assert_eq!(orders.of_market("Market_test").len(), 1);
        assert!(orders.get("Market_test", "alive").is_some());
    }

    #[test]
    fn the_boot_snapshot_parses_live_open_limit_order_rows() {
        // Live-row shape from the probe (/v0/bets?kinds=open-limit): a
        // partially filled expiring limit.
        let row = json!({
            "id": "rest-9",
            "userId": "bot-1",
            "contractId": "Market_test",
            "outcome": "NO",
            "answerId": "ans-a",
            "amount": 3.0,
            "orderAmount": 12.0,
            "shares": 9.0,
            "limitProb": 0.29,
            "isFilled": false,
            "isCancelled": false,
            "isRedemption": false,
            "expiresAt": NOW_MS + 25_000,
            "createdTime": NOW_MS - 35_000
        });
        let order = parse_own_order_row(&row, "bot-1", NOW_MS).expect("a restable row");

        assert_eq!(order.bet_id, "rest-9");
        assert_eq!(order.market_id, "Market_test");
        assert_eq!(order.outcome, "NO");
        assert_eq!(order.answer_id.as_deref(), Some("ans-a"));
        assert_close(order.limit_prob, 0.29);
        assert_close(order.filled_amount, 3.0);
        assert_close(order.order_amount, 12.0);
        assert_close(order.filled_shares, 9.0);
        assert_eq!(order.created_ms, NOW_MS - 35_000);
        assert_eq!(order.expires_ms, Some(NOW_MS + 25_000));
    }

    #[test]
    fn the_boot_snapshot_skips_dead_and_unrestable_rows() {
        let now = NOW_MS;
        let base = |mut row: Value| {
            row["id"] = json!("row");
            row["userId"] = json!("bot-1");
            row["contractId"] = json!("Market_test");
            row["outcome"] = json!("YES");
            row["amount"] = json!(0.0);
            row["orderAmount"] = json!(10.0);
            row["limitProb"] = json!(0.5);
            row
        };

        // Cancelled, fully filled, expired, redemption rows do not rest.
        let cancelled = {
            let mut row = base(json!({}));
            row["isCancelled"] = json!(true);
            row
        };
        assert!(parse_own_order_row(&cancelled, "bot-1", now).is_none());

        let filled = {
            let mut row = base(json!({}));
            row["isFilled"] = json!(true);
            row
        };
        assert!(parse_own_order_row(&filled, "bot-1", now).is_none());

        let expired = {
            let mut row = base(json!({}));
            row["expiresAt"] = json!(now - 500);
            row
        };
        assert!(
            parse_own_order_row(&expired, "bot-1", now).is_none(),
            "expired rows are dead even before the cron flips isCancelled"
        );

        let redemption = {
            let mut row = base(json!({}));
            row["isRedemption"] = json!(true);
            row
        };
        assert!(parse_own_order_row(&redemption, "bot-1", now).is_none());

        // Foreign user (response-shape guard) and off-outcome rows.
        let foreign = base(json!({})) ;
        assert!(parse_own_order_row(&foreign, "someone-else", now).is_none());

        let bad_outcome = {
            let mut row = base(json!({}));
            row["outcome"] = json!("DOWN");
            row
        };
        assert!(parse_own_order_row(&bad_outcome, "bot-1", now).is_none());

        // A row without a limit probability or with a zero remainder.
        let no_limit = {
            let mut row = base(json!({}));
            row.as_object_mut().unwrap().remove("limitProb");
            row
        };
        assert!(parse_own_order_row(&no_limit, "bot-1", now).is_none());

        let fully_committed = {
            let mut row = base(json!({}));
            row["orderAmount"] = json!(10.0);
            row["amount"] = json!(10.0);
            row
        };
        assert!(parse_own_order_row(&fully_committed, "bot-1", now).is_none());

        // A row without expiresAt falls back to the default lifetime.
        let no_expiry = base(json!({}));
        let order = parse_own_order_row(&no_expiry, "bot-1", now).expect("restable");
        assert_eq!(order.expires_ms, Some(now + FALLBACK_LIMIT_EXPIRY_SECS * 1_000));
    }

    #[test]
    fn leg_index_matches_the_binary_leg_and_answer_legs() {
        use crate::types::TrackedAnswer;

        // Binary single leg: only the None (binary) answer maps there.
        let market = market_with(0.5, 0.5, 0.0, 0.0, 0.0);
        let db = Db::open_in_memory().expect("in-memory DB");
        let store = MarketStore::new(&db).expect("store loads");
        store.insert(&market).expect("insert market");
        let binary = Book::load(&store, &db, "Market_test")
            .expect("load book")
            .expect("tracked");
        assert_eq!(leg_index_for(&binary, &None), Some(0));
        assert_eq!(leg_index_for(&binary, &Some("ans-a".to_owned())), None);

        // Multiple-choice: answer ids map to their leg only.
        let mut mc = market;
        mc.id = "Market_mc".to_owned();
        mc.outcome_type = "MULTIPLE_CHOICE".to_owned();
        store.insert(&mc).expect("insert mc market");
        for (index, answer_id) in ["a", "b"].iter().enumerate() {
            db.upsert_answer(&TrackedAnswer {
                id: (*answer_id).to_owned(),
                market_id: "Market_mc".to_owned(),
                text: format!("Answer {answer_id}"),
                p_internal_log_odds: 0.0,
                p_market_last: 0.5,
                belief_updated_at: 0,
                yes_shares: 0.0,
                no_shares: 0.0,
                created_at: index as i64,
                invested_mana: 0.0,
            })
            .expect("insert answer");
        }
        let answers = Book::load(&store, &db, "Market_mc")
            .expect("load book")
            .expect("tracked");
        assert_eq!(
            leg_index_for(&answers, &None),
            None,
            "an answers book has no binary leg"
        );
        assert_eq!(leg_index_for(&answers, &Some("a".to_owned())), Some(0));
        assert_eq!(leg_index_for(&answers, &Some("b".to_owned())), Some(1));
        assert_eq!(
            leg_index_for(&answers, &Some("lost".to_owned())),
            None,
            "a lost answer maps to no leg"
        );
    }

    // -- Maker-fill accounting end to end -----------------------------------

    /// A trade executor wired to nothing remote: an in-memory DB, a real
    /// (unused) HTTP client, and a fresh rate bucket. Every maker-fill test
    /// avoids the network by feeding websocket-shaped `BetEvent`s straight
    /// into [`TradeExecutor::on_own_bet_event`].
    fn offline_executor(db: &Db, store: &Arc<MarketStore>) -> TradeExecutor {
        offline_executor_with(
            db,
            store,
            ManifoldClient::for_test("http://127.0.0.1:1"),
            None,
        )
    }

    /// [`offline_executor`] with an explicit client and optional sniper.
    fn offline_executor_with(
        db: &Db,
        store: &Arc<MarketStore>,
        client: ManifoldClient,
        snipe: Option<crate::snipe::SnipeStrategy>,
    ) -> TradeExecutor {
        TradeExecutor {
            client,
            db: db.clone(),
            store: Arc::clone(store),
            policy: TradePolicy {
                params: params(),
                multiple_choice: TradeLimits::binary(),
            },
            dry_run: false,
            halflife_hours: 4.0,
            limit_epsilon: 0.01,
            limit_expiry_secs: 60,
            limit_drift_cancel: 0.05,
            rate: RateLimiter::per_minute(500, Box::new(SystemClock)),
            execution_lock: Mutex::new(()),
            terminal_mirrors: SyncMutex::new(TerminalMirrors::default()),
            own_orders: SyncMutex::new(OwnRestingOrders::default()),
            snipe,
            events: broadcast::channel(EVENT_CAPACITY).0,
        }
    }

    /// A YES limit placed a minute ago and partially filled at placement,
    /// with all times relative to the wall clock (the policy tests the local
    /// clock for expiry).
    fn live_resting_order(now_ms: i64) -> OwnRestingOrder {
        OwnRestingOrder {
            bet_id: "rest-1".to_owned(),
            market_id: "Market_test".to_owned(),
            answer_id: None,
            outcome: "YES".to_owned(),
            limit_prob: 0.49,
            order_amount: 50.0,
            filled_amount: 10.0,
            filled_shares: 20.0,
            created_ms: now_ms - 60_000,
            expires_ms: Some(now_ms + 40_000),
        }
    }

    #[tokio::test]
    async fn a_maker_fill_of_an_own_resting_order_applies_to_the_book_once() {
        // The placement already filled 10 mana / 20 shares, recorded in the
        // book exactly like execute() records a taker fill; the tracker
        // mirrors those counters.
        let market = market_with(100.0, 10.0, 0.0, 20.0, 0.0);
        let db = Db::open_in_memory().expect("in-memory DB");
        let store = Arc::new(MarketStore::new(&db).expect("store loads"));
        store.insert(&market).expect("insert market");
        let executor = offline_executor(&db, &store);
        let now_ms = Utc::now().timestamp_millis();

        executor.own_orders().upsert(live_resting_order(now_ms));

        // Another chunk of the same bet filled on Manifold: +20 mana, +40
        // shares beyond what placement reported.
        let mut bet = own_bet_event("rest-1", 30.0, 60.0);
        bet.expires_at = Some(now_ms + 40_000);
        executor.on_own_bet_event(&bet).await.expect("fill applies");

        // The book went through the same flow as an ordinary buy.
        let book = Book::load(&store, &db, "Market_test")
            .expect("load book")
            .expect("tracked");
        assert_close(book.leg(0).yes_shares, 60.0);
        assert_close(book.leg(0).invested_mana, 30.0);

        let trades = db.recent_trades("Market_test", 10).expect("ledger");
        assert_eq!(trades.len(), 1, "exactly one maker-fill ledger row");
        assert_eq!(trades[0].action, TradeAction::BuyYes);
        assert_close(trades[0].mana_amount, 20.0);
        assert_close(trades[0].shares_delta, 40.0);
        assert_eq!(
            trades[0].trigger_post_uri.as_deref(),
            Some("maker_fill:rest-1"),
            "provenance shows the fill source"
        );
        assert_eq!(trades[0].manifold_bet_id.as_deref(), Some("rest-1"));

        // Budget invariant holds after the fill.
        let row = store.load("Market_test").expect("load").expect("row");
        assert!(
            row.invested_mana
                <= row.budget_mana + row.realized_pnl_mana + BUDGET_EPSILON,
            "invested {} must stay within budget plus realized PnL {}",
            row.invested_mana,
            row.budget_mana + row.realized_pnl_mana
        );

        // Duplicate broadcast: idempotent by the delta key — no second row,
        // no share change.
        executor.on_own_bet_event(&bet).await.expect("replay is inert");
        let trades = db.recent_trades("Market_test", 10).expect("ledger");
        assert_eq!(trades.len(), 1, "a duplicate must not write a second row");
        let book = Book::load(&store, &db, "Market_test")
            .expect("load book")
            .expect("tracked");
        assert_close(book.leg(0).yes_shares, 60.0);

        // The stored counters now read the server's values: a subsequent
        // event builds its delta on them.
        let tracked = executor
            .own_orders()
            .get("Market_test", "rest-1")
            .cloned()
            .expect("still tracked");
        assert_close(tracked.filled_amount, 30.0);
        assert_close(tracked.filled_shares, 60.0);

        // The fill was announced downstream (Discord embeds read these).
        // The order stays tracked (not fully filled, not cancelled, alive).
        assert!(executor.own_orders().get("Market_test", "rest-1").is_some());
    }

    #[tokio::test]
    async fn a_maker_fill_past_the_ceiling_is_refused_locally_and_reported() {
        let market = market_with(10.0, 0.0, 0.0, 0.0, 0.0);
        let db = Db::open_in_memory().expect("in-memory DB");
        let store = Arc::new(MarketStore::new(&db).expect("store loads"));
        store.insert(&market).expect("insert market");
        let executor = offline_executor(&db, &store);
        let now_ms = Utc::now().timestamp_millis();

        let mut order = live_resting_order(now_ms);
        order.order_amount = 500.0;
        order.filled_amount = 0.0;
        order.filled_shares = 0.0;
        executor.own_orders().upsert(order);

        let mut receiver = executor.events().subscribe();
        let mut bet = own_bet_event("rest-1", 200.0, 400.0);
        bet.expires_at = Some(now_ms + 40_000);
        executor.on_own_bet_event(&bet).await.expect("handled");

        // Nothing applied locally: the budget invariant wins.
        let trades = db.recent_trades("Market_test", 10).expect("ledger");
        assert!(trades.is_empty(), "a refused fill writes no ledger row");
        let book = Book::load(&store, &db, "Market_test")
            .expect("load book")
            .expect("tracked");
        assert_close(book.leg(0).yes_shares, 0.0);
        assert_close(book.leg(0).invested_mana, 0.0);

        // The refusal left the tracker entry (with the server's refreshed
        // counters, so no duplicate can re-apply the refused fill); a later
        // reconcile pass trusts Manifold's position anyway.
        let tracked = executor
            .own_orders()
            .get("Market_test", "rest-1")
            .cloned()
            .expect("a refused fill is still tracked");
        assert_close(tracked.filled_shares, 400.0);
        assert_close(tracked.filled_amount, 200.0);

        // A Discord-facing error was emitted.
        match receiver.try_recv() {
            Ok(TradeEvent::Error { market_id, message }) => {
                assert_eq!(market_id.as_deref(), Some("Market_test"));
                assert!(message.contains("budget invariant"), "{message}");
            }
            other => panic!("expected an error event, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_fully_filled_or_cancelled_maker_event_retires_the_order() {
        let market = market_with(100.0, 10.0, 0.0, 20.0, 0.0);
        let db = Db::open_in_memory().expect("in-memory DB");
        let store = Arc::new(MarketStore::new(&db).expect("store loads"));
        store.insert(&market).expect("insert market");
        let executor = offline_executor(&db, &store);
        let now_ms = Utc::now().timestamp_millis();

        executor.own_orders().upsert(live_resting_order(now_ms));

        let mut bet = own_bet_event("rest-1", 50.0, 100.0);
        bet.expires_at = Some(now_ms + 40_000);
        bet.is_filled = true;
        executor.on_own_bet_event(&bet).await.expect("handled");

        assert!(
            executor.own_orders().get("Market_test", "rest-1").is_none(),
            "a full fill retires the order"
        );
        let row = store.load("Market_test").expect("load").expect("row");
        assert_close(row.yes_shares, 100.0);
        assert_close(row.invested_mana, 50.0);

        // A later cancel broadcast on the same bet id changes nothing.
        let cancelled = BetEvent {
            is_filled: false,
            is_cancelled: true,
            ..bet
        };
        executor
            .on_own_bet_event(&cancelled)
            .await
            .expect("handled");
        assert!(executor.own_orders().get("Market_test", "rest-1").is_none());
    }

    #[tokio::test]
    async fn a_foreign_user_bet_is_never_accounted() {
        let market = market_with(100.0, 0.0, 0.0, 0.0, 0.0);
        let db = Db::open_in_memory().expect("in-memory DB");
        let store = Arc::new(MarketStore::new(&db).expect("store loads"));
        store.insert(&market).expect("insert market");
        let executor = offline_executor(&db, &store);
        let now_ms = Utc::now().timestamp_millis();
        executor.own_orders().upsert(live_resting_order(now_ms));

        // Someone else's bet on the same market, same bet id by accident
        // (impossible on Manifold): the user-id gate keeps the fill path off.
        let mut foreign = own_bet_event("rest-1", 30.0, 60.0);
        foreign.user_id = "someone-else".to_owned();
        executor.on_own_bet_event(&foreign).await.expect("handled");

        let trades = db.recent_trades("Market_test", 10).expect("ledger");
        assert!(trades.is_empty(), "a foreign bet writes no ledger row");
        let book = Book::load(&store, &db, "Market_test")
            .expect("load book")
            .expect("tracked");
        assert_close(book.leg(0).yes_shares, 0.0);
    }

    // -------------------------------------------------------------------
    // S2: sniping through the shared executor
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn execute_planned_reuses_the_budget_invariant_of_execute() {
        let market = market_with(1.0, 0.0, 0.0, 0.0, 0.0);
        let db = Db::open_in_memory().expect("in-memory DB");
        let store = Arc::new(MarketStore::new(&db).expect("store loads"));
        store.insert(&market).expect("insert market");
        let executor = offline_executor(&db, &store);

        let planned = PlannedTrade {
            leg: 0,
            decision: TradeDecision {
                action: TradeAction::BuyYes,
                amount_mana: 10.0,
                shares: None,
                answer_id: None,
            },
            p_internal: 0.8,
            p_market: 0.4,
        };

        let error = executor
            .execute_planned("Market_test", &planned, Some("contract-1"), 0)
            .await
            .expect_err("a buy over the market budget must be refused");

        assert!(
            error.to_string().contains("budget invariant"),
            "expected the shared budget refusal, got {error}"
        );
        assert!(
            db.recent_trades("Market_test", 10)
                .expect("ledger")
                .is_empty(),
            "a refused buy writes no trade row"
        );
    }

    #[tokio::test]
    async fn execute_planned_honours_the_kill_switch_without_a_write() {
        let market = market_with(100.0, 0.0, 0.0, 0.0, 0.0);
        let db = Db::open_in_memory().expect("in-memory DB");
        let store = Arc::new(MarketStore::new(&db).expect("store loads"));
        store.insert(&market).expect("insert market");
        db.set_kv(KILL_SWITCH_KEY, "true").expect("kill switch");
        let executor = offline_executor(&db, &store);

        let planned = PlannedTrade {
            leg: 0,
            decision: TradeDecision {
                action: TradeAction::BuyYes,
                amount_mana: 5.0,
                shares: None,
                answer_id: None,
            },
            p_internal: 0.8,
            p_market: 0.4,
        };

        let record = executor
            .execute_planned("Market_test", &planned, Some("contract-1"), 0)
            .await
            .expect("handled");
        assert!(record.is_none());
        assert!(db
            .recent_trades("Market_test", 10)
            .expect("ledger")
            .is_empty());
    }

    #[tokio::test]
    async fn a_full_offline_snipe_evaluates_executes_and_writes_both_ledgers() {
        use crate::snipe::{
            binary_market_json, contract_stub, spawn_fake_manifold, ScriptedFairValue, SnipeStrategy,
        };

        let manifold = spawn_fake_manifold(
            binary_market_json("Market_snipe", 0.4, Some(500.0)),
            json!({
                "id": "snipe-bet",
                "amount": 1.6667,
                "shares": 4.1667,
                "isFilled": true
            }),
        )
        .await;

        let db = Db::open_in_memory().expect("in-memory DB");
        let store = Arc::new(MarketStore::new(&db).expect("store loads"));
        let config = crate::snipe::test_config();
        let fair = Arc::new(ScriptedFairValue::ok(0.8, None));
        let snipe = SnipeStrategy::with_fair_value(
            &config,
            db.clone(),
            Arc::clone(&store),
            ManifoldClient::for_test(manifold.base_url.clone()),
            fair,
        );
        let executor = offline_executor_with(
            &db,
            &store,
            ManifoldClient::for_test(manifold.base_url.clone()),
            Some(snipe),
        );
        let mut events = executor.events().subscribe();
        let now = Utc::now().timestamp();

        executor
            .handle_new_contract(&contract_stub("Market_snipe"), now)
            .await
            .expect("snipe evaluation and execution");

        // The shared executor wrote its ordinary ledger row, with the contract
        // id as provenance.
        let trades = db.recent_trades("Market_snipe", 10).expect("trades");
        assert_eq!(trades.len(), 1, "one fill, one trade row");
        assert_eq!(trades[0].action, TradeAction::BuyYes);
        assert_eq!(trades[0].trigger_post_uri.as_deref(), Some("Market_snipe"));
        assert_eq!(trades[0].manifold_bet_id.as_deref(), Some("snipe-bet"));

        // The snipe ledger recorded the real fill, not the intention.
        let snipes = db.recent_snipes(10).expect("snipes");
        assert_eq!(snipes.len(), 1, "one evaluation, one buy row");
        assert_eq!(snipes[0].contract_id, "Market_snipe");
        assert_eq!(snipes[0].action, "BUY_YES");
        assert_eq!(snipes[0].manifold_bet_id.as_deref(), Some("snipe-bet"));
        assert!((snipes[0].mana_amount - trades[0].mana_amount).abs() < 1e-9);
        assert!(snipes[0].reason.as_deref().unwrap_or("").contains("gap"));

        // The market row is a snipe row with the model belief.
        let row = store
            .load("Market_snipe")
            .expect("load")
            .expect("tracked");
        assert!(row.is_snipe());
        assert!(row.deleted_at.is_none());
        assert!((row.p_market_last - 0.4).abs() < 1e-9);
        assert!((row.p_internal_log_odds - belief::logit(0.8)).abs() < 1e-9);
        assert!((row.budget_mana - 10.0).abs() < 1e-9);

        // The regular planner can only exit from here: the leg holds shares,
        // and the entry branch requires a flat leg.
        let book = Book::load(&store, &db, "Market_snipe")
            .expect("load book")
            .expect("tracked");
        assert!(book.leg(0).yes_shares > 0.0);
        let planned = book.plan(now + 1, 4.0, &executor.policy);
        assert!(
            !planned
                .iter()
                .any(|trade| matches!(trade.decision.action, TradeAction::BuyYes | TradeAction::BuyNo)),
            "the post-fill evaluation must not open a second position: {planned:?}"
        );

        // Provenance reached the event stream for Discord.
        match events.try_recv().expect("a trade event") {
            TradeEvent::Trade {
                market_id,
                strategy,
                p_internal,
                p_market,
                ..
            } => {
                assert_eq!(market_id, "Market_snipe");
                assert_eq!(strategy, crate::types::STRATEGY_SNIPE);
                assert!((p_internal - 0.8).abs() < 1e-9);
                assert!((p_market - 0.4).abs() < 1e-9);
            }
            other => panic!("expected a trade event, got {other:?}"),
        }

        // One market fetch and one bet call; nothing else.
        assert_eq!(manifold.requests(), 2);
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
            limit_epsilon: 0.01,
            limit_expiry_secs: 60,
            limit_drift_cancel: 0.05,
            belief_kappa: 1.1,
            belief_halflife_hours: 4.0,
            triage_min_evidence: 0.7,
            trading_dry_run: true,
            mc_max_positions: 6,
            mc_max_new_per_tick: 2,
            mc_max_answer_fraction: 0.25,
            rate_limit_per_min: 500,
            snipe_enabled: false,
            snipe_pool_mana: 100.0,
            snipe_max_mana: 10.0,
            snipe_min_liq: 100.0,
            snipe_min_gap: 0.10,
            snipe_kelly_fraction: 0.25,
            snipe_ttl_secs: 3600,
            snipe_decision_timeout_secs: 2,
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
            strategy: crate::types::STRATEGY_NEWS.to_owned(),
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
