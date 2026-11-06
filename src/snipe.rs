//! S2: new-market fair-value sniping (`docs/ALGO.md` §S2).
//!
//! `global/new-contract` pushes every newly created market to us within
//! seconds. A creator sets the opening probability by hand, so a new market is
//! often the most mispriced object on the platform. This module turns one such
//! broadcast into one fair-value decision and, when the gap is large enough,
//! one [`SnipeOrder`] for the shared [`crate::trade::TradeExecutor`] to execute.
//!
//! # Why a separate strategy instead of the news pipeline
//!
//! A snipe has no post, no Sieve rule, and no tracking history: the market did
//! not exist a moment ago. The decision is therefore one `fair_prob` call on
//! the market's own text ([`crate::triage::TriageClient::fair_prob`]), not the
//! four-question news triage. Everything after the decision — budget
//! invariants, the fair-capped sweep, the expiring rest, the ledger, and
//! Discord — is the existing executor, unchanged.
//!
//! # Pool, not per-market budgets
//!
//! Snipe markets must not consume an operator's `/track` budget, so they draw
//! from one global [`crate::config::Config::snipe_pool_mana`]. Each snipe row
//! still gets a *per-market* budget of `min(SNIPE_MAX_MANA, pool remaining)`:
//! the existing `budget + realized_pnl − invested` invariant then holds
//! unchanged for the row, while the pool is the sum over every active snipe
//! row ([`Db::snipe_pool_invested`]).
//!
//! # Ledger
//!
//! Every gate writes one `snipe_ledger` row with action `SKIP` and a stable
//! reason, including the observed `p_initial`/`p_model`/`gap` when they are
//! known. The buy row is written *after* the executor ran, so its
//! `mana_amount` and `manifold_bet_id` are the real fill, not the intention.
//! This is the tuning surface for `SNIPE_MIN_GAP`, `SNIPE_MIN_LIQ`, and the
//! Kelly fraction.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
#[cfg(test)]
use chrono::Utc;

use crate::book::{kelly_target_mana, Book, PlannedTrade, TradeParams};
use crate::config::Config;
use crate::db::Db;
use crate::error::{anyhow, AppResult, Context};
use crate::manifold_ws::ContractStub;
use crate::market_store::MarketStore;
use crate::rate::{Priority, RateLimiter, WRITE_COST_TOKENS};
use crate::strategy_market::{insert_market_row, resolution_criteria, MarketRow};
use crate::trade::{millis_to_seconds, ManifoldClient};
use crate::triage::{FairProb, FairProbInput, TriageClient};
use crate::types::{
    MarketId, SnipeRecord, TradeAction, TradeDecision, TradeRecord, STRATEGY_SNIPE,
};

/// Types whose outcome model this strategy can price.
const SUPPORTED_OUTCOME_TYPES: [&str; 2] = ["BINARY", "MULTIPLE_CHOICE"];

/// Mechanisms that price shares through a CPMM family and are safe to model.
///
/// `dpm-2` (the old automated-market-maker family) and anything unknown are
/// skipped: their fill semantics and price impact do not match the CPMM
/// assumptions the fair-value gap relies on.
const CPMM_MECHANISMS: [&str; 3] = ["cpmm-1", "cpmm-multi-1", "cpmm-multi-2"];

/// One fair-value call, abstracted so the strategy's gates can be tested
/// without the decisions transport.
///
/// The production implementation is [`TriageClient`]. The trait exists only as
/// a test seam: the snipe path never adds a second HTTP client or a second
/// endpoint (the transport invariant in `AGENTS.md`).
#[async_trait]
pub trait FairValue: Send + Sync {
    /// Probability that the market resolves YES, or that the named answer wins
    /// on a multiple-choice market.
    async fn fair_prob(&self, input: &FairProbInput<'_>, timeout: Duration) -> AppResult<FairProb>;
}

#[async_trait]
impl FairValue for TriageClient {
    async fn fair_prob(&self, input: &FairProbInput<'_>, timeout: Duration) -> AppResult<FairProb> {
        self.fair_prob_with_timeout(input, timeout).await
    }
}

/// What the executor should do with one evaluated new market.
#[derive(Clone, Debug)]
pub struct SnipeOrder {
    /// Manifold contract ID; also the row just inserted into `tracked_markets`.
    pub market_id: MarketId,
    /// The exact plan the shared executor runs (budget invariant, fair cap,
    /// expiring rest, ledger, Discord).
    pub planned: PlannedTrade,
    /// Provenance carried into `trade_ledger.trigger_post_uri`: the contract
    /// ID that triggered this snipe.
    pub trigger: String,
    pub question: String,
    /// The market's opening price (binary), or the chosen answer's price
    /// (multiple choice).
    pub p_initial: f64,
    /// The model's fair probability.
    pub p_model: f64,
    /// `p_model − p_initial`.
    pub gap: f64,
    /// Chosen answer text on a multiple-choice market.
    pub answer: Option<String>,
    /// Human-readable rationale, copied into the snipe ledger.
    pub reason: String,
    /// Decision-call cost in USD, when reported.
    pub cost_usd: Option<f64>,
    /// Evaluation time (Unix seconds), used for the ledger row.
    pub created_at: i64,
}

/// The S2 new-market sniper.
///
/// Holds a Manifold client (cheaply cloneable: base URL, reqwest pool, key, and
/// bot id), the process triage client wrapped in the [`FairValue`] seam, the
/// database, and the market store. The rate bucket is passed into
/// [`SnipeStrategy::evaluate`] because the executor owns the one global bucket
/// and the market fetch must spend a Discovery-class token from it.
pub struct SnipeStrategy {
    enabled: bool,
    pool_mana: f64,
    max_mana: f64,
    min_liq: f64,
    min_gap: f64,
    kelly_fraction: f64,
    ttl_secs: i64,
    decision_timeout: Duration,
    min_trade_size: f64,
    exit_edge_threshold: f64,
    db: Db,
    store: Arc<MarketStore>,
    client: ManifoldClient,
    fair: Arc<dyn FairValue>,
}

impl SnipeStrategy {
    /// Builds the strategy from process configuration.
    ///
    /// A second `TriageClient` instance is built here rather than cloned from
    /// the pipeline: the pipeline keeps its client private, the type carries no
    /// per-instance state (one shared reqwest pool per process either way), and
    /// "one shared client" in the architecture notes means one decisions
    /// *transport*, not one handle. The alternative — exposing a pipeline
    /// handle — would couple the trading layer to the news pipeline for no
    /// behavioural gain.
    pub fn new(
        config: &Config,
        db: Db,
        store: Arc<MarketStore>,
        client: ManifoldClient,
    ) -> AppResult<Self> {
        let fair: Arc<dyn FairValue> = Arc::new(TriageClient::new(config)?);
        Ok(Self::with_fair_value(config, db, store, client, fair))
    }

    /// [`SnipeStrategy::new`] with an injected [`FairValue`] implementation.
    ///
    /// `pub(crate)` so the executor's integration tests can script the decision
    /// call without an OpenRouter key.
    pub(crate) fn with_fair_value(
        config: &Config,
        db: Db,
        store: Arc<MarketStore>,
        client: ManifoldClient,
        fair: Arc<dyn FairValue>,
    ) -> Self {
        Self {
            enabled: config.snipe_enabled,
            pool_mana: config.snipe_pool_mana,
            max_mana: config.snipe_max_mana,
            min_liq: config.snipe_min_liq,
            min_gap: config.snipe_min_gap,
            kelly_fraction: config.snipe_kelly_fraction,
            ttl_secs: config.snipe_ttl_secs as i64,
            decision_timeout: Duration::from_secs(config.snipe_decision_timeout_secs as u64),
            min_trade_size: config.min_trade_size,
            exit_edge_threshold: config.exit_edge_threshold,
            db,
            store,
            client,
            fair,
        }
    }

    /// Evaluates one newly created contract.
    ///
    /// Returns `Some` only for a buy the shared executor should run: the market
    /// row has already been inserted (so the order can load its book), and the
    /// snipe ledger holds the buy decision. Every gate writes a `SKIP` row and
    /// returns `None`.
    ///
    /// # Gate order
    ///
    /// 1. outcome type and mechanism (no REST);
    /// 2. `SNIPE_ENABLED`;
    /// 3. contract age against `SNIPE_TTL_SECS` (no REST);
    /// 4. duplicate row (`already_tracked`, no REST);
    /// 5. one Discovery-class token, then `GET /v0/market/{id}`;
    /// 6. resolved / unsupported shape from the snapshot;
    /// 7. pool remaining against `MIN_TRADE_SIZE`;
    /// 8. `totalLiquidity` against `SNIPE_MIN_LIQ`;
    /// 9. one `fair_prob` call with the snipe deadline;
    /// 10. answer selection (multiple choice) and the gap gate;
    /// 11. Kelly sizing, capped by the pool and `SNIPE_MAX_MANA`;
    /// 12. insert the market row and answers, resolve the book leg, return.
    ///
    /// The order matters for spend: every free check runs before the REST fetch
    /// and every non-decision check before the model call.
    pub async fn evaluate(
        &self,
        contract: &ContractStub,
        now: i64,
        rate: &RateLimiter,
    ) -> AppResult<Option<SnipeOrder>> {
        let mut draft = SnipeDraft {
            contract_id: contract.id.clone(),
            question: contract.question.clone().unwrap_or_default(),
            p_initial: contract.initial_prob,
            p_model: None,
            gap: None,
            answer: None,
            cost_usd: None,
            created_at: now,
        };

        // (a) Shape gate: types and mechanisms this strategy cannot model.
        if let Some(outcome_type) = contract
            .outcome_type
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            if !SUPPORTED_OUTCOME_TYPES.contains(&outcome_type) {
                return self.skip(&draft, "unsupported_outcome_type");
            }
        }
        if let Some(mechanism) = contract
            .mechanism
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            if !CPMM_MECHANISMS.contains(&mechanism) {
                return self.skip(&draft, "unsupported_mechanism");
            }
        }

        // (b) The executor normally does not construct the strategy when
        // sniping is off; this keeps a directly built strategy inert too.
        if !self.enabled {
            return self.skip(&draft, "disabled");
        }

        // (b2) TTL: a contract older than SNIPE_TTL_SECS is no longer a new
        // market. Checked before the REST fetch so a lagging broadcast costs
        // nothing.
        if let Some(created_ms) = contract.created_time {
            let age = now.saturating_sub(millis_to_seconds(created_ms));
            if age > self.ttl_secs {
                return self.skip(&draft, "ttl_expired");
            }
        }

        // (b3) Duplicate rows: a restart, a replayed fixture, or a market the
        // operator already tracks must not be re-inserted (the primary key
        // would fail) or bought twice. `load` includes soft-deleted rows, which
        // would still collide on insert.
        if self.store.load(&contract.id)?.is_some() {
            return self.skip(&draft, "already_tracked");
        }

        // (c) One REST fetch, charged to the global bucket as discovery work.
        if rate
            .try_acquire(Priority::Discovery, WRITE_COST_TOKENS)
            .is_err()
        {
            return self.skip(&draft, "rate_limited");
        }
        let snapshot = self
            .client
            .market(&contract.id)
            .await
            .with_context(|| format!("fetching snipe market {}", contract.id))?;

        if snapshot.is_resolved {
            return self.skip(&draft, "resolved");
        }
        if !SUPPORTED_OUTCOME_TYPES.contains(&snapshot.outcome_type.as_str()) {
            return self.skip(&draft, "unsupported_outcome_type");
        }
        if !snapshot.mechanism.trim().is_empty()
            && !CPMM_MECHANISMS.contains(&snapshot.mechanism.as_str())
        {
            return self.skip(&draft, "unsupported_mechanism");
        }

        let is_multiple_choice = snapshot.outcome_type == "MULTIPLE_CHOICE";
        let snapshot_answers = snapshot.answers.as_deref().unwrap_or(&[]);
        if is_multiple_choice && snapshot_answers.is_empty() {
            return self.skip(&draft, "no_answers");
        }

        // The market-level opening probability: `probability` on a binary
        // market, `initialProb` otherwise. On a multiple-choice market this is
        // only a fallback input for the fair-value state; the gap uses the
        // chosen answer's own price (below).
        let market_initial = snapshot
            .probability
            .or(snapshot.initial_prob)
            .or(contract.initial_prob);
        draft.p_initial = market_initial;
        if !is_multiple_choice && market_initial.is_none() {
            return self.skip(&draft, "no_probability");
        }

        // (d) Pool gate. `snipe_pool_invested` is the sum over active snipe
        // rows of `invested − realized_pnl`, so profits flow back into the
        // pool automatically.
        let pool_remaining = self.pool_mana - self.db.snipe_pool_invested()?;
        if pool_remaining < self.min_trade_size {
            return self.skip(&draft, "pool_exhausted");
        }

        // (e) Liquidity gate on the value the snapshot already carries.
        let Some(total_liquidity) = snapshot.total_liquidity else {
            return self.skip(&draft, "liquidity_unknown");
        };
        if total_liquidity < self.min_liq {
            return self.skip(&draft, "low_liquidity");
        }

        // (f) One fair-value decision call, on the snipe deadline.
        let answer_texts: Vec<String> = snapshot_answers
            .iter()
            .map(|answer| answer.text.clone())
            .collect();
        let fair_input = FairProbInput {
            question: &snapshot.question,
            resolution_criteria: resolution_criteria(&snapshot),
            close_time: snapshot.close_time_ms.map(millis_to_seconds),
            initial_prob: market_initial.unwrap_or(0.5).clamp(0.0, 1.0),
            creator_id: contract.creator_id.as_deref(),
            answers: &answer_texts,
        };
        let fair = match self
            .fair
            .fair_prob(&fair_input, self.decision_timeout)
            .await
        {
            Ok(fair) => fair,
            Err(error) => {
                tracing::warn!(
                    contract_id = %contract.id,
                    error = %error,
                    "snipe fair-value call failed; the market is recorded as a skip"
                );
                return self.skip(&draft, &format!("decision_failed: {error}"));
            }
        };
        draft.p_model = Some(fair.p);
        draft.cost_usd = fair.cost_usd;

        // (g) Chosen answer and its price; gap gate.
        let (answer_id, answer_text, p_initial) = if is_multiple_choice {
            let Some(chosen) = fair.answer.as_deref() else {
                return self.skip(&draft, "no_answer_selected");
            };
            let Some(answer) = snapshot_answers
                .iter()
                .find(|candidate| candidate.text == chosen)
            else {
                return self.skip(&draft, "answer_not_in_market");
            };
            let Some(price) = answer.probability else {
                return self.skip(&draft, "answer_price_missing");
            };
            draft.answer = Some(answer.text.clone());
            draft.p_initial = Some(price);
            (Some(answer.id.clone()), Some(answer.text.clone()), price)
        } else {
            let Some(price) = market_initial else {
                return self.skip(&draft, "no_probability");
            };
            (None, None, price)
        };

        let gap = fair.p - p_initial;
        draft.gap = Some(gap);
        if gap.abs() < self.min_gap {
            return self.skip(&draft, "small_gap");
        }

        let action = if gap > 0.0 {
            TradeAction::BuyYes
        } else {
            TradeAction::BuyNo
        };
        // A sum-to-one multiple-choice NO is a basket over the whole field and
        // the planner never buys one; sniping must not open a position shape
        // the regular exit path cannot manage.
        if is_multiple_choice && snapshot.should_answers_sum_to_one && action == TradeAction::BuyNo
        {
            return self.skip(&draft, "sum_to_one_no_leg");
        }

        // (h) Fractional Kelly, on the same formula and cap as the planner. The
        // pool remaining and SNIPE_MAX_MANA bound the *budget* the formula sees,
        // so the per-market invariant holds after the row is inserted.
        let cap_budget = self.max_mana.min(pool_remaining);
        let params = TradeParams {
            kelly_fraction: self.kelly_fraction,
            entry_edge_threshold: self.min_gap,
            exit_edge_threshold: self.exit_edge_threshold,
            min_trade_size: self.min_trade_size,
        };
        let size = kelly_target_mana(gap, p_initial, cap_budget, params).min(cap_budget);
        if size < self.min_trade_size {
            return self.skip(&draft, "below_min_size");
        }

        // (i) Insert the snipe market (budget = the per-market cap, not the
        // actual size) and its answers, through the shared row builder.
        insert_market_row(
            &self.store,
            &self.db,
            MarketRow {
                snapshot: &snapshot,
                fallback_slug: contract.slug.as_deref().unwrap_or(""),
                p_market: p_initial,
                p_internal: fair.p,
                budget_mana: cap_budget,
                strategy: STRATEGY_SNIPE,
                now,
            },
        )?;

        let book = Book::load(&self.store, &self.db, &snapshot.id)?
            .ok_or_else(|| anyhow!("snipe market {} was not cached after insert", snapshot.id))?;
        let leg = book
            .legs()
            .iter()
            .position(|leg| leg.answer_id == answer_id)
            .ok_or_else(|| {
                anyhow!(
                    "snipe answer {:?} has no book leg in market {}",
                    answer_id,
                    snapshot.id
                )
            })?;

        let reason = format!(
            "gap {:+.3} (p_model {:.3} vs p_initial {:.3}); kelly target {:.3}",
            gap, fair.p, p_initial, size
        );

        // (j) Hand the plan to the shared executor.
        Ok(Some(SnipeOrder {
            market_id: snapshot.id.clone(),
            planned: PlannedTrade {
                leg,
                decision: TradeDecision {
                    action,
                    amount_mana: size,
                    shares: None,
                    answer_id,
                },
                p_internal: fair.p,
                p_market: p_initial,
            },
            trigger: contract.id.clone(),
            question: snapshot.question.clone(),
            p_initial,
            p_model: fair.p,
            gap,
            answer: answer_text,
            reason,
            cost_usd: fair.cost_usd,
            created_at: now,
        }))
    }

    /// Writes the ledger row for an order the executor filled.
    ///
    /// The row records the *actual* mana and bet id from `trade_ledger`, so the
    /// snipe ledger is a decision log that can be joined to the fill, not a
    /// pre-trade guess.
    pub fn record_filled(&self, order: &SnipeOrder, trade: &TradeRecord) -> AppResult<()> {
        self.db.insert_snipe(&SnipeRecord {
            id: 0,
            contract_id: order.market_id.clone(),
            question: order.question.clone(),
            p_initial: Some(order.p_initial),
            p_model: Some(order.p_model),
            gap: Some(order.gap),
            answer: order.answer.clone(),
            action: order.planned.decision.action.as_str().to_owned(),
            reason: Some(order.reason.clone()),
            mana_amount: trade.mana_amount,
            limit_prob: None,
            manifold_bet_id: trade.manifold_bet_id.clone(),
            cost_usd: order.cost_usd,
            created_at: order.created_at,
        })?;
        Ok(())
    }

    /// Writes a `SKIP` row for an order the executor did not fill.
    ///
    /// The reason separates a control-plane refusal (kill switch, budget, rate
    /// floor) from a wire outcome (nothing filled at the cap) without inventing
    /// a second trade path.
    pub fn record_unfilled(&self, order: &SnipeOrder, reason: &str) -> AppResult<()> {
        self.db.insert_snipe(&SnipeRecord {
            id: 0,
            contract_id: order.market_id.clone(),
            question: order.question.clone(),
            p_initial: Some(order.p_initial),
            p_model: Some(order.p_model),
            gap: Some(order.gap),
            answer: order.answer.clone(),
            action: "SKIP".to_owned(),
            reason: Some(format!("unfilled: {reason}")),
            mana_amount: 0.0,
            limit_prob: None,
            manifold_bet_id: None,
            cost_usd: order.cost_usd,
            created_at: order.created_at,
        })?;
        Ok(())
    }

    /// Records one skip (with whatever model output is already known).
    fn skip(&self, draft: &SnipeDraft, reason: &str) -> AppResult<Option<SnipeOrder>> {
        self.db.insert_snipe(&SnipeRecord {
            id: 0,
            contract_id: draft.contract_id.clone(),
            question: draft.question.clone(),
            p_initial: draft.p_initial,
            p_model: draft.p_model,
            gap: draft.gap,
            answer: draft.answer.clone(),
            action: "SKIP".to_owned(),
            reason: Some(reason.to_owned()),
            mana_amount: 0.0,
            limit_prob: None,
            manifold_bet_id: None,
            cost_usd: draft.cost_usd,
            created_at: draft.created_at,
        })?;
        tracing::debug!(
            contract_id = %draft.contract_id,
            reason,
            p_initial = ?draft.p_initial,
            p_model = ?draft.p_model,
            gap = ?draft.gap,
            "snipe skipped"
        );
        Ok(None)
    }
}

/// Mutable evaluation context, so each gate records what it already knows.
struct SnipeDraft {
    contract_id: String,
    question: String,
    p_initial: Option<f64>,
    p_model: Option<f64>,
    gap: Option<f64>,
    answer: Option<String>,
    cost_usd: Option<f64>,
    created_at: i64,
}

// -----------------------------------------------------------------------------
// Test fakes and fixtures
// -----------------------------------------------------------------------------

#[cfg(test)]
pub(crate) struct ScriptedFairValue {
    result: std::sync::Mutex<Result<FairProb, String>>,
    calls: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
impl ScriptedFairValue {
    pub(crate) fn ok(p: f64, answer: Option<&str>) -> Self {
        Self::new(Ok(FairProb {
            p,
            answer: answer.map(str::to_owned),
            cost_usd: Some(0.0001),
        }))
    }

    pub(crate) fn err(message: &str) -> Self {
        Self::new(Err(message.to_owned()))
    }

    fn new(result: Result<FairProb, String>) -> Self {
        Self {
            result: std::sync::Mutex::new(result),
            calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub(crate) fn calls(&self) -> usize {
        self.calls.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(test)]
#[async_trait]
impl FairValue for ScriptedFairValue {
    async fn fair_prob(
        &self,
        _input: &FairProbInput<'_>,
        _timeout: Duration,
    ) -> AppResult<FairProb> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        match &*self.result.lock().expect("script lock") {
            Ok(fair) => Ok(fair.clone()),
            Err(message) => Err(anyhow!(message.clone())),
        }
    }
}

/// A loopback Manifold stub: one market payload for every `GET /v0/market/…`
/// and one bet payload for every `POST /v0/bet`.
#[cfg(test)]
pub(crate) struct FakeManifold {
    pub(crate) base_url: String,
    requests: Arc<std::sync::atomic::AtomicUsize>,
}

#[cfg(test)]
impl FakeManifold {
    pub(crate) fn requests(&self) -> usize {
        self.requests.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(test)]
pub(crate) async fn spawn_fake_manifold(
    market: serde_json::Value,
    bet: serde_json::Value,
) -> FakeManifold {
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the fake Manifold listener");
    let address = listener.local_addr().expect("local address");
    let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = Arc::clone(&requests);

    tokio::spawn(async move {
        loop {
            let (mut stream, _) = match listener.accept().await {
                Ok(pair) => pair,
                Err(_) => break,
            };
            let market = market.clone();
            let bet = bet.clone();
            let counter = Arc::clone(&counter);
            tokio::spawn(async move {
                let request = read_request(&mut stream).await;
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let body = if request.starts_with("POST /v0/bet") {
                    bet.to_string()
                } else if request.starts_with("GET /v0/market/") {
                    market.to_string()
                } else {
                    "{}".to_owned()
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.flush().await;
            });
        }
    });

    FakeManifold {
        base_url: format!("http://{address}"),
        requests,
    }
}

/// Reads one HTTP/1.1 request fully (headers plus a declared body).
#[cfg(test)]
async fn read_request(stream: &mut tokio::net::TcpStream) -> String {
    use tokio::io::AsyncReadExt;

    let mut buffer = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        let read = match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(end) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&buffer[..end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    if name.eq_ignore_ascii_case("content-length") {
                        value.trim().parse::<usize>().ok()
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            if buffer.len() >= end + 4 + content_length {
                break;
            }
        }
    }
    String::from_utf8_lossy(&buffer).into_owned()
}

/// Configuration used by the S2 tests: sniping enabled with small, exact caps.
#[cfg(test)]
pub(crate) fn test_config() -> Config {
    Config {
        jetstream_url: "wss://jetstream.example/subscribe".to_owned(),
        cloudflare_account_id: None,
        triage_model: "openrouter-decisions/baseten/joey-1".to_owned(),
        onboarding_model: "openrouter/anthropic/claude".to_owned(),
        firecrawl_api_key: "fc-".to_owned(),
        manifold_api_key: "unused".to_owned(),
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
        trading_dry_run: false,
        mc_max_positions: 6,
        mc_max_new_per_tick: 2,
        mc_max_answer_fraction: 0.25,
        rate_limit_per_min: 500,
        snipe_enabled: true,
        snipe_pool_mana: 100.0,
        snipe_max_mana: 10.0,
        snipe_min_liq: 100.0,
        snipe_min_gap: 0.10,
        snipe_kelly_fraction: 0.25,
        snipe_ttl_secs: 3600,
        snipe_decision_timeout_secs: 2,
        traders_enabled: true,
        trader_watch_size: 200,
        trader_min_bets: 25,
        trader_refresh_min: 60,
        trader_request_budget: 120,
        copy_enabled: false,
        copy_min_size: 5.0,
        copy_min_informed: 0.60,
        copy_min_edge: 0.05,
        copy_kelly_fraction: 0.125,
        copy_max_mana: 10.0,
        copy_pool_mana: 100.0,
        copy_daily_mana: 30.0,
        copy_max_age_secs: 30,
        copy_decision_timeout_secs: 2,
        db_path: std::path::PathBuf::from("/tmp/opencode/manif0ld-snipe-test.db"),
    }
}

/// One binary market payload as `GET /v0/market/{id}` returns it.
#[cfg(test)]
pub(crate) fn binary_market_json(
    id: &str,
    probability: f64,
    total_liquidity: Option<f64>,
) -> serde_json::Value {
    let mut value = serde_json::json!({
        "id": id,
        "slug": "will-the-thing-happen",
        "url": "https://manifold.markets/Alice/will-the-thing-happen",
        "question": "Will the thing happen?",
        "textDescription": "A market about the thing.",
        "resolutionCriteria": "Resolves YES if the thing happens.",
        "mechanism": "cpmm-1",
        "probability": probability,
        "initialProb": probability,
        "outcomeType": "BINARY",
        "shouldAnswersSumToOne": false,
        "isResolved": false,
        "closeTime": 1_700_000_000_000_i64
    });
    if let Some(liquidity) = total_liquidity {
        value["totalLiquidity"] = serde_json::json!(liquidity);
    }
    value
}

/// One contract stub as `global/new-contract` carries it.
#[cfg(test)]
pub(crate) fn contract_stub(id: &str) -> ContractStub {
    ContractStub {
        id: id.to_owned(),
        slug: Some("will-the-thing-happen".to_owned()),
        question: Some("Will the thing happen?".to_owned()),
        outcome_type: Some("BINARY".to_owned()),
        mechanism: Some("cpmm-1".to_owned()),
        close_time: Some(1_700_000_000_000),
        initial_prob: Some(0.4),
        created_time: Some(Utc::now().timestamp_millis()),
        creator_id: Some("creator-1".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::rate::SystemClock;
    use crate::types::{MarketStatus, TrackedMarket};

    fn in_memory() -> (Db, Arc<MarketStore>) {
        let db = Db::open_in_memory().expect("in-memory DB");
        let store = Arc::new(MarketStore::new(&db).expect("store loads"));
        (db, store)
    }

    fn limiter() -> RateLimiter {
        RateLimiter::per_minute(500, Box::new(SystemClock))
    }

    fn build_strategy(
        config: &Config,
        db: &Db,
        store: &Arc<MarketStore>,
        base_url: &str,
        fair: Arc<dyn FairValue>,
    ) -> SnipeStrategy {
        SnipeStrategy::with_fair_value(
            config,
            db.clone(),
            Arc::clone(store),
            ManifoldClient::for_test(base_url),
            fair,
        )
    }

    fn only_snipe(db: &Db) -> SnipeRecord {
        let rows = db.recent_snipes(10).expect("snipe ledger");
        assert_eq!(rows.len(), 1, "expected one snipe row, got {rows:?}");
        rows.into_iter().next().expect("one row")
    }

    #[tokio::test]
    async fn disabled_strategy_spends_no_rest_and_records_the_skip() {
        let manifold = spawn_fake_manifold(
            binary_market_json("c1", 0.4, Some(500.0)),
            serde_json::json!({}),
        )
        .await;
        let mut config = test_config();
        config.snipe_enabled = false;
        let (db, store) = in_memory();
        let fair = Arc::new(ScriptedFairValue::ok(0.8, None));
        let strategy = build_strategy(&config, &db, &store, &manifold.base_url, fair.clone());

        let order = strategy
            .evaluate(&contract_stub("c1"), Utc::now().timestamp(), &limiter())
            .await
            .expect("evaluation");

        assert!(order.is_none());
        assert_eq!(manifold.requests(), 0, "a disabled strategy must not fetch");
        assert_eq!(fair.calls(), 0);
        let row = only_snipe(&db);
        assert_eq!(row.action, "SKIP");
        assert_eq!(row.reason.as_deref(), Some("disabled"));
    }

    #[tokio::test]
    async fn unsupported_outcome_type_and_mechanism_are_skipped_before_rest() {
        let manifold = spawn_fake_manifold(
            binary_market_json("c1", 0.4, Some(500.0)),
            serde_json::json!({}),
        )
        .await;
        let (db, store) = in_memory();
        let fair = Arc::new(ScriptedFairValue::ok(0.8, None));
        let strategy = build_strategy(&test_config(), &db, &store, &manifold.base_url, fair);

        let mut poll = contract_stub("c1");
        poll.outcome_type = Some("POLL".to_owned());
        assert!(strategy
            .evaluate(&poll, Utc::now().timestamp(), &limiter())
            .await
            .expect("evaluation")
            .is_none());
        assert_eq!(
            only_snipe(&db).reason.as_deref(),
            Some("unsupported_outcome_type")
        );

        let mut dpm = contract_stub("c2");
        dpm.mechanism = Some("dpm-2".to_owned());
        assert!(strategy
            .evaluate(&dpm, Utc::now().timestamp(), &limiter())
            .await
            .expect("evaluation")
            .is_none());
        assert!(db
            .recent_snipes(10)
            .expect("ledger")
            .iter()
            .any(|row| row.contract_id == "c2"
                && row.reason.as_deref() == Some("unsupported_mechanism")));
        assert_eq!(manifold.requests(), 0, "only free gates ran");
    }

    #[tokio::test]
    async fn a_contract_older_than_the_ttl_is_ineligible() {
        let manifold = spawn_fake_manifold(
            binary_market_json("c1", 0.4, Some(500.0)),
            serde_json::json!({}),
        )
        .await;
        let mut config = test_config();
        config.snipe_ttl_secs = 60;
        let (db, store) = in_memory();
        let fair = Arc::new(ScriptedFairValue::ok(0.8, None));
        let strategy = build_strategy(&config, &db, &store, &manifold.base_url, fair);

        let mut contract = contract_stub("c1");
        contract.created_time = Some(Utc::now().timestamp_millis() - 120_000);
        assert!(strategy
            .evaluate(&contract, Utc::now().timestamp(), &limiter())
            .await
            .expect("evaluation")
            .is_none());
        assert_eq!(only_snipe(&db).reason.as_deref(), Some("ttl_expired"));
        assert_eq!(manifold.requests(), 0);
    }

    #[tokio::test]
    async fn an_already_tracked_market_is_skipped_before_rest() {
        let manifold = spawn_fake_manifold(
            binary_market_json("c1", 0.4, Some(500.0)),
            serde_json::json!({}),
        )
        .await;
        let (db, store) = in_memory();
        store
            .insert(&TrackedMarket {
                id: "c1".to_owned(),
                slug: "c1".to_owned(),
                market_url: String::new(),
                question: "Tracked already".to_owned(),
                description: String::new(),
                resolution_criteria: String::new(),
                close_time: None,
                status: MarketStatus::Active,
                budget_mana: 10.0,
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
            })
            .expect("insert existing row");
        let fair = Arc::new(ScriptedFairValue::ok(0.8, None));
        let strategy = build_strategy(&test_config(), &db, &store, &manifold.base_url, fair);

        assert!(strategy
            .evaluate(&contract_stub("c1"), Utc::now().timestamp(), &limiter())
            .await
            .expect("evaluation")
            .is_none());
        assert_eq!(only_snipe(&db).reason.as_deref(), Some("already_tracked"));
        assert_eq!(manifold.requests(), 0);
    }

    #[tokio::test]
    async fn low_liquidity_is_skipped_after_one_fetch_without_a_decision_call() {
        let manifold = spawn_fake_manifold(
            binary_market_json("c1", 0.4, Some(10.0)),
            serde_json::json!({}),
        )
        .await;
        let (db, store) = in_memory();
        let fair = Arc::new(ScriptedFairValue::ok(0.8, None));
        let strategy = build_strategy(
            &test_config(),
            &db,
            &store,
            &manifold.base_url,
            fair.clone(),
        );

        assert!(strategy
            .evaluate(&contract_stub("c1"), Utc::now().timestamp(), &limiter())
            .await
            .expect("evaluation")
            .is_none());
        assert_eq!(only_snipe(&db).reason.as_deref(), Some("low_liquidity"));
        assert_eq!(manifold.requests(), 1);
        assert_eq!(
            fair.calls(),
            0,
            "the liquidity gate precedes the decision call"
        );
    }

    #[tokio::test]
    async fn a_small_gap_is_skipped_and_its_numbers_are_recorded() {
        let manifold = spawn_fake_manifold(
            binary_market_json("c1", 0.48, Some(500.0)),
            serde_json::json!({}),
        )
        .await;
        let (db, store) = in_memory();
        let fair = Arc::new(ScriptedFairValue::ok(0.52, None));
        let strategy = build_strategy(
            &test_config(),
            &db,
            &store,
            &manifold.base_url,
            fair.clone(),
        );

        assert!(strategy
            .evaluate(&contract_stub("c1"), Utc::now().timestamp(), &limiter())
            .await
            .expect("evaluation")
            .is_none());
        assert_eq!(fair.calls(), 1);
        let row = only_snipe(&db);
        assert_eq!(row.reason.as_deref(), Some("small_gap"));
        assert!((row.p_initial.expect("p_initial") - 0.48).abs() < 1e-9);
        assert!((row.p_model.expect("p_model") - 0.52).abs() < 1e-9);
        assert!((row.gap.expect("gap") - 0.04).abs() < 1e-9);
    }

    #[tokio::test]
    async fn the_kelly_target_is_capped_by_the_per_snipe_maximum() {
        let manifold = spawn_fake_manifold(
            binary_market_json("c1", 0.1, Some(500.0)),
            serde_json::json!({}),
        )
        .await;
        let mut config = test_config();
        config.snipe_max_mana = 6.0;
        config.snipe_kelly_fraction = 1.0;
        let (db, store) = in_memory();
        let fair = Arc::new(ScriptedFairValue::ok(0.95, None));
        let strategy = build_strategy(&config, &db, &store, &manifold.base_url, fair);

        let order = strategy
            .evaluate(&contract_stub("c1"), Utc::now().timestamp(), &limiter())
            .await
            .expect("evaluation")
            .expect("a buy");
        // f = 1.0 * 0.85 / (1 - 0.1) = 0.944, clamped to 0.5; budget is 6.
        assert!(
            (order.planned.decision.amount_mana - 3.0).abs() < 1e-9,
            "{order:?}"
        );
        let row = store.load("c1").expect("load").expect("row");
        assert!((row.budget_mana - 6.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn the_pool_remaining_caps_the_budget_and_the_size() {
        let manifold = spawn_fake_manifold(
            binary_market_json("c1", 0.1, Some(500.0)),
            serde_json::json!({}),
        )
        .await;
        let mut config = test_config();
        config.snipe_pool_mana = 4.0;
        config.snipe_max_mana = 10.0;
        config.snipe_kelly_fraction = 1.0;
        let (db, store) = in_memory();
        let fair = Arc::new(ScriptedFairValue::ok(0.95, None));
        let strategy = build_strategy(&config, &db, &store, &manifold.base_url, fair);

        let order = strategy
            .evaluate(&contract_stub("c1"), Utc::now().timestamp(), &limiter())
            .await
            .expect("evaluation")
            .expect("a buy");
        // 0.5 * pool_remaining (4) = 2; the row budget is the whole remaining pool.
        assert!(
            (order.planned.decision.amount_mana - 2.0).abs() < 1e-9,
            "{order:?}"
        );
        let row = store.load("c1").expect("load").expect("row");
        assert!((row.budget_mana - 4.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn gap_sign_selects_yes_or_no() {
        let (db, store) = in_memory();
        let manifold = spawn_fake_manifold(
            binary_market_json("yes1", 0.4, Some(500.0)),
            serde_json::json!({}),
        )
        .await;
        let fair = Arc::new(ScriptedFairValue::ok(0.8, None));
        let strategy = build_strategy(&test_config(), &db, &store, &manifold.base_url, fair);
        let order = strategy
            .evaluate(&contract_stub("yes1"), Utc::now().timestamp(), &limiter())
            .await
            .expect("evaluation")
            .expect("a buy");
        assert_eq!(order.planned.decision.action, TradeAction::BuyYes);
        assert_eq!(order.planned.leg, 0);

        let manifold = spawn_fake_manifold(
            binary_market_json("no1", 0.8, Some(500.0)),
            serde_json::json!({}),
        )
        .await;
        let fair = Arc::new(ScriptedFairValue::ok(0.4, None));
        let strategy = build_strategy(&test_config(), &db, &store, &manifold.base_url, fair);
        let order = strategy
            .evaluate(&contract_stub("no1"), Utc::now().timestamp(), &limiter())
            .await
            .expect("evaluation")
            .expect("a buy");
        assert_eq!(order.planned.decision.action, TradeAction::BuyNo);
    }

    #[tokio::test]
    async fn multiple_choice_selects_the_named_answer_and_its_price() {
        let market = serde_json::json!({
            "id": "mc1",
            "slug": "who-wins",
            "url": "https://manifold.markets/Alice/who-wins",
            "question": "Who wins?",
            "textDescription": "A race.",
            "resolutionCriteria": "Resolves to the winner.",
            "mechanism": "cpmm-multi-1",
            "initialProb": 0.5,
            "outcomeType": "MULTIPLE_CHOICE",
            "shouldAnswersSumToOne": true,
            "isResolved": false,
            "totalLiquidity": 500.0,
            "answers": [
                {"id": "a", "text": "Alice", "probability": 0.3},
                {"id": "b", "text": "Bob", "probability": 0.6}
            ]
        });
        let manifold = spawn_fake_manifold(market, serde_json::json!({})).await;
        let (db, store) = in_memory();
        let fair = Arc::new(ScriptedFairValue::ok(0.85, Some("Bob")));
        let strategy = build_strategy(&test_config(), &db, &store, &manifold.base_url, fair);

        let order = strategy
            .evaluate(&contract_stub("mc1"), Utc::now().timestamp(), &limiter())
            .await
            .expect("evaluation")
            .expect("a buy");

        assert_eq!(order.planned.decision.action, TradeAction::BuyYes);
        assert_eq!(order.planned.decision.answer_id.as_deref(), Some("b"));
        assert_eq!(order.answer.as_deref(), Some("Bob"));
        assert!((order.p_initial - 0.6).abs() < 1e-9);
        // Answers are ordered by (created_at, id): "a" is leg 0, "b" is leg 1.
        assert_eq!(order.planned.leg, 1);
        let row = store.load("mc1").expect("load").expect("row");
        assert!(row.is_snipe());
        assert_eq!(row.outcome_type, "MULTIPLE_CHOICE");
        assert_eq!(db.list_answers("mc1").expect("answers").len(), 2);
    }

    #[tokio::test]
    async fn an_abstain_multiple_choice_answer_is_skipped() {
        let market = serde_json::json!({
            "id": "mc1",
            "question": "Who wins?",
            "outcomeType": "MULTIPLE_CHOICE",
            "mechanism": "cpmm-multi-1",
            "shouldAnswersSumToOne": true,
            "isResolved": false,
            "totalLiquidity": 500.0,
            "answers": [
                {"id": "a", "text": "Alice", "probability": 0.3},
                {"id": "b", "text": "Bob", "probability": 0.6}
            ]
        });
        let manifold = spawn_fake_manifold(market, serde_json::json!({})).await;
        let (db, store) = in_memory();
        let fair = Arc::new(ScriptedFairValue::ok(0.5, None));
        let strategy = build_strategy(&test_config(), &db, &store, &manifold.base_url, fair);

        assert!(strategy
            .evaluate(&contract_stub("mc1"), Utc::now().timestamp(), &limiter())
            .await
            .expect("evaluation")
            .is_none());
        assert_eq!(
            only_snipe(&db).reason.as_deref(),
            Some("no_answer_selected")
        );
        assert!(
            store.load("mc1").expect("load").is_none(),
            "no row is inserted"
        );
    }

    #[tokio::test]
    async fn a_sum_to_one_negative_gap_has_no_no_leg() {
        let market = serde_json::json!({
            "id": "mc1",
            "question": "Who wins?",
            "outcomeType": "MULTIPLE_CHOICE",
            "mechanism": "cpmm-multi-1",
            "shouldAnswersSumToOne": true,
            "isResolved": false,
            "totalLiquidity": 500.0,
            "answers": [
                {"id": "a", "text": "Alice", "probability": 0.3},
                {"id": "b", "text": "Bob", "probability": 0.6}
            ]
        });
        let manifold = spawn_fake_manifold(market, serde_json::json!({})).await;
        let (db, store) = in_memory();
        let fair = Arc::new(ScriptedFairValue::ok(0.4, Some("Bob")));
        let strategy = build_strategy(&test_config(), &db, &store, &manifold.base_url, fair);

        assert!(strategy
            .evaluate(&contract_stub("mc1"), Utc::now().timestamp(), &limiter())
            .await
            .expect("evaluation")
            .is_none());
        assert_eq!(only_snipe(&db).reason.as_deref(), Some("sum_to_one_no_leg"));
    }

    #[tokio::test]
    async fn a_decision_failure_is_a_skip_without_a_model_probability() {
        let manifold = spawn_fake_manifold(
            binary_market_json("c1", 0.4, Some(500.0)),
            serde_json::json!({}),
        )
        .await;
        let (db, store) = in_memory();
        let fair = Arc::new(ScriptedFairValue::err("timed out"));
        let strategy = build_strategy(&test_config(), &db, &store, &manifold.base_url, fair);

        assert!(strategy
            .evaluate(&contract_stub("c1"), Utc::now().timestamp(), &limiter())
            .await
            .expect("evaluation")
            .is_none());
        let row = only_snipe(&db);
        assert_eq!(row.action, "SKIP");
        assert!(row
            .reason
            .as_deref()
            .unwrap_or("")
            .contains("decision_failed"));
        assert_eq!(row.p_model, None);
        assert!(store.load("c1").expect("load").is_none());
    }

    #[tokio::test]
    async fn a_filled_order_records_the_actual_mana_and_bet_id() {
        let (db, store) = in_memory();
        let manifold = spawn_fake_manifold(
            binary_market_json("c1", 0.4, Some(500.0)),
            serde_json::json!({}),
        )
        .await;
        let fair = Arc::new(ScriptedFairValue::ok(0.8, None));
        let strategy = build_strategy(&test_config(), &db, &store, &manifold.base_url, fair);
        let order = strategy
            .evaluate(&contract_stub("c1"), Utc::now().timestamp(), &limiter())
            .await
            .expect("evaluation")
            .expect("a buy");

        strategy
            .record_filled(
                &order,
                &TradeRecord {
                    id: 1,
                    market_id: "c1".to_owned(),
                    answer_id: None,
                    action: TradeAction::BuyYes,
                    mana_amount: 4.25,
                    shares_delta: 7.5,
                    p_internal: 0.8,
                    p_market: 0.4,
                    trigger_post_uri: Some("c1".to_owned()),
                    manifold_bet_id: Some("bet-1".to_owned()),
                    created_at: order.created_at,
                },
            )
            .expect("record");

        let row = only_snipe(&db);
        assert_eq!(row.action, "BUY_YES");
        assert!((row.mana_amount - 4.25).abs() < 1e-9);
        assert_eq!(row.manifold_bet_id.as_deref(), Some("bet-1"));
        assert!(row.reason.as_deref().unwrap_or("").contains("gap"));
        assert_eq!(row.cost_usd, Some(0.0001));

        strategy
            .record_unfilled(&order, "budget refused")
            .expect("record");
        let rows = db.recent_snipes(10).expect("ledger");
        assert_eq!(rows.len(), 2, "the unfilled attempt is a separate row");
        assert!(rows
            .iter()
            .any(|row| row.reason.as_deref() == Some("unfilled: budget refused")));
    }

    #[tokio::test]
    async fn the_snipe_timeout_is_passed_to_the_fair_value_call() {
        let (db, store) = in_memory();
        let manifold = spawn_fake_manifold(
            binary_market_json("c1", 0.4, Some(500.0)),
            serde_json::json!({}),
        )
        .await;
        let fair = Arc::new(RecordingTimeoutFair::default());
        let mut config = test_config();
        config.snipe_decision_timeout_secs = 3;
        let strategy = build_strategy(&config, &db, &store, &manifold.base_url, fair.clone());
        let _ = strategy
            .evaluate(&contract_stub("c1"), Utc::now().timestamp(), &limiter())
            .await
            .expect("evaluation");
        assert_eq!(
            fair.timeout
                .lock()
                .expect("timeout lock")
                .map(|timeout| timeout.as_secs()),
            Some(3)
        );
    }

    #[derive(Default)]
    struct RecordingTimeoutFair {
        timeout: std::sync::Mutex<Option<Duration>>,
    }

    #[async_trait]
    impl FairValue for RecordingTimeoutFair {
        async fn fair_prob(
            &self,
            _input: &FairProbInput<'_>,
            timeout: Duration,
        ) -> AppResult<FairProb> {
            *self.timeout.lock().expect("timeout lock") = Some(timeout);
            Ok(FairProb {
                p: 0.5,
                answer: None,
                cost_usd: None,
            })
        }
    }

    #[test]
    fn unsupported_mechanisms_are_not_cpmm() {
        for mechanism in CPMM_MECHANISMS {
            assert!(CPMM_MECHANISMS.contains(&mechanism));
        }
        assert!(!CPMM_MECHANISMS.contains(&"dpm-2"));
        assert!(!CPMM_MECHANISMS.contains(&"cpmm-multi-3"));
    }
}
