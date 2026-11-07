//! S3b: smart-money copy-trading (`docs/ALGO.md` §3/S3, `docs/SPEC.md` §6.11).
//!
//! `global/new-bet` broadcasts every bet on Manifold with its `userId` the
//! moment it lands. S3a ([`crate::traders`]) answers "which traders are worth
//! watching"; this module answers "is *this* bet worth following" and turns one
//! broadcast into one [`CopyOrder`] for the shared
//! [`crate::trade::TradeExecutor`].
//!
//! # What this strategy does *not* do
//!
//! A copy is not a blind mirror. Their bet already moved the price, so the
//! question is whether the *residual* move is still worth taking. ALGO's step
//! 4 sketches "extrapolate their move" as `p_fair_est`; this implementation
//! makes the extrapolation explicit and conservative instead:
//!
//! ```text
//! residual_yes = p_fair − p_current      (positive: YES is still cheap)
//! edge         = residual_yes            when their outcome is YES
//! edge         = −residual_yes           when their outcome is NO
//! ```
//!
//! `p_current` is a fresh `GET /v0/market/{id}` probability taken *after*
//! their bet, and `p_fair` is one `fair_prob` call on that same state. A copy
//! therefore requires model edge that survives their own price impact; a fully
//! consumed move has `edge ≈ 0` and is skipped. This is the honest substitute
//! for inventing a "how much more will they move it" number.
//!
//! # Gate order and the ledger
//!
//! `copy_ledger` is the tuning surface: every gate writes one `SKIP` row with a
//! stable reason and whatever model output was already known. Gates a–c are
//! deliberate exceptions, because they run on **every** bet on the platform:
//!
//! | # | Gate | Writes a row? |
//! |---|---|---|
//! | a | `COPY_ENABLED` | no |
//! | b | not our own bet, not a redemption or cancellation, not a sale | no |
//! | c | trader is on the watch list | no |
//! | d | bet id not seen before (bounded ring) | no |
//! | e | age ≤ `COPY_MAX_AGE_SECS` | yes, `stale_bet` |
//! | f | filled, and `amount ≥ COPY_MIN_SIZE` | yes, `unfilled_bet`, `below_min_size` |
//! | g | market ownership, status, direction, snapshot, mechanism | yes |
//! | h | binary only | yes, `mcq_not_supported` |
//! | i | their outcome is `YES`/`NO` | yes, `unsupported_outcome` |
//! | j | `is_informed_bet ≥ COPY_MIN_INFORMED`, then `fair_prob`, then residual edge | yes, `not_informed`, `*_failed`, `small_edge` |
//! | k | pool, daily, max caps, Kelly target ≥ `MIN_TRADE_SIZE` | yes |
//! | l | insert the row if untracked, return the order | — |
//!
//! Writing nothing for a–d is what keeps the platform-wide stream free: only a
//! watched trader's bet can ever reach SQLite, and the watch list is a
//! lock-only read of [`TraderBoard`].
//!
//! # Scope
//!
//! Only binary markets are copied. Multiple-choice markets need per-answer
//! semantics (which answer the trader bought, what the residual edge is on
//! *that* answer, and how a basket interacts with the sum-to-one projection),
//! so a multiple-choice trigger is recorded as `mcq_not_supported` rather than
//! guessed at.
//!
//! # Caps
//!
//! Copy positions draw from one global pool (`COPY_POOL_MANA`) and one rolling
//! daily budget (`COPY_DAILY_MANA`), both separate from every operator budget
//! and from the snipe pool:
//!
//! ```text
//! cap  = min(COPY_MAX_MANA,
//!            COPY_POOL_MANA  − Db::copy_pool_invested(),
//!            COPY_DAILY_MANA − Db::copy_spent_last_24h(now))
//! size = min(cap, kelly_target_mana(residual_yes, p_current, cap, λ))
//! ```
//!
//! The inserted row's `budget_mana` is `cap`, not `size`, so the ordinary
//! `budget + realized_pnl − invested` invariant holds for the row.
//!
//! # Source exits
//!
//! [`CopyStrategy::exit_signal`] mirrors ALGO step 5 in the honest form
//! available without position provenance: when a **watched** trader sells or
//! redeems on a market where a `strategy = 'copy'` row holds shares on that
//! same outcome, the executor cancels our resting orders there and runs one
//! cached evaluation. Belief decay does the rest. Nothing is force-sold: the
//! normal edge check still decides.

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use crate::book::{kelly_target_mana, Book, PlannedTrade, TradeParams};
use crate::config::Config;
use crate::db::Db;
use crate::error::{anyhow, AppResult};
use crate::manifold_ws::BetEvent;
use crate::market_store::MarketStore;
use crate::rate::{Priority, RateLimiter, WRITE_COST_TOKENS};
use crate::snipe::FairValue;
use crate::strategy_market::{insert_market_row, resolution_criteria, MarketRow};
use crate::trade::{millis_to_seconds, ManifoldClient};
use crate::traders::TraderBoard;
use crate::triage::{FairProb, FairProbInput, InformedBetInput, TriageClient};
use crate::types::{
    CopyRecord, MarketId, MarketStatus, TradeAction, TradeDecision, TradeRecord, COPY_ACTION_EXIT_SIGNAL,
    COPY_ACTION_SKIP, STRATEGY_COPY, STRATEGY_NEWS, STRATEGY_SNIPE,
};

/// Mechanisms whose fills and price impact the residual-edge model assumes.
///
/// `dpm-2`, perps, and anything unknown are skipped: their fill semantics do
/// not match the CPMM family the fair-value question is priced against.
const CPMM_MECHANISMS: [&str; 3] = ["cpmm-1", "cpmm-multi-1", "cpmm-multi-2"];

/// How many source bet IDs the duplicate filter remembers.
///
/// `global/new-bet` delivers ~1.3 messages/s (measured, `docs/ALGO.md` §6.1),
/// so 4096 entries cover roughly an hour of the platform's bets — far longer
/// than any duplicate delivery, and a bounded ~100 KB of memory. The set is
/// deliberately in memory only: a restart replays nothing, and any replayed
/// bet is stopped by the age gate anyway.
const SEEN_BETS_CAPACITY: usize = 4_096;

/// One `is_informed_bet` call, abstracted so the gates are testable without
/// the decisions transport.
///
/// The production implementation is [`TriageClient`]; this trait exists only as
/// a test seam and never adds a second HTTP client or endpoint.
#[async_trait]
pub trait InformedGate: Send + Sync {
    async fn is_informed_bet(
        &self,
        input: &InformedBetInput<'_>,
        timeout: Duration,
    ) -> AppResult<f64>;
}

#[async_trait]
impl InformedGate for TriageClient {
    async fn is_informed_bet(
        &self,
        input: &InformedBetInput<'_>,
        timeout: Duration,
    ) -> AppResult<f64> {
        TriageClient::is_informed_bet(self, input, timeout).await
    }
}

/// What the executor should do with one evaluated bet.
#[derive(Clone, Debug)]
pub struct CopyOrder {
    /// Manifold contract ID; the row is inserted if the market was untracked.
    pub market_id: MarketId,
    /// The exact plan the shared executor runs (budget invariant, fair cap,
    /// expiring rest, ledger, Discord).
    pub planned: PlannedTrade,
    /// Provenance for `trade_ledger.trigger_post_uri`: the source bet ID.
    pub trigger: String,
    pub question: String,
    /// `is_informed_bet` output.
    pub p_informed: f64,
    /// Directional residual edge after their move (positive for both sides).
    pub edge: f64,
    /// Fresh market probability the edge was measured against.
    pub p_current: f64,
    /// The model's fair probability for YES.
    pub p_fair: f64,
    /// Human-readable rationale, copied into the ledger.
    pub reason: String,
    /// Evaluation time (Unix seconds).
    pub created_at: i64,
    /// Source trader ID (the copied bet's `userId`).
    pub trader_id: String,
    /// Mana size of the source bet.
    pub bet_amount: f64,
    /// The source bet's own probability pair, recorded for the audit trail.
    pub p_before: Option<f64>,
    pub p_after: Option<f64>,
}

/// One market to reconsider because a copied trader exited.
#[derive(Clone, Debug)]
pub struct CopyExit {
    pub market_id: MarketId,
    pub bet_id: String,
    pub question: String,
}

/// The S3b copy-trader.
pub struct CopyStrategy {
    enabled: bool,
    min_size: f64,
    min_informed: f64,
    min_edge: f64,
    kelly_fraction: f64,
    max_mana: f64,
    pool_mana: f64,
    daily_mana: f64,
    max_age_secs: i64,
    decision_timeout: Duration,
    min_trade_size: f64,
    exit_edge_threshold: f64,
    db: Db,
    store: Arc<MarketStore>,
    client: ManifoldClient,
    board: Arc<TraderBoard>,
    fair: Arc<dyn FairValue>,
    informed: Arc<dyn InformedGate>,
    /// Recently seen source bet IDs, see [`SEEN_BETS_CAPACITY`].
    seen: Mutex<SeenBets>,
}

impl CopyStrategy {
    /// Builds the strategy from process configuration.
    ///
    /// A second `TriageClient` is built here for the same reason
    /// [`crate::snipe::SnipeStrategy::new`] builds one: the pipeline keeps its
    /// client private, the type carries no per-instance state, and "one shared
    /// client" in the architecture notes means one decisions *transport*.
    pub fn new(
        config: &Config,
        db: Db,
        store: Arc<MarketStore>,
        client: ManifoldClient,
        board: Arc<TraderBoard>,
    ) -> AppResult<Self> {
        let triage = Arc::new(TriageClient::new(config)?);
        let fair: Arc<dyn FairValue> = triage.clone();
        let informed: Arc<dyn InformedGate> = triage;
        Ok(Self::with_seams(
            config, db, store, client, board, fair, informed,
        ))
    }

    /// [`CopyStrategy::new`] with injected decision seams, for tests.
    pub(crate) fn with_seams(
        config: &Config,
        db: Db,
        store: Arc<MarketStore>,
        client: ManifoldClient,
        board: Arc<TraderBoard>,
        fair: Arc<dyn FairValue>,
        informed: Arc<dyn InformedGate>,
    ) -> Self {
        Self {
            enabled: config.copy_enabled,
            min_size: config.copy_min_size,
            min_informed: config.copy_min_informed,
            min_edge: config.copy_min_edge,
            kelly_fraction: config.copy_kelly_fraction,
            max_mana: config.copy_max_mana,
            pool_mana: config.copy_pool_mana,
            daily_mana: config.copy_daily_mana,
            max_age_secs: config.copy_max_age_secs as i64,
            decision_timeout: Duration::from_secs(config.copy_decision_timeout_secs as u64),
            min_trade_size: config.min_trade_size,
            exit_edge_threshold: config.exit_edge_threshold,
            db,
            store,
            client,
            board,
            fair,
            informed,
            seen: Mutex::new(SeenBets::default()),
        }
    }

    /// Evaluates one `global/new-bet` event.
    ///
    /// Returns `Some` only for a buy the shared executor should run: the market
    /// row exists by then (inserted here when the market was untracked), so the
    /// order can load its book. Every gate from (d) on writes one ledger row;
    /// see the module table.
    pub async fn on_bet(
        &self,
        bet: &BetEvent,
        rate: &RateLimiter,
        now: i64,
        resting_mana: f64,
    ) -> AppResult<Option<CopyOrder>> {
        // (a) Disabled: not one allocation, lock, REST call, or ledger row.
        if !self.enabled {
            return Ok(None);
        }
        // (b) Pure pre-filters on the platform-wide stream. Our own bets are
        // handled by the executor's own-order path; a redemption is a payout,
        // not a signal; a cancelled order never happened on the book; and a
        // sale is an exit, handled by `exit_signal` before this method is
        // reached. None of these may write a row on a market we do not hold.
        if bet.user_id == self.client.bot_user_id()
            || bet.is_redemption
            || bet.is_cancelled
            || is_exit_bet(bet)
        {
            return Ok(None);
        }
        // (c) The watch-list lookup is the expensive-path gate: one short read
        // lock, no I/O, no database, no row. Everything below it is rare.
        let Some(trader) = self.board.lookup(&bet.user_id) else {
            return Ok(None);
        };
        // (d) Duplicate deliveries of the same bet must not copy twice. The
        // first evaluation is already the record, so a duplicate is silent.
        if !self.first_seen(&bet.id) {
            tracing::debug!(
                bet_id = %bet.id,
                contract_id = %bet.contract_id,
                "duplicate bet broadcast; already evaluated"
            );
            return Ok(None);
        }

        let mut draft = CopyDraft {
            trader_id: bet.user_id.clone(),
            contract_id: bet.contract_id.clone(),
            question: String::new(),
            bet_id: Some(bet.id.clone()),
            bet_amount: Some(bet.amount),
            p_before: finite_probability(bet.prob_before),
            p_after: finite_probability(bet.prob_after),
            p_informed: None,
            fair_prob: None,
            gap: None,
            created_at: now,
        };

        // (e) Latency. ALGO asks to follow "within the same second"; the
        // honest bound is a configurable maximum age, because the decision
        // calls plus one REST fetch plus the write typically take 1–3 s. A
        // bet older than `COPY_MAX_AGE_SECS` has usually been digested.
        let age = now.saturating_sub(millis_to_seconds(bet.created_time));
        if age > self.max_age_secs {
            return self.skip(&draft, "stale_bet");
        }

        // (f) Only a filled bet is a signal, and only one of a meaningful size
        // may be followed. An unfilled limit order sits on the book at a price
        // nobody has traded against yet.
        if !bet.is_filled {
            return self.skip(&draft, "unfilled_bet");
        }
        if bet.amount < self.min_size {
            return self.skip(&draft, "below_min_size");
        }

        // (g) Market ownership and eligibility. The row read is the cached
        // database truth (`MarketStore::load`), never REST.
        let existing = self.store.load(&bet.contract_id)?;
        if let Some(row) = &existing {
            if row.deleted_at.is_some() {
                return self.skip(&draft, "market_untracked");
            }
            if row.strategy == STRATEGY_NEWS {
                // The operator owns this market: its Sieve rule decides when
                // the belief moves, and a copy must not trade someone else's
                // budget or compete with their signal.
                return self.skip(&draft, "news_tracked");
            }
            if row.strategy == STRATEGY_SNIPE {
                // Same reasoning for an S2 row: its budget comes from the
                // snipe pool, which copy caps do not account for.
                return self.skip(&draft, "snipe_tracked");
            }
            if row.status != MarketStatus::Active {
                return self.skip(&draft, "market_closed");
            }
            // The same-leg guard: never open a second position on a leg that
            // already holds shares. `Book::plan` would refuse the entry
            // anyway, but skipping here saves the REST fetch and both decision
            // calls. The direction comes straight from the broadcast, so no
            // snapshot is needed.
            let holds = match bet.outcome.as_str() {
                "YES" => row.yes_shares > 0.0,
                "NO" => row.no_shares > 0.0,
                _ => false,
            };
            if holds {
                return self.skip(&draft, "already_positioned");
            }
        }

        if rate
            .try_acquire(Priority::Discovery, WRITE_COST_TOKENS)
            .is_err()
        {
            return self.skip(&draft, "rate_limited");
        }
        let snapshot = match self.client.market(&bet.contract_id).await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                tracing::warn!(
                    contract_id = %bet.contract_id,
                    error = %error,
                    "copy market fetch failed; the bet is recorded as a skip"
                );
                return self.skip(&draft, &format!("market_fetch_failed: {error}"));
            }
        };
        draft.question = snapshot.question.clone();

        if snapshot.is_resolved {
            return self.skip(&draft, "resolved");
        }
        if !snapshot.mechanism.trim().is_empty()
            && !CPMM_MECHANISMS.contains(&snapshot.mechanism.as_str())
        {
            return self.skip(&draft, "unsupported_mechanism");
        }

        // (h) Binary only. A multiple-choice trigger needs per-answer
        // semantics this milestone deliberately does not guess at; the skip is
        // recorded so the gap is visible in the ledger.
        if snapshot.outcome_type != "BINARY" {
            let reason = if snapshot.outcome_type == "MULTIPLE_CHOICE" {
                "mcq_not_supported"
            } else {
                "unsupported_outcome_type"
            };
            return self.skip(&draft, reason);
        }

        let Some(p_current) = snapshot.probability.filter(|p| p.is_finite()) else {
            return self.skip(&draft, "no_probability");
        };
        let p_current = p_current.clamp(0.01, 0.99);

        // (i) Their side is our side. A NO bet on a binary market copies as a
        // NO purchase, never as a short or a YES leg.
        let action = match bet.outcome.as_str() {
            "YES" => TradeAction::BuyYes,
            "NO" => TradeAction::BuyNo,
            _ => return self.skip(&draft, "unsupported_outcome"),
        };

        // (j) Informed gate: one noul call on the trader and the bet.
        let volume = snapshot.recent_volume();
        let size_vs_volume = volume.map(|volume| bet.amount / volume);
        let informed_input = InformedBetInput {
            trader_score: trader.score,
            trader_profit_weekly: trader.profit_weekly,
            trader_profit_all_time: trader.profit_all_time,
            trader_bet_count: trader.bet_count,
            question: &snapshot.question,
            market_probability: p_current,
            volume,
            amount: bet.amount,
            outcome: &bet.outcome,
            seconds_since: age.max(0),
            size_vs_volume,
        };
        let p_informed = match self
            .informed
            .is_informed_bet(&informed_input, self.decision_timeout)
            .await
        {
            Ok(p) => p,
            Err(error) => {
                tracing::warn!(
                    contract_id = %bet.contract_id,
                    bet_id = %bet.id,
                    error = %error,
                    "informed-bet decision call failed; recorded as a skip"
                );
                return self.skip(&draft, &format!("informed_decision_failed: {error}"));
            }
        };
        draft.p_informed = Some(p_informed);
        if p_informed < self.min_informed {
            return self.skip(&draft, "not_informed");
        }

        // (j) Residual edge: fair value on the post-bet state.
        //
        // The shared fair-value call takes exactly one price input; S2 feeds it
        // the creation price, and here it is the current post-bet probability.
        // The question is the same ("what is the probability this resolves
        // YES?"), so the rubric and the decoding are not forked.
        let fair_input = FairProbInput {
            question: &snapshot.question,
            resolution_criteria: resolution_criteria(&snapshot),
            close_time: snapshot.close_time_ms.map(millis_to_seconds),
            initial_prob: p_current,
            creator_id: None,
            answers: &[],
        };
        let fair: FairProb = match self.fair.fair_prob(&fair_input, self.decision_timeout).await {
            Ok(fair) => fair,
            Err(error) => {
                tracing::warn!(
                    contract_id = %bet.contract_id,
                    bet_id = %bet.id,
                    error = %error,
                    "copy fair-value call failed; recorded as a skip"
                );
                return self.skip(&draft, &format!("fair_prob_failed: {error}"));
            }
        };
        draft.fair_prob = Some(fair.p);

        // The algebra, pinned here and in the unit tests:
        //   residual_yes = p_fair − p_current
        //   YES copy edge = residual_yes
        //   NO  copy edge = (1 − p_fair) − (1 − p_current) = −residual_yes
        // Both are "what one share of our leg is worth minus what it costs".
        let residual = fair.p - p_current;
        draft.gap = Some(residual);
        let edge = if action == TradeAction::BuyYes {
            residual
        } else {
            -residual
        };
        if edge < self.min_edge {
            return self.skip(&draft, "small_edge");
        }

        // (k) Caps, then half-Kelly on the signed YES-space edge. Passing the
        // signed residual (not the direction-flipped `edge`) with the YES price
        // is exactly the planner's convention: a negative edge selects the NO
        // formula `λ·|edge|/p_current`, which is Kelly for a NO contract.
        //
        // `resting_mana` is unfilled mana of own resting orders on copy rows.
        // It is already committed to this pool and to today's deployment, so
        // both remainders subtract it. The authoritative check under the
        // execution lock repeats this with a fresh read (see
        // `TradeExecutor::execute_planned_with_provenance`).
        let pool_remaining = self.pool_mana - self.db.copy_pool_invested()? - resting_mana;
        if pool_remaining < self.min_trade_size {
            return self.skip(&draft, "pool_exhausted");
        }
        let spent = self.db.copy_spent_last_24h(now)?;
        let daily_remaining = self.daily_mana - spent - resting_mana;
        if daily_remaining < self.min_trade_size {
            return self.skip(&draft, "daily_cap");
        }
        let cap = self
            .max_mana
            .min(pool_remaining)
            .min(daily_remaining)
            .max(0.0);
        if cap < self.min_trade_size {
            return self.skip(&draft, "cap_below_min_size");
        }

        let params = TradeParams {
            kelly_fraction: self.kelly_fraction,
            entry_edge_threshold: self.min_edge,
            exit_edge_threshold: self.exit_edge_threshold,
            min_trade_size: self.min_trade_size,
        };
        let mut size = kelly_target_mana(residual, p_current, cap, params).min(cap);
        // An existing copy row keeps its original ceiling; a recovered pool
        // must not size a new copy above that row's frozen `budget_mana`.
        if let Some(row) = &existing {
            size = size.min(row.budget_mana);
        }
        if size < self.min_trade_size {
            return self.skip(&draft, "below_min_size");
        }

        // (l) Materialise the market row when this is a new copy market. An
        // existing copy row keeps its original ceiling: the row budget is a
        // per-market cap, not a per-copy one.
        if existing.is_none() {
            insert_market_row(
                &self.store,
                &self.db,
                MarketRow {
                    snapshot: &snapshot,
                    fallback_slug: "",
                    p_market: p_current,
                    p_internal: fair.p,
                    budget_mana: cap,
                    strategy: STRATEGY_COPY,
                    now,
                },
            )?;
        }

        let book = Book::load(&self.store, &self.db, &snapshot.id)?.ok_or_else(|| {
            anyhow!("copy market {} was not cached after insert", snapshot.id)
        })?;
        let leg = book
            .legs()
            .iter()
            .position(|leg| leg.answer_id.is_none())
            .ok_or_else(|| anyhow!("binary copy market {} has no root leg", snapshot.id))?;

        let reason = format!(
            "informed {:.2}; residual edge {:+.3} (p_fair {:.3} vs p_current {:.3}); kelly {:.3}",
            p_informed, edge, fair.p, p_current, size
        );

        // (m) Hand the plan to the shared executor.
        Ok(Some(CopyOrder {
            market_id: snapshot.id.clone(),
            planned: PlannedTrade {
                leg,
                decision: TradeDecision {
                    action,
                    amount_mana: size,
                    shares: None,
                    answer_id: None,
                },
                p_internal: fair.p,
                p_market: p_current,
            },
            trigger: bet.id.clone(),
            question: snapshot.question.clone(),
            p_informed,
            edge,
            p_current,
            p_fair: fair.p,
            reason,
            created_at: now,
            trader_id: bet.user_id.clone(),
            bet_amount: bet.amount,
            p_before: draft.p_before,
            p_after: draft.p_after,
        }))
    }

    /// Writes the ledger row for a copy the executor filled.
    ///
    /// The row records the *actual* mana and bet id from `trade_ledger`, so the
    /// copy ledger is a decision log that joins to the fill.
    pub fn record_filled(&self, order: &CopyOrder, trade: &TradeRecord) -> AppResult<()> {
        self.db.insert_copy(&CopyRecord {
            id: 0,
            trader_id: order.trader_id.clone(),
            contract_id: order.market_id.clone(),
            question: order.question.clone(),
            bet_id: Some(order.trigger.clone()),
            bet_amount: Some(order.bet_amount),
            p_before: order.p_before,
            p_after: order.p_after,
            p_informed: Some(order.p_informed),
            fair_prob: Some(order.p_fair),
            gap: Some(order.p_fair - order.p_current),
            action: trade.action.as_str().to_owned(),
            reason: Some(order.reason.clone()),
            mana_amount: trade.mana_amount,
            manifold_bet_id: trade.manifold_bet_id.clone(),
            created_at: order.created_at,
        })?;
        Ok(())
    }

    /// Whether a source bet has aged past `COPY_MAX_AGE_SECS` at `now`.
    ///
    /// The executor calls this immediately before executing a planned copy:
    /// the decision calls and the market fetch take real time, so the early
    /// gate alone would let a copy execute up to ~54 s after the source bet.
    pub(crate) fn is_stale(&self, bet: &BetEvent, now: i64) -> bool {
        now.saturating_sub(millis_to_seconds(bet.created_time)) > self.max_age_secs
    }

    /// The copy caps recomputed from the database and the resting layer.
    ///
    /// This is the authoritative check the executor runs while holding
    /// `execution_lock`, so two copies that passed their pre-checks against
    /// the same remainder cannot both spend it. `resting_mana` is unfilled
    /// mana of own resting orders on copy rows: it is committed to the pool
    /// and to today's deployment even before it fills, and the maker-fill path
    /// converts it to an invested position (and a `copy_ledger` buy row)
    /// without changing the total commitment.
    pub(crate) fn remaining_cap(&self, now: i64, resting_mana: f64) -> AppResult<f64> {
        let resting = resting_mana.max(0.0);
        let pool = self.pool_mana - self.db.copy_pool_invested()? - resting;
        let daily = self.daily_mana - self.db.copy_spent_last_24h(now)? - resting;
        Ok(self.max_mana.min(pool).min(daily).max(0.0))
    }

    /// Records a copy rejected because the source bet aged out while the
    /// decision calls ran, using the bare `stale_bet` reason of the early gate.
    pub fn record_stale(&self, order: &CopyOrder) -> AppResult<()> {
        self.db.insert_copy(&CopyRecord {
            id: 0,
            trader_id: order.trader_id.clone(),
            contract_id: order.market_id.clone(),
            question: order.question.clone(),
            bet_id: Some(order.trigger.clone()),
            bet_amount: Some(order.bet_amount),
            p_before: order.p_before,
            p_after: order.p_after,
            p_informed: Some(order.p_informed),
            fair_prob: Some(order.p_fair),
            gap: Some(order.p_fair - order.p_current),
            action: COPY_ACTION_SKIP.to_owned(),
            reason: Some("stale_bet".to_owned()),
            mana_amount: 0.0,
            manifold_bet_id: None,
            created_at: order.created_at,
        })?;
        Ok(())
    }

    /// Writes a `SKIP` row for a copy the executor did not fill.
    ///
    /// The reason separates a control-plane refusal (kill switch, budget, rate
    /// floor, dry run) from a wire outcome (nothing filled at the cap).
    pub fn record_unfilled(&self, order: &CopyOrder, reason: &str) -> AppResult<()> {
        self.db.insert_copy(&CopyRecord {
            id: 0,
            trader_id: order.trader_id.clone(),
            contract_id: order.market_id.clone(),
            question: order.question.clone(),
            bet_id: Some(order.trigger.clone()),
            bet_amount: Some(order.bet_amount),
            p_before: order.p_before,
            p_after: order.p_after,
            p_informed: Some(order.p_informed),
            fair_prob: Some(order.p_fair),
            gap: Some(order.p_fair - order.p_current),
            action: COPY_ACTION_SKIP.to_owned(),
            reason: Some(format!("unfilled: {reason}")),
            mana_amount: 0.0,
            manifold_bet_id: None,
            created_at: order.created_at,
        })?;
        Ok(())
    }

    /// Detects a source-trader exit on one of our copy positions.
    ///
    /// "Exit" is any bet that closes rather than opens: a sale (negative mana
    /// or shares) or a redemption. The bet is accepted only when the trader is
    /// on the watch list, the market is an active `strategy = 'copy'` row, and
    /// that row holds shares on the outcome they exited. The executor then
    /// cancels our resting orders on the market and runs one cached
    /// evaluation; nothing is force-sold.
    pub async fn exit_signal(&self, bet: &BetEvent, now: i64) -> AppResult<Option<CopyExit>> {
        if !self.enabled || bet.user_id == self.client.bot_user_id() || !is_exit_bet(bet) {
            return Ok(None);
        }
        if self.board.lookup(&bet.user_id).is_none() {
            return Ok(None);
        }
        if !self.first_seen(&bet.id) {
            return Ok(None);
        }

        let Some(row) = self.store.load(&bet.contract_id)? else {
            return Ok(None);
        };
        if !row.is_copy() || row.deleted_at.is_some() || row.status != MarketStatus::Active {
            return Ok(None);
        }
        let holds = match bet.outcome.as_str() {
            "YES" => row.yes_shares > 0.0,
            "NO" => row.no_shares > 0.0,
            _ => false,
        };
        if !holds {
            tracing::debug!(
                contract_id = %bet.contract_id,
                outcome = %bet.outcome,
                "source exit on a copy market we do not hold; ignored"
            );
            return Ok(None);
        }

        let reason = if bet.is_redemption {
            format!("source redemption on {}", bet.outcome)
        } else {
            format!("source sale on {}", bet.outcome)
        };
        self.db.insert_copy(&CopyRecord {
            id: 0,
            trader_id: bet.user_id.clone(),
            contract_id: bet.contract_id.clone(),
            question: row.question.clone(),
            bet_id: Some(bet.id.clone()),
            bet_amount: Some(bet.amount),
            p_before: finite_probability(bet.prob_before),
            p_after: finite_probability(bet.prob_after),
            p_informed: None,
            fair_prob: None,
            gap: None,
            action: COPY_ACTION_EXIT_SIGNAL.to_owned(),
            reason: Some(reason.clone()),
            mana_amount: 0.0,
            manifold_bet_id: None,
            created_at: now,
        })?;
        tracing::info!(
            contract_id = %bet.contract_id,
            trader_id = %bet.user_id,
            bet_id = %bet.id,
            reason,
            "copied trader exited; the market will be reconsidered"
        );
        Ok(Some(CopyExit {
            market_id: bet.contract_id.clone(),
            bet_id: bet.id.clone(),
            question: row.question,
        }))
    }

    /// Records one skip with whatever model output is already known.
    fn skip(&self, draft: &CopyDraft, reason: &str) -> AppResult<Option<CopyOrder>> {
        self.db.insert_copy(&CopyRecord {
            id: 0,
            trader_id: draft.trader_id.clone(),
            contract_id: draft.contract_id.clone(),
            question: draft.question.clone(),
            bet_id: draft.bet_id.clone(),
            bet_amount: draft.bet_amount,
            p_before: draft.p_before,
            p_after: draft.p_after,
            p_informed: draft.p_informed,
            fair_prob: draft.fair_prob,
            gap: draft.gap,
            action: COPY_ACTION_SKIP.to_owned(),
            reason: Some(reason.to_owned()),
            mana_amount: 0.0,
            manifold_bet_id: None,
            created_at: draft.created_at,
        })?;
        tracing::debug!(
            contract_id = %draft.contract_id,
            reason,
            p_informed = ?draft.p_informed,
            gap = ?draft.gap,
            "copy skipped"
        );
        Ok(None)
    }

    /// Marks one source bet as evaluated; `false` means it was already seen.
    fn first_seen(&self, bet_id: &str) -> bool {
        let mut seen = self
            .seen
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        seen.observe(bet_id)
    }
}

/// Whether one `global/new-bet` event closes rather than opens exposure.
///
/// Manifold reports a sale with a negative mana `amount` and negative `shares`;
/// a redemption is flagged explicitly. Both end the source trader's position,
/// which is the only signal the copy strategy needs.
pub fn is_exit_bet(bet: &BetEvent) -> bool {
    bet.is_redemption || bet.amount < 0.0 || bet.shares < 0.0
}

/// Mutable evaluation context, so each gate records what it knows.
struct CopyDraft {
    trader_id: String,
    contract_id: String,
    question: String,
    bet_id: Option<String>,
    bet_amount: Option<f64>,
    p_before: Option<f64>,
    p_after: Option<f64>,
    p_informed: Option<f64>,
    fair_prob: Option<f64>,
    gap: Option<f64>,
    created_at: i64,
}

/// A probability, or `None` when the wire carried something unusable.
fn finite_probability(value: f64) -> Option<f64> {
    value.is_finite().then(|| value.clamp(0.0, 1.0))
}

/// Bounded, insertion-ordered set of recently seen source bet IDs.
#[derive(Default)]
struct SeenBets {
    order: VecDeque<String>,
    set: HashSet<String>,
}

impl SeenBets {
    /// Records an ID and reports whether it was new.
    fn observe(&mut self, bet_id: &str) -> bool {
        if !self.set.insert(bet_id.to_owned()) {
            return false;
        }
        self.order.push_back(bet_id.to_owned());
        if self.order.len() > SEEN_BETS_CAPACITY {
            if let Some(evicted) = self.order.pop_front() {
                self.set.remove(&evicted);
            }
        }
        true
    }
}

/// Configuration used by the S3b tests: copy execution on, and a Kelly
/// fraction large enough that a 0.10 residual clears `MIN_TRADE_SIZE`.
#[cfg(test)]
pub(crate) fn test_config() -> Config {
    let mut config = crate::snipe::test_config();
    config.copy_enabled = true;
    config.copy_kelly_fraction = 0.5;
    config.copy_max_mana = 20.0;
    config
}

/// An `is_informed_bet` seam that answers one scripted probability.
#[cfg(test)]
pub(crate) struct ScriptedInformed {
    p: f64,
    calls: std::sync::atomic::AtomicUsize,
    timeout: Mutex<Option<Duration>>,
}

#[cfg(test)]
impl ScriptedInformed {
    pub(crate) fn new(p: f64) -> Self {
        Self {
            p,
            calls: std::sync::atomic::AtomicUsize::new(0),
            timeout: Mutex::new(None),
        }
    }

    pub(crate) fn calls(&self) -> usize {
        self.calls.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn timeout(&self) -> Option<Duration> {
        *self.timeout.lock().expect("timeout lock")
    }
}

#[cfg(test)]
#[async_trait]
impl InformedGate for ScriptedInformed {
    async fn is_informed_bet(
        &self,
        _input: &InformedBetInput<'_>,
        timeout: Duration,
    ) -> AppResult<f64> {
        self.calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        *self.timeout.lock().expect("timeout lock") = Some(timeout);
        Ok(self.p)
    }
}

/// A binary `strategy = 'copy'` market row, shared with the executor's
/// integration tests.
#[cfg(test)]
pub(crate) fn copy_market_row(id: &str, yes_shares: f64, no_shares: f64) -> crate::types::TrackedMarket {
    crate::types::TrackedMarket {
        id: id.to_owned(),
        slug: id.to_owned(),
        market_url: String::new(),
        question: "Will the thing happen?".to_owned(),
        description: String::new(),
        resolution_criteria: String::new(),
        close_time: None,
        status: MarketStatus::Active,
        budget_mana: 10.0,
        invested_mana: 0.0,
        realized_pnl_mana: 0.0,
        yes_shares,
        no_shares,
        p_internal_log_odds: 0.0,
        p_market_last: 0.5,
        belief_updated_at: 0,
        sieve_source: String::new(),
        discord_thread_id: String::new(),
        created_at: 0,
        deleted_at: None,
        outcome_type: "BINARY".to_owned(),
        should_answers_sum_to_one: false,
        strategy: STRATEGY_COPY.to_owned(),
    }
}

// -----------------------------------------------------------------------------
// Test fakes and fixtures
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::Value;

    use crate::rate::SystemClock;
    use crate::snipe::{binary_market_json, spawn_fake_manifold, FakeManifold, ScriptedFairValue};
    use crate::traders::TraderScore;
    use crate::types::TrackedMarket;

    const WATCHED: &str = "trader-1";
    const MARKET: &str = "Market_copy";

    fn in_memory() -> (Db, Arc<MarketStore>) {
        let db = Db::open_in_memory().expect("in-memory DB");
        let store = Arc::new(MarketStore::new(&db).expect("store loads"));
        (db, store)
    }

    fn limiter() -> RateLimiter {
        RateLimiter::per_minute(500, Box::new(SystemClock))
    }

    fn board_with(db: &Db, watched: &[&str]) -> Arc<TraderBoard> {
        let board = TraderBoard::empty();
        board
            .publish(
                db,
                watched
                    .iter()
                    .map(|user_id| TraderScore {
                        user_id: (*user_id).to_owned(),
                        username: None,
                        profit_all_time: 5_000.0,
                        profit_weekly: 500.0,
                        bet_count: 50,
                        score: 2.0,
                        updated_at: 0,
                    })
                    .collect(),
            )
            .expect("publish board");
        Arc::new(board)
    }

    fn config() -> Config {
        test_config()
    }

    fn market_json(probability: f64) -> Value {
        let mut value = binary_market_json(MARKET, probability, Some(500.0));
        value["volume24Hours"] = serde_json::json!(10_000.0);
        value
    }

    async fn fake_manifold(probability: f64) -> FakeManifold {
        spawn_fake_manifold(market_json(probability), serde_json::json!({})).await
    }

    fn bet(id: &str, user: &str, amount: f64, now: i64) -> BetEvent {
        BetEvent {
            id: id.to_owned(),
            user_id: user.to_owned(),
            contract_id: MARKET.to_owned(),
            answer_id: None,
            outcome: "YES".to_owned(),
            amount,
            order_amount: amount,
            shares: amount,
            prob_before: 0.50,
            prob_after: 0.55,
            is_filled: true,
            limit_prob: None,
            expires_at: None,
            is_cancelled: false,
            is_redemption: false,
            created_time: now.saturating_mul(1_000),
            updated_time: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        config: &Config,
        db: &Db,
        store: &Arc<MarketStore>,
        board: Arc<TraderBoard>,
        base_url: &str,
        fair: Arc<dyn FairValue>,
        informed: Arc<dyn InformedGate>,
    ) -> CopyStrategy {
        CopyStrategy::with_seams(
            config,
            db.clone(),
            Arc::clone(store),
            ManifoldClient::for_test(base_url),
            board,
            fair,
            informed,
        )
    }

    fn only_row(db: &Db) -> CopyRecord {
        let rows = db.recent_copies(10).expect("copy ledger");
        assert_eq!(rows.len(), 1, "expected one copy row, got {rows:?}");
        rows.into_iter().next().expect("one row")
    }

    fn assert_reason(db: &Db, reason: &str) {
        let row = only_row(db);
        assert_eq!(row.action, COPY_ACTION_SKIP, "{row:?}");
        assert_eq!(row.reason.as_deref(), Some(reason), "{row:?}");
    }

    fn copy_row(
        id: &str,
        yes_shares: f64,
        no_shares: f64,
        strategy: &str,
        status: MarketStatus,
    ) -> TrackedMarket {
        TrackedMarket {
            strategy: strategy.to_owned(),
            status,
            ..copy_market_row(id, yes_shares, no_shares)
        }
    }

    // -------------------------------------------------------------------
    // Pre-filters: no state, no database, no REST
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn a_disabled_strategy_spends_nothing() {
        let manifold = fake_manifold(0.5).await;
        let (db, store) = in_memory();
        let board = board_with(&db, &[WATCHED]);
        let mut config = config();
        config.copy_enabled = false;
        let informed = Arc::new(ScriptedInformed::new(0.95));
        let strategy = build(
            &config,
            &db,
            &store,
            board,
            &manifold.base_url,
            Arc::new(ScriptedFairValue::ok(0.8, None)),
            informed.clone(),
        );

        let now = 1_700_000_000;
        assert!(strategy
            .on_bet(&bet("b1", WATCHED, 50.0, now), &limiter(), now, 0.0)
            .await
            .expect("evaluation")
            .is_none());
        assert!(db.recent_copies(10).expect("ledger").is_empty());
        assert_eq!(manifold.requests(), 0);
        assert_eq!(informed.calls(), 0);
    }

    #[tokio::test]
    async fn an_unwatched_trader_is_a_silent_fast_path() {
        let manifold = fake_manifold(0.5).await;
        let (db, store) = in_memory();
        let board = board_with(&db, &["someone-else"]);
        let informed = Arc::new(ScriptedInformed::new(0.95));
        let strategy = build(
            &config(),
            &db,
            &store,
            board,
            &manifold.base_url,
            Arc::new(ScriptedFairValue::ok(0.8, None)),
            informed.clone(),
        );

        let now = 1_700_000_000;
        assert!(strategy
            .on_bet(&bet("b1", WATCHED, 50.0, now), &limiter(), now, 0.0)
            .await
            .expect("evaluation")
            .is_none());
        assert!(
            db.recent_copies(10).expect("ledger").is_empty(),
            "an unwatched bet must not reach the database"
        );
        assert_eq!(manifold.requests(), 0);
        assert_eq!(informed.calls(), 0);
    }

    #[tokio::test]
    async fn own_redemption_and_cancelled_bets_are_silent() {
        let manifold = fake_manifold(0.5).await;
        let (db, store) = in_memory();
        let board = board_with(&db, &[WATCHED]);
        let strategy = build(
            &config(),
            &db,
            &store,
            board,
            &manifold.base_url,
            Arc::new(ScriptedFairValue::ok(0.8, None)),
            Arc::new(ScriptedInformed::new(0.95)),
        );
        let now = 1_700_000_000;

        let own = bet("b1", "bot-1", 50.0, now);
        assert!(strategy
            .on_bet(&own, &limiter(), now, 0.0)
            .await
            .expect("evaluation")
            .is_none());

        let mut redemption = bet("b2", WATCHED, 50.0, now);
        redemption.is_redemption = true;
        assert!(strategy
            .on_bet(&redemption, &limiter(), now, 0.0)
            .await
            .expect("evaluation")
            .is_none());

        let mut cancelled = bet("b3", WATCHED, 50.0, now);
        cancelled.is_cancelled = true;
        assert!(strategy
            .on_bet(&cancelled, &limiter(), now, 0.0)
            .await
            .expect("evaluation")
            .is_none());

        assert!(db.recent_copies(10).expect("ledger").is_empty());
        assert_eq!(manifold.requests(), 0);
    }

    #[tokio::test]
    async fn a_duplicate_bet_id_is_evaluated_once() {
        let manifold = fake_manifold(0.5).await;
        let (db, store) = in_memory();
        let board = board_with(&db, &[WATCHED]);
        let strategy = build(
            &config(),
            &db,
            &store,
            board,
            &manifold.base_url,
            Arc::new(ScriptedFairValue::ok(0.8, None)),
            Arc::new(ScriptedInformed::new(0.95)),
        );
        let now = 1_700_000_000;
        let tiny = bet("dup-1", WATCHED, 1.0, now);

        for _ in 0..3 {
            assert!(strategy
                .on_bet(&tiny, &limiter(), now, 0.0)
                .await
                .expect("evaluation")
                .is_none());
        }
        // The first delivery wrote one row; the duplicates wrote none.
        assert_reason(&db, "below_min_size");
    }

    #[test]
    fn the_seen_set_is_bounded_and_evicts_oldest() {
        let mut seen = SeenBets::default();
        for index in 0..SEEN_BETS_CAPACITY {
            assert!(seen.observe(&format!("id-{index}")));
        }
        assert!(!seen.observe("id-0"), "a known id is a duplicate");
        assert!(seen.observe("fresh"), "the ring evicted the oldest id");
        assert_eq!(seen.order.len(), SEEN_BETS_CAPACITY);
        assert_eq!(seen.set.len(), SEEN_BETS_CAPACITY);
    }

    // -------------------------------------------------------------------
    // Free gates
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn latency_size_and_fill_gates_run_before_rest() {
        let manifold = fake_manifold(0.5).await;
        let (db, store) = in_memory();
        let board = board_with(&db, &[WATCHED]);
        let strategy = build(
            &config(),
            &db,
            &store,
            board,
            &manifold.base_url,
            Arc::new(ScriptedFairValue::ok(0.8, None)),
            Arc::new(ScriptedInformed::new(0.95)),
        );
        let now = 1_700_000_000;

        // 31 s old against COPY_MAX_AGE_SECS=30.
        let stale = bet("stale", WATCHED, 50.0, now - 31);
        assert!(strategy.on_bet(&stale, &limiter(), now, 0.0).await.expect("eval").is_none());
        assert_reason(&db, "stale_bet");

        let mut unfilled = bet("unfilled", WATCHED, 50.0, now);
        unfilled.is_filled = false;
        assert!(strategy
            .on_bet(&unfilled, &limiter(), now, 0.0)
            .await
            .expect("eval")
            .is_none());
        assert!(db
            .recent_copies(10)
            .expect("ledger")
            .iter()
            .any(|row| row.reason.as_deref() == Some("unfilled_bet")));

        let small = bet("small", WATCHED, 4.99, now);
        assert!(strategy.on_bet(&small, &limiter(), now, 0.0).await.expect("eval").is_none());
        assert!(db
            .recent_copies(10)
            .expect("ledger")
            .iter()
            .any(|row| row.reason.as_deref() == Some("below_min_size")));

        assert_eq!(manifold.requests(), 0, "only free gates ran");
    }

    #[tokio::test]
    async fn a_bet_exactly_at_the_max_age_is_still_eligible() {
        let manifold = fake_manifold(0.5).await;
        let (db, store) = in_memory();
        let board = board_with(&db, &[WATCHED]);
        let strategy = build(
            &config(),
            &db,
            &store,
            board,
            &manifold.base_url,
            Arc::new(ScriptedFairValue::ok(0.50, None)),
            Arc::new(ScriptedInformed::new(0.10)),
        );
        let now = 1_700_000_000;
        let order = strategy
            .on_bet(&bet("edge-age", WATCHED, 50.0, now - 30), &limiter(), now, 0.0)
            .await
            .expect("eval");
        assert!(order.is_none());
        // It reached the informed gate, so the age gate passed.
        assert_reason(&db, "not_informed");
        assert_eq!(manifold.requests(), 1);
    }

    // -------------------------------------------------------------------
    // Market ownership and shape
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn operator_and_snipe_markets_are_left_alone() {
        let manifold = fake_manifold(0.5).await;
        let (db, store) = in_memory();
        let board = board_with(&db, &[WATCHED]);
        store
            .insert(&copy_row(MARKET, 0.0, 0.0, STRATEGY_NEWS, MarketStatus::Active))
            .expect("news row");
        let strategy = build(
            &config(),
            &db,
            &store,
            board,
            &manifold.base_url,
            Arc::new(ScriptedFairValue::ok(0.8, None)),
            Arc::new(ScriptedInformed::new(0.95)),
        );
        let now = 1_700_000_000;
        assert!(strategy
            .on_bet(&bet("b1", WATCHED, 50.0, now), &limiter(), now, 0.0)
            .await
            .expect("eval")
            .is_none());
        assert_reason(&db, "news_tracked");

        let (db2, store2) = in_memory();
        let board2 = board_with(&db2, &[WATCHED]);
        store2
            .insert(&copy_row(MARKET, 0.0, 0.0, STRATEGY_SNIPE, MarketStatus::Active))
            .expect("snipe row");
        let strategy = build(
            &config(),
            &db2,
            &store2,
            board2,
            &manifold.base_url,
            Arc::new(ScriptedFairValue::ok(0.8, None)),
            Arc::new(ScriptedInformed::new(0.95)),
        );
        assert!(strategy
            .on_bet(&bet("b2", WATCHED, 50.0, now), &limiter(), now, 0.0)
            .await
            .expect("eval")
            .is_none());
        assert_reason(&db2, "snipe_tracked");
        assert_eq!(manifold.requests(), 0, "ownership gates precede REST");
    }

    #[tokio::test]
    async fn a_closed_copy_row_and_a_held_leg_are_skipped() {
        let manifold = fake_manifold(0.5).await;
        let (db, store) = in_memory();
        let board = board_with(&db, &[WATCHED]);
        store
            .insert(&copy_row(MARKET, 10.0, 0.0, STRATEGY_COPY, MarketStatus::Active))
            .expect("copy row");
        let strategy = build(
            &copy_signal_config(),
            &db,
            &store,
            board,
            &manifold.base_url,
            Arc::new(ScriptedFairValue::ok(0.2, None)),
            Arc::new(ScriptedInformed::new(0.95)),
        );
        let now = 1_700_000_000;
        assert!(strategy
            .on_bet(&bet("held", WATCHED, 50.0, now), &limiter(), now, 0.0)
            .await
            .expect("eval")
            .is_none());
        assert_reason(&db, "already_positioned");
        assert_eq!(manifold.requests(), 0, "the held-leg gate precedes REST");

        // The opposite leg is still open.
        let mut no_bet = bet("open-no", WATCHED, 50.0, now);
        no_bet.outcome = "NO".to_owned();
        assert!(strategy
            .on_bet(&no_bet, &limiter(), now, 0.0)
            .await
            .expect("eval")
            .is_some());

        // A closed row is not tradeable at all.
        let (db2, store2) = in_memory();
        let board2 = board_with(&db2, &[WATCHED]);
        store2
            .insert(&copy_row(MARKET, 0.0, 0.0, STRATEGY_COPY, MarketStatus::Closed))
            .expect("closed row");
        let strategy = build(
            &config(),
            &db2,
            &store2,
            board2,
            &manifold.base_url,
            Arc::new(ScriptedFairValue::ok(0.8, None)),
            Arc::new(ScriptedInformed::new(0.95)),
        );
        assert!(strategy
            .on_bet(&bet("closed", WATCHED, 50.0, now), &limiter(), now, 0.0)
            .await
            .expect("eval")
            .is_none());
        assert_reason(&db2, "market_closed");
    }

    #[tokio::test]
    async fn a_multiple_choice_trigger_is_recorded_as_the_gap() {
        let market = serde_json::json!({
            "id": MARKET,
            "question": "Who wins?",
            "mechanism": "cpmm-multi-1",
            "outcomeType": "MULTIPLE_CHOICE",
            "shouldAnswersSumToOne": true,
            "isResolved": false,
            "answers": [
                {"id": "a", "text": "Alice", "probability": 0.3},
                {"id": "b", "text": "Bob", "probability": 0.6}
            ]
        });
        let manifold = spawn_fake_manifold(market, serde_json::json!({})).await;
        let (db, store) = in_memory();
        let board = board_with(&db, &[WATCHED]);
        let strategy = build(
            &config(),
            &db,
            &store,
            board,
            &manifold.base_url,
            Arc::new(ScriptedFairValue::ok(0.8, None)),
            Arc::new(ScriptedInformed::new(0.95)),
        );
        let now = 1_700_000_000;
        assert!(strategy
            .on_bet(&bet("mcq", WATCHED, 50.0, now), &limiter(), now, 0.0)
            .await
            .expect("eval")
            .is_none());
        assert_reason(&db, "mcq_not_supported");
        assert!(store.load(MARKET).expect("load").is_none());
    }

    #[tokio::test]
    async fn an_unsupported_mechanism_and_a_resolved_market_are_skipped() {
        let mut dpm = market_json(0.5);
        dpm["mechanism"] = serde_json::json!("dpm-2");
        let manifold = spawn_fake_manifold(dpm, serde_json::json!({})).await;
        let (db, store) = in_memory();
        let board = board_with(&db, &[WATCHED]);
        let strategy = build(
            &config(),
            &db,
            &store,
            board,
            &manifold.base_url,
            Arc::new(ScriptedFairValue::ok(0.8, None)),
            Arc::new(ScriptedInformed::new(0.95)),
        );
        let now = 1_700_000_000;
        assert!(strategy
            .on_bet(&bet("dpm", WATCHED, 50.0, now), &limiter(), now, 0.0)
            .await
            .expect("eval")
            .is_none());
        assert_reason(&db, "unsupported_mechanism");

        let mut resolved = market_json(0.5);
        resolved["isResolved"] = serde_json::json!(true);
        let manifold = spawn_fake_manifold(resolved, serde_json::json!({})).await;
        let (db, store) = in_memory();
        let board = board_with(&db, &[WATCHED]);
        let strategy = build(
            &config(),
            &db,
            &store,
            board,
            &manifold.base_url,
            Arc::new(ScriptedFairValue::ok(0.8, None)),
            Arc::new(ScriptedInformed::new(0.95)),
        );
        assert!(strategy
            .on_bet(&bet("resolved", WATCHED, 50.0, now), &limiter(), now, 0.0)
            .await
            .expect("eval")
            .is_none());
        assert_reason(&db, "resolved");
    }

    // -------------------------------------------------------------------
    // Decision gates and the residual-edge algebra
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn an_uninformed_bet_is_skipped_before_the_fair_call() {
        let manifold = fake_manifold(0.5).await;
        let (db, store) = in_memory();
        let board = board_with(&db, &[WATCHED]);
        let fair = Arc::new(ScriptedFairValue::ok(0.9, None));
        let strategy = build(
            &config(),
            &db,
            &store,
            board,
            &manifold.base_url,
            fair.clone(),
            Arc::new(ScriptedInformed::new(0.59)),
        );
        let now = 1_700_000_000;
        assert!(strategy
            .on_bet(&bet("b1", WATCHED, 50.0, now), &limiter(), now, 0.0)
            .await
            .expect("eval")
            .is_none());
        let row = only_row(&db);
        assert_eq!(row.reason.as_deref(), Some("not_informed"));
        assert_eq!(row.p_informed, Some(0.59));
        assert_eq!(fair.calls(), 0, "the fair call follows the informed gate");
        assert_eq!(row.question, "Will the thing happen?");
    }

    #[tokio::test]
    async fn a_fully_consumed_move_has_no_residual_edge() {
        let manifold = fake_manifold(0.58).await;
        let (db, store) = in_memory();
        let board = board_with(&db, &[WATCHED]);
        let strategy = build(
            &config(),
            &db,
            &store,
            board,
            &manifold.base_url,
            Arc::new(ScriptedFairValue::ok(0.60, None)),
            Arc::new(ScriptedInformed::new(0.9)),
        );
        let now = 1_700_000_000;
        // residual = 0.60 - 0.58 = 0.02 < COPY_MIN_EDGE (0.05).
        assert!(strategy
            .on_bet(&bet("b1", WATCHED, 50.0, now), &limiter(), now, 0.0)
            .await
            .expect("eval")
            .is_none());
        let row = only_row(&db);
        assert_eq!(row.reason.as_deref(), Some("small_edge"));
        assert!((row.gap.expect("gap") - 0.019999999999999997).abs() < 1e-12);
        assert!((row.fair_prob.expect("fair") - 0.60).abs() < 1e-12);
    }

    #[tokio::test]
    async fn a_yes_follow_takes_the_positive_yes_space_residual() {
        let manifold = fake_manifold(0.50).await;
        let (db, store) = in_memory();
        let board = board_with(&db, &[WATCHED]);
        let strategy = build(
            &copy_signal_config(),
            &db,
            &store,
            board,
            &manifold.base_url,
            Arc::new(ScriptedFairValue::ok(0.60, None)),
            Arc::new(ScriptedInformed::new(0.9)),
        );
        let now = 1_700_000_000;
        let order = strategy
            .on_bet(&bet("yes", WATCHED, 50.0, now), &limiter(), now, 0.0)
            .await
            .expect("eval")
            .expect("a copy");

        assert_eq!(order.planned.decision.action, TradeAction::BuyYes);
        assert_eq!(order.planned.leg, 0);
        assert!((order.p_current - 0.50).abs() < 1e-12);
        assert!((order.p_fair - 0.60).abs() < 1e-12);
        assert!((order.edge - (order.p_fair - order.p_current)).abs() < 1e-12);
        assert!((order.edge - 0.10).abs() < 1e-12);
        assert!(store.load(MARKET).expect("load").expect("row").is_copy());
    }

    #[tokio::test]
    async fn a_no_follow_takes_the_negative_residual_as_its_own_edge() {
        let manifold = fake_manifold(0.50).await;
        let (db, store) = in_memory();
        let board = board_with(&db, &[WATCHED]);
        let strategy = build(
            &copy_signal_config(),
            &db,
            &store,
            board,
            &manifold.base_url,
            Arc::new(ScriptedFairValue::ok(0.40, None)),
            Arc::new(ScriptedInformed::new(0.9)),
        );
        let now = 1_700_000_000;
        let mut sell_pressure = bet("no", WATCHED, 50.0, now);
        sell_pressure.outcome = "NO".to_owned();
        let order = strategy
            .on_bet(&sell_pressure, &limiter(), now, 0.0)
            .await
            .expect("eval")
            .expect("a copy");

        assert_eq!(order.planned.decision.action, TradeAction::BuyNo);
        // residual_yes = 0.40 - 0.50 = -0.10
        assert!((order.p_fair - order.p_current + 0.10).abs() < 1e-12);
        // edge = −residual_yes = 0.10, exactly the NO leg's (1−p_fair) − (1−p_current).
        assert!((order.edge - (-(order.p_fair - order.p_current))).abs() < 1e-12);
        assert!((order.edge - 0.10).abs() < 1e-12);
        assert!((order.planned.p_internal - 0.40).abs() < 1e-12);
    }

    #[tokio::test]
    async fn a_no_follow_needs_the_model_to_be_below_the_market() {
        let manifold = fake_manifold(0.50).await;
        let (db, store) = in_memory();
        let board = board_with(&db, &[WATCHED]);
        let strategy = build(
            &config(),
            &db,
            &store,
            board,
            &manifold.base_url,
            Arc::new(ScriptedFairValue::ok(0.60, None)),
            Arc::new(ScriptedInformed::new(0.9)),
        );
        let now = 1_700_000_000;
        let mut no_bet = bet("no-wrong-way", WATCHED, 50.0, now);
        no_bet.outcome = "NO".to_owned();
        // Their NO bet has no edge if the model says YES is worth 0.60.
        assert!(strategy
            .on_bet(&no_bet, &limiter(), now, 0.0)
            .await
            .expect("eval")
            .is_none());
        assert_reason(&db, "small_edge");
    }

    #[tokio::test]
    async fn an_unsupported_outcome_is_recorded() {
        let manifold = fake_manifold(0.50).await;
        let (db, store) = in_memory();
        let board = board_with(&db, &[WATCHED]);
        let strategy = build(
            &config(),
            &db,
            &store,
            board,
            &manifold.base_url,
            Arc::new(ScriptedFairValue::ok(0.60, None)),
            Arc::new(ScriptedInformed::new(0.9)),
        );
        let now = 1_700_000_000;
        let mut weird = bet("weird", WATCHED, 50.0, now);
        weird.outcome = "MAYBE".to_owned();
        assert!(strategy
            .on_bet(&weird, &limiter(), now, 0.0)
            .await
            .expect("eval")
            .is_none());
        assert_reason(&db, "unsupported_outcome");
    }

    // -------------------------------------------------------------------
    // Sizing and the three caps
    // -------------------------------------------------------------------

    /// The shared signal configuration; kept as a name for readability at
    /// the call sites that need a copy to actually fire.
    fn copy_signal_config() -> Config {
        config()
    }

    /// The per-copy cap test needs a deliberately small ceiling.
    fn strong_signal_config() -> Config {
        let mut config = config();
        config.copy_kelly_fraction = 0.5;
        config.copy_max_mana = 4.0;
        config
    }

    #[tokio::test]
    async fn the_per_copy_cap_bounds_the_kelly_target_and_the_row_budget() {
        let manifold = fake_manifold(0.10).await;
        let (db, store) = in_memory();
        let board = board_with(&db, &[WATCHED]);
        let strategy = build(
            &strong_signal_config(),
            &db,
            &store,
            board,
            &manifold.base_url,
            Arc::new(ScriptedFairValue::ok(0.95, None)),
            Arc::new(ScriptedInformed::new(0.9)),
        );
        let now = 1_700_000_000;
        let order = strategy
            .on_bet(&bet("capped", WATCHED, 50.0, now), &limiter(), now, 0.0)
            .await
            .expect("eval")
            .expect("a copy");
        // f = 0.5 * 0.85 / (1 - 0.10) = 0.4722… -> 1.888… mana of the 4 cap.
        assert!(
            (order.planned.decision.amount_mana - 1.888_888_888_888_888_8).abs() < 1e-9,
            "{order:?}"
        );
        let row = store.load(MARKET).expect("load").expect("row");
        assert!((row.budget_mana - 4.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn resting_copy_orders_count_against_the_pool_and_the_daily_cap() {
        let manifold = fake_manifold(0.10).await;
        let (db, store) = in_memory();
        let board = board_with(&db, &[WATCHED]);
        let strategy = build(
            &strong_signal_config(),
            &db,
            &store,
            board,
            &manifold.base_url,
            Arc::new(ScriptedFairValue::ok(0.95, None)),
            Arc::new(ScriptedInformed::new(0.9)),
        );
        let now = 1_700_000_000;

        // Unfilled own resting orders on copy rows are committed mana: 99.5
        // of the 100 mana pool leaves less than MIN_TRADE_SIZE.
        assert!(strategy
            .on_bet(&bet("resting", WATCHED, 50.0, now), &limiter(), now, 99.5)
            .await
            .expect("eval")
            .is_none());
        assert_reason(&db, "pool_exhausted");
    }

    #[tokio::test]
    async fn an_existing_row_budget_bounds_a_recovered_pool() {
        let manifold = fake_manifold(0.50).await;
        let (db, store) = in_memory();
        let board = board_with(&db, &[WATCHED]);
        let mut existing = copy_row(MARKET, 0.0, 0.0, STRATEGY_COPY, MarketStatus::Active);
        existing.budget_mana = 1.0;
        store.insert(&existing).expect("existing copy row");
        let strategy = build(
            &config(),
            &db,
            &store,
            board,
            &manifold.base_url,
            Arc::new(ScriptedFairValue::ok(0.60, None)),
            Arc::new(ScriptedInformed::new(0.9)),
        );
        let now = 1_700_000_000;

        // The Kelly target is 2.0 mana against the global caps, but the row's
        // frozen 1.0 mana budget is the per-market ceiling.
        let order = strategy
            .on_bet(&bet("row-budget", WATCHED, 50.0, now), &limiter(), now, 0.0)
            .await
            .expect("eval")
            .expect("a copy");
        assert!(
            (order.planned.decision.amount_mana - 1.0).abs() < 1e-9,
            "{order:?}"
        );
    }

    #[tokio::test]
    async fn an_exhausted_pool_stops_the_copy() {
        let manifold = fake_manifold(0.10).await;
        let (db, store) = in_memory();
        let board = board_with(&db, &[WATCHED]);
        let mut occupied = copy_row("Market_old", 0.0, 0.0, STRATEGY_COPY, MarketStatus::Active);
        occupied.budget_mana = 100.0;
        occupied.invested_mana = 99.5;
        store.insert(&occupied).expect("occupied row");
        let strategy = build(
            &strong_signal_config(),
            &db,
            &store,
            board,
            &manifold.base_url,
            Arc::new(ScriptedFairValue::ok(0.95, None)),
            Arc::new(ScriptedInformed::new(0.9)),
        );
        let now = 1_700_000_000;
        assert!(strategy
            .on_bet(&bet("pool", WATCHED, 50.0, now), &limiter(), now, 0.0)
            .await
            .expect("eval")
            .is_none());
        assert_reason(&db, "pool_exhausted");
    }

    #[tokio::test]
    async fn the_daily_cap_counts_executed_buys_only() {
        let manifold = fake_manifold(0.10).await;
        let (db, store) = in_memory();
        let board = board_with(&db, &[WATCHED]);
        let now = 1_700_000_000;
        db.insert_copy(&CopyRecord {
            id: 0,
            trader_id: WATCHED.to_owned(),
            contract_id: "Market_old".to_owned(),
            question: "old".to_owned(),
            bet_id: Some("old-bet".to_owned()),
            bet_amount: Some(10.0),
            p_before: None,
            p_after: None,
            p_informed: Some(0.9),
            fair_prob: Some(0.7),
            gap: Some(0.1),
            action: "BUY_YES".to_owned(),
            reason: None,
            mana_amount: 30.0,
            manifold_bet_id: Some("fill".to_owned()),
            created_at: now - 60,
        })
        .expect("spent row");
        // A skip and an old buy must not consume the daily budget.
        db.insert_copy(&CopyRecord {
            id: 0,
            trader_id: WATCHED.to_owned(),
            contract_id: "Market_old".to_owned(),
            question: "old".to_owned(),
            bet_id: Some("skip".to_owned()),
            bet_amount: None,
            p_before: None,
            p_after: None,
            p_informed: None,
            fair_prob: None,
            gap: None,
            action: COPY_ACTION_SKIP.to_owned(),
            reason: Some("noise".to_owned()),
            mana_amount: 0.0,
            manifold_bet_id: None,
            created_at: now - 60,
        })
        .expect("skip row");
        db.insert_copy(&CopyRecord {
            id: 0,
            trader_id: WATCHED.to_owned(),
            contract_id: "Market_old".to_owned(),
            question: "old".to_owned(),
            bet_id: Some("ancient".to_owned()),
            bet_amount: Some(10.0),
            p_before: None,
            p_after: None,
            p_informed: Some(0.9),
            fair_prob: Some(0.7),
            gap: Some(0.1),
            action: "BUY_NO".to_owned(),
            reason: None,
            mana_amount: 50.0,
            manifold_bet_id: Some("old-fill".to_owned()),
            created_at: now - crate::db::COPY_SPEND_WINDOW_SECS - 1,
        })
        .expect("ancient row");
        assert!((db.copy_spent_last_24h(now).expect("spend") - 30.0).abs() < 1e-9);

        let strategy = build(
            &strong_signal_config(),
            &db,
            &store,
            board,
            &manifold.base_url,
            Arc::new(ScriptedFairValue::ok(0.95, None)),
            Arc::new(ScriptedInformed::new(0.9)),
        );
        assert!(strategy
            .on_bet(&bet("daily", WATCHED, 50.0, now), &limiter(), now, 0.0)
            .await
            .expect("eval")
            .is_none());
        let newest = db.recent_copies(1).expect("ledger");
        assert_eq!(newest[0].reason.as_deref(), Some("daily_cap"), "{newest:?}");
    }

    #[tokio::test]
    async fn a_target_below_the_minimum_trade_size_is_refused() {
        let manifold = fake_manifold(0.10).await;
        let (db, store) = in_memory();
        let board = board_with(&db, &[WATCHED]);
        let mut config = config();
        // λ tiny: 0.01 * 0.85 / 0.9 * 10 = 0.094 mana < MIN_TRADE_SIZE.
        config.copy_kelly_fraction = 0.01;
        let strategy = build(
            &config,
            &db,
            &store,
            board,
            &manifold.base_url,
            Arc::new(ScriptedFairValue::ok(0.95, None)),
            Arc::new(ScriptedInformed::new(0.9)),
        );
        let now = 1_700_000_000;
        assert!(strategy
            .on_bet(&bet("tiny", WATCHED, 50.0, now), &limiter(), now, 0.0)
            .await
            .expect("eval")
            .is_none());
        assert_reason(&db, "below_min_size");
        assert!(store.load(MARKET).expect("load").is_none());
    }

    #[tokio::test]
    async fn the_copy_decision_timeout_reaches_both_calls() {
        struct RecordingFair {
            timeout: Mutex<Option<Duration>>,
        }

        #[async_trait]
        impl FairValue for RecordingFair {
            async fn fair_prob(
                &self,
                _input: &FairProbInput<'_>,
                timeout: Duration,
            ) -> AppResult<FairProb> {
                *self.timeout.lock().expect("timeout lock") = Some(timeout);
                Ok(FairProb {
                    p: 0.6,
                    answer: None,
                    cost_usd: None,
                })
            }
        }

        let manifold = fake_manifold(0.50).await;
        let (db, store) = in_memory();
        let board = board_with(&db, &[WATCHED]);
        let mut config = copy_signal_config();
        config.copy_decision_timeout_secs = 3;
        let fair = Arc::new(RecordingFair {
            timeout: Mutex::new(None),
        });
        let informed = Arc::new(ScriptedInformed::new(0.9));
        let strategy = build(
            &config,
            &db,
            &store,
            board,
            &manifold.base_url,
            fair.clone(),
            informed.clone(),
        );
        let now = 1_700_000_000;
        assert!(strategy
            .on_bet(&bet("timeout", WATCHED, 50.0, now), &limiter(), now, 0.0)
            .await
            .expect("eval")
            .is_some());
        assert_eq!(informed.timeout().map(|t| t.as_secs()), Some(3));
        assert_eq!(
            fair.timeout.lock().expect("lock").map(|t| t.as_secs()),
            Some(3)
        );
    }

    // -------------------------------------------------------------------
    // Ledger writes for fills and exit signals
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn a_filled_copy_records_the_actual_mana_and_bet_id() {
        let manifold = fake_manifold(0.50).await;
        let (db, store) = in_memory();
        let board = board_with(&db, &[WATCHED]);
        let strategy = build(
            &copy_signal_config(),
            &db,
            &store,
            board,
            &manifold.base_url,
            Arc::new(ScriptedFairValue::ok(0.60, None)),
            Arc::new(ScriptedInformed::new(0.9)),
        );
        let now = 1_700_000_000;
        let order = strategy
            .on_bet(&bet("filled", WATCHED, 50.0, now), &limiter(), now, 0.0)
            .await
            .expect("eval")
            .expect("a copy");

        strategy
            .record_filled(
                &order,
                &TradeRecord {
                    id: 1,
                    market_id: MARKET.to_owned(),
                    answer_id: None,
                    action: TradeAction::BuyYes,
                    mana_amount: 2.75,
                    shares_delta: 5.0,
                    p_internal: 0.60,
                    p_market: 0.50,
                    trigger_post_uri: Some(order.trigger.clone()),
                    limit_prob: Some(0.61),
                    manifold_bet_id: Some("copy-fill".to_owned()),
                    created_at: order.created_at,
                },
            )
            .expect("record fill");
        let row = only_row(&db);
        assert_eq!(row.action, "BUY_YES");
        assert_eq!(row.trader_id, WATCHED);
        assert_eq!(row.bet_id.as_deref(), Some("filled"));
        assert!((row.mana_amount - 2.75).abs() < 1e-9);
        assert_eq!(row.manifold_bet_id.as_deref(), Some("copy-fill"));
        assert!(row.reason.as_deref().unwrap_or("").contains("informed"));

        strategy
            .record_unfilled(&order, "kill switch")
            .expect("record refusal");
        assert!(db
            .recent_copies(10)
            .expect("ledger")
            .iter()
            .any(|row| row.reason.as_deref() == Some("unfilled: kill switch")));
    }

    #[tokio::test]
    async fn a_watched_exit_on_a_held_copy_position_is_an_exit_signal() {
        let manifold = fake_manifold(0.50).await;
        let (db, store) = in_memory();
        let board = board_with(&db, &[WATCHED]);
        let mut held = copy_row(MARKET, 12.0, 0.0, STRATEGY_COPY, MarketStatus::Active);
        held.budget_mana = 10.0;
        held.invested_mana = 6.0;
        store.insert(&held).expect("held row");
        let strategy = build(
            &copy_signal_config(),
            &db,
            &store,
            board,
            &manifold.base_url,
            Arc::new(ScriptedFairValue::ok(0.8, None)),
            Arc::new(ScriptedInformed::new(0.95)),
        );
        let now = 1_700_000_000;

        let mut sale = bet("exit-1", WATCHED, -8.0, now);
        sale.outcome = "YES".to_owned();
        sale.shares = -12.0;
        let exit = strategy
            .exit_signal(&sale, now)
            .await
            .expect("exit handled")
            .expect("an exit signal");
        assert_eq!(exit.market_id, MARKET);
        assert_eq!(exit.bet_id, "exit-1");
        let row = only_row(&db);
        assert_eq!(row.action, COPY_ACTION_EXIT_SIGNAL);
        assert!(row.reason.as_deref().unwrap_or("").contains("sale"));

        // The opposite leg is not held, so a NO exit is not ours to act on.
        let mut other_leg = bet("exit-2", WATCHED, -8.0, now);
        other_leg.outcome = "NO".to_owned();
        other_leg.shares = -12.0;
        assert!(strategy
            .exit_signal(&other_leg, now)
            .await
            .expect("exit handled")
            .is_none());
    }

    #[tokio::test]
    async fn an_unwatched_exit_or_a_news_position_is_ignored() {
        let manifold = fake_manifold(0.50).await;
        let (db, store) = in_memory();
        let board = board_with(&db, &[WATCHED]);
        store
            .insert(&copy_row(MARKET, 12.0, 0.0, STRATEGY_COPY, MarketStatus::Active))
            .expect("copy row");
        let strategy = build(
            &config(),
            &db,
            &store,
            board,
            &manifold.base_url,
            Arc::new(ScriptedFairValue::ok(0.8, None)),
            Arc::new(ScriptedInformed::new(0.95)),
        );
        let now = 1_700_000_000;
        let mut sale = bet("exit-1", "stranger", -8.0, now);
        sale.shares = -12.0;
        assert!(strategy.exit_signal(&sale, now).await.expect("exit").is_none());
        assert!(db.recent_copies(10).expect("ledger").is_empty());

        // A news-owned row is not a copy position even with shares.
        let (db2, store2) = in_memory();
        let board2 = board_with(&db2, &[WATCHED]);
        store2
            .insert(&copy_row(MARKET, 12.0, 0.0, STRATEGY_NEWS, MarketStatus::Active))
            .expect("news row");
        let strategy = build(
            &config(),
            &db2,
            &store2,
            board2,
            &manifold.base_url,
            Arc::new(ScriptedFairValue::ok(0.8, None)),
            Arc::new(ScriptedInformed::new(0.95)),
        );
        assert!(strategy.exit_signal(&sale, now).await.expect("exit").is_none());
        assert!(db2.recent_copies(10).expect("ledger").is_empty());
    }

    #[test]
    fn only_negative_or_redeeming_bets_are_exits() {
        let now = 1_700_000_000;
        let entry = bet("e", WATCHED, 10.0, now);
        assert!(!is_exit_bet(&entry));

        let mut sale = bet("s", WATCHED, -10.0, now);
        assert!(is_exit_bet(&sale));
        sale.amount = 10.0;
        sale.shares = -10.0;
        assert!(is_exit_bet(&sale));

        let mut redemption = bet("r", WATCHED, 10.0, now);
        redemption.is_redemption = true;
        assert!(is_exit_bet(&redemption));
    }
}
