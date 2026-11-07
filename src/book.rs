//! The position book of one market: its legs, its aggregate, and its policy.
//!
//! A tracked market owns either one position (a binary market, stored in the
//! `tracked_markets` position columns) or one position per `market_answers` row
//! (a multiple-choice market). [`Book`] models both shapes the same way: one
//! [`Leg`] per owned position, plus a derived [`Position`] aggregate.
//!
//! ```text
//! Binary:   legs = [Leg { answer_id: None, .. }]        aggregate = the leg
//! Answers:  legs = [Leg { answer_id: Some(id), .. }]    aggregate = sum(legs)
//! ```
//!
//! # Column ownership
//!
//! `Book` keeps the loaded [`TrackedMarket`] for identity, lifecycle,
//! `budget_mana`, `realized_pnl_mana`, and the kind flags. Its **position
//! columns are inputs only**: after [`Book::load`], nothing reads
//! `market.yes_shares`, `market.no_shares`, or `market.invested_mana` again.
//! The legs are the truth and [`Book::persist`] is the only writer of those
//! columns. That rule is what stops a second source of truth from reappearing
//! inside the book.
//!
//! Price columns are deliberately more open: the book is one of three writers.
//! The trade path (`Book::observe_prices`) writes prices from a REST snapshot;
//! the live mirror (`MarketStore::market_prices_observed`, called from
//! `trade.rs` for websocket events and the batched reconcile refresh) writes
//! prices without touching any book; and [`Book::persist`]'s price write is a
//! re-observation of the loaded legs evaluated here before it is persisted.
//! Positions, invested, and PnL are never written by the mirror.
//!
//! | Fact | Owned by | Written by |
//! |---|---|---|
//! | `yes_shares`, `no_shares`, `invested_mana` per leg | [`Leg`] | [`Book::apply_buy`], [`Book::apply_sell`], [`Book::reconcile`] |
//! | root `invested_mana`, `yes_shares`, `no_shares` | [`Book::aggregate`] | `MarketStore::record_position` via [`Book::persist`] |
//! | `realized_pnl_mana` | `Book.market` | [`Book::apply_sell`] |
//! | `p_market_last` | [`Leg`] | [`Book::observe_prices`]; `MarketStore::market_prices_observed` (ws mirror + reconcile, outside the book) |
//! | answer rows' positions | [`Leg`] | [`Book::persist`] via `Db::update_answer_position` |
//! | answer rows' belief and text | triage / [`Book::observe_prices`] text only | unchanged from today |
//!
//! [`Book::persist`] writes answer positions first, then the root aggregate,
//! then the price. That order is preserved from the hand-written code it
//! replaces.
//!
//! # One planner
//!
//! [`Book::plan`] is the only trading policy. The three legitimate
//! binary/multiple-choice differences — the entry price band, the per-answer
//! budget fraction, and the sum-to-one projection — are [`TradeLimits`] data
//! selected by [`Book::shape`], not separate code paths.

use std::collections::HashMap;

use crate::belief;
use crate::config::Config;
use crate::db::Db;
use crate::error::{AppResult, anyhow, bail};
use crate::market_store::MarketStore;
use crate::types::{
    MarketStatus, TrackedAnswer, TrackedMarket, TradeAction, TradeDecision, available_budget,
};

/// Kelly fractions are capped at half of the available budget.
pub const MAX_KELLY_FRACTION: f64 = 0.5;

/// Probability bounds used by the sizing and decision math.
///
/// A market probability of exactly 0 or 1 makes Kelly division and log-odds
/// undefined, so the trading layer treats the probability as bounded away from
/// those endpoints.
pub const MIN_TRADING_PROB: f64 = 0.01;
pub const MAX_TRADING_PROB: f64 = 0.99;

/// Answers priced outside this band are never entered.
///
/// A multiple-choice market can contain answers that already resolved to 0 or 1
/// while the market is still open (23 of 53 in the MEGA market at probe time).
/// Entering at 99 cents for a one-cent edge is a bad trade, and a near-zero
/// price makes the Kelly denominator meaningless.
pub const MC_MIN_PRICE: f64 = 0.02;
pub const MC_MAX_PRICE: f64 = 0.98;

/// Tolerance for the budget invariant, in mana. Floating-point sums of budgets,
/// PnL, and invested amounts can differ by a few ULPs from zero.
pub const BUDGET_EPSILON: f64 = 1e-9;

/// Difference between local and remote share counts that is still considered
/// equal. Manifold reports fractional shares, so an exact comparison would warn
/// on every tick.
pub const POSITION_EPSILON: f64 = 1e-6;

/// The bot's position in one contract, as Manifold reports it.
///
/// `invested` is Manifold's cost basis for the shares still held. `profit` is
/// Manifold's total profit (realized plus unrealized), so it is diagnostic
/// only: the local `realized_pnl_mana` is not overwritten from it. The book
/// derives `profit` as zero because it tracks cost basis, not remote profit.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Position {
    pub yes_shares: f64,
    pub no_shares: f64,
    pub invested: f64,
    pub profit: f64,
}

/// Every position the bot holds in one market, split by answer.
///
/// Manifold returns one root summary row (`answerId` null) plus one row per
/// answer the user has ever bet on. The root row is not a position of its own:
/// its totals equal the sum of the answer rows. A mismatch means the response
/// was truncated, which [`MarketPositions::checksum_ok`] reports.
#[derive(Debug, Clone, Default)]
pub struct MarketPositions {
    /// The market-level summary row. Zero when Manifold returned no rows.
    pub root: Position,
    /// Positions keyed by answer ID, for multiple choice markets.
    pub answers: HashMap<String, Position>,
    /// Whether the response included a root summary row.
    pub saw_root: bool,
}

impl MarketPositions {
    /// The position for one answer, or a zero position when never traded.
    pub fn answer(&self, answer_id: &str) -> Position {
        self.answers.get(answer_id).copied().unwrap_or_default()
    }

    /// Sum of every per-answer row.
    pub fn totals(&self) -> Position {
        self.answers
            .values()
            .fold(Position::default(), |mut sum, row| {
                sum.yes_shares += row.yes_shares;
                sum.no_shares += row.no_shares;
                sum.invested += row.invested;
                sum.profit += row.profit;
                sum
            })
    }

    /// Whether the root summary still equals the sum of the answer rows.
    ///
    /// A response without a root row (no positions at all) is consistent.
    pub fn checksum_ok(&self) -> bool {
        if !self.saw_root {
            return true;
        }
        let totals = self.totals();
        (self.root.yes_shares - totals.yes_shares).abs() <= POSITION_EPSILON
            && (self.root.no_shares - totals.no_shares).abs() <= POSITION_EPSILON
            && (self.root.invested - totals.invested).abs() <= POSITION_EPSILON
    }
}

/// What Manifold says about one answer's price and label.
pub struct AnswerObservation<'a> {
    pub id: &'a str,
    pub text: &'a str,
    pub probability: Option<f64>,
}

/// One owned position inside a market.
#[derive(Debug, Clone)]
pub struct Leg {
    /// `None` for the single binary leg.
    pub answer_id: Option<String>,
    /// Answer label; empty for the binary leg.
    pub text: String,
    pub yes_shares: f64,
    pub no_shares: f64,
    pub invested_mana: f64,
    pub p_internal_log_odds: f64,
    pub p_market_last: f64,
    pub belief_updated_at: i64,
    /// Creation time of the owning row. Orders the legs exactly as
    /// `Db::list_answers` orders the answer rows.
    pub created_at: i64,
}

impl Leg {
    /// The single leg of a binary market, taken from the market row.
    fn binary(market: &TrackedMarket) -> Self {
        Self {
            answer_id: None,
            text: String::new(),
            yes_shares: market.yes_shares,
            no_shares: market.no_shares,
            invested_mana: market.invested_mana,
            p_internal_log_odds: market.p_internal_log_odds,
            p_market_last: market.p_market_last,
            belief_updated_at: market.belief_updated_at,
            created_at: market.created_at,
        }
    }

    fn from_answer(answer: &TrackedAnswer) -> Self {
        Self {
            answer_id: Some(answer.id.clone()),
            text: answer.text.clone(),
            yes_shares: answer.yes_shares,
            no_shares: answer.no_shares,
            invested_mana: answer.invested_mana,
            p_internal_log_odds: answer.p_internal_log_odds,
            p_market_last: answer.p_market_last,
            belief_updated_at: answer.belief_updated_at,
            created_at: answer.created_at,
        }
    }
}

/// Which leg layout a market uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    /// One leg without an answer ID.
    Binary,
    /// One leg per `market_answers` row.
    Answers,
}

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

/// Exposure limits for one market evaluation.
///
/// The binary/multiple-choice differences live here as data so the planner has
/// no shape branch beyond picking a value of this struct.
#[derive(Debug, Clone, Copy)]
pub struct TradeLimits {
    /// Maximum legs holding shares at the same time.
    pub max_positions: usize,
    /// Maximum new entries in one evaluation.
    pub max_new_per_tick: usize,
    /// Cap on one leg's entry as a fraction of `budget_mana`.
    /// `None` means no cap (binary).
    pub max_answer_fraction: Option<f64>,
    /// Entry price band, inclusive. `None` means no gate (binary).
    pub entry_price_band: Option<(f64, f64)>,
}

impl TradeLimits {
    /// Limits for multiple-choice markets, from process configuration.
    pub fn multiple_choice(config: &Config) -> Self {
        Self {
            max_positions: config.mc_max_positions,
            max_new_per_tick: config.mc_max_new_per_tick,
            max_answer_fraction: Some(config.mc_max_answer_fraction),
            entry_price_band: Some((MC_MIN_PRICE, MC_MAX_PRICE)),
        }
    }

    /// Limits for a binary market: one position, one entry, no fraction cap,
    /// no price band.
    pub fn binary() -> Self {
        Self {
            max_positions: 1,
            max_new_per_tick: 1,
            max_answer_fraction: None,
            entry_price_band: None,
        }
    }
}

/// The trading policy of the process.
#[derive(Debug, Clone, Copy)]
pub struct TradePolicy {
    pub params: TradeParams,
    pub multiple_choice: TradeLimits,
}

impl TradePolicy {
    /// Reads the whole trading policy from process configuration.
    pub fn from_config(config: &Config) -> Self {
        Self {
            params: TradeParams::from_config(config),
            multiple_choice: TradeLimits::multiple_choice(config),
        }
    }
}

/// A decayed view of one leg, the planner's input.
#[derive(Debug, Clone, PartialEq)]
pub struct LegView {
    /// Answer ID, `None` for the binary leg. Copied into a decision.
    pub answer_id: Option<String>,
    /// Decayed internal probability for this leg.
    pub p_internal: f64,
    /// Clamped market price for this leg.
    pub p_market: f64,
    pub yes_shares: f64,
    pub no_shares: f64,
}

/// One planned action plus the numbers the ledger needs.
#[derive(Debug, Clone, PartialEq)]
pub struct PlannedTrade {
    /// Index into [`Book::legs`].
    pub leg: usize,
    /// The action, named with the leg's answer ID.
    pub decision: TradeDecision,
    pub p_internal: f64,
    pub p_market: f64,
}

/// What one sell did to a leg.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Settlement {
    /// Shares actually sold, clamped to the held amount.
    pub sold: f64,
    /// Cost basis released by the sale.
    pub basis: f64,
    /// Proceeds minus basis.
    pub realized: f64,
}

/// Computes the edge of every leg.
///
/// Independent answers use their own belief minus their own price. A sum-to-one
/// market normalizes the internal distribution first, so the edges add up to
/// zero: an impulse that favors one answer necessarily creates offsetting
/// negative edges on its rivals. Without that projection the bot could buy two
/// mutually exclusive YES legs from one post.
fn answer_edges(views: &[LegView], sum_to_one: bool) -> Vec<f64> {
    if !sum_to_one {
        return views
            .iter()
            .map(|view| view.p_internal - view.p_market)
            .collect();
    }

    let total: f64 = views
        .iter()
        .map(|view| view.p_internal.clamp(0.0, 1.0))
        .sum();
    if !total.is_finite() || total <= 0.0 {
        return vec![0.0; views.len()];
    }

    views
        .iter()
        .map(|view| view.p_internal.clamp(0.0, 1.0) / total - view.p_market)
        .collect()
}

/// Chooses the actions for one evaluation over an arbitrary set of leg views.
///
/// Exits come first and are all-or-nothing per side. Entries are ordered by
/// absolute edge, sized with fractional Kelly, and capped by `limits`. The
/// returned vector may contain several actions; the caller executes them one by
/// one under the execution lock.
///
/// Sum-to-one markets are YES-only: a NO leg there is a basket over the field,
/// and Manifold may add internal arbitrage legs of its own. Independent
/// (`cpmm-multi-2`) markets allow both sides.
///
/// `allow_entries` is false for rows owned by a strategy (`snipe`, `copy`):
/// those strategies own their entries and size them against their isolated
/// pool, so the planner may only manage their exits. A refused or unfilled
/// strategy execution leaves a flat row with a real edge; without this gate
/// the regular evaluation would enter it as a news trade, bypassing the
/// strategy caps.
fn plan_views(
    views: &[LegView],
    sum_to_one: bool,
    available_budget: f64,
    budget_mana: f64,
    params: TradeParams,
    limits: TradeLimits,
    allow_entries: bool,
) -> Vec<PlannedTrade> {
    let edges = answer_edges(views, sum_to_one);
    let mut planned = Vec::new();

    for (leg, (view, edge)) in views.iter().zip(&edges).enumerate() {
        if view.yes_shares > 0.0 && *edge < params.exit_edge_threshold {
            planned.push(planned_exit(
                leg,
                view,
                TradeAction::SellYes,
                view.yes_shares,
            ));
        }
        if view.no_shares > 0.0 && !sum_to_one && *edge > -params.exit_edge_threshold {
            planned.push(planned_exit(leg, view, TradeAction::SellNo, view.no_shares));
        }
    }

    if !allow_entries {
        return planned;
    }

    let open_positions = views
        .iter()
        .filter(|view| view.yes_shares > 0.0 || view.no_shares > 0.0)
        .count();
    let mut slots = limits.max_positions.saturating_sub(open_positions);
    if slots == 0 || limits.max_new_per_tick == 0 {
        return planned;
    }

    let mut candidates: Vec<(usize, f64)> = edges
        .iter()
        .enumerate()
        .filter(|(index, edge)| {
            let view = &views[*index];
            view.yes_shares <= 0.0
                && view.no_shares <= 0.0
                && edge.abs() >= params.entry_edge_threshold
                && limits
                    .entry_price_band
                    .is_none_or(|(low, high)| (low..=high).contains(&view.p_market))
        })
        .map(|(index, edge)| (index, *edge))
        .collect();
    candidates.sort_by(|left, right| {
        right
            .1
            .abs()
            .total_cmp(&left.1.abs())
            .then_with(|| left.0.cmp(&right.0))
    });

    let mut remaining = available_budget;
    let mut new_entries = 0;
    for (leg, edge) in candidates {
        if slots == 0 || new_entries >= limits.max_new_per_tick {
            break;
        }

        let view = &views[leg];
        let action = if edge > params.entry_edge_threshold {
            TradeAction::BuyYes
        } else if !sum_to_one && edge < -params.entry_edge_threshold {
            TradeAction::BuyNo
        } else {
            continue;
        };

        let mut target = kelly_target_mana(edge, view.p_market, remaining, params);
        if let Some(fraction) = limits.max_answer_fraction {
            target = target.min(fraction * budget_mana);
        }
        if target < params.min_trade_size {
            continue;
        }

        planned.push(PlannedTrade {
            leg,
            decision: TradeDecision {
                action,
                amount_mana: target,
                shares: None,
                answer_id: view.answer_id.clone(),
            },
            p_internal: view.p_internal,
            p_market: view.p_market,
        });
        remaining = (remaining - target).max(0.0);
        slots -= 1;
        new_entries += 1;
    }

    planned
}

/// A planned all-or-nothing exit of one side.
fn planned_exit(leg: usize, view: &LegView, action: TradeAction, shares: f64) -> PlannedTrade {
    PlannedTrade {
        leg,
        decision: TradeDecision {
            action,
            amount_mana: 0.0,
            shares: Some(shares),
            answer_id: view.answer_id.clone(),
        },
        p_internal: view.p_internal,
        p_market: view.p_market,
    }
}

/// Everything the bot holds in one market, plus the policy over it.
pub struct Book {
    market: TrackedMarket,
    legs: Vec<Leg>,
    shape: Shape,
}

impl Book {
    /// Loads the row and, for a multiple-choice market, its answer rows.
    pub fn load(store: &MarketStore, db: &Db, market_id: &str) -> AppResult<Option<Self>> {
        let Some(market) = store.load(market_id)? else {
            return Ok(None);
        };

        let shape = shape_of(&market);
        let legs = match shape {
            Shape::Binary => vec![Leg::binary(&market)],
            Shape::Answers => answers_to_legs(db.list_answers(market_id)?),
        };

        Ok(Some(Self {
            market,
            legs,
            shape,
        }))
    }

    pub fn id(&self) -> &str {
        &self.market.id
    }

    pub fn status(&self) -> MarketStatus {
        self.market.status
    }

    pub fn shape(&self) -> Shape {
        self.shape
    }

    pub fn legs(&self) -> &[Leg] {
        &self.legs
    }

    /// The owning strategy of this row ([`crate::types::STRATEGY_NEWS`],
    /// [`crate::types::STRATEGY_SNIPE`], or [`crate::types::STRATEGY_COPY`]).
    pub fn strategy(&self) -> &str {
        &self.market.strategy
    }

    pub fn leg(&self, index: usize) -> &Leg {
        &self.legs[index]
    }

    /// Mana the market may still deploy.
    ///
    /// Reads the same formula as `types::available_budget` over the aggregate
    /// row, so the planner and the pre-buy budget check cannot disagree.
    pub fn available_budget(&self) -> f64 {
        available_budget(&self.market())
    }

    /// The root position, derived from the legs.
    ///
    /// `profit` is always zero: the book tracks cost basis, and Manifold's
    /// `profit` includes unrealized gains, which the budget accounting
    /// deliberately ignores.
    pub fn aggregate(&self) -> Position {
        self.legs.iter().fold(Position::default(), |mut sum, leg| {
            sum.yes_shares += leg.yes_shares;
            sum.no_shares += leg.no_shares;
            sum.invested += leg.invested_mana;
            sum
        })
    }

    /// The row with the aggregate position columns applied. Used by
    /// `sync_market` and by logging; never a second writer.
    pub fn market(&self) -> TrackedMarket {
        let aggregate = self.aggregate();
        let mut market = self.market.clone();
        market.invested_mana = aggregate.invested;
        market.yes_shares = aggregate.yes_shares;
        market.no_shares = aggregate.no_shares;

        // Only the binary path refreshes the root price. A multiple-choice
        // market has no single price, so its root column keeps the value it was
        // loaded with.
        if self.shape == Shape::Binary {
            if let Some(leg) = self.legs.first() {
                market.p_market_last = leg.p_market_last;
            }
        }

        market
    }

    /// Writes `outcome_type` and `should_answers_sum_to_one` when they differ.
    ///
    /// Rebuilds the legs when the shape changes, so a market that Manifold
    /// re-kinds mid-life gets the right leg set in the same call.
    pub fn refresh_kind(
        &mut self,
        store: &MarketStore,
        db: &Db,
        outcome_type: &str,
        sum_to_one: bool,
    ) -> AppResult<()> {
        if self.market.outcome_type == outcome_type
            && self.market.should_answers_sum_to_one == sum_to_one
        {
            return Ok(());
        }

        store.record_kind(&self.market.id, outcome_type, sum_to_one)?;
        tracing::info!(
            market_id = %self.market.id,
            previous_outcome_type = %self.market.outcome_type,
            outcome_type = %outcome_type,
            should_answers_sum_to_one = sum_to_one,
            "market kind updated from Manifold"
        );

        self.market.outcome_type = outcome_type.to_owned();
        self.market.should_answers_sum_to_one = sum_to_one;
        self.rebuild_legs(db)
    }

    /// Applies fresh prices.
    ///
    /// Binary: sets the single leg's price from `root_probability`, keeping the
    /// stored price when Manifold omits it. Answers: inserts missing answers
    /// anchored at their price (believing the price,
    /// `belief_updated_at = now`), updates changed text, and refreshes prices
    /// only. Belief columns are never overwritten.
    pub fn observe_prices(
        &mut self,
        db: &Db,
        now: i64,
        root_probability: Option<f64>,
        answers: &[AnswerObservation<'_>],
    ) -> AppResult<()> {
        match self.shape {
            Shape::Binary => {
                if let Some(leg) = self.legs.first_mut() {
                    leg.p_market_last = root_probability
                        .unwrap_or(leg.p_market_last)
                        .clamp(MIN_TRADING_PROB, MAX_TRADING_PROB);
                }
                Ok(())
            }
            Shape::Answers => {
                let market_id = self.market.id.clone();

                for remote in answers {
                    let price = remote
                        .probability
                        .unwrap_or(0.5)
                        .clamp(MIN_TRADING_PROB, MAX_TRADING_PROB);

                    match self
                        .legs
                        .iter_mut()
                        .find(|leg| leg.answer_id.as_deref() == Some(remote.id))
                    {
                        Some(leg) => {
                            if !remote.text.trim().is_empty() && leg.text != remote.text {
                                db.update_answer_text(&market_id, remote.id, remote.text)?;
                                leg.text = remote.text.to_owned();
                            }
                            if (leg.p_market_last - price).abs() > POSITION_EPSILON {
                                db.update_answer_market_price(&market_id, remote.id, price)?;
                                leg.p_market_last = price;
                            }
                        }
                        None => {
                            let answer = TrackedAnswer {
                                id: remote.id.to_owned(),
                                market_id: market_id.clone(),
                                text: remote.text.to_owned(),
                                p_internal_log_odds: belief::logit(price),
                                p_market_last: price,
                                belief_updated_at: now,
                                yes_shares: 0.0,
                                no_shares: 0.0,
                                created_at: now,
                                invested_mana: 0.0,
                            };
                            if db.insert_answer_if_missing(&answer)? {
                                tracing::info!(
                                    market_id = %market_id,
                                    answer_id = %answer.id,
                                    answer = %answer.text,
                                    "discovered a new multiple-choice answer"
                                );
                                self.legs.push(Leg::from_answer(&answer));
                            }
                        }
                    }
                }

                self.sort_legs();
                Ok(())
            }
        }
    }

    /// Replaces leg positions from Manifold, warns on a mismatch, and (for the
    /// answers shape) checks the root summary against the answer totals.
    ///
    /// Pure: no IO. [`Book::persist`] writes the result. Returns the number of
    /// legs whose saved position differed from the remote one, so a reconcile
    /// caller can summarize without re-implementing the comparison.
    pub fn reconcile(&mut self, positions: &MarketPositions) -> usize {
        let market_id = self.market.id.clone();
        let mut mismatches = 0;

        match self.shape {
            Shape::Binary => {
                let Some(leg) = self.legs.first_mut() else {
                    return 0;
                };
                let remote = positions.root;
                if leg_differs(leg, &remote) {
                    mismatches += 1;
                    tracing::warn!(
                        market_id = %market_id,
                        local_yes_shares = leg.yes_shares,
                        remote_yes_shares = remote.yes_shares,
                        local_no_shares = leg.no_shares,
                        remote_no_shares = remote.no_shares,
                        local_invested = leg.invested_mana,
                        remote_invested = remote.invested,
                        "local position differs from Manifold; trusting Manifold"
                    );
                }
                apply_remote(leg, &remote);
            }
            Shape::Answers => {
                if !positions.checksum_ok() {
                    let totals = positions.totals();
                    mismatches += 1;
                    tracing::warn!(
                        market_id = %market_id,
                        root_invested = positions.root.invested,
                        answer_invested = totals.invested,
                        root_yes_shares = positions.root.yes_shares,
                        answer_yes_shares = totals.yes_shares,
                        root_no_shares = positions.root.no_shares,
                        answer_no_shares = totals.no_shares,
                        "per-answer positions do not add up to the root summary; trusting the answer rows"
                    );
                }

                for leg in &mut self.legs {
                    let remote = positions.answer(leg.answer_id.as_deref().unwrap_or_default());
                    if leg_differs(leg, &remote) {
                        mismatches += 1;
                        tracing::warn!(
                            market_id = %market_id,
                            answer_id = ?leg.answer_id,
                            local_yes_shares = leg.yes_shares,
                            remote_yes_shares = remote.yes_shares,
                            local_no_shares = leg.no_shares,
                            remote_no_shares = remote.no_shares,
                            local_invested = leg.invested_mana,
                            remote_invested = remote.invested,
                            "local answer position differs from Manifold; trusting Manifold"
                        );
                    }
                    apply_remote(leg, &remote);
                }
            }
        }

        mismatches
    }

    /// Answer rows first, then the root aggregate and price.
    pub fn persist(&self, store: &MarketStore, db: &Db) -> AppResult<()> {
        let market = self.market();

        if self.shape == Shape::Answers {
            for leg in &self.legs {
                let Some(answer_id) = leg.answer_id.as_deref() else {
                    continue;
                };
                db.update_answer_position(
                    &market.id,
                    answer_id,
                    leg.yes_shares,
                    leg.no_shares,
                    leg.invested_mana,
                )?;
            }
        }

        store.record_position(
            &market.id,
            market.invested_mana,
            market.realized_pnl_mana,
            market.yes_shares,
            market.no_shares,
        )?;
        store.record_price(&market.id, market.p_market_last)
    }

    /// Records a filled buy on one leg. Rejects sell actions.
    pub fn apply_buy(
        &mut self,
        leg: usize,
        action: TradeAction,
        cost: f64,
        shares: f64,
    ) -> AppResult<()> {
        let leg = self.leg_mut(leg)?;
        match action {
            TradeAction::BuyYes => leg.yes_shares += shares,
            TradeAction::BuyNo => leg.no_shares += shares,
            other => bail!("apply_buy called with non-buy action {other}"),
        }
        leg.invested_mana += cost;
        Ok(())
    }

    /// Records a filled sell on one leg, adds the realized part to the market's
    /// PnL, and returns the settlement. Rejects buy actions and empty sides.
    ///
    /// The cost basis is proportional: with `sold` of `held` shares sold,
    /// `basis = invested * sold / held`. Every value is clamped at zero so
    /// rounding can never produce a negative position.
    pub fn apply_sell(
        &mut self,
        leg: usize,
        action: TradeAction,
        shares_sold: f64,
        proceeds: f64,
    ) -> AppResult<Settlement> {
        let leg = self.leg_mut(leg)?;
        let held = match action {
            TradeAction::SellYes => leg.yes_shares,
            TradeAction::SellNo => leg.no_shares,
            other => bail!("apply_sell called with non-sell action {other}"),
        };

        if !shares_sold.is_finite() || shares_sold <= 0.0 {
            bail!("sell must dispose of a positive finite share count, got {shares_sold}");
        }
        if held <= 0.0 {
            bail!("cannot {action}: the position holds no shares on that side");
        }

        let sold = shares_sold.min(held);
        let basis = (leg.invested_mana * sold / held).max(0.0);
        let realized = proceeds - basis;

        leg.invested_mana = (leg.invested_mana - basis).max(0.0);
        match action {
            TradeAction::SellYes => leg.yes_shares = (held - sold).max(0.0),
            _ => leg.no_shares = (held - sold).max(0.0),
        }

        self.market.realized_pnl_mana += realized;

        Ok(Settlement {
            sold,
            basis,
            realized,
        })
    }

    /// The sides a liquidation must sell: every leg's held YES and NO shares,
    /// empty when the market is resolved.
    pub fn sell_sides(&self, resolved: bool) -> Vec<(usize, TradeAction, f64)> {
        if resolved {
            return Vec::new();
        }

        let mut sides = Vec::new();
        for (index, leg) in self.legs.iter().enumerate() {
            if leg.yes_shares > 0.0 {
                sides.push((index, TradeAction::SellYes, leg.yes_shares));
            }
            if leg.no_shares > 0.0 {
                sides.push((index, TradeAction::SellNo, leg.no_shares));
            }
        }
        sides
    }

    /// Decayed internal probability and clamped price for one leg.
    pub fn view(&self, leg: usize, now: i64, halflife_hours: f64) -> LegView {
        let leg = &self.legs[leg];
        let p_market = leg.p_market_last.clamp(MIN_TRADING_PROB, MAX_TRADING_PROB);

        LegView {
            answer_id: leg.answer_id.clone(),
            p_internal: belief::current_p_internal(
                leg.p_internal_log_odds,
                leg.belief_updated_at,
                p_market,
                now,
                halflife_hours,
            ),
            p_market,
            yes_shares: leg.yes_shares,
            no_shares: leg.no_shares,
        }
    }

    /// One decision cycle: exits first, then ranked entries, capped by the
    /// limits that belong to this shape. Returns every action to execute.
    ///
    /// Only operator-owned (`news`) rows may open entries here. A `snipe` or
    /// `copy` row is planned **exits only**: its entries are sized by the
    /// strategy against the isolated pool and executed through
    /// `TradeExecutor::execute_planned`, so letting the planner add one would
    /// bypass the strategy caps and the copy daily cap.
    pub fn plan(&self, now: i64, halflife_hours: f64, policy: &TradePolicy) -> Vec<PlannedTrade> {
        let views: Vec<LegView> = self
            .legs
            .iter()
            .enumerate()
            .map(|(index, _)| self.view(index, now, halflife_hours))
            .collect();

        let limits = match self.shape {
            Shape::Binary => TradeLimits::binary(),
            Shape::Answers => policy.multiple_choice,
        };
        let sum_to_one = self.shape == Shape::Answers && self.market.should_answers_sum_to_one;

        plan_views(
            &views,
            sum_to_one,
            self.available_budget(),
            self.market.budget_mana,
            policy.params,
            limits,
            self.market.is_news(),
        )
    }

    fn leg_mut(&mut self, index: usize) -> AppResult<&mut Leg> {
        let market_id = self.market.id.clone();
        self.legs
            .get_mut(index)
            .ok_or_else(|| anyhow!("market {market_id} has no leg {index}"))
    }

    /// Rebuilds the legs after a kind change.
    fn rebuild_legs(&mut self, db: &Db) -> AppResult<()> {
        let shape = shape_of(&self.market);
        if shape == self.shape {
            return Ok(());
        }

        match shape {
            Shape::Binary => {
                let market = self.market();
                self.legs = vec![Leg::binary(&market)];
            }
            Shape::Answers => {
                self.legs = answers_to_legs(db.list_answers(&self.market.id)?);
            }
        }

        self.shape = shape;
        Ok(())
    }

    /// Orders the legs by creation time then answer ID, as `Db::list_answers`
    /// orders the answer rows.
    fn sort_legs(&mut self) {
        self.legs.sort_by(|left, right| {
            left.created_at.cmp(&right.created_at).then_with(|| {
                left.answer_id
                    .as_deref()
                    .unwrap_or_default()
                    .cmp(right.answer_id.as_deref().unwrap_or_default())
            })
        });
    }
}

/// The leg layout of a market row.
fn shape_of(market: &TrackedMarket) -> Shape {
    if market.is_multiple_choice() {
        Shape::Answers
    } else {
        Shape::Binary
    }
}

fn answers_to_legs(answers: Vec<TrackedAnswer>) -> Vec<Leg> {
    answers.iter().map(Leg::from_answer).collect()
}

/// Whether a local leg differs from the remote position beyond tolerance.
fn leg_differs(leg: &Leg, remote: &Position) -> bool {
    (leg.yes_shares - remote.yes_shares).abs() > POSITION_EPSILON
        || (leg.no_shares - remote.no_shares).abs() > POSITION_EPSILON
        || (leg.invested_mana - remote.invested).abs() > POSITION_EPSILON
}

/// Replaces a leg's position with Manifold's, clamped at zero.
///
/// `realized_pnl_mana` is deliberately absent: Manifold's `profit` includes
/// unrealized gains, while the budget accounting treats realized PnL as the
/// only source of recycled budget.
fn apply_remote(leg: &mut Leg, remote: &Position) {
    leg.yes_shares = remote.yes_shares.max(0.0);
    leg.no_shares = remote.no_shares.max(0.0);
    leg.invested_mana = remote.invested.max(0.0);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fixed clock so that `belief_updated_at = NOW` means "no decay".
    const NOW: i64 = 1_700_000_000;

    fn params() -> TradeParams {
        TradeParams {
            kelly_fraction: 0.25,
            entry_edge_threshold: 0.05,
            exit_edge_threshold: 0.01,
            min_trade_size: 1.0,
        }
    }

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-9,
            "expected {expected}, got {actual}"
        );
    }

    fn sample_market(id: &str) -> TrackedMarket {
        TrackedMarket {
            id: id.to_owned(),
            slug: format!("slug-{id}"),
            market_url: String::new(),
            question: format!("Question {id}?"),
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

    /// A binary market with a known price, belief, and position.
    fn market_with(
        p_market: f64,
        p_internal: f64,
        invested: f64,
        yes: f64,
        no: f64,
    ) -> TrackedMarket {
        let mut market = sample_market("Market_test");
        market.p_market_last = p_market;
        market.p_internal_log_odds = belief::logit(p_internal);
        market.belief_updated_at = NOW;
        market.invested_mana = invested;
        market.yes_shares = yes;
        market.no_shares = no;
        market
    }

    fn sample_answer(market_id: &str, id: &str, created_at: i64) -> TrackedAnswer {
        TrackedAnswer {
            id: id.to_owned(),
            market_id: market_id.to_owned(),
            text: format!("Answer {id}"),
            p_internal_log_odds: 0.0,
            p_market_last: 0.5,
            belief_updated_at: created_at,
            yes_shares: 0.0,
            no_shares: 0.0,
            created_at,
            invested_mana: 0.0,
        }
    }

    fn store_with(markets: &[TrackedMarket]) -> (Db, MarketStore) {
        let db = Db::open_in_memory().expect("in-memory DB");
        let store = MarketStore::new(&db).expect("store loads");
        for market in markets {
            store.insert(market).expect("insert market");
        }
        (db, store)
    }

    fn answers_store(market_id: &str, answers: &[&str]) -> (Db, MarketStore) {
        let db = Db::open_in_memory().expect("in-memory DB");
        let store = MarketStore::new(&db).expect("store loads");

        let mut market = sample_market(market_id);
        market.outcome_type = "MULTIPLE_CHOICE".to_owned();
        store.insert(&market).expect("insert market");

        for (index, id) in answers.iter().enumerate() {
            db.upsert_answer(&sample_answer(market_id, id, index as i64))
                .expect("insert answer");
        }

        (db, store)
    }

    fn limits(max_positions: usize, max_new_per_tick: usize, fraction: f64) -> TradeLimits {
        TradeLimits {
            max_positions,
            max_new_per_tick,
            max_answer_fraction: Some(fraction),
            entry_price_band: Some((MC_MIN_PRICE, MC_MAX_PRICE)),
        }
    }

    /// The policy a multiple-choice market would use.
    fn multiple_choice_policy(fraction: f64) -> TradePolicy {
        TradePolicy {
            params: params(),
            multiple_choice: limits(6, 2, fraction),
        }
    }

    // -- Load ---------------------------------------------------------------

    #[test]
    fn load_binary_market_has_one_leg_without_an_answer_id() {
        let (db, store) = store_with(&[sample_market("Market_test")]);

        let book = Book::load(&store, &db, "Market_test")
            .expect("load book")
            .expect("market is tracked");

        assert_eq!(book.shape(), Shape::Binary);
        assert_eq!(book.legs().len(), 1);
        assert_eq!(book.leg(0).answer_id, None);
        assert_eq!(book.leg(0).text, "");
        assert!(
            Book::load(&store, &db, "missing")
                .expect("load missing")
                .is_none()
        );
    }

    #[test]
    fn load_multiple_choice_market_has_one_leg_per_answer_row() {
        let (db, store) = answers_store("m1", &["a", "b"]);

        let book = Book::load(&store, &db, "m1")
            .expect("load book")
            .expect("market is tracked");

        assert_eq!(book.shape(), Shape::Answers);
        assert_eq!(book.legs().len(), 2);
        assert_eq!(book.leg(0).answer_id.as_deref(), Some("a"));
        assert_eq!(book.leg(1).answer_id.as_deref(), Some("b"));
        assert_eq!(book.leg(1).text, "Answer b");
    }

    // -- Aggregate accounting ----------------------------------------------

    #[test]
    fn a_buy_on_an_answer_leg_updates_the_root_aggregate() {
        let (db, store) = answers_store("m1", &["a", "b"]);
        let mut book = Book::load(&store, &db, "m1")
            .expect("load book")
            .expect("tracked");

        book.apply_buy(1, TradeAction::BuyYes, 10.0, 20.0)
            .expect("buy on answer b");

        let total: f64 = book.legs().iter().map(|leg| leg.invested_mana).sum();
        assert_close(book.aggregate().invested, total);
        assert_close(book.aggregate().invested, 10.0);
        assert_close(book.aggregate().yes_shares, 20.0);

        book.persist(&store, &db).expect("persist");
        let row = store.load("m1").expect("load").expect("row");
        assert_close(row.invested_mana, 10.0);
        assert_close(row.yes_shares, 20.0);
        assert_close(row.no_shares, 0.0);
    }

    #[test]
    fn a_sell_on_an_answer_leg_updates_realized_pnl_and_the_root_aggregate() {
        let (db, store) = answers_store("m1", &["a", "b"]);
        let mut book = Book::load(&store, &db, "m1")
            .expect("load book")
            .expect("tracked");
        book.apply_buy(0, TradeAction::BuyYes, 10.0, 20.0)
            .expect("buy");
        let before = book.available_budget();

        let settlement = book
            .apply_sell(0, TradeAction::SellYes, 20.0, 12.0)
            .expect("sell");

        assert_close(settlement.sold, 20.0);
        assert_close(settlement.realized, 2.0);
        assert_close(book.market().realized_pnl_mana, 2.0);
        assert_close(book.aggregate().invested, 0.0);
        assert_close(book.aggregate().yes_shares, 0.0);
        // The released cost basis and the profit both return to the budget.
        assert_close(book.available_budget(), before + 12.0);

        book.persist(&store, &db).expect("persist");
        let row = store.load("m1").expect("load").expect("row");
        assert_close(row.realized_pnl_mana, 2.0);
        assert_close(row.invested_mana, 0.0);
        assert_close(row.yes_shares, 0.0);
    }

    #[test]
    fn a_partial_sell_on_an_answer_leg_releases_a_proportional_cost_basis() {
        let (db, store) = answers_store("m1", &["a", "b"]);
        let mut book = Book::load(&store, &db, "m1")
            .expect("load book")
            .expect("tracked");
        book.apply_buy(0, TradeAction::BuyYes, 10.0, 20.0)
            .expect("buy");

        let settlement = book
            .apply_sell(0, TradeAction::SellYes, 10.0, 6.0)
            .expect("partial sell");

        assert_close(settlement.sold, 10.0);
        assert_close(settlement.basis, 5.0); // 10 * 10/20
        assert_close(settlement.realized, 1.0);
        assert_close(book.leg(0).invested_mana, 5.0);
        assert_close(book.leg(0).yes_shares, 10.0);
        assert_close(book.aggregate().invested, 5.0);
    }

    #[test]
    fn a_sell_on_a_flat_leg_is_rejected() {
        let (db, store) = answers_store("m1", &["a"]);
        let mut book = Book::load(&store, &db, "m1")
            .expect("load book")
            .expect("tracked");

        assert!(book.apply_sell(0, TradeAction::SellYes, 1.0, 1.0).is_err());
        assert!(book.apply_sell(0, TradeAction::SellNo, 1.0, 1.0).is_err());
    }

    #[test]
    fn apply_buy_rejects_sell_actions() {
        let (db, store) = store_with(&[sample_market("Market_test")]);
        let mut book = Book::load(&store, &db, "Market_test")
            .expect("load book")
            .expect("tracked");

        assert!(book.apply_buy(0, TradeAction::SellYes, 1.0, 1.0).is_err());
        assert!(book.apply_buy(0, TradeAction::SellNo, 1.0, 1.0).is_err());
    }

    // -- Binary accounting --------------------------------------------------

    #[test]
    fn apply_buy_adds_cost_and_shares_to_the_matching_side() {
        let (db, store) = store_with(&[sample_market("Market_test")]);
        let mut book = Book::load(&store, &db, "Market_test")
            .expect("load book")
            .expect("tracked");

        book.apply_buy(0, TradeAction::BuyYes, 10.0, 20.0)
            .expect("buy YES");
        assert_close(book.leg(0).invested_mana, 10.0);
        assert_close(book.leg(0).yes_shares, 20.0);
        assert_close(book.leg(0).no_shares, 0.0);

        book.apply_buy(0, TradeAction::BuyNo, 5.0, 10.0)
            .expect("buy NO");
        assert_close(book.leg(0).invested_mana, 15.0);
        assert_close(book.leg(0).no_shares, 10.0);
    }

    #[test]
    fn profitable_sell_increases_realized_pnl_and_available_budget() {
        let (db, store) = store_with(&[market_with(0.5, 0.5, 10.0, 20.0, 0.0)]);
        let mut book = Book::load(&store, &db, "Market_test")
            .expect("load book")
            .expect("tracked");
        let before = book.available_budget();

        book.apply_sell(0, TradeAction::SellYes, 20.0, 12.0)
            .expect("sell YES");

        assert_close(book.market().realized_pnl_mana, 2.0);
        assert_close(book.aggregate().invested, 0.0);
        assert_close(book.leg(0).yes_shares, 0.0);
        // The released cost basis and the profit both return to the budget.
        assert_close(book.available_budget(), before + 12.0);
    }

    #[test]
    fn partial_sell_uses_a_proportional_cost_basis() {
        let (db, store) = store_with(&[market_with(0.5, 0.5, 10.0, 20.0, 0.0)]);
        let mut book = Book::load(&store, &db, "Market_test")
            .expect("load book")
            .expect("tracked");
        let before = book.available_budget();

        book.apply_sell(0, TradeAction::SellYes, 10.0, 6.0)
            .expect("partial sell");

        assert_close(book.market().realized_pnl_mana, 1.0); // 6 - 10 * 10/20
        assert_close(book.aggregate().invested, 5.0);
        assert_close(book.leg(0).yes_shares, 10.0);
        assert_close(book.available_budget(), before + 6.0);
    }

    #[test]
    fn losing_sell_reduces_available_budget() {
        let (db, store) = store_with(&[market_with(0.5, 0.5, 10.0, 20.0, 0.0)]);
        let mut book = Book::load(&store, &db, "Market_test")
            .expect("load book")
            .expect("tracked");
        let before = book.available_budget();

        book.apply_sell(0, TradeAction::SellYes, 20.0, 3.0)
            .expect("sell YES");

        assert_close(book.market().realized_pnl_mana, -7.0);
        // The released basis minus the loss equals the actual proceeds.
        assert_close(book.available_budget(), before + 3.0);
    }

    #[test]
    fn sell_of_more_shares_than_held_is_clamped() {
        let (db, store) = store_with(&[market_with(0.5, 0.5, 10.0, 20.0, 0.0)]);
        let mut book = Book::load(&store, &db, "Market_test")
            .expect("load book")
            .expect("tracked");

        book.apply_sell(0, TradeAction::SellYes, 50.0, 12.0)
            .expect("clamped sell");

        assert_close(book.leg(0).yes_shares, 0.0);
        assert_close(book.aggregate().invested, 0.0);
        assert_close(book.market().realized_pnl_mana, 2.0);
    }

    #[test]
    fn apply_sell_rejects_buy_actions_and_empty_positions() {
        let (db, store) = store_with(&[market_with(0.5, 0.5, 10.0, 20.0, 0.0)]);
        let mut book = Book::load(&store, &db, "Market_test")
            .expect("load book")
            .expect("tracked");

        assert!(book.apply_sell(0, TradeAction::BuyYes, 1.0, 1.0).is_err());
        assert!(book.apply_sell(0, TradeAction::BuyNo, 1.0, 1.0).is_err());
        assert!(book.apply_sell(0, TradeAction::SellNo, 1.0, 1.0).is_err());
        assert!(book.apply_sell(0, TradeAction::SellYes, 0.0, 1.0).is_err());
    }

    // -- Reconciliation -----------------------------------------------------

    #[test]
    fn reconcile_trusts_the_answer_rows_when_the_root_checksum_disagrees() {
        let (db, store) = answers_store("m1", &["a", "b"]);
        let mut book = Book::load(&store, &db, "m1")
            .expect("load book")
            .expect("tracked");

        let mut positions = MarketPositions {
            saw_root: true,
            ..MarketPositions::default()
        };
        positions.root = Position {
            yes_shares: 14.0,
            no_shares: 3.0,
            invested: 9.0,
            profit: 0.0,
        };
        positions.answers.insert(
            "a".to_owned(),
            Position {
                yes_shares: 10.0,
                invested: 6.0,
                ..Position::default()
            },
        );
        positions.answers.insert(
            "b".to_owned(),
            Position {
                no_shares: 7.0,
                invested: 5.0,
                ..Position::default()
            },
        );

        assert!(!positions.checksum_ok(), "the fixture must be inconsistent");
        book.reconcile(&positions);

        assert_close(book.leg(0).yes_shares, 10.0);
        assert_close(book.leg(0).invested_mana, 6.0);
        assert_close(book.leg(1).no_shares, 7.0);
        assert_close(book.leg(1).invested_mana, 5.0);
        assert_close(book.aggregate().invested, 11.0);
        assert_close(book.market().invested_mana, 11.0);
        assert_close(book.market().yes_shares, 10.0);
    }

    #[test]
    fn reconcile_clamps_negative_remote_positions_to_zero() {
        let (db, store) = store_with(&[sample_market("Market_test")]);
        let mut book = Book::load(&store, &db, "Market_test")
            .expect("load book")
            .expect("tracked");

        book.reconcile(&MarketPositions {
            root: Position {
                yes_shares: -5.0,
                no_shares: -3.0,
                invested: -2.0,
                profit: 0.0,
            },
            ..MarketPositions::default()
        });

        assert_eq!(book.leg(0).yes_shares, 0.0);
        assert_eq!(book.leg(0).no_shares, 0.0);
        assert_eq!(book.leg(0).invested_mana, 0.0);
        assert_eq!(book.market().invested_mana, 0.0);
    }

    // -- Persistence --------------------------------------------------------

    #[test]
    fn persist_writes_answer_rows_then_the_root_aggregate() {
        let (db, store) = answers_store("m1", &["a", "b"]);
        let mut book = Book::load(&store, &db, "m1")
            .expect("load book")
            .expect("tracked");

        book.apply_buy(1, TradeAction::BuyYes, 10.0, 20.0)
            .expect("buy on answer b");
        book.persist(&store, &db).expect("persist");

        let answer = db
            .get_answer("m1", "b")
            .expect("load answer")
            .expect("tracked");
        assert_close(answer.yes_shares, 20.0);
        assert_close(answer.invested_mana, 10.0);

        let untouched = db
            .get_answer("m1", "a")
            .expect("load answer")
            .expect("tracked");
        assert_close(untouched.yes_shares, 0.0);
        assert_close(untouched.invested_mana, 0.0);

        let row = store.load("m1").expect("load").expect("row");
        assert_close(row.invested_mana, 10.0);
        assert_close(row.yes_shares, 20.0);
    }

    #[test]
    fn persist_does_not_insert_a_soft_deleted_market() {
        let (db, store) = answers_store("m1", &["a", "b"]);
        let mut book = Book::load(&store, &db, "m1")
            .expect("load book")
            .expect("tracked");
        book.apply_buy(0, TradeAction::BuyYes, 4.0, 8.0)
            .expect("buy");

        store.untrack("m1", 100).expect("untrack");
        assert!(store.cached("m1").is_none());

        book.persist(&store, &db).expect("late persist");

        assert!(
            store.cached("m1").is_none(),
            "a late write must not re-insert the market into the cache"
        );
        let row = store.load("m1").expect("load").expect("row kept");
        assert_close(row.invested_mana, 4.0);
        assert!(row.is_deleted());
    }

    // -- Planner over legs --------------------------------------------------

    #[test]
    fn a_binary_market_ignores_the_entry_price_band_and_the_answer_fraction_cap() {
        // A 0.01 price is outside the multiple-choice band, and the Kelly target
        // exceeds the 10% cap. A binary market has neither limit.
        let (db, store) = store_with(&[market_with(0.01, 0.9, 0.0, 0.0, 0.0)]);
        let book = Book::load(&store, &db, "Market_test")
            .expect("load book")
            .expect("tracked");

        let planned = book.plan(NOW, 4.0, &multiple_choice_policy(0.1));

        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].decision.action, TradeAction::BuyYes);
        assert_eq!(planned[0].leg, 0);
        assert_close(
            planned[0].decision.amount_mana,
            0.25 * (0.9 - 0.01) / (1.0 - 0.01) * 100.0,
        );
        assert!(
            planned[0].decision.amount_mana > 10.0,
            "the cap did not bind"
        );
    }

    #[test]
    fn a_strategy_row_never_opens_an_entry_but_still_exits_a_held_leg() {
        // A refused or unfilled strategy execution leaves a flat row with a
        // real edge. The planner must not enter it as a news trade.
        let mut flat = market_with(0.5, 0.8, 0.0, 0.0, 0.0);
        flat.strategy = crate::types::STRATEGY_COPY.to_owned();
        let (db, store) = store_with(&[flat]);
        let book = Book::load(&store, &db, "Market_test")
            .expect("load book")
            .expect("tracked");
        assert!(
            book.plan(NOW, 4.0, &multiple_choice_policy(0.25))
                .is_empty(),
            "a flat copy row must not open a planner entry"
        );

        // A held leg still exits on edge decay: exits are unchanged.
        let mut held = market_with(0.5, 0.505, 10.0, 20.0, 0.0);
        held.strategy = crate::types::STRATEGY_COPY.to_owned();
        let (db, store) = store_with(&[held]);
        let book = Book::load(&store, &db, "Market_test")
            .expect("load book")
            .expect("tracked");
        let planned = book.plan(NOW, 4.0, &multiple_choice_policy(0.25));
        assert_eq!(planned.len(), 1, "{planned:?}");
        assert_eq!(planned[0].decision.action, TradeAction::SellYes);
        assert_eq!(planned[0].decision.shares, Some(20.0));
    }

    #[test]
    fn plan_reports_the_leg_index_and_the_decayed_probabilities() {
        let (db, store) = answers_store("m1", &["a", "b"]);
        let mut book = Book::load(&store, &db, "m1")
            .expect("load book")
            .expect("tracked");
        // `a` is priced at its belief; `b` carries a stale, bullish belief.
        book.legs[0].p_market_last = 0.2;
        book.legs[0].p_internal_log_odds = belief::logit(0.2);
        book.legs[0].belief_updated_at = NOW;
        book.legs[1].p_market_last = 0.3;
        book.legs[1].p_internal_log_odds = belief::logit(0.9);
        book.legs[1].belief_updated_at = NOW - 4 * 3_600;

        let planned = book.plan(NOW, 4.0, &multiple_choice_policy(0.25));

        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].leg, 1);
        assert_eq!(planned[0].decision.action, TradeAction::BuyYes);
        assert_eq!(planned[0].decision.answer_id.as_deref(), Some("b"));
        assert_close(planned[0].p_market, 0.3);

        let expected = belief::current_p_internal(
            book.leg(1).p_internal_log_odds,
            book.leg(1).belief_updated_at,
            0.3,
            NOW,
            4.0,
        );
        assert_close(planned[0].p_internal, expected);
        assert!(
            planned[0].p_internal < 0.9,
            "one half-life of decay must move the belief toward the price"
        );
    }

    #[test]
    fn plan_sells_both_sides_of_a_binary_market_in_one_pass() {
        // Unreachable through the bot's own path (it never scales in), but
        // possible when the operator traded the same market by hand.
        let (db, store) = store_with(&[market_with(0.5, 0.5, 10.0, 20.0, 15.0)]);
        let book = Book::load(&store, &db, "Market_test")
            .expect("load book")
            .expect("tracked");

        let planned = book.plan(NOW, 4.0, &multiple_choice_policy(0.25));

        assert_eq!(planned.len(), 2);
        assert_eq!(planned[0].decision.action, TradeAction::SellYes);
        assert_eq!(planned[0].decision.shares, Some(20.0));
        assert_eq!(planned[0].leg, 0);
        assert_eq!(planned[1].decision.action, TradeAction::SellNo);
        assert_eq!(planned[1].decision.shares, Some(15.0));
        assert_eq!(planned[1].leg, 0);
    }

    #[test]
    fn available_budget_matches_the_root_market_row() {
        let mut market = market_with(0.5, 0.5, 10.0, 20.0, 0.0);
        market.realized_pnl_mana = 3.0;
        let (db, store) = store_with(&[market]);
        let mut book = Book::load(&store, &db, "Market_test")
            .expect("load book")
            .expect("tracked");

        assert_close(book.available_budget(), available_budget(&book.market()));
        book.apply_buy(0, TradeAction::BuyYes, 5.0, 5.0)
            .expect("buy");
        assert_close(book.available_budget(), available_budget(&book.market()));
        assert_close(book.available_budget(), 100.0 + 3.0 - 15.0);

        let (db, store) = answers_store("m1", &["a", "b"]);
        let mut book = Book::load(&store, &db, "m1")
            .expect("load book")
            .expect("tracked");
        book.apply_buy(0, TradeAction::BuyYes, 4.0, 8.0)
            .expect("buy YES");
        book.apply_buy(1, TradeAction::BuyNo, 6.0, 6.0)
            .expect("buy NO");

        assert_close(book.available_budget(), available_budget(&book.market()));
        assert_close(book.available_budget(), 90.0);
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

    // -- Decisions through the shared planner -------------------------------

    fn view(id: &str, p_internal: f64, p_market: f64, yes: f64, no: f64) -> LegView {
        LegView {
            answer_id: Some(id.to_owned()),
            p_internal,
            p_market,
            yes_shares: yes,
            no_shares: no,
        }
    }

    fn binary_view(p_internal: f64, p_market: f64, yes: f64, no: f64) -> LegView {
        LegView {
            answer_id: None,
            p_internal,
            p_market,
            yes_shares: yes,
            no_shares: no,
        }
    }

    fn actions_for(views: &[LegView], sum_to_one: bool, limits: TradeLimits) -> Vec<TradeDecision> {
        plan_views(views, sum_to_one, 100.0, 100.0, params(), limits, true)
            .into_iter()
            .map(|planned| planned.decision)
            .collect()
    }

    fn binary_actions(
        available_budget: f64,
        p_internal: f64,
        p_market: f64,
        yes: f64,
        no: f64,
    ) -> Vec<TradeDecision> {
        plan_views(
            &[binary_view(p_internal, p_market, yes, no)],
            false,
            available_budget,
            available_budget,
            params(),
            TradeLimits::binary(),
            true,
        )
        .into_iter()
        .map(|planned| planned.decision)
        .collect()
    }

    #[test]
    fn flat_market_with_a_bullish_edge_buys_yes() {
        let decisions = binary_actions(100.0, 0.6, 0.5, 0.0, 0.0);
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].action, TradeAction::BuyYes);
        assert_close(decisions[0].amount_mana, 5.0);
        assert_eq!(decisions[0].shares, None);
    }

    #[test]
    fn flat_market_with_a_bearish_edge_buys_no() {
        let decisions = binary_actions(100.0, 0.4, 0.5, 0.0, 0.0);
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].action, TradeAction::BuyNo);
        assert_close(decisions[0].amount_mana, 5.0);
        assert_eq!(decisions[0].shares, None);
    }

    #[test]
    fn edge_inside_the_entry_band_does_not_trade() {
        assert!(binary_actions(100.0, 0.54, 0.5, 0.0, 0.0).is_empty());
        assert!(binary_actions(100.0, 0.46, 0.5, 0.0, 0.0).is_empty());
        assert!(binary_actions(100.0, 0.5, 0.5, 0.0, 0.0).is_empty());
    }

    #[test]
    fn holding_yes_exits_when_the_edge_drops_below_the_exit_threshold() {
        let decisions = binary_actions(90.0, 0.505, 0.5, 20.0, 0.0);
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].action, TradeAction::SellYes);
        assert_eq!(decisions[0].shares, Some(20.0));
    }

    #[test]
    fn holding_no_exits_when_the_edge_rises_above_the_exit_threshold() {
        let decisions = binary_actions(90.0, 0.495, 0.5, 0.0, 15.0);
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].action, TradeAction::SellNo);
        assert_eq!(decisions[0].shares, Some(15.0));
    }

    #[test]
    fn holding_yes_does_not_exit_inside_the_band() {
        // edge = 0.02, above EXIT_EDGE_THRESHOLD, but below the entry threshold.
        assert!(binary_actions(90.0, 0.52, 0.5, 20.0, 0.0).is_empty());
        // A large edge does not scale the position in either.
        assert!(binary_actions(90.0, 0.7, 0.5, 20.0, 0.0).is_empty());
    }

    #[test]
    fn holding_no_does_not_exit_or_add_inside_the_band() {
        assert!(binary_actions(90.0, 0.48, 0.5, 0.0, 15.0).is_empty());
        assert!(binary_actions(90.0, 0.3, 0.5, 0.0, 15.0).is_empty());
    }

    #[test]
    fn entries_below_the_minimum_trade_size_are_skipped() {
        // A 10 mana budget with a 0.1 edge sizes to 0.5 mana, below the 1 mana
        // minimum.
        assert!(binary_actions(10.0, 0.6, 0.5, 0.0, 0.0).is_empty());

        // Increasing the edge makes the same budget tradable.
        let decisions = binary_actions(10.0, 0.8, 0.5, 0.0, 0.0);
        assert_eq!(decisions.len(), 1);
        assert_close(decisions[0].amount_mana, 1.5);
    }

    #[test]
    fn entries_use_only_the_available_budget() {
        // budget 100 + pnl 20 - invested 50 = 70 available; f = 0.05.
        let decisions = binary_actions(70.0, 0.6, 0.5, 0.0, 0.0);
        assert_eq!(decisions.len(), 1);
        assert_close(decisions[0].amount_mana, 3.5);
    }

    #[test]
    fn decision_never_sells_a_zero_share_position() {
        // A strongly bearish edge with no shares is an entry, never a sell.
        let decisions = binary_actions(100.0, 0.1, 0.5, 0.0, 0.0);
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].action, TradeAction::BuyNo);
    }

    #[test]
    fn sum_to_one_edges_are_normalized_and_block_the_offsetting_side() {
        // Raw beliefs sum to 1.0 here, so the normalized probabilities match.
        let views = [view("a", 0.8, 0.5, 0.0, 0.0), view("b", 0.2, 0.5, 0.0, 0.0)];

        let decisions = actions_for(&views, true, limits(6, 2, 0.25));

        assert_eq!(decisions.len(), 1, "only the favored answer may be bought");
        assert_eq!(decisions[0].action, TradeAction::BuyYes);
        assert_eq!(decisions[0].answer_id.as_deref(), Some("a"));
    }

    #[test]
    fn independent_edges_allow_both_sides() {
        let views = [view("a", 0.8, 0.5, 0.0, 0.0), view("b", 0.2, 0.5, 0.0, 0.0)];

        let decisions = actions_for(&views, false, limits(6, 2, 0.25));

        assert_eq!(decisions.len(), 2);
        assert_eq!(decisions[0].action, TradeAction::BuyYes);
        assert_eq!(decisions[0].answer_id.as_deref(), Some("a"));
        assert_eq!(decisions[1].action, TradeAction::BuyNo);
        assert_eq!(decisions[1].answer_id.as_deref(), Some("b"));
    }

    #[test]
    fn normalization_removes_a_duplicate_yes_entry() {
        // Two high raw beliefs cannot both stay cheap once the distribution is
        // projected onto prices that sum to one. The edges must cancel.
        let views = [
            view("a", 0.7, 0.55, 0.0, 0.0),
            view("b", 0.5, 0.45, 0.0, 0.0),
        ];
        let edges = answer_edges(&views, true);
        let total: f64 = 1.2;
        assert_close(edges[0], 0.7 / total - 0.55);
        assert_close(edges[1], 0.5 / total - 0.45);
        assert_close(edges.iter().sum::<f64>(), 0.0);
    }

    #[test]
    fn exits_are_planned_before_entries() {
        let views = [
            view("held", 0.4, 0.6, 10.0, 0.0),
            view("fresh", 0.9, 0.5, 0.0, 0.0),
        ];

        let decisions = actions_for(&views, false, limits(6, 2, 0.25));

        assert_eq!(decisions[0].action, TradeAction::SellYes);
        assert_eq!(decisions[0].answer_id.as_deref(), Some("held"));
        assert_eq!(decisions[0].shares, Some(10.0));
        assert!(
            decisions
                .iter()
                .any(|decision| decision.action == TradeAction::BuyYes
                    && decision.answer_id.as_deref() == Some("fresh"))
        );
    }

    #[test]
    fn new_entries_are_capped_per_tick() {
        let views: Vec<LegView> = (0..5)
            .map(|index| view(&format!("a{index}"), 0.9, 0.5, 0.0, 0.0))
            .collect();

        let decisions = actions_for(&views, false, limits(6, 2, 0.25));

        assert_eq!(decisions.len(), 2);
        assert!(
            decisions
                .iter()
                .all(|decision| decision.action == TradeAction::BuyYes)
        );
    }

    #[test]
    fn open_positions_consume_the_slots() {
        let views = [
            view("held1", 0.6, 0.5, 1.0, 0.0),
            view("held2", 0.6, 0.5, 1.0, 0.0),
            view("fresh", 0.9, 0.5, 0.0, 0.0),
        ];

        let decisions = actions_for(&views, false, limits(2, 2, 0.25));

        assert!(
            decisions
                .iter()
                .all(|decision| decision.action != TradeAction::BuyYes),
            "no slot is free: {decisions:?}"
        );
    }

    #[test]
    fn one_answer_cannot_take_more_than_its_share_of_the_budget() {
        let views = [view("a", 0.99, 0.2, 0.0, 0.0)];

        // Kelly would size this near 24.7 mana; the 10% cap binds first.
        let decisions = actions_for(&views, false, limits(6, 2, 0.1));

        let buy = decisions
            .iter()
            .find(|decision| decision.action == TradeAction::BuyYes)
            .expect("a buy");
        assert_close(buy.amount_mana, 10.0);
    }

    #[test]
    fn near_certain_answers_are_not_entered() {
        let views = [
            view("almost-won", 0.999, 0.99, 0.0, 0.0),
            view("almost-lost", 0.001, 0.01, 0.0, 0.0),
        ];

        let decisions = actions_for(&views, false, limits(6, 2, 0.25));

        assert!(decisions.is_empty(), "near-certain prices: {decisions:?}");
    }

    #[test]
    fn zero_internal_mass_produces_no_edges() {
        let views = [view("a", 0.0, 0.5, 0.0, 0.0), view("b", 0.0, 0.5, 0.0, 0.0)];

        let decisions = actions_for(&views, true, limits(6, 2, 0.25));

        assert!(decisions.is_empty());
    }

    // -- Liquidation --------------------------------------------------------

    #[test]
    fn liquidation_sells_only_the_sides_that_hold_shares() {
        let (db, store) = store_with(&[market_with(0.5, 0.5, 0.0, 20.0, 5.0)]);
        let mut book = Book::load(&store, &db, "Market_test")
            .expect("load book")
            .expect("tracked");

        assert_eq!(
            book.sell_sides(false),
            vec![
                (0, TradeAction::SellYes, 20.0),
                (0, TradeAction::SellNo, 5.0)
            ]
        );

        book.legs[0].no_shares = 0.0;
        assert_eq!(
            book.sell_sides(false),
            vec![(0, TradeAction::SellYes, 20.0)]
        );

        book.legs[0].yes_shares = 0.0;
        book.legs[0].no_shares = 5.0;
        assert_eq!(book.sell_sides(false), vec![(0, TradeAction::SellNo, 5.0)]);
    }

    #[test]
    fn liquidation_of_a_resolved_or_empty_position_sells_nothing() {
        let (db, store) = store_with(&[market_with(0.5, 0.5, 0.0, 20.0, 5.0)]);
        let mut book = Book::load(&store, &db, "Market_test")
            .expect("load book")
            .expect("tracked");

        assert!(book.sell_sides(true).is_empty());

        book.legs[0].yes_shares = 0.0;
        book.legs[0].no_shares = 0.0;
        assert!(book.sell_sides(false).is_empty());
    }
}
