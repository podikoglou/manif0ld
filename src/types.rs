//! Domain types shared by the ingestion, triage, belief, trading, and storage
//! layers.
//!
//! These structs mirror the SQLite rows in `migrations/001_init.sql` and use the
//! same string encodings for enums, so values translate losslessly between the
//! database and the rest of the application.

use std::fmt;
use std::str::FromStr;

use serde::Deserialize;

/// Manifold contract identifier.
pub type MarketId = String;

/// Strategy that owns a `tracked_markets` row created by `/track` or onboarding.
///
/// These are the operator's markets: they run through Sieve matching, triage,
/// and the news-latency pipeline, and they are the only rows shown by
/// operator-facing Discord listings.
pub const STRATEGY_NEWS: &str = "news";

/// Strategy that owns a market discovered autonomously by S2 new-market
/// sniping (`docs/ALGO.md` §S2).
///
/// Snipe markets have no Sieve rule, no Discord thread, and no per-market
/// budget of their own; they draw from the shared snipe pool
/// (`SNIPE_POOL_MANA`) and stay visible to the executor and reconciliation.
pub const STRATEGY_SNIPE: &str = "snipe";

/// Strategy that owns a market entered by S3b smart-money copy-trading
/// (`docs/ALGO.md` §S3, `docs/SPEC.md` §6.11).
///
/// A copy market is discovered from another trader's `global/new-bet`
/// broadcast, so it has no Sieve rule, no Discord thread, and no operator
/// budget; its per-market ceiling comes from `COPY_MAX_MANA` and the shared
/// copy pool, exactly as a snipe row's comes from `SNIPE_MAX_MANA`.
pub const STRATEGY_COPY: &str = "copy";

/// Lifecycle state of a tracked market.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MarketStatus {
    Active,
    Paused,
    Closed,
}

impl MarketStatus {
    /// Database and display encoding.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "ACTIVE",
            Self::Paused => "PAUSED",
            Self::Closed => "CLOSED",
        }
    }
}

impl FromStr for MarketStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_uppercase().as_str() {
            "ACTIVE" => Ok(Self::Active),
            "PAUSED" => Ok(Self::Paused),
            "CLOSED" => Ok(Self::Closed),
            other => Err(format!("unknown market status: {other}")),
        }
    }
}

impl fmt::Display for MarketStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Direction and strength of the evidence in a triaged post.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    StrongYes,
    Yes,
    Neutral,
    No,
    StrongNo,
}

impl Direction {
    /// Buckets a signed impulse scale into the ledger's coarse direction.
    ///
    /// The scale comes from [`crate::triage::assess`] and is in `[-1, 1]`. The
    /// buckets exist for display and for the `signal_ledger.direction` column;
    /// they never feed belief arithmetic.
    pub fn from_scale(scale: f64) -> Self {
        if scale >= 0.66 {
            Self::StrongYes
        } else if scale >= 0.2 {
            Self::Yes
        } else if scale <= -0.66 {
            Self::StrongNo
        } else if scale <= -0.2 {
            Self::No
        } else {
            Self::Neutral
        }
    }

    /// Database encoding.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::StrongYes => "strong_yes",
            Self::Yes => "yes",
            Self::Neutral => "neutral",
            Self::No => "no",
            Self::StrongNo => "strong_no",
        }
    }
}

impl FromStr for Direction {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "strong_yes" => Ok(Self::StrongYes),
            "yes" => Ok(Self::Yes),
            "neutral" => Ok(Self::Neutral),
            "no" => Ok(Self::No),
            "strong_no" => Ok(Self::StrongNo),
            other => Err(format!("unknown direction: {other}")),
        }
    }
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A Bluesky post after structural field extraction.
///
/// `text` is raw: no case folding, punctuation stripping, or Unicode
/// normalization is applied here. The Sieve engine performs lexical
/// normalization when matching.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractedPost {
    pub uri: String,
    pub did: String,
    pub handle: String,
    pub text: String,
    pub created_at: i64,
}

/// A market row from `tracked_markets`.
#[derive(Debug, Clone)]
pub struct TrackedMarket {
    pub id: String,
    pub slug: String,
    /// Canonical Manifold page URL captured at onboarding time. Empty for rows
    /// created before migration 002; renderers fall back to a slug-only URL.
    pub market_url: String,
    pub question: String,
    pub description: String,
    pub resolution_criteria: String,
    pub close_time: Option<i64>,
    pub status: MarketStatus,
    pub budget_mana: f64,
    pub invested_mana: f64,
    pub realized_pnl_mana: f64,
    pub yes_shares: f64,
    pub no_shares: f64,
    pub p_internal_log_odds: f64,
    pub p_market_last: f64,
    pub belief_updated_at: i64,
    pub sieve_source: String,
    pub discord_thread_id: String,
    pub created_at: i64,
    /// Unix seconds at which the operator ran `/untrack`; `None` while the
    /// market is still tracked.
    pub deleted_at: Option<i64>,
    /// Manifold `outcomeType`, for example `BINARY` or `MULTIPLE_CHOICE`.
    pub outcome_type: String,
    /// Whether the answers of a multiple-choice market compete for mass.
    ///
    /// True for `cpmm-multi-1` markets where exactly one answer wins; the
    /// trading layer then normalizes the internal distribution. False for
    /// independent (`cpmm-multi-2`) answer sets.
    pub should_answers_sum_to_one: bool,
    /// Component that owns this row: [`STRATEGY_NEWS`] or [`STRATEGY_SNIPE`].
    ///
    /// Migration 007 adds the column with a `'news'` default, so pre-existing
    /// rows and every `/track`ed market stay news markets.
    pub strategy: String,
}

impl TrackedMarket {
    /// Whether `/untrack` removed this market.
    ///
    /// A soft-deleted row stays in the database and keeps its ledger history,
    /// but it is hidden from listings and no longer trades.
    pub fn is_deleted(&self) -> bool {
        self.deleted_at.is_some()
    }

    /// Whether this market carries a separate position per answer.
    pub fn is_multiple_choice(&self) -> bool {
        self.outcome_type == "MULTIPLE_CHOICE"
    }

    /// Whether `/track` (or onboarding) owns this market.
    ///
    /// Operator-facing views filter on this; the executor does not.
    pub fn is_news(&self) -> bool {
        self.strategy == STRATEGY_NEWS
    }

    /// Whether S2 new-market sniping owns this market.
    pub fn is_snipe(&self) -> bool {
        self.strategy == STRATEGY_SNIPE
    }

    /// Whether S3b copy-trading owns this market.
    pub fn is_copy(&self) -> bool {
        self.strategy == STRATEGY_COPY
    }
}

/// A ledger action after a trade decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TradeAction {
    BuyYes,
    BuyNo,
    SellYes,
    SellNo,
}

impl TradeAction {
    /// Database and embed encoding.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BuyYes => "BUY_YES",
            Self::BuyNo => "BUY_NO",
            Self::SellYes => "SELL_YES",
            Self::SellNo => "SELL_NO",
        }
    }

    /// Manifold outcome identifier for this action.
    pub fn outcome(self) -> &'static str {
        match self {
            Self::BuyYes | Self::SellYes => "YES",
            Self::BuyNo | Self::SellNo => "NO",
        }
    }
}

impl FromStr for TradeAction {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_uppercase().as_str() {
            "BUY_YES" => Ok(Self::BuyYes),
            "BUY_NO" => Ok(Self::BuyNo),
            "SELL_YES" => Ok(Self::SellYes),
            "SELL_NO" => Ok(Self::SellNo),
            other => Err(format!("unknown trade action: {other}")),
        }
    }
}

impl fmt::Display for TradeAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Tracked answer within a multiple choice market.
#[derive(Debug, Clone, PartialEq)]
pub struct TrackedAnswer {
    pub id: String,
    pub market_id: MarketId,
    pub text: String,
    pub p_internal_log_odds: f64,
    pub p_market_last: f64,
    pub belief_updated_at: i64,
    pub yes_shares: f64,
    pub no_shares: f64,
    pub created_at: i64,
    /// Mana currently committed to this answer; the cost basis for a sell.
    pub invested_mana: f64,
}

/// Outcome of the trading rules for one market.
#[derive(Debug, Clone, PartialEq)]
pub struct TradeDecision {
    pub action: TradeAction,
    pub amount_mana: f64,
    /// Shares to sell for exit actions; `None` for market buys.
    pub shares: Option<f64>,
    /// Target answer ID for multiple choice markets (`None` for binary).
    pub answer_id: Option<String>,
}

/// A row from `signal_ledger`.
#[derive(Debug, Clone, PartialEq)]
pub struct SignalRecord {
    pub id: i64,
    pub market_id: MarketId,
    pub answer_id: Option<String>,
    pub post_uri: String,
    pub author_handle: String,
    pub post_text: String,
    pub material: bool,
    pub direction: Direction,
    /// Rounded expected strength, kept for the pre-probability column.
    pub strength: u8,
    pub impulse: f64,
    pub p_after: f64,
    /// Probability that the post carried evidence.
    pub p_evidence: Option<f64>,
    /// Probability mass on the alternative the post favored.
    pub p_target: Option<f64>,
    /// Expected evidence strength on the 0-3 rubric.
    pub strength_real: Option<f64>,
    /// Probability that the market already reflected the information.
    pub p_priced: Option<f64>,
    pub created_at: i64,
}

/// A row from `trade_ledger`.
#[derive(Debug, Clone, PartialEq)]
pub struct TradeRecord {
    pub id: i64,
    pub market_id: MarketId,
    pub answer_id: Option<String>,
    pub action: TradeAction,
    pub mana_amount: f64,
    pub shares_delta: f64,
    pub p_internal: f64,
    pub p_market: f64,
    pub trigger_post_uri: Option<String>,
    /// Wire `limitProb` sent with a buy (YES scale, whole-percent quantized).
    /// `None` for a sell or a row written before migration 009.
    pub limit_prob: Option<f64>,
    pub manifold_bet_id: Option<String>,
    pub created_at: i64,
}

/// A row from `snipe_ledger`: one S2 evaluation, including skips.
///
/// The ledger exists so `SNIPE_MIN_GAP`, `SNIPE_MIN_LIQ`, and the Kelly
/// fraction can be tuned from measured outcomes. Model outputs are optional:
/// a timed-out or failed decision call still records the evaluation.
#[derive(Debug, Clone, PartialEq)]
pub struct SnipeRecord {
    pub id: i64,
    pub contract_id: MarketId,
    /// Question text captured at evaluation time.
    pub question: String,
    /// Creator's `initialProb` when the market was seen.
    pub p_initial: Option<f64>,
    /// Model's fair probability.
    pub p_model: Option<f64>,
    /// `p_model - p_initial`.
    pub gap: Option<f64>,
    /// Chosen answer text for a multiple-choice market.
    pub answer: Option<String>,
    /// `BUY_YES`, `BUY_NO`, or `SKIP`.
    pub action: String,
    /// Why the strategy skipped, or why it bought.
    pub reason: Option<String>,
    /// Mana committed by this evaluation; zero for a skip.
    pub mana_amount: f64,
    /// Limit price sent with the buy, when one was sent.
    pub limit_prob: Option<f64>,
    /// Manifold bet ID when a bet was placed.
    pub manifold_bet_id: Option<String>,
    /// Decision-model cost in USD, when reported.
    pub cost_usd: Option<f64>,
    pub created_at: i64,
}

/// A row from `copy_ledger`: one S3b copy evaluation, skip, or exit signal.
///
/// Every gate after the watch-list lookup writes a row, so the thresholds
/// (`COPY_MIN_SIZE`, `COPY_MIN_INFORMED`, `COPY_MIN_EDGE`) are tunable from
/// measured outcomes. Model outputs are optional: a failed decision call still
/// records the evaluation with the numbers known so far.
#[derive(Debug, Clone, PartialEq)]
pub struct CopyRecord {
    pub id: i64,
    /// Manifold user ID whose bet triggered the evaluation.
    pub trader_id: String,
    pub contract_id: MarketId,
    /// Question text captured at evaluation time; empty when the market was
    /// skipped before it was described.
    pub question: String,
    /// The copied bet (provenance for duplicate detection and auditing).
    pub bet_id: Option<String>,
    /// Mana size of the copied bet.
    pub bet_amount: Option<f64>,
    /// Market probability before their bet.
    pub p_before: Option<f64>,
    /// Market probability after their bet.
    pub p_after: Option<f64>,
    /// `is_informed_bet` gate output.
    pub p_informed: Option<f64>,
    /// Model fair probability for YES.
    pub fair_prob: Option<f64>,
    /// Residual edge in YES space: `fair_prob − p_after`. Negative means the
    /// only value is on NO.
    pub gap: Option<f64>,
    /// `BUY_YES`, `BUY_NO`, `SKIP`, or `EXIT_SIGNAL`.
    pub action: String,
    /// Skip reason, follow rationale, or which leg the source trader exited.
    pub reason: Option<String>,
    /// Mana committed by this evaluation; zero for a skip or an exit signal.
    pub mana_amount: f64,
    /// Manifold bet ID when a bet was placed.
    pub manifold_bet_id: Option<String>,
    pub created_at: i64,
}

/// Actions written by S3b into `copy_ledger`.
///
/// Executed fills use the ordinary trade-action names so a
/// `SUM(mana_amount) ... WHERE action IN (BUY_YES, BUY_NO)` query is the
/// daily spend, while every refusal and every exit signal has its own marker.
pub const COPY_ACTION_SKIP: &str = "SKIP";
pub const COPY_ACTION_EXIT_SIGNAL: &str = "EXIT_SIGNAL";

/// Mana that the market may still deploy: budget, plus realized profit, minus
/// what is currently invested.
///
/// The result is not clamped. Callers that need a non-negative amount are
/// responsible for clamping, because a negative value signals an invariant
/// violation worth reporting.
pub fn available_budget(market: &TrackedMarket) -> f64 {
    market.budget_mana + market.realized_pnl_mana - market.invested_mana
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direction_from_scale_buckets_the_impulse() {
        assert_eq!(Direction::from_scale(1.0), Direction::StrongYes);
        assert_eq!(Direction::from_scale(0.66), Direction::StrongYes);
        assert_eq!(Direction::from_scale(0.65), Direction::Yes);
        assert_eq!(Direction::from_scale(0.2), Direction::Yes);
        assert_eq!(Direction::from_scale(0.19), Direction::Neutral);
        assert_eq!(Direction::from_scale(0.0), Direction::Neutral);
        assert_eq!(Direction::from_scale(-0.19), Direction::Neutral);
        assert_eq!(Direction::from_scale(-0.2), Direction::No);
        assert_eq!(Direction::from_scale(-0.65), Direction::No);
        assert_eq!(Direction::from_scale(-0.66), Direction::StrongNo);
        assert_eq!(Direction::from_scale(-1.0), Direction::StrongNo);
    }

    #[test]
    fn direction_as_str_and_from_str_roundtrip() {
        for direction in [
            Direction::StrongYes,
            Direction::Yes,
            Direction::Neutral,
            Direction::No,
            Direction::StrongNo,
        ] {
            let encoded = direction.as_str();
            let decoded: Direction = encoded.parse().expect("known direction should parse");
            assert_eq!(decoded, direction);
            assert_eq!(direction.to_string(), encoded);
        }
    }

    #[test]
    fn direction_from_str_is_case_insensitive() {
        assert_eq!(
            "STRONG_YES".parse::<Direction>().expect("valid"),
            Direction::StrongYes
        );
        assert_eq!(
            "Strong_No".parse::<Direction>().expect("valid"),
            Direction::StrongNo
        );
        assert!("sideways".parse::<Direction>().is_err());
    }

    #[test]
    fn direction_serde_uses_snake_case() {
        let parsed: Direction = serde_json::from_str("\"strong_yes\"")
            .expect("snake_case direction should deserialize");
        assert_eq!(parsed, Direction::StrongYes);
    }

    #[test]
    fn market_status_roundtrips() {
        for status in [
            MarketStatus::Active,
            MarketStatus::Paused,
            MarketStatus::Closed,
        ] {
            let encoded = status.as_str();
            let decoded: MarketStatus = encoded.parse().expect("known status should parse");
            assert_eq!(decoded, status);
            assert_eq!(status.to_string(), encoded);
        }
        assert_eq!(
            "active".parse::<MarketStatus>().expect("case-insensitive"),
            MarketStatus::Active
        );
        assert!("RESOLVED".parse::<MarketStatus>().is_err());
    }

    #[test]
    fn trade_action_roundtrips_and_reports_outcome() {
        for action in [
            TradeAction::BuyYes,
            TradeAction::BuyNo,
            TradeAction::SellYes,
            TradeAction::SellNo,
        ] {
            let encoded = action.as_str();
            let decoded: TradeAction = encoded.parse().expect("known action should parse");
            assert_eq!(decoded, action);
            assert_eq!(action.to_string(), encoded);
        }
        assert_eq!(TradeAction::BuyYes.outcome(), "YES");
        assert_eq!(TradeAction::SellYes.outcome(), "YES");
        assert_eq!(TradeAction::BuyNo.outcome(), "NO");
        assert_eq!(TradeAction::SellNo.outcome(), "NO");
        assert_eq!(
            "buy_yes".parse::<TradeAction>().expect("case-insensitive"),
            TradeAction::BuyYes
        );
        assert!("HOLD".parse::<TradeAction>().is_err());
    }

    #[test]
    fn available_budget_combines_budget_pnl_and_investment() {
        let market = TrackedMarket {
            id: "m".into(),
            slug: "m".into(),
            market_url: String::new(),
            question: "?".into(),
            description: String::new(),
            resolution_criteria: String::new(),
            close_time: None,
            status: MarketStatus::Active,
            budget_mana: 100.0,
            invested_mana: 40.0,
            realized_pnl_mana: 10.0,
            yes_shares: 0.0,
            no_shares: 0.0,
            p_internal_log_odds: 0.0,
            p_market_last: 0.5,
            belief_updated_at: 0,
            sieve_source: "match M { \"x\" }".into(),
            discord_thread_id: String::new(),
            created_at: 0,
            deleted_at: None,
            outcome_type: "BINARY".to_owned(),
            should_answers_sum_to_one: false,
            strategy: STRATEGY_NEWS.to_owned(),
        };
        assert_eq!(available_budget(&market), 70.0);
    }

    #[test]
    fn available_budget_is_not_clamped() {
        let mut market = TrackedMarket {
            id: "m".into(),
            slug: "m".into(),
            market_url: String::new(),
            question: "?".into(),
            description: String::new(),
            resolution_criteria: String::new(),
            close_time: None,
            status: MarketStatus::Active,
            budget_mana: 10.0,
            invested_mana: 25.0,
            realized_pnl_mana: -5.0,
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
            strategy: STRATEGY_NEWS.to_owned(),
        };
        assert_eq!(available_budget(&market), -20.0);
        market.invested_mana = 0.0;
        assert_eq!(available_budget(&market), 5.0);
    }
}
