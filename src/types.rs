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
    /// Signed weight used by the belief engine.
    pub fn polarity(self) -> f64 {
        match self {
            Self::StrongYes => 1.0,
            Self::Yes => 0.5,
            Self::Neutral => 0.0,
            Self::No => -0.5,
            Self::StrongNo => -1.0,
        }
    }

    /// Database and LLM encoding.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::StrongYes => "strong_yes",
            Self::Yes => "yes",
            Self::Neutral => "neutral",
            Self::No => "no",
            Self::StrongNo => "strong_no",
        }
    }

    /// Whether this direction can carry a material signal.
    ///
    /// A neutral direction never moves belief, regardless of the model's
    /// `material` flag or strength score.
    pub fn is_material_candidate(&self) -> bool {
        !matches!(self, Self::Neutral)
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
}

/// A Bluesky account whose posts bypass Sieve matching for one market.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VipEntry {
    pub did: String,
    pub handle: String,
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

/// Parsed response from the triage LLM.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TriageResult {
    pub material: bool,
    #[serde(default)]
    pub direction: Option<Direction>,
    /// Evidence strength, 0 = noise through 3 = definitive.
    #[serde(default)]
    pub strength: u8,
    #[serde(default)]
    pub rationale: String,
    /// Answers affected by the post for multiple choice markets.
    #[serde(default)]
    pub affected_answers: Vec<AnswerTriage>,
}

/// An individual answer evaluation in a multiple choice market.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct AnswerTriage {
    pub answer_id: String,
    pub direction: Direction,
    #[serde(default)]
    pub strength: u8,
    #[serde(default)]
    pub rationale: String,
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
    pub strength: u8,
    pub impulse: f64,
    pub p_after: f64,
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
    pub manifold_bet_id: Option<String>,
    pub created_at: i64,
}

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
    fn direction_polarity_maps_to_expected_weights() {
        assert_eq!(Direction::StrongYes.polarity(), 1.0);
        assert_eq!(Direction::Yes.polarity(), 0.5);
        assert_eq!(Direction::Neutral.polarity(), 0.0);
        assert_eq!(Direction::No.polarity(), -0.5);
        assert_eq!(Direction::StrongNo.polarity(), -1.0);
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
    fn neutral_is_not_a_material_candidate() {
        assert!(!Direction::Neutral.is_material_candidate());
        for direction in [
            Direction::StrongYes,
            Direction::Yes,
            Direction::No,
            Direction::StrongNo,
        ] {
            assert!(direction.is_material_candidate());
        }
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
        };
        assert_eq!(available_budget(&market), 70.0);
    }

    #[test]
    fn available_budget_is_not_clamped() {
        let mut market = TrackedMarket {
            id: "m".into(),
            slug: "m".into(),
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
        };
        assert_eq!(available_budget(&market), -20.0);
        market.invested_mana = 0.0;
        assert_eq!(available_budget(&market), 5.0);
    }

    #[test]
    fn triage_result_deserializes() {
        let parsed: TriageResult = serde_json::from_str(
            r#"{
                "material": true,
                "direction": "no",
                "strength": 2,
                "rationale": "Official denial."
            }"#,
        )
        .expect("triage JSON should deserialize");

        assert_eq!(
            parsed,
            TriageResult {
                material: true,
                direction: Some(Direction::No),
                strength: 2,
                rationale: "Official denial.".into(),
                affected_answers: Vec::new(),
            }
        );
    }
}
