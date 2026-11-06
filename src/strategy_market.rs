//! Shared construction of a strategy-owned market row from a REST snapshot.
//!
//! S2 new-market sniping and S3b copy-trading both discover a market without
//! an operator `/track`, so both must materialise the same row shape: no Sieve
//! rule, no Discord thread, the strategy name as the owner, one per-market
//! budget equal to that strategy's ceiling, and the model's fair probability
//! in the log-odds belief column. The row construction lives here once, so the
//! two strategies cannot drift apart (a difference in this shape would change
//! how the planner and the reconcile pass treat the row).
//!
//! The row is inserted with `invested_mana = 0`, so the ordinary
//! `budget + realized_pnl − invested` invariant applies unchanged from the
//! first fill.

use crate::belief;
use crate::db::Db;
use crate::error::AppResult;
use crate::market_store::MarketStore;
use crate::trade::{millis_to_seconds, MarketSnapshot};
use crate::types::{MarketStatus, TrackedAnswer, TrackedMarket};

/// Everything one strategy-owned market row needs.
pub struct MarketRow<'a> {
    pub snapshot: &'a MarketSnapshot,
    /// Slug to use when the snapshot has none (a stub's slug, or empty).
    pub fallback_slug: &'a str,
    /// The market price the strategy saw: `probability` on a binary market or
    /// the chosen answer's price on a multiple-choice one.
    pub p_market: f64,
    /// The model's fair probability; stored as a log-odds belief.
    pub p_internal: f64,
    /// Per-market budget ceiling, never the actual trade size.
    pub budget_mana: f64,
    /// [`crate::types::STRATEGY_SNIPE`] or [`crate::types::STRATEGY_COPY`].
    pub strategy: &'a str,
    /// Unix seconds of the evaluation.
    pub now: i64,
}

/// Inserts the market row and one row per snapshot answer.
///
/// The answers are written for every market, not only multiple-choice ones:
/// a binary snapshot simply carries no answers, so the loop is a no-op there.
pub fn insert_market_row(store: &MarketStore, db: &Db, row: MarketRow<'_>) -> AppResult<()> {
    let snapshot = row.snapshot;
    let slug = if snapshot.slug.trim().is_empty() {
        row.fallback_slug.to_owned()
    } else {
        snapshot.slug.clone()
    };
    let market_url = if !snapshot.url.trim().is_empty() {
        snapshot.url.clone()
    } else if slug.is_empty() {
        String::new()
    } else {
        format!("https://manifold.markets/{slug}")
    };

    store.insert(&TrackedMarket {
        id: snapshot.id.clone(),
        slug,
        market_url,
        question: snapshot.question.clone(),
        description: snapshot.text_description.clone(),
        resolution_criteria: resolution_criteria(snapshot).to_owned(),
        close_time: snapshot.close_time_ms.map(millis_to_seconds),
        status: MarketStatus::Active,
        budget_mana: row.budget_mana,
        invested_mana: 0.0,
        realized_pnl_mana: 0.0,
        yes_shares: 0.0,
        no_shares: 0.0,
        p_internal_log_odds: belief::logit(row.p_internal),
        p_market_last: row.p_market.clamp(0.01, 0.99),
        belief_updated_at: row.now,
        sieve_source: String::new(),
        discord_thread_id: String::new(),
        created_at: row.now,
        deleted_at: None,
        outcome_type: snapshot.outcome_type.clone(),
        should_answers_sum_to_one: snapshot.should_answers_sum_to_one,
        strategy: row.strategy.to_owned(),
    })?;

    for answer in snapshot.answers.as_deref().unwrap_or(&[]) {
        let price = answer.probability.unwrap_or(0.5).clamp(0.01, 0.99);
        db.upsert_answer(&TrackedAnswer {
            id: answer.id.clone(),
            market_id: snapshot.id.clone(),
            text: answer.text.clone(),
            p_internal_log_odds: belief::logit(price),
            p_market_last: price,
            belief_updated_at: row.now,
            yes_shares: 0.0,
            no_shares: 0.0,
            created_at: row.now,
            invested_mana: 0.0,
        })?;
    }

    Ok(())
}

/// The resolution definition used in decision states and on the market row:
/// explicit criteria when present, description otherwise.
pub fn resolution_criteria(snapshot: &MarketSnapshot) -> &str {
    snapshot
        .resolution_criteria
        .as_deref()
        .filter(|text| !text.trim().is_empty())
        .unwrap_or(&snapshot.text_description)
}
