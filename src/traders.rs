//! S3a smart-money trader leaderboard (`docs/ALGO.md` §3/S3 step 1,
//! `docs/SPEC.md` §6.10).
//!
//! This module answers one question cheaply and often: **which Manifold
//! traders are worth watching?** It builds a scored watch list from public
//! reads, persists it to `trader_scores`, and publishes it through
//! [`TraderBoard`] so the copy-execution task (S3b) can answer
//! "is this `global/new-bet` user on the list?" without touching the network.
//!
//! No bet is placed here. Copying has its own flag, its own gate
//! (`is_informed_bet`), and its own ledger (`copy_ledger`, created by
//! migration 008 for that task).
//!
//! # Verified endpoints (live probe, 2026-11-06)
//!
//! All reads are public; no auth header is needed, and the client still sends
//! the bot key (harmless, and the wire is one code path).
//!
//! | Request | Cost | Shape that was observed |
//! |---|---|---|
//! | `GET /v0/leaderboard?kind=profit&limit=100` | 1 | `[{"userId","score"}]`, highest all-time profit first |
//! | `GET /v0/leaderboard?kind=creator&limit=100` | 1 | same shape, ranked by creator earnings |
//! | `GET /v0/users/by-id?ids=a&ids=b…` | 1 per 100 ids | `[{"id","name","username","isBot","avatarUrl"}]`; unknown ids are absent |
//! | `GET /v0/get-user-portfolio-history?userId=…&period=weekly` | 1 | ~585 hourly points `{"timestamp","profit",…}` spanning ~168 h |
//! | `GET /v0/bets?userId=…&limit=N` | 1 | bare array of the newest N bet rows |
//!
//! Findings that shaped this module:
//!
//! - `GET /v0/leaderboard` caps `limit` at 100 and rejects `offset`; there is
//!   no time-window parameter (`period` is rejected). `kind=volume` answers an
//!   empty body (the request hangs until the client gives up), so the two
//!   usable ranked lists are `profit` and `creator`.
//! - The last point's `profit` in a `period=weekly` history equals the
//!   leaderboard's all-time `score` exactly (checked against the first-place
//!   account), so one history call is an authoritative source for **both**
//!   profit windows. A brand-new account's single point has no `profit`.
//! - Per-contract metrics (`GET /v0/get-user-contract-metrics-with-contracts`,
//!   also verified) carry `profit` and `from.week.profit`, but they paginate
//!   (1000 rows max, 5.6 MB for a whale) and the weekly sum is visibly biased
//!   when truncated: the top account's weekly sum moved by 35 000 mana between
//!   `limit=200` and `limit=1000`, against an exact −23 752 from history. That
//!   is why the weekly window comes from history, not from summed metrics.
//! - `GET /v0/get-user-portfolio` (live) exposes `dailyProfit`, not a weekly
//!   figure; `GET /v0/leagues?season=N` returns ~6 000 unsorted rows with
//!   `manaEarned` still zero early in the current season, so neither is used
//!   for scoring.
//!
//! # Request cost of one refresh
//!
//! Three universe requests (two leaderboard pages plus one bulk name lookup
//! per 100 candidates) and **two per enriched trader**: one history call for
//! the profit windows and one bounded bets call for the activity gate. The
//! default `TRADER_REQUEST_BUDGET=120` therefore fully enriches 58 traders per
//! pass; candidates are visited stalest-first, so the whole watch list
//! rotates across passes and everyone keeps their last score meanwhile. Raise
//! the budget to `3 + 2 × TRADER_WATCH_SIZE` (403 at the defaults) to refresh
//! the whole list in one pass — still inside one minute of the 500/min bucket,
//! and every request spends a [`Priority::Discovery`] token.
//!
//! # Scoring
//!
//! See [`score`] for the formula and its rationale. The score is a pure
//! function of the persisted columns, so a leaderboard row can be recomputed
//! from the database alone.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Duration;

use chrono::Utc;
use rusqlite::params;

use crate::config::Config;
use crate::db::Db;
use crate::error::{AppResult, Context};
use crate::rate::{Priority, RateLimiter, WRITE_COST_TOKENS};
use crate::trade::{ManifoldClient, PortfolioHistoryPoint, LEADERBOARD_MAX_LIMIT};

/// Largest refresh budget any configuration may claim, as a defensive ceiling
/// on a bulk discovery pass. At the defaults a full refresh needs 403
/// requests, so this leaves room for a much larger watch list without letting
/// a typo drain the shared 500/min bucket.
pub const MAX_REFRESH_REQUESTS: usize = 1_000;

/// Ranking used for the skill core of the candidate universe: all-time profit.
const PROFIT_LEADERBOARD: &str = "profit";

/// Ranking used for fresh blood: accounts that created high-volume markets.
/// They are active, large participants; the score still sorts them by measured
/// profit and [`ScoreParams::min_bets`] rejects one-shot accounts.
const CREATOR_LEADERBOARD: &str = "creator";

/// History window scored as "recent" profit. Its first point is ~7 days old.
const HISTORY_PERIOD: &str = "weekly";

/// User IDs per `GET /v0/users/by-id` request. 200 IDs worked live (~6.6 KB of
/// query string); 100 keeps the URL near 3.3 KB, comfortably inside any proxy
/// limit, and still resolves a whole 200-trader list in two requests.
const USER_LOOKUP_CHUNK: usize = 100;

/// Weight of the recent (weekly) profit term in [`score`].
pub const WEEKLY_WEIGHT: f64 = 0.7;

/// Weight of the all-time profit term in [`score`].
pub const ALL_TIME_WEIGHT: f64 = 0.3;

/// Mana at which the logarithm in [`score`] has compressed one `e`-fold step:
/// profits below it stay close to linear, and the heavy tail (millions of
/// mana) is squeezed instead of dominating every comparison.
pub const PROFIT_UNIT_MANA: f64 = 1_000.0;

/// One scored trader, as stored in `trader_scores` and served by
/// [`TraderBoard`].
#[derive(Debug, Clone, PartialEq)]
pub struct TraderScore {
    /// Manifold user ID (`/v0/get-user-portfolio` and `new-bet.userId`).
    pub user_id: String,
    /// Human-readable handle from `GET /v0/users/by-id`; `None` when the bulk
    /// lookup did not return the account (deleted or banned).
    pub username: Option<String>,
    /// Cumulative, deposit-adjusted all-time profit in mana.
    pub profit_all_time: f64,
    /// Profit over the trailing week, from the history series.
    pub profit_weekly: f64,
    /// Bets sampled by the activity probe, capped at [`ScoreParams::min_bets`].
    /// A full sample means "at least this many"; it gates, it does not rank.
    pub bet_count: i64,
    /// The blended score; [`f64::NEG_INFINITY`] for a gated account.
    pub score: f64,
    /// Unix seconds of the refresh that produced this row.
    pub updated_at: i64,
}

/// The tunables of [`score`], derived once per refresh from [`Config`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScoreParams {
    /// Minimum sampled bet count; below it the trader is ineligible.
    pub min_bets: usize,
    /// Weight of the weekly profit term ([`WEEKLY_WEIGHT`]).
    pub weekly_weight: f64,
    /// Weight of the all-time profit term ([`ALL_TIME_WEIGHT`]).
    pub all_time_weight: f64,
    /// Logarithmic compression unit in mana ([`PROFIT_UNIT_MANA`]).
    pub profit_unit: f64,
}

impl ScoreParams {
    /// Derives the parameters from the process configuration.
    ///
    /// Only `TRADER_MIN_BETS` is operator-tunable; the weights and the
    /// compression unit are scoring design, so they stay constants with one
    /// documented value rather than becoming another env-var surface.
    pub fn from_config(config: &Config) -> Self {
        Self {
            min_bets: config.trader_min_bets,
            weekly_weight: WEEKLY_WEIGHT,
            all_time_weight: ALL_TIME_WEIGHT,
            profit_unit: PROFIT_UNIT_MANA,
        }
    }
}

/// Blends a trader's profit windows into one comparable score.
///
/// ```text
/// compress(x) = sign(x) · ln(1 + |x| / profit_unit)
/// score       = weekly_weight · compress(profit_weekly)
///             + all_time_weight · compress(profit_all_time)
///             = -∞   when bet_count < min_bets
/// ```
///
/// Rationale:
///
/// - **Monotone in both windows.** A bigger profit in either window can only
///   raise the score, so ranking never contradicts the measured numbers.
/// - **Recent form dominates.** `weekly_weight` is 0.7 against 0.3 for the
///   all-time total: a trader who is currently good is a better bet to copy
///   than one whose edge is years old, and all-time profit over-rewards early
///   adopters who have since stopped trading.
/// - **Log compression, not percentiles.** Manifold profit is heavy-tailed
///   (top account ~5.8 M mana, hundredth place ~220 k, a healthy trader ~1 k).
///   Rank percentiles would need the whole cohort and would change every
///   refresh, so the same trader could score differently with no new trading.
///   `ln(1 + |x| / 1 000)` is deterministic, order-independent, unbounded
///   above (unlike clamping) and sign-preserving, so losers score negative.
/// - **Bet count gates, it does not blend.** The activity probe is capped at
///   `min_bets` to stay cheap, so every passing trader reports the same full
///   sample; blending it would only add noise. One lucky bet on one market
///   therefore never buys a watch-list slot.
pub fn score(
    profit_all_time: f64,
    profit_weekly: f64,
    bet_count: i64,
    params: &ScoreParams,
) -> f64 {
    if bet_count < params.min_bets as i64 {
        return f64::NEG_INFINITY;
    }
    params.weekly_weight * compress(profit_weekly, params.profit_unit)
        + params.all_time_weight * compress(profit_all_time, params.profit_unit)
}

/// Sign-preserving logarithmic compression; see [`score`].
///
/// Non-finite input (which the JSON parser should never produce) scores as
/// zero rather than poisoning the whole board with `NaN`.
fn compress(profit: f64, unit: f64) -> f64 {
    if !profit.is_finite() || !unit.is_finite() || unit <= 0.0 {
        return 0.0;
    }
    profit.signum() * (1.0 + profit.abs() / unit).ln()
}

/// Extracts `(profit_all_time, profit_weekly)` from a weekly history series.
///
/// The series is cumulative and deposit-adjusted, so the weekly window is the
/// difference between the last and first points. `None` means there is no
/// history at all and the trader cannot be scored this pass. A missing
/// `profit` field (a brand-new account's only point) counts as zero.
pub fn weekly_profit_from_history(points: &[PortfolioHistoryPoint]) -> Option<(f64, f64)> {
    let first = points.first()?;
    let last = points.last()?;
    let all_time = last.profit.unwrap_or(0.0);
    let weekly = all_time - first.profit.unwrap_or(0.0);
    Some((all_time, weekly))
}

/// The shared, read-mostly watch list.
///
/// Rows are the authority in `trader_scores`; this map is the read cache in
/// front of them, the same relationship [`crate::market_store::MarketStore`]
/// has with `tracked_markets`. The hourly refresh replaces the whole map in
/// one write after committing the database transaction, and readers (the copy
/// strategy, Discord, a status command) take a short read lock.
pub struct TraderBoard {
    scores: RwLock<HashMap<String, TraderScore>>,
}

impl TraderBoard {
    /// An empty board, used before the first refresh and by tests.
    pub fn empty() -> Self {
        Self {
            scores: RwLock::new(HashMap::new()),
        }
    }

    /// Loads the persisted board; this is what startup uses.
    pub fn load(db: &Db) -> AppResult<Self> {
        let conn = db.lock()?;
        let mut statement = conn
            .prepare(
                "SELECT user_id, username, profit_all_time, profit_weekly, bet_count, score, \
                 updated_at FROM trader_scores",
            )
            .context("preparing the trader-score listing")?;
        let rows = statement
            .query_map([], |row| {
                Ok(TraderScore {
                    user_id: row.get(0)?,
                    username: row.get(1)?,
                    profit_all_time: row.get::<_, Option<f64>>(2)?.unwrap_or(0.0),
                    profit_weekly: row.get::<_, Option<f64>>(3)?.unwrap_or(0.0),
                    bet_count: row.get::<_, Option<i64>>(4)?.unwrap_or(0),
                    score: row.get::<_, Option<f64>>(5)?.unwrap_or(f64::NEG_INFINITY),
                    updated_at: row.get(6)?,
                })
            })
            .context("querying trader scores")?;

        let mut scores = HashMap::new();
        for row in rows {
            let score = row.context("reading a trader-score row")?;
            scores.insert(score.user_id.clone(), score);
        }
        Ok(Self {
            scores: RwLock::new(scores),
        })
    }

    /// Number of scored traders currently held.
    pub fn len(&self) -> usize {
        self.read().len()
    }

    /// Whether the board is empty (no refresh has completed yet).
    pub fn is_empty(&self) -> bool {
        self.read().is_empty()
    }

    /// Returns one trader's score, if the watch list holds them.
    ///
    /// This is the copy strategy's hot path on `global/new-bet`.
    pub fn lookup(&self, user_id: &str) -> Option<TraderScore> {
        self.read().get(user_id).cloned()
    }

    /// Returns the `n` best scored traders, highest score first.
    ///
    /// Ties break on `user_id` so the order is stable across refreshes.
    /// Ineligible rows (non-finite score) are never returned.
    pub fn top(&self, n: usize) -> Vec<TraderScore> {
        top_scores(&self.read(), n)
    }

    /// Persists the given rows (replace-all) and swaps them into the cache.
    ///
    /// The database transaction commits before the map is swapped, so a reader
    /// never sees a row the database has not stored. Returns the row count.
    pub fn publish(&self, db: &Db, rows: Vec<TraderScore>) -> AppResult<usize> {
        persist(db, &rows)?;
        let mut scores = HashMap::with_capacity(rows.len());
        for row in rows {
            scores.insert(row.user_id.clone(), row);
        }
        let count = scores.len();
        *self.write() = scores;
        Ok(count)
    }

    /// Unix seconds of the last refresh for one trader, if known.
    fn updated_at(&self, user_id: &str) -> Option<i64> {
        self.read().get(user_id).map(|row| row.updated_at)
    }

    fn read(&self) -> RwLockReadGuard<'_, HashMap<String, TraderScore>> {
        self.scores
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write(&self) -> RwLockWriteGuard<'_, HashMap<String, TraderScore>> {
        self.scores
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Sorts and truncates one score map; shared by [`TraderBoard::top`] and the
/// refresh trim so both use the same ordering.
fn top_scores(scores: &HashMap<String, TraderScore>, n: usize) -> Vec<TraderScore> {
    let mut rows: Vec<TraderScore> = scores
        .values()
        .filter(|row| row.score.is_finite())
        .cloned()
        .collect();
    rows.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| left.user_id.cmp(&right.user_id))
    });
    rows.truncate(n);
    rows
}

/// Replaces every `trader_scores` row in one transaction.
fn persist(db: &Db, rows: &[TraderScore]) -> AppResult<()> {
    let mut conn = db.lock()?;
    let tx = conn
        .transaction()
        .context("starting the trader-score transaction")?;
    tx.execute("DELETE FROM trader_scores", [])
        .context("clearing trader scores")?;
    {
        let mut statement = tx
            .prepare(
                "INSERT INTO trader_scores \
                 (user_id, username, profit_all_time, profit_weekly, bet_count, score, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )
            .context("preparing the trader-score insert")?;
        for row in rows {
            statement
                .execute(params![
                    &row.user_id,
                    row.username.as_deref(),
                    row.profit_all_time,
                    row.profit_weekly,
                    row.bet_count,
                    row.score,
                    row.updated_at,
                ])
                .with_context(|| format!("inserting the score of trader {}", row.user_id))?;
        }
    }
    tx.commit().context("committing trader scores")
}

/// Charges one request against the refresh budget and the shared bucket.
///
/// Returns `false` when the budget is spent or the bucket is at the discovery
/// reserve; both are skips, not errors (`crate::rate`), and the caller stops
/// the pass and keeps the scores it already has.
fn spend(rate: &RateLimiter, used: &mut usize, budget: usize) -> bool {
    if *used >= budget {
        return false;
    }
    if rate.try_acquire(Priority::Discovery, WRITE_COST_TOKENS).is_err() {
        tracing::debug!(
            used = *used,
            budget,
            "trader refresh yielded: the rate bucket is at the discovery reserve"
        );
        return false;
    }
    *used += 1;
    true
}

/// Runs one leaderboard refresh and returns the number of kept traders.
///
/// Steps: discover the candidate universe (two leaderboard pages), resolve
/// names in bulk, then enrich candidates stalest-first within the request
/// budget — one history call and one activity probe each — score them, and
/// publish the trimmed board.
///
/// A failure of a universe request aborts the pass (`?`) because there is
/// nothing to score. Per-trader failures only skip that trader: the rest of
/// the pass and the previous scores stay useful.
pub async fn refresh(
    client: &ManifoldClient,
    db: &Db,
    config: &Config,
    board: &TraderBoard,
    rate: &RateLimiter,
) -> AppResult<usize> {
    let budget = config.trader_request_budget.min(MAX_REFRESH_REQUESTS);
    let watch_size = config.trader_watch_size;
    let params = ScoreParams::from_config(config);
    let now = Utc::now().timestamp();
    let mut used = 0usize;

    // 1. Candidate universe: the wire caps one ranked page at 100 entries.
    let mut candidates: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for kind in [PROFIT_LEADERBOARD, CREATOR_LEADERBOARD] {
        if !spend(rate, &mut used, budget) {
            tracing::debug!(used, budget, "trader refresh stopped while discovering");
            return Ok(board.len());
        }
        let entries = client
            .leaderboard(kind, LEADERBOARD_MAX_LIMIT)
            .await
            .with_context(|| format!("fetching the {kind} leaderboard"))?;
        for entry in entries {
            if seen.insert(entry.user_id.clone()) {
                candidates.push(entry.user_id);
            }
        }
    }
    candidates.truncate(watch_size);

    // 2. Names in bulk; a failure is cosmetic, so the scores still proceed.
    let mut usernames: HashMap<String, String> = HashMap::new();
    for chunk in candidates.chunks(USER_LOOKUP_CHUNK) {
        if !spend(rate, &mut used, budget) {
            break;
        }
        match client.users_by_id(chunk).await {
            Ok(users) => {
                for user in users {
                    if let Some(username) = user.username {
                        usernames.insert(user.id, username);
                    }
                }
            }
            Err(error) => {
                tracing::warn!(error = %error, "bulk trader name lookup failed; names stay empty");
                break;
            }
        }
    }

    // 3. Enrichment order: never-seen candidates first, then the stalest rows,
    //    so a budget-limited pass rotates through the whole list instead of
    //    refreshing the same head forever.
    let mut order: Vec<(i64, usize, String)> = candidates
        .iter()
        .enumerate()
        .map(|(rank, user_id)| (board.updated_at(user_id).unwrap_or(0), rank, user_id.clone()))
        .collect();
    order.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));

    let mut scores: HashMap<String, TraderScore> = board.read().clone();
    let mut enriched = 0usize;
    for (_, _, user_id) in order {
        // A trader costs exactly two requests; never start a half pair.
        if used + 2 > budget {
            break;
        }
        if !spend(rate, &mut used, budget) {
            break;
        }
        let history = match client.portfolio_history(&user_id, HISTORY_PERIOD).await {
            Ok(points) => points,
            Err(error) => {
                tracing::warn!(%user_id, error = %error, "trader history fetch failed; keeping the previous score");
                continue;
            }
        };
        let Some((profit_all_time, profit_weekly)) = weekly_profit_from_history(&history) else {
            tracing::debug!(%user_id, "trader has no portfolio history yet");
            continue;
        };

        if !spend(rate, &mut used, budget) {
            break;
        }
        let bet_count = match client.user_bet_count(&user_id, params.min_bets).await {
            Ok(count) => count,
            Err(error) => {
                tracing::warn!(%user_id, error = %error, "trader activity probe failed; keeping the previous score");
                continue;
            }
        };

        let value = score(
            profit_all_time,
            profit_weekly,
            bet_count as i64,
            &params,
        );
        if value.is_finite() {
            enriched += 1;
            let username = usernames
                .get(&user_id)
                .cloned()
                .or_else(|| scores.get(&user_id).and_then(|row| row.username.clone()));
            scores.insert(
                user_id.clone(),
                TraderScore {
                    username,
                    user_id,
                    profit_all_time,
                    profit_weekly,
                    bet_count: bet_count as i64,
                    score: value,
                    updated_at: now,
                },
            );
        } else {
            // A refreshed account that fails the activity gate loses its slot
            // instead of keeping a stale, unearned score.
            scores.remove(&user_id);
        }
    }

    let kept = board.publish(db, top_scores(&scores, watch_size))?;
    tracing::info!(
        candidates = candidates.len(),
        enriched,
        kept,
        requests = used,
        budget,
        "trader leaderboard refreshed"
    );
    Ok(kept)
}

/// Runs the leaderboard refresh forever, first pass immediately.
///
/// One pass per `TRADER_REFRESH_MIN` minutes. A failed pass logs and waits for
/// the next tick: the persisted board stays usable, and a transient API error
/// must not stop the task. Aborting the returned future (Ctrl-C, as in
/// [`crate::app`]) is safe at any await point — no lock is held across one.
pub async fn run(
    client: ManifoldClient,
    db: Db,
    config: Arc<Config>,
    board: Arc<TraderBoard>,
    rate: Arc<RateLimiter>,
) -> AppResult<()> {
    let period = Duration::from_secs((config.trader_refresh_min as u64).saturating_mul(60));
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // `interval`'s first tick is immediate. Consume it before the loop so the
    // loop body is the only first pass, then one pass per period.
    ticker.tick().await;

    loop {
        match refresh(&client, &db, &config, &board, &rate).await {
            Ok(kept) => tracing::debug!(kept, "trader refresh finished"),
            Err(error) => tracing::warn!(error = %error, "trader refresh failed; retrying next tick"),
        }
        ticker.tick().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rate::SystemClock;
    use serde_json::json;

    /// The documented defaults, built without touching the environment.
    fn params() -> ScoreParams {
        ScoreParams {
            min_bets: 25,
            weekly_weight: WEEKLY_WEIGHT,
            all_time_weight: ALL_TIME_WEIGHT,
            profit_unit: PROFIT_UNIT_MANA,
        }
    }

    fn trader(user_id: &str, score: f64) -> TraderScore {
        TraderScore {
            user_id: user_id.to_owned(),
            username: None,
            profit_all_time: score * 1_000.0,
            profit_weekly: score * 100.0,
            bet_count: 50,
            score,
            updated_at: 1,
        }
    }

    #[test]
    fn score_is_monotone_in_all_time_profit() {
        let params = params();
        let low = score(1_000.0, 100.0, 50, &params);
        let mid = score(10_000.0, 100.0, 50, &params);
        let high = score(1_000_000.0, 100.0, 50, &params);
        assert!(low < mid, "{low} < {mid}");
        assert!(mid < high, "{mid} < {high}");
    }

    #[test]
    fn score_is_monotone_in_weekly_profit() {
        let params = params();
        let low = score(5_000.0, -500.0, 50, &params);
        let mid = score(5_000.0, 0.0, 50, &params);
        let high = score(5_000.0, 2_000.0, 50, &params);
        assert!(low < mid, "{low} < {mid}");
        assert!(mid < high, "{mid} < {high}");
    }

    #[test]
    fn weekly_profit_outweighs_all_time_profit_at_equal_magnitude() {
        let params = params();
        // Equal absolute profit, opposite windows: recent form must win.
        let recent = score(0.0, 500.0, 50, &params);
        let ancient = score(500.0, 0.0, 50, &params);
        assert!(recent > ancient, "{recent} > {ancient}");
        assert_eq!(
            (recent - ancient).signum(),
            1.0,
            "the weekly weight must dominate the all-time weight"
        );
    }

    #[test]
    fn recency_weighting_breaks_ties_between_equal_totals() {
        let params = params();
        // Same all-time total, different recent form: the weekly-heavy trader
        // scores higher, which is the recency weighting the design promises.
        let weekly_heavy = score(10_000.0, 4_000.0, 50, &params);
        let evenly_earned = score(10_000.0, 1_000.0, 50, &params);
        assert!(weekly_heavy > evenly_earned);
    }

    #[test]
    fn score_rejects_traders_below_the_minimum_bet_count() {
        let params = params();
        assert_eq!(
            score(1_000_000.0, 500_000.0, 24, &params),
            f64::NEG_INFINITY,
            "one lucky bet must not buy a slot"
        );
        assert!(score(1_000.0, 10.0, 25, &params).is_finite());
    }

    #[test]
    fn score_keeps_losses_negative() {
        let params = params();
        let loser = score(-50_000.0, -10_000.0, 500, &params);
        assert!(loser < 0.0, "a losing trader must not rank above a flat one");
        assert!(loser > score(-500_000.0, -10_000.0, 500, &params));
        assert_eq!(score(0.0, 0.0, 25, &params), 0.0);
    }

    #[test]
    fn weekly_profit_uses_the_last_minus_the_first_history_point() {
        // Trimmed from a live `period=weekly` reply (2026-11-06): the first
        // point is ~7 days old, and cumulative profit is deposit-adjusted.
        let weekly = json!([
            {
                "timestamp": 1793401672779_i64,
                "profit": 5843848.58366028_f64,
                "balance": 1781797.103991025_f64,
                "investmentValue": 4250190.549261168_f64,
                "totalDeposits": 187041.8958238168_f64,
                "userId": "BgCeVUcOzkexeJpSPRNomWQaQaD3"
            },
            {
                "timestamp": 1794005089760_i64,
                "profit": 5820093.493731922_f64,
                "balance": 3294230.5418741135_f64,
                "investmentValue": 2754292.099697541_f64,
                "totalDeposits": 227328.995735215_f64,
                "userId": "BgCeVUcOzkexeJpSPRNomWQaQaD3"
            }
        ]);
        let points: Vec<PortfolioHistoryPoint> =
            serde_json::from_value(weekly).expect("history fixture decodes");

        let (all_time, weekly_profit) =
            weekly_profit_from_history(&points).expect("two points score");
        assert!((all_time - 5_820_093.493_731_922).abs() < 1e-6);
        assert!((weekly_profit - -23_755.089_928_358).abs() < 1e-6);
    }

    #[test]
    fn a_brand_new_account_has_no_weekly_change_and_no_history_is_unscoreable() {
        // A new account's only history point has no `profit` field.
        let single = json!([{
            "timestamp": 1794005146751_i64,
            "balance": 1000.0,
            "investmentValue": 0.0,
            "totalDeposits": 1000.0,
            "userId": "D4Og2LqJaqbYSB58DggyZ5GOVmZ2"
        }]);
        let points: Vec<PortfolioHistoryPoint> =
            serde_json::from_value(single).expect("single-point fixture decodes");
        assert_eq!(weekly_profit_from_history(&points), Some((0.0, 0.0)));

        assert_eq!(weekly_profit_from_history(&[]), None);
    }

    #[test]
    fn decodes_the_leaderboard_and_bulk_user_shapes() {
        // Trimmed from live `kind=profit` and `users/by-id` replies.
        let leaderboard = json!([
            { "userId": "BgCeVUcOzkexeJpSPRNomWQaQaD3", "score": 5820093.493731922_f64 },
            { "userId": "srFlJRuVlGa7SEJDM4cY9B5k4Lj2", "score": 3724431.856912597_f64 }
        ]);
        let entries: Vec<crate::trade::LeaderboardEntry> =
            serde_json::from_value(leaderboard).expect("leaderboard fixture decodes");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].user_id, "BgCeVUcOzkexeJpSPRNomWQaQaD3");
        assert!((entries[0].score - 5_820_093.493_731_922).abs() < 1e-6);
        assert!(entries[0].score > entries[1].score);

        let users = json!([
            {
                "id": "4juQfJkFnwX9nws3dFOpz4gc1mi2",
                "name": "jackson polack",
                "username": "jacksonpolack",
                "isBot": false,
                "avatarUrl": "https://example.invalid/a.png"
            },
            { "id": "deleted-user", "username": null }
        ]);
        let rows: Vec<crate::trade::LiteUserRow> =
            serde_json::from_value(users).expect("bulk user fixture decodes");
        assert_eq!(rows[0].username.as_deref(), Some("jacksonpolack"));
        assert_eq!(rows[1].username, None);
    }

    #[test]
    fn board_top_orders_by_score_then_id_and_respects_n() {
        let board = TraderBoard::empty();
        let db = Db::open_in_memory().expect("in-memory DB");
        board
            .publish(
                &db,
                vec![
                    trader("b", 2.0),
                    trader("a", 2.0),
                    trader("c", 3.0),
                    trader("d", f64::NEG_INFINITY),
                ],
            )
            .expect("publish");

        let top = board.top(10);
        assert_eq!(top.len(), 3, "an ineligible row is never returned");
        assert_eq!(
            top.iter().map(|row| row.user_id.as_str()).collect::<Vec<_>>(),
            vec!["c", "a", "b"],
            "score descending, ties by user id"
        );
        assert_eq!(board.top(1).len(), 1);
    }

    #[test]
    fn board_lookup_returns_only_known_traders() {
        let board = TraderBoard::empty();
        let db = Db::open_in_memory().expect("in-memory DB");
        board
            .publish(&db, vec![trader("known", 1.0)])
            .expect("publish");

        assert_eq!(board.lookup("known").map(|row| row.user_id), Some("known".to_owned()));
        assert_eq!(board.lookup("unknown"), None);
    }

    #[test]
    fn board_persistence_round_trips_and_replaces_all_rows() {
        let db = Db::open_in_memory().expect("in-memory DB");

        let first = TraderBoard::empty();
        let mut alice = trader("alice", 1.5);
        alice.username = Some("alice".to_owned());
        alice.profit_all_time = 12_345.0;
        alice.profit_weekly = -678.0;
        alice.bet_count = 25;
        alice.updated_at = 42;
        first
            .publish(&db, vec![alice.clone(), trader("bob", 0.5)])
            .expect("publish two");

        let reloaded = TraderBoard::load(&db).expect("reload");
        assert_eq!(reloaded.len(), 2);
        let stored = reloaded.lookup("alice").expect("alice persisted");
        assert_eq!(stored.username.as_deref(), Some("alice"));
        assert_eq!(stored.profit_all_time, 12_345.0);
        assert_eq!(stored.profit_weekly, -678.0);
        assert_eq!(stored.bet_count, 25);
        assert_eq!(stored.updated_at, 42);
        assert_eq!(stored, alice);

        // Replace-all: publishing one row drops the others, in the database
        // and in the cache of the board that performed the write.
        let second = TraderBoard::empty();
        second
            .publish(&db, vec![trader("carol", 9.0)])
            .expect("publish one");
        let reloaded = TraderBoard::load(&db).expect("reload after replace");
        assert_eq!(reloaded.len(), 1);
        assert_eq!(reloaded.lookup("carol").map(|row| row.user_id), Some("carol".to_owned()));
        assert_eq!(reloaded.lookup("alice"), None);
        assert_eq!(second.len(), 1);
    }

    #[test]
    fn a_gated_trader_is_dropped_from_the_board_only_when_refreshed() {
        // Model the refresh merge without touching the network: the board's
        // own map plus a gated row must not keep a non-finite score.
        let board = TraderBoard::empty();
        let db = Db::open_in_memory().expect("in-memory DB");
        board
            .publish(&db, vec![trader("kept", 1.0), trader("gated", 2.0)])
            .expect("publish");

        let rows = top_scores(&board.read(), 10);
        assert_eq!(rows.len(), 2);

        let mut merged: HashMap<String, TraderScore> = board.read().clone();
        merged.remove("gated");
        board
            .publish(&db, top_scores(&merged, 10))
            .expect("publish trim");
        assert_eq!(board.len(), 1);
        assert_eq!(board.lookup("gated"), None);
    }

    /// End-to-end check of the live wire, in the style of the other `live_*`
    /// tests in this crate: ignored by default because it spends real requests
    /// and needs network access.
    #[test]
    #[ignore = "network call; hits the public Manifold API (about 23 requests)"]
    fn live_refresh_scores_real_traders() {
        let mut config = Config::from_env().expect("process configuration with .env");
        // A deliberately small pass: two leaderboard pages, one name lookup,
        // then five traders at two requests each.
        config.trader_watch_size = 10;
        config.trader_request_budget = 13;
        config.trader_min_bets = 25;

        let client = ManifoldClient::for_test(crate::trade::MANIFOLD_BASE_URL);
        let db = Db::open_in_memory().expect("in-memory DB");
        let board = TraderBoard::empty();
        let rate = RateLimiter::per_minute(config.rate_limit_per_min, Box::new(SystemClock));

        let runtime = tokio::runtime::Runtime::new().expect("runtime builds");
        let kept = runtime
            .block_on(refresh(&client, &db, &config, &board, &rate))
            .expect("live refresh");

        assert!(kept > 0, "the live leaderboard scored nobody");
        let top = board.top(10);
        assert!(top.len() <= 10);
        assert!(
            top.windows(2).all(|pair| pair[0].score >= pair[1].score),
            "the board must be score-ordered: {top:?}"
        );
        assert!(
            top.iter().any(|row| row.username.is_some()),
            "the bulk name lookup returned nothing: {top:?}"
        );
        for row in &top {
            assert!(row.score.is_finite());
            assert!(row.bet_count >= config.trader_min_bets as i64);
            assert!(row.profit_all_time.is_finite() && row.profit_weekly.is_finite());
        }
        eprintln!("live top: {:#?}", top.first());
    }
}
