//! The single writer for market state.
//!
//! Market state exists in two forms: the `tracked_markets` row and an in-memory
//! snapshot that the hot matching path reads without a SQLite query per post.
//! [`MarketStore`] owns both. No other module emits SQL against
//! `tracked_markets`; the row-owning methods live here rather than on
//! [`Db`](crate::db::Db) so that forgetting the cache update is impossible.
//!
//! # Column ownership
//!
//! Every column group has exactly one intent method. A caller that wants to
//! change one group cannot accidentally rewrite another, which is what makes
//! concurrent writers safe:
//!
//! | Column group | Intent |
//! |---|---|
//! | `status`, `deleted_at` | [`MarketStore::close`], [`MarketStore::untrack`], [`MarketStore::record_status`] |
//! | `budget_mana` | [`MarketStore::record_budget`] |
//! | `p_internal_log_odds`, `belief_updated_at` | [`MarketStore::record_belief`] |
//! | `p_market_last` | [`MarketStore::record_price`] |
//! | `invested_mana`, `realized_pnl_mana`, `yes_shares`, `no_shares` | [`MarketStore::record_position`] |
//! | `outcome_type`, `should_answers_sum_to_one` | [`MarketStore::record_kind`] |
//!
//! Belief and price are deliberately separate. The belief engine decays toward
//! the current market price, which it reads as a parameter at read time, so a
//! newer price next to an older `belief_updated_at` is the intended model. When
//! one write covered both groups, a stale trade snapshot could revert a newer
//! belief and a pause.
//!
//! # Rules
//!
//! 1. Durable write first. If SQLite fails, the cache is left untouched.
//! 2. Then patch only the columns of that intent in the cached entry. A missing
//!    entry is not an error: a late write to an untracked market must not
//!    re-insert it.
//! 3. Every method is synchronous. No lock is held across an `.await`, and the
//!    database guard is released before the cache lock is taken.
//!
//! # Reads
//!
//! [`MarketStore::cached`], [`MarketStore::active`], and
//! [`MarketStore::active_count`] are hot-path reads served from the map.
//! [`MarketStore::load`], [`MarketStore::list`], and
//! [`MarketStore::list_including_deleted`] read the database because the cache
//! holds only live rows, while listings also show closed-but-tracked rows and
//! `/untrack` needs to recognize already-untracked ones.

use std::collections::HashMap;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use rusqlite::{params, OptionalExtension, Row};

use crate::db::{ensure_updated, invalid_text, Db};
use crate::error::{AppResult, Context};
use crate::types::{MarketId, MarketStatus, TrackedMarket};

/// Every column of `tracked_markets`, in [`TrackedMarket`] field order.
///
/// [`market_from_row`] maps by position, so a change here must be mirrored
/// there.
const MARKET_COLUMNS: &str =
    "id, slug, market_url, question, description, resolution_criteria, close_time, \
     status, budget_mana, invested_mana, realized_pnl_mana, yes_shares, no_shares, \
     p_internal_log_odds, p_market_last, belief_updated_at, sieve_source, discord_thread_id, \
     created_at, deleted_at, outcome_type, should_answers_sum_to_one";

/// Row truth plus the in-memory snapshot of the live markets.
///
/// The snapshot is not the authority; it is a read cache in front of
/// `tracked_markets`. Every mutation goes through an intent method that writes
/// the row first and then patches the snapshot.
pub struct MarketStore {
    db: Db,
    markets: RwLock<HashMap<MarketId, TrackedMarket>>,
}

impl MarketStore {
    /// Loads the live snapshot. Same rows as [`MarketStore::list`].
    pub fn new(db: &Db) -> AppResult<Self> {
        Ok(Self {
            db: db.clone(),
            markets: RwLock::new(snapshot(db)?),
        })
    }

    // -----------------------------------------------------------------
    // Cached reads: hot path only
    // -----------------------------------------------------------------

    /// Returns a clone of one cached market.
    pub fn cached(&self, id: &str) -> Option<TrackedMarket> {
        self.read().get(id).cloned()
    }

    /// Returns every active market, sorted by ID for deterministic callers.
    pub fn active(&self) -> Vec<TrackedMarket> {
        let markets = self.read();
        let mut active: Vec<TrackedMarket> = markets
            .values()
            .filter(|market| market.status == MarketStatus::Active)
            .cloned()
            .collect();
        active.sort_by(|left, right| left.id.cmp(&right.id));
        active
    }

    /// Number of active markets without cloning their snapshots.
    ///
    /// The heartbeat calls this every few seconds, so it avoids allocating a
    /// `Vec<TrackedMarket>` just to read its length.
    pub fn active_count(&self) -> usize {
        self.read()
            .values()
            .filter(|market| market.status == MarketStatus::Active)
            .count()
    }

    // -----------------------------------------------------------------
    // Durable reads: row truth, including closed-but-visible rows
    // -----------------------------------------------------------------

    /// Fetches one market by contract ID, including untracked rows.
    ///
    /// A soft-deleted row is still returned so stale Discord embeds and
    /// repeated commands can report the truth instead of "not tracked". Use
    /// [`TrackedMarket::is_deleted`] to tell the two apart.
    pub fn load(&self, id: &str) -> AppResult<Option<TrackedMarket>> {
        let conn = self.db.lock()?;
        conn.query_row(
            &format!("SELECT {MARKET_COLUMNS} FROM tracked_markets WHERE id = ?1"),
            params![id],
            market_from_row,
        )
        .optional()
        .with_context(|| format!("loading market {id}"))
    }

    /// Lists every tracked market in creation order, excluding soft deletes.
    pub fn list(&self) -> AppResult<Vec<TrackedMarket>> {
        rows(&self.db, false)
    }

    /// Lists every market row, including soft-deleted ones.
    ///
    /// Only `/untrack` needs this: it must recognize a market that was already
    /// untracked and answer with a clear message rather than "not tracked".
    pub fn list_including_deleted(&self) -> AppResult<Vec<TrackedMarket>> {
        rows(&self.db, true)
    }

    // -----------------------------------------------------------------
    // Lifecycle intents: change cache membership too
    // -----------------------------------------------------------------

    /// Inserts a new tracked market and caches it.
    ///
    /// The primary key must not already exist.
    pub fn insert(&self, market: &TrackedMarket) -> AppResult<()> {
        let conn = self.db.lock()?;
        conn.execute(
            "INSERT INTO tracked_markets (id, slug, market_url, question, description, resolution_criteria, \
             close_time, status, budget_mana, invested_mana, realized_pnl_mana, yes_shares, \
             no_shares, p_internal_log_odds, p_market_last, belief_updated_at, sieve_source, \
             discord_thread_id, created_at, deleted_at, outcome_type, should_answers_sum_to_one) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22)",
            params![
                market.id,
                market.slug,
                market.market_url,
                market.question,
                market.description,
                market.resolution_criteria,
                market.close_time,
                market.status.as_str(),
                market.budget_mana,
                market.invested_mana,
                market.realized_pnl_mana,
                market.yes_shares,
                market.no_shares,
                market.p_internal_log_odds,
                market.p_market_last,
                market.belief_updated_at,
                market.sieve_source,
                market.discord_thread_id,
                market.created_at,
                market.deleted_at,
                market.outcome_type,
                market.should_answers_sum_to_one,
            ],
        )
        .with_context(|| format!("inserting market {}", market.id))?;
        drop(conn);

        self.write().insert(market.id.clone(), market.clone());
        Ok(())
    }

    /// Marks a market closed without deleting it, and drops the cache entry.
    ///
    /// `deleted_at` stays NULL: a closed market is still tracked, so `/status`
    /// and autocomplete keep showing it. Use [`MarketStore::untrack`] for the
    /// end state that also hides the row.
    pub fn close(&self, id: &str) -> AppResult<()> {
        let conn = self.db.lock()?;
        let updated = conn
            .execute(
                "UPDATE tracked_markets SET status = ?2 WHERE id = ?1",
                params![id, MarketStatus::Closed.as_str()],
            )
            .with_context(|| format!("closing market {id}"))?;
        ensure_updated(updated, id)?;
        drop(conn);

        self.write().remove(id);
        Ok(())
    }

    /// Marks a market untracked while keeping its row, answers, and ledgers.
    ///
    /// The status becomes `CLOSED` so the trading layer stops acting on it, and
    /// `deleted_at` hides it from every listing. The call is idempotent:
    /// re-deleting keeps the original `deleted_at`.
    pub fn untrack(&self, id: &str, at: i64) -> AppResult<()> {
        let conn = self.db.lock()?;
        let updated = conn
            .execute(
                "UPDATE tracked_markets SET status = ?2, deleted_at = COALESCE(deleted_at, ?3) \
                 WHERE id = ?1",
                params![id, MarketStatus::Closed.as_str(), at],
            )
            .with_context(|| format!("untracking market {id}"))?;
        ensure_updated(updated, id)?;
        drop(conn);

        self.write().remove(id);
        Ok(())
    }

    /// Removes a market row outright and drops the cache entry.
    ///
    /// Only lifecycle replacement uses this: re-onboarding a market whose row
    /// is dead, and the rollback of a failed activation. `/untrack` goes
    /// through [`MarketStore::untrack`] instead, so an operator's decision
    /// stays auditable. Deleting a missing row is not an error.
    pub fn delete(&self, id: &str) -> AppResult<()> {
        let conn = self.db.lock()?;
        conn.execute("DELETE FROM tracked_markets WHERE id = ?1", params![id])
            .with_context(|| format!("deleting market {id}"))?;
        drop(conn);

        self.write().remove(id);
        Ok(())
    }

    // -----------------------------------------------------------------
    // Column-group intents: patch the cache field only
    // -----------------------------------------------------------------

    /// Replaces a market's lifecycle state.
    ///
    /// Only `ACTIVE` and `PAUSED` are transitions of their own: a market that
    /// becomes `CLOSED` must also leave the cache, which is
    /// [`MarketStore::close`] and [`MarketStore::untrack`]. Fails when the
    /// market does not exist, which surfaces stale IDs instead of silently
    /// dropping the update.
    pub fn record_status(&self, id: &str, status: MarketStatus) -> AppResult<()> {
        debug_assert!(
            status != MarketStatus::Closed,
            "close and untrack own the CLOSED transition"
        );

        self.update(
            "UPDATE tracked_markets SET status = ?2 WHERE id = ?1",
            params![id, status.as_str()],
            || format!("updating status for market {id}"),
            id,
        )?;
        self.patch_cached(id, |market| market.status = status);
        Ok(())
    }

    /// Replaces a market's mana budget.
    pub fn record_budget(&self, id: &str, budget_mana: f64) -> AppResult<()> {
        self.update(
            "UPDATE tracked_markets SET budget_mana = ?2 WHERE id = ?1",
            params![id, budget_mana],
            || format!("updating budget for market {id}"),
            id,
        )?;
        self.patch_cached(id, |market| market.budget_mana = budget_mana);
        Ok(())
    }

    /// Stores the belief engine state after a triage impulse or an override.
    ///
    /// `log_odds` is the internal log-odds value `L_t` and `updated_at` is the
    /// moment the impulse was applied. The market price is deliberately not
    /// written: [`MarketStore::record_price`] owns `p_market_last`.
    pub fn record_belief(&self, id: &str, log_odds: f64, updated_at: i64) -> AppResult<()> {
        self.update(
            "UPDATE tracked_markets SET p_internal_log_odds = ?2, belief_updated_at = ?3 \
             WHERE id = ?1",
            params![id, log_odds, updated_at],
            || format!("updating belief for market {id}"),
            id,
        )?;
        self.patch_cached(id, |market| {
            market.p_internal_log_odds = log_odds;
            market.belief_updated_at = updated_at;
        });
        Ok(())
    }

    /// Stores the market probability observed by the trade sync.
    ///
    /// `belief_updated_at` is left alone: it marks the last impulse, and decay
    /// is computed from it against whatever price is current.
    pub fn record_price(&self, id: &str, p_market: f64) -> AppResult<()> {
        self.update(
            "UPDATE tracked_markets SET p_market_last = ?2 WHERE id = ?1",
            params![id, p_market],
            || format!("updating market price for {id}"),
            id,
        )?;
        self.patch_cached(id, |market| market.p_market_last = p_market);
        Ok(())
    }

    /// Stores the reconciled position and profit-and-loss snapshot.
    ///
    /// Shares and cost basis move together because they always come from the
    /// same remote `ContractMetric` row. Belief and price columns are untouched:
    /// triage and the price sync own those and may write them concurrently.
    pub fn record_position(
        &self,
        id: &str,
        invested: f64,
        realized_pnl: f64,
        yes_shares: f64,
        no_shares: f64,
    ) -> AppResult<()> {
        self.update(
            "UPDATE tracked_markets SET invested_mana = ?2, realized_pnl_mana = ?3, \
             yes_shares = ?4, no_shares = ?5 WHERE id = ?1",
            params![id, invested, realized_pnl, yes_shares, no_shares],
            || format!("updating position for market {id}"),
            id,
        )?;
        self.patch_cached(id, |market| {
            market.invested_mana = invested;
            market.realized_pnl_mana = realized_pnl;
            market.yes_shares = yes_shares;
            market.no_shares = no_shares;
        });
        Ok(())
    }

    /// Refreshes the outcome type and the sum-to-one flag from Manifold.
    pub fn record_kind(&self, id: &str, outcome_type: &str, sum_to_one: bool) -> AppResult<()> {
        self.update(
            "UPDATE tracked_markets SET outcome_type = ?2, should_answers_sum_to_one = ?3 \
             WHERE id = ?1",
            params![id, outcome_type, sum_to_one],
            || format!("updating outcome type for market {id}"),
            id,
        )?;
        self.patch_cached(id, |market| {
            market.outcome_type = outcome_type.to_owned();
            market.should_answers_sum_to_one = sum_to_one;
        });
        Ok(())
    }

    // -----------------------------------------------------------------
    // Internals
    // -----------------------------------------------------------------

    /// Runs one column-group update and fails when it matched no row.
    ///
    /// The caller patches the cache only after this returns `Ok`, so a failed
    /// write cannot leave the two stores disagreeing.
    fn update(
        &self,
        sql: &str,
        params: impl rusqlite::Params,
        context: impl FnOnce() -> String,
        id: &str,
    ) -> AppResult<()> {
        let conn = self.db.lock()?;
        let updated = conn.execute(sql, params).with_context(context)?;
        ensure_updated(updated, id)
    }

    /// Applies `patch` to the cached entry of `id`, if it has one.
    ///
    /// A missing entry is normal: the market may be paused out of the cache,
    /// closed, or untracked between the durable write and this patch. Never
    /// re-insert here, or a stale write would resurrect a removed market.
    fn patch_cached(&self, id: &str, patch: impl FnOnce(&mut TrackedMarket)) {
        let mut markets = self.write();
        if let Some(market) = markets.get_mut(id) {
            patch(market);
        }
    }

    fn read(&self) -> RwLockReadGuard<'_, HashMap<MarketId, TrackedMarket>> {
        self.markets
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write(&self) -> RwLockWriteGuard<'_, HashMap<MarketId, TrackedMarket>> {
        self.markets
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Loads the market map used by [`MarketStore::new`].
fn snapshot(db: &Db) -> AppResult<HashMap<MarketId, TrackedMarket>> {
    Ok(rows(db, false)?
        .into_iter()
        .map(|market| (market.id.clone(), market))
        .collect())
}

/// Reads market rows in creation order, optionally including soft deletes.
fn rows(db: &Db, include_deleted: bool) -> AppResult<Vec<TrackedMarket>> {
    let sql = if include_deleted {
        format!("SELECT {MARKET_COLUMNS} FROM tracked_markets ORDER BY created_at ASC, id ASC")
    } else {
        format!(
            "SELECT {MARKET_COLUMNS} FROM tracked_markets WHERE deleted_at IS NULL \
             ORDER BY created_at ASC, id ASC"
        )
    };

    let conn = db.lock()?;
    let mut statement = conn.prepare(&sql).context("preparing market listing")?;
    let mapped = statement
        .query_map([], market_from_row)
        .context("querying markets")?;
    let mut markets = Vec::new();
    for row in mapped {
        markets.push(row.context("listing markets: reading row")?);
    }
    Ok(markets)
}

fn market_from_row(row: &Row<'_>) -> rusqlite::Result<TrackedMarket> {
    let status: String = row.get(7)?;
    Ok(TrackedMarket {
        id: row.get(0)?,
        slug: row.get(1)?,
        market_url: row.get(2)?,
        question: row.get(3)?,
        description: row.get(4)?,
        resolution_criteria: row.get(5)?,
        close_time: row.get(6)?,
        status: status.parse().map_err(|message| invalid_text(7, message))?,
        budget_mana: row.get(8)?,
        invested_mana: row.get(9)?,
        realized_pnl_mana: row.get(10)?,
        yes_shares: row.get(11)?,
        no_shares: row.get(12)?,
        p_internal_log_odds: row.get(13)?,
        p_market_last: row.get(14)?,
        belief_updated_at: row.get(15)?,
        sieve_source: row.get(16)?,
        discord_thread_id: row.get(17)?,
        created_at: row.get(18)?,
        deleted_at: row.get(19)?,
        outcome_type: row.get(20)?,
        should_answers_sum_to_one: row.get(21)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TrackedAnswer;

    fn sample_market(id: &str, created_at: i64) -> TrackedMarket {
        TrackedMarket {
            id: id.to_owned(),
            slug: format!("slug-{id}"),
            market_url: String::new(),
            question: format!("Question {id}?"),
            description: "Description".to_owned(),
            resolution_criteria: "Resolves YES if the thing happens.".to_owned(),
            close_time: Some(created_at + 86_400),
            status: MarketStatus::Active,
            budget_mana: 100.0,
            invested_mana: 0.0,
            realized_pnl_mana: 0.0,
            yes_shares: 0.0,
            no_shares: 0.0,
            p_internal_log_odds: 0.0,
            p_market_last: 0.5,
            belief_updated_at: created_at,
            sieve_source: "match Market { \"thing\" }".to_owned(),
            discord_thread_id: String::new(),
            created_at,
            deleted_at: None,
            outcome_type: "BINARY".to_owned(),
            should_answers_sum_to_one: false,
        }
    }

    fn sample_answer(market_id: &str) -> TrackedAnswer {
        TrackedAnswer {
            id: "answer-1".to_owned(),
            market_id: market_id.to_owned(),
            text: "Yes".to_owned(),
            p_internal_log_odds: 0.0,
            p_market_last: 0.5,
            belief_updated_at: 1,
            yes_shares: 0.0,
            no_shares: 0.0,
            created_at: 1,
            invested_mana: 0.0,
        }
    }

    fn assert_market_eq(actual: &TrackedMarket, expected: &TrackedMarket) {
        assert_eq!(actual.id, expected.id);
        assert_eq!(actual.slug, expected.slug);
        assert_eq!(actual.market_url, expected.market_url);
        assert_eq!(actual.question, expected.question);
        assert_eq!(actual.description, expected.description);
        assert_eq!(actual.resolution_criteria, expected.resolution_criteria);
        assert_eq!(actual.close_time, expected.close_time);
        assert_eq!(actual.status, expected.status);
        assert_eq!(actual.budget_mana, expected.budget_mana);
        assert_eq!(actual.invested_mana, expected.invested_mana);
        assert_eq!(actual.realized_pnl_mana, expected.realized_pnl_mana);
        assert_eq!(actual.yes_shares, expected.yes_shares);
        assert_eq!(actual.no_shares, expected.no_shares);
        assert_eq!(actual.p_internal_log_odds, expected.p_internal_log_odds);
        assert_eq!(actual.p_market_last, expected.p_market_last);
        assert_eq!(actual.belief_updated_at, expected.belief_updated_at);
        assert_eq!(actual.sieve_source, expected.sieve_source);
        assert_eq!(actual.discord_thread_id, expected.discord_thread_id);
        assert_eq!(actual.created_at, expected.created_at);
        assert_eq!(actual.deleted_at, expected.deleted_at);
        assert_eq!(actual.outcome_type, expected.outcome_type);
        assert_eq!(
            actual.should_answers_sum_to_one,
            expected.should_answers_sum_to_one
        );
    }

    fn market_ids(markets: &[TrackedMarket]) -> Vec<&str> {
        markets.iter().map(|market| market.id.as_str()).collect()
    }

    fn store_with(markets: &[TrackedMarket]) -> (Db, MarketStore) {
        let db = Db::open_in_memory().expect("in-memory DB");
        let store = MarketStore::new(&db).expect("store loads");
        for market in markets {
            store.insert(market).expect("insert market");
        }
        (db, store)
    }

    #[test]
    fn market_roundtrip() {
        let (_db, store) = store_with(&[]);
        let market = sample_market("m1", 1_700_000_000);

        assert!(store.load("m1").expect("load missing").is_none());
        assert!(store.cached("m1").is_none());
        store.insert(&market).expect("insert market");

        let loaded = store.load("m1").expect("load market").expect("present");
        assert_market_eq(&loaded, &market);
        assert_market_eq(&store.cached("m1").expect("cached"), &market);

        let listed = store.list().expect("list markets");
        assert_eq!(listed.len(), 1);
        assert_market_eq(&listed[0], &market);

        store.delete("m1").expect("delete market");
        assert!(store.load("m1").expect("load deleted").is_none());
        assert!(store.cached("m1").is_none());
    }

    #[test]
    fn insert_duplicate_market_fails() {
        let (_db, store) = store_with(&[]);
        let market = sample_market("m1", 1);
        store.insert(&market).expect("first insert");
        assert!(store.insert(&market).is_err());
        assert_eq!(store.list().expect("list").len(), 1);
    }

    #[test]
    fn market_url_survives_a_roundtrip() {
        let (_db, store) = store_with(&[]);
        let mut market = sample_market("m1", 1);
        market.market_url = "https://manifold.markets/Jack1/2026-melbourne-cup-winner".to_owned();
        store.insert(&market).expect("insert market");

        let loaded = store.load("m1").expect("load market").expect("row exists");
        assert_eq!(loaded.market_url, market.market_url);
    }

    #[test]
    fn list_is_ordered_by_creation_time() {
        let markets: Vec<TrackedMarket> = [("late", 300), ("early", 100), ("middle", 200)]
            .into_iter()
            .map(|(id, created_at)| sample_market(id, created_at))
            .collect();
        let (_db, store) = store_with(&markets);

        let listed = store.list().expect("list markets");
        assert_eq!(market_ids(&listed), vec!["early", "middle", "late"]);
    }

    #[test]
    fn list_includes_closed_rows_and_hides_deleted_ones() {
        let mut closed = sample_market("closed", 1);
        closed.status = MarketStatus::Closed;
        let deleted = sample_market("deleted", 2);
        let (_db, store) = store_with(&[closed, deleted]);
        store.untrack("deleted", 1_700_000_900).expect("untrack");

        // A closed market is still tracked, so `/status` keeps showing it.
        let listed = store.list().expect("list markets");
        assert_eq!(market_ids(&listed), vec!["closed"]);

        // Only `/untrack` needs the deleted row back.
        let all = store.list_including_deleted().expect("list all");
        assert_eq!(market_ids(&all), vec!["closed", "deleted"]);
    }

    #[test]
    fn active_excludes_paused_and_is_sorted() {
        // Inserted out of ID order and before the store is built, so this also
        // covers the initial snapshot load.
        let mut paused = sample_market("b-paused", 2);
        paused.status = MarketStatus::Paused;
        let mut closed = sample_market("c-closed", 3);
        closed.status = MarketStatus::Closed;
        let markets = [
            sample_market("c-active", 4),
            paused,
            closed,
            sample_market("a-active", 1),
        ];
        let (_db, store) = store_with(&markets);

        assert_eq!(market_ids(&store.active()), vec!["a-active", "c-active"]);
    }

    #[test]
    fn active_count_matches() {
        let mut paused = sample_market("m2", 2);
        paused.status = MarketStatus::Paused;
        let (_db, store) = store_with(&[sample_market("m1", 1), paused]);
        assert_eq!(store.active_count(), 1, "paused markets are not counted");

        store.close("m1").expect("close");
        assert_eq!(store.active_count(), 0);
        assert!(store.active().is_empty());
    }

    #[test]
    fn insert_is_visible_to_cached_and_to_load() {
        let (_db, store) = store_with(&[]);
        let market = sample_market("m3", 3);
        store.insert(&market).expect("insert");

        assert_eq!(store.cached("m3").expect("cached").question, "Question m3?");
        assert!(store.load("m3").expect("load").is_some());
        assert_eq!(market_ids(&store.active()), vec!["m3"]);
    }

    #[test]
    fn record_status_persists_the_new_status() {
        let (_db, store) = store_with(&[sample_market("m1", 1)]);

        store
            .record_status("m1", MarketStatus::Paused)
            .expect("record status");

        let row = store.load("m1").expect("load").expect("row");
        assert_eq!(row.status, MarketStatus::Paused);
        assert_eq!(
            store.cached("m1").expect("cached").status,
            MarketStatus::Paused
        );
        assert!(store.active().is_empty(), "a paused market is not active");
    }

    #[test]
    fn record_budget_persists_the_new_budget() {
        let (_db, store) = store_with(&[sample_market("m1", 1)]);

        store.record_budget("m1", 250.0).expect("record budget");

        assert_eq!(
            store.load("m1").expect("load").expect("row").budget_mana,
            250.0
        );
        assert_eq!(store.cached("m1").expect("cached").budget_mana, 250.0);
    }

    #[test]
    fn record_kind_persists_the_new_kind() {
        let (_db, store) = store_with(&[sample_market("m1", 1)]);

        store
            .record_kind("m1", "MULTIPLE_CHOICE", true)
            .expect("record kind");

        let row = store.load("m1").expect("load").expect("row");
        assert_eq!(row.outcome_type, "MULTIPLE_CHOICE");
        assert!(row.should_answers_sum_to_one);
        let cached = store.cached("m1").expect("cached");
        assert_eq!(cached.outcome_type, "MULTIPLE_CHOICE");
        assert!(cached.should_answers_sum_to_one);
    }

    #[test]
    fn record_belief_leaves_position_status_and_price_alone() {
        let (_db, store) = store_with(&[sample_market("m1", 1)]);

        store.record_belief("m1", 0.8, 500).expect("record belief");

        let row = store.load("m1").expect("load").expect("row");
        assert_eq!(row.p_internal_log_odds, 0.8);
        assert_eq!(row.belief_updated_at, 500);
        assert_eq!(row.p_market_last, 0.5, "belief must not write the price");
        assert_eq!(row.status, MarketStatus::Active);
        assert_eq!(row.invested_mana, 0.0);
        assert_eq!(row.yes_shares, 0.0);

        let cached = store.cached("m1").expect("cached");
        assert_eq!(cached.p_internal_log_odds, 0.8);
        assert_eq!(cached.belief_updated_at, 500);
        assert_eq!(cached.p_market_last, 0.5);
    }

    #[test]
    fn record_position_leaves_belief_price_and_status_alone() {
        let (_db, store) = store_with(&[sample_market("m1", 1)]);
        store.record_belief("m1", 0.9, 700).expect("record belief");
        store.record_price("m1", 0.61).expect("record price");

        store
            .record_position("m1", 30.0, 4.5, 12.0, 3.0)
            .expect("record position");

        let row = store.load("m1").expect("load").expect("row");
        assert_eq!(row.invested_mana, 30.0);
        assert_eq!(row.realized_pnl_mana, 4.5);
        assert_eq!(row.yes_shares, 12.0);
        assert_eq!(row.no_shares, 3.0);
        assert_eq!(row.p_internal_log_odds, 0.9);
        assert_eq!(row.belief_updated_at, 700);
        assert_eq!(row.p_market_last, 0.61);
        assert_eq!(row.status, MarketStatus::Active);

        let cached = store.cached("m1").expect("cached");
        assert_eq!(cached.invested_mana, 30.0);
        assert_eq!(cached.p_internal_log_odds, 0.9);
        assert_eq!(cached.p_market_last, 0.61);
    }

    #[test]
    fn record_price_leaves_belief_alone() {
        let (_db, store) = store_with(&[sample_market("m1", 1)]);
        store.record_belief("m1", 1.25, 800).expect("record belief");

        store.record_price("m1", 0.72).expect("record price");

        let row = store.load("m1").expect("load").expect("row");
        assert_eq!(row.p_market_last, 0.72);
        assert_eq!(row.p_internal_log_odds, 1.25);
        assert_eq!(row.belief_updated_at, 800);

        let cached = store.cached("m1").expect("cached");
        assert_eq!(cached.p_market_last, 0.72);
        assert_eq!(cached.p_internal_log_odds, 1.25);
        assert_eq!(cached.belief_updated_at, 800);
    }

    #[test]
    fn a_stale_position_write_does_not_revert_a_newer_belief() {
        let (_db, store) = store_with(&[sample_market("m1", 1)]);

        // The trade path loads a whole-row snapshot, then does network calls.
        let mut snapshot = store.load("m1").expect("load").expect("row");
        snapshot.p_internal_log_odds = 0.1;
        snapshot.p_market_last = 0.5;
        snapshot.belief_updated_at = 1;

        // Triage applies an impulse while the trade check is still in flight.
        store.record_belief("m1", 2.0, 900).expect("record belief");

        // The trade check now persists its stale position values.
        snapshot.invested_mana = 40.0;
        snapshot.realized_pnl_mana = 3.0;
        snapshot.yes_shares = 12.0;
        snapshot.no_shares = 1.0;
        store
            .record_position(
                &snapshot.id,
                snapshot.invested_mana,
                snapshot.realized_pnl_mana,
                snapshot.yes_shares,
                snapshot.no_shares,
            )
            .expect("record position");

        let row = store.load("m1").expect("load").expect("row");
        assert_eq!(row.p_internal_log_odds, 2.0, "the newer belief survives");
        assert_eq!(row.belief_updated_at, 900);
        assert_eq!(row.invested_mana, 40.0);

        let cached = store.cached("m1").expect("cached");
        assert_eq!(cached.p_internal_log_odds, 2.0);
        assert_eq!(cached.belief_updated_at, 900);
        assert_eq!(cached.invested_mana, 40.0);
    }

    #[test]
    fn a_position_write_does_not_revert_a_pause() {
        let (_db, store) = store_with(&[sample_market("m1", 1)]);

        store
            .record_status("m1", MarketStatus::Paused)
            .expect("pause");

        store
            .record_position("m1", 25.0, 0.0, 5.0, 0.0)
            .expect("record position");

        assert_eq!(
            store.load("m1").expect("load").expect("row").status,
            MarketStatus::Paused
        );
        assert_eq!(
            store.cached("m1").expect("cached").status,
            MarketStatus::Paused,
            "the paused market must stay out of the match path"
        );
    }

    #[test]
    fn close_and_untrack_remove_the_cache_entry_and_keep_the_row() {
        let (_db, store) = store_with(&[sample_market("m1", 1), sample_market("m2", 2)]);

        store.close("m1").expect("close");
        store.untrack("m2", 1_700_000_900).expect("untrack");

        assert!(store.cached("m1").is_none());
        assert!(store.cached("m2").is_none());

        let closed = store.load("m1").expect("load").expect("row kept");
        assert_eq!(closed.status, MarketStatus::Closed);
        assert_eq!(closed.deleted_at, None, "close keeps the market tracked");

        let untracked = store.load("m2").expect("load").expect("row kept");
        assert_eq!(untracked.status, MarketStatus::Closed);
        assert_eq!(untracked.deleted_at, Some(1_700_000_900));
        assert!(untracked.is_deleted());
    }

    #[test]
    fn untrack_closes_the_row_and_hides_it_from_listings() {
        let (_db, store) = store_with(&[sample_market("m1", 1)]);

        store.untrack("m1", 1_700_000_900).expect("untrack");

        assert!(store.list().expect("list markets").is_empty());
        assert!(store.active().is_empty());

        // The row itself survives, closed and time-stamped.
        let row = store.load("m1").expect("load").expect("row kept");
        assert_eq!(row.status, MarketStatus::Closed);
        assert_eq!(row.deleted_at, Some(1_700_000_900));
        assert!(row.is_deleted());
    }

    #[test]
    fn untrack_keeps_answers_and_is_idempotent() {
        let (db, store) = store_with(&[sample_market("m1", 1)]);
        db.upsert_answer(&sample_answer("m1"))
            .expect("insert answer");

        store.untrack("m1", 1_700_000_900).expect("first untrack");
        store.untrack("m1", 1_700_001_999).expect("second untrack");

        let row = store.load("m1").expect("load").expect("row kept");
        assert_eq!(
            row.deleted_at,
            Some(1_700_000_900),
            "the first removal time is kept"
        );
        assert_eq!(
            db.list_answers("m1").expect("list answers").len(),
            1,
            "answers are kept for audit"
        );
    }

    #[test]
    fn an_intent_on_a_missing_market_reports_an_error_and_changes_nothing() {
        let (_db, store) = store_with(&[]);

        assert!(store
            .record_status("missing", MarketStatus::Active)
            .is_err());
        assert!(store.record_budget("missing", 10.0).is_err());
        assert!(store.record_belief("missing", 1.0, 1).is_err());
        assert!(store.record_price("missing", 0.6).is_err());
        assert!(store
            .record_position("missing", 1.0, 0.0, 1.0, 0.0)
            .is_err());
        assert!(store.record_kind("missing", "BINARY", false).is_err());
        assert!(store.close("missing").is_err());
        assert!(store.untrack("missing", 1).is_err());

        assert!(store.cached("missing").is_none());
        assert!(store.load("missing").expect("load").is_none());
        assert!(store.list().expect("list").is_empty());
    }

    #[test]
    fn an_intent_on_an_uncached_row_updates_the_database_only() {
        let (_db, store) = store_with(&[sample_market("m1", 1)]);
        store.untrack("m1", 100).expect("untrack");
        assert!(store.cached("m1").is_none());

        // A late trade write against an untracked row is harmless.
        store.record_belief("m1", 1.5, 200).expect("late belief");

        assert!(
            store.cached("m1").is_none(),
            "a late write must not re-insert the market into the cache"
        );
        let row = store.load("m1").expect("load").expect("row kept");
        assert_eq!(row.p_internal_log_odds, 1.5);
        assert_eq!(row.belief_updated_at, 200);
        assert!(row.is_deleted());
    }
}
