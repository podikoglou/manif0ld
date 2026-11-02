//! SQLite persistence for markets, signals, trades, and key/value state.
//!
//! [`Db`] is a cheaply cloneable handle around a single connection. Every query
//! is short and synchronous; callers share one handle across tasks and the
//! internal mutex serializes access. Async callers should keep critical
//! sections small and avoid holding a connection guard across `.await` points.

use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use rusqlite::{params, Connection, OptionalExtension, Row};

use crate::error::{anyhow, bail, AppResult, Context};
use crate::types::{MarketStatus, SignalRecord, TrackedAnswer, TrackedMarket, TradeRecord};

const MARKET_COLUMNS: &str = "id, slug, question, description, resolution_criteria, close_time, \
     status, budget_mana, invested_mana, realized_pnl_mana, yes_shares, no_shares, \
     p_internal_log_odds, p_market_last, belief_updated_at, sieve_source, discord_thread_id, \
     created_at";

const ANSWER_COLUMNS: &str = "id, market_id, text, p_internal_log_odds, p_market_last, \
     belief_updated_at, yes_shares, no_shares, created_at";

const SIGNAL_COLUMNS: &str = "id, market_id, answer_id, post_uri, author_handle, post_text, material, \
     direction, strength, impulse, p_after, created_at";

const TRADE_COLUMNS: &str = "id, market_id, answer_id, action, mana_amount, shares_delta, p_internal, \
     p_market, trigger_post_uri, manifold_bet_id, created_at";

const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Cloneable database handle.
///
/// The `Arc<Mutex<_>>` makes the handle usable from multiple tasks while keeping
/// one SQLite connection (and therefore one transaction domain) per process.
#[derive(Clone)]
pub struct Db {
    conn: Arc<Mutex<Connection>>,
}

impl Db {
    /// Opens (or creates) a database file and applies the schema.
    ///
    /// Parent directories are created as needed. The connection uses WAL mode
    /// so readers do not block the short write transactions used by the bot.
    pub fn open(path: &Path) -> AppResult<Self> {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating database directory {}", parent.display()))?;
        }

        let conn = Connection::open(path)
            .with_context(|| format!("opening database {}", path.display()))?;
        Self::from_connection(conn, true)
            .with_context(|| format!("initializing database {}", path.display()))
    }

    /// Opens a fresh in-memory database with the schema applied.
    ///
    /// Intended for tests and for short-lived tooling; the data is discarded
    /// when the handle is dropped.
    pub fn open_in_memory() -> AppResult<Self> {
        let conn = Connection::open_in_memory().context("opening in-memory database")?;
        Self::from_connection(conn, false)
    }

    fn from_connection(conn: Connection, wal: bool) -> AppResult<Self> {
        conn.busy_timeout(BUSY_TIMEOUT)
            .context("setting SQLite busy timeout")?;
        if wal {
            // `PRAGMA journal_mode` returns a row, so `execute` would fail here.
            conn.query_row("PRAGMA journal_mode = WAL", [], |row| {
                row.get::<_, String>(0)
            })
            .context("enabling WAL journal mode")?;
        }
        conn.execute_batch(include_str!("../migrations/001_init.sql"))
            .context("applying database schema")?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    fn lock(&self) -> AppResult<MutexGuard<'_, Connection>> {
        self.conn
            .lock()
            .map_err(|_| anyhow!("database mutex poisoned"))
    }

    // ---------------------------------------------------------------------
    // Markets
    // ---------------------------------------------------------------------

    /// Inserts a new tracked market.
    ///
    /// The primary key must not already exist.
    pub fn insert_market(&self, market: &TrackedMarket) -> AppResult<()> {
        let conn = self.lock()?;
        conn.execute(
            "INSERT INTO tracked_markets (id, slug, question, description, resolution_criteria, \
             close_time, status, budget_mana, invested_mana, realized_pnl_mana, yes_shares, \
             no_shares, p_internal_log_odds, p_market_last, belief_updated_at, sieve_source, \
             discord_thread_id, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)",
            params![
                market.id,
                market.slug,
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
            ],
        )
        .with_context(|| format!("inserting market {}", market.id))?;
        Ok(())
    }

    /// Fetches one market by contract ID.
    pub fn get_market(&self, id: &str) -> AppResult<Option<TrackedMarket>> {
        let conn = self.lock()?;
        conn.query_row(
            &format!("SELECT {MARKET_COLUMNS} FROM tracked_markets WHERE id = ?1"),
            params![id],
            market_from_row,
        )
        .optional()
        .with_context(|| format!("loading market {id}"))
    }

    /// Lists all markets in creation order.
    pub fn list_markets(&self) -> AppResult<Vec<TrackedMarket>> {
        let conn = self.lock()?;
        let mut statement = conn
            .prepare(&format!(
                "SELECT {MARKET_COLUMNS} FROM tracked_markets ORDER BY created_at ASC, id ASC"
            ))
            .context("preparing market listing")?;
        let rows = statement
            .query_map([], market_from_row)
            .context("querying markets")?;
        collect_rows(rows, "listing markets")
    }

    /// Lists markets in one lifecycle state, in creation order.
    pub fn list_markets_by_status(&self, status: MarketStatus) -> AppResult<Vec<TrackedMarket>> {
        let conn = self.lock()?;
        let mut statement = conn
            .prepare(&format!(
                "SELECT {MARKET_COLUMNS} FROM tracked_markets WHERE status = ?1 \
                 ORDER BY created_at ASC, id ASC"
            ))
            .context("preparing market listing by status")?;
        let rows = statement
            .query_map(params![status.as_str()], market_from_row)
            .with_context(|| format!("querying markets with status {status}"))?;
        collect_rows(rows, "listing markets by status")
    }

    /// Replaces a market's lifecycle state.
    ///
    /// Fails when the market does not exist, which surfaces stale IDs instead
    /// of silently dropping the update.
    pub fn update_market_status(&self, id: &str, status: MarketStatus) -> AppResult<()> {
        let conn = self.lock()?;
        let updated = conn
            .execute(
                "UPDATE tracked_markets SET status = ?2 WHERE id = ?1",
                params![id, status.as_str()],
            )
            .with_context(|| format!("updating status for market {id}"))?;
        ensure_updated(updated, id)
    }

    /// Replaces a market's mana budget.
    pub fn update_budget(&self, id: &str, budget_mana: f64) -> AppResult<()> {
        let conn = self.lock()?;
        let updated = conn
            .execute(
                "UPDATE tracked_markets SET budget_mana = ?2 WHERE id = ?1",
                params![id, budget_mana],
            )
            .with_context(|| format!("updating budget for market {id}"))?;
        ensure_updated(updated, id)
    }

    /// Stores the belief engine state after a signal or override.
    ///
    /// `log_odds` is the internal log-odds value `L_t`; `p_market` is the market
    /// probability observed at `updated_at`.
    pub fn update_belief(
        &self,
        id: &str,
        log_odds: f64,
        p_market: f64,
        updated_at: i64,
    ) -> AppResult<()> {
        let conn = self.lock()?;
        let updated = conn
            .execute(
                "UPDATE tracked_markets SET p_internal_log_odds = ?2, p_market_last = ?3, \
                 belief_updated_at = ?4 WHERE id = ?1",
                params![id, log_odds, p_market, updated_at],
            )
            .with_context(|| format!("updating belief for market {id}"))?;
        ensure_updated(updated, id)
    }

    /// Stores the position and profit-and-loss snapshot for a market.
    pub fn update_position(
        &self,
        id: &str,
        invested: f64,
        realized_pnl: f64,
        yes_shares: f64,
        no_shares: f64,
    ) -> AppResult<()> {
        let conn = self.lock()?;
        let updated = conn
            .execute(
                "UPDATE tracked_markets SET invested_mana = ?2, realized_pnl_mana = ?3, \
                 yes_shares = ?4, no_shares = ?5 WHERE id = ?1",
                params![id, invested, realized_pnl, yes_shares, no_shares],
            )
            .with_context(|| format!("updating position for market {id}"))?;
        ensure_updated(updated, id)
    }

    /// Stores the Discord thread that receives this market's events.
    pub fn set_thread_id(&self, id: &str, thread_id: &str) -> AppResult<()> {
        let conn = self.lock()?;
        let updated = conn
            .execute(
                "UPDATE tracked_markets SET discord_thread_id = ?2 WHERE id = ?1",
                params![id, thread_id],
            )
            .with_context(|| format!("setting thread for market {id}"))?;
        ensure_updated(updated, id)
    }

    /// Removes a market row.
    pub fn delete_market(&self, id: &str) -> AppResult<()> {
        let conn = self.lock()?;
        conn.execute("DELETE FROM tracked_markets WHERE id = ?1", params![id])
            .with_context(|| format!("deleting market {id}"))?;
        Ok(())
    }

    // ---------------------------------------------------------------------
    // Signal ledger
    // ---------------------------------------------------------------------

    /// Appends a signal and returns its assigned row ID.
    ///
    /// `record.id` is ignored; SQLite assigns the identifier.
    pub fn insert_signal(&self, record: &SignalRecord) -> AppResult<i64> {
        let conn = self.lock()?;
        conn.execute(
            "INSERT INTO signal_ledger (market_id, answer_id, post_uri, author_handle, post_text, material, \
             direction, strength, impulse, p_after, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                record.market_id,
                record.answer_id,
                record.post_uri,
                record.author_handle,
                record.post_text,
                record.material as i64,
                record.direction.as_str(),
                record.strength,
                record.impulse,
                record.p_after,
                record.created_at,
            ],
        )
        .with_context(|| format!("inserting signal for market {}", record.market_id))?;
        Ok(conn.last_insert_rowid())
    }

    /// Returns the newest signals for a market, newest first.
    pub fn recent_signals(&self, market_id: &str, limit: usize) -> AppResult<Vec<SignalRecord>> {
        let limit = limit_to_i64(limit);
        let conn = self.lock()?;
        let mut statement = conn
            .prepare(&format!(
                "SELECT {SIGNAL_COLUMNS} FROM signal_ledger WHERE market_id = ?1 \
                 ORDER BY created_at DESC, id DESC LIMIT ?2"
            ))
            .context("preparing signal history")?;
        let rows = statement
            .query_map(params![market_id, limit], signal_from_row)
            .with_context(|| format!("querying signals for market {market_id}"))?;
        collect_rows(rows, "listing signals")
    }

    /// Returns the creation time of the newest signal for a market.
    pub fn latest_signal_time(&self, market_id: &str) -> AppResult<Option<i64>> {
        let conn = self.lock()?;
        conn.query_row(
            "SELECT MAX(created_at) FROM signal_ledger WHERE market_id = ?1",
            params![market_id],
            |row| row.get::<_, Option<i64>>(0),
        )
        .with_context(|| format!("querying latest signal time for market {market_id}"))
    }

    // ---------------------------------------------------------------------
    // Trade ledger
    // ---------------------------------------------------------------------

    /// Appends a trade and returns its assigned row ID.
    ///
    /// `record.id` is ignored; SQLite assigns the identifier.
    pub fn insert_trade(&self, record: &TradeRecord) -> AppResult<i64> {
        let conn = self.lock()?;
        conn.execute(
            "INSERT INTO trade_ledger (market_id, answer_id, action, mana_amount, shares_delta, p_internal, \
             p_market, trigger_post_uri, manifold_bet_id, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                record.market_id,
                record.answer_id,
                record.action.as_str(),
                record.mana_amount,
                record.shares_delta,
                record.p_internal,
                record.p_market,
                record.trigger_post_uri,
                record.manifold_bet_id,
                record.created_at,
            ],
        )
        .with_context(|| format!("inserting trade for market {}", record.market_id))?;
        Ok(conn.last_insert_rowid())
    }

    /// Returns the newest trades for a market, newest first.
    pub fn recent_trades(&self, market_id: &str, limit: usize) -> AppResult<Vec<TradeRecord>> {
        let limit = limit_to_i64(limit);
        let conn = self.lock()?;
        let mut statement = conn
            .prepare(&format!(
                "SELECT {TRADE_COLUMNS} FROM trade_ledger WHERE market_id = ?1 \
                 ORDER BY created_at DESC, id DESC LIMIT ?2"
            ))
            .context("preparing trade history")?;
        let rows = statement
            .query_map(params![market_id, limit], trade_from_row)
            .with_context(|| format!("querying trades for market {market_id}"))?;
        collect_rows(rows, "listing trades")
    }

    // ---------------------------------------------------------------------
    // Answers (multiple choice markets)
    // ---------------------------------------------------------------------

    /// Inserts or replaces a market answer.
    pub fn upsert_answer(&self, answer: &TrackedAnswer) -> AppResult<()> {
        let conn = self.lock()?;
        conn.execute(
            "INSERT OR REPLACE INTO market_answers (id, market_id, text, p_internal_log_odds, \
             p_market_last, belief_updated_at, yes_shares, no_shares, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                answer.id,
                answer.market_id,
                answer.text,
                answer.p_internal_log_odds,
                answer.p_market_last,
                answer.belief_updated_at,
                answer.yes_shares,
                answer.no_shares,
                answer.created_at,
            ],
        )
        .with_context(|| format!("upserting answer {} for market {}", answer.id, answer.market_id))?;
        Ok(())
    }

    /// Lists all tracked answers for a market.
    pub fn list_answers(&self, market_id: &str) -> AppResult<Vec<TrackedAnswer>> {
        let conn = self.lock()?;
        let mut statement = conn
            .prepare(&format!(
                "SELECT {ANSWER_COLUMNS} FROM market_answers WHERE market_id = ?1 ORDER BY created_at ASC, id ASC"
            ))
            .context("preparing answer listing")?;
        let rows = statement
            .query_map(params![market_id], answer_from_row)
            .with_context(|| format!("querying answers for market {market_id}"))?;
        collect_rows(rows, "listing answers")
    }

    /// Gets a specific answer by (answer_id, market_id).
    pub fn get_answer(&self, market_id: &str, answer_id: &str) -> AppResult<Option<TrackedAnswer>> {
        let conn = self.lock()?;
        conn.query_row(
            &format!("SELECT {ANSWER_COLUMNS} FROM market_answers WHERE market_id = ?1 AND id = ?2"),
            params![market_id, answer_id],
            answer_from_row,
        )
        .optional()
        .with_context(|| format!("loading answer {answer_id} for market {market_id}"))
    }

    /// Updates belief state for a specific answer.
    pub fn update_answer_belief(
        &self,
        market_id: &str,
        answer_id: &str,
        log_odds: f64,
        p_market: f64,
        updated_at: i64,
    ) -> AppResult<()> {
        let conn = self.lock()?;
        let updated = conn
            .execute(
                "UPDATE market_answers SET p_internal_log_odds = ?3, p_market_last = ?4, \
                 belief_updated_at = ?5 WHERE market_id = ?1 AND id = ?2",
                params![market_id, answer_id, log_odds, p_market, updated_at],
            )
            .with_context(|| format!("updating belief for answer {answer_id} in {market_id}"))?;
        ensure_updated(updated, &format!("{market_id}/{answer_id}"))
    }

    /// Updates shares for a specific answer.
    pub fn update_answer_shares(
        &self,
        market_id: &str,
        answer_id: &str,
        yes_shares: f64,
        no_shares: f64,
    ) -> AppResult<()> {
        let conn = self.lock()?;
        let updated = conn
            .execute(
                "UPDATE market_answers SET yes_shares = ?3, no_shares = ?4 \
                 WHERE market_id = ?1 AND id = ?2",
                params![market_id, answer_id, yes_shares, no_shares],
            )
            .with_context(|| format!("updating shares for answer {answer_id} in {market_id}"))?;
        ensure_updated(updated, &format!("{market_id}/{answer_id}"))
    }

    /// Deletes all answers for a market.
    pub fn delete_answers_for_market(&self, market_id: &str) -> AppResult<()> {
        let conn = self.lock()?;
        conn.execute(
            "DELETE FROM market_answers WHERE market_id = ?1",
            params![market_id],
        )
        .with_context(|| format!("deleting answers for market {market_id}"))?;
        Ok(())
    }

    // ---------------------------------------------------------------------
    // Key/value state
    // ---------------------------------------------------------------------

    /// Reads a key/value entry.
    pub fn get_kv(&self, key: &str) -> AppResult<Option<String>> {
        let conn = self.lock()?;
        conn.query_row("SELECT value FROM kv WHERE key = ?1", params![key], |row| {
            row.get(0)
        })
        .optional()
        .with_context(|| format!("reading key/value entry {key}"))
    }

    /// Inserts or replaces a key/value entry.
    pub fn set_kv(&self, key: &str, value: &str) -> AppResult<()> {
        let conn = self.lock()?;
        conn.execute(
            "INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)",
            params![key, value],
        )
        .with_context(|| format!("writing key/value entry {key}"))?;
        Ok(())
    }

    /// Removes a key/value entry. Missing keys are not an error.
    pub fn delete_kv(&self, key: &str) -> AppResult<()> {
        let conn = self.lock()?;
        conn.execute("DELETE FROM kv WHERE key = ?1", params![key])
            .with_context(|| format!("deleting key/value entry {key}"))?;
        Ok(())
    }
}

/// Converts a caller-supplied row limit to SQLite's signed integer.
fn limit_to_i64(limit: usize) -> i64 {
    i64::try_from(limit).unwrap_or(i64::MAX)
}

/// Fails when an update matched no market row.
fn ensure_updated(rows: usize, market_id: &str) -> AppResult<()> {
    if rows == 0 {
        bail!("market {market_id} not found");
    }
    Ok(())
}

/// Turns a fallible row iterator into a vector.
fn collect_rows<T>(
    rows: rusqlite::MappedRows<'_, impl FnMut(&Row<'_>) -> rusqlite::Result<T>>,
    operation: &str,
) -> AppResult<Vec<T>> {
    let mut items = Vec::new();
    for row in rows {
        items.push(row.with_context(|| format!("{operation}: reading row"))?);
    }
    Ok(items)
}

/// Builds a column-conversion error that preserves the parse failure.
fn invalid_text(column: usize, message: String) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        column,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            message,
        )),
    )
}

fn market_from_row(row: &Row<'_>) -> rusqlite::Result<TrackedMarket> {
    let status: String = row.get(6)?;
    Ok(TrackedMarket {
        id: row.get(0)?,
        slug: row.get(1)?,
        question: row.get(2)?,
        description: row.get(3)?,
        resolution_criteria: row.get(4)?,
        close_time: row.get(5)?,
        status: status.parse().map_err(|message| invalid_text(6, message))?,
        budget_mana: row.get(7)?,
        invested_mana: row.get(8)?,
        realized_pnl_mana: row.get(9)?,
        yes_shares: row.get(10)?,
        no_shares: row.get(11)?,
        p_internal_log_odds: row.get(12)?,
        p_market_last: row.get(13)?,
        belief_updated_at: row.get(14)?,
        sieve_source: row.get(15)?,
        discord_thread_id: row.get(16)?,
        created_at: row.get(17)?,
    })
}

fn signal_from_row(row: &Row<'_>) -> rusqlite::Result<SignalRecord> {
    let direction: String = row.get(7)?;
    Ok(SignalRecord {
        id: row.get(0)?,
        market_id: row.get(1)?,
        answer_id: row.get(2)?,
        post_uri: row.get(3)?,
        author_handle: row.get(4)?,
        post_text: row.get(5)?,
        material: row.get(6)?,
        direction: direction
            .parse()
            .map_err(|message| invalid_text(7, message))?,
        strength: row.get(8)?,
        impulse: row.get(9)?,
        p_after: row.get(10)?,
        created_at: row.get(11)?,
    })
}

fn trade_from_row(row: &Row<'_>) -> rusqlite::Result<TradeRecord> {
    let action: String = row.get(3)?;
    Ok(TradeRecord {
        id: row.get(0)?,
        market_id: row.get(1)?,
        answer_id: row.get(2)?,
        action: action.parse().map_err(|message| invalid_text(3, message))?,
        mana_amount: row.get(4)?,
        shares_delta: row.get(5)?,
        p_internal: row.get(6)?,
        p_market: row.get(7)?,
        trigger_post_uri: row.get(8)?,
        manifold_bet_id: row.get(9)?,
        created_at: row.get(10)?,
    })
}

fn answer_from_row(row: &Row<'_>) -> rusqlite::Result<TrackedAnswer> {
    Ok(TrackedAnswer {
        id: row.get(0)?,
        market_id: row.get(1)?,
        text: row.get(2)?,
        p_internal_log_odds: row.get(3)?,
        p_market_last: row.get(4)?,
        belief_updated_at: row.get(5)?,
        yes_shares: row.get(6)?,
        no_shares: row.get(7)?,
        created_at: row.get(8)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Direction, TradeAction};
    use chrono::Utc;

    fn sample_market(id: &str, created_at: i64) -> TrackedMarket {
        TrackedMarket {
            id: id.to_owned(),
            slug: format!("slug-{id}"),
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
        }
    }

    fn sample_signal(id: i64, market_id: &str, created_at: i64) -> SignalRecord {
        SignalRecord {
            id,
            market_id: market_id.to_owned(),
            answer_id: None,
            post_uri: format!("at://post/{created_at}"),
            author_handle: "alice.test".to_owned(),
            post_text: "The thing happened.".to_owned(),
            material: true,
            direction: Direction::Yes,
            strength: 2,
            impulse: 0.37,
            p_after: 0.62,
            created_at,
        }
    }

    fn sample_trade(market_id: &str, created_at: i64) -> TradeRecord {
        TradeRecord {
            id: 0,
            market_id: market_id.to_owned(),
            answer_id: None,
            action: TradeAction::BuyYes,
            mana_amount: 12.5,
            shares_delta: 20.0,
            p_internal: 0.65,
            p_market: 0.55,
            trigger_post_uri: Some("at://post/1".to_owned()),
            manifold_bet_id: Some("bet-1".to_owned()),
            created_at,
        }
    }

    fn assert_market_eq(actual: &TrackedMarket, expected: &TrackedMarket) {
        assert_eq!(actual.id, expected.id);
        assert_eq!(actual.slug, expected.slug);
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
    }

    fn market_ids(markets: &[TrackedMarket]) -> Vec<&str> {
        markets.iter().map(|market| market.id.as_str()).collect()
    }

    #[test]
    fn market_roundtrip() {
        let db = Db::open_in_memory().expect("in-memory DB");
        let market = sample_market("m1", 1_700_000_000);

        assert!(db.get_market("m1").expect("get missing").is_none());
        db.insert_market(&market).expect("insert market");

        let loaded = db.get_market("m1").expect("get market").expect("present");
        assert_market_eq(&loaded, &market);

        let listed = db.list_markets().expect("list markets");
        assert_eq!(listed.len(), 1);
        assert_market_eq(&listed[0], &market);

        db.delete_market("m1").expect("delete market");
        assert!(db.get_market("m1").expect("get deleted").is_none());
    }

    #[test]
    fn insert_duplicate_market_fails() {
        let db = Db::open_in_memory().expect("in-memory DB");
        let market = sample_market("m1", 1);
        db.insert_market(&market).expect("first insert");
        assert!(db.insert_market(&market).is_err());
    }

    #[test]
    fn list_markets_is_ordered_by_creation_time() {
        let db = Db::open_in_memory().expect("in-memory DB");
        for (id, created_at) in [("late", 300), ("early", 100), ("middle", 200)] {
            db.insert_market(&sample_market(id, created_at))
                .expect("insert market");
        }

        let listed = db.list_markets().expect("list markets");
        assert_eq!(market_ids(&listed), vec!["early", "middle", "late"]);
    }

    #[test]
    fn list_markets_by_status_filters() {
        let db = Db::open_in_memory().expect("in-memory DB");
        let mut paused = sample_market("paused", 1);
        paused.status = MarketStatus::Paused;
        let closed = {
            let mut market = sample_market("closed", 2);
            market.status = MarketStatus::Closed;
            market
        };
        db.insert_market(&paused).expect("insert paused");
        db.insert_market(&closed).expect("insert closed");
        db.insert_market(&sample_market("active", 3))
            .expect("insert active");

        let active = db
            .list_markets_by_status(MarketStatus::Active)
            .expect("list active");
        assert_eq!(market_ids(&active), vec!["active"]);

        let paused = db
            .list_markets_by_status(MarketStatus::Paused)
            .expect("list paused");
        assert_eq!(market_ids(&paused), vec!["paused"]);

        let closed = db
            .list_markets_by_status(MarketStatus::Closed)
            .expect("list closed");
        assert_eq!(market_ids(&closed), vec!["closed"]);
    }

    #[test]
    fn updates_persist() {
        let db = Db::open_in_memory().expect("in-memory DB");
        db.insert_market(&sample_market("m1", 1))
            .expect("insert market");

        db.update_market_status("m1", MarketStatus::Paused)
            .expect("update status");
        db.update_budget("m1", 250.0).expect("update budget");
        db.update_belief("m1", 0.8, 0.61, 1_700_000_500)
            .expect("update belief");
        db.update_position("m1", 30.0, 4.5, 12.0, 3.0)
            .expect("update position");
        db.set_thread_id("m1", "thread-9").expect("set thread");

        let market = db.get_market("m1").expect("get market").expect("present");
        assert_eq!(market.status, MarketStatus::Paused);
        assert_eq!(market.budget_mana, 250.0);
        assert_eq!(market.p_internal_log_odds, 0.8);
        assert_eq!(market.p_market_last, 0.61);
        assert_eq!(market.belief_updated_at, 1_700_000_500);
        assert_eq!(market.invested_mana, 30.0);
        assert_eq!(market.realized_pnl_mana, 4.5);
        assert_eq!(market.yes_shares, 12.0);
        assert_eq!(market.no_shares, 3.0);
        assert_eq!(market.discord_thread_id, "thread-9");
    }

    #[test]
    fn updating_missing_market_fails() {
        let db = Db::open_in_memory().expect("in-memory DB");
        assert!(db.update_budget("missing", 10.0).is_err());
        assert!(db
            .update_market_status("missing", MarketStatus::Closed)
            .is_err());
        assert!(db.set_thread_id("missing", "t").is_err());
    }

    #[test]
    fn signal_ledger_roundtrip_and_ordering() {
        let db = Db::open_in_memory().expect("in-memory DB");
        db.insert_market(&sample_market("m1", 1)).expect("market");

        let first_id = db
            .insert_signal(&sample_signal(999, "m1", 100))
            .expect("insert signal");
        let second_id = db
            .insert_signal(&sample_signal(999, "m1", 200))
            .expect("insert signal");
        db.insert_signal(&sample_signal(999, "other", 300))
            .expect("insert other signal");

        assert!(first_id > 0);
        assert!(second_id > first_id);

        let recent = db.recent_signals("m1", 10).expect("recent signals");
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].created_at, 200);
        assert_eq!(recent[0].id, second_id);
        assert_eq!(recent[0].direction, Direction::Yes);
        assert!(recent[0].material);
        assert_eq!(recent[0].strength, 2);
        assert_eq!(recent[1].created_at, 100);

        let limited = db.recent_signals("m1", 1).expect("limited signals");
        assert_eq!(limited.len(), 1);
        assert_eq!(limited[0].created_at, 200);

        assert_eq!(db.latest_signal_time("m1").expect("latest time"), Some(200));
        assert_eq!(db.latest_signal_time("none").expect("latest missing"), None);
    }

    #[test]
    fn trade_ledger_roundtrip_and_ordering() {
        let db = Db::open_in_memory().expect("in-memory DB");
        let first_id = db
            .insert_trade(&sample_trade("m1", 100))
            .expect("insert trade");
        let mut sell = sample_trade("m1", 200);
        sell.action = TradeAction::SellYes;
        sell.trigger_post_uri = None;
        sell.manifold_bet_id = None;
        let second_id = db.insert_trade(&sell).expect("insert sell");
        db.insert_trade(&sample_trade("other", 300))
            .expect("insert other trade");

        assert!(first_id > 0 && second_id > first_id);

        let recent = db.recent_trades("m1", 10).expect("recent trades");
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].id, second_id);
        assert_eq!(recent[0].action, TradeAction::SellYes);
        assert_eq!(recent[0].trigger_post_uri, None);
        assert_eq!(recent[0].manifold_bet_id, None);
        assert_eq!(recent[1].id, first_id);
        assert_eq!(recent[1].action, TradeAction::BuyYes);
        assert_eq!(recent[1].mana_amount, 12.5);
        assert_eq!(recent[1].shares_delta, 20.0);
        assert_eq!(recent[1].p_internal, 0.65);
        assert_eq!(recent[1].p_market, 0.55);

        assert_eq!(db.recent_trades("m1", 1).expect("limited").len(), 1);
        assert!(db.recent_trades("none", 10).expect("empty").is_empty());
    }

    #[test]
    fn key_value_roundtrip() {
        let db = Db::open_in_memory().expect("in-memory DB");
        assert_eq!(db.get_kv("cursor").expect("missing key"), None);

        db.set_kv("cursor", "123").expect("set kv");
        assert_eq!(db.get_kv("cursor").expect("get kv").as_deref(), Some("123"));

        db.set_kv("cursor", "456").expect("overwrite kv");
        assert_eq!(
            db.get_kv("cursor").expect("get overwritten").as_deref(),
            Some("456")
        );

        db.delete_kv("cursor").expect("delete kv");
        assert_eq!(db.get_kv("cursor").expect("get deleted"), None);
        // Deleting a missing key is idempotent.
        db.delete_kv("cursor").expect("delete missing kv");
    }

    #[test]
    fn open_creates_parent_directories_and_schema() {
        let path = std::env::temp_dir().join(format!(
            "bsky-manifold-arb-test-{}-{}/nested/arb.db",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let db = Db::open(&path).expect("open file DB");
        db.set_kv("k", "v").expect("write with schema");

        // Reopening the same file keeps data and tolerates an existing schema.
        let reopened = Db::open(&path).expect("reopen file DB");
        assert_eq!(
            reopened.get_kv("k").expect("read reopened").as_deref(),
            Some("v")
        );

        drop(db);
        drop(reopened);
        let _ = std::fs::remove_dir_all(path.parent().and_then(Path::parent).expect("parent"));
    }

    #[test]
    fn answers_crud_and_belief_update() {
        let db = Db::open_in_memory().expect("in-memory DB");
        db.insert_market(&sample_market("m1", 1)).expect("market");

        let answer = TrackedAnswer {
            id: "ans1".to_owned(),
            market_id: "m1".to_owned(),
            text: "Candidate A".to_owned(),
            p_internal_log_odds: 0.0,
            p_market_last: 0.5,
            belief_updated_at: 100,
            yes_shares: 10.0,
            no_shares: 0.0,
            created_at: 100,
        };
        db.upsert_answer(&answer).expect("upsert answer");

        let fetched = db.get_answer("m1", "ans1").expect("get").expect("found");
        assert_eq!(fetched, answer);

        let list = db.list_answers("m1").expect("list");
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].text, "Candidate A");

        db.update_answer_belief("m1", "ans1", 1.23, 0.77, 200).expect("update belief");
        let updated = db.get_answer("m1", "ans1").expect("get").expect("found");
        assert_eq!(updated.p_internal_log_odds, 1.23);
        assert_eq!(updated.p_market_last, 0.77);
        assert_eq!(updated.belief_updated_at, 200);

        db.update_answer_shares("m1", "ans1", 25.0, 5.0).expect("update shares");
        let updated_shares = db.get_answer("m1", "ans1").expect("get").expect("found");
        assert_eq!(updated_shares.yes_shares, 25.0);
        assert_eq!(updated_shares.no_shares, 5.0);

        db.delete_answers_for_market("m1").expect("delete");
        assert!(db.list_answers("m1").expect("empty list").is_empty());
    }
}
