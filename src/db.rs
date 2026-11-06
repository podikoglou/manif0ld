//! SQLite persistence for signal, trade, answer, and key/value rows.
//!
//! Market rows (`tracked_markets`) are not here: [`crate::market_store`] owns
//! them so that no caller can write a market column without keeping the shared
//! in-memory snapshot in step. [`Db::snipe_pool_invested`] is the one read-only
//! aggregate over that table, because the snipe pool has no cached counterpart.
//! This module keeps the transaction domain (one connection) that both layers
//! share.
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
use crate::types::{
    MarketStatus, SignalRecord, SnipeRecord, TrackedAnswer, TradeRecord, STRATEGY_SNIPE,
};

const ANSWER_COLUMNS: &str = "id, market_id, text, p_internal_log_odds, p_market_last, \
     belief_updated_at, yes_shares, no_shares, created_at, invested_mana";

const SIGNAL_COLUMNS: &str = "id, market_id, answer_id, post_uri, author_handle, post_text, material, \
     direction, strength, impulse, p_after, created_at, p_evidence, p_target, strength_real, p_priced";

const TRADE_COLUMNS: &str = "id, market_id, answer_id, action, mana_amount, shares_delta, p_internal, \
     p_market, trigger_post_uri, manifold_bet_id, created_at";

const SNIPE_COLUMNS: &str = "id, contract_id, question, p_initial, p_model, gap, answer, action, \
     reason, mana_amount, limit_prob, manifold_bet_id, cost_usd, created_at";

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
        Self::apply_migrations(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Applies guarded, one-way migrations on top of the base schema.
    ///
    /// The base file uses `IF NOT EXISTS`, so it is safe to run on every open.
    /// Anything beyond that must be checked first because existing databases
    /// were created before the column existed.
    fn apply_migrations(conn: &Connection) -> AppResult<()> {
        // 002: `market_url` on `tracked_markets`. Run the ALTER exactly once.
        if !table_has_column(conn, "tracked_markets", "market_url")? {
            conn.execute_batch(include_str!("../migrations/002_market_url.sql"))
                .context("applying database migration 002 (market_url)")?;
        }
        // 003: `deleted_at` on `tracked_markets`, the soft-delete marker.
        if !table_has_column(conn, "tracked_markets", "deleted_at")? {
            conn.execute_batch(include_str!("../migrations/003_soft_delete.sql"))
                .context("applying database migration 003 (soft delete)")?;
        }
        // 004: calibrated triage probabilities on `signal_ledger`.
        if !table_has_column(conn, "signal_ledger", "p_evidence")? {
            conn.execute_batch(include_str!("../migrations/004_signal_probabilities.sql"))
                .context("applying database migration 004 (signal probabilities)")?;
        }
        // 005: per-answer trading state for multiple-choice markets.
        if !table_has_column(conn, "market_answers", "invested_mana")? {
            conn.execute_batch(include_str!("../migrations/005_answer_trading.sql"))
                .context("applying database migration 005 (answer trading)")?;
        }
        // 006: remove the obsolete Jetstream cursor. The statement is a guarded
        // `DELETE`, so it is idempotent and needs no structural check.
        conn.execute_batch(include_str!("../migrations/006_remove_jetstream_cursor.sql"))
            .context("applying database migration 006 (remove jetstream cursor)")?;
        // 007: `strategy` on `tracked_markets` plus the `snipe_ledger` table.
        // The `ALTER` must run exactly once; the table creates are guarded.
        if !table_has_column(conn, "tracked_markets", "strategy")? {
            conn.execute_batch(include_str!("../migrations/007_strategy.sql"))
                .context("applying database migration 007 (strategy and snipe ledger)")?;
        }
        // 008: the S3 trader leaderboard (`trader_scores`) and the copy-trade
        // ledger. Every statement is `IF NOT EXISTS`, and the guard keys on a
        // column of the new table, so the batch runs once per database.
        if !table_has_column(conn, "trader_scores", "user_id")? {
            conn.execute_batch(include_str!("../migrations/008_traders.sql"))
                .context("applying database migration 008 (trader scores and copy ledger)")?;
        }
        Ok(())
    }

    /// Acquires the single connection.
    ///
    /// Visible to [`crate::market_store`], which owns the `tracked_markets`
    /// queries and therefore needs the same connection.
    pub(crate) fn lock(&self) -> AppResult<MutexGuard<'_, Connection>> {
        self.conn
            .lock()
            .map_err(|_| anyhow!("database mutex poisoned"))
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
             direction, strength, impulse, p_after, created_at, p_evidence, p_target, strength_real, \
             p_priced) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
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
                record.p_evidence,
                record.p_target,
                record.strength_real,
                record.p_priced,
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
    // Snipe evaluation ledger
    // ---------------------------------------------------------------------

    /// Appends one snipe evaluation and returns its assigned row ID.
    ///
    /// Skips are recorded too, so the thresholds can be tuned later.
    /// `record.id` is ignored; SQLite assigns the identifier.
    pub fn insert_snipe(&self, record: &SnipeRecord) -> AppResult<i64> {
        let conn = self.lock()?;
        conn.execute(
            "INSERT INTO snipe_ledger (contract_id, question, p_initial, p_model, gap, answer, \
             action, reason, mana_amount, limit_prob, manifold_bet_id, cost_usd, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                record.contract_id,
                record.question,
                record.p_initial,
                record.p_model,
                record.gap,
                record.answer,
                record.action,
                record.reason,
                record.mana_amount,
                record.limit_prob,
                record.manifold_bet_id,
                record.cost_usd,
                record.created_at,
            ],
        )
        .with_context(|| {
            format!(
                "inserting snipe evaluation for contract {}",
                record.contract_id
            )
        })?;
        Ok(conn.last_insert_rowid())
    }

    /// Returns the newest snipe evaluations, newest first.
    pub fn recent_snipes(&self, limit: usize) -> AppResult<Vec<SnipeRecord>> {
        let limit = limit_to_i64(limit);
        let conn = self.lock()?;
        let mut statement = conn
            .prepare(&format!(
                "SELECT {SNIPE_COLUMNS} FROM snipe_ledger \
                 ORDER BY created_at DESC, id DESC LIMIT ?1"
            ))
            .context("preparing snipe history")?;
        let rows = statement
            .query_map(params![limit], snipe_from_row)
            .context("querying snipe history")?;
        collect_rows(rows, "listing snipe evaluations")
    }

    /// Mana currently committed to open S2 snipe positions.
    ///
    /// Exact semantics: `SUM(invested_mana - realized_pnl_mana)` over every
    /// `tracked_markets` row where `strategy = 'snipe'`, `status = 'ACTIVE'`,
    /// and `deleted_at IS NULL`. Returns `0.0` when no row matches. The snipe
    /// pool budget in `docs/ALGO.md` §S2 is separate from the per-market
    /// budgets: `SNIPE_POOL_MANA - snipe_pool_invested()` is what a new snipe
    /// may still deploy.
    pub fn snipe_pool_invested(&self) -> AppResult<f64> {
        let conn = self.lock()?;
        conn.query_row(
            "SELECT COALESCE(SUM(invested_mana - realized_pnl_mana), 0.0) \
             FROM tracked_markets \
             WHERE strategy = ?1 AND status = ?2 AND deleted_at IS NULL",
            params![STRATEGY_SNIPE, MarketStatus::Active.as_str()],
            |row| row.get::<_, f64>(0),
        )
        .context("summing invested mana across active snipe markets")
    }

    // ---------------------------------------------------------------------
    // Answers (multiple choice markets)
    // ---------------------------------------------------------------------

    /// Inserts or replaces a market answer.
    pub fn upsert_answer(&self, answer: &TrackedAnswer) -> AppResult<()> {
        let conn = self.lock()?;
        conn.execute(
            "INSERT OR REPLACE INTO market_answers (id, market_id, text, p_internal_log_odds, \
             p_market_last, belief_updated_at, yes_shares, no_shares, created_at, invested_mana) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
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
                answer.invested_mana,
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

    /// Stores the reconciled position of one answer.
    ///
    /// Shares and cost basis move together because they always come from the
    /// same remote `ContractMetric` row. Belief columns are untouched: triage
    /// owns those and may write them concurrently.
    pub fn update_answer_position(
        &self,
        market_id: &str,
        answer_id: &str,
        yes_shares: f64,
        no_shares: f64,
        invested: f64,
    ) -> AppResult<()> {
        let conn = self.lock()?;
        let updated = conn
            .execute(
                "UPDATE market_answers SET yes_shares = ?3, no_shares = ?4, invested_mana = ?5 \
                 WHERE market_id = ?1 AND id = ?2",
                params![market_id, answer_id, yes_shares, no_shares, invested],
            )
            .with_context(|| format!("updating position for answer {answer_id} in {market_id}"))?;
        ensure_updated(updated, &format!("{market_id}/{answer_id}"))
    }

    /// Updates the market price of one answer without touching belief state.
    ///
    /// This is the per-tick price sync. `belief_updated_at` is deliberately
    /// left alone: it marks the last impulse, and decay is computed from it.
    pub fn update_answer_market_price(
        &self,
        market_id: &str,
        answer_id: &str,
        p_market: f64,
    ) -> AppResult<()> {
        let conn = self.lock()?;
        let updated = conn
            .execute(
                "UPDATE market_answers SET p_market_last = ?3 WHERE market_id = ?1 AND id = ?2",
                params![market_id, answer_id, p_market],
            )
            .with_context(|| format!("updating market price for answer {answer_id} in {market_id}"))?;
        ensure_updated(updated, &format!("{market_id}/{answer_id}"))
    }

    /// Updates the text of one answer after an operator or Manifold edit.
    pub fn update_answer_text(
        &self,
        market_id: &str,
        answer_id: &str,
        text: &str,
    ) -> AppResult<()> {
        let conn = self.lock()?;
        let updated = conn
            .execute(
                "UPDATE market_answers SET text = ?3 WHERE market_id = ?1 AND id = ?2",
                params![market_id, answer_id, text],
            )
            .with_context(|| format!("updating text for answer {answer_id} in {market_id}"))?;
        ensure_updated(updated, &format!("{market_id}/{answer_id}"))
    }

    /// Inserts an answer only when it is not tracked yet.
    ///
    /// Unlike [`Db::upsert_answer`], an existing row is left untouched, so a
    /// new answer discovered during a price sync cannot overwrite belief or
    /// position state.
    pub fn insert_answer_if_missing(&self, answer: &TrackedAnswer) -> AppResult<bool> {
        let conn = self.lock()?;
        let inserted = conn
            .execute(
                "INSERT OR IGNORE INTO market_answers (id, market_id, text, p_internal_log_odds, \
                 p_market_last, belief_updated_at, yes_shares, no_shares, created_at, invested_mana) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
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
                    answer.invested_mana,
                ],
            )
            .with_context(|| {
                format!("inserting new answer {} for market {}", answer.id, answer.market_id)
            })?;
        Ok(inserted > 0)
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
pub(crate) fn ensure_updated(rows: usize, market_id: &str) -> AppResult<()> {
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
pub(crate) fn invalid_text(column: usize, message: String) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        column,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            message,
        )),
    )
}

/// Reports whether `table` has a column named `column`.
fn table_has_column(conn: &Connection, table: &str, column: &str) -> AppResult<bool> {
    let mut statement = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .context("preparing column listing")?;
    let names = statement
        .query_map([], |row| row.get::<_, String>(1))
        .context("querying table columns")?;
    for name in names {
        if name? == column {
            return Ok(true);
        }
    }
    Ok(false)
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
        p_evidence: row.get(12)?,
        p_target: row.get(13)?,
        strength_real: row.get(14)?,
        p_priced: row.get(15)?,
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

fn snipe_from_row(row: &Row<'_>) -> rusqlite::Result<SnipeRecord> {
    Ok(SnipeRecord {
        id: row.get(0)?,
        contract_id: row.get(1)?,
        question: row.get(2)?,
        p_initial: row.get(3)?,
        p_model: row.get(4)?,
        gap: row.get(5)?,
        answer: row.get(6)?,
        action: row.get(7)?,
        reason: row.get(8)?,
        mana_amount: row.get(9)?,
        limit_prob: row.get(10)?,
        manifold_bet_id: row.get(11)?,
        cost_usd: row.get(12)?,
        created_at: row.get(13)?,
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
        invested_mana: row.get(9)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Direction, SnipeRecord, TradeAction, STRATEGY_NEWS};
    use chrono::Utc;

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
            p_evidence: Some(0.81),
            p_target: Some(0.88),
            strength_real: Some(2.4),
            p_priced: Some(0.1),
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

    #[test]
    fn market_url_migration_backfills_existing_rows() {
        // Simulate a database created before migration 002: run only the base
        // schema and insert a row without the column.
        let conn = Connection::open_in_memory().expect("in-memory connection");
        conn.execute_batch(include_str!("../migrations/001_init.sql"))
            .expect("base schema applies");
        conn.execute(
            "INSERT INTO tracked_markets \
             (id, slug, question, description, resolution_criteria, status, budget_mana, \
              invested_mana, realized_pnl_mana, yes_shares, no_shares, p_internal_log_odds, \
              p_market_last, belief_updated_at, sieve_source, discord_thread_id, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
            params![
                "old", "old-slug", "Q?", "D", "C", "ACTIVE", 100.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                0.5, 1, "match Old { \"x\" }", "", 1,
            ],
        )
        .expect("pre-migration row inserts");

        Db::apply_migrations(&conn).expect("migration 002 applies");
        assert!(table_has_column(&conn, "tracked_markets", "market_url").expect("has column"));

        // The pre-existing row is readable and defaults to an empty URL. Only
        // the columns under test are selected: market rows are not this
        // module's contract any more.
        let (id, market_url): (String, String) = conn
            .query_row(
                "SELECT id, market_url FROM tracked_markets WHERE id = 'old'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("pre-002 row still reads");
        assert_eq!(id, "old");
        assert_eq!(market_url, "");

        // Applying again is a no-op.
        Db::apply_migrations(&conn).expect("migration 002 is idempotent");
    }

    fn sample_snipe(contract_id: &str, created_at: i64) -> SnipeRecord {
        SnipeRecord {
            id: 0,
            contract_id: contract_id.to_owned(),
            question: "Will the thing happen?".to_owned(),
            p_initial: Some(0.40),
            p_model: Some(0.62),
            gap: Some(0.22),
            answer: None,
            action: "BUY_YES".to_owned(),
            reason: Some("gap above threshold".to_owned()),
            mana_amount: 8.5,
            limit_prob: Some(0.61),
            manifold_bet_id: Some("bet-snipe-1".to_owned()),
            cost_usd: Some(0.0004),
            created_at,
        }
    }

    #[test]
    fn strategy_migration_backfills_news_and_is_idempotent() {
        // Simulate a database created before migration 007: base schema only.
        let conn = Connection::open_in_memory().expect("in-memory connection");
        conn.execute_batch(include_str!("../migrations/001_init.sql"))
            .expect("base schema applies");
        conn.execute(
            "INSERT INTO tracked_markets \
             (id, slug, question, description, resolution_criteria, status, budget_mana, \
              invested_mana, realized_pnl_mana, yes_shares, no_shares, p_internal_log_odds, \
              p_market_last, belief_updated_at, sieve_source, discord_thread_id, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
            params![
                "old", "old-slug", "Q?", "D", "C", "ACTIVE", 100.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                0.5, 1, "match Old { \"x\" }", "", 1,
            ],
        )
        .expect("pre-007 row inserts");

        Db::apply_migrations(&conn).expect("migration 007 applies");
        assert!(table_has_column(&conn, "tracked_markets", "strategy").expect("has column"));

        // The pre-existing row is now a news market.
        let strategy: String = conn
            .query_row(
                "SELECT strategy FROM tracked_markets WHERE id = 'old'",
                [],
                |row| row.get(0),
            )
            .expect("pre-007 row still reads");
        assert_eq!(strategy, STRATEGY_NEWS);

        // The snipe ledger exists after the migration.
        conn.execute(
            "INSERT INTO snipe_ledger (contract_id, question, action, mana_amount, created_at) \
             VALUES ('c1', 'Q?', 'SKIP', 0, 1)",
            [],
        )
        .expect("snipe ledger accepts a row");

        // Applying again is a no-op.
        Db::apply_migrations(&conn).expect("migration 007 is idempotent");
    }

    #[test]
    fn trader_migration_creates_both_tables_and_is_idempotent() {
        // Simulate a database created before migration 008: base schema only,
        // then the earlier migrations, as `from_connection` would apply them.
        let conn = Connection::open_in_memory().expect("in-memory connection");
        conn.execute_batch(include_str!("../migrations/001_init.sql"))
            .expect("base schema applies");
        Db::apply_migrations(&conn).expect("migrations apply");

        assert!(table_has_column(&conn, "trader_scores", "user_id").expect("has column"));
        assert!(table_has_column(&conn, "copy_ledger", "trader_id").expect("has column"));

        conn.execute(
            "INSERT INTO trader_scores \
             (user_id, username, profit_all_time, profit_weekly, bet_count, score, updated_at) \
             VALUES ('u1', 'alice', 1000.0, 250.0, 40, 1.5, 10)",
            [],
        )
        .expect("trader_scores accepts a row");
        conn.execute(
            "INSERT INTO copy_ledger \
             (trader_id, contract_id, question, action, created_at) \
             VALUES ('u1', 'c1', 'Q?', 'SKIP', 10)",
            [],
        )
        .expect("copy_ledger accepts a row");

        // The `mana_amount` default belongs to the unpriced case.
        let mana: f64 = conn
            .query_row("SELECT mana_amount FROM copy_ledger WHERE contract_id = 'c1'", [], |row| {
                row.get(0)
            })
            .expect("copy row reads");
        assert_eq!(mana, 0.0);

        // Applying again is a no-op: no error and no extra rows.
        Db::apply_migrations(&conn).expect("migration 008 is idempotent");
        let scored: i64 = conn
            .query_row("SELECT COUNT(*) FROM trader_scores", [], |row| row.get(0))
            .expect("trader rows count");
        assert_eq!(scored, 1);
    }

    #[test]
    fn snipe_ledger_roundtrip_and_ordering() {
        let db = Db::open_in_memory().expect("in-memory DB");
        let first_id = db
            .insert_snipe(&sample_snipe("c1", 100))
            .expect("insert snipe");
        let mut skip = sample_snipe("c2", 200);
        skip.action = "SKIP".to_owned();
        skip.reason = Some("liquidity below SNIPE_MIN_LIQ".to_owned());
        skip.mana_amount = 0.0;
        skip.p_model = None;
        skip.gap = None;
        skip.limit_prob = None;
        skip.manifold_bet_id = None;
        skip.cost_usd = None;
        let second_id = db.insert_snipe(&skip).expect("insert skip");

        assert!(first_id > 0 && second_id > first_id);

        let recent = db.recent_snipes(10).expect("recent snipes");
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].id, second_id);
        assert_eq!(recent[0].contract_id, "c2");
        assert_eq!(recent[0].action, "SKIP");
        assert_eq!(recent[0].p_model, None);
        assert_eq!(recent[0].reason.as_deref(), Some("liquidity below SNIPE_MIN_LIQ"));
        assert_eq!(recent[1].id, first_id);
        assert_eq!(recent[1].action, "BUY_YES");
        assert_eq!(recent[1].p_initial, Some(0.40));
        assert_eq!(recent[1].p_model, Some(0.62));
        assert_eq!(recent[1].gap, Some(0.22));
        assert_eq!(recent[1].mana_amount, 8.5);
        assert_eq!(recent[1].limit_prob, Some(0.61));
        assert_eq!(recent[1].manifold_bet_id.as_deref(), Some("bet-snipe-1"));
        assert_eq!(recent[1].cost_usd, Some(0.0004));

        assert_eq!(db.recent_snipes(1).expect("limited").len(), 1);
        assert_eq!(db.recent_snipes(1).expect("limited")[0].id, second_id);
    }

    #[test]
    fn snipe_pool_invested_sums_active_undeleted_snipe_rows() {
        let db = Db::open_in_memory().expect("in-memory DB");
        let store = crate::market_store::MarketStore::new(&db).expect("store loads");

        let snipe_market = |id: &str, invested: f64, realized: f64| crate::types::TrackedMarket {
            id: id.to_owned(),
            slug: format!("slug-{id}"),
            market_url: String::new(),
            question: format!("Question {id}?"),
            description: String::new(),
            resolution_criteria: String::new(),
            close_time: None,
            status: crate::types::MarketStatus::Active,
            budget_mana: 0.0,
            invested_mana: invested,
            realized_pnl_mana: realized,
            yes_shares: 0.0,
            no_shares: 0.0,
            p_internal_log_odds: 0.0,
            p_market_last: 0.5,
            belief_updated_at: 1,
            sieve_source: String::new(),
            discord_thread_id: String::new(),
            created_at: 1,
            deleted_at: None,
            outcome_type: "BINARY".to_owned(),
            should_answers_sum_to_one: false,
            strategy: crate::types::STRATEGY_SNIPE.to_owned(),
        };

        store.insert(&snipe_market("s1", 30.0, 5.0)).expect("s1");
        store.insert(&snipe_market("s2", 12.0, 2.0)).expect("s2");
        store.insert(&snipe_market("s3", 50.0, 0.0)).expect("s3");
        store.insert(&snipe_market("s4", 70.0, 0.0)).expect("s4");
        store
            .record_status("s3", crate::types::MarketStatus::Paused)
            .expect("pause s3");
        store.untrack("s4", 1_700_000_900).expect("untrack s4");
        insert_market_row(&db);

        // (30 - 5) + (12 - 2) = 35. The paused, soft-deleted, and news rows
        // are excluded.
        assert_eq!(db.snipe_pool_invested().expect("pool total"), 35.0);
    }

    #[test]
    fn snipe_pool_invested_is_zero_without_snipe_rows() {
        let db = Db::open_in_memory().expect("in-memory DB");
        insert_market_row(&db);
        assert_eq!(db.snipe_pool_invested().expect("pool total"), 0.0);
    }

    #[test]
    fn signal_ledger_roundtrip_and_ordering() {
        let db = Db::open_in_memory().expect("in-memory DB");

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
        assert_eq!(db.get_kv("example_key").expect("missing key"), None);

        db.set_kv("example_key", "123").expect("set kv");
        assert_eq!(
            db.get_kv("example_key").expect("get kv").as_deref(),
            Some("123")
        );

        db.set_kv("example_key", "456").expect("overwrite kv");
        assert_eq!(
            db.get_kv("example_key").expect("get overwritten").as_deref(),
            Some("456")
        );

        db.delete_kv("example_key").expect("delete kv");
        assert_eq!(db.get_kv("example_key").expect("get deleted"), None);
        // Deleting a missing key is idempotent.
        db.delete_kv("example_key").expect("delete missing kv");
    }

    #[test]
    fn open_removes_the_obsolete_jetstream_cursor() {
        let path = std::env::temp_dir().join(format!(
            "manif0ld-test-{}-{}/cursor/manif0ld.db",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        std::fs::create_dir_all(path.parent().expect("parent")).expect("create dirs");
        {
            // Simulate a database written by a build that still replayed.
            let conn = Connection::open(&path).expect("raw connection");
            conn.execute_batch(include_str!("../migrations/001_init.sql"))
                .expect("base schema applies");
            conn.execute(
                "INSERT INTO kv (key, value) VALUES ('jetstream_cursor_us', '1793983780195469')",
                [],
            )
            .expect("seed cursor");
        }

        let db = Db::open(&path).expect("open file DB");
        assert_eq!(
            db.get_kv("jetstream_cursor_us").expect("get cursor"),
            None,
            "the obsolete cursor must be deleted on open"
        );

        drop(db);
        let _ = std::fs::remove_dir_all(path.parent().expect("parent"));
    }

    #[test]
    fn open_creates_parent_directories_and_schema() {
        let path = std::env::temp_dir().join(format!(
            "manif0ld-test-{}-{}/nested/manif0ld.db",
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

    /// Inserts one tracked market so answer rows have a valid foreign key.
    ///
    /// Market rows belong to [`crate::market_store`]; these tests only need a
    /// row to exist, so they create it through the store like production code.
    fn insert_market_row(db: &Db) {
        let store = crate::market_store::MarketStore::new(db).expect("store loads");
        store
            .insert(&crate::types::TrackedMarket {
                id: "m1".to_owned(),
                slug: "slug-m1".to_owned(),
                market_url: String::new(),
                question: "Question m1?".to_owned(),
                description: "Description".to_owned(),
                resolution_criteria: "Resolves YES if the thing happens.".to_owned(),
                close_time: Some(86_401),
                status: crate::types::MarketStatus::Active,
                budget_mana: 100.0,
                invested_mana: 0.0,
                realized_pnl_mana: 0.0,
                yes_shares: 0.0,
                no_shares: 0.0,
                p_internal_log_odds: 0.0,
                p_market_last: 0.5,
                belief_updated_at: 1,
                sieve_source: "match Market { \"thing\" }".to_owned(),
                discord_thread_id: String::new(),
                created_at: 1,
                deleted_at: None,
                outcome_type: "BINARY".to_owned(),
                should_answers_sum_to_one: false,
                strategy: crate::types::STRATEGY_NEWS.to_owned(),
            })
            .expect("insert market");
    }

    #[test]
    fn answers_crud_and_belief_update() {
        let db = Db::open_in_memory().expect("in-memory DB");
        insert_market_row(&db);

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
            invested_mana: 0.0,
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

    fn answer_row(id: &str, text: &str) -> TrackedAnswer {
        TrackedAnswer {
            id: id.to_owned(),
            market_id: "m1".to_owned(),
            text: text.to_owned(),
            p_internal_log_odds: 0.0,
            p_market_last: 0.5,
            belief_updated_at: 100,
            yes_shares: 0.0,
            no_shares: 0.0,
            created_at: 100,
            invested_mana: 0.0,
        }
    }

    #[test]
    fn answer_price_sync_leaves_belief_and_position_alone() {
        let db = Db::open_in_memory().expect("in-memory DB");
        insert_market_row(&db);
        db.upsert_answer(&answer_row("ans1", "Candidate A"))
            .expect("answer");

        db.update_answer_belief("m1", "ans1", 1.0, 0.73, 200)
            .expect("belief");
        db.update_answer_position("m1", "ans1", 12.0, 3.0, 9.5)
            .expect("position");
        db.update_answer_market_price("m1", "ans1", 0.61)
            .expect("price");

        let answer = db.get_answer("m1", "ans1").expect("get").expect("found");
        assert_eq!(answer.p_market_last, 0.61);
        assert_eq!(answer.p_internal_log_odds, 1.0);
        assert_eq!(answer.belief_updated_at, 200);
        assert_eq!(answer.yes_shares, 12.0);
        assert_eq!(answer.no_shares, 3.0);
        assert_eq!(answer.invested_mana, 9.5);
    }

    #[test]
    fn insert_answer_if_missing_preserves_an_existing_row() {
        let db = Db::open_in_memory().expect("in-memory DB");
        insert_market_row(&db);

        let mut existing = answer_row("ans1", "Candidate A");
        existing.p_internal_log_odds = 2.0;
        existing.invested_mana = 4.0;
        db.upsert_answer(&existing).expect("answer");

        let mut replacement = answer_row("ans1", "Renamed");
        replacement.p_internal_log_odds = -1.0;
        replacement.invested_mana = 99.0;
        assert!(
            !db.insert_answer_if_missing(&replacement)
                .expect("insert existing answer"),
            "an existing answer must not be replaced"
        );

        let unchanged = db.get_answer("m1", "ans1").expect("get").expect("found");
        assert_eq!(unchanged.text, "Candidate A");
        assert_eq!(unchanged.p_internal_log_odds, 2.0);
        assert_eq!(unchanged.invested_mana, 4.0);

        assert!(
            db.insert_answer_if_missing(&answer_row("ans2", "Candidate B"))
                .expect("insert new answer"),
            "a new answer must be inserted"
        );
        assert_eq!(db.list_answers("m1").expect("list").len(), 2);
    }

    #[test]
    fn answer_position_update_leaves_belief_alone() {
        let db = Db::open_in_memory().expect("in-memory DB");
        insert_market_row(&db);
        db.upsert_answer(&answer_row("ans1", "Candidate A"))
            .expect("answer");
        db.update_answer_belief("m1", "ans1", 1.5, 0.8, 300)
            .expect("belief");

        db.update_answer_position("m1", "ans1", 20.0, 4.0, 12.5)
            .expect("position");

        let answer = db.get_answer("m1", "ans1").expect("get").expect("found");
        assert_eq!(answer.yes_shares, 20.0);
        assert_eq!(answer.no_shares, 4.0);
        assert_eq!(answer.invested_mana, 12.5);
        assert_eq!(answer.p_market_last, 0.8);
        assert_eq!(answer.p_internal_log_odds, 1.5);
        assert_eq!(answer.belief_updated_at, 300);
    }
}
