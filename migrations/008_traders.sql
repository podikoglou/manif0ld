-- 008: S3 smart-money trader leaderboard and the copy-trade ledger.
--
-- `trader_scores` is the persisted leaderboard (`docs/SPEC.md` §6.10). One row
-- per scored trader: the two profit windows the score blends, the activity
-- sample used by the `TRADER_MIN_BETS` gate, the derived score, and when the
-- row was last refreshed. The table is rewritten by one replace-all
-- transaction per refresh, so it always mirrors the in-memory
-- [`crate::traders::TraderBoard`]; `username` is nullable because the bulk
-- user lookup can miss a deleted or banned account.
CREATE TABLE IF NOT EXISTS trader_scores (
  user_id          TEXT PRIMARY KEY,
  username         TEXT,
  profit_all_time  REAL,
  profit_weekly    REAL,
  bet_count        INTEGER,
  score            REAL,
  updated_at       INTEGER NOT NULL
);

-- The board is always read in score order (`TraderBoard::top`).
CREATE INDEX IF NOT EXISTS idx_trader_scores_score
  ON trader_scores(score DESC);

-- One row per S3 copy-trade EVALUATION, including refusals. The columns are
-- fixed here, in the same migration as the leaderboard, so the copy strategy
-- (next task) adds no schema change. `action` is 'COPY' | 'SKIP'; `reason`
-- explains a skip or the follow rationale; model outputs (`p_informed`,
-- `fair_prob`, `gap`) are nullable because a failed or skipped gate still
-- produces a row.
CREATE TABLE IF NOT EXISTS copy_ledger (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  trader_id        TEXT NOT NULL,
  contract_id      TEXT NOT NULL,
  question         TEXT NOT NULL,
  bet_id           TEXT,
  bet_amount       REAL,
  p_before         REAL,
  p_after          REAL,
  p_informed       REAL,
  fair_prob        REAL,
  gap              REAL,
  action           TEXT NOT NULL,
  reason           TEXT,
  mana_amount      REAL NOT NULL DEFAULT 0,
  manifold_bet_id  TEXT,
  created_at       INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_copy_ledger_created
  ON copy_ledger(created_at DESC);
