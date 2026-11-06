-- 007: strategy ownership for tracked markets, plus the snipe evaluation ledger.
--
-- `strategy` names the component that owns a market row: 'news' for the
-- operator's `/track`ed markets, 'snipe' for markets created autonomously by
-- the S2 new-market sniper. Operator-facing listings show only 'news'; the
-- trading executor and reconciliation still see every active row. Existing
-- rows default to 'news', which is what they were before this column existed.
ALTER TABLE tracked_markets ADD COLUMN strategy TEXT NOT NULL DEFAULT 'news';

-- One row per snipe EVALUATION, including skipped markets, so the decision
-- thresholds can be tuned from evidence instead of guesses. `action` records
-- what the strategy would do or did ('BUY_YES' | 'BUY_NO' | 'SKIP') and
-- `reason` explains the decision (a skip reason, or empty for a buy).
-- Model outputs are nullable because a timed-out or failed decision call still
-- produces an evaluated row with no probability.
CREATE TABLE IF NOT EXISTS snipe_ledger (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  contract_id      TEXT NOT NULL,
  question         TEXT NOT NULL,
  p_initial        REAL,
  p_model          REAL,
  gap              REAL,
  answer           TEXT,
  action           TEXT NOT NULL,
  reason           TEXT,
  mana_amount      REAL NOT NULL DEFAULT 0,
  limit_prob       REAL,
  manifold_bet_id  TEXT,
  cost_usd         REAL,
  created_at       INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_snipe_created
  ON snipe_ledger(created_at DESC);
