CREATE TABLE IF NOT EXISTS tracked_markets (
  id                   TEXT PRIMARY KEY,
  slug                 TEXT NOT NULL,
  question             TEXT NOT NULL,
  description          TEXT NOT NULL,
  resolution_criteria  TEXT NOT NULL,
  close_time           INTEGER,
  status               TEXT NOT NULL,
  budget_mana          REAL NOT NULL,
  invested_mana        REAL NOT NULL DEFAULT 0,
  realized_pnl_mana    REAL NOT NULL DEFAULT 0,
  yes_shares           REAL NOT NULL DEFAULT 0,
  no_shares            REAL NOT NULL DEFAULT 0,
  p_internal_log_odds  REAL NOT NULL,
  p_market_last        REAL NOT NULL,
  belief_updated_at    INTEGER NOT NULL,
  sieve_source         TEXT NOT NULL,
  discord_thread_id    TEXT NOT NULL,
  created_at           INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS market_vip_dids (
  market_id  TEXT NOT NULL,
  did        TEXT NOT NULL,
  handle     TEXT NOT NULL,
  PRIMARY KEY (market_id, did),
  FOREIGN KEY (market_id) REFERENCES tracked_markets(id)
);

CREATE TABLE IF NOT EXISTS signal_ledger (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  market_id      TEXT NOT NULL,
  post_uri       TEXT NOT NULL,
  author_handle  TEXT NOT NULL,
  post_text      TEXT NOT NULL,
  material       INTEGER NOT NULL,
  direction      TEXT NOT NULL,
  strength       INTEGER NOT NULL,
  impulse        REAL NOT NULL,
  p_after        REAL NOT NULL,
  created_at     INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS trade_ledger (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  market_id        TEXT NOT NULL,
  action           TEXT NOT NULL,
  mana_amount      REAL NOT NULL,
  shares_delta     REAL NOT NULL,
  p_internal       REAL NOT NULL,
  p_market         REAL NOT NULL,
  trigger_post_uri TEXT,
  manifold_bet_id  TEXT,
  created_at       INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS kv (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_signal_market_created
  ON signal_ledger(market_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_trade_market_created
  ON trade_ledger(market_id, created_at DESC);
