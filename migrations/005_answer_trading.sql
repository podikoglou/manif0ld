-- 005: per-answer trading state for multiple-choice markets.
--
-- `tracked_markets.invested_mana` stays the single budget ledger. For an MCQ
-- market it equals the sum of its answer rows. `market_answers.invested_mana`
-- is the cost basis used by the proportional sell math.
--
-- `outcome_type` lets the trading layer choose the binary or the per-answer
-- reconciliation path without inferring it from the presence of answers.
-- `should_answers_sum_to_one` selects normalized or independent edges.
ALTER TABLE tracked_markets ADD COLUMN outcome_type TEXT NOT NULL DEFAULT 'BINARY';
ALTER TABLE tracked_markets ADD COLUMN should_answers_sum_to_one INTEGER NOT NULL DEFAULT 0;
ALTER TABLE market_answers ADD COLUMN invested_mana REAL NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS idx_answers_market ON market_answers(market_id);
