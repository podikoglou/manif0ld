-- Soft deletes tracked markets instead of dropping their rows.
--
-- `/untrack` marks a market CLOSED and records the removal time here. The row,
-- its answers, and its ledger history stay in the database for audit, while
-- every listing query skips it. `NULL` means the market is still tracked.
ALTER TABLE tracked_markets ADD COLUMN deleted_at INTEGER;
