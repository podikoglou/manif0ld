-- 009: the wire `limitProb` of every `trade_ledger` fill.
--
-- A buy is sent as a fair-capped, whole-percent limit (`docs/SPEC.md` §6.6), and
-- the cap is a function of the belief that produced the decision. Persisting it
-- makes a fill auditable after the fact: a fill above the recorded cap is a
-- real bug, and a fill far below it shows the book was better than the plan.
-- Sells and rows written before this migration carry NULL.
ALTER TABLE trade_ledger ADD COLUMN limit_prob REAL;
