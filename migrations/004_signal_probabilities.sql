-- Calibrated triage answers replace the discrete direction and strength.
--
-- The decision model returns a probability for each question, so the ledger
-- keeps the numbers that produced each impulse. `direction` and `strength`
-- stay for rows written before this migration and for display; `strength_real`
-- holds the expected rubric position that `strength` rounds.
ALTER TABLE signal_ledger ADD COLUMN p_evidence REAL;
ALTER TABLE signal_ledger ADD COLUMN p_target REAL;
ALTER TABLE signal_ledger ADD COLUMN strength_real REAL;
ALTER TABLE signal_ledger ADD COLUMN p_priced REAL;
