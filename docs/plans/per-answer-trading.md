# Plan: Per-answer trading for multiple-choice markets

## 1. Problem

The bot has never traded. `trade_ledger` is empty and both tracked markets show
`invested_mana = 0`. Two independent causes:

1. **Triage has not produced a material signal yet.** 31 posts were triaged in
   the first five minutes; every one had `p_evidence <= 0.66` against
   `TRIAGE_MIN_EVIDENCE = 0.7`, so `assess()` returned `None`.
   That can unblock itself; cause 2 cannot.
2. **The trade path is binary-only.** Both tracked markets are
   `MULTIPLE_CHOICE`:
   - `hEpuZQpgR2` (US Midterms MEGA): `cpmm-multi-2`,
     `shouldAnswersSumToOne = false`, 53 answers.
   - `thlctZn5PU` (Israel PM): `cpmm-multi-1`,
     `shouldAnswersSumToOne = true`, 7 answers, `addAnswersMode = ANYONE`.

   Triage writes impulses into `market_answers`, but `evaluate_market` only
   reads market-level belief. For MCQ, `p_market_last` stays at the seeded
   `0.5` and market-level `p_internal_log_odds` stays at `0.0`. Edge is exactly
   `0`. `decide_trade_for_answer` exists and is unit-tested but has **no call
   site outside tests**.

There is also a latent correctness bug that must be fixed before any answer
trade: `sync_market` and `evaluate_market` reconcile the **root** position
(`parse_position`, `answerId = null`). On an MCQ market that returns nothing, so
after the first 60-second tick they would zero `invested_mana` and the share
counts. That would silently defeat the per-market budget invariant and allow
unbounded spending.

## 2. Design goals

- **One accounting implementation.** Binary and per-answer positions must share
  the same buy/sell/basis/realized-PnL math. No copy of `apply_sell`.
- **Manifold is the truth.** Local state is a cache; every tick reconciles
  shares and invested per answer, and any difference is resolved in Manifold's
  favour with a warning.
- **Coherent economics per mechanism.** Independent answers get independent
  edges. Sum-to-one answers get a normalized internal distribution so the bot
  cannot buy two mutually exclusive YES legs that are both "underpriced".
- **Bounded blast radius.** Dry-run mode, per-answer caps, a maximum number of
  positions, and a canary phase before real sizing.
- **Pure decision layer.** Network, database, and clock stay at the edges so the
  whole decision can be tested as a table.

## 3. Decisions and rejected alternatives

| Decision | Why | Rejected alternative |
| --- | --- | --- |
| Trade each answer as its own position via `POST /v0/bet` with `answerId` | One bet per decision; works on both `cpmm-multi-1` and `cpmm-multi-2`; keeps the decision layer simple | `POST /v0/multi-bet`: only `cpmm-multi-1` with `shouldAnswersSumToOne`, YES-only, equal shares per leg, refuses one-answer baskets. Too rigid. |
| Normalize internal probabilities for `shouldAnswersSumToOne` markets | A single answer impulse must logically steal mass from its rivals; otherwise edges sum to more than zero and the bot buys correlated YES legs | Propagating negative impulses to every rival at triage time: changes stored belief semantics, rewrites the signal ledger, and needs a second normalization anyway |
| YES-only entries on sum-to-one markets | A NO leg is a basket over the field; `p=0.5`-style sizing and exit rules become ambiguous, and Manifold may add internal arbitrage legs | Allow NO everywhere: more expressive, but risks churning hedge legs and fees on a mechanism we do not fully control |
| YES and NO allowed on independent (`cpmm-multi-2`) markets | Answers are true independent binaries; NO is a clean short | YES-only: leaves negative edges unusable |
| Per-answer `invested_mana` column | Sell basis is proportional to invested; without it partial exits cannot compute realized PnL | Derive basis from remote `invested`: works until a sell completes between syncs, then basis is gone |
| Column-scoped DB updates during sync | Triage writes `p_internal_log_odds`; the trade loop writes shares/invested. A full-row upsert would clobber concurrent belief updates | Full `INSERT OR REPLACE` on every sync |

## 4. Data model

Migration `005_answer_trading.sql`:

```sql
ALTER TABLE tracked_markets ADD COLUMN outcome_type TEXT NOT NULL DEFAULT 'BINARY';
ALTER TABLE tracked_markets ADD COLUMN should_answers_sum_to_one INTEGER NOT NULL DEFAULT 0;
ALTER TABLE market_answers   ADD COLUMN invested_mana REAL NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS idx_answers_market ON market_answers(market_id);
```

`Db::apply_migrations` gains a guarded `table_has_column` check, same shape as
002–004.

Rules:

- `tracked_markets.invested_mana` stays the **single budget ledger**
  (`available_budget = budget + realized_pnl - invested`). For MCQ it equals
  the sum of answer `invested_mana`.
- `market_answers.invested_mana` is the cost basis of that answer's position.
- `tracked_markets.yes_shares/no_shares` remain a display aggregate for MCQ and
  are never used for decisions.
- New answers (an `addAnswersMode = ANYONE` market like the Israel one can grow)
  are inserted with `p_internal_log_odds = logit(price)` and
  `belief_updated_at = now`, mirroring onboarding.

New DB methods (all column-scoped):

```text
update_answer_market_price(market_id, answer_id, p_market)   -- never touches belief
update_answer_invested(market_id, answer_id, invested)
insert_answer_if_missing(&TrackedAnswer)                     -- does not replace
```

`upsert_answer` stays for onboarding only.

## 5. Read path: prices and positions

### 5.1 Prices

`GET /v0/market/{id}` already returns `answers[]` with `probability` for both
mechanisms (verified live). Extend `MarketSnapshot`:

```rust
struct AnswerSnapshot { id: String, text: String, probability: Option<f64> }
answers: Vec<AnswerSnapshot>,
outcome_type: String,
should_answers_sum_to_one: bool,
```

`GET /v0/market/{id}/prob` (`answerProbs`) is the fallback when an answer omits
`probability` or when `probability` is `0/1`. Clamp to
`[MIN_TRADING_PROB, MAX_TRADING_PROB]`. Do not cache prices locally beyond the
tick.

### 5.2 Positions

One request per market: `GET /v0/market/{id}/positions?userId={bot}`. Group
rows by `answerId`. Ignore rows with `answerId = null` on MCQ. Each row is a
`ContractMetric` with `totalShares.YES/NO`, `invested`, `profit`.

Add:

```rust
async fn answer_positions(&self, market_id: &str)
    -> AppResult<HashMap<String, PositionSnapshot>>;
```

Reuse `parse_answer_position` per row. Spike item: confirm the endpoint returns
**all** answer rows (it may default to top/bottom limits). If it caps, fetch the
missing ones with `&answerId=...` for locally held positions and warn.

### 5.3 Sync

`sync_market` branches on `market.outcome_type`:

- Binary: unchanged root reconcile.
- MCQ:
  1. Upsert any new answers (price-derived belief), update price and text on
     existing ones.
  2. Fetch answer positions; reconcile each answer's shares and invested.
  3. Recompute `market.invested_mana = Σ answer.invested`,
     `market.yes_shares/no_shares = Σ`.
  4. Warn on any drift, same message shape as `reconcile_position`.

**No market-level `p_market_last` update for MCQ.** It has no meaning and today
is only the misleading `0.5` fallback.

## 6. Decision model

Extract a pure planner so the whole tick is table-testable.

```rust
pub struct AnswerView {
    pub id: String,
    pub text: String,
    pub p_internal: f64,   // decayed belief for this answer
    pub p_market: f64,     // clamped answer probability
    pub yes_shares: f64,
    pub no_shares: f64,
}

pub fn plan_actions(
    market: &TrackedMarket,
    answers: &[AnswerView],
    sum_to_one: bool,
    available_budget: f64,
    params: TradeParams,
    limits: TradeLimits,
) -> Vec<TradeDecision>;
```

### 6.1 Internal probabilities

- **Independent (`shouldAnswersSumToOne = false`):**
  `q_i = p_internal_i`, `edge_i = q_i - p_market_i`. Edges need not sum to zero;
  that is correct for a set market where several answers can be true.
- **Sum-to-one:** normalize across **tracked** answers:
  `q_i = p_internal_i / Σ p_internal_j`, `edge_i = q_i - p_market_i`.
  Before normalizing, ensure `Σ p_internal > 0`; otherwise return no actions.
  Edges now sum to zero, so an impulse on one answer necessarily creates
  offsetting negative edges on rivals. This is the correct competitive
  semantics and it prevents the correlated-YES overbet.

Decayed belief uses the fresh answer price as the anchor:
`belief::current_p_internal(log_odds, belief_updated_at, p_market, now,
halflife)`.

### 6.2 Actions (exits first, then entries)

```text
exits:
  for each answer with shares:
    if yes_shares > 0 && edge < exit_threshold            -> SellYes (all)
    if no_shares  > 0 && !sum_to_one && edge > -exit_threshold -> SellNo (all)

entries:
  candidates = answers with no position and price in [MC_MIN_PRICE, MC_MAX_PRICE]
  order      = descending |edge| (strongest conviction first)
  remaining  = available_budget
  for c in candidates:
    action = edge > entry_threshold              -> BuyYes
           = !sum_to_one && edge < -entry_threshold -> BuyNo
           = none
    size   = min(kelly_target_mana(edge, price, remaining, params),
                 policy.max_answer_fraction * market.budget_mana)
    if size < min_trade_size: continue
    push action; remaining -= size
    stop after policy.max_new_per_tick
    stop if open positions reach policy.max_positions
```

Notes:

- Exits are all-or-nothing per side, matching the binary rule "one entry per
  lifecycle, no scale-in".
- Exit proceeds must not be counted as available budget for entries in the same
  tick. Conservative is correct here.
- `max_positions` counts answers currently holding shares. A market can still
  deploy its full budget across positions.
- Enforcement of the cap also happens in `execute_buy`, so a bug in the planner
  cannot violate the budget.

### 6.3 Trade limits

```rust
pub struct TradeLimits {
    pub max_positions: usize,       // MC_MAX_POSITIONS, default 6
    pub max_new_per_tick: usize,    // MC_MAX_NEW_PER_TICK, default 2
    pub max_answer_fraction: f64,   // MC_MAX_ANSWER_FRACTION, default 0.25
}
```

Binary markets keep `max_positions = 1`, `max_new_per_tick = 1`, so behaviour is
unchanged.

## 7. Execution and accounting

### 7.1 Shared accounting

Replace the duplicated market/answer math with position primitives:

```rust
struct PositionFields<'a> {
    yes: &'a mut f64,
    no: &'a mut f64,
    invested: &'a mut f64,
}

fn apply_buy_at(p: PositionFields, action: TradeAction, cost: f64, shares: f64) -> AppResult<()>;
fn apply_sell_at(p: PositionFields, action: TradeAction, sold: f64, proceeds: f64)
    -> AppResult<f64>; // returns the realized-PnL delta
```

`apply_buy` / `apply_sell` become thin wrappers that pass the market's fields and
add the realized delta to `market.realized_pnl_mana`. The answer path passes the
answer's fields but also adds the delta to `market.realized_pnl_mana`
(per-answer realized PnL is not stored; the aggregate is what the budget needs).

### 7.2 `evaluate_market`

```text
lock execution
market = db.get_market()                     // bail if missing / not Active
snapshot = client.market()                   // one request, includes answers
if resolved: return None
if binary: existing path, unchanged
else:
  answers = sync_answers(market, snapshot)   // prices + new answers
  positions = client.answer_positions()      // one request
  reconcile_answers(market, answers, positions)
  views = decayed views
  decisions = plan_actions(...)
  for d in decisions: execute(d)             // each writes its own ledger row
  persist market aggregate
```

`execute_buy` / `execute_sell` gain an answer parameter:

- Buy body already accepts `answerId`; keep `limitProb` logic.
- Sell requires `answerId` on MCQ (Manifold API contract).
- Update answer shares/invested **and** market aggregate in the same critical
  section. The DB is synchronous SQLite, so both updates happen under the
  executor lock; a failure between them is reconciled on the next tick.
- `TradeRecord.answer_id` is already stored; `TradeEvent::Trade` gains
  `answer_text: Option<String>` for the Discord embed.

### 7.3 Reconciliation invariants

For MCQ, after every sync:

```text
market.invested == Σ answers.invested          (within POSITION_EPSILON)
market.yes_shares == Σ answers.yes_shares       (display only)
market.no_shares  == Σ answers.no_shares        (display only)
```

Log at `warn!` on mismatch, and trust the per-answer remote rows. Never
reconcile the root contract on MCQ.

## 8. Liquidation and control plane

- `liquidate`: branch on outcome type. MCQ fetches answer positions and sells
  each nonzero side with its `answerId`, in the existing idempotent shape.
  `AlreadyResolved`, `NoPosition`, `Sold`, `AlreadyClosed` keep their meanings.
- `/untrack`: unchanged; it calls `liquidate`, which now covers answers.
- `/status market:<id>`: add the top answer positions (text, shares, price,
  internal probability / edge) for MCQ.
- `[TRADE]` embed: show the answer text and the answer edge.
- `/panic`: unchanged. It already blocks all buys.

## 9. Configuration

| Key | Default | Meaning |
| --- | --- | --- |
| `TRADING_DRY_RUN` | `false` | Log decisions and call `/v0/bet` with `dryRun: true`; persist nothing |
| `MC_MAX_POSITIONS` | `6` | Maximum answers holding shares per market |
| `MC_MAX_NEW_PER_TICK` | `2` | Maximum new entries in one evaluation |
| `MC_MAX_ANSWER_FRACTION` | `0.25` | Maximum share of `budget_mana` in one answer |

`MC_MIN_PRICE = 0.02`, `MC_MAX_PRICE = 0.98` are code constants next to
`MIN_TRADING_PROB`: never open a position at a near-certain price, and exit any
held answer that reaches the bounds.

`TRADING_DRY_RUN` is the one non-obvious feature. Manifold supports
`"dryRun": true` on `POST /v0/bet`, which returns a simulated fill. It lets the
new path run end to end against the real API with zero mana and zero position
changes, before the canary.

## 10. Phasing and gates

### Phase 0 — Probe the live API (no code, no mana)

Checklist:

1. `GET /v0/market/{id}` includes `probability` for every answer on both
   mechanisms (already seen for `hEpuZQpgR2` and `thlctZn5PU`).
2. `GET /v0/market/{id}/positions?userId=...` returns one row per answer with
   `answerId`, and does not cap at a default limit for 53 answers.
3. `POST /v0/bet` with `"dryRun": true` returns a filled `Bet` with
   `answerId`, and creates no position.
4. Whether a real single-answer YES bet on a `cpmm-multi-1` market creates
   synthetic NO legs on the bot's account. If yes, keep the YES-only design
   and exempt non-decided legs from exits.
5. Whether `/v0/market/{id}/prob` agrees with `answers[].probability`.

Gate: findings recorded in this plan; no code started until 1, 2, 3 and 5 pass.

### Phase 1 — Read-only answer sync (no trading)

- Migration, types, snapshot answers, `answer_positions`, MCQ `sync_market`,
  aggregate invariants, `/status` display.
- Fix the root-position wipe for MCQ.
- Tests: parsing, grouping, column-scoped updates, aggregate invariant.

Gate: 24 hours of ticks with answer prices tracking Manifold, zero rows
clobbered, `invested` still `0`, invariants clean in the log.

### Phase 2 — Decision layer, dry-run only

- `AnswerView`, `plan_actions`, normalization, caps, exits.
- `evaluate_market` MCQ branch; execution behind `TRADING_DRY_RUN`.
- Refactor `ManifoldClient` behind an `async_trait ManifoldApi` so the executor
  can be driven by a fake in tests (the crate already depends on `async-trait`).
- Log every planned action with `answer`, `edge`, `size`, `reason`.

Gate: at least 24 hours of dry-run decisions, and **at least one material
signal** during that window. If triage still produces none, fix the evidence
gate (Section 12) before continuing — otherwise the wiring is untested.

### Phase 3 — Canary

- `TRADING_DRY_RUN=false`, `MC_MAX_POSITIONS=1`, `MC_MAX_NEW_PER_TICK=1`,
  `MC_MAX_ANSWER_FRACTION=0.01` (1 mana on a 100-mana budget).
- Prefer a small 3–5 answer test market that the operator creates and `/track`s,
  then the Israel market.
- Verify: fill price, share count, `invested` accounting, exit on decay,
  `/untrack` liquidation, and no churn.

Gate: three full entry→exit cycles with ledger, DB, and Manifold positions
agreeing to within `POSITION_EPSILON`; no repeated sell/buy loops.

### Phase 4 — Full sizing

- Raise the caps to the Section 9 defaults; canary on MEGA (independent) next.
- Monitor for a week: trade count, win rate, realized PnL, fee drag, edge
  calibration (`p_internal - p_market` versus subsequent move).

Gate: positive realized PnL after fees, or a written post-mortem before raising
budgets.

### Phase 5 — Belief coherence (optional, same PR window if cheap)

- For sum-to-one markets, optionally project the normalized distribution back
  into stored log-odds once per tick so the signal ledger and `/status` show a
  coherent distribution.
- Decide whether `belief_kappa` needs per-mechanism tuning (a 0.05 impulse on a
  7-answer market moves more mass than on a 53-answer market).

## 11. Test plan

Pure units:

- Normalization: sum-to-one yields edges summing to zero; independent does not.
- Planner: exits before entries; all-or-nothing exits; no scale-in; entry order
  by `|edge|`; caps (`max_positions`, `max_new_per_tick`, per-answer fraction);
  budget never exceeded; NO only on independent markets; price-bound skips.
- Accounting: `apply_buy_at` / `apply_sell_at` for full and partial sells;
  basis proportional; realized delta; clamping; rejects wrong actions.
- Parsing: `MarketSnapshot.answers`, missing probability, `answerProbs`
  fallback, `answer_positions` grouping, root rows ignored on MCQ.
- `TradeExecutor` with a fake API: MCQ tick buys N answers, writes N ledger
  rows, updates aggregate; binary tick is unchanged.

Database:

- Migration applies once and is idempotent.
- `update_answer_market_price` does not change `p_internal_log_odds` or
  `belief_updated_at` (the concurrency guarantee).
- `insert_answer_if_missing` leaves an existing row untouched.
- Aggregate invariant after a simulated buy and sell.

Everything must keep the existing gates green:

```sh
cargo build --release
cargo test
cargo clippy -p manif0ld --all-targets --no-deps -- -D warnings
```

## 12. Triage calibration (independent but required for trades to happen)

Wiring alone can still produce zero trades. Current evidence:

- 31 signals, `p_evidence` mean `0.37`, max `0.66`, gate `0.7`.
- `strength` max `2.09` on the 0–3 rubric, so the strength gate is not binding.

Plan:

1. Query `signal_ledger` after a dry-run day: distribution of `p_evidence`,
   `p_target`, `p_priced`, and `material`.
2. Spot-check 20 neutral rows against the market question. If genuinely
   irrelevant chatter dominates, the gate is fine and volume is the issue.
   If relevant posts cluster at `0.6–0.69`, lower `TRIAGE_MIN_EVIDENCE` to the
   knee (likely `0.6`) and re-measure.
3. Only then tune `BELIEF_KAPPA`. Do not change the evidence question wording
   and the threshold in the same step; one variable at a time.

## 13. Risks

| Risk | Mitigation |
| --- | --- |
| Manifold API is alpha and MCQ fields change | Parse defensively; fixtures from live payloads; `answerProbs` fallback; fail the market, not the process |
| Positions endpoint caps rows | Phase 0 check; per-answer fallback query for held answers |
| Sum-to-one arb legs appear on the account | Phase 0 check; YES-only entries; never auto-exit legs the planner did not open; reconcile instead of fighting |
| Belief/price race between triage and trading | Column-scoped updates; executor lock; reconcile every tick |
| Budget wipe by root reconcile | Fixed in Phase 1 for MCQ; invariant test |
| Dry-run path diverges from the real path | The only difference is the `dryRun` flag and persistence; share everything else |
| `answer.probability = 1` on resolved answers | Clamp; skip near-certain prices; exit held positions at the bounds |

## 14. File map

| File | Change |
| --- | --- |
| `migrations/005_answer_trading.sql` | New columns and index |
| `src/db.rs` | Guarded migration, column-scoped answer updates, aggregate helpers |
| `src/types.rs` | `TrackedAnswer.invested_mana`, `TrackedMarket.outcome_type` / `should_answers_sum_to_one` |
| `src/onboarding.rs` | Persist outcome type and sum-to-one flag at `/track` |
| `src/trade.rs` | Snapshot answers, `answer_positions`, planner, MCQ branch, shared accounting, MCQ reconcile/liquidate, dry-run |
| `src/config.rs` | Four new keys |
| `src/discord.rs` | Answer text in trade embeds and `/status` |
| `src/main.rs` | No new task; `TradeExecutor` unchanged in shape |
| `docs/SPEC.md` | §4 API contracts, §6.6 trading rules, §2 pipeline |
| `docs/OVERVIEW.tex` | Trading box if the diagram shows binary-only flow |

## 15. Definition of done

- A material signal on an MCQ market produces one Manifold bet with the right
  `answerId`, a ledger row, and matching local state.
- The 60-second tick reconciles answers without touching beliefs or the budget
  by accident.
- Sum-to-one markets cannot open two mutually exclusive YES legs from the same
  impulse.
- Exits and `/untrack` liquidate every answer side.
- Dry-run and canary evidence is in the logs before real sizing.
- `cargo build --release`, `cargo test`, and `cargo clippy -D warnings` are
  clean.
