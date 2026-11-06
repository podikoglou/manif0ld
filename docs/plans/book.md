# Plan: one position book per market (`Book`)

Status: implemented (`6be938d`, `4401515`, `8b823de`). §6 items 9-11 were found
during implementation and are recorded there.

## 1. Problem

A tracked market owns either one binary position (the `tracked_markets` columns) or
one position per `market_answers` row. The code models those two shapes as two
parallel implementations in `src/trade.rs`, and the second one is maintained by hand
on top of the first.

Four defects follow. All line numbers are `trade.rs` at `e844f08`.

1. **Root aggregates are hand-maintained in five places.** For a multiple-choice
   market, `tracked_markets.invested_mana`, `yes_shares`, and `no_shares` are the sum
   of the answer rows. Five sites keep that sum:
   - `execute_buy` (`2128`), `Some(answer)` branch (`2213-2217`)
   - `execute_sell` (`2251`), `Some(answer)` branch (`2311-2318`)
   - `liquidate_answers` (`1911`), inline settlement (`1977-1984`) — a verbatim copy
     of the `execute_sell` block
   - `reconcile_answers` (`1669`), recompute from sums (`1729-1731`)
   - `apply_buy` / `apply_sell` (`1121`, `1146`) for the binary root

   `available_budget = budget + realized_pnl − invested` reads that aggregate, so a
   missed update silently changes position sizing and the pre-buy budget check
   (`execute_buy`, `2147-2163`).

2. **The `Some` branch bypasses the shared arithmetic.** `apply_buy` (`1121`) and
   `settle_sell` (`1080`) are the one accounting implementation for a position, but
   `execute_buy`/`execute_sell` only call them when `answer` is `None`. The
   `Some` branch re-implements the same additions and clamps by hand. A fix to
   `settle_sell` never reaches the multiple-choice path, and the case analysis
   `Option<&mut TrackedAnswer>` exists only because `Book` does not.

3. **Three public methods fork on `is_multiple_choice()` plus three helpers.**
   `evaluate_market` (`1339`), `sync_market` (`1754`), and `liquidate` (`1834`) each
   fetch the market, refresh its kind, then branch to `evaluate_answers` (`1428`),
   the inline MC sync, or `liquidate_answers` (`1911`). The shared prologue
   (load → check status → fetch → refresh kind) is repeated three times. Binary
   additionally uses `ManifoldClient::position()` (`198`) while multiple choice uses
   `positions()` (`212`), and both parse the same `/positions` response through
   different code (`parse_position` `502`, `parse_market_positions` `610`).

4. **Dead and duplicated policy.** `decide_trade` (`739`) and
   `decide_trade_for_answer` (`795`) are byte-for-byte identical except for where
   `available_budget` comes from; the second has no caller and no test. `plan_actions`
   (`938`) is a third, different implementation of the same policy. The three
   binary/multiple-choice differences are real but undocumented in code: binary has
   no entry price band, no per-answer fraction cap, and no sum-to-one projection.
   They live in control flow instead of data, which is how they drifted.

The deletion test: delete `Book` and the complexity does not vanish — it reconstitutes
as an `Option` match in every executor helper, five aggregate updates, and three
branches. The complexity belongs behind one interface.

## 2. Decisions taken

1. **One `Book` per market, indexed by legs.** A leg is an owned position with an
   optional `answer_id`. Binary is exactly one leg with `answer_id: None`; multiple
   choice is one leg per answer row. The root aggregate is always *derived* from the
   legs, never incremented by a caller.
2. **One control flow for both shapes.** `evaluate_market`, `sync_market`, and
   `liquidate` each have a single body. They ask the book to refresh, reconcile, and
   persist; the shape knowledge lives inside `Book`.
3. **One planner over legs.** `decide_trade`, `decide_trade_for_answer`, and
   `plan_actions` collapse into `Book::plan`. The three legitimate binary/multiple-
   choice differences become fields on `TradeLimits` (`entry_price_band`,
   `max_answer_fraction`, plus the existing caps), so `SPEC.md` §6.6 behaviour is
   preserved as data and any future difference is visible in one struct.
4. **New module `src/book.rs`.** It holds `Book`, `Leg`, `Shape`, `Settlement`,
   `Position`, `MarketPositions`, and the trading policy (`TradeParams`,
   `TradeLimits`, `TradePolicy`, `kelly_target_mana`, `PlannedTrade`). `trade.rs`
   keeps `ManifoldClient` and its wire types, `TradeExecutor`, `TradeEvent`, and
   `LiquidationOutcome`. Dependency edge is one-way: `trade → book`. `book` depends
   on `types`, `db`, `market_store`, `belief`, `config`, `error`.

Out of scope: the four `#[ignore]` live-network tests keep testing the same paths
through the unchanged public executor API. `discord.rs`, `main.rs`, `pipeline.rs`,
`db.rs`, and `market_store.rs` are not touched. Answer *belief* writes stay with
triage and `/override`; the book never writes them.

## 3. The model

### Shapes

```
Binary:   legs = [ Leg { answer_id: None, .. } ]        aggregate = legs[0]
Answers:  legs = [ Leg { answer_id: Some(id), .. } ]    aggregate = sum(legs)
```

Root columns `invested_mana`, `yes_shares`, `no_shares` are always the aggregate.
`realized_pnl_mana` has no per-answer column, so it stays on the market row and on
`Book` as a single value; a sell on any leg adds to it.

### Column ownership inside the book

`Book` keeps the loaded `TrackedMarket` for identity, lifecycle, `budget_mana`,
`realized_pnl_mana`, and the kind flags. Its **position and price columns are inputs
only**: after `load`, nothing reads `market.yes_shares`, `market.no_shares`,
`market.invested_mana`, or `market.p_market_last` again; legs are the truth and
`persist` is the only writer of those columns. This rule goes in the module docs;
it is what stops a second source of truth from reappearing inside the book.

| Fact | Owned by | Written by |
|---|---|---|
| `yes_shares`, `no_shares`, `invested_mana` per leg | `Leg` | `Book::apply_buy`, `Book::apply_sell`, `Book::reconcile` |
| root `invested_mana`, `yes_shares`, `no_shares` | `Book::aggregate` | `MarketStore::record_position` via `Book::persist` |
| `realized_pnl_mana` | `Book.market` | `Book::apply_sell` |
| `p_market_last` | `Leg` (binary) / `Leg` per answer (answers shape) | `Book::observe_prices` |
| answer rows' positions | `Leg` | `Book::persist` via `Db::update_answer_position` |
| answer rows' belief and text ownership | triage / `observe_prices` text only | unchanged from today |

Write order in `persist`, preserved from today: answer positions first, then the root
aggregate. `store.record_price` follows, with the binary leg's price.

## 4. Interface

### `src/book.rs`

```rust
/// What Manifold says about one answer's price and label.
pub struct AnswerObservation<'a> {
    pub id: &'a str,
    pub text: &'a str,
    pub probability: Option<f64>,
}

/// One owned position inside a market.
pub struct Leg {
    pub answer_id: Option<String>,  // None for the single binary leg
    pub text: String,               // empty for binary
    pub yes_shares: f64,
    pub no_shares: f64,
    pub invested_mana: f64,
    pub p_internal_log_odds: f64,
    pub p_market_last: f64,
    pub belief_updated_at: i64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Shape { Binary, Answers }

/// The root position, derived from the legs.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Position {
    pub yes_shares: f64,
    pub no_shares: f64,
    pub invested: f64,
    pub profit: f64,
}

/// Every position the bot holds in one market, split by answer.
pub struct MarketPositions {
    pub root: Position,
    pub answers: HashMap<String, Position>,
    pub saw_root: bool,
}
impl MarketPositions {
    pub fn answer(&self, answer_id: &str) -> Position;
    pub fn totals(&self) -> Position;
    pub fn checksum_ok(&self) -> bool;
}

/// What one sell did to a leg.
pub struct Settlement { pub sold: f64, pub basis: f64, pub realized: f64 }

/// A decayed view of one leg, the planner's input.
pub struct LegView {
    pub p_internal: f64,
    pub p_market: f64,        // clamped to [MIN_TRADING_PROB, MAX_TRADING_PROB]
    pub yes_shares: f64,
    pub no_shares: f64,
}

/// One planned action plus the numbers the ledger needs.
pub struct PlannedTrade {
    pub leg: usize,
    pub decision: TradeDecision,   // answer_id is filled from the leg
    pub p_internal: f64,
    pub p_market: f64,
}

pub struct Book {
    market: TrackedMarket,
    legs: Vec<Leg>,
    shape: Shape,
}

impl Book {
    /// Loads the row and, for a multiple-choice market, its answer rows.
    pub fn load(store: &MarketStore, db: &Db, market_id: &str) -> AppResult<Option<Self>>;

    pub fn id(&self) -> &str;
    pub fn status(&self) -> MarketStatus;
    pub fn shape(&self) -> Shape;
    pub fn legs(&self) -> &[Leg];
    pub fn leg(&self, index: usize) -> &Leg;
    pub fn available_budget(&self) -> f64;

    /// The row with the aggregate position columns applied. Used by `sync_market`
    /// and by logging; never a second writer.
    pub fn market(&self) -> TrackedMarket;

    /// Writes `outcome_type` and `should_answers_sum_to_one` when they differ.
    /// Rebuilds the legs when the shape changes, so a market that Manifold
    /// re-kinds mid-life gets the right leg set in the same call.
    pub fn refresh_kind(
        &mut self,
        store: &MarketStore,
        db: &Db,
        outcome_type: &str,
        sum_to_one: bool,
    ) -> AppResult<()>;

    /// Applies fresh prices. Binary: sets the single leg's price from
    /// `root_probability`. Answers: inserts missing answers anchored at their
    /// price (believing the price, `belief_updated_at = now`), updates changed
    /// text, and refreshes prices only. Belief columns are never overwritten.
    pub fn observe_prices(
        &mut self,
        db: &Db,
        now: i64,
        root_probability: Option<f64>,
        answers: &[AnswerObservation<'_>],
    ) -> AppResult<()>;

    /// Replaces leg positions from Manifold, warns on a mismatch, and (for the
    /// answers shape) checks the root summary against the answer totals.
    /// Pure: no IO.
    pub fn reconcile(&mut self, positions: &MarketPositions);

    /// Answer rows first, then the root aggregate and price.
    pub fn persist(&self, store: &MarketStore, db: &Db) -> AppResult<()>;

    /// Records a filled buy on one leg. Rejects sell actions.
    pub fn apply_buy(&mut self, leg: usize, action: TradeAction, cost: f64, shares: f64)
        -> AppResult<()>;

    /// Records a filled sell on one leg, adds the realized part to the market's
    /// PnL, and returns the settlement. Rejects buy actions and empty sides.
    pub fn apply_sell(&mut self, leg: usize, action: TradeAction, shares: f64, proceeds: f64)
        -> AppResult<Settlement>;

    /// The sides a liquidation must sell: every leg's held YES and NO shares,
    /// empty when the market is resolved.
    pub fn sell_sides(&self, resolved: bool) -> Vec<(usize, TradeAction, f64)>;

    /// Decayed internal probability and clamped price for one leg.
    pub fn view(&self, leg: usize, now: i64, halflife_hours: f64) -> LegView;

    /// One decision cycle: exits first, then ranked entries, capped by the
    /// limits that belong to this shape. Returns every action to execute.
    pub fn plan(&self, now: i64, halflife_hours: f64, policy: &TradePolicy) -> Vec<PlannedTrade>;
}
```

### Policy types (moved from `trade.rs`, two fields added)

```rust
pub struct TradeParams { kelly_fraction, entry_edge_threshold, exit_edge_threshold, min_trade_size }
impl TradeParams { pub fn from_config(&Config) -> Self }

pub struct TradeLimits {
    pub max_positions: usize,
    pub max_new_per_tick: usize,
    /// Cap on one leg's entry as a fraction of `budget_mana`.
    /// `None` means no cap (binary).
    pub max_answer_fraction: Option<f64>,
    /// Entry price band, inclusive. `None` means no gate (binary).
    pub entry_price_band: Option<(f64, f64)>,
}
impl TradeLimits {
    /// From config: the multiple-choice limits.
    pub fn multiple_choice(config: &Config) -> Self;  // band Some((MC_MIN_PRICE, MC_MAX_PRICE))
    /// One position, one entry, no fraction cap, no price band.
    pub fn binary() -> Self;
}

pub struct TradePolicy { pub params: TradeParams, pub multiple_choice: TradeLimits }
impl TradePolicy { pub fn from_config(config: &Config) -> Self }
```

`Book::plan` picks `TradeLimits::binary()` or `policy.multiple_choice` from
`self.shape`. That single line is the only remaining shape conditional in the
planner, and it returns data.

`kelly_target_mana` keeps its signature and doc comment. `MAX_KELLY_FRACTION`,
`MIN_TRADING_PROB`, `MAX_TRADING_PROB`, `MC_MIN_PRICE`, `MC_MAX_PRICE`,
`BUDGET_EPSILON`, and `POSITION_EPSILON` move to `book.rs`;
`YES_LIMIT_PROB`, `NO_LIMIT_PROB`, `REQUEST_TIMEOUT`, `ERROR_SNIPPET_CHARS`,
`SYNC_INTERVAL`, `EVENT_CAPACITY` stay in `trade.rs`.

## 5. Code moves

Move into `src/book.rs`:

- `TradeParams` (`669`), `kelly_target_mana` (`701`), `AnswerView` (`857`, becomes
  `LegView`), `TradeLimits` (`870`), `answer_edges` (`906`), `plan_actions` (`938`,
  becomes `Book::plan`), `PositionFields` (`1042`), `apply_buy_at` (`1049`),
  `SellSettlement` (`1066`, becomes `Settlement`), `settle_sell` (`1080`),
  `apply_buy` (`1121`) and `apply_sell` (`1146`, become `Book` methods),
  `liquidation_sides` (`1221`, becomes `Book::sell_sides`),
  `PositionSnapshot` (`413`, becomes `Position`), `MarketPositions` (`566`).
- From the executor: `evaluate_answers` (`1428`), `refresh_market_kind` (`1569`,
  becomes `Book::refresh_kind`), `sync_answers` (`1603`), `reconcile_answers`
  (`1669`), `liquidate_answers` (`1911`), `reconcile_position` (`2416`), and the
  body of `persist_market` (`2376`) — all absorbed by `Book`.

Delete:

- `decide_trade` (`739`), `decide_trade_for_answer` (`795`) — replaced by `Book::plan`;
  the second has no caller and no test today.
- `parse_position` (`502`) and `parse_answer_position` (`507`) — `position()` becomes
  `Ok(self.positions(id).await?.root)` and `parse_market_positions` is the single
  parser. Their four parse tests move their assertions onto
  `parse_market_positions(..).root`.

Keep, unchanged in shape:

- `ManifoldClient` with `market`, `position` (now a two-line delegate), `positions`,
  `buy`, `sell`, `bets`.
- `TradeExecutor`'s public API: `new`, `events`, `client`, `evaluate_market`,
  `sync_market`, `sync_all`, `liquidate`, `liquidate_all`, `run`. `discord.rs` and
  `main.rs` compile untouched.

## 6. Behaviour ledger

Every difference between today and after, with the reason. Anything not listed must
behave identically; the tests in §8 are the enforcement.

| # | Change | Why | Verdict |
|---|---|---|---|
| 1 | Binary fetches `positions()` and uses `.root`; `position()` delegates to it | one parser, same URL | same numbers; `position()` kept for the live tests |
| 2 | Binary entry has no price band and no fraction cap | `TradeLimits::binary()` sets both to `None` | deliberately identical; test 12 pins it |
| 3 | A binary market holding **both** YES and NO shares sells both in one evaluation | one planner over legs | new; unreachable through the bot's own path (no scale-in), reachable if the operator traded manually. Test 14 documents it |
| 4 | Multiple-choice liquidation refreshes answer prices and text, and inserts answers it did not know | `liquidate` shares `prepare` with `evaluate` | a new answer has no position, so nothing is sold; a sell now records the freshly observed `p_market` instead of the stored one |
| 5 | An existing answer whose remote `probability` is absent is forced to `0.5` | today's `sync_answers` does exactly this (`1612-1628`) | preserved verbatim; recorded as residual §10 |
| 6 | `sync_market` returns a row whose position columns equal the aggregate | `Book::market()` applies it | same values as today's mutated struct |
| 7 | Root `p_market_last` still is not refreshed for a multiple-choice market | today only the binary path writes it | preserved; see residual §10 |
| 8 | Reconciliation `warn!` lines keep their fields and wording; the other trade messages were unified | operators grep the reconcile lines | the kill-switch warn now always carries `action`; the resolved-liquidation and no-shares messages use the binary wording for both shapes; the dry-run plan message is `"planned trade"` |
| 9 | The ledger row is written before the position rows | `execute` records the trade, the caller then calls `Book::persist` | only observable if a write fails mid-trade. Neither order is atomic, and the 60 s reconcile repairs the position either way |
| 10 | A dry-run binary market logs one extra `"planned trade"` line | the planned-action log moved out of the multiple-choice path (§7) | log volume only |
| 11 | `Book::load` reads answer rows before the `Active`/`Closed` guard | the book is loaded whole | a DB read error on answers surfaces where the old code returned `Ok(None)` / `AlreadyClosed` |

## 7. Call-site migration

| Site | Today | After |
|---|---|---|
| `trade.rs:1312` `evaluate_market` | load, status, kill switch, `market()`, resolved guard, kind, **fork** | load book, status, kill switch, `market()`, resolved guard, `prepare`, `plan`, execute loop |
| `trade.rs:1428` `evaluate_answers` | separate body | deleted |
| `trade.rs:1569` `refresh_market_kind` | free-standing | `book.refresh_kind(&store, &db, ..)` |
| `trade.rs:1603` `sync_answers` | free-standing MC-only | `book.observe_prices(&db, now, prob, &observations)` |
| `trade.rs:1669` `reconcile_answers` | free-standing MC-only | `book.reconcile(&positions)` |
| `trade.rs:1740` `sync_market` | **fork** on shape | load book, `market()`, `prepare`, `book.market()` |
| `trade.rs:1817` `liquidate` | **fork** on shape | load book, closed guard, `market()`, `prepare`, resolved guard, loop over `sell_sides` |
| `trade.rs:1911` `liquidate_answers` | separate body | deleted |
| `trade.rs:2128` `execute_buy` | `Option<&mut TrackedAnswer>` | `book.apply_buy(planned.leg, ..)`; no aggregate updates |
| `trade.rs:2251` `execute_sell` | `Option<&mut TrackedAnswer>` | `book.apply_sell(planned.leg, ..)`; no aggregate updates |
| `trade.rs:2376` `persist_market` | `record_position` + `record_price` | `book.persist(&store, &db)` |
| `trade.rs:2416` `reconcile_position` | free-standing binary-only | `Book::reconcile` |
| `trade.rs:2051` `emit_trade` | `available_budget(market)` | `book.available_budget()`; `TradeEvent` unchanged |

New executor internals (private):

```rust
fn load_book(&self, market_id: &str) -> AppResult<Book>;            // bail "not tracked"
async fn prepare(&self, book: &mut Book, snapshot: &MarketSnapshot, now: i64)
    -> AppResult<()>;                                               // kind, positions, prices, reconcile, persist
async fn execute(&self, book: &mut Book, planned: &PlannedTrade, trigger: Option<&str>,
    now: i64, dry_run: bool) -> AppResult<Option<TradeRecord>>;     // buy or sell by action, ledger row
```

`execute` merges today's `execute_buy` and `execute_sell`: it keeps the budget
invariant checks (buy only), the `dry_run` branches, `ensure_filled`, the
`insert_trade` call, and the `TradeRecord` fields. It writes no position columns
directly; the caller calls `book.persist` afterwards.

`evaluate_market` after the change, in full:

```rust
let _guard = self.execution_lock.lock().await;
let mut book = self.load_book(market_id)?;
if book.status() != MarketStatus::Active { return Ok(None); }
let kill_switch = self.kill_switch_on()?;
let snapshot = self.client.market(market_id).await?;
if snapshot.is_resolved { info; return Ok(None); }
let now = Utc::now().timestamp();
self.prepare(&mut book, &snapshot, now).await?;
let planned = book.plan(now, self.halflife_hours, &self.policy);
let mut first = None;
for planned_trade in &planned {
    if is_buy(planned_trade.decision.action) && kill_switch { warn; continue; }
    if self.dry_run { log the planned action with book.leg(planned_trade.leg).text; }
    let Some(record) = self.execute(&mut book, planned_trade, trigger_post_uri, now, self.dry_run).await? else { continue };
    book.persist(&self.store, &self.db)?;
    self.emit_trade(&book, &record);
    first.get_or_insert(record);
}
Ok(first)
```

The dry-run logging for a planned trade moves up, before execution, so a dry-run
multiple-choice market still logs every planned action (`1519-1517` today).

## 8. Tests

New tests in `src/book.rs`, using a `sample_market` fixture and `Db::open_in_memory()`
where IO is involved:

1. `load_binary_market_has_one_leg_without_an_answer_id`
2. `load_multiple_choice_market_has_one_leg_per_answer_row`
3. `a_buy_on_an_answer_leg_updates_the_root_aggregate` — the defect: after
   `apply_buy(leg, ..)`, `aggregate().invested == sum(legs.invested)` and `persist`
   writes that sum to `tracked_markets`.
4. `a_sell_on_an_answer_leg_updates_realized_pnl_and_the_root_aggregate`
5. `a_partial_sell_on_an_answer_leg_releases_a_proportional_cost_basis`
6. `a_sell_on_a_flat_leg_is_rejected`
7. `apply_buy_rejects_sell_actions` (moved from `trade.rs:2852`)
8. `reconcile_trusts_the_answer_rows_when_the_root_checksum_disagrees`
9. `reconcile_clamps_negative_remote_positions_to_zero`
10. `persist_writes_answer_rows_then_the_root_aggregate` — asserts the DB after a
    buy on an answer leg: answer row moved, root row equals the sum.
11. `persist_does_not_insert_a_soft_deleted_market` — an intent on an untracked row
    updates the row and leaves the cache empty (mirrors market-store test 9).
12. `a_binary_market_ignores_the_entry_price_band_and_the_answer_fraction_cap` —
    the two `TradeLimits::binary()` deltas, with a budget/PnL pair where a cap would
    have bound.
13. `plan_reports_the_leg_index_and_the_decayed_probabilities` — the ledger inputs.
14. `plan_sells_both_sides_of_a_binary_market_in_one_pass` — behaviour entry 3 of §6.

Moved from `trade.rs`, adapted to `Book::plan` and a `Book` fixture; assertions stay:

- Planner: `sum_to_one_edges_are_normalized_and_block_the_offsetting_side`,
  `independent_edges_allow_both_sides`, `normalization_removes_a_duplicate_yes_entry`,
  `exits_are_planned_before_entries`, `new_entries_are_capped_per_tick`,
  `open_positions_consume_the_slots`,
  `one_answer_cannot_take_more_than_its_share_of_the_budget`,
  `near_certain_answers_are_not_entered`, `zero_internal_mass_produces_no_edges`,
  `entries_below_the_minimum_trade_size_are_skipped`,
  `entries_use_only_the_available_budget`.
- Binary decisions through the same planner: `flat_market_with_a_bullish_edge_buys_yes`,
  `flat_market_with_a_bearish_edge_buys_no`, `edge_inside_the_entry_band_does_not_trade`,
  `holding_yes_exits_when_the_edge_drops_below_the_exit_threshold`,
  `holding_no_exits_when_the_edge_rises_above_the_exit_threshold`,
  `holding_yes_does_not_exit_inside_the_band`,
  `holding_no_does_not_exit_or_add_inside_the_band`,
  `decision_never_sells_a_zero_share_position`.
- Accounting: `apply_buy_adds_cost_and_shares_to_the_matching_side`,
  `profitable_sell_increases_realized_pnl_and_available_budget`,
  `losing_sell_reduces_available_budget`, `sell_of_more_shares_than_held_is_clamped`,
  `apply_sell_rejects_buy_actions_and_empty_positions`,
  `liquidation_sells_only_the_sides_that_hold_shares`,
  `liquidation_of_a_resolved_or_empty_position_sells_nothing`.
- Kelly and parsers stay where their functions live: `kelly_*` move to `book.rs`;
  `market_snapshot_*`, `bet_result_*`, `position_parser_*`, `market_positions_*` stay
  in `trade.rs` and now assert on `book::Position` / `book::MarketPositions`.

New invariant test: `available_budget_matches_the_root_market_row` — for both shapes,
`book.available_budget()` equals `types::available_budget(&book.market())`.

No new executor tests. `TradeExecutor` still needs a live Manifold key, which is why
the arithmetic moved out of it: everything in §8 needs neither network nor a Discord
mock.

## 9. Commit sequence

1. `feat(book): add the position book for a market`
   New `src/book.rs` with the types, the accounting, the planner, and the tests;
   `mod book;` in `main.rs`. `trade.rs` is untouched, so the old code keeps working
   while the new module compiles dead (`#![allow(dead_code)]` already covers it).
   The moved `Position`/`MarketPositions`/policy types are *copied*, not yet moved,
   so this commit stands alone.
2. `refactor(trade): route evaluation, sync, and liquidation through Book`
   The single switch: add `load_book`/`prepare`/`execute`, rewrite the three public
   methods, delete the six shape-specific bodies and the three deciders, make
   `position()` delegate to `positions()`, delete `parse_position`/
   `parse_answer_position`, point `trade.rs` at the `book` types, and delete the
   duplicated definitions. One commit because a partial switch leaves two
   implementations of the aggregate live at once — the same reason the market-store
   switch was one commit.
3. `docs: record the trading book`
   `docs/ARCHITECTURE.md`: §2 dependency graph (add `book`, note the one-way edge),
   §3 (legs own positions, the aggregate has one writer), §4 (add the `book.rs`
   interface, update `trade.rs` to the reduced surface), §5.1/§5.3 (one control flow),
   §7 E and I marked resolved with the residual noted. `docs/SPEC.md` §6.6: one
   sentence that both market kinds share one planner and that the binary/multiple-
   choice differences are `TradeLimits` data, not separate paths.

Run the gates after every commit:

```sh
cargo build --release
cargo test
cargo clippy -p manif0ld --all-targets --no-deps -- -D warnings
```

`cargo fmt --check` is already dirty at HEAD for `triage.rs`, `config.rs`, and
`belief.rs`. Format only `src/book.rs` and `src/trade.rs` (`rustfmt` on those two
files) and do not reformat unrelated files.

## 10. Risks and residuals

- **Large commit 2.** It is the smallest correct switch. Review order: `evaluate_market`
  and `prepare` first, then `execute`, then `liquidate`, then the deletions.
- **Planner unification is the riskiest part.** The three binary/multiple-choice
  differences are pinned by tests 12 and 14 and by the moved decision tests. If any
  moved test needs an assertion changed rather than a call shape changed, stop: that
  is a behaviour change that needs a SPEC decision, not a refactor.
- **`liquidate` now discovers new answers** (behaviour entry 4). Accept it or pass a
  flag; the plan recommends accepting and testing it, because an answer row with no
  position cannot be sold.
- **An existing answer with no remote price is forced to 0.5** (today's behaviour,
  `sync_answers`). Preserved on purpose; fixing it changes belief anchors and must be
  a separate decision.
- **Multiple-choice root `p_market_last` stays stale.** Only the binary path writes
  it. `/status` shows it as `P_mkt` for every market, so a multiple-choice market
  displays a price nothing maintains. Fixing that is a display decision (what is
  "the" price of a sum-to-one market?) and is out of scope.
- **The global `execution_lock` stays.** ARCHITECTURE §7E suggests one lock per
  market. That changes concurrency and failure behaviour, so it belongs in its own
  plan, not this refactor.
- **`TradeEvent` shape is untouched.** Candidate 5 owns the presentation fields; this
  plan only stops the executor from needing a whole `TrackedMarket` to build an event.
- **Answer-level budget races are unchanged.** A sell on an answer leg updates the
  leg and then the root aggregate in one synchronous block, as today.
- **`Position.profit` is always zero in `Book::aggregate`, and
  `ManifoldClient::position()` now returns the root of `positions()`.** Nothing
  reads `profit` today, and the book tracks cost basis rather than Manifold's
  realized-plus-unrealized profit. A future caller that needs remote profit must
  read `positions().root.profit` directly.
- **Live tests cannot run here.** `live_dry_run_places_a_simulated_answer_bet` and
  `live_sync_reads_every_answer_of_both_mechanisms` call `sync_market`/`evaluate_market`
  with the new internals but keep the same public API, so they need no edit; they
  still require a real key and stay `#[ignore]`d.

## 11. Acceptance

- `rg "is_multiple_choice" src/trade.rs` returns nothing; the only occurrences in
  `src/` are `types.rs` (the predicate), `book.rs` (`Book::load`), and `discord.rs`
  (display).
- `rg "Option<&mut TrackedAnswer>" src/` returns nothing.
- `rg "decide_trade_for_answer|evaluate_answers|liquidate_answers|reconcile_answers|sync_answers|apply_buy_at|PositionFields|parse_answer_position" src/`
  returns nothing.
- Every write to a position column goes through `Book::persist`
  (`rg "record_position|update_answer_position" src/` → `market_store.rs`, `db.rs`,
  `book.rs` only).
- `src/trade.rs` contains no position arithmetic: no `yes_shares +=`, no
  `invested_mana -=`.
- `src/book.rs` has no `use crate::trade` line.
- Tests 3, 10, 12, and 14 pass, and the 26 moved tests keep their original
  assertions.
- The three gates are clean, and `rustfmt --check src/book.rs src/trade.rs` passes.
