# Plan: one writer for market state (`MarketStore`)

Status: ready to implement.

## 1. Problem

Market state lives in two places: the `tracked_markets` row and a whole-struct
copy in `pipeline::MarketCache`. Five modules write it, and each one reads a
whole `TrackedMarket`, changes one field group, then writes the whole struct
back.

The trade path shows the defect. `TradeExecutor::evaluate_market` reads the row
(`trade.rs:1318`), then awaits `GET /v0/market` (`trade.rs:1327`) and
`GET /v0/market/{id}/positions` (`trade.rs:1348`). Then
`persist_market` (`trade.rs:2374-2390`) writes `invested_mana`,
`realized_pnl_mana`, shares **and** `p_internal_log_odds`, `p_market_last`,
`belief_updated_at` from the snapshot it read before the HTTP calls. It then
calls `cache.upsert(market.clone())` with the same snapshot.

Two defects follow:

1. **A newer belief is lost.** If the pipeline worker applies an impulse during
   the trade check, the trade writes the old belief back to SQLite and to the
   cache. This contradicts `docs/SPEC.md` §6.6: "Belief columns belong to triage
   and are never overwritten by a sync."
2. **A pause is lost.** If `/pause` runs during the trade check, the cache
   returns to `ACTIVE` while the database says `PAUSED`. `Pipeline::dispatch`
   reads the cache (`pipeline.rs:362`), so the paused market keeps matching
   posts until the next reload.

The deletion test is clear. Delete `MarketCache::upsert` and the read-modify-write
does not vanish; it reappears in five callers. The complexity belongs behind one
interface.

## 2. Decisions taken

1. **Structural enforcement.** Market-row SQL moves out of `db.rs` into
   `src/market_store.rs`. `Db` keeps ledgers, answers, and key-value rows. No
   module outside the store can write a market column, because the methods no
   longer exist.
2. **Hot reads only.** The store serves `cached`, `active`, and `active_count`
   from its map. Lists and row truth come from the database through the store,
   because the cache drops closed rows and `Db::list_markets` includes them.
3. **Split belief and price.** `record_belief(log_odds, updated_at)` and
   `record_price(p_market)` are separate intents. The trade path becomes the
   only price writer. The pipeline and `/override` stop writing the price.

Out of scope: answer rows (`market_answers`). Their writes are already
intent-shaped (`update_answer_belief`, `update_answer_position`,
`update_answer_market_price`), so they have no whole-struct defect. Candidate #2
owns them. Lifecycle composition with `EngineRegistry` and Discord threads stays
for the control-plane candidate; this plan only fixes the state primitive.

## 3. Column ownership

Each column group gets exactly one writer. This table is the reason for the
interface.

| Column group | Sole writer |
|---|---|
| `p_internal_log_odds`, `belief_updated_at` | pipeline triage, `/override` |
| `p_market_last` | trade sync (60 s tick and material signal) |
| `invested_mana`, `realized_pnl_mana`, `yes_shares`, `no_shares` | trade |
| `status`, `deleted_at` | lifecycle transitions |
| `budget_mana` | `/budget` |
| `outcome_type`, `should_answers_sum_to_one` | trade sync |
| `discord_thread_id`, `sieve_source` | onboarding insert |

`p_market_last` is not part of belief. The decay function
(`belief::decayed_log_odds`) takes the current price as a parameter at read
time, so a newer price with an older `belief_updated_at` is the intended model.
The trade sync already refreshes the price every 60 seconds, so this split does
not change decay behaviour; it only stops the stale write.

## 4. Interface

New file `src/market_store.rs`.

```rust
pub struct MarketStore {
    db: Db,
    markets: RwLock<HashMap<MarketId, TrackedMarket>>,
}

impl MarketStore {
    /// Loads the live snapshot. Same rows as `Db::list_markets`.
    pub fn new(db: &Db) -> AppResult<Self>;

    // Cached reads: hot path only.
    pub fn cached(&self, id: &str) -> Option<TrackedMarket>;
    pub fn active(&self) -> Vec<TrackedMarket>;      // sorted by id
    pub fn active_count(&self) -> usize;

    // Durable reads: row truth, includes closed-but-visible rows.
    pub fn load(&self, id: &str) -> AppResult<Option<TrackedMarket>>;
    pub fn list(&self) -> AppResult<Vec<TrackedMarket>>;
    pub fn list_including_deleted(&self) -> AppResult<Vec<TrackedMarket>>;

    // Lifecycle intents: change cache membership too.
    pub fn insert(&self, market: &TrackedMarket) -> AppResult<()>;
    pub fn close(&self, id: &str) -> AppResult<()>;            // CLOSED, cache remove
    pub fn untrack(&self, id: &str, at: i64) -> AppResult<()>; // CLOSED + deleted_at, cache remove
    pub fn delete(&self, id: &str) -> AppResult<()>;           // hard delete, cache remove

    // Column-group intents: patch the cache field only.
    pub fn record_status(&self, id: &str, status: MarketStatus) -> AppResult<()>;
    pub fn record_budget(&self, id: &str, budget_mana: f64) -> AppResult<()>;
    pub fn record_belief(&self, id: &str, log_odds: f64, updated_at: i64) -> AppResult<()>;
    pub fn record_price(&self, id: &str, p_market: f64) -> AppResult<()>;
    pub fn record_position(
        &self,
        id: &str,
        invested: f64,
        realized_pnl: f64,
        yes_shares: f64,
        no_shares: f64,
    ) -> AppResult<()>;
    pub fn record_kind(&self, id: &str, outcome_type: &str, sum_to_one: bool) -> AppResult<()>;
}
```

Rules, enforced in every intent:

1. Write SQLite first. On error, return `Err` and leave the cache untouched.
2. Patch only the columns of that intent in the cache entry. If the entry is
   absent, do nothing. A soft-deleted row can still receive a late write; that
   is harmless and must not re-insert it.
3. No `await` and no lock held across an `.await`. Every method is
   synchronous.
4. `record_status` accepts `Active` and `Paused` only. `Closed` must go through
   `close` or `untrack`, which also drop the cache entry. Use
   `debug_assert!(status != MarketStatus::Closed)`.

`db.rs` keeps `insert_signal`, the signal and trade ledgers, every answer
method, and the key-value methods. `MarketStore` is the only path to
`tracked_markets`.

## 5. Code moves

Move into `src/market_store.rs`:

- `MARKET_COLUMNS` (`db.rs:17`)
- `insert_market`, `get_market`, `list_markets`, `list_markets_including_deleted`,
  `list_markets_by_status`, `update_market_status`, `update_budget`,
  `update_belief`, `update_position`, `update_market_kind`, `set_thread_id`,
  `delete_market`, `soft_delete_market` (`db.rs:128-362`)
- `market_from_row` (`db.rs:754`)
- `MarketCache` and its tests (`pipeline.rs:62-154`, `pipeline.rs:790-889`)

`list_markets_by_status` becomes `active()`; its only production caller is
`sync_all` (`trade.rs:1794`). `set_thread_id` and `MarketCache::reload` have no
production callers; both are deleted.

Make these helpers visible to the new module:

- `Db::lock` → `pub(crate)` (`db.rs:115`)
- `ensure_updated` → `pub(crate)` (`db.rs:707`); answer methods still use it
- `invalid_text` → `pub(crate)` (`db.rs:727`); the signal and trade row mappers
  still use it

New SQL for the split intents:

```sql
-- record_belief
UPDATE tracked_markets SET p_internal_log_odds = ?2, belief_updated_at = ?3 WHERE id = ?1;
-- record_price
UPDATE tracked_markets SET p_market_last = ?2 WHERE id = ?1;
```

The two migration tests that reach into `tracked_markets`
(`db.rs:958-1007`) keep living in `db.rs`. Rewrite them to select only the
columns under test (`id, market_url, deleted_at`) instead of `MARKET_COLUMNS`
and `market_from_row`. Do not widen the store interface for a test.

## 6. Call-site migration

| Site | Today | After |
|---|---|---|
| `main.rs:61` | `db.list_markets()` | `store.list()` |
| `main.rs:80` | `MarketCache::load(&db)` | `MarketStore::new(&db)` |
| `main.rs:163` | `pipeline.cache.active_count()` | `store.active_count()` |
| `onboarding.rs:320,373` | `db.get_market()` | `store.load()` |
| `onboarding.rs:376,383` | `db.delete_market()` | `store.delete()` |
| `onboarding.rs:379` | `db.insert_market()` | `store.insert()` |
| `onboarding.rs:402` | `cache.upsert()` | remove; `insert` already cached it |
| `discord.rs:346` | `cache.active_markets()` | `store.active()` |
| `discord.rs:347,451,528,1462,2215,2227` | `db.list_markets*()` | `store.list*()` |
| `discord.rs:1157,1724,1835,2158` | `db.get_market()` | `store.load()` |
| `discord.rs:1280-1281` | `soft_delete_market` + `cache.remove` | `store.untrack(id, now)` |
| `discord.rs:1347-1350` | `update_budget` + reload + `upsert` | `store.record_budget(id, new_budget)` |
| `discord.rs:1393-1396` | `update_belief` + reload + `upsert` | `store.record_belief(id, log_odds, now)` |
| `discord.rs:1738-1740` | `update_market_status` + reload + `upsert` | `store.record_status(id, new_status)` |
| `discord.rs:1873-1878` | `update_market_status(Closed)` + `cache.remove` | `store.close(id)` |
| `pipeline.rs:362,491` | `cache.get()` | `store.cached()` |
| `pipeline.rs:671-674` | `db.update_belief` + `cache.record_belief` | `store.record_belief(id, log_odds, updated_at)` |
| `pipeline.rs:700` | `db.update_answer_belief` | unchanged (answers out of scope) |
| `trade.rs:1318,1742,1821` | `db.get_market()` | `store.load()` |
| `trade.rs:1579` | `db.update_market_kind()` | `store.record_kind()` |
| `trade.rs:1794` | `db.list_markets_by_status(Active)` | `store.active()` |
| `trade.rs:2031` | `db.list_markets()` | `store.list()` |
| `trade.rs:2374-2390` | `persist_market` | `record_position` + `record_price` |
| `trade.rs:2388` | `cache.upsert()` | remove |

`persist_market` keeps its name and its eight call sites
(`trade.rs:1367,1417,1442,1554,1762,1783,1860,1900,1925,2011`). Its body
becomes:

```rust
fn persist_market(&self, market: &TrackedMarket) -> AppResult<()> {
    self.store.record_position(
        &market.id,
        market.invested_mana,
        market.realized_pnl_mana,
        market.yes_shares,
        market.no_shares,
    )?;
    self.store.record_price(&market.id, market.p_market_last)
}
```

The belief write disappears. `refresh_market_kind` calls `record_kind` and then
patches its local `market` value, as it does today.

`Pipeline` loses `pub cache: Arc<MarketCache>` and gains
`store: Arc<MarketStore>`. `TradeExecutor`, `OnboardingService`, and
`DiscordState` lose `cache` and gain `store`. Construct the store once in
`main` and pass `Arc` clones.

## 7. Tests

New tests in `src/market_store.rs`, all with `Db::open_in_memory()`:

1. `record_belief_leaves_position_status_and_price_alone`
2. `record_position_leaves_belief_price_and_status_alone`
3. `record_price_leaves_belief_alone`
4. `a_stale_position_write_does_not_revert_a_newer_belief` — the defect:
   load a snapshot, apply a belief, then write the stale position values.
   Assert the belief survives in both the row and the cache.
5. `a_position_write_does_not_revert_a_pause` — the second defect: set
   `Paused`, write a position, assert `Paused` in both stores.
6. `close_and_untrack_remove_the_cache_entry_and_keep_the_row` — `close` leaves
   `deleted_at` NULL, `untrack` sets it; both hide the market from `cached`.
7. `an_intent_on_a_missing_market_reports_an_error_and_changes_nothing`
8. `insert_is_visible_to_cached_and_to_load`
9. `an_intent_on_an_uncached_row_updates_the_database_only` — a soft-deleted row
   must not be re-inserted into the cache.
10. `active_excludes_paused_and_is_sorted` and `active_count_matches`
    (moved from `pipeline.rs`).

Move from `db.rs` to `market_store.rs`, adapted to the new API:

- `market_roundtrip`, `insert_duplicate_market_fails`,
  `list_markets_is_ordered_by_creation_time`, `list_markets_by_status_filters`,
  `soft_delete_closes_the_row_and_hides_it_from_listings`,
  `soft_delete_keeps_answers_and_is_idempotent`, `updates_persist`,
  `updating_missing_market_fails`.
- `updates_persist` becomes six intent tests, one per column group, instead of
  one whole-row update. This is the point of the module.

Keep in `db.rs`: every signal, trade, answer, key-value, and migration test.
The store gets its own `sample_market` fixture; the repo already duplicates
fixtures per module.

Update existing tests: `onboarding.rs:682-698` builds the service with a cache;
`trade.rs:3315-3380` builds `MarketCache::load`; `pipeline.rs` cache tests move.

## 8. Commit sequence

1. `feat(market-store): add intent-based market state writer`
   New `src/market_store.rs`, `mod market_store;` in `main.rs`, the three
   `pub(crate)` helper changes, and the new tests. Nothing else calls it yet;
   `#![allow(dead_code)]` already covers that.
2. `refactor: route all market writes through MarketStore`
   The single switch: `main`, `pipeline`, `trade`, `onboarding`, `discord`;
   delete `MarketCache`. This cannot be split per module without a window where
   two caches are live and can disagree, so it is one logical commit.
3. `refactor(db): remove the superseded market API`
   Delete the moved SQL and methods from `db.rs`, delete their tests, rewrite
   the two migration tests to select only the migrated columns.
4. `docs: record market state ownership`
   Update `docs/ARCHITECTURE.md` §2 (dependency edges), §3 (mirrors and
   writers), §4 (`MarketStore` replaces the `MarketCache` contract), and §7A
   (mark resolved). Add one line to `docs/SPEC.md` §6.5: `p_market_last` is
   written by the trade sync only, never by triage or override.

Run the gates after every commit:

```sh
cargo build --release
cargo test
cargo clippy -p manif0ld --all-targets --no-deps -- -D warnings
```

## 9. Risks and residuals

- **Behaviour change:** the pipeline and `/override` no longer refresh
  `p_market_last`. The 60-second sync still does. A market that fails every
  `/v0/market` fetch keeps its last price; today it would keep the same cached
  price anyway.
- **Large commit 2.** It is the smallest correct switch. See the note above.
- **Registry membership remains separate.** A paused market still keeps its
  Sieve engine and is skipped by status in `dispatch`. That is intended and
  unchanged. `EngineRegistry` disagreement with status is the control-plane
  candidate's concern.
- **`onboarding::activate` still spans several steps**: `store.insert`, answer
  upserts, engine registration, and manual rollback. The rollback must call
  `store.delete` instead of `db.delete_market`. The ordering does not change.
- **Late writes to untracked markets.** `record_position` on a soft-deleted row
  updates the row and leaves the cache empty. Harmless, and covered by test 9.
- **Answer price race remains** in `apply_to_answer`, inside one synchronous
  block with no `await`, so the window is negligible. Candidate #2 owns it.
- **`record_position` takes four floats.** A `Position` value type would be
  deeper, but it would ripple through `TrackedMarket`, the row mapper, and
  `trade.rs`. Candidate #2 should introduce it.

## 10. Acceptance

- `rg "cache\.upsert|MarketCache" src/` returns nothing.
- Every write to `tracked_markets` lives in `src/market_store.rs`.
- `persist_market` contains no belief write.
- Tests 4 and 5 pass; they fail against the current `persist_market`.
- The three gates are clean.
