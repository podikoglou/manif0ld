# Plan: one control plane for operator commands (`ControlPlane`)

Status: proposed. Continues the architecture review after `docs/plans/market-store.md`
(candidate 1) and `docs/plans/book.md` (candidate 2). This is candidate 4, with the
lifecycle half of candidate B folded in.

## 1. Problem

Every operator command is an orchestrator. A single Discord handler reads a row,
validates it, calls the trade executor, writes a lifecycle transition, edits the
engine registry, archives a thread, and formats the reply. Nothing owns those steps,
so two commands that should do the same thing do it twice, in different orders, with
different end states.

All line numbers are `src/` at `c544be2`.

1. **Seven handlers re-orchestrate three layers by hand.** `DiscordState`
   (`discord.rs:118`) exposes `config`, `db`, `registry`, `store`, `pipeline`,
   `onboarding`, and `executor` as seven public fields, and every handler reaches
   into whichever it needs:

   | Handler | Line | Writes |
   |---|---|---|
   | `track` / `onboard` | `948` / `1145` | onboarding activate (row + answers + engine), thread create, thread archive on failure |
   | `untrack` / `untrack_market` | `1212` / `1250` | executor sell, engine unregister, `store.untrack`, thread archive |
   | `budget` | `1315` | `store.record_budget` |
   | `override_belief` | `1360` | `store.record_belief`, spawned trade check |
   | `panic_mode` | `1427` | `db.set_kv`, `executor.liquidate_all` |
   | `pause_button` | `1702` | `store.record_status` |
   | `liquidate_button` / `liquidate_market` | `1777` / `1816` | executor sell, `store.close`, engine unregister, thread archive |

2. **Two teardown bodies, two end states.** `untrack_market` (`1250-1308`) and
   `liquidate_market` (`1816-1891`) both do: validate the row → sell → write the
   status → unregister the engine → archive the thread → format a report. They differ
   in six ways that nobody wrote down together:

   | | `/untrack` | Liquidate button |
   |---|---|---|
   | end state | `CLOSED` + `deleted_at` (`1279`) | `CLOSED`, `deleted_at` NULL (`1857`) |
   | row lookup | `list_including_deleted` (`2208`) | `store.load` (`1821`) |
   | already closed | skips the sale, still untracks | returns early, no cleanup |
   | row write failure | propagated (`?`, `1279`) | logged and ignored (`1857`) |
   | executor `AlreadyClosed` | not inspected | returns early, **no** archive (`1845`) |
   | message set | 5 shapes | 8 shapes |

   The ordering is duplicated; the differences are not data. A third teardown (a
   future `/close`) would be a third copy and a third set of rules.

3. **Thread lifecycle is inlined in four places.** Create at `1171`, compensation
   archive at `1184`, archive at `1282`, archive at `1863`. Two of them (`1184`,
   `1282`) exist only to clean up after a step that failed. No test can reach them.

4. **None of it is testable.** `discord.rs` has 30 tests; all test formatting
   (`truncate_chars`, `sieve_report`, `answer_lines`, …) and none builds a
   `DiscordState`. A handler test is impossible today for a structural reason:
   `TradeExecutor::new` (`trade.rs:604`) needs `GET /v0/me` and
   `OnboardingService::new` (`onboarding.rs`) needs `LlmClient::new` plus a Firecrawl
   key. So the two teardown end states, the idempotence rules, and the compensation
   order are pinned by nothing but the operator's memory.

5. **The adapter owns domain rules.** "A closed market cannot be resumed"
   (`1719`), "an already untracked market changed nothing" (`1260`), "the thread is
   created before activation" (`1141-1144`), and "an override triggers one trade
   check" (`1387`) are trading rules, and they live in Discord handlers.

The deletion test: delete the orchestration from the seven handlers and the complexity
does not vanish. It reconstitutes in `main.rs` or in the next handler, and the two end
states drift further apart. It belongs behind one interface.

## 2. Decisions taken

1. **New module `src/control.rs`** with one `ControlPlane`. It is the only writer of
   a lifecycle transition (`close`, `untrack`), the only caller of
   `EngineRegistry::unregister` outside onboarding's own rollback, and the only
   caller of `TradeExecutor::liquidate*` and of the kill-switch write. Dependency
   edge: `discord → control → {store, registry, trade, onboarding, belief, db,
   types, error}`. `control` never imports `serenity` or `discord`.

2. **Three small ports, defined in `control.rs`.** `Threads`, `Onboarder`, and
   `Trading` are traits. The real adapters are the Discord HTTP client (in
   `discord.rs`), `OnboardingService`, and `TradeExecutor` (both implemented in
   `control.rs`). Each has two adapters (real + test fake), so each is a real seam,
   and every rule in §1 becomes testable in-process with no network and no Discord.
   `async-trait` is already a direct dependency (`Cargo.toml`), so this adds nothing.

3. **One teardown body, with the end state as a value.** `End::Hide` (`/untrack`:
   `CLOSED` + `deleted_at`) and `End::Keep` (the button: `CLOSED`, row visible). Both
   today's behaviours are preserved exactly; the difference moves from two code paths
   into one documented parameter, and each gets a test. Trading behaviour, `SPEC.md`
   §5.1, and `SPEC.md` §6.8 do not change.

4. **Reports are data; rendering stays in the adapter.** Each intent returns a small
   enum or struct of facts. `discord.rs` renders the same strings as today. This
   keeps presentation out of the service and lets a test assert facts
   (`AlreadyClosed`, `Sale::Failed`) instead of substrings.

5. **Mutations only.** `/status`, autocomplete, `TrackProgress`, the embed builders,
   and the two event consumers stay in `discord.rs`. They only read, so they cannot
   make two stores disagree, and moving them would rewrite a large block of a
   3013-line file that is already fmt-dirty.

6. **The thread *name* stays in the adapter.** `Threads::create_for_market(budget,
   question)` receives the two inputs, and `discord.rs` keeps `shorten_question` and
   the `[M{budget}] {question}` format. The control plane owns only the *ordering*
   (create → activate → archive on failure).

7. **The `/track` progress sender is passed by value.** `mpsc::Sender<OnboardUpdate>`
   must drop when `track` returns, because the handler awaits the progress updater
   right after (`discord.rs:1059`). If the sender stayed alive in the handler scope,
   the updater would wait on an open channel forever. This is a real deadlock hazard,
   so it is part of the signature, not a style choice.

## 3. The model

### What each layer owns after the change

| Fact | Owner after the change | Writers |
|---|---|---|
| `status`, `deleted_at` | `MarketStore` | `control` (`set_paused`, teardown), onboarding activate |
| Engine membership | `EngineRegistry` | `main` startup, onboarding activate, onboarding rollback, `control` teardown |
| Position | `Book` | `Book::persist` only (unchanged) |
| Budget | `MarketStore` | `control::set_budget` |
| Belief | `MarketStore` | `control::set_probability`, pipeline (unchanged) |
| Kill switch | `kv` | `control::panic` |
| Thread | `tracked_markets.discord_thread_id` | onboarding activate (unchanged); the thread's life is driven by `control` |
| Discord presentation | `discord.rs` | unchanged |

`DiscordState` loses `registry`, `onboarding`, and `executor`, and gains
`control: Arc<ControlPlane>`. The four remaining direct handles (`config`, `db`,
`store`, `pipeline`) are read-only in every handler that keeps one. The invariant is
enforceable by reading `control.rs`: no other module calls `store.close`,
`store.untrack`, or `registry.unregister` outside tests.

### Teardown order, preserved step by step

`ControlPlane::teardown(market, end, threads)` is the single body:

| Step | `End::Hide` (`/untrack`) | `End::Keep` (Liquidate button) |
|---|---|---|
| 1. unregister the engine | yes (`1278`) | after the row write |
| 2. write the row | `store.untrack` (`1279`), **error propagated** | `store.close` (`1857`), **error logged** |
| 3. unregister the engine | — | yes (`1860`) |
| 4. archive the thread | only when step 2 succeeded | always (`1862`) |

The divergence is deliberate and is recorded as residual §10 item 1. The sale is a
caller step, because `close` inspects `LiquidationOutcome::AlreadyClosed` before it
retires anything (`1845`).

### The two end states

```
/untrack        : store.untrack  → CLOSED + deleted_at  → hidden from list()/active()
Liquidate button: store.close    → CLOSED, deleted_at NULL → stays in list()/active()
```

`End` is private to `control.rs`; callers choose it by calling `untrack` or `close`.

## 4. Interface

### `src/control.rs`

```rust
/// The public thread that belongs to one market.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ThreadId(pub u64);

/// Thread lifecycle, and nothing else.
///
/// The control plane owns the *ordering*; the adapter owns the Discord API and the
/// thread name. Archive is best effort: the adapter logs a failure, never raises.
#[async_trait]
pub trait Threads: Send + Sync {
    async fn create_for_market(&self, budget: f64, question: &str) -> AppResult<ThreadId>;
    async fn archive(&self, thread: ThreadId);
}

/// The onboarding transaction. `prepare` is read-only; `activate` is the only writer.
#[async_trait]
pub trait Onboarder: Send + Sync {
    async fn prepare(
        &self,
        url: &str,
        budget: f64,
        initial_prob: Option<u8>,
        updates: mpsc::Sender<OnboardUpdate>,
    ) -> AppResult<OnboardingDraft>;
    fn activate(&self, draft: OnboardingDraft, thread_id: &str) -> AppResult<TrackedMarket>;
}

/// The trading layer, as the control plane needs it.
#[async_trait]
pub trait Trading: Send + Sync {
    /// One trade check, no trigger post.
    async fn evaluate(&self, market_id: &str) -> AppResult<()>;
    async fn liquidate(&self, market_id: &str) -> AppResult<LiquidationOutcome>;
    async fn liquidate_all(&self) -> Vec<(MarketId, anyhow::Error)>;
}

pub struct ControlPlane {
    db: Db,
    store: Arc<MarketStore>,
    registry: Arc<EngineRegistry>,
    onboarding: Arc<dyn Onboarder>,
    trading: Arc<dyn Trading>,
}

impl ControlPlane {
    pub fn new(
        db: Db,
        store: Arc<MarketStore>,
        registry: Arc<EngineRegistry>,
        onboarding: Arc<dyn Onboarder>,
        trading: Arc<dyn Trading>,
    ) -> Self;

    /// `/track`. prepare → duplicate check → thread → activate → archive on failure.
    /// The embed is posted by the caller.
    pub async fn track(
        &self,
        url: &str,
        budget: f64,
        initial_prob: Option<u8>,
        updates: mpsc::Sender<OnboardUpdate>,
        threads: &dyn Threads,
    ) -> AppResult<Onboarded>;

    /// `/untrack`. Sell when asked, then `End::Hide`.
    pub async fn untrack(
        &self,
        query: &str,
        liquidate: bool,
        threads: &dyn Threads,
    ) -> AppResult<UntrackReport>;

    /// The Liquidate button. Sell, then `End::Keep`.
    pub async fn close(&self, market_id: &str, threads: &dyn Threads) -> AppResult<CloseReport>;

    pub fn set_budget(&self, query: &str, new_budget: f64) -> AppResult<BudgetChange>;
    pub fn set_probability(&self, query: &str, probability: f64) -> AppResult<ProbabilityChange>;
    /// Toggles ACTIVE <-> PAUSED. Errors carry the operator text verbatim.
    pub fn set_paused(&self, market_id: &str) -> AppResult<MarketStatus>;
    /// One trade check. Logs instead of raising; the caller spawns it.
    pub async fn check_now(&self, market_id: &str);
    /// `/panic`. Sets the kill switch, then sweeps when asked.
    pub async fn panic(&self, liquidate_all: bool) -> AppResult<PanicReport>;
}

/// The resolver, moved from `discord.rs`. `/status` uses it too.
pub(crate) fn resolve_market<'a>(markets: &'a [TrackedMarket], query: &str)
    -> Option<&'a TrackedMarket>;
```

### Reports

```rust
pub struct Onboarded {
    pub draft: OnboardingDraft,   // market + rule, for the [ONBOARDED] embed
    pub thread: ThreadId,
}

/// What happened to the position during a teardown.
pub enum Sale {
    /// The sale ran and the executor reported shares sold.
    Liquidated,
    /// `/untrack liquidate:false` on a live market.
    Untouched,
    /// The row was already closed, so nothing was attempted.
    AlreadyClosed,
    /// Manifold resolved the market; positions settle automatically.
    Resolved,
    /// The market held no shares.
    NoPosition,
    /// The sale failed. The text is `Display` of the executor error.
    Failed(String),
}

pub enum UntrackReport {
    AlreadyUntracked { question: String, market_id: String },
    Untracked { question: String, market_id: String, sale: Sale },
}

pub enum CloseReport {
    NotTracked { market_id: String },
    AlreadyUntracked { question: String, market_id: String },
    AlreadyClosed { question: String, market_id: String },
    /// The executor saw `AlreadyClosed`; nothing was closed, unregistered, or archived.
    ClosedByAnotherAction { question: String, market_id: String },
    Closed { question: String, market_id: String, sale: Sale },
}

pub struct BudgetChange {
    pub question: String,
    pub market_id: String,
    pub before: f64,
    pub after: f64,
}

pub struct ProbabilityChange {
    pub question: String,
    pub market_id: String,
    pub log_odds: f64,
    pub clamped_p: f64,
}

pub struct PanicReport {
    /// `None` when `liquidate_all` was false.
    pub attempted: Option<usize>,
    pub liquidated: usize,
    /// `(market_id, Display of the error)`, untruncated.
    pub errors: Vec<(String, String)>,
}
```

`Sale` is not `Clone`; it is consumed by the renderer. `LiquidationOutcome` and
`KILL_SWITCH_KEY` are imported from `trade.rs`; `OnboardingDraft`, `OnboardUpdate`, and
`parse_slug` from `onboarding.rs`.

### Adapter implementations

```rust
// in control.rs: keep the edges pointing at the services
#[async_trait] impl Onboarder for OnboardingService { .. }
#[async_trait] impl Trading for TradeExecutor { .. }

// in discord.rs: the adapter type is defined there, so control.rs must not name it
struct DiscordThreads { http: Arc<Http>, control_channel: ChannelId }
#[async_trait] impl control::Threads for DiscordThreads { .. }
```

## 5. Code moves

Move into `src/control.rs`:

- `resolve_market` (`discord.rs:672`) with its two tests, and the bodies of
  `resolve_single_market` (`2196`) and `resolve_any_market` (`2208`) as private
  helpers `resolve_tracked` and `resolve_any`.
- The transaction bodies of `onboard` (`1145-1189`), `untrack_market` (`1250-1308`),
  `liquidate_market` (`1816-1891`), `budget` (`1342-1350`), the belief write and
  trade check of `override_belief` (`1379-1406`), `panic_mode` (`1435-1469`) minus
  its prose, and the toggle of `pause_button` (`1712-1729`).

Delete from `src/discord.rs`:

- `untrack_market` and `liquidate_market` entirely.
- The duplicate check, thread creation, activation, and compensation inside
  `onboard`; it keeps only the `[ONBOARDED]` embed post, using `Onboarded`.
- The two resolver tests that move.

Keep in `src/discord.rs`, unchanged:

- `status` and its helpers, `autocomplete`/`market_choices`, `TrackProgress`,
  `track`'s progress updater and timeout, `finish_track_progress`,
  `sieve_report*`, all embed builders, the two consumers, `channel_for_market`,
  `truncate_chars`, `shorten_question`, button ids, and the option helpers.
- New private renderers: `render_untrack(&UntrackReport)`,
  `render_close(&CloseReport)`, `render_sale(...)`, `render_panic(&PanicReport)`, and
  one line each for budget, probability, and pause. The strings are copied from the
  current code **verbatim**; see §6 item 1.

Keep in `src/trade.rs`, `src/onboarding.rs`, `src/market_store.rs`: every public
method. This plan adds no method to them.

## 6. Behaviour ledger

Every difference between today and after. Anything not listed must behave
identically; §8 is the enforcement.

| # | Change | Why | Verdict |
|---|---|---|---|
| 1 | The reply strings now come from report enums, rendered in `discord.rs` | facts move, presentation stays | **must be byte-identical**; copy from `discord.rs:1260-1307`, `1848-1889`, `1346`, `1401-1405`, `1456-1464`, `1756` |
| 2 | `/untrack` and the button share one teardown body | one ordering, one place to fix | same SQL, same order; the end state is `End::Hide` vs `End::Keep` |
| 3 | The thread name is built in the adapter, from `(budget, question)` | keeps `shorten_question` with its tests; avoids a `control → discord` edge | same string: `format!("[M{budget}] {}", shorten_question(question, 50))` |
| 4 | `/untrack` still propagates a `store.untrack` failure, and then does not archive | today's order (`1278-1279`) | preserved; recorded as residual §10 item 1 |
| 5 | The button path still logs a `store.close` failure and continues | today (`1857`) | preserved |
| 6 | `/override` still triggers one trade check off the interaction path | latency rule unchanged | the `tokio::spawn` stays in the handler; the check and its log line move to `check_now` |
| 7 | `/panic` returns untruncated error text; the handler truncates to 400 chars | truncation is presentation | `error.to_string()` then `truncate_chars(.., 400)` gives today's output |
| 8 | `/track` posts the embed after the service returns | the thread is activated before the embed, as today | embed failure is logged only, as today |
| 9 | Input validation stays in the handler | it is about command options, not market state | unchanged: positive budget, positive new budget, 1-99, non-empty query |
| 10 | `DiscordState` has five fields instead of seven | `registry`, `onboarding`, `executor` are no longer reachable from a handler | no behaviour change; `main.rs` wiring changes |
| 11 | The `TrackProgress` log no longer sees service-side state | it only ever saw `OnboardUpdate` values | unchanged |
| 12 | `check_now` is `pub` and awaitable; a test can call it directly | determinism | production still spawns it |

## 7. Call-site migration

| Site | Today | After |
|---|---|---|
| `discord.rs:948` `track` | parses, defers, spawns the updater, calls `onboard` | same; calls `control.track(url, budget, prob, updates_tx, &threads)` and posts the embed from `Onboarded` |
| `discord.rs:1145` `onboard` | prepare, duplicate check, thread, activate, compensate, embed | 8 lines: call the service, post the embed, log a post failure |
| `discord.rs:1212` `untrack` | parse, defer, call `untrack_market`, reply | call `control.untrack`, `render_untrack` |
| `discord.rs:1250` `untrack_market` | resolve, sell, unregister, `store.untrack`, archive, summary | **deleted** |
| `discord.rs:1315` `budget` | resolve, `record_budget`, summary | `control.set_budget`, one format |
| `discord.rs:1360` `override_belief` | resolve, `override_probability`, `record_belief`, spawn | `control.set_probability`, spawn `control.check_now(id)` |
| `discord.rs:1427` `panic_mode` | kv, sweep, count, summary | `control.panic`, `render_panic` |
| `discord.rs:1702` `pause_button` | load, validate, `record_status`, swap label | `control.set_paused`, swap label |
| `discord.rs:1777` `liquidate_button` | call `liquidate_market`, disable buttons | call `control.close`, disable buttons |
| `discord.rs:1816` `liquidate_market` | load, validate, sell, close, unregister, archive, summary | **deleted** |
| `discord.rs:2196`/`2208` resolvers | two free functions over `state.store` | private `ControlPlane` helpers |
| `discord.rs:672` `resolve_market` | `pub(crate)` here | moved to `control.rs`; `/status` imports it |
| `discord.rs:530` `/status` | `resolve_market(&markets, query)` | `crate::control::resolve_market(..)`, same call |
| `main.rs:124-132` | `DiscordState` with seven fields | build `Arc<ControlPlane>` after the pipeline and the executor; `DiscordState` with five |

`DiscordState` after the change:

```rust
pub struct DiscordState {
    pub config: Arc<Config>,
    pub db: Db,
    pub store: Arc<MarketStore>,
    pub pipeline: Arc<Pipeline>,
    pub control: Arc<ControlPlane>,
}
```

`db` stays for `/status` (`latest_signal_time`, `list_answers`, `recent_signals`).
`main.rs` builds the port values with a coercion:

```rust
let control = Arc::new(ControlPlane::new(
    db.clone(),
    Arc::clone(&store),
    Arc::clone(&pipeline.registry),
    onboarding,              // Arc<OnboardingService> -> Arc<dyn Onboarder>
    Arc::clone(&executor),   // Arc<TradeExecutor>    -> Arc<dyn Trading>
));
```

## 8. Tests

New tests in `src/control.rs`. Fixtures:

- `market(id, question, slug, status)` — the `discord.rs:2404` helper with
  `sieve_source: "match Market { \"thing\" }"` and a non-empty
  `discord_thread_id`, so `insert` and the registry both accept it.
- `store_with(&[TrackedMarket])` — `Db::open_in_memory()` +
  `MarketStore::new` + `insert` per row.
- `registry_with(&[id])` — `EngineRegistry::new()` + `register_source`.
- `RecordingThreads` — records `created: Vec<(f64, String)>`, `archived:
  Vec<ThreadId>`, and can fail `create`.
- `ScriptedOnboarder` — returns a fixed draft; can fail `activate`; records the
  thread id it was given.
- `RecordingTrading` — records `evaluate`/`liquidate`/`liquidate_all` calls; returns a
  scripted `LiquidationOutcome` or error.

1. `track_creates_the_thread_and_passes_it_to_activate`
2. `track_archives_the_thread_when_activation_fails`
3. `track_refuses_a_market_that_is_already_tracked` — no thread created, no activate
4. `track_replaces_a_market_that_is_closed` — the duplicate guard lets Closed through
5. `untrack_hides_the_row_and_unregisters_the_engine` — `CLOSED`, `deleted_at` set,
   `list()` no longer returns it, `registry.contains` false, thread archived
6. `close_keeps_the_row_visible_and_unregisters_the_engine` — `CLOSED`, `deleted_at`
   `None`, `list()` still returns it, thread archived
7. `untrack_without_liquidation_sells_nothing` — `Sale::Untouched`, no liquidate call
8. `untrack_continues_when_the_sale_fails` — row still hidden, engine gone, thread
   archived, `Sale::Failed(text)`
9. `untrack_reports_an_already_untracked_market` — no write, no sale, no archive
10. `untrack_skips_an_empty_thread_id` — row hidden, `archived` empty
11. `close_reports_a_market_that_is_already_closed` — no sale, no archive
12. `close_does_not_archive_when_the_executor_reports_already_closed` — the race path:
    `ClosedByAnotherAction`, status untouched, `archived` empty
13. `close_reports_a_resolved_market` and an empty position — `Sale::Resolved` /
    `Sale::NoPosition`, both still closed and archived
14. `set_paused_toggles_between_active_and_paused` — two calls return the two states
15. `set_paused_rejects_a_closed_or_untracked_market` — the three error texts
16. `set_budget_reports_the_old_and_the_new_value`
17. `set_probability_writes_the_belief_and_leaves_the_price` — `p_market_last`
    unchanged, `p_internal_log_odds` set to `belief::override_probability(p)`
18. `check_now_runs_one_evaluation`
19. `panic_sets_the_kill_switch_and_counts_the_sweep` — `kv` is `"true"`,
    `attempted`/`liquidated` follow today's arithmetic
20. `panic_without_liquidate_all_sells_nothing` — `attempted` is `None`, no sweep call

Moved from `discord.rs` with their assertions unchanged:

21. `resolves_a_market_by_case_insensitive_id_or_slug`
22. `ambiguous_slug_matches_resolve_to_none`

No test in `discord.rs` is deleted except those two. No test changes an assertion;
call shapes may change.

## 9. Commit sequence

1. `feat(control): add the control plane service`
   New `src/control.rs`: the three ports and their impls, `ControlPlane`, the report
   types, the moved resolver, and tests 1-22. `mod control;` in `main.rs`. Nothing
   calls it yet, so the crate keeps working (`#![allow(dead_code)]` already covers
   it). `discord.rs` is untouched; its copy of `resolve_market` and its two tests
   still exist, so the test census grows by 21 here.

2. `refactor(discord): route the operator commands through the control plane`
   The single switch: add `DiscordThreads`, shrink `DiscordState` to five fields,
   rewrite `track`/`onboard`/`untrack`/`budget`/`override_belief`/`panic_mode`/
   `pause_button`/`liquidate_button`, delete `untrack_market`, `liquidate_market`,
   the two resolvers, and the two moved tests, point `/status` at
   `control::resolve_market`, and update `main.rs`. One commit because a partial
   switch leaves two teardown implementations live at once — the same reason the
   market-store switch was one commit.

3. `docs: record the control plane`
   `docs/ARCHITECTURE.md`: §1 task table (handlers now call one service), §2
   dependency graph (add `control`, mark `discord → control`), §3 (one writer for
   lifecycle transitions and engine membership; the teardown order table), §4 (add
   the `control.rs` interface; reduce the `discord.rs` surface),
   §5.2/§5.3 (one teardown body), §7 B/C/D marked resolved with residuals.
   `docs/SPEC.md` §6.8: one sentence that every mutating command goes through one
   service and that the two teardown end states are explicit (`/untrack` hides the
   row, `Liquidate & Close` keeps it).

Run the gates after every commit:

```sh
cargo build --release
cargo test
cargo clippy -p manif0ld --all-targets --no-deps -- -D warnings
rustfmt --edition 2021 --check src/control.rs src/main.rs
```

`cargo fmt --check` is dirty at HEAD for `discord.rs`. Do **not** run rustfmt on
`discord.rs`; it would reformat the whole file inside a refactor. `control.rs` and
`main.rs` must be clean.

## 10. Risks and residuals

- **Message wording is the main risk.** Twelve strings must survive unchanged. The
  plan lists their source lines in §6 item 1; the implementor should copy them, not
  retype them. Review the renderers first.
- **The `/track` deadline.** The handler's `ONBOARD_TIMEOUT` wraps the whole call,
  including thread creation. Passing `updates` by value (§2 item 7) keeps the
  progress updater able to finish; passing it by reference deadlocks the handler at
  `discord.rs:1059`. This is the one place where the signature is correctness, not
  style.
- **Trait objects and `Send`.** `ControlPlane` is shared across tasks; the ports need
  `Send + Sync` supertraits, which `#[async_trait]` does not add by itself. Write
  them in the trait declaration.
- **Residual 1: the teardown failure order still differs** between `End::Hide`
  (unregister first, then a propagated row write) and `End::Keep` (logged row write,
  then unregister). Preserved on purpose. Unifying it changes which store a failed
  transition leaves behind, which is an operator-visible difference under a DB error
  and needs its own decision.
- **Residual 2: the read path keeps four direct handles.** `/status` reads the
  store, the DB, and the pipeline's queue depth without the service. Accepted: it
  cannot write, and moving it would move 12 formatting tests.
- **Residual 3: input validation is in the adapter.** The service trusts its
  arguments. `OnboardingService::prepare` still re-checks the budget
  (`onboarding.rs:302`), so a bad value cannot reach the row.
- **Residual 4: `/panic` cannot be undone.** Unchanged; the operator edits `kv`.
- **Residual 5: the global `execution_lock` stays.** Deferred on purpose, as recorded
  in `docs/plans/book.md` §10.
- **The `Onboarder` trait can drift from `OnboardingService`.** The impl is a thin
  forwarder; `cargo build` catches a signature change.
- **`check_now` awaits an evaluation that can take a Manifold round trip.** Production
  still spawns it; only tests await it.

## 11. Acceptance

- `rg "state\.(registry|onboarding|executor)" src/discord.rs` returns nothing.
- `rg "fn untrack_market|fn liquidate_market|fn resolve_single_market|fn resolve_any_market" src/discord.rs`
  returns nothing.
- `rg "store\.untrack\(|store\.close\(|registry\.unregister\(" src/` returns only
  `control.rs`, `onboarding.rs:385` (its own activate rollback), `market_store.rs`
  (the definitions), and tests.
- `rg "serenity|discord::" src/control.rs` returns nothing.
- `DiscordState` has exactly five fields, and `main.rs` constructs `ControlPlane`.
- The twelve reply strings in §6 item 1 are byte-identical to HEAD: diff the
  rendered output against `git show c544be2:src/discord.rs` for each variant.
- Tests 1-22 pass; the only deleted tests are the two moved resolvers.
- The three gates are clean, and `rustfmt --edition 2021 --check src/control.rs
  src/main.rs` passes.
