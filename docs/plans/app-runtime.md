# Plan: one composition root and one task owner (`App` / `Tasks`)

Status: proposed. This is candidate 7J of `docs/ARCHITECTURE.md`, after
`docs/plans/market-store.md` (1), `docs/plans/book.md` (2), and
`docs/plans/control-plane.md` (4).

## 1. Problem

`src/main.rs` is the whole runtime topology written as one function body. It
builds every `Arc`, calls every constructor, subscribes every broadcast
receiver in a specific order, spawns every task, waits for Ctrl-C, and aborts
every handle. The ordering rule that makes the broadcast channels correct lives
in two comments.

All line numbers are `src/main.rs` at `cfc8509` (326 lines).

1. **Orchestration is 171 of 326 lines** (`66-236`, 52% of the file), inside one
   `async fn main`. The file's own module doc (`1-13`) narrates the startup
   order in prose, which is a third copy of the sequence after the code and
   `docs/ARCHITECTURE.md` §1.

2. **Five subscriptions, six spawns, six aborts, all position-dependent.**
   Line numbers at `cfc8509`:

   | What | Lines |
   |---|---|
   | `pipeline.events().subscribe()` → logger | `105` |
   | `pipeline.events().subscribe()` → trade executor | `108` |
   | `pipeline.events().subscribe()` → Discord | `111` |
   | `executor.events().subscribe()` → Discord | `124` |
   | `ingest::channel()` / `post_tx.subscribe()` | `144-145` |
   | `tokio::spawn` | `147`, `156`, `189`, `198`, `204`, `212` (six: pipeline, heartbeat, trade, ingest, discord, event logger) |
   | `.abort()` | `229-234` (six, in the order pipeline, heartbeat, trade, ingest, event, discord) |
   | ordering comments | `106-107`, `109-110` |
   | heartbeat loop | `156-187` |
   | stdout event logger (+ `log_pipeline_event`) | `212-222`, `248-325` |

   The two comments are the entire specification of the rule:

   ```rust
   // Subscribe before spawning the executor: the broadcast channel has no
   // history, so a subscriber created later would miss early signals.
   let trade_events = pipeline.events().subscribe();
   // Discord subscribes before either loop starts for the same reason: a
   // receiver created after the tasks are running may miss startup events.
   let discord_pipeline_events = pipeline.events().subscribe();
   ```

3. **A new module touches six regions of one function.** Adding a consumer
   service means editing: the `mod` list (`19-35`), the `use` block (`37-50`),
   the construction block (`66-134`), the subscription block (`105-124`), a
   `tokio::spawn` block (`147-222`), and the abort list (`229-234`). The fourth
   and the sixth of those are order-sensitive, and neither is checked by
   anything. The same six edits are needed to add a *producer*, because its
   channel's receiver must also exist before the first spawn.

4. **The rule is documented as an invariant but not enforced.** `AGENTS.md:71-72`
   says "Wire new long-running loops in `src/main.rs` and abort them on Ctrl-C";
   `docs/ARCHITECTURE.md` §6 says "`main.rs` subscribes all receivers before
   spawning tasks". §1 also records "there is **no single composition root
   object** after `main.rs`". Three statements, no structural guarantee: a
   receiver created after its producer's task starts silently drops events
   (`broadcast` keeps no history, and `Pipeline::emit` ignores the send error —
   `pipeline.rs:603-605`).

5. **None of this is testable today, and this plan does not fix it.** `App`
   construction needs a live Manifold key (`TradeExecutor::new` calls `GET
   /v0/me`), a process-environment OpenRouter key (`TriageClient::new`) and a
   Firecrawl client (`OnboardingService::new`). §7 says what that means for
   coverage instead of inventing a test; see §9.

The deletion test: delete the orchestration from `main.rs` and the complexity
does not vanish. Every future entry point — a second binary, an example, a
harness — re-implements the same five-subscribe-then-six-spawn sequence, and
each re-implementation can silently drop startup events. The sequence is a rule,
so it belongs behind one interface that cannot express the wrong order.

## 2. Decisions taken

1. **New module `src/app.rs`.** `App` holds the composed state — `config`, `db`,
   `store`, `registry`, `onboarding`, `pipeline`, `executor`, `control`.
   `App::build(config: Config) -> AppResult<App>` is `async` and does all
   construction, including the Sieve startup loop. `App::spawn(self) -> Tasks` is
   sync and does all subscription and all spawning. `Tasks::run(self) ->
   AppResult<()>` waits for Ctrl-C and aborts every handle. Trade-off: one more
   module (~270 lines) for orchestration that currently sits in the entry point;
   the alternative keeps the rule unenforceable and the entry point at 326 lines.

2. **All subscriptions happen inside `App::spawn`, before the first
   `tokio::spawn`.** No caller may create a receiver. This is the rule the
   candidate exists to encode. Enforcement is structural, not documentary: `App`'s
   fields are private and `spawn` takes `self` by value, so no code outside
   `app.rs` can reach a `Pipeline` or a `TradeExecutor` to call `events()` on it.
   Trade-off: a future channel means editing two blocks of one function
   (`subscribe`, then `spawn`) instead of six regions of one function, and the
   ordering becomes one block a reviewer can read in ten seconds instead of a
   property smeared across 171 lines.

3. **`main.rs` keeps process setup only:** the rustls provider install,
   `init_tracing`, `Config::from_env`, then `App::build(config).await?.spawn()
   .run().await`. The heartbeat loop, the stdout pipeline-event logger, and
   `log_pipeline_event` move to `app.rs`. Trade-off: `main.rs` becomes a ~55-line
   file that no longer explains the system; the explanation moves to the `app.rs`
   module doc, so a reader follows exactly one in-repo link to find startup.

4. **`Pipeline::registry` becomes private** (`pipeline.rs:170`, `pub registry` →
   `registry`). Its only reader outside `pipeline.rs` is `main.rs:131`
   (`Arc::clone(&pipeline.registry)` for `ControlPlane::new`). Verified:

   ```
   rg -n "pipeline\.registry" src/          # -> src/main.rs:131 only
   rg -n "pub registry" src/pipeline.rs     # -> line 170
   ```

   `App::build` already owns the `Arc<EngineRegistry>` it passes to
   `Pipeline::new`, so it passes `Arc::clone(&registry)` to `ControlPlane::new`
   instead. Trade-off: none functional; the registry stops being reachable
   through a service that does not own its lifecycle.

5. **No behaviour change.** Same six tasks, same subscription order, same spawn
   order, same abort-on-Ctrl-C (no graceful shutdown), same log lines with the
   same fields and levels, same `HEARTBEAT_INTERVAL` (5 s) and missed-tick
   policy, same startup ordering — in particular `TradeExecutor::new` still runs
   before any task starts, and the `Manifold client ready` line still logs from
   the same point in startup. §6 justifies this claim line by line.

6. **Be honest about the test surface.** `App::build` needs a live Manifold key
   and LLM/Firecrawl env, so there is no new integration test. One small test was
   considered and rejected as not genuinely testable: `Tasks::run` awaits
   `tokio::signal::ctrl_c()`, and a test cannot complete that future without
   either a real SIGINT (kills the harness) or a shutdown parameter on `run`
   (a design change decision 5 forbids). Recorded as a residual in §9; the gate
   is that behaviour is unchanged (§6) plus the greps (§10). No fake is invented.

7. **Keep the crate-level `#![allow(dead_code)]` in `main.rs`** (`15-17`) where it
   is. It is an inner attribute at the crate root, so it covers `app.rs` and
   every other module. Moving it into a module would change what it covers and
   risk a wave of unrelated warnings.

### Resolutions inside those seven

These are the local choices the seven leave open. Each is fixed here so the
implementer decides nothing.

- **The post channel is created in `spawn`.** `ingest::channel()` and
  `post_rx = post_tx.subscribe()` are four lines apart today (`144-145`); both
  move into the same subscription block, so a sender is never created in one
  function and subscribed in another.

- **`DiscordState` is built in `spawn`, not stored on `App`.** Decision 1's field
  list is the composed *services*; `discord::DiscordState` is an adapter value
  derived from five of them. It is constructed from clones at the top of the task
  section of `spawn`, exactly where `main.rs:136-142` builds it today.

- **`App::spawn` destructures `self` with `..`.** `let App { config, db, store,
  pipeline, executor, control, .. } = self;` drops the two retained handles
  (`registry`, `onboarding`) on purpose, with a comment saying so. Every service
  already holds its own clone (`Pipeline` and `ControlPlane` hold the registry;
  `ControlPlane` holds the onboarding `Arc<dyn Onboarder>`), so the drop is
  unobservable.

- **`Tasks` has six named `JoinHandle<()>` fields** — `pipeline`, `heartbeat`,
  `trade`, `ingest`, `discord`, `event` — and no collection. `run` aborts them in
  today's order.

- **`onboarding` reaches `ControlPlane::new` through a typed binding.** An `as`
  cast cannot perform the `Arc<OnboardingService> → Arc<dyn Onboarder>` unsized
  coercion, so `build` writes
  `let onboarder: Arc<dyn Onboarder> = Arc::clone(&onboarding);` and passes
  `onboarder`. That is what lets `App` keep the concrete handle that decision 1
  requires.

- **`main` chains the three calls** on one line:
  `App::build(Config::from_env()?).await?.spawn().run().await`, under a two-line
  comment. No `let` bindings for `App` or `Tasks`, so no intermediate value can be
  dropped before `run` (a dropped `Tasks` would detach the tasks instead of
  aborting them).

## 3. The model

### Task lifecycle after the change

| Task (binding in `App::spawn`) | Spawned in | Owns | Ends when |
|---|---|---|---|
| `pipeline` | `app.rs` `App::spawn` | `post_rx` (`ExtractedPost`), dispatch loop | Ctrl-C (`Tasks::run` aborts) |
| `heartbeat` | `app.rs` `App::spawn` | 5 s ticker, `processed_posts` deltas | Ctrl-C |
| `trade` | `app.rs` `App::spawn` | `trade_events` (`PipelineEvent`), 60 s sync ticker inside `TradeExecutor::run` | Ctrl-C |
| `ingest` | `app.rs` `App::spawn` | WS connection, `post_tx` | Ctrl-C |
| `discord` | `app.rs` `App::spawn` | gateway client, both Discord receivers, handlers | gateway error (logged, task exits) / Ctrl-C |
| `event` | `app.rs` `App::spawn` | `pipeline_events` (`PipelineEvent`), stdout logging | channel closed / Ctrl-C |
| per-market worker | `pipeline.rs` `spawn_worker` (lazily) | `TriageJob` receiver, queue depth | sender dropped / Ctrl-C |
| Discord pipeline consumer | `discord.rs` `spawn_pipeline_consumer` (inside `discord::run`) | `PipelineEvent` receiver, embeds | channel closed / gateway stop |
| Discord trade consumer | `discord.rs` `spawn_trade_consumer` (inside `discord::run`) | `TradeEvent` receiver, embeds | channel closed / gateway stop |

`Tasks` owns the six tasks in the first six rows and nothing else. The last three
rows stay owner-spawned, as today: `App` is a composition root, not a task
registry (§9).

### Subscription order, preserved exactly

`App::spawn` performs these five subscriptions in this order, in one block,
before its first `tokio::spawn`:

| # | Receiver | Created from | Consumed by | Today in `main.rs` |
|---|---|---|---|---|
| 1 | `pipeline_events: broadcast::Receiver<PipelineEvent>` (bound `mut`) | `pipeline.events().subscribe()` | stdout logger task (`event`) | `105` |
| 2 | `trade_events: broadcast::Receiver<PipelineEvent>` | `pipeline.events().subscribe()` | `TradeExecutor::run` (`trade`) | `108` |
| 3 | `discord_pipeline_events: broadcast::Receiver<PipelineEvent>` | `pipeline.events().subscribe()` | `discord::run` | `111` |
| 4 | `discord_trade_events: broadcast::Receiver<TradeEvent>` | `executor.events().subscribe()` | `discord::run` | `124` |
| 5 | `post_rx: broadcast::Receiver<ExtractedPost>` | `ingest::channel()` then `post_tx.subscribe()` | `Pipeline::run` (`pipeline`) | `144-145` |

`sender`/`receiver` mapping:

| Broadcast sender | Capacity | Receivers after the change |
|---|---|---|
| `ingest::channel()` (`ExtractedPost`) | 1024 | `post_rx` |
| `Pipeline::events()` (`PipelineEvent`) | 1024 | rows 1, 2, 3 |
| `TradeExecutor::events()` (`TradeEvent`) | 256 (`trade.rs:82` `EVENT_CAPACITY`) | row 4 |

Ordering claim, stated precisely: all five receivers exist before the first
`sender.send` can run. No producer task is started until every `subscribe()` has
returned, and construction (`build`) publishes nothing — `Pipeline::new` only
creates the channel, and `TradeExecutor::new` only performs `GET /v0/me`. Rows
1-3 are now created after `TradeExecutor::new` returns instead of before it; that
window contains no sender activity in either arrangement (§6 item 3).

## 4. Interface

### `src/app.rs`

```rust
//! Composition root, broadcast subscriptions, and task lifetime.
//!
//! [`App::build`] constructs every shared `Arc` and service in dependency order.
//! [`App::spawn`] creates every broadcast receiver and then starts the long-running
//! tasks. [`Tasks::run`] waits for Ctrl-C and aborts them.
//!
//! # The rule this module exists to enforce
//!
//! `tokio::sync::broadcast` keeps no history: a receiver created after a sender
//! has published does not see that event. Every subscription must therefore exist
//! before the first `tokio::spawn`. `App::spawn` is the only function in the crate
//! that calls `subscribe()`, and it does so in one block before its first spawn.
//! A new channel adds its sender and its receiver in that same block.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::config::Config;
use crate::control::{ControlPlane, Onboarder};
use crate::db::Db;
use crate::discord::{self, DiscordState};
use crate::error::{AppResult, Context};
use crate::ingest;
use crate::market_store::MarketStore;
use crate::onboarding::OnboardingService;
use crate::pipeline::{Pipeline, PipelineEvent};
use crate::sieve::{self, EngineRegistry};
use crate::trade::TradeExecutor;
use crate::types::MarketStatus;

/// How often the INFO heartbeat reports throughput and active markets.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// The composed process state, built once in dependency order.
pub struct App {
    config: Arc<Config>,
    db: Db,
    store: Arc<MarketStore>,
    registry: Arc<EngineRegistry>,
    onboarding: Arc<OnboardingService>,
    pipeline: Arc<Pipeline>,
    executor: Arc<TradeExecutor>,
    control: Arc<ControlPlane>,
}

impl App {
    /// Builds every shared `Arc` and service, and loads the Sieve engines.
    ///
    /// Failure here aborts startup: a bad key, a missing database, or an
    /// unreachable Manifold must not leave a half-running process.
    pub async fn build(config: Config) -> AppResult<App>;

    /// Subscribes every broadcast receiver, then starts the six tasks.
    ///
    /// The two blocks are ordered on purpose and must stay in this function: see
    /// the module doc.
    pub fn spawn(self) -> Tasks;
}

/// The six long-running tasks this process owns.
pub struct Tasks {
    pipeline: JoinHandle<()>,
    heartbeat: JoinHandle<()>,
    trade: JoinHandle<()>,
    ingest: JoinHandle<()>,
    discord: JoinHandle<()>,
    event: JoinHandle<()>,
}

impl Tasks {
    /// Runs until Ctrl-C, then aborts every task and returns.
    ///
    /// This is not a graceful shutdown: no handle is awaited, and in-flight work
    /// is discarded, exactly as before.
    pub async fn run(self) -> AppResult<()>;
}

/// Prints pipeline events to stdout.
///
/// Moved verbatim from `main.rs`: same match arms, same fields, same levels.
fn log_pipeline_event(event: &PipelineEvent);
```

`App` and `Tasks` have private fields, so nothing outside `app.rs` can construct
`App`, inspect `Tasks`, or reach a service handle. `pub` on the types and methods
is required only because `main` is a sibling module.

### What stays in `main.rs`

```rust
//! Process entry point for the Bluesky-to-Manifold trading bot.
//!
//! Setup only: install the rustls crypto provider, configure `tracing`, load
//! [`config::Config`], then hand the process to [`app::App`]. The composition,
//! the broadcast subscriptions, and the task lifetime live in [`app`].

// Several module APIs are exercised by unit tests or reserved for the Discord
// layer that is wired in later.
#![allow(dead_code)]

mod app;
mod belief;
mod book;
// ... the other 15 `mod` lines, unchanged ...

use crate::app::App;
use crate::config::Config;
use crate::error::{anyhow, AppResult};

#[tokio::main]
async fn main() -> AppResult<()> {
    // Serenity compiles with the rustls `ring` provider while rig's reqwest
    // stack adds `aws-lc-rs`, so rustls cannot pick one automatically. Pin
    // `ring` here, before any TLS handshake.
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|error| anyhow!("installing the default rustls crypto provider: {error:?}"))?;

    init_tracing();

    // Composition and task ownership live in `app`: `build` wires every service,
    // `spawn` subscribes every receiver before it starts any task.
    App::build(Config::from_env()?).await?.spawn().run().await
}

/// Configures `tracing` from `RUST_LOG`, defaulting to `info`.
fn init_tracing() {
    // verbatim, `239-246`
}
```

`main.rs` keeps imports `anyhow`, `AppResult`, `Config`, `App`; it drops
`Arc`, `Duration`, `Instant`, `broadcast`, `ControlPlane`, `Db`, `MarketStore`,
`OnboardingService`, `Pipeline`, `PipelineEvent`, `TradeExecutor`, `MarketStatus`
and `Context`. Unused imports are errors under
`clippy -- -D warnings`, so this list is part of the change, not a cleanup.

## 5. Code moves

| `main.rs` at `cfc8509` | Destination | Treatment |
|---|---|---|
| `1-13` module doc (startup narrative) | `app.rs` module doc | rewritten: the narrative gains the subscribe-before-spawn rule and loses the stale "Discord is not started yet" sentence; `main.rs` keeps a 5-line setup doc |
| `15-17` `#![allow(dead_code)]` + comment | stays | unchanged (decision 7) |
| `19-35` `mod` list | stays | add `mod app;` as the first entry |
| `37-50` imports | split | `app.rs` gets the service imports (§4); `main.rs` keeps four |
| `52-53` `HEARTBEAT_INTERVAL` | `app.rs` | verbatim, `pub`-less `const` |
| `60-62` rustls install | stays | unchanged |
| `64` `init_tracing()` call, `239-246` its body | stays | unchanged |
| `66` `Arc::new(Config::from_env()?)` | `App::build` | `pub async fn build(config: Config)` takes the value; the body starts `let config = Arc::new(config);` |
| `67-68` `Db::open`, `MarketStore::new` | `App::build` | verbatim |
| `70-88` registry build + compile loop + `loaded Sieve engines` | `App::build` | verbatim; `sieve::EngineRegistry` becomes the imported `EngineRegistry` |
| `92-97` `OnboardingService::new` | `App::build` | verbatim |
| `99-104` `Pipeline::new` | `App::build` | one change: the registry argument is `Arc::clone(&registry)` instead of a moved `registry`, because `App` keeps the handle |
| `113-123` `TradeExecutor::new` + `Manifold client ready` | `App::build` | verbatim, still before any task starts |
| `126-134` `ControlPlane::new` | `App::build` | `Arc::clone(&pipeline.registry)` → `Arc::clone(&registry)`; `onboarding` → `onboarder` binding (see §2); `executor.clone()` unchanged |
| `105`, `108`, `111`, `124`, `144-145` subscriptions | `App::spawn` | same order, one block, new single comment |
| `136-142` `DiscordState` | `App::spawn` | verbatim, built from clones before the spawns |
| `147-222` six `tokio::spawn` blocks | `App::spawn` | same order, same bodies; `post_rx`, `trade_events`, `discord_*`, `pipeline_events` are the locals from the subscription block |
| `224-227` Ctrl-C + `shutting down` | `Tasks::run` | verbatim |
| `229-234` six `abort()` | `Tasks::run` | same order: pipeline, heartbeat, trade, ingest, event, discord |
| `236` `Ok(())` | `Tasks::run` | verbatim |
| `248-325` `log_pipeline_event` | `app.rs` | verbatim, including doc comment |
| `pipeline.rs:170` `pub registry` | `pipeline.rs` | `pub` removed; no other line in the file changes |

Deleted from `main.rs`: the 171-line body of `main` (`66-236`). It is re-expressed,
not discarded, in three functions: `App::build` (`66-134` plus the `Arc<Config>`),
`App::spawn` (`105-222`), and `Tasks::run` (`224-236`). Deleted from the crate:
nothing — no function, type, constant, or log line leaves the process. What is
gone is the *shape*: one 171-line function that mixed construction, channel
wiring, task ownership, and shutdown.

Visibility change: only `Pipeline::registry`. `Pipeline::new` keeps taking
`Arc<EngineRegistry>`, and `pipeline.rs:260` still reads `self.registry`, so the
field is not dead.

## 6. Behaviour ledger

"Unchanged" is a claim, so each row names the check.

| # | Claim | Evidence after the change | Verdict |
|---|---|---|---|
| 1 | Six tasks | six `tokio::spawn` in `App::spawn` (`rg -c "tokio::spawn" src/app.rs` → 6); owner-spawned workers and Discord consumers untouched (`pipeline.rs`, `discord.rs` unmodified) | identical |
| 2 | Five broadcast receivers | five `subscribe()` calls in the subscription block, in today's order 105 → 108 → 111 → 124 → 144-145 | identical |
| 3 | No event can be missed | every `subscribe()` returns before the first `spawn`; `Pipeline::new`/`TradeExecutor::new` send nothing (they only build a sender and do `GET /v0/me`) | identical |
| 4 | Spawn order | pipeline, heartbeat, trade, ingest, discord, event — copied in today's order | identical |
| 5 | Abort order | pipeline, heartbeat, trade, ingest, event, discord — copied in today's order, including the fact that `event` precedes `discord` | identical |
| 6 | Abort semantics | `JoinHandle::abort` then return; no handle is awaited; `main` returns and the runtime drops | identical: no graceful shutdown, no `select!` |
| 7 | Log lines | `loaded Sieve engines` (`markets`, `active`), `Manifold client ready` (`bot_user_id`), `heartbeat` (`messages_per_sec`, `messages_processed`, `active_markets`), `shutting down`, the four `error!` texts (`pipeline stopped`, `trade executor stopped`, `ingest stopped`, `discord stopped`), the `warn!` `pipeline event consumer lagged` (`dropped`), and every arm of `log_pipeline_event` | copied verbatim; same level, same field names, same order |
| 8 | Log *sequence* | build emits engines then client-ready; `spawn` emits nothing; `run` emits `shutting down`. No line is added or removed between them | identical |
| 9 | Heartbeat timing | `HEARTBEAT_INTERVAL` 5 s, `MissedTickBehavior::Delay`, first-tick skip before `last_at` | verbatim body |
| 10 | Startup failure | same `?`/`.context("initializing the trade executor")` chain, now inside `build`; a failure returns before `spawn`, so no task ever starts | identical message, identical point |
| 11 | Ctrl-C failure | `.context("waiting for Ctrl-C")` in `Tasks::run` | identical |
| 12 | Rustls provider installed before any TLS | `main` installs it before `App::build`, which is the first thing that can make a request | identical |
| 13 | One `Arc<Config>` | `build` wraps the value once, as `main.rs:66` did | identical |
| 14 | `post_tx`/`post_rx` | created in `spawn`; the sender moves into the ingest task, the receiver into the pipeline task | identical |
| 15 | `Pipeline::registry` visibility | one keyword removed; no reader changes behaviour | no runtime change |
| 16 | `App`'s retained `registry`/`onboarding` handles | dropped at the top of `spawn`; `Pipeline` and `ControlPlane` hold their own clones | no observable change |
| 17 | `main.rs` module doc | rewritten (it wrongly says "Discord is not started yet") | documentation only |

Anything not in this table must behave identically; §10 and the three gates are
the enforcement.

## 7. Tests

**No new tests.** The reasoning, so it is reviewable rather than assumed:

- `App::build` cannot be exercised in a unit test. It takes a `Config` by value, so a
  test could build one, but construction still performs three real dependencies:
  `TriageClient::new` reads `OPENROUTER_API_KEY` from the process environment
  (process-wide, therefore racy with the other tests in this crate),
  `TradeExecutor::new` performs `GET /v0/me` against Manifold, and
  `OnboardingService::new` builds a Firecrawl client from the config. Stubbing
  those three constructors is a design change outside this plan's scope.
- `Tasks::run` cannot be tested honestly. Its only input is
  `tokio::signal::ctrl_c()`. Completing that future in-process needs either a real
  SIGINT (which terminates the test harness) or a shutdown future injected into
  `run`, which changes the signature and the semantics this plan promises to keep
  (decision 5, and the "no graceful-shutdown machinery" constraint). Once
  `ctrl_c()` resolves, the only assertion left is that `abort()` was called on
  handles the test created — which tests `JoinHandle::abort`, not this code.
- `log_pipeline_event` only writes to `tracing`; there is no sink to assert on
  without adding a subscriber harness used by no other test in the crate.

Residual: the subscribe-before-spawn rule is pinned by review, the module doc,
and the §10 greps — not by a test. The test census must not change:
`rg -o '(#\[test\]|#\[tokio::test\])' src/ | wc -l` is **275** at `cfc8509` and must
still be 275 after the change.

## 8. Commit sequence

Two commits. No intermediate commit has two composition roots.

1. `refactor(app): own composition and task lifecycle`
   - Add `src/app.rs`: `App`, `Tasks`, `App::build`, `App::spawn`, `Tasks::run`,
     `HEARTBEAT_INTERVAL`, `log_pipeline_event`, and the module doc.
   - Rewrite `src/main.rs` to process setup only; add `mod app;`.
   - Remove `pub` from `Pipeline::registry` (`src/pipeline.rs:170`).
   - One commit, because a two-step switch is not available here: `main.rs` is
     either the composition root or it is not, and the visibility change must land
     with the commit that deletes its only external reader. Splitting it would
     leave a commit where `app.rs` exists but nothing calls it and `main.rs` still
     owns the ordering — two composition roots, one dead.
   - Gates: all three. `main.rs` and `app.rs` must be rustfmt-clean under
     `--config skip_children=true`.

2. `docs: record the app composition root`
   - `docs/ARCHITECTURE.md`: §1 task-inventory `Spawned in` cells for the six
     tasks become `app.rs` (`App::spawn`); the §1 "no single composition root
     object" paragraph is rewritten (the root is now `app::App`, and modules still
     take `Arc`s through constructors, so the "any module can hold any other
     module's state" caveat stands); §2 graph gets `APP["app.rs"]` with
     `MAIN --> APP` and `APP --> ALL`; §6 property bullet becomes "`app::App::spawn`
     subscribes all receivers before the first `tokio::spawn`"; §7J marked
     **resolved** with its residuals.
   - `AGENTS.md:71-72`: "Wire new long-running loops in `src/app.rs` and abort
     them on Ctrl-C" — plus one sentence that every subscription goes in
     `App::spawn`.
   - `docs/SPEC.md:53`: layout line becomes `main.rs # process setup: rustls,
     tracing, config` plus a new `app.rs # composition root: build, subscribe,
     spawn, run`. (The current text says "graceful shutdown", which was already
     wrong; this removes the error rather than repeating it.)
   - `docs/OVERVIEW.tex:80`: "spawns five tasks (ingest, pipeline, trade executor,
     Discord, stdout logger)" → "`app.rs` spawns six tasks (ingest, pipeline,
     trade executor, Discord, console logger, heartbeat)". Regenerate
     `docs/OVERVIEW.pdf` with
     `cd docs && latexmk -pdf -interaction=nonstopmode OVERVIEW.tex && latexmk -c`
     (verified that this builds cleanly at `cfc8509`), and commit both files. If
     the build fails, commit the `.tex` fix alone and say so in the message.
   - Do not touch `docs/plans/*.md`; the reviewer commits this plan.

Run the gates after every commit:

```sh
cargo build --release
cargo test
cargo clippy -p manif0ld --all-targets --no-deps -- -D warnings
rustfmt --edition 2021 --check --config skip_children=true src/app.rs src/main.rs
```

`cargo fmt --check` is dirty at HEAD for
`belief.rs`, `config.rs`, `db.rs`, `discord.rs`, `ingest.rs`,
`onboarding_agent.rs`, `pipeline.rs`, `sieve.rs`, `triage.rs` and
`sieve/src/engine.rs`. Do not run `cargo fmt` or `rustfmt` on any of them. The
acceptance is that the dirty-file list is byte-identical before and after (§10).

## 9. Risks and residuals

- **`App` is not unit-testable without live credentials.** Accepted: the same
  three constructors that block a test today (a process-environment key, a
  Manifold round trip, a Firecrawl client) still block one after the change.
  Nothing in this plan improves coverage, and it should not claim to.
- **`main.rs` shrinks to ~55 lines, so a reviewer may think something was lost.**
  Mitigation: the `app.rs` module doc carries the startup narrative, the ordering
  rule, and the six-task table; commit 1's message points at it; §5 is the
  line-by-line move list.
- **`Tasks` must not become a generic task registry.** Six named
  `JoinHandle<()>` fields, no `Vec`, no `push`, no `add`. A seventh task is added
  by adding a field and an `abort()` in `run` — never by iterating a collection,
  because that would make the abort order implicit.
- **Ctrl-C semantics must not change.** No graceful shutdown is introduced: no
  `select!`, no `JoinSet::join_all`, no `CancellationToken`, no awaiting a
  handle after abort. If an implementer is tempted to "improve" shutdown, that
  is a different plan and changes in-flight behaviour.
- **The runtime is still one tokio runtime.** `App::spawn` calls `tokio::spawn`,
  so it must be called inside `#[tokio::main]`; `main` remains the only caller. No
  `Handle::current()` juggling, no second runtime, no blocking pool.
- **Residual: the ordering rule has no test.** See §7. The gate is §6 plus §10.
- **Residual: `App` retains two handles nothing reads** (`registry`,
  `onboarding`). Deliberate per decision 1; they are the composition's owned
  references and their drop is documented at the top of `spawn`. A future reader
  must not treat `App` as a service locator — its fields are private and
  `spawn` consumes it, so it cannot be.
- **Residual: `pipeline.rs:232` becomes stale.** The doc comment on
  `Pipeline::run` says "the heartbeat in `main` reports as throughput"; the
  heartbeat is now in `app.rs`. This plan keeps the allowed `pipeline.rs` edit to
  the one visibility keyword, so the wording is left for a docs-only follow-up
  (the phrase becomes "the heartbeat in `app`"). It is a comment, not a claim the
  compiler checks, so it cannot fail a gate.
- **Residual: `discord::run` still spawns its own two consumers**, and
  `pipeline.rs` still spawns one worker per market. `App` does not own them, and
  the §3 table says so; anyone adding a task must decide which of the two owners
  it belongs to.
- **`#![allow(dead_code)]` hides a real mistake here.** It already does: it is
  what lets an unused `App` field or an unused `Tasks` method compile. The greps
  in §10 are the counterweight.
- **`OVERVIEW.pdf` is a tracked binary.** Regenerating it makes a large diff with
  no reviewable content; the `.tex` diff is the reviewable part.

## 10. Acceptance

Structural greps (each must hold after commit 1):

```sh
# main.rs no longer orchestrates
rg -n "subscribe\(\)" src/main.rs            # no output
rg -n "tokio::spawn" src/main.rs            # no output
rg -n "\.abort\(\)" src/main.rs             # no output
rg -n "pipeline\.registry" src/             # no output
rg -n "pub registry" src/pipeline.rs        # no output

# app.rs owns exactly the six tasks and the five receivers
rg -n "pub async fn build|pub fn spawn|pub async fn run" src/app.rs   # 3 lines
rg -c "subscribe\(\)" src/app.rs            # 5
rg -c "tokio::spawn" src/app.rs             # 6
rg -c "\.abort\(\)" src/app.rs              # 6
rg -n "let (pipeline|heartbeat|trade|ingest|discord|event)_task" src/app.rs   # 6 lines

# main.rs is process setup only
rg -n "fn main" -A 16 src/main.rs           # rustls install, init_tracing, App::build(..).spawn().run()
rg -c "^mod " src/main.rs                   # 18 (17 existing + app)
rg -n "^mod app;" src/main.rs               # 1 line
rg -n "#!\[allow\(dead_code\)\]" src/       # src/main.rs only
rg -n "HEARTBEAT_INTERVAL|fn log_pipeline_event|loaded Sieve engines|Manifold client ready|\"shutting down\"|pipeline event consumer lagged" src/
                                            # every hit in src/app.rs, none in src/main.rs
wc -l src/main.rs                           # 70 or fewer (expected ~55)

# tests unchanged
rg -o '(#\[test\]|#\[tokio::test\])' src/ | wc -l   # 275, same as cfc8509

# only one line of pipeline.rs changed
git diff --stat -- src/pipeline.rs          # 1 insertion(+), 1 deletion(-)
```

Gates:

```sh
cargo build --release
cargo test
cargo clippy -p manif0ld --all-targets --no-deps -- -D warnings
```

Formatting — the recursion gotcha: `rustfmt --check src/main.rs` also checks the
whole `mod` tree (`rc=1` at HEAD because of `belief.rs`), which makes it useless
as a signal. Use:

```sh
rustfmt --edition 2021 --check --config skip_children=true src/app.rs src/main.rs   # exit 0
```

and confirm the `cargo fmt --check` dirty set is unchanged, i.e. exactly these ten
paths and no others:

```sh
cargo fmt --check 2>&1 | grep '^Diff in' | sed 's/:.*//' | sort -u
# src/belief.rs src/config.rs src/db.rs src/discord.rs src/ingest.rs
# src/onboarding_agent.rs src/pipeline.rs src/sieve.rs src/triage.rs
# sieve/src/engine.rs
```

Behavioural confirmation (manual, needs `.env`): one run shows the same startup
lines in the same order (`loaded Sieve engines`, `Manifold client ready`), the
5 s `heartbeat` line, and on Ctrl-C the `shutting down` line followed by process
exit with no drain. No load test is required: the task set, the channel set, and
the abort order are unchanged by construction and by the greps above.
