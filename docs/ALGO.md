# Optimal Trading Algorithm Design (v3 proposal)

Status: **proposal**, not yet implemented. `docs/SPEC.md` stays the source of
truth for what is built. This document is the design for the next
architecture.

## 1. Assets and constraints

| Asset | Property |
|---|---|
| Bluesky Jetstream | Every post, free, unlimited. **~55 posts/s observed.** |
| Decision model | Typed answers (noul / choice / score) in a few ms. |
| Manifold REST API | 500 requests/minute per IP. Shared across everything. |
| Manifold websocket | `wss://api.manifold.markets/ws`. Free, no rate limit. **Measured ~2 msg/s total.** |

The single most important finding from reading the full API: **the websocket
makes almost all REST reads unnecessary.** The current 60 s per-market
polling loop (`GET /v0/market/{id}` + `/positions`) spends the rate budget to
buy 60-second staleness. The websocket gives the same data in real time for
free:

- `global/new-bet` — every bet on the platform, with `userId`, size, outcome.
- `global/new-contract` — every new market the moment it exists.
- `global/updated-contract` — every market update (probability included).
- `contract/{id}/orders` — the live limit-order book per market.
- `contract/{id}/new-bet`, `/new-answer`, `/updated-answers` — per-market stream.
- `contract/{id}/user-metrics/{userId}` — our own position, pushed live.

REST stays only for: writes (bets, sells, cancels), startup snapshots,
slow reconciliation, and one-shot discovery queries.

## 2. Rate-limit budget (500/min)

With websocket reads, steady-state REST usage is small:

| Class | Calls | Budget |
|---|---|---|
| Trade writes (bet/sell/cancel) | event-driven, hard-capped | ≤ 60/min |
| Startup + restart snapshots (`/v0/bets?kinds=open-limit`, `/v0/me`) | a few per boot | ≤ 20/min |
| Reconcile: `GET /v0/market-probs` (100 ids per call) | 1 call per 100 markets per 5 min | ≤ 20/min |
| Reconcile: `GET /v0/get-user-contract-metrics-with-contracts` (whole portfolio in 1 call) | 1 per 5 min | negligible |
| Discovery sweeps (`/v0/search-markets`) | a few per hour | negligible |
| **Total steady state** | | **< 40/min** |

Implementation: one global token bucket (`rate.rs`) with priority classes.
Trades always win; reconcile and discovery yield when the bucket is low.
Headroom exists on purpose: a breaking-news burst must never queue behind
housekeeping.

## 3. The strategy stack

Five strategies, ranked by expected value per unit of complexity. They share
one belief/execution core.

### S1. News-latency trading (existing core — keep, fix execution)

Jetstream → Sieve prefilter → one decision call → log-odds belief → edge
trade. This stays. The weaknesses are in execution, not in signal:

**Current flaw:** buys use `limitProb=0.99` (YES) / `0.01` (NO). On a thin
book this sweeps the AMM far past fair value and hands back the edge.
**Fix:** cap the sweep at the bot's own fair price:

```
limitProb(YES) = p_fair − ε      limitProb(NO) = 1 − p_fair + ε
```

The bot can never fill worse than its own belief. Unfilled remainder becomes
a resting order with `expiresMillisAfter = 60_000`, so it either gets picked
off (free passive fill) or dies without a cancel call.

**Stale-limit pickoff (new, high value):** `contract/{id}/orders` keeps a
local mirror of the resting limit book (`book.rs` already exists — extend it
to mirror other people's orders from the websocket, snapshot on boot with
`GET /v0/bets?contractId={id}&kinds=open-limit`). When triage fires a
material signal, execution order is:

1. **Take stale limits first.** Resting orders on the wrong side of fresh
   news are free options: fill them before their owners cancel. Zero price
   impact, better-than-AMM price.
2. **Then sweep the AMM** with the fair-capped `limitProb` above for the
   Kelly-remainder.

This execution ladder — *stale limits → capped AMM sweep → expiring rest* —
is the core execution algorithm for every strategy below.

### S2. New-market sniping (highest alpha per decision call)

`global/new-contract` pushes every new market to us instantly. New markets
are the most mispriced objects on Manifold: the creator sets `initialProb`
by gut, and early liquidity sits at that price until informed money arrives.
Speed wins; the first informed trader takes the whole gap.

Algorithm per `new-contract` event (BINARY and MCQ only):

1. Ignore if `totalLiquidity < MIN_SNIPE_LIQ` (can't size in) or the market
   is a POLL/BOUNTY/Numeric (skip types we don't model).
2. One decision call — a `score` question, the model's native strength:

   > state: {question, close_time, initial_prob, creator_hint}
   > question: "What is the probability (0–100) that this resolves YES?"

3. `gap = p_model − p_initial`. If `|gap| ≥ SNIPE_MIN_GAP` (default 0.10),
   take it through the S1 ladder at size `λ · |gap| · budget`, capped per
   snipe (`SNIPE_MAX_MANA`, small — new markets are thin and creators can
   resolve oddly).

This inverts the current "no autonomous discovery" invariant deliberately:
the operator asked for discovery. Guardrail: sniping runs with its own small
global budget pool, separate from `/track`ed markets, and every snipe posts
to Discord like any trade.

### S3. Smart-money copy-trading (second independent alpha stream)

`global/new-bet` shows every bet with `userId` the moment it lands. The
portfolio endpoints make trader skill measurable:

- `GET /v0/get-user-portfolio?userId=` → live profit, balance.
- `GET /v0/get-user-portfolio-history?userId=&period=allTime` → track record.
- `GET /v0/get-user-contract-metrics-with-contracts?userId=&order=profit` →
  per-market profit (domain expertise).
- `GET /v0/leagues?season=N` → current-season standings.

Algorithm:

1. **Leaderboard (`traders.rs`).** Refresh a scored watch-list of the top
   ~200 traders hourly (≈200 cheap reads/hour, well inside budget). Score =
   blend of all-time profit, recent (weekly) profit, and bet count.
2. **Trigger.** On `global/new-bet`: if `userId ∈ watch-list` and
   `amount ≥ COPY_MIN_SIZE`, start a follow check within the same second —
   latency is the whole game, the market is still digesting their trade.
3. **Gate (one decision call, `noul`):**

   > state: {trader_stats, market question + fresh prob, bet size vs
   > market volume, seconds since their fill}
   > question: "Is this bet likely to be informed?"

   Fast, and filters celebrity-size noise bets and league-farming patterns.
4. **Follow** through the S1 ladder if the residual edge survives: their bet
   already moved the price, so require `|p_follow − p_fair_est| ≥ threshold`
   where `p_fair_est` extrapolates their move (a top trader's size-on bet
   historically carries edge; follow only the un-consumed part). Size:
   half-Kelly of the S1 formula, per-market and global daily copy caps.
5. **Exit:** same edge-decay rule as S1, using the copied trader's exit as
   an additional signal — if they sell, our belief impulse decays faster.

### S4. Sum-to-one arbitrage (risk-free, competitive, do it cheaply)

On `cpmm-multi-1` markets (`shouldAnswersSumToOne=true`), probabilities are
kept near Σ=1 by Manifold's internal arb, but large bets and new answers open
transient windows where Σ answer prices < 1. Buying equal shares of every
answer then pays 1 mana on the winner for < 1 mana of cost.

Algorithm:

1. Watch set: `GET /v0/search-markets?contractType=DEPENDENT_MULTIPLE_CHOICE
   &sort=24-hour-vol&filter=open&limit=200` hourly (1 request).
2. Subscribe `contract/{id}/updated-answers` for each. On every update
   compute Σp from the local mirror.
3. If `Σp < 1 − ARB_MARGIN` (margin covers fees + fill risk): size the
   basket so post-trade Σ ≈ 1 — binary-search the `amount` against the
   answer pool parameters already in the market object — then fire one
   `POST /v0/multi-bet` over all answer ids. One request, one atomic basket.
4. `POST /v0/market/{id}/rebalance` is a free accounting op that collapses
   mixed YES/NO positions on these markets; run it after arbs and after S1
   exits on sum-to-one markets to redeem locked value.

Expected value per window is small but risk-free; the cost is one websocket
subscription and one write when it fires.

### S5. Passive market making on tracked markets (optional, last)

When belief edge exists but is below the taker threshold, rest an expiring
limit order at fair value instead of doing nothing. We earn the spread when
crossed; `expiresMillisAfter` (30–120 s) means no cancel churn when belief
moves. Keep this off until S1–S4 are measured; churning orders is the only
strategy here that can eat the rate budget.

### Rejected: perp oracle-latency arb

Perp orders are queued market orders that fill at the *next* oracle
observation — every API trader gets the same print. There is no stale price
to pick off through the API. Skip perps.

## 4. Decision-model question set

The triage four-question call stays exactly as specced (`is_evidence`,
`supports_yes`/`supported_answer`, `evidence_strength`, `already_priced`).
Add two more calls, each used by one strategy:

| ID | Type | Used by | Wording sketch |
|---|---|---|---|
| `fair_prob` | score 0–100 | S2 sniping | "Probability this market resolves YES?" (per-answer variant for MCQ: `choice` over answers + score) |
| `is_informed_bet` | noul | S3 copy | "Is this trader's bet on this market likely informed?" |

All three calls are few-ms, so the total model budget stays trivial: one
call per surviving post, one per new market, one per large smart-money bet.

## 5. Architecture changes

New modules, same single binary / single tokio runtime:

```
src/
  mfd_ws.rs    Manifold websocket: subscribe, mirror probs/orders/positions,
               reconnect; emits MarketEvent into the pipeline
  rate.rs      Global 500/min token bucket, priority classes
  book.rs      EXTEND: mirror of resting limit orders per market (ws + boot snapshot)
  traders.rs   Leaderboard scoring + copy-trade engine (S3)
  snipe.rs     New-market fair-value sniper (S2)
  arb.rs       Sum-to-one arb watcher + multi-bet sizing (S4)
```

Changed modules:

- `trade.rs` — execution ladder (stale limits → fair-capped sweep → expiring
  rest); `limitProb` derived from belief, never 0.99/0.01; `rebalance` after
  MCQ trades.
- `app.rs` — spawn `mfd_ws`, `traders`, `snipe`, `arb` loops; delete the
  per-market 60 s REST polling; keep a slow batched reconcile
  (`/v0/market-probs`, 100 ids/call) as websocket backstop.
- `pipeline.rs` — consume `MarketEvent`s (price jumps, smart-money bets,
  new contracts) alongside Bluesky posts; all five strategies converge on
  the same edge→ladder executor.
- `db.rs` — tables: `watch_markets` (S4 set), `trader_scores`,
  `snipe_ledger`, `copy_ledger`, `arb_ledger`. Tracked-market schema
  unchanged.

Invariants kept: per-market isolated budgets; budget invariant asserted
before every buy; Discord-only control plane; triage wording stays in
`triage.rs`; Sieve matching stays in-process.

Invariants changed (operator requested): autonomous discovery is now in
scope (S2/S3/S4), and limit orders are now in scope (S1 ladder, S5).

## 6. Strategy abstraction (modularity)

The five strategies are one pattern with five signal sources. Make that
explicit: a `Strategy` trait, one event bus, one intent type, one shared
executor. Strategies never touch Manifold, the rate limiter, the database,
or Discord directly — they emit *intents*, and the shared core enforces
every invariant.

```rust
/// One unified event stream: Jetstream + Manifold ws + timers.
pub enum Event {
    Post(Arc<ExtractedPost>),      // Jetstream
    Bet(BetEvent),                 // global/new-bet
    Contract(NewContractEvent),    // global/new-contract
    Answers(UpdatedAnswersEvent),  // contract/{id}/updated-answers
    Orders(OrdersEvent),           // resting-book delta
    Price(PriceEvent),             // updated-contract probability
    Tick(u64),                     // periodic timer
}

/// What a strategy asks the core to do.
pub enum Intent {
    /// "Answer X is worth p" → belief engine → edge check → ladder.
    /// Multiple strategies' beliefs on one market COMPOSE in the
    /// belief engine instead of competing for trades.
    Belief {
        market: MarketId,
        answer: Option<AnswerId>,
        p: f64,
        weight: f64,           // e.g. trader score for S3
        ttl: Duration,         // belief impulse half-life override
    },
    /// "Run this exact plan" (arb basket, rebalance) — executor
    /// validates budgets and risk, then runs it atomically.
    Action(ActionPlan),
}

#[async_trait]
pub trait Strategy: Send + Sync {
    fn name(&self) -> &'static str;
    /// Event filter, so posts are not cloned into the arb module.
    fn interest(&self) -> Interest; // bitflags over Event kinds
    async fn on_event(&self, ev: &Event, ctx: &Ctx, out: &mut Vec<Intent>);
}
```

The dispatcher fans events out by `interest()`. The **executor** is the
only writer to Manifold:

```
events ──► dispatcher ──► strategies ──► intents ──► Executor
                                       (provenance)   │ budget invariant
                                                       │ kill switch / dry-run
                                                       │ Kelly sizing
                                                       │ rate-bucket priority
                                                       │ execution ladder
                                                       ▼
                                                    Manifold
```

Design wins:

1. **Invariants live in one place.** Budget caps, the kill switch, dry-run,
   and the audit ledger cannot be bypassed by a buggy new strategy, because
   no strategy can place a bet itself.
2. **Beliefs compose.** S1 says 0.70 on a market, S3's copy signal implies
   0.65 — the belief engine merges the impulses and *one* trade decision
   comes out. Two strategies firing simultaneously on the same market can
   never double-bet; the edge check runs on merged state.
3. **Provenance is free PnL attribution.** Every intent carries
   `{strategy, evidence}` (post URI / bet id / contract id). It flows into
   `trade_ledger` and the Discord embed, so a per-strategy scoreboard —
   "which strategy actually makes mana?" — is a query, not a project. That
   scoreboard later feeds `weight` auto-tuning.
4. **Priority is data.** Latency-critical intents (stale-limit pickoff,
   arb) declare the highest rate-bucket class; housekeeping declares the
   lowest. The bucket arbitrates.
5. **Adding strategy N+1 = one file + one registration + one config flag.**
   No changes to belief, sizing, execution, rate limiting, or Discord.

Migration: today's `pipeline.rs` (extract → sieve → triage → belief)
becomes `NewsStrategy` — Sieve is its internal prefilter, triage its
judgment call, and it emits `Intent::Belief`. `pipeline.rs` shrinks to the
dispatcher. Per-strategy risk caps (e.g. the snipe pool) are executor-side
config keyed by `strategy.name()`.

### 6.1 Channel topology

Yes — channels, but at the rate boundaries, not everywhere. Measured scale
(2026-11-06, 2-minute live probe of all five global topics, Fri evening EET;
single sample — re-probe before sizing anything hard):

| Stream | avg rate | peak 15 s | payload | notes |
|---|---|---|---|---|
| Bluesky posts | ~55/s | — | — | the dominant stream, ~25× everything else |
| `global/new-bet` | 1.3 msg/s | ~3.1/s | ~1.8 KB/msg | **`data.bets` is an array** (batched); includes unfilled limit orders (`limitProb`, `orderAmount`, `expiresAt`) — this one stream can mirror the resting book globally |
| `global/updated-contract` | 0.5 msg/s | ~1.7/s | ~3.3 KB/msg | carries the full contract incl. `answers[].prob` — price updates need no REST |
| `global/new-contract` | 0 in 2 min | — | — | rare (hours scale) — every snipe is precious: never-drop is correct |
| `global/new-comment` | 0 in 2 min | — | — | — |
| `global/new-subsidy` | 0 in 2 min | — | — | — |

Totals: ~1.8 msg/s average, ~5 msg/s peak, ~4 KiB/s. Trivial. One task can
consume every topic we will ever subscribe to; channel caps of 256–1024 are
generous everywhere. The real bottleneck of the whole system was never
throughput — it is the decision-call count, and at 55 posts/s the Sieve
prefilter keeps that at a trickle.

The system has two rates: the post stream (~55/s) and everything else
(~2 events/s, ~1 intent/s). Channels sit exactly where the rate changes:

```
Jetstream ──► broadcast<Post>  (cap 1024, drop-oldest) ──┐
Manifold ws ──► mpsc<MfdEvent> (cap 1024, never drop) ───┤
timers ──────────────────────────────────────────────────┤
                                                         ▼
                                            dispatcher task (select!,
                                            interest filter, Arc<Event>)
              ┌──────────────┬──────────────┬────────────┴──────────┐
              ▼              ▼              ▼                       ▼
        news mpsc      snipe mpsc     copy mpsc               arb mpsc
        (drop-oldest)  (never drop)   (never drop)            (never drop)
              │              │              │                       │
              ▼              ▼              ▼                       ▼
        NewsStrategy   SnipeStrategy  CopyStrategy            ArbStrategy
              └──────────────┴──────┬───────┴───────────────────────┘
                                    ▼
                  intent mpsc ×2 (priority, normal)  ◄── strategies
                                    ▼
                    CORE task (single consumer, biased to priority):
                    apply Belief → edge check → ladder → Manifold
                    = the ONLY writer to Manifold, and the ONLY
                      mutator of belief/position state → no locks
                                    ▼
                             ledger + Discord
```

Rules:

1. **One core task, single consumer.** All intents flow through two mpsc
   queues (`priority` for stale-limit pickoffs and arb, `normal` for the
   rest) into one task. It owns belief state, position state, the rate
   bucket, and the Manifold client. Sequential draining means no `RwLock`
   on hot state, no double-bet when two strategies fire together, and the
   kill switch is literally "stop draining".
2. **Drop policy is per-channel, by preciousness:**
   | Channel | Policy | Why |
   |---|---|---|
   | posts broadcast | drop-oldest + `warn!` on lag | freshness beats completeness; stale news is toxic |
   | mfd events | never drop; large cap, alert if full | every bet/contract/answer event is rare and tradeable |
   | news inbox | drop-oldest | inherits the firehose policy |
   | snipe/copy/arb inboxes | never drop; alert if full | each item is one decision call + maybe one trade |
   | intent queues | bounded 256; full ⇒ kill-switch alert | intents are ≤ a few/s; full means a bug |
3. **State is not a channel.** The mirrored world — current probs, resting
   order books, our positions (`market_store.rs`, extended) — is read-mostly
   shared state: `Arc<RwLock<..>>` written by the ws task, read by
   strategies for context (e.g. triage needs `market_probability`).
   Channels carry *work*; `Arc`/`watch` carries *state*. Do not stream state
   snapshots through mpsc.
4. **Strategies never await Manifold.** They push an intent and move on;
   the core task serializes all writes against the rate bucket. A slow
   strategy can only clog its own inbox, never the bus, never the executor.
5. **Subscriptions in `App::spawn`.** All channels are created and all
   broadcast/mpsc receivers are handed out in `App::spawn`, before the first
   `tokio::spawn` — the existing convention, unchanged.

What does *not* get a channel: Sieve matching stays a synchronous in-process
call inside `NewsStrategy`'s task (microseconds), and the per-market triage
queue stays the bounded queue it already is. Channels are for crossing task
boundaries where rates or latencies differ — nothing smaller earns one.

## 7. Rollout order

1. **`mfd_ws` + `rate` + reconcile swap** — kill 60 s polling. Pure win, no
   strategy change.
2. **S1 execution ladder** — fair-capped `limitProb` + stale-limit pickoff.
   Raises EV of every existing trade.
3. **S2 sniping** — one decision call per new market; smallest code, highest
   marginal alpha.
4. **S3 copy-trading** — leaderboard + gated follow.
5. **S4 arb** — cheap, risk-free, run it.
6. **S5 market making** — only if the bucket still has headroom.

Each phase ships behind a config flag (`STRAT_SNIPE_ENABLED` etc.) and posts
its trades to Discord like any `/track`ed market, so the operator keeps one
control plane and one audit trail.
