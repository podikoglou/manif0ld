# Optimal Trading Algorithm Design (v3 proposal)

Status: **proposal**, not yet implemented. `docs/SPEC.md` stays the source of
truth for what is built. This document is the design for the next
architecture.

## 1. Assets and constraints

| Asset | Property |
|---|---|
| Bluesky Jetstream | Every post, free, unlimited. Zero cost to listen wider. |
| Decision model | Typed answers (noul / choice / score) in a few ms. |
| Manifold REST API | 500 requests/minute per IP. Shared across everything. |
| Manifold websocket | `wss://api.manifold.markets/ws`. Free, no rate limit. |

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

## 6. Rollout order

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
