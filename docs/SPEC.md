# Technical Specification: `manif0ld` (v2, trimmed)

**Scope:** Personal Bluesky → Manifold auto-trader. The core value is **filtering**: turning the Jetstream firehose into a small number of "this matters for market X" posts, and acting on them. Everything else is minimal infrastructure to make that useful.

## 1. Purpose & Invariants

1. **Filter-first.** Sieve + one decision-model call per surviving post is the product. If the filter works, the bot works.
2. **Auto-trading is in scope, but simple.** Fair-capped limit buys (§6.6) whose unfilled remainder rests with an expiry. Enter once per market per direction on strong edge, exit once on edge decay. No partial scaling, no stop-loss regimes, no client-placed standing orders.
3. **Per-market isolated budgets.** Each market has `budget_mana`. Never invest more than `budget_mana` net in a market. Realized profits cycle back into that market's available budget only.
4. **No autonomous *news* market discovery.** Operator markets enter only via
   `/track` (or onboarding). The `snipe` and `copy` strategies may create
   `tracked_markets` rows autonomously, but each row is strategy-tagged and
   drawn from its own isolated pool (`SNIPE_POOL_MANA`, `COPY_POOL_MANA`), and
   both flags (`SNIPE_ENABLED`, `COPY_ENABLED`) default to `false`.
5. **No calibration, no self-healing.** The onboarding LLM writes one Sieve rule. If it compiles, it's live. If the operator doesn't like it, they `/untrack` and `/track` again with a fresh prompt.
6. **Discord is the only control plane.**
7. **Text extraction ≠ text normalization.** Our pipeline extracts the searchable text from a Jetstream event (post body + image alt-texts + quoted text). It does **not** lowercase, strip punctuation, expand leetspeak, or apply word-boundary logic. All lexical normalization (case folding, NFKC, zero-width stripping, leetspeak, morphological expansion, implicit word boundaries) is performed by the Sieve engine internally, per the DSL spec in §7. We pass raw extracted text into Sieve and let the engine handle the rest.

## 2. Architecture

```
Jetstream WS ──► post extractor ──► EngineRegistry (N markets, N CompiledSieve)
                                          │
                                          ├─ per-market Sieve match
                                          ▼
                                    Triage queue (bounded per market)
                                          │  baseten/joey-1 via OpenRouter decisions
                                          ▼
                                    {p_evidence, p_target, strength, p_priced}
                                          │  assess() → impulse scale if material
                                          ▼
                                    Belief update (log-odds w/ time decay)
                                          │
                                          ▼
                                    Trade check → Manifold /v0/bet | /sell
                                          │
                                          ▼
                                    Discord thread notification
```

**Flow per post:** extract → loop 5 Sieve engines → if match, enqueue → single decisions call → belief update → trade check.

**Single binary, single tokio runtime, single process.** No worker threads for Sieve (in-process, ~microseconds), no batching (LLM calls are ~1/s at most), no separate services.

## 3. Repository Layout

Single Rust crate:

```
manif0ld/
├── Cargo.toml
├── .env.example
├── README.md
├── migrations/
│   └── 001_init.sql
└── src/
    ├── main.rs          # process setup: rustls, tracing, config
    ├── app.rs           # composition root: build, subscribe, spawn, run
    ├── config.rs        # env loading, constants
    ├── error.rs         # anyhow/thiserror aliases
    ├── types.rs         # domain types: Market, ExtractedPost, Trade, Belief, etc.
    ├── db.rs            # rusqlite schema + queries
    ├── extract.rs       # Jetstream event → ExtractedPost (structural extraction only)
    ├── ingest.rs        # Jetstream websocket, reconnect, live-tail subscribe
    ├── sieve.rs         # EngineRegistry + per-market CompiledSieve wrapper
    ├── llm.rs           # chat transport for onboarding (rig providers)
    ├── triage.rs        # OpenRouter decisions call, question wording, impulse fold
    ├── belief.rs        # log-odds state, decay, impulse
    ├── trade.rs         # Manifold client, Kelly sizing, order manager
    ├── snipe.rs         # S2 new-market sniper (strategy + snipe ledger)
    ├── copy.rs          # S3b copy-trader (gated follow + copy ledger)
    ├── strategy_market.rs # shared market-row builder for autonomous strategies
    ├── traders.rs       # S3a leaderboard scoring and the shared watch list
    ├── manifold_ws.rs   # Manifold broadcast websocket client
    ├── book.rs          # positions, planner, Kelly, execution ladder
    ├── rate.rs          # global priority token bucket
    ├── onboarding.rs    # fetch market, prepare/activate a draft
    ├── onboarding_agent.rs # research agent: Firecrawl + Sieve submission
    ├── pipeline.rs      # extract → match → triage → belief → trade orchestration
    └── discord.rs       # bot, commands, embeds, thread management
```

Key dependencies: `tokio`, `tokio-tungstenite`, `serde`, `serde_json`, `reqwest`, `rusqlite`, `serenity`, `tracing`, `anyhow`, `chrono`, `sieve` (path dependency).

`extract.rs` is split out from `ingest.rs` deliberately: it's the only place where we touch the raw event structure, and it makes it obvious from the module name that we're extracting fields, not normalizing text.

## 4. Configuration (`.env.example`)

```ini
# Bluesky Jetstream
JETSTREAM_URL=wss://jetstream2.us-east.bsky.network/subscribe

# Cloudflare AI Gateway / Workers AI
# The gateway URL ends with the account ID. The gateway ID is sent in the
# cf-aig-gateway-id header.
CF_AI_GATEWAY_URL=https://gateway.ai.cloudflare.com/v1/{account_id}
CF_AI_GATEWAY_ID=manif0ld
CF_API_KEY=...

# OpenRouter
# The same key serves triage (decisions endpoint) and an openrouter/ chat model.
OPENROUTER_API_KEY=...
TRIAGE_MODEL=openrouter-decisions/baseten/joey-1       # decision model
ONBOARDING_MODEL=@cf/deepseek/deepseek-v4.1-flash-1017 # chat model, tool calling
TRIAGE_MIN_EVIDENCE=0.7                                # probability of evidence gate

# Manifold
MANIFOLD_API_KEY=...
# The bot user ID is fetched from GET /v0/me at startup.
# Live prices come from the Manifold broadcast websocket; REST is demoted to
# a slow batched reconcile.

# Discord
DISCORD_BOT_TOKEN=...
DISCORD_GUILD_ID=...
DISCORD_CONTROL_CHANNEL_ID=...
DISCORD_OPERATOR_USER_ID=...

# Trading (defaults, overridable per market via /track)
DEFAULT_MARKET_BUDGET=100
KELLY_FRACTION=0.25        # effective fraction is clamped to 0.5 by the planner
ENTRY_EDGE_THRESHOLD=0.05
EXIT_EDGE_THRESHOLD=0.01
MIN_TRADE_SIZE=1.0

# Belief engine
BELIEF_KAPPA=1.1
BELIEF_HALFLIFE_HOURS=4.0

# S2 new-market sniping (off by default)
SNIPE_ENABLED=false
SNIPE_POOL_MANA=100        # dedicated global pool, separate from per-market budgets
SNIPE_MAX_MANA=10          # per-snipe cap (clamped to the pool remaining at use time)
SNIPE_MIN_LIQ=100          # minimum totalLiquidity
SNIPE_MIN_GAP=0.10         # minimum |p_model - p_initial|
SNIPE_KELLY_FRACTION=0.25  # effective fraction is clamped to 0.5
SNIPE_TTL_SECS=3600        # eligibility window from contract creation, [1, 604800]
SNIPE_DECISION_TIMEOUT_SECS=2

# S3 smart-money leaderboard (read-only; copy execution has its own flag)
TRADERS_ENABLED=true
TRADER_WATCH_SIZE=200      # [10, 1000]
TRADER_MIN_BETS=25         # activity gate
TRADER_REFRESH_MIN=60      # [5, 1440]
TRADER_REQUEST_BUDGET=120  # hard cap per refresh; 2 + ceil(N/100) + 2 per enriched trader

# S3b copy-trading execution (off by default)
COPY_ENABLED=false
COPY_MIN_SIZE=5            # minimum source-bet size in mana
COPY_MIN_INFORMED=0.60     # is_informed_bet gate, (0, 1)
COPY_MIN_EDGE=0.05         # residual edge after their move, (0, 1)
COPY_KELLY_FRACTION=0.125  # (0, 1]; effective fraction is clamped to 0.5
COPY_MAX_MANA=10           # per-copy cap
COPY_POOL_MANA=100         # global pool for open copy positions
COPY_DAILY_MANA=30         # rolling 24 h cap on executed copy buys
COPY_MAX_AGE_SECS=30       # [1, 300] source-bet age bound
COPY_DECISION_TIMEOUT_SECS=2  # [1, 30] per decision call

# Storage
DB_PATH=./data/manif0ld.db
```

**Note on the transport.** Triage never calls a chat endpoint. The decisions
call is:

```
POST https://openrouter.ai/api/alpha/decisions
Authorization: Bearer {OPENROUTER_API_KEY}
Content-Type: application/json

{"model": "baseten/joey-1",
 "state": {...},
 "questions": {"is_evidence": {"type": "noul", ...}, ...}}
```

The reply is `{"answers": {"<id>": {"type": "noul"|"choice"|"score", ...}},
"usage": {"input_tokens", "output_tokens", "cost"}}`. Onboarding still goes
through a chat model; when that model is Cloudflare, the call is
`POST /accounts/{account_id}/ai/run` with the `cf-aig-gateway-id` header, as
before.

## 5. Data Model (SQLite)

```sql
CREATE TABLE tracked_markets (
  id                   TEXT PRIMARY KEY,       -- Manifold contractId
  slug                 TEXT NOT NULL,
  question             TEXT NOT NULL,
  description          TEXT NOT NULL,
  resolution_criteria  TEXT NOT NULL,
  close_time           INTEGER,
  status               TEXT NOT NULL,          -- 'ACTIVE' | 'PAUSED' | 'CLOSED'
  budget_mana          REAL NOT NULL,
  invested_mana        REAL NOT NULL DEFAULT 0,
  realized_pnl_mana    REAL NOT NULL DEFAULT 0,
  yes_shares           REAL NOT NULL DEFAULT 0,
  no_shares            REAL NOT NULL DEFAULT 0,
  p_internal_log_odds  REAL NOT NULL,          -- L_t
  p_market_last        REAL NOT NULL,
  belief_updated_at    INTEGER NOT NULL,
  sieve_source         TEXT NOT NULL,
  discord_thread_id    TEXT NOT NULL,
  created_at           INTEGER NOT NULL,
  deleted_at           INTEGER,           -- NULL while tracked; set by /untrack
  strategy             TEXT NOT NULL DEFAULT 'news' -- 'news' | 'snipe' | 'copy'
);

CREATE TABLE signal_ledger (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  market_id      TEXT NOT NULL,
  post_uri       TEXT NOT NULL,
  author_handle  TEXT NOT NULL,
  post_text      TEXT NOT NULL,
  material       INTEGER NOT NULL,
  direction      TEXT NOT NULL,          -- display bucket derived from impulse
  strength       INTEGER NOT NULL,       -- rounded expected rubric position
  impulse        REAL NOT NULL,          -- applied log-odds delta
  p_after        REAL NOT NULL,
  p_evidence     REAL,                   -- p the post carried evidence
  p_target       REAL,                   -- mass on the favored alternative
  strength_real  REAL,                   -- expected rubric position, 0..3
  p_priced       REAL,                   -- p the market already knew this
  created_at     INTEGER NOT NULL
);

CREATE TABLE trade_ledger (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  market_id       TEXT NOT NULL,
  action          TEXT NOT NULL,          -- 'BUY_YES'|'BUY_NO'|'SELL_YES'|'SELL_NO'
  mana_amount     REAL NOT NULL,
  shares_delta    REAL NOT NULL,
  p_internal      REAL NOT NULL,
  p_market        REAL NOT NULL,
  trigger_post_uri TEXT,
  limit_prob      REAL,                   -- migration 009: wire limitProb of a buy
  manifold_bet_id TEXT,
  created_at      INTEGER NOT NULL
);

CREATE TABLE snipe_ledger (                -- migration 007, one row per S2 evaluation
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  contract_id      TEXT NOT NULL,
  question         TEXT NOT NULL,          -- snapshot at evaluation time
  p_initial        REAL,                   -- creator's initialProb
  p_model          REAL,                   -- NULL when the decision call failed
  gap              REAL,                   -- p_model - p_initial
  answer           TEXT,                   -- chosen answer text for MCQ
  action           TEXT NOT NULL,          -- 'BUY_YES' | 'BUY_NO' | 'SKIP'
  reason           TEXT,                   -- skip reason or buy rationale
  mana_amount      REAL NOT NULL DEFAULT 0,
  limit_prob       REAL,
  manifold_bet_id  TEXT,
  cost_usd         REAL,
  created_at       INTEGER NOT NULL
);

CREATE TABLE kv (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);  -- 'global_kill_switch'

CREATE TABLE trader_scores (               -- migration 008, S3a watch list
  user_id          TEXT PRIMARY KEY,       -- Manifold user ID
  username         TEXT,                   -- NULL when the bulk lookup missed it
  profit_all_time  REAL,                   -- deposit-adjusted, mana
  profit_weekly    REAL,                   -- trailing seven days, mana
  bet_count        INTEGER,                -- sampled bets, capped at TRADER_MIN_BETS
  score            REAL,                   -- §6.10 formula
  updated_at       INTEGER NOT NULL
);

CREATE TABLE copy_ledger (                 -- migration 008, S3b (§6.11)
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  trader_id        TEXT NOT NULL,
  contract_id      TEXT NOT NULL,
  question         TEXT NOT NULL,
  bet_id           TEXT,                   -- the copied bet
  bet_amount       REAL,                   -- size of the copied bet
  p_before         REAL,
  p_after          REAL,
  p_informed       REAL,                   -- decision-model gate output
  fair_prob        REAL,
  gap              REAL,
  action           TEXT NOT NULL,          -- 'BUY_YES' | 'BUY_NO' | 'SKIP' | 'EXIT_SIGNAL'
  reason           TEXT,
  mana_amount      REAL NOT NULL DEFAULT 0,
  manifold_bet_id  TEXT,
  created_at       INTEGER NOT NULL
);
```

### 5.1 Market lifecycle and soft deletion

- `ACTIVE` / `PAUSED` / `CLOSED` describe trading: a `CLOSED` market keeps its
  row but no longer trades.
- `deleted_at` describes tracking: `NULL` means the market is tracked. Setting
  it (via `/untrack`) also forces `status = 'CLOSED'`.
- `strategy` names the component that owns the row: `'news'` for `/track`ed
  and onboarded markets, `'snipe'` for rows created by S2 new-market sniping,
  `'copy'` for rows created by S3b copy-trading. Both autonomous strategies
  hide their rows from operator listings but not from the executor.
  Operator listings (`/status`, autocomplete) show only `'news'`; the trade
  executor and reconciliation keep every active row, and a snipe market has no
  Sieve rule and no Discord thread (its embeds fall back to the control
  channel).
- A soft-deleted row, its `market_answers`, and its `signal_ledger` /
  `trade_ledger` history stay in the database. Every listing query
  (`list_markets`, `list_markets_by_status`) skips it, so it leaves the market
  cache, `/status`, autocomplete, and `/panic` sweeps.
- `get_market` still returns soft-deleted rows, so stale Discord embeds and a
  repeated `/untrack` report "already untracked" instead of "not tracked".
- Only re-onboarding is a hard delete: when `/track` targets a market whose
  existing row is `CLOSED` (including a soft-deleted one), activation removes
  that row and its answers before inserting the fresh lifecycle. The ledgers
  keep the old history.

## 6. Components

### 6.1 Jetstream Ingestion (`ingest.rs`)

- Connect to `JETSTREAM_URL` with `wantedCollections=app.bsky.feed.post`.
- Filter each event: keep only `kind == "commit"` and `commit.operation == "create"`. Discard the rest.
- Hand the raw event to `extract.rs` (§6.2) to produce an `ExtractedPost`.
- **Always subscribe at the live tail.** Never send a `cursor` parameter and never replay. Reconciliation is not worth it: the belief update uses the wall clock (`Utc::now()`), so a burst of old posts skips the decay that belongs between them and moves belief away from a price that already contains that news. A missed post costs one trade; a replayed post can produce a false edge and a bad trade.
- On reconnect: exponential backoff (1s → 2s → 4s → ... → 60s cap). Posts published while the socket is down are lost by design.
- Emit each `ExtractedPost` to a bounded broadcast channel (capacity ~1024). If the channel is full, drop oldest and log a rate-limited `warn!`.

`ingest.rs` does no text processing beyond event filtering. All structural extraction lives in `extract.rs`.

### 6.2 Post Extraction (`extract.rs`)

`extract.rs` turns a raw Jetstream event into a domain `ExtractedPost`. It performs **structural field extraction only** — it does not touch character content of the extracted strings.

```rust
pub struct ExtractedPost {
    pub uri: String,
    pub did: String,
    pub handle: String,
    /// Concatenation of: post body, image alt-texts, and quoted post body.
    /// Raw text as-is; no case folding, punctuation stripping, or Unicode normalization.
    pub text: String,
    pub created_at: i64,
}

pub fn extract(event: &serde_json::Value) -> Option<ExtractedPost> {
    // Skip events that aren't commit/create app.bsky.feed.post
    // ...
    let mut parts: Vec<&str> = Vec::new();
    if let Some(body) = event["commit"]["record"]["text"].as_str() {
        parts.push(body);
    }
    if let Some(images) = event["commit"]["record"]["embed"]["images"].as_array() {
        for img in images {
            if let Some(alt) = img["alt"].as_str() {
                if !alt.is_empty() { parts.push(alt); }
            }
        }
    }
    if let Some(quoted) = event["commit"]["record"]["embed"]["record"]["value"]["text"].as_str() {
        parts.push(quoted);
    }
    let text = parts.join(" ");
    if text.trim().is_empty() { return None; }
    Some(ExtractedPost {
        // Jetstream commit events omit `commit.uri` and carry only `commit.rkey`.
        // Build the canonical AT URI when the explicit field is absent.
        uri: match event["commit"]["uri"].as_str() {
            Some(uri) => uri.to_string(),
            None => format!(
                "at://{}/{}/{}",
                event["did"].as_str()?,
                event["commit"]["collection"].as_str()?,
                event["commit"]["rkey"].as_str()?
            ),
        },
        did: event["did"].as_str()?.to_string(),
        handle: /* from account cache, or fallback to DID */,
        text,
        created_at: (event["time_us"].as_i64()? / 1_000_000) as i64,
    })
}
```

**Jetstream event shape.** Real `commit` events contain `did`, `time_us`, `kind`, and a
`commit` object with `rev`, `operation`, `collection`, `rkey`, `record`, and `cid`.
They do **not** contain `commit.uri`; the canonical `at://{did}/{collection}/{rkey}`
URI must be constructed. An explicit `commit.uri`, if a future event ever carries one,
is preferred verbatim.

**Explicitly not done here** (and belongs to Sieve, §7):

| Not done in `extract.rs` | Handled by Sieve internally |
|---|---|
| Lowercasing / case folding | Implicit case-insensitivity per literal (or `!` to opt out) |
| Unicode NFKC normalization | Implicit per Sieve's lexical defaults |
| Zero-width joiner / space stripping | Implicit |
| Leetspeak expansion (`4` → `a`, `1` → `i`, etc.) | `~` modifier on a literal |
| Morphological stemming (`-s`, `-ed`, `-ing`) | `+` modifier on a literal |
| Word-boundary detection | Implicit per literal (except `*`-prefixed / `*`-suffixed wildcards) |
| Punctuation stripping | Implicit through word-boundary logic |
| Quote extraction | Done here (structural) |
| Alt-text extraction | Done here (structural) |

If the spec ever needs to change extracted text (e.g., start including link-preview card titles), it goes in `extract.rs`. If the spec ever needs to change how text is *matched*, it does not go here — it goes into the Sieve rule or the LLM prompt.

### 6.3 Sieve Matching (`sieve.rs`)

```rust
pub struct EngineRegistry {
    engines: RwLock<HashMap<MarketId, CompiledSieve>>,
}

impl EngineRegistry {
    pub fn register(&self, id: MarketId, engine: CompiledSieve);
    pub fn unregister(&self, id: MarketId);
    /// Returns market IDs whose Sieve rule matched.
    pub fn matching_markets(&self, text: &str) -> Vec<MarketId>;
}
```

- One `CompiledSieve` per market, compiled via `CompiledSieve::compile(&source)`.
- **No pre-processing of `text` before passing it to `.matches()`.** Sieve applies its own lexical normalization internally (case folding, NFKC, leetspeak expansion, word boundaries, morphological expansion) per its DSL. Duplicating any of that here would break case-sensitive `!` literals and double-apply expansions.
- The matching loop reads from `RwLock` and calls `.matches(text).is_empty()` per engine. 5 engines × ~1000 posts/s = 5000 `.matches()` calls/s. Trivial for Sieve.
- Hot-swap on `/track`, `/untrack` = insert/remove from the map. `std::sync::RwLock` is fine at this scale.

### 6.4 Decision Model Triage (`triage.rs`)

One decisions call per matched post. No batching, no tiering.

Triage is a **System One / decision model** (`baseten/joey-1` via OpenRouter),
not a chat model. It never generates text. It reads a `state` object and four
typed questions in one forward pass, and returns calibrated probabilities.
OpenRouter serves this on `POST /api/alpha/decisions`; the crate calls it
through the vendored `rig::typesafeai` surface with
`TRIAGE_MODEL=openrouter-decisions/baseten/joey-1`.

**State (shared by all questions):**

```json
{
  "market": {"question": "...", "resolution_criteria": "...",
              "close_time": "RFC 3339 or unknown", "market_probability": 0.63},
  "post":   {"author_handle": "@x", "author_followers": 1234,
              "posted_at": "RFC 3339", "text": "..."},
  "answers": [{"text": "...", "market_probability": 0.25}]
}
```

`market.description` is deliberately absent: it is long and it sways the
evidence judgment. `answers` is present only for multiple-choice markets.

**Questions:**

| ID | Type | Result |
|---|---|---|
| `is_evidence` | `noul` | p that the post carries genuine evidence. Binary markets ask about the market question; multiple-choice markets ask **about any of the market's answers**, because the question text there is often a bare title. |
| `supports_yes` (binary) / `supported_answer` (MCQ) | `choice` | the favored alternative and its full distribution. The MCQ choice set always includes one extra labeled alternative, **"None of the market's answers"**: choosing it means the post bears on no tracked answer and no belief moves (`Target::NoAnswer` is never material). This is what keeps a genuine but unrelated race call from leaking an impulse onto a random answer. |
| `evidence_strength` | `score` | expected position on the 4-level rubric (0 noise … 3 definitive) |
| `already_priced` | `noul` | p that the market price already reflects it |

JSON parsing is gone by design. A missing question key, an unknown answer
type, or a non-finite probability fails the call, and a triage failure leaves
belief untouched.

**From probabilities to an impulse:** `assess()` folds them into one signed
scale in `[-1, 1]`:

```
direction  = (p_target - 1/N) / (1 - 1/N)     /  N = number of alternatives
novelty    = 1 - p_priced
scale      = p_evidence * direction * novelty * (strength / 3)
```

On an MCQ market, `p_target` is renormalized over the tracked answers only:
mass the model places on the abstain alternative is a "supports no answer"
judgment, not mass against the chosen alternative, and without the
renormalization it would flip the direction of the chosen one. Answer mass
that never names a choice fails the call closed.

A post moves *no* belief when `p_evidence < TRIAGE_MIN_EVIDENCE` (default
0.7), when `strength < 0.5`, when the direction is a draw (uniform mass), or
when the fold lands inside `0.01`. Direction and strength in the signal
ledger are display buckets derived from the scale, never inputs to belief
arithmetic.

**Threshold:** `TRIAGE_MIN_EVIDENCE` is the only knob. Log
`p_evidence`, `p_target`, `strength_real` and `p_priced` per post
(`signal_ledger` columns) so the threshold can be tuned against resolution
outcomes later.

**Rate limit:** Bound the per-market triage queue at 30 in-flight. If a market
is queue-saturated, drop oldest and log `warn!`. Surface this count in
`/status` per market. A decisions call times out after 10 seconds and sends
again on the queue's next turn; nothing is retried inside the call.

**`fair_prob` (S2):** `TriageClient::fair_prob` is a second decisions call, one
per newly created market, for new-market sniping. It asks the probability that
the market resolves YES and returns `{p, answer, cost_usd}`. The state is
`{question, resolution_criteria, close_time, initial_prob, creator_id?}`, plus
the answer texts on a multiple-choice market. It has no market history and no
current price, because at creation `initial_prob` **is** the price.
`resolution_criteria` is the key input, and `description` is absent.

ALGO §4 sketches `fair_prob` as a score from 0 to 100. The vendored
`rig::typesafeai` `Score` accepts only 2 to 10 levels, so a 101-level score is
not expressible. The call uses a 10-level **decile rubric** instead ("between
0% and 10%", …, "between 90% and 100%") and maps the model's expected
zero-based band `s` to the band midpoint, `p = (s + 0.5) / 10`, clamped to
`[0.01, 0.99]`. The strategy sees the same probability either way.

On a multiple-choice market the one call sends `supported_answer`, a `choice`
over the answer texts plus **"None of the market's answers"**, and
`answer_probability`, the same decile rubric for the probability that the
supported answer wins. The abstain choice returns `answer: None`, and the
strategy must not snipe then.

### 6.5 Belief Engine (`belief.rs`)

Log-odds with exponential decay toward market price. No credibility multipliers, no claim clustering, no slow-path consolidation.

```rust
// Persisted state: p_internal_log_odds (L_t), belief_updated_at
pub fn current_p_internal(
    l_stored: f64,
    belief_updated_at: i64,
    p_market_now: f64,
    now: i64,
) -> f64 {
    let l_market = (p_market_now / (1.0 - p_market_now)).ln();
    let dt_hours = (now - belief_updated_at) as f64 / 3600.0;
    let decay = 0.5_f64.powf(dt_hours / BELIEF_HALFLIFE_HOURS);
    let l = l_market + (l_stored - l_market) * decay;
    (1.0 / (1.0 + (-l).exp())).clamp(0.01, 0.99)
}

pub fn apply_scaled(
    l_stored: &mut f64,
    belief_updated_at: &mut i64,
    impulse_scale: f64,     // [-1, 1], from triage::assess
    p_market_now: f64,
    now: i64,
    kappa: f64,
    halflife_hours: f64,
) {
    let decayed = decayed_log_odds(*l_stored, *belief_updated_at, p_market_now, now, halflife_hours);
    let impulse = kappa * impulse_scale;
    *l_stored = (decayed + impulse).clamp(-4.595, 4.595);
    *belief_updated_at = now;
}
```

**Decay behavior:** Belief pulls toward the *current market price* with a 4-hour half-life. If the market catches up to the bot's belief → edge disappears → trade exits via edge-decay rule. If the bot was wrong and the market moves away → belief also pulls toward the market → trade exits. This is the only stop-loss mechanism. It's simple, automatic, and hard to get wrong.

**Manual override:** `/override market probability:X` sets `p_internal_log_odds = logit(X)` and updates `belief_updated_at = now`. It does not write `p_market_last`: that column is written by the trade sync only, never by triage or override.

### 6.6 Trade Execution (`trade.rs`)

**Market kinds.** `tracked_markets.outcome_type` is `BINARY` or
`MULTIPLE_CHOICE` and is refreshed from `GET /v0/market/{id}` on every
evaluation. A multiple-choice market trades one position per `market_answers`
row; `should_answers_sum_to_one` records whether the answers compete for one
winner (`cpmm-multi-1`) or are independent binaries (`cpmm-multi-2`).

Both kinds run through one planner (`book::Book::plan`): the binary/multiple-
choice differences — the entry price band, the per-answer budget fraction, and
the sum-to-one projection — are `TradeLimits` data selected by the market shape,
not separate code paths. The planner opens entries only on operator `news`
rows. A `snipe` or `copy` row is **exits only**: its entries are owned by the
strategy, sized against the isolated pool, and executed through
`TradeExecutor::execute_planned`, so a refused or unfilled strategy order can
never be re-entered by the periodic evaluation as a news trade.

**Sizing** (fractional Kelly, λ = `KELLY_FRACTION`, capped at 50% of the
relevant budget):

```
Edge = P_internal − P_market

If Edge ≥ ENTRY_EDGE_THRESHOLD (bullish YES):
    f = λ · Edge / (1 − P_market)
If Edge ≤ −ENTRY_EDGE_THRESHOLD (bullish NO):
    f = λ · |Edge| / P_market

f = clamp(f, 0, 0.5)
TargetMana = f × available_budget
available_budget = budget_mana + realized_pnl_mana − invested_mana
```

For an answer entry the target is additionally capped at
`MC_MAX_ANSWER_FRACTION × budget_mana`.

**Binary decision (called after each belief update, on every debounced
websocket price update, and after every 5-minute reconcile pass over all
active markets on the cached state):**

```
edge = p_internal − p_market

if holding YES:
    if edge < EXIT_EDGE_THRESHOLD:  → SELL_YES (100% of shares)
if holding NO:
    if edge > −EXIT_EDGE_THRESHOLD: → SELL_NO (100% of shares)

if no position and edge > ENTRY_EDGE_THRESHOLD and target_mana >= MIN_TRADE_SIZE:
    → BUY_YES(target_mana)
if no position and edge < −ENTRY_EDGE_THRESHOLD and target_mana >= MIN_TRADE_SIZE:
    → BUY_NO(target_mana)
```

**Multiple-choice decision (same material-signal trigger):**

1. **Sync answers.** New answers are inserted with
   `p_internal = logit(price)`; existing rows get a price-only update. Belief
   columns belong to triage and are never overwritten by a sync.
2. **Sync positions.** One
   `GET /v0/market/{id}/positions?userId={bot}` call returns a root summary row
   plus one row per answer. The root row is ignored for accounting and used as
   a checksum: root invested and share totals must equal the sum of the answer
   rows, otherwise log `warn!` and trust the answer rows. For an MCQ market
   `tracked_markets.invested_mana` is the sum of the answer rows.
3. **Edges.**
   - Independent (`shouldAnswersSumToOne = false`):
     `edge_i = p_internal_i − price_i`.
   - Sum-to-one: `q_i = p_internal_i / Σ p_internal_j`,
     `edge_i = q_i − price_i`. Edges then sum to zero, so an impulse on one
     answer creates offsetting negative edges on its rivals.
4. **Exits first**, all-or-nothing per side:
   `yes_shares > 0 and edge < EXIT_EDGE_THRESHOLD → SELL_YES`;
   `no_shares > 0`, independent market, `edge > −EXIT_EDGE_THRESHOLD →
   SELL_NO`.
5. **Entries.** Candidates have no position, a price in `[0.02, 0.98]`, and
   `|edge| ≥ ENTRY_EDGE_THRESHOLD`; they are ordered by `|edge|` descending and
   sized against the remaining budget. At most `MC_MAX_NEW_PER_TICK` entries per
   evaluation and `MC_MAX_POSITIONS` open answers per market.
6. **Sum-to-one markets are YES-only.** A NO leg there is a basket over the
   field, and Manifold may add internal arbitrage legs on its own. Independent
   markets allow both sides.

**No partial positions, no scale-in, no separate limit-order strategy.** One
entry per position lifecycle; exit is all-or-nothing. A fair-capped buy's
unfilled remainder *rests* with an expiry (above) — that is execution
detail of the buy, not a client-placed standing order.

**Dry run.** `TRADING_DRY_RUN` defaults to `true`. Buys are sent with
`"dryRun": true` (simulated fill, no position), sells are only logged, and
nothing is persisted. The canary phase flips it to `false` with tiny caps.

**Manifold API calls:**

| Action | Endpoint | Body |
|---|---|---|
| Buy | `POST /v0/bet` | `{contractId, answerId?, amount, outcome:"YES"\|"NO", limitProb: fair-capped sweep (below), expiresMillisAfter: LIMIT_EXPIRY_SECS×1000, dryRun?}` |
| Sell | `POST /v0/market/{id}/sell` | `{answerId?, outcome:"YES"\|"NO", shares: <amount>}`; `answerId` is required on a multiple-choice market |
| Cancel | `POST /v0/bet/cancel/{betId}` (one bet per request) | — |
| Position | `GET /v0/market/{id}/positions?userId={bot_user_id}` (bot ID from `GET /v0/me` at startup) | — |
| Resting book | `GET /v0/bets?userId={bot}&kinds=open-limit` (boot snapshot of the bot's own open orders, one call for every market) | — |
| Market | `GET /v0/market/{id}` (includes `answers[].probability`) | — |

Buys sweep the AMM with a **fair-capped limit**, not the old
`limitProb=0.99` (YES) / `0.01` (NO) market-buy emulation that could sweep a
thin book far past fair value (`docs/ALGO.md` §S1, `LIMIT_EPSILON`, default
`0.01`). Manifold interprets `limitProb` as the market probability of YES for
both outcomes — a YES sweep stops when the probability rises to it (the limit
*is* the per-share price ceiling), a NO sweep stops when the probability
falls to it (the per-NO-share ceiling is `1 − limitProb`) — so one expression
caps both outcomes on the wire:

```
limitProb = clamp(p_internal, 0.01, 0.99) − ε
```

a YES buy then pays up to `p_internal − ε` per share and a NO buy up to
`(1 − p_internal) + ε` per NO share; the bot never fills worse than its own
believed fair price on either side. `p_internal` is the decayed belief the
edge math computed on the same evaluation (`PlannedTrade.p_internal` from
`Book::plan`; the binary market leg is the no-`answerId` leg). On sum-to-one
multiple-choice markets the entry edge is a *normalized* share, so the cap is
built from the normalized leg belief, mirroring the planner's edge
computation. Manifold validates `limitProb` into `[0.01, 0.99]`, and the
config validation keeps `ε` in `(0, 0.05)` — below the default entry-edge
threshold, so a threshold-sized edge still crosses the market price and the
order sweeps instead of resting unfilled.

**One call is the whole ladder (wire-verified).** One `POST /v0/bet` with
`limitProb + amount + expiresMillisAfter` consumes opposing resting orders
first — at *their* price (which may be stale and favorable), in price-time
priority — then sweeps the AMM up to the cap, and then rests whatever is
left as the same bet row. There is no manual "take stale limits first"
step and no mirror of other people's orders: the wire does the ladder
(see **Deliberate S1 deviations** below).

**Whole-percent quantization.** Manifold quantizes `limitProb`
server-side onto whole percents and rejects finer values with HTTP 400.
Before sending, the executor quantizes the exact cap in the one direction
that can only make it *less* aggressive for the outcome being bought — the
single invariant: the sent cap never permits a worse per-share fill than
the unquantized cap:

```
YES buy:  limitProb = floor(cap_exact × 100) / 100        (per-share price IS limitProb)
NO buy:   limitProb  = ceil(cap_exact × 100) / 100        (per-NO price is 1 − limitProb)
```

Both sides are clamped into `[0.01, 0.99]` afterwards. (A plain `floor`
for both would make NO fills *worse*, since lowering the YES-scale limit
raises the per-NO-share ceiling.)

**Expiring rests.** Manifold fills `amount` silently up to what the limit
allows (the response reports the filled part in `amount`/`shares` and echoes
the request in `orderAmount`, so the remainder is `orderAmount − amount`);
the remainder **rests automatically as the same bet row**. Every bot buy
sends `expiresMillisAfter = LIMIT_EXPIRY_SECS × 1000` (default 60 s,
validated into `[1, 300]`): the edge the rally is betting on can drift away,
so a rest must not sit longer than minutes. When the cap stops the sweep
before the requested mana is spent, the executor logs the shortfall at
`info!`, persists the ledger row with the *actual* shares and mana only,
and never retries; the rest is picked off or dies without a cancel call.

**Own resting-order tracking (executor-side, in memory — no schema
change).** The unfilled remainder is committed money, so the executor keeps
`OwnRestingOrders` (keyed market → bet id), fed by three sources:

1. **Buy responses.** A (partially) unfilled fill response records
   `(bet_id, outcome, answer_id, limit_prob, order_amount, filled_amount,
   filled_shares)`, expiry `=` placement time + `LIMIT_EXPIRY_SECS`.
2. **Own `global/new-bet` events.** Websocket `BetEvent`s with the bot's
   `user_id` (probe-verified: this stream carries maker fills) update the
   cumulative `amount`/`shares` counters; a cancelled, fully filled, or
   expired order leaves the tracker.
3. **One boot snapshot.** `GET /v0/bets?userId={bot}&kinds=open-limit`
   (payloads verified: `kinds` and `userId` combine; the backend filter is
   `is_filled=false AND is_cancelled=false`; no expiry filter — a row whose
   `expiresAt` passed can still appear for up to the ~1-minute cron lag, so
   rows are filtered locally on the wall clock). One REST call covers all
   markets.

Because the rest is a spending commitment the position columns do not yet
see, the pre-buy budget check counts it: `committed = invested + Σ
remainder(market) + amount`, against the same
`budget_mana + realized_pnl_mana` ceiling.

**Maker-fill accounting.** When a websocket event shows one of our rests
got (partially) filled *after* placement — at the rest's own limit price,
better than the sweep — the delta beyond the last seen counters is applied
through the same flow an ordinary buy uses: `Book::apply_buy` into the
order's leg with the server-reported mana delta, a `trade_ledger` row
carrying provenance `maker_fill:{betId}` in the trigger column, and a
Discord `[TRADE]` event. It all runs under the execution lock, and the
budget invariant gates the local application (a fill that would breach the
ceiling — only possible after external tampering with the budget columns —
is refused locally and left to the next reconcile pass, which trusts
Manifold). Duplicate broadcasts are idempotent by construction: the delta
key is *remaining amount consumed* (the stored cumulative
`amount`/`shares` overwritten with the server's values first), so a
replayed event computes a zero delta and writes nothing.

**Reverse-drift cancel policy** (checked after the price-mirror step of
every cached evaluation, before the plan). With a freshly derived fair
value — the same `entry_fair_value` derivation the entry cap uses — and
`cap_now = fair_capped_limit_prob(p_fair_now, ε, outcome)`:

```
YES order:  cancel iff  limitProb − cap_now > LIMIT_DRIFT_CANCEL   (default 0.05)
NO order:   cancel iff  cap_now − limitProb > LIMIT_DRIFT_CANCEL   (compared on the YES scale)
```

A resting order is never left in place when filling it would take the book
into a position at odds with current belief; favorable drift keeps it (it
fills at a price we could not replicate). In addition, always cancel an own
order when:

- the market's latest price mirror carried the resolution signature
  (`TerminalMirrors`), checked both at the top of a cached evaluation and
  when the websocket patch arrives;
- an entry executed in the same evaluation bought the opposite outcome on
  the same leg (per leg, so an answer-A entry does not touch a resting
  order on answer B);
- the operator liquidates the market (`/close`, `/untrack --liquidate`,
  `/panic` — the one hook those control-plane paths share).

Orders younger than 5 seconds are never cancelled by the drift or
opposite-entry rules: they were placed against the same evaluation's
belief, and the next price update re-checks them. Untracked-without-sale
(`/untrack`) has no cancel hook — its rests simply expire. Cancel writes
are rate-checked like every other write (one Trade-class token), and a
refusal or a failed cancel leaves the order resting, bounded by its expiry.

**Deliberate S1 deviations** (against the `docs/ALGO.md` §S1 sketch, on
wire grounds):

1. **No stale-book sweep / mirror of other traders' orders.** The ladder's
   first rung — crossing other participants' resting orders — is already
   executed by the single bet call at *their* price, in price-time
   priority; a manual sweep through our own order would only rearrange the
   same fills. A passive `contract/{id}/orders` mirror of the open book is
   deferred to the SNIPER phase unless S2 needs it.
2. **No passive sell side.** `/v0/market/{id}/sell` is an instant,
   uncapped sweep with no `limitProb`/expiry on the wire. (It may cross
   opponents' resting orders at their stale prices — favorable, but not
   controllable.) Exits remain instant.
3. **No `contract/{id}/orders` subscriptions, no `cpmm-multi-2` multi-bet**
   — out of scope for S1.

**Position sync (phase 1 of `docs/ALGO.md`, websocket live prices):** Prices
arrive live over the Manifold broadcast websocket
(`global/updated-contract`, coalesced into one evaluation per market per
2 s — `WS_DEBOUNCE_SECS`) and are written through
`MarketStore::market_prices_observed`. REST is demoted to a slow 5-minute
reconcile pass in `TradeExecutor`: batched probabilities
(`GET /v0/market-probs`, 100 IDs per call) through the same mirror, plus one
paginated portfolio snapshot
(`GET /v0/get-user-contract-metrics-with-contracts?userId={bot}&perAnswer=true`)
that replaces the per-market `/positions` sync. Rows map through
`Book::reconcile` with the same semantics. Every reconcile read spends a
Reconcile-class token from `rate.rs` and yields below the housekeeping
reserve. The pass runs on its own task with the summary returned over a
channel, so the websocket-event arm never stops draining while it works.

A market whose shape disagrees with a batched probability reading — a
multiple-choice market read as a bare `prob`, a binary market read with
`answerProbs`, or an answer ID that has never been recorded — does **not**
get the trading evaluation: it gets the non-trading full per-market
`sync_market` REST call for that one market, which owns kind refresh,
answer discovery with texts, and the position re-sync. A market **absent**
from the `market-probs` response map (unknown ID, closed, unrecognized
shape) is silently skipped for that pass and left to the next pass or the
websocket.

An incomplete portfolio snapshot is not corrected by per-market REST
calls either: whatever pages arrived are reconciled through
`Book::reconcile`, markets absent from an incomplete snapshot keep their
stored position (the pass logs that it is running on partial data), and
pagination offset plus completeness are computed from the raw response
rows, including ones the parser drops. A price patch
pinned exactly at 0/1 mirrors one final clamped observation (0.99/0.01)
and then takes the market out of evaluation — the price-patch stand-in
for the snapshot's `isResolved` check — until a full `sync_market`
refresh with a not-resolved snapshot clears the skip.

**Ledger:** Every successful bet writes a row to `trade_ledger`, including
`answer_id` for a multiple-choice market, and updates
`tracked_markets.invested_mana`, `realized_pnl_mana`, `yes_shares`, and
`no_shares`.

**Budget invariant:** Assert `invested_mana <= budget_mana + realized_pnl_mana`
before every buy. If violated (should be impossible after position sync), reject
the trade and alert Discord. Per-market budgets stay isolated; answers share
their market's budget.


### 6.7 Onboarding (`onboarding.rs`)

Triggered by `/track url:<url> budget:<mana>`:

1. **Parse slug** from URL's last path segment.
2. **Fetch market:** `GET https://api.manifold.markets/v0/slug/{slug}`. Verify
   `outcomeType` is `BINARY` or `MULTIPLE_CHOICE` and `isResolved == false`.
   Extract `{id, question, textDescription, resolutionCriteria?, closeTime,
   probability?, url, outcomeType, shouldAnswersSumToOne, answers?}`. For a
   multiple-choice market every answer is stored with
   `p_internal = logit(price)`. The canonical `url` is persisted as
   `tracked_markets.market_url` and shown in the ONBOARDED embed; the bare slug
   cannot reconstruct it because the creator segment is not part of the slug.
3. **Onboarding research agent:** `ONBOARDING_MODEL` runs as a `rig` agent in
   `onboarding_agent.rs`. The preamble is `SIEVE_SYSTEM_PROMPT` plus the agent
   instructions, and the model must support tool calling. Every request carries
   the fixed OpenRouter `session_id` `manif0ld-onboarding`, so sticky routing
   keeps the preamble and tool schemas on one provider and its prompt cache
   stays warm across turns and `/track` runs. The option is inert on the
   Cloudflare wire. The agent has four tools:
   - `web_search` — Firecrawl search over web and news (at most 8 results per
     call). The agent uses at most 8 searches.
   - `read_page` — Firecrawl Markdown scrape of one http(s) page, truncated to
     20,000 characters. The agent uses at most 5 page reads.
   - `dry_run_sieve` — compiles a candidate program on a blocking thread and
     returns, per sample post, whether it matched, plus the engine's authoring
     warnings (short literals, vacuous proximity windows). The agent is
     instructed to test 6–10 sample posts, including off-topic look-alikes
     (other languages, bots, deal spam) and posts that use a keyword in an
     unrelated sense, and to narrow any arm that matched an off-topic post.
   - `submit_sieve` — compiles the submitted program with `sieve::compile` on
     a blocking thread, and returns `ok` (with any compile warnings) or the
     root cause of the compiler error.

   The engine rejects constructs that can never match — multi-token literals
   (use adjacency instead) and case-sensitive `!` literals containing
   uppercase letters — and warns on literals under 3 characters and
   proximity windows whose operands share a literal. These surface through
   both tools, so the agent can converge without a compile-time dead rule.

   The agent researches the market topic for names, aliases, tickers, people,
   organizations, and events that Bluesky posts would use. The research feeds
   only the Sieve script.
4. **Compile with agentic self-correction:** there is no fixed attempt limit.
   Each `submit_sieve` call compiles one complete program; on failure the model
   reads the real compiler error in the tool result and submits a corrected
   program. The agent stops when `submit_sieve` returns `ok`; the accepted
   program is stored in a shared slot, so the model's final text is ignored.
   `MAX_AGENT_TURNS` (200) is only a runaway guard. Judgment stops at compile
   correctness — matching behavior has no ground truth at onboarding time. If
   the run ends without a compiling rule, abort and post the last compiler
   error to the Discord channel. `/track` wraps the whole run in a 600-second
   timeout; research plus several agent turns regularly exceeds the old 120 s.
   Each model turn also carries a 120-second total deadline. A read timeout
   alone cannot end a stalled turn: OpenRouter holds a queued upstream request
   open with `: OPENROUTER PROCESSING` comment lines, and each comment resets
   the per-read timeout, so the turn fails on a deadline instead of running to
   the `/track` wall.
5. **Persist** market row (initial `p_internal_log_odds = logit(market.probability)`), register `CompiledSieve` in `EngineRegistry`, create Discord thread, post the `[ONBOARDED]` embed.

   **Live progress:** the ephemeral `/track` followup is edited as onboarding
   advances. It shows the current step, a rolling log of completed actions —
   each `web_search` with its result count, each `read_page` with its size, and
   each `dry_run_sieve` with its match count and warnings — plus elapsed time,
   running counters, and the latest `submit_sieve` attempt in a code block.
   Edits are coalesced to at most one every 1.2 seconds so a burst of tool
   calls cannot trip Discord's per-message edit rate limit; the newest state
   always wins.

**No ring-buffer benchmarking, no pass-rate gate.** If it compiles, it's live. Operator is responsible for reviewing the script in the thread embed and using `/untrack` if it looks wrong.

### 6.8 Discord Control Plane (`discord.rs`)

**Commands** (all operator-gated):

| Command | Params | Behavior |
|---|---|---|
| `/track` | `url`, `budget` (default 100), `initial_prob` (optional) | Run onboarding. Reply with progress messages. |
| `/untrack` | `market` (autocomplete), `liquidate` (default true) | Remove Sieve engine. If `liquidate`, sell all shares. Soft delete the market, keep its row and history, archive thread. |
| `/budget` | `market`, `new_budget` | Update `budget_mana`. |
| `/status` | `market` (optional) | Without arg: table of all markets (budget, invested, PnL, P_int vs P_mkt, triage queue depth, last signal). With arg: that market's full state including Sieve source and top 10 recent signals. |
| `/override` | `market`, `probability` (1–99) | Set `p_internal_log_odds`. Trigger trade check. |
| `/panic` | `liquidate_all` (default false) | Set `global_kill_switch` in `kv`. If `liquidate_all`, sell all positions across markets. |

Every mutating command goes through one control-plane service (`control.rs`), and
the two teardown end states are explicit: `/untrack` hides the row
(`CLOSED` + `deleted_at`) while `Liquidate & Close` keeps it visible (`CLOSED`,
`deleted_at` NULL).

**Threads:** One per tracked market, created under `DISCORD_CONTROL_CHANNEL_ID`, named `📈 [M{budget}] {short_title}`. All subsequent events for that market (signals, trades, errors) post into the thread.

**Embeds emitted into threads:**

1. `[ONBOARDED]` — market question, link, budget, initial `P_market`, Sieve source block.
   The rule arrives as up to three fields named `Sieve source #1`, `#2`, `#3`;
   these are continuation chunks of one program, not onboarding attempts. The
   same split and naming is used by `/status market`, so the two views never
   disagree; the last field states how many chunks were dropped.
2. `[SIGNAL]` — author handle + followers, link to Bluesky post, answer (MCQ), evidence / direction / strength / priced probabilities, `P_internal` before → after.
3. `[TRADE]` — action (BUY_YES / SELL_YES / etc.), amount, shares delta, `P_internal` vs `P_market`, remaining available budget. A snipe trade adds `Strategy: ⛏ SNIPE · gap ±x.xxx` (§6.9); a copy trade adds `Strategy: 🤝 COPY · informed 0.72 · edge +0.08` (§6.11).
4. `[ERROR]` — any trade rejection, Manifold API error, LLM call failure, or rate-limit breach.

**Action buttons** on the ONBOARDED embed:

- `⏸ Pause / Resume` — toggles `status` between `ACTIVE`/`PAUSED`. A `CLOSED`
  market cannot be resumed; the button reports that and changes nothing.
- `💰 Liquidate & Close` — sells all shares, marks `CLOSED`, unregisters Sieve
  engine, archives the thread, then disables both buttons on the embed. The
  action is validated and idempotent: a market that is already `CLOSED` or no
  longer tracked is reported without any sale, and a market that Manifold has
  resolved is not sold because its positions settle automatically.

### 6.9 S2 New-Market Sniping (`snipe.rs`)

**Trigger.** `global/new-contract` (`ManifoldEvent::NewContract`) on the
broadcast websocket. The trade executor handles it inline: new contracts are an
hours-scale stream (measured: none in two minutes), and both remote steps are
bounded — the market fetch by the HTTP client and the decision call by
`SNIPE_DECISION_TIMEOUT_SECS` (default 2 s). Queued events wait in the
never-drop channel.

`SnipeStrategy` is constructed only when `SNIPE_ENABLED=true`; with the flag off
the executor never evaluates, fetches, or writes a snipe row.

**Gate order** (each skip writes one `snipe_ledger` row with `action = 'SKIP'`
and the reason; free gates run before any REST spend):

| # | Gate | Default | Skip reason |
|---|---|---|---|
| 1 | Stub `outcomeType` is `BINARY` or `MULTIPLE_CHOICE` | — | `unsupported_outcome_type` |
| 2 | Stub `mechanism` ∈ {`cpmm-1`, `cpmm-multi-1`, `cpmm-multi-2`} when present | — | `unsupported_mechanism` |
| 3 | `SNIPE_ENABLED` | `false` | `disabled` |
| 4 | Age from stub `createdTime` ≤ `SNIPE_TTL_SECS` | 3600 | `ttl_expired` |
| 5 | No existing `tracked_markets` row (`load`, including soft-deleted) | — | `already_tracked` |
| 6 | Discovery-class rate token, then one `GET /v0/market/{id}` | — | `rate_limited` |
| 7 | Snapshot not resolved; snapshot type/mechanism supported | — | `resolved`, `unsupported_*` |
| 8 | Multiple choice has at least one answer | — | `no_answers`, `no_probability` |
| 9 | `SNIPE_POOL_MANA − SUM(invested − realized_pnl) − resting ≥ MIN_TRADE_SIZE` | pool 100 | `pool_exhausted` |
| 10 | `totalLiquidity` ≥ `SNIPE_MIN_LIQ` | 100 | `low_liquidity`, `liquidity_unknown` |
| 11 | One `fair_prob` call with the snipe deadline | 2 s | `decision_failed: …` |
| 12 | Multiple choice: model names a tracked answer and its price exists | — | `no_answer_selected`, `answer_not_in_market`, `answer_price_missing` |
| 13 | `\|p_model − p_initial\|` ≥ `SNIPE_MIN_GAP` | 0.10 | `small_gap` |
| 14 | A sum-to-one market is never a NO leg | — | `sum_to_one_no_leg` |
| 15 | Kelly target ≥ `MIN_TRADE_SIZE` | 1.0 | `below_min_size` |
| 16 | Insert row + answers, resolve the book leg, return the order | — | — |

**Fair value.** One `TriageClient::fair_prob` call (decile rubric; §6.4, an
ALGO §4 deviation because the vendored `Score` has at most 10 levels). It
carries `{question, resolution_criteria, close_time, initial_prob,
creator_id?}` and, on a multiple-choice market, the answer texts. The market
object has one `initialProb`, not one per answer, so the multiple-choice state
carries `initialProb` when Manifold provides it and `0.5` otherwise; the gap is
always computed against the **chosen answer's actual price**.

**`p_initial` and the gap.** On a binary market `p_initial` is the snapshot
`probability` (falling back to `initialProb`); at creation it is the price. On
a multiple-choice market the model picks an answer (the abstain alternative
returns `answer: None` and is skipped), and `p_initial` is that answer's
`answers[].probability`. Every price is clamped to `[0.01, 0.99]` exactly as
`strategy_market.rs` clamps the row and answer columns, so the ledger, the gap,
and the book always agree. `gap = p_model − p_initial`; a positive gap buys
YES, a negative one buys NO. A NO leg is opened only on an **independent**
answer set (`shouldAnswersSumToOne = false`, i.e. `cpmm-multi-2`): on a
`shouldAnswersSumToOne = true` field a NO leg is a basket over the whole field
and the guard skips it regardless of mechanism name.

**Multiple-choice belief write.** `insert_market_row` anchors every answer
belief at its own price. Before returning the order, the snipe writes the
model's value onto the chosen answer
(`db.update_answer_belief(market, answer_id, logit(p_model), p_initial, now)`),
so after the fill the leg reads a real edge instead of `price − price = 0`
(which would make the planner exit the position immediately). On a sum-to-one
field the planner projects `q_i = p_i / Σ p_j`; the other answer beliefs are
therefore rescaled to sum to exactly `1 − p_model`, which makes the projection
return `p_model` on the chosen leg. Binary markets need no extra write: their
single leg already reads the root row's `p_internal`.

**Sizing.** The shared fractional-Kelly formula (`book::kelly_target_mana`), on
one budget value:

```
cap      = min(SNIPE_MAX_MANA,
               SNIPE_POOL_MANA − snipe_pool_invested() − resting_snipe)
size     = min(cap, kelly_target_mana(gap, p_initial, cap,
                                       λ = SNIPE_KELLY_FRACTION,
                                       entry_edge_threshold = SNIPE_MIN_GAP))
skip if  size < MIN_TRADE_SIZE
```

`SNIPE_KELLY_FRACTION` is passed straight through, but
`kelly_target_mana` clamps the effective fraction to `0.5`, so a configured
`1.0` still risks at most half of `cap`. `resting_snipe` is the unfilled mana
of own resting orders on `strategy = 'snipe'` rows.

**Per-market budget and the pool.** The inserted row's `budget_mana` is `cap`,
not `size`, so the existing
`available = budget + realized_pnl − invested` invariant holds unchanged for
the row. The pool is the sum over `strategy = 'snipe'`, `status = 'ACTIVE'`,
`deleted_at IS NULL` rows of `invested − realized_pnl`; realized profit returns
to the pool automatically. Unfilled own resting orders on snipe rows are also
subtracted from the pool (`resting_snipe`): the mana is committed to the same
pool before it fills, and a maker fill converts it to invested mana without
changing the total commitment. A maker fill on a snipe row stays
`trade_ledger`-only; its pool effect arrives through the invested column, and
the unfilled remainder is already counted through `resting_snipe`.

**Execution.** `SnipeStrategy` returns a `PlannedTrade` plus provenance; the
executor's `execute_planned` takes the execution lock, loads the book, reads the
kill switch, and calls the existing `execute` — budget invariant, Trade-class
token, fair-capped whole-percent limit, expiring rest, own-order tracking,
`trade_ledger`, position persistence, and the Discord event. No second trade
path exists. `trade_ledger.trigger_post_uri` carries the contract ID, and the
sent `limit_prob` is written to `trade_ledger.limit_prob` (migration 009).

The pre-execution pool number is only an early filter: the market fetch and the
decision call take seconds. After taking `execution_lock` and loading the row,
the executor re-reads `snipe_pool_invested()` and the resting layer and clamps
the buy to the remainder (or refuses it with a `snipe_cap: …` ledger reason when
the remainder is below `MIN_TRADE_SIZE`). That under-lock check is
authoritative, so two snipes that passed their pre-checks against the same
remainder cannot both spend it.

A row that passes every gate is inserted before execution, so an execution
refusal (kill switch, rate floor, dry run, cap, zero fill) leaves a tracked
snipe row with no position; the snipe row simply waits for the next price or
reconcile evaluation like any other active row. Only the ledger distinguishes
the refusal. The regular planner may only exit such a row, never enter it
(§6.6).

On a sum-to-one multiple-choice market the fair cap is built from the planner's
normalized leg belief (§6.6), while sizing uses the raw `gap`; the fill
therefore never exceeds the normalized fair value even when the raw gap is
larger.

**Why it cannot double-bet.** Our own bet produces an `updated-contract`, so
the regular cached evaluation may run moments later. Non-`news` rows are
planned exits-only (§6.6), so the regular path can produce an exit but never an
entry; the snipe order is not retried there. A zero-share fill leaves the leg
flat, and only a new strategy evaluation (never the planner) may enter it, with
the same pool and per-market cap.

**Ledger.** Every gate writes a `SKIP` row with whatever `p_initial`,
`p_model`, and `gap` were already known. The buy row is written **after**
execution with the real `mana_amount` and `manifold_bet_id` from `trade_ledger`;
the wire limit is the `trade_ledger.limit_prob` column written by `execute`
(migration 009). A refusal or a zero-share fill writes `SKIP` with the
executor's reason (`unfilled: snipe_cap: …`, `dry_run`, …). `cost_usd` is the
decision-call cost when OpenRouter reports it.

**TTL.** `SNIPE_TTL_SECS` gates eligibility only: a contract whose `createdTime`
is older than the TTL when the broadcast is evaluated is skipped
(`ttl_expired`). It does not expire the tracked row, cap the holding period, or
cancel later orders. It can be reached after a long executor stall or a
replayed event; with a healthy socket a new-market event is milliseconds old.
The config validates it into `[1, 604800]` seconds (one week), and the strategy
converts it with a checked conversion.

**Provenance.** Snipe rows have no Discord thread, so the trade lands in the
control channel through the normal `channel_for_market` fallback. The `[TRADE]`
embed gains a `Strategy: ⛏ SNIPE · gap ±x.xxx` field when
`tracked_markets.strategy = 'snipe'`; the existing `P_int vs P_mkt` field already
shows `p_model` against the creation price for a fresh entry.

**Visibility.** Snipe rows are hidden from operator listings (`/status`,
autocomplete) and from the Sieve registry, but the executor (`active()`) and
the reconcile pass treat them like every other active row: prices mirror, makes
fills apply, exits fire, and `/panic` sweeps them.

### 6.10 Trader Leaderboard (`traders.rs`, S3a)

**Scope.** This milestone builds and persists the smart-money watch list only.
Copy execution — the `global/new-bet` trigger, the `is_informed_bet` gate, the
follow ladder, and `copy_ledger` writes — is §6.11; the ledger table exists
already so the copy strategy needs no schema change.

**Endpoints (verified live, 2026-11-06).** All are public; the bot key is sent
but not required.

| Request | Cost | Observed shape |
|---|---|---|
| `GET /v0/leaderboard?kind=profit&limit=100` | 1 | `[{userId, score}]`, all-time profit, highest first |
| `GET /v0/leaderboard?kind=creator&limit=100` | 1 | same shape, ranked by creator earnings |
| `GET /v0/users/by-id?ids[]=…` | 1 per 100 ids | `[{id, name, username, isBot, avatarUrl}]`; unknown ids absent |
| `GET /v0/get-user-portfolio-history?userId=…&period=weekly` | 1 | ~585 hourly points, ~168 h, `{timestamp, profit, balance, …}` |
| `GET /v0/bets?userId=…&limit=N` | 1 | bare array of the newest N bet rows |

Wire facts that shaped the implementation:

- `leaderboard` caps `limit` at 100 and rejects `offset`; it has no time-window
  parameter. `kind=volume` answers an empty body (the request hangs), so the
  two usable ranked lists are `profit` and `creator`.
- The last point's cumulative `profit` in a `period=weekly` history equals the
  leaderboard's all-time `score` for the same account (checked exactly on the
  first-place trader). One history call therefore yields **both** profit
  windows: `profit_all_time = last.profit` and
  `profit_weekly = last.profit − first.profit`. The series is already
  deposit-adjusted, and a brand-new account's single point has no `profit`.
- Per-contract metrics (`get-user-contract-metrics-with-contracts`, also
  verified) expose `profit` and `from.week.profit`, but paginate at 1000 rows
  (5.6 MB for a whale) and the weekly sum is biased when truncated: the top
  account's weekly sum moved 35 000 mana between `limit=200` and `limit=1000`,
  against the exact −23 752 from history. Metrics are therefore not a scoring
  source.
- `get-user-portfolio` exposes `dailyProfit`, not a weekly figure, and
  `/v0/leagues?season=N` returns ~6 000 unsorted rows whose `manaEarned` is
  still zero early in the current season. Neither is used.
- `GET /v0/user/{id}` takes a *username*; `users/by-id` is the only batch
  identity lookup. Bet rows on the broadcast websocket carry no username.

**Candidate universe.** Two ranked pages: `kind=profit` (the skill core) and
`kind=creator` (active high-volume accounts as fresh blood). Deduplicated in
order and truncated to `TRADER_WATCH_SIZE`. Names are resolved in bulk, 100 IDs
per request; a name-lookup failure is cosmetic and never blocks scoring.

**Scoring.** One pure function of the persisted columns:

```
compress(x) = sign(x) · ln(1 + |x| / 1 000)
score       = 0.7 · compress(profit_weekly) + 0.3 · compress(profit_all_time)
score       = −∞   when bet_count < TRADER_MIN_BETS
```

Recent form carries 70 % because a trader who is good *now* is the better
person to copy, while all-time profit over-rewards early adopters who have
since stopped. The logarithm replaces rank percentiles on purpose: Manifold
profit is heavy-tailed (top ≈ 5.8 M, healthy ≈ 1 k), so `ln(1 + x/1 000)`
compresses the tail deterministically, keeps losers negative, and makes a
trader's score independent of who else is in the list — the same rows always
produce the same score. The bet count gates instead of blending because the
activity probe is capped at `TRADER_MIN_BETS` to stay cheap, so every passing
trader reports the same full sample; one lucky bet never buys a slot.

**Refresh cost and cadence.** One pass costs `2 + ceil(N/100)` universe
requests: two leaderboard pages plus one bulk name lookup per 100 candidates
(`N = TRADER_WATCH_SIZE`; four at the default 200) and **two per enriched
trader** (history for the profit windows, a bounded bets call for the gate).
`TRADER_REQUEST_BUDGET` (default 120) is a hard cap, so a default pass fully
enriches 58 traders. Candidates are visited stalest-first (never-seen first,
then oldest `updated_at`), so the whole list rotates across passes and everyone
keeps their last score meanwhile; raise the budget to
`2 + ceil(N/100) + 2 × TRADER_WATCH_SIZE` (404 at the defaults) to refresh the
full list in one pass. Every request spends a `Priority::Discovery` token from
the shared 500/min bucket; a refusal ends the pass early instead of erroring. A
full 200-trader pass is 404 requests and ~30 MB of history payload (~175 KB per
trader), still inside one minute of the bucket; at the default budget it is
~120 requests. `TRADER_REFRESH_MIN` (default 60) sets the cadence and the
first pass runs immediately at startup.

**Persistence and sharing.** `trader_scores` is rewritten by one replace-all
transaction per pass and is the row truth. `TraderBoard` is the read cache in
front of it, built at startup by `TraderBoard::load` before anything trades.
`App` owns one `Arc<TraderBoard>` and hands the same handle to the executor,
which exposes `TradeExecutor::trader_board()`; the copy strategy reads it on
every `global/new-bet` with `lookup(user_id)` (no I/O, one short read lock).
`TRADERS_ENABLED` (default true) gates only the refresh task — the board is
read-only, so it needs no canary phase; copy execution keeps its own flag.

**Gating and failure.** A refreshed trader below `TRADER_MIN_BETS` loses the
slot instead of keeping a stale score. A per-trader fetch failure keeps the
previous score; a failed pass logs and waits for the next tick, leaving the
persisted board usable. The board is trimmed to `TRADER_WATCH_SIZE` by score,
so unrefreshed members survive until displaced.

### 6.11 S3b Copy-Trading (`copy.rs`)

**Trigger.** `global/new-bet` (`ManifoldEvent::Bet`) on the broadcast
websocket, evaluated inline in the trade executor's loop exactly like S2's
`global/new-contract` arm. The stream is ~1.3 messages/s platform-wide, but the
watch-list lookup gates it: only a bet from one of the ~200 scored traders can
reach REST, two decision calls, or SQLite. Both decision calls are bounded by
`COPY_DECISION_TIMEOUT_SECS` (default 2 s) and the market fetch by the HTTP
client timeout, so a stalled model cannot block the price mirror for long.
Queued bets wait in the never-drop channel.

`CopyStrategy` is constructed only when `COPY_ENABLED=true`; with the flag off
the executor performs one `Option` check per bet and never fetches, decides, or
writes.

**Gate order.** Every gate from (e) on writes one `copy_ledger` row with
`action = 'SKIP'` and a stable reason. Gates (a)–(d) are deliberate exceptions:
they run on every bet on the platform, so they must not allocate, lock the
database, or write rows.

| # | Gate | Default | Skip reason / outcome |
|---|---|---|---|
| a | `COPY_ENABLED` | `false` | silent `None` |
| b | not our own bet, not a redemption, not cancelled | — | silent `None` (our own bets go to the maker-fill path) |
| c | `TraderBoard::lookup(user_id)` hits | — | silent `None` (high-volume path; one short read lock) |
| d | Bet ID not seen in the bounded 4096-entry ring | — | silent `None` (duplicate delivery of one bet) |
| e | `now − created_time ≤ COPY_MAX_AGE_SECS` | 30 s | `stale_bet` |
| e′ | The same age re-checked immediately before execution | 30 s | `stale_bet` |
| f | `is_filled` and `amount ≥ COPY_MIN_SIZE` | 5 | `unfilled_bet`, `below_min_size` |
| g | Row ownership: no `news` row, no `snipe` row, not soft-deleted, `ACTIVE` | — | `news_tracked`, `snipe_tracked`, `market_untracked`, `market_closed` |
| g | Same-direction position already held on the row | — | `already_positioned` |
| g | One Discovery token, then `GET /v0/market/{id}`; mechanism ∈ CPMM | — | `rate_limited`, `market_fetch_failed: …`, `resolved`, `unsupported_mechanism` |
| h | `outcomeType == BINARY` | — | `mcq_not_supported`, `unsupported_outcome_type` |
| i | `outcome` is `YES`/`NO`; direction = their outcome | — | `unsupported_outcome` |
| j | `is_informed_bet ≥ COPY_MIN_INFORMED` | 0.60 | `informed_decision_failed: …`, `not_informed` |
| j | One `fair_prob` call on the post-bet state | — | `fair_prob_failed: …` |
| j | Residual edge ≥ `COPY_MIN_EDGE` | 0.05 | `small_edge` |
| k | Pool, daily, and per-copy caps; Kelly target ≥ `MIN_TRADE_SIZE` | — | `pool_exhausted`, `daily_cap`, `cap_below_min_size`, `below_min_size` |
| l | Insert the row if untracked; return the order | — | — |

A market with no `tracked_markets` row gets one through the shared
`strategy_market::insert_market_row` (the same construction S2 uses):
`strategy = 'copy'`, empty Sieve source and thread, `budget_mana = cap`,
`p_market_last = p_current`, `p_internal_log_odds = logit(p_fair)`,
`belief_updated_at = now`.

**Residual edge, not move extrapolation.** ALGO §S3 step 4 asks to
"extrapolate their move". The bot does not invent a number for how much more a
trader will move a market; it requires model edge that survives the move they
already made. One REST snapshot (post-bet) and one `fair_prob` call give:

```
residual_yes = p_fair − p_current

their outcome YES:  edge = residual_yes            = p_fair − p_current
their outcome NO:   edge = −residual_yes           = (1 − p_fair) − (1 − p_current)
```

`edge ≥ COPY_MIN_EDGE` is required, so a fully consumed move (`|p_fair −
p_current|` inside the threshold) is skipped. The signed residual (not the
direction-flipped edge) is what Kelly sees, with `p_current` as the YES price:
that is exactly the planner's convention, where a negative edge selects the NO
formula `λ·|edge| / p_market` and a positive one `λ·edge / (1 − p_market)`.

**Sizing and caps.** One per-market ceiling is computed from three caps and the
resting layer:

```
cap  = min(COPY_MAX_MANA,
           COPY_POOL_MANA  − Db::copy_pool_invested() − resting_copy,
           COPY_DAILY_MANA − Db::copy_spent_last_24h(now) − resting_copy)
size = min(cap, kelly_target_mana(residual_yes, p_current, cap, λ))
skip when size < MIN_TRADE_SIZE
```

`resting_copy` is the unfilled mana of own resting orders on
`strategy = 'copy'` rows. `COPY_KELLY_FRACTION` defaults to `0.125`, half the
news default: the source move is already in the price, so the remaining edge is
smaller and less certain than a fresh news edge. `kelly_target_mana` clamps the
effective fraction to `0.5`, so values above that (including a configured
`COPY_KELLY_FRACTION` of 1) never risk more than half of `cap`.

- **Pool accounting.** `copy_pool_invested()` is
  `SUM(invested_mana − realized_pnl_mana)` over `strategy = 'copy'`,
  `status = 'ACTIVE'`, `deleted_at IS NULL` rows — the same truth the executor
  persists after every fill and every exit. Realised profit returns to the
  pool, and the numbers survive a restart.
- **Daily cap semantics.** `copy_spent_last_24h(now)` is `SUM(mana_amount)`
  over `copy_ledger` rows whose `action` is an executed buy (`BUY_YES` or
  `BUY_NO`) and whose `created_at ≥ now − 86 400`. Refused copies and exit
  signals never consume it, and closing a position does **not** refund it:
  the point of a daily cap is to bound new deployment per day. A maker fill of
  a resting copy remainder writes the same `BUY_*` row (`trader_id =
  'resting_fill'`, the order's bet id in `manifold_bet_id`), so it consumes the
  day's budget just like a taker fill.
- **Resting remainders.** An unfilled remainder is committed to the pool and
  to the day's deployment before it fills, so both remainders subtract
  `resting_copy` (unfilled own-order mana on copy rows). The maker fill
  converts that commitment: `invested_mana` rises and a `copy_ledger` buy row
  appears, while the order's outstanding falls; the total commitment is
  unchanged.
- **Concurrency and the authoritative check.** The strategy pre-check runs
  before the market fetch and the two decision calls, so another copy can
  consume the same remainder during those awaits. After taking
  `execution_lock`, `execute_planned_with_provenance` re-reads
  `copy_pool_invested()`, `copy_spent_last_24h(now)`, and the resting layer,
  clamps the order to the remainder, or refuses it with a `copy_cap: …` ledger
  reason when the remainder is below `MIN_TRADE_SIZE`. That check is
  authoritative; the pre-check is an early filter. The regular planner never
  enters a copy row (§6.6).

The inserted row's `budget_mana` is `cap`, not `size`, so the ordinary
`budget + realized_pnl − invested` invariant holds. An existing copy row keeps
its original ceiling: the budget is a per-market cap, not a per-copy one, and
the strategy clamps a new copy's size to that frozen `budget_mana` even when
the global pool has recovered.

**Ledger.** `copy_ledger` records one row per evaluation: `p_before`/`p_after`
from the source bet, `p_informed` and `fair_prob` from the two decision calls,
`gap` as the signed YES-space residual, and `bet_id`/`bet_amount` as
provenance. A filled copy uses the ordinary action names (`BUY_YES`,
`BUY_NO`) with the real `mana_amount` and `manifold_bet_id` from
`trade_ledger`; a control-plane refusal or zero fill writes `SKIP` with reason
`unfilled: …`, and a source bet that ages out after the decision calls writes
`SKIP` with the bare `stale_bet`. A maker fill of a resting copy remainder
writes a `BUY_*` row too (`trader_id = 'resting_fill'`, bet id in
`manifold_bet_id`, share count in `reason`) so the rolling daily cap sees it.

**Deviations from ALGO §S3, and why.**

1. **No `p_fair_est` extrapolation.** The residual model edge described above
   replaces a fabricated "how much further" estimate. It is measurable with one
   decision call and cannot follow a fully consumed move.
2. **Binary markets only.** Multiple-choice copying needs per-answer semantics
   (which answer they bought, that answer's residual edge, and interaction with
   the sum-to-one projection). A multiple-choice trigger is recorded as
   `mcq_not_supported` so the gap is visible; no guess is traded.
3. **A configurable latency bound instead of "the same second".** The decision
   calls, one REST fetch, and the write take 1–3 s in practice, so the honest
   contract is `COPY_MAX_AGE_SECS` (default 30) rather than an unenforceable
   one-second promise.
4. **Exit signals cancel and re-evaluate; they do not force a sale.** ALGO says
   the copied trader's exit accelerates belief decay. The bot has no
   per-position source-trader provenance, so the proxy is: a watched trader
   sells or redeems on a market where a `copy` row holds that leg ⇒ cancel our
   resting orders there and run one cached evaluation. The normal exit edge
   check still decides, so a sale is never forced by an exit signal alone.
5. **Trader exits are only honoured from watched traders.** This keeps the same
   watch-list gate as entries (a platform-wide exit watcher would need position
   provenance to be meaningful).

**TTL, duplicates.** The age gate is a trigger filter only: it never expires a
row, caps a holding period, or cancels an order. It is checked twice: before
the market fetch (the cheap filter) and again immediately before execution,
because the fetch (HTTP timeout) and both decision calls take real time and a
copy must not execute ~54 s after the source bet. The duplicate filter is an
in-memory 4096-entry ring (about an hour of platform bets), because replayed
events after a restart are already stopped by the age gate; the first
evaluation is the ledger record, so a duplicate writes nothing.

**Provenance and visibility.** `trade_ledger.trigger_post_uri` carries the
source bet ID. The `[TRADE]` embed renders `🤝 COPY · informed 0.72 · edge
+0.08` when `tracked_markets.strategy = 'copy'`; `p_informed` travels on the
existing `TradeEvent::Trade` variant (no new event). Copy rows are hidden from
operator listings (`is_news()` filters them, as it does snipe rows) but the
executor and the reconcile pass treat them like every other active row:
prices mirror, maker fills apply, exits fire, and `/panic` sweeps them. A
`news` row the operator owns is never copied onto and never re-owned.

### 6.12 S4 sum-to-one arbitrage — evaluated, not implemented

**Decision: no-go.** The S4 design in `docs/ALGO.md` §S4 was probed against the
live API on 2026-11-07 and is not feasible as written. Details, citations and
the sizing math are in
`docs/research/2026-11-07-sum-to-one-arb-probe.md` (a research artifact, not a
spec). No S4 module, watcher, or ledger exists.

- **The deficit never appears.** `Σp = 1` to within `4e-14` on all 926 open
  sum-to-one markets (two snapshots 90 s apart). Every bet, answer creation and
  liquidity change re-arbs `Σ` back to 1 inside the same transaction, and
  `cpmm-multi-2` rejects any trade that would break it.
- **A basket is blocked on 81% of the set.** `POST /v0/multi-bet` over more
  than one answer is hard-rejected on `cpmm-multi-2` (752 of 926 markets). Only
  174 small markets are even eligible.
- **The bound is negligible.** Profit scales as `δ² × depth`; a median market
  (8 answers, ~1000 liquidity) at a hypothetical 1% deficit is worth ≈0.1 mana
  per window, and a 1% deficit was never observed.
- **`rebalance` does not help.** It is real, free and accurate, but it only
  collapses an existing YES/NO basket. The S1 rebalance action therefore
  remains unused: with no S4 basket there is nothing to collapse, and S1 is
  YES-only on sum-to-one markets.

Reopen S4 only if a live deficit is ever measured; use
`contract/{id}/new-bet` as the trigger (not `updated-answers`), and size with
equal YES **shares** per answer (see the research note §6).

## 7. Sieve DSL Reference (for onboarding LLM)
Embed the following verbatim in `onboarding.rs` as `SIEVE_SYSTEM_PROMPT`; `onboarding_agent.rs` appends its agent instructions to it at run time. Word content is matched verbatim apart from ASCII case folding; the engine's tokenizer, normalization, and word-boundary behavior are all described in the prompt itself, and must stay in sync with the engine.

```
You are an expert filtration engineer writing a program in the Sieve DSL.
Sieve is a declarative, zero-backtracking O(N) DSL that compiles into an
Aho-Corasick trie over case-folded tokens and native compiled proximity
state machines.

### 1. LEXICAL FACTS (handled internally by the Sieve engine)
- Case-Insensitive: "launch" matches "Launch", "LAUNCH".
- A token is a contiguous run of ASCII letters, digits, and underscores.
  Accented characters END a token: "após" tokenizes as "ap" + "s". This is
  why short literals are dangerous: a 2-letter token in a Romance-language
  post is almost always a fragment of a longer word.
- Implicit word boundaries: "bot" matches "a bot!", "#bot", but NOT "robot".
- Word content is matched verbatim apart from case folding. There is NO
  Unicode normalization of word content.

### 2. LITERAL MODIFIERS
- "word"    : exact token (case-insensitive)
- "word"+   : morphological expansion (-s, -es, -ed, -ing, -er)
- "word"~   : leetspeak/obfuscation normalization (@/4→a, 3→e, 1/!→i, 0→o, $/5→s, s.c.a.m→scam)
- "word"*   : prefix wildcard ("gpt5"* matches "gpt5turbo")
- *"word"   : suffix wildcard (*"coin" matches "bitcoin", "memecoin")
- Chainable: *"coin"+~ = suffix + morphology + leetspeak
- The case-sensitive `!` modifier is NOT supported. Do not use it.

### 3. COMBINATORS
- A | B      : OR
- A B        : adjacent sequence (ordered, distance 1)
- A ..N B    : ordered proximity (≤ N intervening tokens, 1≤N≤16)
- A &..N B   : unordered proximity window (1≤N≤16)
- A & B      : document co-occurrence
- A - B      : exception guard (match A unless B also matches)

### 4. EBNF
program     = { statement } ;
statement   = macro_def | rule_def ;
macro_def   = "@" IDENT "=" expr ";" ;
rule_def    = "match" [ IDENT ] "{" expr "}" ;
expr        = diff_expr ;
diff_expr   = and_expr { "-" and_expr } ;
and_expr    = alt_expr { "&" alt_expr } ;
alt_expr    = seq_expr { ( "|" | NEWLINE ) seq_expr } ;
seq_expr    = term { [ prox_op ] term } ;
prox_op     = ".." INT | "&.." INT ;
term        = literal | "@" IDENT | "(" expr ")" ;
literal     = [ "*" ] STRING { modifier } ;
modifier    = "+" | "~" | "*" ;

### 5. CONSTRUCTS THE COMPILER REJECTS
1. A quoted literal containing a space fails to compile ("Donald Trump"),
   because a pattern may not span tokens. Write phrases with adjacency:
   "donald" "trump", or use a single distinctive token ("trump").
2. A case-sensitive `!` literal containing uppercase letters ("SEC"!) fails
   to compile: patterns are scanned on the case-folded stream. Drop `!`.

### 6. WRITING RULES THAT DO NOT MISFIRE
1. `&..N` and `..N` bind TIGHTER than `|`. Parenthesize the group a window
   applies to: ("associated" "press" | "ap" | "reuters") &..8 ("call"+ | "win"+).
   Written without the parentheses, only the last alternative is constrained
   and the others match standalone.
2. Never include the anchor token in its own context set. `"midterms" &..8
   @terms` is vacuous when @terms contains "midterm"+: the same occurrence
   satisfies the window at distance 0.
3. Never rely on a 1-2 character token ("AP", "US", "FF") without a tight
   proximity window, and never attach `+` to one. Wire-service abbreviations
   need context: ("ap" | "associated" "press" | "nbc") &..6 ("call"+ |
   "projection"+ | "race" | "win"+).
4. Name people by their distinctive surname ("talarico"), or as an
   adjacency phrase ("mary" "peltola"). Common first names alone match
   the wrong people.
5. The `-` exception guard is document-wide: only subtract RARE words
   (airdrop, memecoin, fanfic), never common words like "job" or "call".
6. Prefer verbs/events with strong signalling value ("call"+, "win"+,
   "projection"+) over bare topic nouns.

### 7. DESIGN REQUIREMENTS FOR THIS USE CASE
1. Every @macro ends with ';'. The final `match Market_<id> { ... }` has no ';'.
2. Polysemy guard: never match ambiguous nouns (e.g., "apple", "orion") standalone;
   always bind to a domain context via `(@codenames &..8 @domain_context)`.
3. This rule's ONLY job is topical filtering — matching posts plausibly related to
   the market. Direction (YES vs NO) is determined by a downstream model, so DO NOT
   try to encode direction. Match both supportive and skeptical posts equally.
4. Always include a `- @noise` exception at the end of the match block to
   subtract rare noise words (airdrop/memecoin promo, giveaway spam,
   fanfiction tags).
```

## 8. Implementation Milestones

**Phase 1 — Foundation:**
- Crate setup, config, SQLite schema + `db.rs` queries.
- Jetstream WS client (`ingest.rs`) + extractor (`extract.rs`); live-tail subscribe, no replay.
- `EngineRegistry` + a hand-written Sieve rule for a known market; verify posts flow through the matcher.
- Assert that `extract.rs` output preserves raw case and punctuation (unit test: post with mixed case and leetspeak should still be extracted verbatim; expansion happens later in Sieve).

**Phase 2 — Triage + Belief:**
- CF AI Gateway client, triage prompt, JSON response parsing.
- Belief engine (`current_p_internal`, `apply_post`).
- End-to-end: post → extract → match → triage → belief update printed to stdout.

**Phase 3 — Trading:**
- Manifold client (bet / sell / bets / market), Kelly sizing, `decide_trade`.
- Position sync on 60s ticker.
- Ledger writes.
- Verify on a small test market with tiny budget before going live.

**Phase 4 — Discord + Onboarding:**
- Discord bot skeleton, slash command registration, thread creation.
- `/track` (onboarding pipeline).
- `/status`, `/untrack`, `/budget`, `/override`, `/panic`.
- Signal / trade / error embeds.

**Verification checklist:**
- [ ] Hand-test the whole pipeline against a known market with `budget=10`.
- [ ] Confirm Manifold `limitProb=0.99` behaves as a market buy on a real test bet.
- [ ] Confirm CF AI Gateway model identifiers.
- [ ] Confirm a restart and a reconnect both resume at the live tail: the connect URL carries no `cursor`, and `messages_processed` never jumps after startup.
- [ ] Confirm budget invariant holds across a buy → sell → buy cycle.
- [ ] Confirm panic switch stops all buys within 2s.
- [ ] Confirm `extract.rs` does not mutate case, punctuation, or Unicode — a post `"S.C.A.M 🚨"` reaches Sieve byte-for-byte intact; Sieve's `"scam"~` literal matches it, and `"SCAM"!` does not.
