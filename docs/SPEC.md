# Technical Specification: `manif0ld` (v2, trimmed)

**Scope:** Personal Bluesky → Manifold auto-trader. The core value is **filtering**: turning the Jetstream firehose into a small number of "this matters for market X" posts, and acting on them. Everything else is minimal infrastructure to make that useful.

## 1. Purpose & Invariants

1. **Filter-first.** Sieve + one decision-model call per surviving post is the product. If the filter works, the bot works.
2. **Auto-trading is in scope, but simple.** Market buys only. Enter once per market per direction on strong edge, exit once on edge decay. No limit orders, no partial scaling, no stop-loss regimes.
3. **Per-market isolated budgets.** Each market has `budget_mana`. Never invest more than `budget_mana` net in a market. Realized profits cycle back into that market's available budget only.
4. **No autonomous market discovery.** Markets only enter via operator `/track`.
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

# Discord
DISCORD_BOT_TOKEN=...
DISCORD_GUILD_ID=...
DISCORD_CONTROL_CHANNEL_ID=...
DISCORD_OPERATOR_USER_ID=...

# Trading (defaults, overridable per market via /track)
DEFAULT_MARKET_BUDGET=100
KELLY_FRACTION=0.25
ENTRY_EDGE_THRESHOLD=0.05
EXIT_EDGE_THRESHOLD=0.01
MIN_TRADE_SIZE=1.0

# Belief engine
BELIEF_KAPPA=1.1
BELIEF_HALFLIFE_HOURS=4.0

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
  deleted_at           INTEGER            -- NULL while tracked; set by /untrack
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
  manifold_bet_id TEXT,
  created_at      INTEGER NOT NULL
);

CREATE TABLE kv (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);  -- 'global_kill_switch'
```

### 5.1 Market lifecycle and soft deletion

- `ACTIVE` / `PAUSED` / `CLOSED` describe trading: a `CLOSED` market keeps its
  row but no longer trades.
- `deleted_at` describes tracking: `NULL` means the market is tracked. Setting
  it (via `/untrack`) also forces `status = 'CLOSED'`.
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
not separate code paths.

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

**Binary decision (called after each belief update and on a 60s ticker per
active market):**

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

**Multiple-choice decision (same 60s ticker, same material-signal trigger):**

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

**No partial positions, no scale-in, no limit orders.** One entry per position
lifecycle. Exit is all-or-nothing.

**Dry run.** `TRADING_DRY_RUN` defaults to `true`. Buys are sent with
`"dryRun": true` (simulated fill, no position), sells are only logged, and
nothing is persisted. The canary phase flips it to `false` with tiny caps.

**Manifold API calls:**

| Action | Endpoint | Body |
|---|---|---|
| Buy | `POST /v0/bet` | `{contractId, answerId?, amount, outcome:"YES"\|"NO", limitProb: 0.99 for YES / 0.01 for NO, dryRun?}` |
| Sell | `POST /v0/market/{id}/sell` | `{answerId?, outcome:"YES"\|"NO", shares: <amount>}`; `answerId` is required on a multiple-choice market |
| Positions | `GET /v0/market/{id}/positions?userId={bot_user_id}` (bot ID from `GET /v0/me` at startup) | — |
| Market | `GET /v0/market/{id}` (includes `answers[].probability`) | — |

Using `limitProb=0.99` (YES) / `0.01` (NO) emulates a market buy that fills at
the current price, sweeping the AMM if needed.

**Position sync:** Every 60 seconds, `evaluate_market` fetches the market and the
positions for each active market and reconciles local state. Binary markets use
the root `ContractMetric`; multiple-choice markets use the per-answer rows with
the root checksum. If they disagree, trust Manifold and log a `warn!`.

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
3. `[TRADE]` — action (BUY_YES / SELL_YES / etc.), amount, shares delta, `P_internal` vs `P_market`, remaining available budget.
4. `[ERROR]` — any trade rejection, Manifold API error, LLM call failure, or rate-limit breach.

**Action buttons** on the ONBOARDED embed:

- `⏸ Pause / Resume` — toggles `status` between `ACTIVE`/`PAUSED`. A `CLOSED`
  market cannot be resumed; the button reports that and changes nothing.
- `💰 Liquidate & Close` — sells all shares, marks `CLOSED`, unregisters Sieve
  engine, archives the thread, then disables both buttons on the embed. The
  action is validated and idempotent: a market that is already `CLOSED` or no
  longer tracked is reported without any sale, and a market that Manifold has
  resolved is not sold because its positions settle automatically.

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
