# Technical Specification: `bsky-manifold-arb` (v2, trimmed)

**Scope:** Personal Bluesky → Manifold auto-trader. The core value is **filtering**: turning the Jetstream firehose into a small number of "this matters for market X" posts, and acting on them. Everything else is minimal infrastructure to make that useful.

## 1. Purpose & Invariants

1. **Filter-first.** Sieve + one small LLM call per surviving post is the product. If the filter works, the bot works.
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
                                          ├─ per-market match OR VIP author bypass
                                          ▼
                                    Triage queue (bounded per market)
                                          │  gemini-3.5-flash-lite via CF AI Gateway
                                          ▼
                                    {material, direction, strength}
                                          │  if material
                                          ▼
                                    Belief update (log-odds w/ time decay)
                                          │
                                          ▼
                                    Trade check → Manifold /v0/bet | /sell
                                          │
                                          ▼
                                    Discord thread notification
```

**Flow per post:** extract → loop 5 Sieve engines → if match, enqueue → single LLM call → belief update → trade check.

**Single binary, single tokio runtime, single process.** No worker threads for Sieve (in-process, ~microseconds), no batching (LLM calls are ~1/s at most), no separate services.

## 3. Repository Layout

Single Rust crate:

```
bsky-manifold-arb/
├── Cargo.toml
├── .env.example
├── README.md
├── migrations/
│   └── 001_init.sql
└── src/
    ├── main.rs          # setup, task spawn, graceful shutdown
    ├── config.rs        # env loading, constants
    ├── error.rs         # anyhow/thiserror aliases
    ├── types.rs         # domain types: Market, ExtractedPost, Trade, Belief, etc.
    ├── db.rs            # rusqlite schema + queries
    ├── extract.rs       # Jetstream event → ExtractedPost (structural extraction only)
    ├── ingest.rs        # Jetstream websocket, reconnect, cursor persistence
    ├── sieve.rs         # EngineRegistry + per-market CompiledSieve wrapper
    ├── triage.rs        # CF AI Gateway client, LLM triage call
    ├── belief.rs        # log-odds state, decay, impulse
    ├── trade.rs         # Manifold client, Kelly sizing, order manager
    ├── onboarding.rs    # fetch market, prompt LLM, compile Sieve, resolve VIPs
    ├── pipeline.rs      # extract → match → triage → belief → trade orchestration
    └── discord.rs       # bot, commands, embeds, thread management
```

Key dependencies: `tokio`, `tokio-tungstenite`, `serde`, `serde_json`, `reqwest`, `rusqlite`, `serenity`, `tracing`, `anyhow`, `chrono`, `sieve` (path dependency).

`extract.rs` is split out from `ingest.rs` deliberately: it's the only place where we touch the raw event structure, and it makes it obvious from the module name that we're extracting fields, not normalizing text.

## 4. Configuration (`.env.example`)

```ini
# Bluesky Jetstream
JETSTREAM_URL=wss://jetstream2.us-east.bsky.network/subscribe

# Cloudflare AI Gateway
CF_AI_GATEWAY_URL=https://gateway.ai.cloudflare.com/v1/{account_id}/{gateway_name}
CF_AI_GATEWAY_TOKEN=...
TRIAGE_MODEL=@cf/google/gemini-3.5-flash-lite          # verify exact identifier
ONBOARDING_MODEL=@cf/deepseek/deepseek-v4.1-flash-1017 # verify exact identifier

# Manifold
MANIFOLD_API_KEY=...
MANIFOLD_BOT_USER_ID=...

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
DB_PATH=./data/arb.db
```

**Note on model identifiers:** Confirm the exact strings CF AI Gateway accepts for `deepseek-v4.1-flash-1017` and `gemini-3.5-flash-lite`. If they route through the gateway's `/compat/chat/completions` (OpenAI-compatible) rather than Workers AI, adjust `reqwest` calls accordingly.

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
  created_at           INTEGER NOT NULL
);

CREATE TABLE market_vip_dids (
  market_id  TEXT NOT NULL,
  did        TEXT NOT NULL,
  handle     TEXT NOT NULL,
  PRIMARY KEY (market_id, did),
  FOREIGN KEY (market_id) REFERENCES tracked_markets(id)
);

CREATE TABLE signal_ledger (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  market_id      TEXT NOT NULL,
  post_uri       TEXT NOT NULL,
  author_handle  TEXT NOT NULL,
  post_text      TEXT NOT NULL,
  material       INTEGER NOT NULL,
  direction      TEXT NOT NULL,          -- 'strong_yes'|'yes'|'neutral'|'no'|'strong_no'
  strength       INTEGER NOT NULL,       -- 0..3
  impulse        REAL NOT NULL,          -- applied log-odds delta
  p_after        REAL NOT NULL,
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
);  -- 'jetstream_cursor_us', 'global_kill_switch'
```

## 6. Components

### 6.1 Jetstream Ingestion (`ingest.rs`)

- Connect to `JETSTREAM_URL` with `wantedCollections=app.bsky.feed.post`.
- Filter each event: keep only `kind == "commit"` and `commit.operation == "create"`. Discard the rest.
- Hand the raw event to `extract.rs` (§6.2) to produce an `ExtractedPost`.
- Persist `time_us` cursor to `kv` every 5 seconds.
- On reconnect: append `&cursor={last_us}`; exponential backoff on failure (1s → 2s → 4s → ... → 60s cap).
- Emit each `ExtractedPost` to a bounded broadcast channel (capacity ~1024). If the channel is full, drop oldest and log a rate-limited `warn!`.

`ingest.rs` does no text processing beyond event filtering and cursor management. All structural extraction lives in `extract.rs`.

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
        uri: event["commit"]["uri"].as_str()?.to_string(),
        did: event["did"].as_str()?.to_string(),
        handle: /* from account cache, or fallback to DID */,
        text,
        created_at: (event["time_us"].as_i64()? / 1_000_000) as i64,
    })
}
```

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
    /// Returns market IDs whose Sieve rule matched OR whose VIP list contains the author DID.
    pub fn matching_markets(&self, text: &str, author_did: &str, vip_lookup: &VipLookup) -> Vec<MarketId>;
}
```

- One `CompiledSieve` per market, compiled via `CompiledSieve::compile(&source)`.
- **No pre-processing of `text` before passing it to `.matches()`.** Sieve applies its own lexical normalization internally (case folding, NFKC, leetspeak expansion, word boundaries, morphological expansion) per its DSL. Duplicating any of that here would break case-sensitive `!` literals and double-apply expansions.
- The matching loop reads from `RwLock` and calls `.matches(text).is_empty()` per engine. 5 engines × ~1000 posts/s = 5000 `.matches()` calls/s. Trivial for Sieve.
- Hot-swap on `/track`, `/untrack` = insert/remove from the map. `std::sync::RwLock` is fine at this scale.

### 6.4 LLM Triage (`triage.rs`)

One call per matched post. No batching, no tiering.

**Prompt:**

```
System: You evaluate Bluesky posts as signal for a prediction market.

User:
Market question: {question}
Resolution criteria: {resolution_criteria}
Market description: {description}
Market close time: {close_time_iso}
Current market probability: {p_market:.2}

Bluesky post:
Author: @{handle} ({followers} followers)
Posted: {post_time_iso}
Text: {text}

Evaluate this post. Respond as JSON only:
{
  "material": true if the post contains genuine evidence, news, or insider commentary that should shift a rational trader's probability; false if it is a coincidence match, joke, spam, or off-topic,
  "direction": one of "strong_yes" | "yes" | "neutral" | "no" | "strong_no",
  "strength": integer 0-3 where 0=noise, 1=weak circumstantial, 2=strong secondary evidence, 3=definitive proof,
  "rationale": one sentence
}
```

**Threshold:** Post is *material* if `material == true` **and** `direction != "neutral"` **and** `strength >= 1`. Otherwise logged, ignored.

**Rate limit:** Bound the per-market triage queue at 30 in-flight. If a market is queue-saturated, drop oldest and log `warn!`. Surface this count in `/status` per market.

### 6.5 Belief Engine (`belief.rs`)

Log-odds with exponential decay toward market price. No credibility multipliers beyond VIP status, no claim clustering, no slow-path consolidation.

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

pub fn apply_post(
    l_stored: &mut f64,
    belief_updated_at: &mut i64,
    direction: Direction,   // maps to ±1.0, ±0.5, 0.0
    strength: u8,           // 0..3
    is_vip: bool,
    p_market_now: f64,
    now: i64,
) {
    let l_market = (p_market_now / (1.0 - p_market_now)).ln();
    // (implementation: decay l_stored toward l_market first using
    //  current_p_internal, then add impulse)
    let credibility = if is_vip { 1.5 } else { 1.0 };
    let impulse = BELIEF_KAPPA * direction.polarity() * (strength as f64 / 3.0) * credibility;
    *l_stored = (l_stored.clamp(-4.595, 4.595) + impulse).clamp(-4.595, 4.595);
    *belief_updated_at = now;
}
```

**Direction mapping:** `strong_yes=+1.0, yes=+0.5, neutral=0.0, no=-0.5, strong_no=-1.0`.

**Decay behavior:** Belief pulls toward the *current market price* with a 4-hour half-life. If the market catches up to the bot's belief → edge disappears → trade exits via edge-decay rule. If the bot was wrong and the market moves away → belief also pulls toward the market → trade exits. This is the only stop-loss mechanism. It's simple, automatic, and hard to get wrong.

**Manual override:** `/override market probability:X` sets `p_internal_log_odds = logit(X)` and updates `belief_updated_at = now`.

### 6.6 Trade Execution (`trade.rs`)

**Sizing** (fractional Kelly, λ = `KELLY_FRACTION`, capped at 50% of available budget):

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

**Decision (called after each belief update and on a 60s ticker per active market):**

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

**No partial positions, no scale-in, no limit orders.** One entry per position lifecycle. Exit is all-or-nothing.

**Manifold API calls:**

| Action | Endpoint | Body |
|---|---|---|
| Buy | `POST /v0/bet` | `{contractId, amount, outcome:"YES"|"NO", limitProb: 0.99 for YES / 0.01 for NO}` |
| Sell | `POST /v0/market/{id}/sell` | `{outcome:"YES"|"NO", shares: <amount>}` |
| Position | `GET /v0/bets?contractId={id}&userId={bot_user_id}` | — |
| Market | `GET /v0/market/{id}` | — |

Using `limitProb=0.99` (YES) / `0.01` (NO) emulates a market buy that fills at the current price, sweeping the AMM if needed. Verify this behavior on a test market before going live.

**Position sync:** Every 60 seconds, fetch `/v0/bets` for each active market; reconcile local `yes_shares`/`no_shares`/`invested_mana` with Manifold's response. If they disagree, trust Manifold and log a `warn!`. This is the only robustness we need against bet failures.

**Ledger:** Every successful bet writes a row to `trade_ledger` and updates `tracked_markets.invested_mana`, `realized_pnl_mana`, `yes_shares`, `no_shares`.

**Budget invariant:** Assert `invested_mana <= budget_mana` before every buy. If violated (should be impossible after position sync), reject the trade and alert Discord.

### 6.7 Onboarding (`onboarding.rs`)

Triggered by `/track url:<url> budget:<mana>`:

1. **Parse slug** from URL's last path segment.
2. **Fetch market:** `GET https://api.manifold.markets/v0/slug/{slug}`. Verify `outcomeType == "BINARY"` and `isResolved == false`. Extract `{id, question, textDescription, resolutionCriteria?, closeTime, probability}`.
3. **LLM Sieve synthesis:** Call `ONBOARDING_MODEL` with `SIEVE_SYSTEM_PROMPT` + market context. Request JSON:
   ```json
   {
     "sieve_program": "match Market_<id> { ... }",
     "vip_actor_queries": ["@handle_or_name", ...]
   }
   ```
4. **Compile:** `CompiledSieve::compile(&sieve_program)`. On failure, retry the LLM up to 2 more times with the compile errors appended. If still failing after 3 attempts, abort and post the error + last LLM output to the Discord channel.
5. **Resolve VIPs:** For each `vip_actor_queries[i]`, call `GET https://public.api.bsky.app/xrpc/app.bsky.actor.searchActors?q={query}&limit=5`. Pick the best match — prefer custom-domain handle (not ending in `.bsky.social`) or `followersCount >= 500`. Store `did` + `handle` in `market_vip_dids`. Log failures but don't abort onboarding.
6. **Persist** market row (initial `p_internal_log_odds = logit(market.probability)`), register `CompiledSieve` in `EngineRegistry`, create Discord thread, post the `[ONBOARDED]` embed.

**No ring-buffer benchmarking, no pass-rate gate.** If it compiles, it's live. Operator is responsible for reviewing the script in the thread embed and using `/untrack` if it looks wrong.

### 6.8 Discord Control Plane (`discord.rs`)

**Commands** (all operator-gated):

| Command | Params | Behavior |
|---|---|---|
| `/track` | `url`, `budget` (default 100), `initial_prob` (optional) | Run onboarding. Reply with progress messages. |
| `/untrack` | `market` (autocomplete), `liquidate` (default true) | Remove Sieve engine. If `liquidate`, sell all shares. Archive thread. |
| `/budget` | `market`, `new_budget` | Update `budget_mana`. |
| `/status` | `market` (optional) | Without arg: table of all markets (budget, invested, PnL, P_int vs P_mkt, triage queue depth, last signal). With arg: that market's full state including Sieve source and top 10 recent signals. |
| `/override` | `market`, `probability` (1–99) | Set `p_internal_log_odds`. Trigger trade check. |
| `/vip` | `market`, `handle` | Resolve handle → DID → insert into `market_vip_dids`. |
| `/panic` | `liquidate_all` (default false) | Set `global_kill_switch` in `kv`. If `liquidate_all`, sell all positions across markets. |

**Threads:** One per tracked market, created under `DISCORD_CONTROL_CHANNEL_ID`, named `📈 [M{budget}] {short_title}`. All subsequent events for that market (signals, trades, errors) post into the thread.

**Embeds emitted into threads:**

1. `[ONBOARDED]` — market question, link, budget, initial `P_market`, VIP handles resolved, Sieve source block.
2. `[SIGNAL]` — author handle + followers, link to Bluesky post, direction, strength, rationale, `P_internal` before → after.
3. `[TRADE]` — action (BUY_YES / SELL_YES / etc.), amount, shares delta, `P_internal` vs `P_market`, remaining available budget.
4. `[ERROR]` — any trade rejection, Manifold API error, LLM call failure, or rate-limit breach.

**Action buttons** on the ONBOARDED embed:

- `⏸ Pause / Resume` — toggles `status` between `ACTIVE`/`PAUSED`.
- `💰 Liquidate & Close` — sells all shares, marks `CLOSED`, unregisters Sieve engine.

## 7. Sieve DSL Reference (for onboarding LLM)

Embed the following verbatim in `onboarding.rs` as `SIEVE_SYSTEM_PROMPT`. Note that lexical normalization (case folding, NFKC, leetspeak expansion, word-boundary handling) is the *engine's* job — the DSL below describes what Sieve does with each literal, not what our pipeline does.

```
You are an expert filtration engineer writing a program in the Sieve DSL.
Sieve is a declarative, zero-backtracking O(N) DSL that compiles into a SIMD
Lookup Table, a Double-Array Aho-Corasick trie, and 64-bit Glushkov Shift-AND
proximity state words.

### 1. LEXICAL DEFAULTS (handled internally by the Sieve engine)
- Case-Insensitive: "launch" matches "Launch", "LAUNCH".
- Unicode Normalized: input is NFKC-normalized, zero-width joiners stripped.
- Implicit Word Boundaries: "bot" matches "a bot!", "#bot", but NOT "robot".

### 2. LITERAL MODIFIERS
- "word"    : exact token (case-insensitive)
- "word"+   : morphological expansion (-s, -es, -ed, -ing, -er)
- "word"~   : leetspeak/obfuscation normalization (@/4→a, 3→e, 1/!→i, 0→o, $/5→s, s.c.a.m→scam)
- "word"*   : prefix wildcard ("gpt5"* matches "gpt5turbo")
- *"word"   : suffix wildcard (*"coin" matches "bitcoin", "memecoin")
- "word"!   : case-sensitive exact match (use on short acronyms: "API"!, "SEC"!, "AGI"!)
- Chainable: *"coin"+~ = suffix + morphology + leetspeak

### 3. COMBINATORS
- A | B      : OR
- A B        : adjacent sequence
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
modifier    = "+" | "~" | "*" | "!" ;

### 5. DESIGN REQUIREMENTS FOR THIS USE CASE
1. Every @macro ends with ';'. The final `match Market_<id> { ... }` has no ';'.
2. Polysemy guard: never match ambiguous nouns (e.g., "apple", "orion") standalone;
   always bind to a domain context via `(@codenames &..8 @domain_context)`.
3. This rule's ONLY job is topical filtering — matching posts plausibly related to
   the market. Direction (YES vs NO) is determined by a downstream LLM, so DO NOT
   try to encode direction. Match both supportive and skeptical posts equally.
4. Always include a `- @noise` exception at the end of the match block to subtract
   crypto airdrops, memecoins, job postings, and fanfiction.
```

## 8. Implementation Milestones

**Phase 1 — Foundation:**
- Crate setup, config, SQLite schema + `db.rs` queries.
- Jetstream WS client (`ingest.rs`) + extractor (`extract.rs`) with cursor persistence.
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
- `/status`, `/untrack`, `/budget`, `/override`, `/vip`, `/panic`.
- Signal / trade / error embeds.

**Verification checklist:**
- [ ] Hand-test the whole pipeline against a known market with `budget=10`.
- [ ] Confirm Manifold `limitProb=0.99` behaves as a market buy on a real test bet.
- [ ] Confirm CF AI Gateway model identifiers.
- [ ] Confirm Jetstream cursor replay on reconnect (kill -9 test).
- [ ] Confirm budget invariant holds across a buy → sell → buy cycle.
- [ ] Confirm panic switch stops all buys within 2s.
- [ ] Confirm `extract.rs` does not mutate case, punctuation, or Unicode — a post `"S.C.A.M 🚨"` reaches Sieve byte-for-byte intact; Sieve's `"scam"~` literal matches it, and `"SCAM"!` does not.
