# Handoff: `bsky-manifold-arb`

Status: **all spec milestones implemented, unit tests green (168), live verification pending.**
Written: 2026-11-02. For the next operator or engineer who takes over this repo.

Full product spec: `docs/SPEC.md`. This document is the operational handoff.

## 1. What this is

A personal Bluesky → Manifold auto-trader. It reads the Bluesky Jetstream
firehose, filters posts through per-market Sieve rules, calls one small LLM
per surviving post, updates a log-odds belief per market, and trades on
Manifold when the edge is strong. Discord is the only control plane.

The product value is the **filter**: Sieve + one LLM call per post is the
whole pipeline. Everything else is minimal infrastructure.

---

## 2. Repository layout

Single Rust crate plus the Sieve engine it depends on.

| Path | Purpose |
|---|---|
| `src/main.rs` | Startup, task spawning, graceful shutdown |
| `src/config.rs` | Env-based configuration, Cloudflare URL helpers |
| `src/error.rs` | `anyhow` re-exports |
| `src/types.rs` | Domain types (`TrackedMarket`, `Direction`, records, …) |
| `src/db.rs` | SQLite (rusqlite) access layer |
| `src/ingest.rs` | Jetstream websocket, reconnect, cursor persistence |
| `src/extract.rs` | Jetstream event → `ExtractedPost` (structural only) |
| `src/sieve.rs` | `EngineRegistry` (one compiled Sieve per market) + VIP bypass |
| `src/triage.rs` | `WorkersAiClient` transport + LLM triage call/parse |
| `src/belief.rs` | Log-odds belief, time decay, impulse |
| `src/trade.rs` | Manifold client, Kelly sizing, `TradeExecutor`, position sync |
| `src/onboarding.rs` | `/track`: fetch market, LLM Sieve synthesis, VIP resolution |
| `src/pipeline.rs` | Orchestration: match → triage queue → belief → ledger → events |
| `src/discord.rs` | Bot, commands, buttons, embeds, event consumers |
| `migrations/001_init.sql` | Schema (applied automatically at startup) |
| `sieve/` | Sieve DSL engine (`CompiledSieve::compile` / `.matches`) — **own git repo, currently untracked** |
| `docs/SPEC.md` | The design spec this implementation follows |

Key dependency versions (in `Cargo.toml`): `tokio`, `tokio-tungstenite 0.30`,
`reqwest 0.12`, `rusqlite 0.41`, `serenity 0.12.5`, `tracing`,
`chrono`, `sieve` (path dependency). No `rig`; see §5.4.

---

## 3. How to run

1. Copy `.env.example` to `.env` and fill every value. See `src/config.rs`
   for the parser and defaults.
2. Build: `cargo build --release`. The `.cargo/config.toml` sends the build
   target dir to `/tmp/opencode/bsky-manifold-arb-target` to save disk.
3. Run: `cargo run --release`. Default `RUST_LOG` level is `info`.
4. Stop: Ctrl-C aborts all spawned tasks.

Startup order (in `main.rs`):
1. Load `Config`.
2. Open SQLite (`DB_PATH`, default `./data/arb.db`), apply schema.
3. Compile one Sieve engine per `ACTIVE` market in the DB; invalid rules are
   logged and skipped.
4. Build `MarketCache`, `Pipeline`, `OnboardingService`, `TradeExecutor`.
5. Spawn: ingest (Jetstream), pipeline, trade executor, stdout event logger,
   Discord gateway + event consumers.

There is no feature flag to disable trading. If `MANIFOLD_API_KEY` is wrong,
startup fails (the bot ID is fetched from `/v0/me` and that call validates the
key).

---

## 4. Data flow

```
Jetstream WS ─► ingest ─► extract ─► pipeline (per market: Sieve match | VIP)
                                        │
                                        ▼
                               per-market bounded queue (30, drop-oldest)
                                        │
                                        ▼
                               triage LLM (Workers AI /ai/run)
                                        │
                                        ▼
                               belief update (log-odds + 4h decay)
                                        │
                                        ▼
                               signal_ledger ─► PipelineEvent ─► Discord [SIGNAL]
                                        │
                                        ▼
                               TradeExecutor (edge check) ─► Manifold
                                        │                      bet | sell | sell-all
                                        ▼
                               trade_ledger ─► TradeEvent ─► Discord [TRADE]

Pipelines ticks every 60s per active market (fresh prob + position, decide).
```

### Bounded queues
- Ingest → pipeline broadcast channel: capacity 1024, drop-oldest.
- Per-market triage queue: capacity 30, drop-oldest (`RecvError::Lagged`),
  one sequential worker per market so belief updates stay in post order.
- PipelineEvent / TradeEvent broadcast channels: 1024 / 256.
- Queue depth is exposed via `/status` and `queue_depth(s)`.
- `extract.rs` is the only module that touches raw event JSON. It never
  normalizes text (no case fold, no NFKC, no leetspeak). The Sieve engine
  does all lexical normalization internally. See the unit tests.

---

## 5. Design decisions and deviations from the spec

These are deliberate; read them before changing things.

### 5.1 Cloudflare: Workers AI REST endpoint, not the old gateway route
The spec §4 note suggested `gateway.ai.cloudflare.com/.../compat`. The env
actually has `CF_AI_GATEWAY_URL`, `CF_AI_GATEWAY_ID`, and `CF_API_KEY`, so the
implementation uses:

```
POST https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/run
Authorization: Bearer {CF_API_KEY}
cf-aig-gateway-id: {CF_AI_GATEWAY_ID}
{"model": "{TRIAGE_MODEL}", "input": {"messages": [...]}}
```

- `{account_id}` is parsed from the last path segment of `CF_AI_GATEWAY_URL`
  (`Config::cf_account_id`); startup fails if the `.env.example` placeholder
  `{account_id}` is still present.
- **`CF_API_KEY` is the same credential the old `CF_AI_GATEWAY_TOKEN` var was
  meant to hold.** The name was changed.
- Target route is any Workers AI account that exposes `/ai/run` for universal
  models. OpenAI-compatible routing (`/ai/v1/chat/completions`) is noted as a
  fallback in the triage module docs if a model is not available on `/ai/run`.

### 5.2 Manifold: positions, not bets, for reconciliation
Spec §6.6 said to reconcile via `GET /v0/bets`. That cannot distinguish held
shares from shares sold later, so `trade.rs` uses
`GET /v0/market/{id}/positions?userId={bot}` and trusts `totalShares` /
`invested`. `/v0/prob` is NOT used; market probability comes from
`GET /v0/market/{id}`. `realized_pnl_mana` stays local (Manifold `profit`
mixes realized + unrealized).

### 5.3 No `MANIFOLD_BOT_USER_ID` env var
There is no such thing; the bot user ID is fetched from `GET /v0/me` at
startup by `TradeExecutor::new`.

### 5.4 No `rig`
`rig` 0.46 has no Workers AI `/ai/run` provider, and its OpenAI-compatible
client does not emit the `{model, input:{messages}}` envelope. The LLM calls
are one stateless JSON POST, so `reqwest` is used directly. Rationale in the
`triage.rs` module doc.

### 5.5 Sieve engine is `Send + Sync` via a newtype
Cranelift's `JITModule` is conservatively `!Send + !Sync`. `src/sieve.rs`
wraps `CompiledSieve` in a `SendEngine` newtype (unsafe impl, documented) so
the registry can be shared across tokio tasks. The engine is immutable after
compilation; `matches` is read-only. This is the one `unsafe` in the codebase.

### 5.6 Budget / kill-switch semantics
- Available budget = `budget_mana + realized_pnl_mana − invested_mana`.
- Buys are refused when `global_kill_switch` kv = `"true"` (or when the
  budget invariant would break). Sells are never blocked.
- `realized_pnl_mana` is NOT overwritten from Manifold `profit` (§5.2).
- Market probability is clamped to `[0.01, 0.99]` wherever log-odds or Kelly
  denominators are computed.

### 5.7 Other
- `extract.rs` handle resolution: Jetstream events carry no handle, so DIDs
  are used; `extract_with_handle` exists for a future handle cache.
- Prices/times: Manifold uses ms; the DB stores seconds. Convert at the
  boundary (`trade.rs` `millis_to_seconds`).
- Thread names truncate the question to 50 chars.
- Onboarding Sieve synthesis runs as a multi-turn compile loop (up to 5
  attempts), feeding each compiler error back as the next message so the model
  auto-corrects its rule; final failure includes the last LLM output.

---

## 6. Discord control plane (`/` commands, operator-only)

All registered under `DISCORD_GUILD_ID`. Operator = `DISCORD_OPERATOR_USER_ID`.

| Command | Params | Behavior |
|---|---|---|
| `/track` | `url`, `budget?` (default `DEFAULT_MARKET_BUDGET`), `initial_prob?` (1–99) | Fetch market, LLM Sieve synthesis, compile, resolve VIPs, create thread `📈 [M{budget}] {short}`, post `[ONBOARDED]` embed with buttons |
| `/untrack` | `market` (autocomplete), `liquidate?` (default true) | Liquidate, unregister engine, delete rows, archive thread |
| `/budget` | `market`, `new_budget` | Update budget, refresh cache |
| `/status` | `market?` | Overview table or per-market detail incl. Sieve source + last 10 signals |
| `/override` | `market`, `probability` (1–99) | Set internal probability, trigger one trade check |
| `/vip` | `market`, `handle` | Resolve handle → DID via Bluesky, insert VIP |
| `/panic` | `liquidate_all?` (default false) | Set kill switch `"true"` (+ optional full liquidation) |

Buttons on the ONBOARDED embed: `⏸ Pause / ▶ Resume` toggles
ACTIVE/PAUSED; `💰 Liquidate & Close` sells all, marks CLOSED, archives thread.

Embeds: `[ONBOARDED]`, `[SIGNAL]`, `[TRADE]`, `[ERROR]`.

Note: there is **no `/un-panic`**. Clearing the kill switch means editing the
`global_kill_switch` row in the SQLite `kv` table (or restarting with the row
deleted). Documented in `discord.rs`.

---

## 7. Verification status

### Done
- `cargo test`: 168 passed, 0 failed.
- `cargo clippy -p bsky-manifold-arb --all-targets --no-deps -- -D warnings`: clean.
- `cargo build --release`: builds.
- Unit coverage includes: `extract.rs` byte-for-byte preservation (`"S.C.A.M 🚨"`,
  leetspeak, zero-width), Sieve case/leet behavior (raw text unchanged),
  belief decay/half-life/clamping, Kelly sizing, buy/sell accounting,
  positions parsing, cursor tracker, queue depth accounting, Discord helpers
  (option parsing, thread naming, autocomplete, button id round-trip),
  onboarding slug/validation/synthesis parsing/VIP selection.
- Verified against Manifold docs (2026): `/v0/me`, `/v0/market/{id}`,
  `/v0/bet` limitProb, `/v0/market/{id}/sell`, positions, ms timestamps.

### Pending — live checklist (needs real credentials and a test market)
1. Confirm `TRIAGE_MODEL` and `ONBOARDING_MODEL` identifiers exist on the
   account (the `.env` values are placeholders and likely wrong).
2. Smoke-test `/track` on a real binary market with `budget=10`; review the
   generated Sieve rule in the thread embed.
3. Confirm `limitProb=0.99` (YES) / `0.01` (NO) fills as a market buy on a
   real test bet.
4. Jetstream cursor replay: run, `kill -9`, restart, confirm no post gap.
5. Budget invariant across buy → sell → buy.
6. `/panic` stops buys within ~2s.

---

## 8. Known gaps / things to watch

- **`sieve/` is an untracked nested git repo.** The build depends on it as a
  workspace member. Commit its sources to this repo so it is self-contained
  (`sieve/target` and `sieve/Cargo.lock` are already gitignored). Currently
  `git status` shows `?? sieve/`.
- **Kill switch clearing** is manual (§6). A `/resume` command is a small
  addition if desired.
- **Handle resolution** falls back to DIDs (§5.7). VIP bypass works by DID.
  Bluesky handles in `[SIGNAL]` embeds may be DIDs for non-VIP authors.
- **Bluesky account cache** in `ingest.rs`: exclude-self filtering is minimal;
  the bot processes all `app.bsky.feed.post` creations, including the
  operator's own posts.
- **`/status` market probability** is the cached `p_market_last`, updated on
  signals and the 60s ticker, not fresh per call.
- **Rate limits**: Manifold 500 req/min, Bluesky public API applies to VIP
  lookup; the executor serializes trades per process with a tokio mutex and
  emits per-market errors without stopping.
- Arguably the biggest risk: a token with a wrong `DISCORD_CONTROL_CHANNEL_ID`
  or an autocomplete-heavy guild command set will make interaction handlers
  fail visibly but non-fatally (logged).

---

## 9. Useful commands

```sh
# Run everything
cargo run --release

# Tests (no network)
cargo test

# Lint gate used for every phase
cargo clippy -p bsky-manifold-arb --all-targets --no-deps -- -D warnings

# Inspect state
sqlite3 ./data/arb.db '.tables'
sqlite3 ./data/arb.db 'select id,slug,status,budget_mana,invested_mana from tracked_markets;'
sqlite3 ./data/arb.db 'select key,value from kv;'   # jetstream_cursor_us, global_kill_switch
```

Phase history: see `git log`. Each spec milestone (§8) maps to one or two
commits with conventional messages.