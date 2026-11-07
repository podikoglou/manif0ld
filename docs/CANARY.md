# Strategy canary runbook

How to take S2 snipe and S3b copy from dry run to live. Behaviour is defined in `docs/SPEC.md`
(§6.6, §6.9, §6.10, §6.11, §6.12); this file is the procedure.

## 1. What is live now
- **Evaluation:** websocket prices coalesce per market per 2 s; a 5-minute REST reconcile is the only refresh when the socket is down.
- **S1 ladder:** fair-capped sweeps (`limitProb = p_internal − ε`), 60 s expiring rests, own-order lifecycle, maker-fill accounting. Ledger: `trade_ledger`.
- **S2 snipe:** built, **off** (`SNIPE_ENABLED=false`). Ledger: `snipe_ledger`.
- **S3a traders:** built, **on** (`TRADERS_ENABLED=true`), read-only.
- **S3b copy:** built, **off** (`COPY_ENABLED=false`). Ledger: `copy_ledger`.
- **S4 arb:** **no-go, not built** — see `docs/research/2026-11-07-sum-to-one-arb-probe.md`, SPEC §6.12.
- **S5 market making:** not built.

## 2. Pre-flight
All three gates must stay clean:
```sh
cargo build --release
cargo test
cargo clippy -p manif0ld --all-targets --no-deps -- -D warnings
```
`.env` key names (never paste values anywhere):
```
JETSTREAM_URL TRIAGE_MODEL ONBOARDING_MODEL
CLOUDFLARE_ACCOUNT_ID CLOUDFLARE_AI_GATEWAY_ID CLOUDFLARE_API_TOKEN OPENROUTER_API_KEY
FIRECRAWL_API_KEY MANIFOLD_API_KEY DB_PATH
DISCORD_BOT_TOKEN DISCORD_GUILD_ID DISCORD_CONTROL_CHANNEL_ID DISCORD_OPERATOR_USER_ID
```
`OPENROUTER_API_KEY` is required by an `openrouter/` or `openrouter-decisions/` model;
`CLOUDFLARE_ACCOUNT_ID` by a `cloudflare/` model. The literal `{account_id}` is rejected at startup.

Start the bot and check, in order:
1. `Manifold client ready` with a non-empty `bot_user_id` — proves `MANIFOLD_API_KEY` resolved through `GET /v0/me`.
2. `connecting to manifold broadcast socket`, `manifold broadcast socket connected`, `manifold subscription acknowledged`.
3. `reconcile pass complete` with `markets_seen`, `mismatches`, `tokens_spent`, `tokens_skipped`. Clean means `mismatches=0`; `tokens_skipped>0` only means the bucket was low.
4. `/status` answers in the Discord control channel. Run `/panic` once, confirm "Global kill switch is **ON**", then clear the kv entry (§9).
## 3. Stage 0 — dry run (now)
```ini
TRADING_DRY_RUN=true
SNIPE_ENABLED=false
COPY_ENABLED=false
TRADERS_ENABLED=true
```
Watch:
- `heartbeat` every 5 s with `messages_per_sec`, `messages_processed`, `active_markets`. A frozen counter means ingest is stuck.
- `manifold stream ended; reconnecting` / `reconnecting to manifold` with `retry_in_secs`. Occasional is normal; a tight loop is not.
- `reconcile pass complete` every 5 minutes. `mismatches > 0` is a mirror bug, not tuning.
- No snipe or copy errors are possible: both flags are off.
- Triage failures appear as `[ERROR]` embeds or `pipeline error` lines.
- `signal_ledger` grows; `trade_ledger` does not.

## 4. Stage 1 — dry run with strategies
```ini
TRADING_DRY_RUN=true
SNIPE_ENABLED=true
COPY_ENABLED=true
SNIPE_POOL_MANA=2
SNIPE_MAX_MANA=2
COPY_POOL_MANA=2
COPY_DAILY_MANA=2
COPY_MAX_MANA=2
```
Restart (flags are read once). Expect:
- Buys log `dry run: simulated buy, nothing persisted`; sells log `dry run: would sell, nothing persisted`.
- No `trade_ledger` row and no position — dry run persists no fill.
- `snipe_ledger`/`copy_ledger` get `action='SKIP'` rows. Normal reasons at these caps:
  `below_min_size` (most common, Kelly under `MIN_TRADE_SIZE=1.0`), `small_gap`, `not_informed`,
  `small_edge`, `ttl_expired`, `already_tracked`, `news_tracked`, `mcq_not_supported`,
  `stale_bet`, `dry_run`.
- A strategy passing every gate inserts its `tracked_markets` row **before** execution. Dry run
  therefore leaves `strategy='snipe'|'copy'` rows with `budget_mana=2`, no position, and they
  survive restarts. That market cannot be sniped again and stays capped at 2. Use a disposable
  database or accept it.

**Dry run cannot validate** pool arithmetic, under-lock cap clamps, partial fills, rests, maker
fills, or the copy daily cap. Those are live-only checks.
## 5. Stage 2 — live snipe only
```ini
TRADING_DRY_RUN=false
SNIPE_ENABLED=true
SNIPE_POOL_MANA=10
SNIPE_MAX_MANA=2
SNIPE_MIN_GAP=0.12
SNIPE_MIN_LIQ=100
SNIPE_KELLY_FRACTION=0.25
COPY_ENABLED=false
```
After the first snipe verify:
1. **Fill:** one `trade_ledger` row with the contract ID in `trigger_post_uri`, non-NULL `limit_prob`, real `mana_amount`. A zero fill writes only a `SKIP` row `unfilled: …`.
2. **Rest:** `buy remainder rests as an own expiring limit order` with `remainder`, `limit_prob`,
   `expires_in_secs`; it leaves the tracker within `LIMIT_EXPIRY_SECS` as
   `swept expired own resting orders without a cancel call`.
3. **Maker fill:** `maker fill applied to the book`, plus one `trade_ledger` row with trigger `maker_fill:{betId}` and no double count in `invested_mana`.
4. **Discord:** a `[TRADE]` embed with `⛏ SNIPE · gap ±x.xxx`.
5. **Pool:** committed mana never exceeds `SNIPE_POOL_MANA`:
```sql
SELECT COALESCE(SUM(invested_mana - realized_pnl_mana), 0)
FROM tracked_markets
WHERE strategy='snipe' AND status='ACTIVE' AND deleted_at IS NULL;
```
A `snipe_ledger` reason `snipe_cap: …` is the under-lock clamp: expected when two snipes race, a bug if the pool was visibly free.
**Abort on:** two consecutive round-trip exits within minutes; a pool overshoot; a `limit_prob` above the fair cap; a maker fill applied twice; a rest alive past `LIMIT_EXPIRY_SECS` + 2 minutes.

**Rollback:** `SNIPE_ENABLED=false`, restart. This stops new snipes and keeps existing rows managed (exits still fire). If a position must close now, run `/panic liquidate_all:true`. Do not `/untrack` a snipe row unless you also liquidate it.

## 6. Stage 3 — live copy
```ini
TRADING_DRY_RUN=false
SNIPE_ENABLED=false
COPY_ENABLED=true
COPY_POOL_MANA=10
COPY_DAILY_MANA=5
COPY_MAX_MANA=2
COPY_MIN_SIZE=5
COPY_MIN_INFORMED=0.60
COPY_MIN_EDGE=0.05
COPY_KELLY_FRACTION=0.125
TRADERS_ENABLED=true
```
Copy is **binary-only**. Two decision calls per candidate: `is_informed_bet ≥ COPY_MIN_INFORMED`,
then `fair_prob`, then residual edge `≥ COPY_MIN_EDGE` after the source move. MCQ triggers are
logged as `mcq_not_supported` and never traded. News-tracked markets skip as `news_tracked`;
snipe rows as `snipe_tracked`.
Verify:
- The embed shows `🤝 COPY · informed 0.72 · edge +0.08`; `p_informed` and the signed residual `gap` are in `copy_ledger`.
- Maker fills count against the day: they write `BUY_*` with `trader_id='resting_fill'`. This must stay under `COPY_DAILY_MANA`:
```sql
SELECT COALESCE(SUM(mana_amount), 0) FROM copy_ledger
WHERE action IN ('BUY_YES','BUY_NO') AND created_at >= strftime('%s','now') - 86400;
```
- No `BUY_*` row points at a `news` row.
- A watched trader's exit logs `copied trader exited; the market will be reconsidered`, cancels our rests, and runs one evaluation. It never forces a sale; the normal exit edge decides.

**Rollback:** `COPY_ENABLED=false`, restart. Existing copy rows stay managed. Use `/panic liquidate_all:true` only to close now.

## 7. Global guardrails
- **Budget:** every buy asserts `invested_mana ≤ budget_mana + realized_pnl_mana`; a violation refuses the trade and alerts Discord. Never raise a budget to clear it mid-canary.
- **`/panic`:** sets `global_kill_switch`, blocks every buy including news, and lets sells run.
  Buys should stop within 2 s. There is no un-panic command — clear the kv key directly.
  `liquidate_all:true` also sells everything. Clear it with
  `sqlite3 <DB_PATH> "DELETE FROM kv WHERE key='global_kill_switch';"` (only the literal `true` blocks).
- **Rate bucket:** `RATE_LIMIT_PER_MIN` (default 500) is global. Housekeeping leaves a 12% reserve; trades may drain to a 1% floor. A trade refusal during a burst is the floor working.
- **Socket down:** the 5-minute reconcile is the only refresh. Snipe and copy triggers are
  socket-only, so no new entries occur, but existing positions still exit. On repeated
  `manifold websocket has been closed for a reconcile cycle; running on the 5-minute REST backstop`, set both strategy flags off.
- **Defaults:** all strategy flags are off and `TRADING_DRY_RUN=true`; a missing variable never enables live trading.

## 8. Tuning loop
Read the skip rows; they exist for this.
- `snipe_ledger`: tune `SNIPE_MIN_GAP` from `gap`. A wall of `small_gap` at 0.12 that clears at 0.10 means lower the gate one step. Use `p_initial` and `p_model` for calibration; raise `SNIPE_MIN_LIQ` if fills move price too far.
- `copy_ledger`: tune `COPY_MIN_INFORMED` from `p_informed` and `COPY_MIN_EDGE` from `gap`. Many `not_informed` means a noisy watch list; many `small_edge` means the move is consumed. Tune `COPY_KELLY_FRACTION` only after a win rate exists.
- `trade_ledger`: read `limit_prob` against `p_internal`, plus `mana_amount`, `shares_delta`, and `trigger_post_uri` (`maker_fill:` prefix).
- Change one value per restart and hold each stage at least a day, or a full copy daily-cap window.

## 9. Rollback table
Config is read at startup; every flag change needs a restart.

| Action | Effect | Restart? |
|---|---|---|
| `TRADING_DRY_RUN=true` | Buys simulated; sells logged; no rows | Yes |
| `SNIPE_ENABLED=false` | No new snipes; open rows managed | Yes |
| `COPY_ENABLED=false` | No new copies or exit signals; open rows managed | Yes |
| `TRADERS_ENABLED=false` | Refresh stops; copy keeps the old board | Yes |
| Lower a pool/cap | New orders shrink; open rows keep their frozen budget | Yes |
| `/panic` | Global buy kill switch | No |
| `/panic liquidate_all:true` | Kill switch plus sell sweep | No |
| `/untrack market:<id> liquidate:true` | Sells and hides the row | No |
`/untrack` is not a strategy switch. Strategy rows are hidden from `/status` and autocomplete,
so you must pass the raw market ID. `liquidate:false` hides a row that still holds a position,
removing it from the executor's active set. It does not allow a re-snipe: `already_tracked` also
checks soft-deleted rows. Prefer the flag switch.

## 10. Known limitations
- S4 is dead (deficit re-arbed in the same transaction); S5 is unbuilt.
- Copy is binary-only; MCQ copy is unsupported.
- `SNIPE_TTL_SECS` gates only the first evaluation; it never expires a row, caps a hold, or cancels an order.
- A snipe refused after passing every gate (kill switch, cap, rate floor, dry run, zero fill) leaves a permanent unsnipoable row. No retry exists.
- Wide sum-to-one fields are refused as `answer_field_too_wide`.
- Dry run cannot exercise caps, fills, rests, maker fills, or the copy daily cap; only live stages prove those.
- Copy exits are a signal, a cancel, and one evaluation — never a forced sale.
