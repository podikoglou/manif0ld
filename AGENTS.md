# AGENTS.md

Rust project: **Bluesky → Manifold auto-trader**. It streams the Bluesky
Jetstream firehose, filters posts per tracked market with the **Sieve** DSL
engine (`sieve/`, a path dependency), calls one small LLM per surviving post
via Cloudflare Workers AI, keeps a log-odds belief per market, and trades on
Manifold only on strong edge. Discord is the only control plane.

- **Full design spec, query contracts, and milestone history: `docs/SPEC.md`.**
- **Operational handoff, verified decisions/deviations, run instructions,
  pending live checks: `docs/HANDOFF.md`.**

Read `docs/HANDOFF.md` first when a task touches trading, Discord commands,
Cloudflare LLM calls, or reconciliation. Read `docs/SPEC.md` for the DSL
reference and API contracts.

## Invariants to respect

- **Single binary, single tokio runtime.** Sieve matching is in-process; do
  not add workers, queues, or services for it.
- **Extraction ≠ normalization.** `src/extract.rs` copies text verbatim (body
  + image alts + quoted text). Never lowercase, strip, NFKC, or leet-expand
  there — the Sieve engine normalizes internally. Text that must be matched
  differently changes in the Sieve rule, not in `extract.rs`.
- **Filter-first.** One LLM call per surviving post; the pipeline exists to
  make that call rare.
- **Per-market isolated budgets.** `available_budget = budget + realized_pnl
  − invested`; never exceed `budget_mana` net.
- **No autonomous market discovery.** Markets enter only via the operator
  `/track` command.
- **Cloudflare LLM calls use `/ai/run`** through the vendored `rig`
  Cloudflare provider with `cf-aig-gateway-id` and `CLOUDFLARE_API_TOKEN`,
  not the legacy gateway/compat route (see `docs/HANDOFF.md` §5.1). Never
  replace that provider with a hand-rolled client; patch the `rig/` vendor if
  the wire must change.
- **Position truth = Manifold `/positions`**, and the bot user ID comes from
  `/v0/me` — there is no bot-user env var (`docs/HANDOFF.md` §5.2–5.3).

## Build and check

```sh
cargo build --release
cargo test
cargo clippy -p bsky-manifold-arb --all-targets --no-deps -- -D warnings
```

All three must stay clean.

## Conventions

- Never commit `.env`, target dirs, or `sieve/target` / `sieve/Cargo.lock`.
- `sieve/` is a nested repo whose sources are currently **untracked** in this
  repo; the build depends on it, so do not remove or ignore it.
- Use conventional, small atomic commits (one per logical change). Don't batch.
- Manifold times are milliseconds; this codebase stores seconds — convert at
  the boundary in `src/trade.rs`.

## Workflow

- Small tweak (a button label, a log line, a query change): edit the single
  module, run the gates above.
- Feature/behavior change: read `docs/HANDOFF.md` first, then the affected
  module. Most cross-cutting work touches `src/pipeline.rs` (orchestration and
  `PipelineEvent`), `src/discord.rs` (embeds/commands/buttons), and
  `src/trade.rs` (`TradeExecutor`). Wire new long-running loops in
  `src/main.rs` and abort them on Ctrl-C.
- Something confusing: the spec and handoff are the source of truth. Update
  them only when an architectural decision actually changes.