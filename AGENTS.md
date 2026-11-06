# AGENTS.md

Rust project: **Bluesky → Manifold auto-trader**. It streams the Bluesky
Jetstream firehose, filters posts per tracked market with the **Sieve** DSL
engine (`sieve/`, a path dependency), calls one small LLM per surviving post
via Cloudflare Workers AI, keeps a log-odds belief per market, and trades on
Manifold only on strong edge. Discord is the only control plane.

- **Full design spec, query contracts, and milestone history: `docs/SPEC.md`.**
- **Rendered architecture overview: `docs/OVERVIEW.tex` / `OVERVIEW.pdf`.**

Read `docs/SPEC.md` when a task touches trading, Discord commands, Cloudflare
LLM calls, or reconciliation. It is the source of truth for the DSL reference
and API contracts.

## Invariants to respect

- **Single binary, single tokio runtime.** Sieve matching is in-process; do
  not add workers, queues, or services for it.
- **Extraction ≠ normalization.** `src/extract.rs` copies text verbatim (body
  + image alts + quoted text). Never lowercase, strip, NFKC, or leet-expand
  there — the Sieve engine normalizes internally. Text that must be matched
  differently changes in the Sieve rule, not in `extract.rs`.
- **Filter-first.** One decision-model call per surviving post; the pipeline
  exists to make that call rare.
- **Onboarding is a rig agent.** It researches with Firecrawl and submits Sieve
  programs through a tool. Do not reintroduce a fixed attempt loop.
- **Per-market isolated budgets.** `available_budget = budget + realized_pnl
  − invested`; never exceed `budget_mana` net.
- **No autonomous market discovery.** Markets enter only via the operator
  `/track` command.
- **Cloudflare LLM calls use `/ai/run`** through the vendored `rig`
  Cloudflare provider with `cf-aig-gateway-id` and `CLOUDFLARE_API_TOKEN`,
  not the legacy gateway/compat route (see `docs/SPEC.md` §4 and §6.4). Never
  replace that provider with a hand-rolled client; patch the `rig/` vendor if
  the wire must change.
- **Triage is a decision model, not a chat model.** It goes through
  `rig::typesafeai` to OpenRouter's `POST /api/alpha/decisions`
  (`TRIAGE_MODEL=openrouter-decisions/...`), in-process, with the question
  wording in `src/triage.rs` only. Do not route it through chat completions
  or add a second HTTP client beside the vendored `rig`.
- **Position truth = Manifold `/positions`**, and the bot user ID comes from
  `/v0/me` — there is no bot-user env var (`docs/SPEC.md` §4 and §6.6).

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
- Feature/behavior change: read `docs/SPEC.md` first, then the affected
  module. Most cross-cutting work touches `src/pipeline.rs` (orchestration and
  `PipelineEvent`), `src/discord.rs` (embeds/commands/buttons), and
  `src/trade.rs` (`TradeExecutor`). Wire new long-running loops in
  `src/main.rs` and abort them on Ctrl-C.
- Something confusing: the spec is the source of truth. Update it only when an
  architectural decision actually changes.