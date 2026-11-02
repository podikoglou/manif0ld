# bsky-manifold-arb

Personal Bluesky → Manifold auto-trader. The core value is filtering:
it turns the Bluesky Jetstream firehose into a small number of posts that
matter for tracked prediction markets, then triages and trades on them.

See [docs/SPEC.md](docs/SPEC.md) for the full specification.

## Status

Implementation is in progress. See `docs/SPEC.md` §8 for milestones.

## Setup

1. Copy `.env.example` to `.env` and fill in the values.
2. Build:

   ```sh
   cargo build --release
   ```

3. Run:

   ```sh
   cargo run --release
   ```

Data is stored in SQLite at `DB_PATH` (default `./data/arb.db`). The schema
is applied automatically from `migrations/001_init.sql` at startup.

## Layout

| Path | Purpose |
|---|---|
| `src/ingest.rs` | Jetstream websocket, reconnect, cursor persistence |
| `src/extract.rs` | Jetstream event → `ExtractedPost` (structural only) |
| `src/sieve.rs` | Per-market Sieve engine registry |
| `src/triage.rs` | Cloudflare AI Gateway triage call |
| `src/belief.rs` | Log-odds belief state and decay |
| `src/trade.rs` | Manifold client, Kelly sizing, order manager |
| `src/onboarding.rs` | `/track` market onboarding and Sieve synthesis |
| `src/pipeline.rs` | Post → match → triage → belief → trade orchestration |
| `src/discord.rs` | Discord control plane |
