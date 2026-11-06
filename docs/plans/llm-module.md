# Plan: move the LLM chat transport into `llm.rs` (candidate 6, §7F)

Status: proposed. This is candidate 6 (`docs/ARCHITECTURE.md` §7F), after
`docs/plans/market-store.md` (1), `docs/plans/book.md` (2),
`docs/plans/control-plane.md` (4), and `docs/plans/app-runtime.md` (7J).

All line numbers are `src/` at `0abfd25` unless a commit is named.

The review text for this candidate is stale in five places. This plan is written
against the code, and §1.7 lists every point where the review and the code
disagree.

## 1. Problem

The chat transport that onboarding runs on lives inside the triage feature. Triage
does not use it.

1. **The transport is 177 lines of `triage.rs`, in three regions.** It is not one
   item; it is seven blocks:

   | Item | Lines |
   |---|---|
   | `LLM_CONNECT_TIMEOUT` (20 s) | `55-56` |
   | `LLM_READ_TIMEOUT` (45 s) | `58-64` |
   | `ONBOARDING_REQUEST_TIMEOUT` (120 s) | `66-74` |
   | `LlmClient` + doc | `605-613` |
   | `chat_transport` + doc | `615-628` |
   | `impl LlmClient` (`new`, `completion_model`, `for_test`) | `630-714` |
   | `keepalive_stream_ends_on_the_request_deadline` | `724-770` |

   Every one of them exists for onboarding: `chat_transport` sets a connect
   timeout, a read timeout, and a total deadline; `LlmClient::new` reads the
   provider credentials; `completion_model` builds the model that
   `onboarding_agent::synthesize` hands to the `rig` agent.

2. **`LlmClient` has exactly one production consumer, and it is not triage.** All
   four call sites are in `src/onboarding.rs`:

   | Site | Line | Use |
   |---|---|---|
   | field type of `OnboardingService` | `256` | `ai: LlmClient` |
   | constructor | `273` | `ai: LlmClient::new()?` |
   | model build | `427` | `self.ai.completion_model(&self.model)?` |
   | test constructor | `687` | `ai: LlmClient::for_test()` |

   `rg -n "LlmClient" src/` returns `triage.rs` (the definition) and those four
   lines, nothing else.

3. **Onboarding is forced to import from a feature it does not belong to.**

   ```rust
   // src/onboarding.rs:33
   use crate::triage::{snippet, LlmClient};
   ```

   Onboarding has no other triage dependency. The import exists because the
   transport was parked in the module that used to own every LLM call.

4. **The module name is now misleading in both directions.** `triage.rs` performs
   no chat completion at all. Its own module doc says so at line `23`:

   > `//! [`LlmClient`] stays for onboarding, which is still a chat agent run.`

   and `TriageClient` (`335-339`) holds `client: Jev` — a decisions client from
   `rig::typesafeai` — plus `model` and `policy`. There is no `LlmClient` field.
   A reader who opens `triage.rs` to understand the LLM layer finds a decision
   model, a chat transport it never calls, and a byte-truncation helper it never
   calls.

5. **`snippet` is a non-LLM helper with no caller inside `triage.rs`.**

   ```
   rg -n "snippet" src/triage.rs
   # 593 (the definition), 1138, 1140, 1141, 1142 (its own test)
   ```

   Its two real callers are `onboarding.rs:610` and `onboarding.rs:614`, both
   bracketing HTTP error bodies and JSON parse failures from the Manifold lookup.
   It is `pub(crate)` only so that another module can reach it, which is how a
   private helper becomes API.

6. **The deletion test.** Delete `src/llm.rs` and the complexity does not vanish:
   it reappears as `use crate::triage::{LlmClient, snippet}` in every module that
   wants a chat model. Onboarding is the only such module today, so the coupling
   is one line — but that line says "a market workflow depends on the triage
   feature", which is false. The transport has one job (build a `rig` completion
   model with real deadlines), one consumer, and no knowledge of triage; it
   belongs in a module named after that job.

7. **What the review got wrong.** §7F (`docs/ARCHITECTURE.md:1033-1044`) and §4
   (`:447-474`) describe a `triage.rs` that no longer exists:

   | Review claim | Reality at `0abfd25` |
   |---|---|
   | `onboarding` imports `LlmClient`, `extract_json_object`, and `snippet` from `triage` | `extract_json_object` does not exist anywhere in `src/`. Onboarding imports `LlmClient` and `snippet` only (`onboarding.rs:33`). |
   | `llm.rs` holds `LlmClient` + JSON helpers | There are no JSON helpers to move. Triage parses no JSON by design (`triage.rs:7`, `docs/SPEC.md:362`). |
   | `pub struct TriageClient { client: LlmClient, model: String }` (§4, line `456`) | `pub struct TriageClient { client: Jev, model: String, policy: TriagePolicy }` (`335-339`). |
   | Triage "keeps the prompt, model choice, `TriageResult` parsing, and `is_material`" | `build_prompt`, `build_multiple_choice_prompt`, `parse_triage_response`, `is_material`, and `TriageResult` are all gone. The module keeps the typed question builders, `assess`, and `TriageOutcome`/`TriageAssessment`. |
   | The target direction is `triage → llm` **and** `onboarding → llm` (§7F, line `1044`) | Only `onboarding → llm` exists. Triage never used the chat transport and does not gain an edge to it. See decision 5. |

   Item 5 above lists the code the review treats as shared. `snippet` is the only
   genuinely shared-looking item, and it is not shared either: both call sites
   are in `onboarding.rs`.

## 2. Decisions taken

1. **New module `src/llm.rs` holding `LlmClient`, `chat_transport`, and the three
   LLM deadlines.** The code moves verbatim: same env handling through
   `OpenAIConfig::from_env_with`, same provider order (Cloudflare first, then
   OpenRouter), same `transport.clone()` split, same error text, same `for_test`
   shape and visibility. `main.rs` gains `mod llm;` in its alphabetical list,
   between `mod ingest;` and `mod market_store;`. Trade-off: one more module (19
   in total, ~200 lines including its one test) for a transport that already had a
   home; the alternative leaves onboarding importing from triage forever, because
   nothing else forces the split.

2. **`snippet` and `ERROR_SNIPPET_BYTES` move into `onboarding.rs` as private
   items**, together with `snippet_truncates_on_a_character_boundary`. Verified:
   the only callers are `onboarding.rs:610` and `onboarding.rs:614`. The function
   is not about LLMs — it truncates any HTTP error body to a byte budget on a
   character boundary — so it goes next to `get_json`, which is its only reader.
   This removes a `pub(crate)` helper instead of creating a shared one.
   Trade-off: `onboarding.rs` grows by about 25 lines and the constant sits ~560
   lines from its reader, which is the same shape `trade.rs` already uses (`const
   ERROR_SNIPPET_CHARS` at `69`, `fn snippet` at `1207`).

3. **Do not merge `trade::snippet` with the moved `snippet`.** They are not the
   same function:

   | | `onboarding::snippet` (moved) | `trade::snippet` (`1207`) |
   |---|---|---|
   | budget unit | bytes (`ERROR_SNIPPET_BYTES = 500`) | characters (`ERROR_SNIPPET_CHARS = 500`) |
   | leading whitespace | trimmed | kept |
   | suffix | `…` when truncated by bytes | `…` when characters exceed the budget |

   Merging changes the error text of one of the two paths, which decision 6
   forbids. Recorded as a residual in §9. Trade-off: two near-identical helpers
   persist, so a reader must know which one a given call site uses — the price of
   keeping both error texts unchanged.

4. **`triage.rs` keeps everything about the decision model**: `TriageClient`,
   `DECISIONS_ENDPOINT`, `DECISIONS_TIMEOUT`, `MIN_STRENGTH`, `MIN_IMPULSE`, the
   question strings and builders (`binary_questions`, `choice_questions`,
   `strength_question`, `STRENGTH_LEVELS`, `NONE_ALTERNATIVE`), the state
   builders, `Polarity`, `EvidenceLevel`, `Target`, `TriageOutcome`,
   `TriagePolicy`, `TriageAssessment`, `assess`, `outcome_from_binary`,
   `outcome_from_choice`, `cost`, and `format_timestamp`. It drops the chat
   client, its transport, its three deadlines, the test-module imports that only
   the keepalive test used, and the two tests that move. Its module doc loses the
   "`LlmClient` stays for onboarding" paragraph (`22-24`) and gains one sentence
   pointing at `crate::llm`. Trade-off: two modules now mention the LLM layer, so
   a reader must know that "decisions" and "chat" are different transports; §4
   states that split in both module docs.

5. **Report the real dependency graph.** After the move, `triage` does not import
   `llm` — it never used it — so the only new edge is `onboarding → llm`, and
   `llm` is a leaf over `config`, `error`, and `rig`. The review's `triage → llm`
   arrow is not drawn, because a diagram that shows a dependency the code does
   not have is worse than no diagram. Trade-off: the docs stop promising a shared
   LLM layer that triage will eventually join, so a future chat-driven triage has
   to add that edge as an explicit decision instead of inheriting a claim.

6. **No behaviour change.** Same environment variables, same two providers, same
   fallback rule, same three deadlines, same error strings down to the
   interpolation order, same call sites with the same receivers, same `for_test`.
   §6 argues this claim item by item. Trade-off: the move fixes the module
   boundary only — it does not shrink the transport, add a deadline knob, or
   improve coverage, so the three known weaknesses in §9 stay as they are.

7. **Tests: move, do not rewrite.** The two moved tests keep their bodies and
   their assertions. Census stays at **275** for the crate, `triage.rs` goes
   `25 → 23`, `onboarding.rs` goes `12 → 13`, `llm.rs` starts at `1`. No test is
   added and none is deleted, because this candidate has no new behaviour to pin.
   Trade-off: the transport's only coverage is one test written for a deadline, so
   a regression in provider construction would be caught by the compiler and by
   the first live `/track`, not by the suite. Inventing a second test for a pure
   move would assert the code you just copied, which is not coverage.

## 3. The model

### What each module owns after the change

| Module | Owns | Explicitly does not own |
|---|---|---|
| `llm.rs` | the chat transport: `LlmClient`, `chat_transport`, `LLM_CONNECT_TIMEOUT`, `LLM_READ_TIMEOUT`, `ONBOARDING_REQUEST_TIMEOUT` | prompts, JSON, retries, feature rules, HTTP error formatting |
| `triage.rs` | the decision model: `TriageClient`, `DECISIONS_ENDPOINT`, `DECISIONS_TIMEOUT`, the four questions and their wording, `Polarity`/`EvidenceLevel`/`Target`, `TriageOutcome`/`TriagePolicy`/`TriageAssessment`, `assess`, the answer→outcome conversions, `format_timestamp` | any chat client, any `rig` completion model, `snippet` |
| `onboarding.rs` | the market workflow (`parse_slug`, `validate_market`, `get_json`, `prepare`, `activate`, the Sieve prompt) **plus** the error-body helper it alone uses: `ERROR_SNIPPET_BYTES`, `snippet` | the LLM transport itself; it holds `ai: LlmClient` by value |
| `config.rs` | `Config`, `ModelProvider`, `parse_model_spec` — unchanged | — (it is now the only shared vocabulary between the two LLM modules) |

### Dependency edges

Before:

```
onboarding ──> triage      (LlmClient, snippet)         onboarding.rs:33
pipeline   ──> triage      (Target, TriageClient)       pipeline.rs:43
triage     ──> config, error, types, rig
```

After:

```
onboarding ──> llm         (LlmClient)                 onboarding.rs:33   [replaces onboarding ──> triage]
onboarding ──> triage      removed entirely
pipeline   ──> triage      unchanged                   pipeline.rs:43
triage     ──> config, error, types, rig               unchanged; no edge to llm
llm        ──> config      (parse_model_spec, ModelProvider)
llm        ──> error       (anyhow, bail, AppResult, Context)
llm        ──> rig         (providers::openai, providers::cloudflare, http_client, rig_reqwest)
main       ──> llm         (mod declaration only)
```

`llm` imports no domain type: no `types`, no `db`, no `sieve`, no `market_store`.
That is the property the split buys — the transport can be read without knowing
what a `TrackedMarket` is.

The old `onboarding → triage` edge disappears; the review predicted two edges into
`llm` and the code supports one.

## 4. Interface

### `src/llm.rs`

```rust
//! Chat-model transport for onboarding, built on the vendored `rig`.
//!
//! [`LlmClient`] holds one `rig` client per chat provider and builds the
//! completion model that [`crate::onboarding_agent`] hands to
//! [`rig::AgentBuilder`]. Triage does not use this module: it is a decision
//! model and reaches OpenRouter's `/api/alpha/decisions` through
//! [`rig::typesafeai`] in [`crate::triage`].
//!
//! # One transport, three deadlines
//!
//! Both providers share one reqwest client built by [`chat_transport`]. rig's
//! bundled client has no request timeout at all, so the transport sets a
//! connect timeout, a read timeout, and a total deadline. The total deadline is
//! the one that ends a stalled streamed turn: OpenRouter holds a queued request
//! open with `: OPENROUTER PROCESSING` comment lines, and every comment is a
//! read that resets the read timeout.
//!
//! # What this module must not become
//!
//! Pure transport: provider construction and completion models. JSON helpers,
//! prompts, response parsers, retries, and feature logic belong to the feature
//! module that needs them — `snippet`, for example, lives in
//! [`crate::onboarding`], its only caller.

use std::time::Duration;

use crate::config::{parse_model_spec, ModelProvider};
use crate::error::{anyhow, bail, AppResult, Context};

/// Upper bound on opening one TCP/TLS connection to an LLM provider.
const LLM_CONNECT_TIMEOUT: Duration = Duration::from_secs(20);

// LLM_READ_TIMEOUT and ONBOARDING_REQUEST_TIMEOUT, doc comments verbatim
// from triage.rs:58-64 and triage.rs:66-74.

pub struct LlmClient { /* private fields, unchanged */ }

fn chat_transport(total: Duration) -> AppResult<rig::http_client::ReqwestClient>;

impl LlmClient {
    pub fn new() -> AppResult<Self>;
    pub fn completion_model(&self, raw_model: &str)
        -> AppResult<rig::DynModel<rig::operation::Completion>>;
    #[cfg(test)]
    pub(crate) fn for_test() -> Self;
}
```

Public surface, exactly: `LlmClient` (public, as today), `LlmClient::new`,
`LlmClient::completion_model`, `#[cfg(test)] LlmClient::for_test` (`pub(crate)`,
because `onboarding.rs` tests call it). `chat_transport` and all three constants
stay **private** — after the move nothing outside `llm.rs` names them, and they
were private before.

Every doc comment on the moved items keeps its current text. The only new prose
is the module doc above. That doc is the guard against the grab-bag risk in §9.

### `src/triage.rs` after the removal

Public surface (unchanged by this cut, listed so the implementer can see that the
deletions touch nothing public):

```rust
pub enum Polarity { Yes, No }
pub enum EvidenceLevel { Noise, Weak, Strong, Definitive }
pub enum Target { Binary, Answer(String), NoAnswer }
pub struct TriageOutcome { p_evidence, p_target, alternatives, strength, p_priced, target, cost_usd }
pub struct TriagePolicy { min_evidence }
pub struct TriageAssessment { outcome, impulse_scale }

impl TriagePolicy { pub fn from_config(&Config) -> Self }
impl TriageClient {
    pub fn new(&Config) -> AppResult<Self>;
    pub async fn evaluate(&self, &TrackedMarket, &[TrackedAnswer], &ExtractedPost,
                          Option<u64>, f64) -> AppResult<TriageAssessment>;
}

pub fn assess(&TriageOutcome, TriagePolicy) -> Option<f64>;
```

Removed from the surface: `pub struct LlmClient`, `pub(crate) fn snippet`,
`pub(crate) const ERROR_SNIPPET_BYTES`.

Module doc edit, exactly (`22-24`, a 3-for-3 replacement, so no other line number in
this plan shifts):

```rust
//!
//! The chat transport for onboarding lives in [`crate::llm`]; unused here.
//!
```

The `[`crate::llm`]` link is a rustdoc reference, not an import. It is the only
place the string `crate::llm` appears outside `onboarding.rs`; §10 accounts for it.

## 5. Code moves

### Into `src/llm.rs`, in this order

| From `triage.rs` | Lines | Destination |
|---|---|---|
| `LLM_CONNECT_TIMEOUT` doc + const | `55-56` | first item after the `use` block |
| `LLM_READ_TIMEOUT` doc + const | `58-64` | second |
| `ONBOARDING_REQUEST_TIMEOUT` doc + const | `66-74` | third |
| `LlmClient` doc + struct | `605-613` | fourth |
| `chat_transport` doc + fn | `615-628` | fifth |
| `impl LlmClient` (`new`, `completion_model`, `for_test`) | `630-714` | sixth |
| `keepalive_stream_ends_on_the_request_deadline` + its doc comment | `724-770` | inside a new `#[cfg(test)] mod tests`, with `use super::*;`, then `use rig::completion::CompletionRequest;` and `use tokio::io::{AsyncReadExt, AsyncWriteExt};` |

The relative order of these seven blocks is preserved verbatim, including
`chat_transport` sitting between the struct and its `impl`. Do not reorder to
"struct, impl, helper"; the diff must read as a move.

`src/llm.rs` imports, exactly three lines — one `std`, two `crate`:

```rust
use std::time::Duration;

use crate::config::{parse_model_spec, ModelProvider};
use crate::error::{anyhow, bail, AppResult, Context};
```

All four `error` items are used by the moved bodies (`anyhow` at `680`/`690`,
`bail` at `653`/`697`, `AppResult` in three signatures, `Context` at `626`).
`rig` types are named by full path in the moved code, so `rig` needs no `use`.

### Into `src/onboarding.rs`

| From `triage.rs` | Lines | Destination | Change |
|---|---|---|---|
| `ERROR_SNIPPET_BYTES` doc + const | `46-47` | top const block, immediately after `HTTP_TIMEOUT` (`onboarding.rs:40`) | `pub(crate)` → private `const` |
| `snippet` doc + fn | `591-603` | immediately after `get_json` (`onboarding.rs:595-615`) | `pub(crate) fn` → `fn` |
| `snippet_truncates_on_a_character_boundary` | `1137-1143` | last item in `mod tests`, after `activate_rolls_back_the_market_when_engine_registration_fails` (`onboarding.rs:924`) | none; body verbatim |

The test needs no import: `use super::*;` at `onboarding.rs:619` already brings
the private `snippet` and the private `ERROR_SNIPPET_BYTES` into the child module.
`"é".repeat(ERROR_SNIPPET_BYTES)` needs `str::repeat`, which is inherent.

Import line `onboarding.rs:33` is **replaced**, not shrunk:

```rust
use crate::triage::{snippet, LlmClient};   // before
use crate::llm::LlmClient;                 // after
```

The new line goes between `use crate::error::{...};` (`29`) and
`use crate::market_store::MarketStore;` (`30`), which keeps the block
alphabetical.

### Deleted from `src/triage.rs` (nothing else changes)

| Delete | Lines | Why the boundary is there |
|---|---|---|
| `ERROR_SNIPPET_BYTES` doc + const + one blank | `46-48` | leaves one blank line between `use crate::types::...` (`45`) and `DECISIONS_ENDPOINT`'s doc (`49`) |
| the three LLM deadline blocks + the blank after them | `55-75` | leaves `DECISIONS_TIMEOUT` (`53`), one blank (`54`), then `MIN_STRENGTH`'s doc (`76`) |
| `snippet` + `LlmClient` + `chat_transport` + `impl LlmClient` | `591-715` | leaves `format_timestamp`'s closing brace (`589`), the blank at `590`, then `#[cfg(test)]` (`716`) |
| `use rig::completion::CompletionRequest;` | `720` | only the keepalive test used it |
| `use tokio::io::{AsyncReadExt, AsyncWriteExt};` | `722` | same |
| `snippet_truncates_on_a_character_boundary` + one preceding blank | `1136-1143` | leaves the previous test's brace (`1135`), one blank, then `format_timestamp_handles_missing_values` (`1145`) |
| the keepalive test + its doc comment + one preceding blank | `723-770` | leaves the import block, one blank, then `type BinaryResponse`'s doc (`772`) |
| the module-doc sentence and its two `//!` blanks | `22-24` | 3-for-3 replacement from §4; the heading at `25` is untouched |

Every number in this table is the original `0abfd25` numbering. Because the
module-doc edit is 3-for-3, no number shifts: apply the edits in any order. Nothing
else in `triage.rs` moves, is reordered, or is reformatted.

Leave `DECISIONS_ENDPOINT` (`50`) and `DECISIONS_TIMEOUT` (`53`) in place with
their doc comments.

### What imports shrink in `triage.rs`

Honest answer: **no top-level `use` line changes.** `LlmClient` and
`chat_transport` name every `rig` type by full path (`rig::providers::openai::OpenAI`,
`rig::http_client::ReqwestClient`, `rig::rig_reqwest::reqwest::Client`), so none of
`triage.rs`'s imports at `32-44` exist for the chat client:

| Import | Still used after the removal |
|---|---|
| `std::time::Duration` | yes — `DECISIONS_TIMEOUT` (`53`) |
| `rig::error::ProviderError` | yes — the two `Query` impls |
| `rig::typesafeai::{...}` | yes — `TriageClient`, the question builders |
| `serde`, `serde_json::Value` | yes — state and `TriageClient` |
| `crate::config::{parse_model_spec, Config, ModelProvider}` | yes — `TriageClient::new` |
| `crate::error::{anyhow, bail, AppResult, Context}` | yes |
| `crate::types::{ExtractedPost, TrackedAnswer, TrackedMarket}` | yes |

Only the test module loses two imports (`720`, `722`). An implementer who expects
the diff to remove a `use` line will think the move is incomplete; it is not.

## 6. Behaviour ledger

| # | Claim | Verdict |
|---|---|---|
| 1 | `LlmClient::new` reads the same credentials in the same order: `OpenAIConfig::from_env_with(&cloudflare::DIALECT)` then `from_env_with(&openai::wire::OPENROUTER)` | unchanged — `640-651` copied verbatim |
| 2 | The Cloudflare client takes the clone and OpenRouter takes the moved transport (`643` `transport.clone()`, `649` `transport`) | unchanged; swapping them is a compile error, so the copy cannot drift silently |
| 3 | Deadlines stay 20 s connect, 45 s read, 120 s total | unchanged — `56`, `64`, `74` verbatim |
| 4 | A keepalive stream still ends on the total deadline, not the read timeout | unchanged; the test that proves it moves with the code |
| 5 | The "neither provider" error text is byte-identical, including both interpolations and the `;` separators | unchanged — `653-658` verbatim |
| 6 | The two per-provider errors in `completion_model` are byte-identical (`Cloudflare AI Gateway client is not initialized; please set CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_API_TOKEN`, `OpenRouter client is not initialized; please set OPENROUTER_API_KEY`) | unchanged — `679-694` verbatim |
| 7 | `ModelProvider::OpenRouterDecisions` is still rejected for agent runs, with `raw_model` in the message | unchanged — `697-699` verbatim |
| 8 | `parse_model_spec` and `ModelProvider` still come from `config.rs` | unchanged; they were never in triage |
| 9 | Call sites unchanged: `onboarding.rs:256`, `273`, `427`, `687` hold the same expressions with the same types | unchanged; only the import path at `33` changes |
| 10 | `for_test` keeps `#[cfg(test)] pub(crate)` and its two placeholder providers | unchanged — `707-713` verbatim |
| 11 | Item visibility is preserved: `LlmClient` public, `chat_transport` and the three constants private | unchanged |
| 12 | `snippet` keeps trim → byte budget → character-boundary walk → `…` suffix | unchanged; only `pub(crate) fn` becomes `fn` |
| 13 | The two error strings that consume `snippet` are byte-identical (`GET {url} failed with HTTP {status}: {}`, `parsing JSON from {url}: {}`) | unchanged — `onboarding.rs:610`, `614` untouched |
| 14 | The keepalive test's body is identical except for one rustfmt line break: `triage.rs:745` `while socket.write_all(b": OPENROUTER PROCESSING\n\n").await.is_ok() {` becomes a five-line `.write_all(..).await.is_ok()` chain, because the statement sits at the same nesting depth and rustfmt re-wraps it in the new file | formatting only: same byte string, same durations (200 ms / 5 s / 10 ms), same two assertions |
| 15 | The snippet test's body is byte-identical, and `onboarding.rs` is rustfmt-clean at HEAD so the copy cannot introduce new formatting dirt | unchanged |
| 16 | `DECISIONS_ENDPOINT` and `DECISIONS_TIMEOUT` stay in `triage.rs` and `TriageClient::run` still wraps the call in `tokio::time::timeout(DECISIONS_TIMEOUT, ...)` | unchanged — `50`, `53`, `412` untouched |
| 17 | No `Cargo.toml` change; `rig` is still the only LLM transport | unchanged |
| 18 | Crate test census stays 275 | unchanged — two moves, one import-line replacement |
| 19 | `triage.rs` gains no new rustfmt dirt: it stays in the dirty set for the same pre-existing reasons, and the deletions remove lines only | unchanged dirty set (§10) |
| 20 | The `LlmClient` doc comment ("Shared chat client for onboarding, built on rig.") and the `chat_transport` doc comment ("Builds the HTTP transport both chat providers share…") keep their text | unchanged |

Anything not in this table must behave identically; the gates in §8 and the greps
in §10 are the enforcement.

## 7. Tests

**No new tests.** Two tests move, one to each new home, and neither body is
rewritten.

| Test | From | To | Why there |
|---|---|---|---|
| `keepalive_stream_ends_on_the_request_deadline` | `triage.rs:724-770` | `llm.rs`, `mod tests` | it calls `chat_transport` directly and proves the total-deadline behaviour that `chat_transport` exists for |
| `snippet_truncates_on_a_character_boundary` | `triage.rs:1137-1143` | `onboarding.rs`, `mod tests` | it tests `snippet`, which is now private to `onboarding.rs` |

Census arithmetic, verified by
`rg -o '(#\[test\]|#\[tokio::test\])' src/ | wc -l`:

| Location | Before | After |
|---|---|---|
| `src/triage.rs` | 25 | 23 |
| `src/onboarding.rs` | 12 | 13 |
| `src/llm.rs` | — (no file) | 1 |
| `src/` total | **275** | **275** |

Note that `src/triage.rs` counts 25 plain `#[test]` and no `#[tokio::test]`; the
crate total of 275 is `243` plain plus `32` `#[tokio::test]` (`control.rs` 20,
`onboarding_agent.rs` 12). Neither moved test is a `#[tokio::test]` — the keepalive
test builds its own runtime with `tokio::runtime::Runtime::new()`, which is why it
can move without a runtime attribute.

The live, `#[ignore]`d probes at `triage.rs:1260` and `1277` stay where they are:
they exercise `TriageClient`, not the chat transport.

## 8. Commit sequence

Two commits. Both keep the tree compiling and all three gates green.

1. `refactor(llm): move the chat transport out of triage`

   - Add `src/llm.rs`: module doc from §4, the three-line `use` list from §5, the
     three deadline constants, `LlmClient`, `chat_transport`, `impl LlmClient`, and
     `#[cfg(test)] mod tests` with the keepalive test.
   - `src/main.rs`: add `mod llm;` between `mod ingest;` (`20`) and
     `mod market_store;` (`21`).
   - `src/onboarding.rs`: replace the import at `33`; add
     `ERROR_SNIPPET_BYTES` after `HTTP_TIMEOUT` (`40`); add `snippet` after
     `get_json` (`615`); add the snippet test after `924`.
   - `src/triage.rs`: the deletions in §5, and the module-doc replacement.
   - Gates: `cargo build --release`, `cargo test`, `cargo clippy -p manif0ld
     --all-targets --no-deps -- -D warnings`.
   - Formatting: `rustfmt --edition 2021 src/llm.rs src/onboarding.rs`, then
     `rustfmt --edition 2021 --check --config skip_children=true src/llm.rs
     src/onboarding.rs src/main.rs` must exit 0. Running `rustfmt` without
     `--check` on those two files is safe: `llm.rs` is new, and `onboarding.rs`
     is clean at HEAD, so any line it rewrites is one this commit added. **Do not
     run rustfmt on `triage.rs`** — it is dirty at HEAD and reformatting it would
     bury the move in unrelated churn.

   **Why one commit and not two.** A "copy into the new module, then switch" split
   is not available here:

   - After step 1 (`llm.rs` added, `mod llm;` declared, nothing removed) the crate
     holds two `LlmClient` types and two copies of the keepalive test. The census
     becomes 276, which breaks the invariant §7 and §10 assert, and one of the two
     structs is dead code that `#![allow(dead_code)]` in `main.rs` would hide.
   - Copying the code without the test is worse: the transport's only behavioural
     proof would be testing a struct that onboarding no longer uses.
   - `snippet`'s move is single-file — the definition and both call sites are in
     `onboarding.rs` after the edit — so it has no switch step either.

   The only split that would compile is `snippet`-first, then `LlmClient`. It is
   rejected because the two moves share one motivation and one acceptance set, and
   the intermediate commit would leave `triage.rs` holding a chat client with no
   in-module caller for no reviewer benefit. One commit, one boundary change.

2. `docs: record the llm module split`

   - `docs/ARCHITECTURE.md` §2: node `TRIAGE["triage.rs<br/>LlmClient + TriageClient<br/>+ prompt + parser"]`
     (`110`) becomes `TRIAGE["triage.rs<br/>TriageClient<br/>+ questions + assess"]`;
     add `LLM["llm.rs<br/>LlmClient + chat transport"]` after it; edge
     `ONB -->|"LlmClient,<br/>json helpers"| TRIAGE` (`163`) becomes
     `ONB -->|"LlmClient"| LLM`; add `LLM --> CFG` and `LLM --> ERR` after
     `TRIAGE`'s three edges (`132-134`).
   - `docs/ARCHITECTURE.md` §2 observation 5 (`210-211`): "`onboarding.rs` reaches
     into `triage.rs` for `LlmClient` and `snippet`. LLM transport is coupled to
     the triage feature." becomes a statement of the new shape — `onboarding`
     imports `crate::llm`, `triage` imports nothing from `llm` because it is a
     decision model, and the `onboarding`/`onboarding_agent` pair paragraph
     (`212-216`) stays.
   - `docs/ARCHITECTURE.md` §4: insert `### llm.rs` immediately before
     `### triage.rs` (`447`) with the surface from §4 of this plan; replace the
     stale `### triage.rs` body (`449-474`) with the real one — `TriageClient { client: Jev, model, policy }`,
     `TriageClient::new`, `async fn evaluate(&TrackedMarket, &[TrackedAnswer], &ExtractedPost, Option<u64>, f64) -> AppResult<TriageAssessment>`,
     `assess`, `TriageOutcome`/`TriagePolicy`/`TriageAssessment`, `Target`,
     `Polarity`, `EvidenceLevel` — and drop `LlmClient`, `complete`,
     `build_prompt`, `build_multiple_choice_prompt`, `parse_triage_response`,
     `is_material`, `TriageResult`, and the "Triage uses `complete`" sentence
     (`473`), which are all wrong at `0abfd25`.
   - `docs/ARCHITECTURE.md` §7F (`1033-1044`): title gains `— **resolved**`, and
     the body becomes the `**Then:**` / `**Now:**` / `**Residual:**` shape used by
     §7A-§7D. `**Then:**` records what the review said *and* that it was stale on
     five counts (§1.7). `**Now:**` states the one real edge, `onboarding → llm`,
     and that triage has no edge to `llm`. `**Residual:**` = `snippet` duplication
     with `trade.rs`, and the `llm.rs` grab-bag guard.
     In `**Then:**`, do **not** write the literals `json helpers` or
     `extract_json_object`: call them "a JSON-extraction helper that no longer
     exists in `src/`". The §10 doc greps assert those two strings are gone, and
     the point of the sentence is that the review named helpers that are not there.
   - `docs/ARCHITECTURE.md` §8: item 3 (`1192-1194`) struck through with a `Done:`
     clause, matching items 1 and 2; in the §8 diagram, `LLM2["LlmClient"]`
     (`1156`) becomes `LLM2["llm.rs<br/>LlmClient<br/>chat transport"]` and the
     `PIPELINE2 --> LLM2` edge (`1172`) is deleted — it was already wrong, because
     the pipeline calls `TriageClient` and never `LlmClient`.
   - `docs/SPEC.md` §3 (`62`): the `triage.rs` layout line is already correct; add
     `├── llm.rs           # chat transport for onboarding (rig providers)` above
     it, because the layout block lists every module and would otherwise omit one.
     §6.4 (`327-395`) and §6.7 (`549-618`) name no module for the chat transport —
     verified with `rg -n "LlmClient" docs/SPEC.md` (no hits) — so no fix is
     required there. Say so in the commit message rather than editing prose that is
     already right.
   - `AGENTS.md`: no edit. Its invariants name `src/triage.rs` only as the home of
     the triage question wording (still true) and say nothing about where the chat
     transport lives (`rg -n "LlmClient" AGENTS.md` — no hits).
   - Do not touch `docs/plans/*.md`.

Run the gates after both commits. The formatting check that actually works:

```sh
rustfmt --edition 2021 --check --config skip_children=true \
    src/llm.rs src/onboarding.rs src/main.rs          # exit 0
```

`cargo fmt --check` is dirty at HEAD for exactly ten paths —
`src/belief.rs`, `src/config.rs`, `src/db.rs`, `src/discord.rs`, `src/ingest.rs`,
`src/onboarding_agent.rs`, `src/pipeline.rs`, `src/sieve.rs`, `src/triage.rs`,
`sieve/src/engine.rs` — and that set must be byte-identical after the change.
`triage.rs` is one of the ten: never run `cargo fmt` or bare `rustfmt` on it.

## 9. Risks and residuals

- **`llm.rs` is small and could become a grab bag.** Its whole content is a
  struct, one constructor helper, one method, the three deadlines, and one test.
  The pressure to re-add "just a JSON helper" or "just a prompt" is exactly what
  created the original coupling. Mitigation: the module doc's "What this module
  must not become" paragraph names the boundary, and §10 greps that `llm.rs`
  defines no prompt builder, no response parser, no feature type, and pulls in no
  `serde`. A helper that formats an HTTP error body belongs with its caller — that
  is why `snippet` went to `onboarding.rs` and not here.

- **Residual: `snippet` is still duplicated with `trade.rs:1207`.** The two differ
  in unit (bytes vs characters), in trimming, and in suffix condition, so a merge
  would change error text on one of the two paths. Left alone deliberately
  (decision 3). A future change that unifies them must decide which semantics
  survives and accept that the other path's error strings change.

- **Residual: the review text was already stale in ways this plan does not fix.**
  `docs/ARCHITECTURE.md:752` still labels the §5.1 sequence reply `TriageResult`,
  and `docs/OVERVIEW.tex:103-106` still says triage makes a Workers AI `/ai/run`
  call and parses `{material, direction, strength, rationale}`. Both were wrong
  before this candidate and are not caused by it. What this plan does fix:
  `ARCHITECTURE.md` §2, §4, §7F, §8 (text and one diagram edge), and the
  `SPEC.md` §3 layout. Do not regenerate `OVERVIEW.pdf` here — no `.tex` edit is
  made, so nothing needs rebuilding.

- **Residual: `triage.rs` now contains no chat code, so the next chat-based triage
  needs a decision.** If triage ever moves to a chat model, it imports
  `crate::llm` — it does not grow its own client and it does not absorb `llm.rs`
  back. The transport is not feature-owned; that is the whole point of the cut.
  Recorded here so the next author does not re-litigate it from the diff alone.

- **Residual: `ONBOARDING_REQUEST_TIMEOUT` keeps its onboarding name inside
  `llm.rs`.** It is accurate: `chat_transport` takes the deadline as a parameter,
  and 120 s is onboarding's value for it. A second consumer must pass its own
  deadline to `chat_transport`, not rename the constant.

- **Residual: `for_test`'s doc comment is inaccurate and moves unchanged.** It
  says "No test calls it, so the surrounding environment is never read", while
  `onboarding.rs:687` calls it. The second clause is the true one — the function
  passes literal keys and reads no environment variable. Decision 1 keeps the
  comment verbatim; a docs-only follow-up should reword it. It cannot fail a gate.

- **`#![allow(dead_code)]` still hides an orphaned `llm.rs` item.** If onboarding
  ever stops using `LlmClient`, the transport becomes dead code and the crate-wide
  allow means no warning fires. §10's `rg "LlmClient" src/` count is the
  counterweight: four hits in `onboarding.rs` is the healthy state, zero means the
  module is dead.

- **The move is not covered by a new test, and it should not be.** There is no new
  behaviour to pin; the keepalive test already pins the only property that could
  break (the total deadline), and it moves intact. No coverage claim is made.

- **`LlmClient::new` still reads the process environment,** so it stays untestable
  and this plan leaves that as found. Nothing here improves testability, and the
  plan should not be read as if it did.

## 10. Acceptance

Structural greps. Each must hold after commit 1; the last block holds after
commit 2.

```sh
# 1. triage.rs holds no part of the chat transport
rg -n "LlmClient|chat_transport|LLM_CONNECT_TIMEOUT|LLM_READ_TIMEOUT|ONBOARDING_REQUEST_TIMEOUT" src/triage.rs
# no output

rg -n "snippet|ERROR_SNIPPET_BYTES" src/triage.rs
# no output

# 2. nothing imports the moved items from triage
rg -n "crate::triage::(LlmClient|snippet)" src/
# no output
rg -n "use crate::triage" src/
# src/pipeline.rs:43 only  (use crate::triage::{Target, TriageClient};)

# 3. the transport is reachable from exactly one consumer
rg -n "LlmClient" src/
# every hit is in src/llm.rs, except exactly these four lines in src/onboarding.rs:
#   33  use crate::llm::LlmClient;
#   256 ai: LlmClient,
#   273 ai: LlmClient::new()?,
#   687 ai: LlmClient::for_test(),
rg -n "^use crate::llm" src/
# src/onboarding.rs:33 only

# 4. one definition of snippet per module family, no shared helper
rg -n "fn snippet" src/
# src/onboarding.rs (private) and src/trade.rs (private) — exactly 2 hits
rg -n "ERROR_SNIPPET_BYTES" src/
# src/onboarding.rs only (the const, its doc link, two uses in the body, two in its test)
rg -n "ERROR_SNIPPET_CHARS" src/
# src/trade.rs only

# 5. the module is declared where main.rs keeps the alphabetical list
rg -n "^mod llm;" src/main.rs          # 1 hit, between mod ingest; and mod market_store;
rg -c "^mod " src/main.rs              # 19 (18 before + llm)

# 6. the only non-import mention of the new path is triage's doc link
rg -n "crate::llm" src/
# two hits: src/triage.rs (the module-doc link) and src/onboarding.rs:33 (the import).
# main.rs cannot match: it declares `mod llm;`, not `crate::llm`.

# 7. test census
rg -o '(#\[test\]|#\[tokio::test\])' src/ | wc -l      # 275
rg -c '(#\[test\]|#\[tokio::test\])' src/triage.rs     # 23
rg -c '(#\[test\]|#\[tokio::test\])' src/onboarding.rs # 13
rg -c '(#\[test\]|#\[tokio::test\])' src/llm.rs        # 1

# 8. only the intended files changed
git diff --stat 0abfd25 -- src/ | tail -1
# src/llm.rs, src/main.rs, src/onboarding.rs, src/triage.rs — four paths, nothing else
git diff --stat 0abfd25 -- Cargo.toml rig/ sieve/
# no output
```

`llm.rs` must stay a leaf and stay transport-only. The module doc itself mentions
`crate::triage` and `crate::onboarding` as prose links, so these greps target the
`use` block and the definitions, not the prose:

```sh
rg -n "^use crate::(types|db|sieve|market_store|triage|onboarding)" src/llm.rs   # no output
rg -n "serde|serde_json" src/llm.rs                                              # no output
rg -n "fn snippet|fn build_state|TriageState|TriageClient" src/llm.rs            # no output
```

Gates after each commit:

```sh
cargo build --release
cargo test
cargo clippy -p manif0ld --all-targets --no-deps -- -D warnings
```

Formatting:

```sh
rustfmt --edition 2021 --check --config skip_children=true \
    src/llm.rs src/onboarding.rs src/main.rs      # exit 0

cargo fmt --check 2>&1 | grep '^Diff in' | sed 's/:.*//' | sort -u
# unchanged, exactly these ten and no others:
# sieve/src/engine.rs src/belief.rs src/config.rs src/db.rs src/discord.rs
# src/ingest.rs src/onboarding_agent.rs src/pipeline.rs src/sieve.rs src/triage.rs
```

Docs acceptance after commit 2:

```sh
rg -n "json helpers|extract_json_object" docs/ARCHITECTURE.md     # no output
rg -n "build_prompt|parse_triage_response|is_material" docs/ARCHITECTURE.md   # no output
rg -n "client: LlmClient" docs/ARCHITECTURE.md                    # no output
rg -n "LlmClient" docs/ARCHITECTURE.md
# §2 node LLM, §2 edge ONB --> LLM, §4 llm.rs subsection, §7F Then/Now (no stale "LlmClient sits inside triage.rs" heading)
rg -n "LlmClient" docs/SPEC.md                                    # no output
rg -n "llm\.rs" docs/SPEC.md docs/ARCHITECTURE.md                 # both files mention it
```
