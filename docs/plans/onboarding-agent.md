# Plan: Onboarding research agent

## 1. Goal

Replace the fixed 5-attempt Sieve synthesis loop with a real `rig` agent.

The agent will:

1. Research the market topic with Firecrawl.
2. Decide which names, aliases, tickers, people, organizations, and events to
   listen for.
3. Write a Sieve program.
4. Submit it through a tool.
5. Read the compiler result and retry without a fixed attempt limit.
6. Stop when the program compiles.

The agent does not write a summary. Its final text is ignored. It only calls
tools. The research output is used only for the Sieve script for now. The
module is structured so more tools and a periodic research agent can be added
later.

## 2. Design overview

```
/track
  └─ OnboardingService::prepare
       ├─ fetch Manifold market
       └─ onboarding_agent::synthesize
            ├─ rig AgentBuilder
            │    ├─ preamble = SIEVE_SYSTEM_PROMPT + agent instructions
            │    ├─ tool: submit_sieve
            │    ├─ tool: web_search   (Firecrawl)
            │    └─ tool: read_page    (Firecrawl)
            └─ agent.prompt(market context)
                 └─ loop until submit_sieve returns "ok"
```

The agent writes the accepted program into a shared slot. `synthesize` reads
the slot after the run. This avoids parsing the model's final text.

## 3. Dependencies

`Cargo.toml`:

```toml
firecrawl = "2"
rig = { path = "rig", default-features = false, features = ["reqwest", "rustls", "agent"] }
```

Notes:

- `agent` enables `rig-agent`, `AgentBuilder`, `DynamicTool`, and the run loop.
- `derive` is not needed if we use `DynamicTool`. Use `DynamicTool` to keep the
  feature set small.
- `firecrawl` uses `thiserror 1`; this project uses `thiserror 2`. Cargo can
  hold both.
- `Cargo.lock` will change.

## 4. Configuration

`src/config.rs`:

- Add `pub firecrawl_api_key: String`.
- Read `FIRECRAWL_API_KEY` as a required variable.
- Add it to the `Config` constructor.
- Add `("FIRECRAWL_API_KEY", "fc-test")` to `required_vars()` in tests.
- Add a test that a missing key fails.

`.env.example`:

```sh
# Firecrawl (web research for the onboarding agent)
FIRECRAWL_API_KEY=fc-...
```

Also add a note that `ONBOARDING_MODEL` must support tool calling. The current
default may not. A safe choice is `cloudflare/@cf/moonshotai/kimi-k2.6` or a
tool-capable OpenRouter model.

## 5. `LlmClient` change

`src/triage.rs`:

Add a method that returns a `rig` completion model:

```rust
pub fn completion_model(
    &self,
    raw_model: &str,
) -> AppResult<rig::DynModel<rig::operation::Completion>>
```

It uses the existing `parse_model_spec` and the Cloudflare/OpenRouter client,
then calls `client.completion(model).into()`.

Remove `complete_chat` and its `Arc`/`Message` imports if they become unused.
`complete` stays for triage.

## 6. New module: `src/onboarding_agent.rs`

This module holds the agent, the tools, and the research instructions. Keep
`onboarding.rs` for market fetch, draft, and activation.

### Constants

```rust
pub const MAX_AGENT_TURNS: usize = 200;   // runaway guard, not an attempt limit
const MAX_SEARCH_RESULTS: u32 = 8;
const MAX_PAGE_CHARS: usize = 20_000;
```

`MAX_AGENT_TURNS` is a safety valve. The `submit_sieve` tool itself has no
attempt limit.

### Agent instructions

Add a constant that is appended to `SIEVE_SYSTEM_PROMPT`:

```text
You are the onboarding research agent for a prediction-market trader.

1. Research the market topic.
   - Use `web_search` to find current, reliable sources.
   - Use `read_page` to read the most useful results.
   - Find the exact names, aliases, ticker symbols, product names, people,
     organizations, and events that Bluesky posts would use.
   - Use at most 8 searches and 5 page reads.
   - Prefer primary sources, official announcements, and recent news.

2. Write and submit one Sieve program.
   - The program must start with `match Market_<id> { ... }`.
   - Call `submit_sieve` with the complete program.
   - `submit_sieve` returns `ok` when the program compiles, or the compiler error.
   - If it returns an error, fix the program and call `submit_sieve` again.
     There is no attempt limit.
   - When `submit_sieve` returns `ok`, stop. Do not write a summary, an
     explanation, or any other final text. Your final message is ignored.

The rule's only job is topical filtering. Match posts that are plausibly
related to the market. Do not encode YES or NO direction. Match both
supportive and skeptical posts.
```

The preamble is:

```rust
format!("{SIEVE_SYSTEM_PROMPT}\n\n{AGENT_INSTRUCTIONS}")
```

`SIEVE_SYSTEM_PROMPT` stays verbatim, as the spec requires.

### `synthesize`

```rust
pub async fn synthesize(
    model: rig::DynModel<rig::operation::Completion>,
    firecrawl: Arc<firecrawl::Client>,
    market: &ManifoldSlugMarket,
    updates: &mpsc::Sender<OnboardUpdate>,
) -> AppResult<String>
```

Steps:

1. Create `submitted: Arc<Mutex<Option<String>>>`,
   `last_error: Arc<Mutex<Option<String>>>`, and
   `attempts: Arc<AtomicUsize>`.
2. Build the three tools.
3. Build the agent with `AgentBuilder::new(model)`, `.preamble(...)`,
   `.dynamic_tools(...)`, `.default_max_turns(MAX_AGENT_TURNS)`.
4. Send `OnboardUpdate::Phase("Researching the market topic…")`.
5. Run `agent.prompt(build_sieve_user_prompt(market)).await`.
6. Ignore the response text. Read the slot.
7. If the slot has a program, return it.
8. If the slot is empty, return an error built from `last_error`, not from the
   model's final text.

```rust
match submitted.lock().ok().and_then(|mut slot| slot.take()) {
    Some(script) => Ok(script),
    None => {
        let last = last_error.lock().ok().and_then(|error| error.clone());
        match last {
            Some(error) => bail!(
                "onboarding agent stopped without a compiling Sieve rule. \
                 Last compiler error:\n{error}"
            ),
            None => bail!("onboarding agent stopped without submitting a Sieve rule"),
        }
    }
}
```

### Tool 1: `submit_sieve`

Arguments:

```json
{
  "type": "object",
  "properties": {
    "sieve_program": {
      "type": "string",
      "description": "The complete Sieve program, starting with `match Market_<id> {`."
    }
  },
  "required": ["sieve_program"]
}
```

Behavior:

- If the slot already has a program, return:
  `"A compiling rule was already submitted. Stop calling tools."`
- Increment the attempt counter.
- Compile with `tokio::task::spawn_blocking` so Cranelift does not block the
  async runtime. The closure returns `Result<(), String>` so the `!Send`
  `CompiledSieve` never crosses the task boundary.
- On success:
  - Store the program in the slot.
  - Send `OnboardUpdate::Sieve(SieveAttempt { attempt, script, error: None })`.
  - Return `"ok: the program compiled. Stop calling tools."`
- On failure:
  - Store the error in `last_error`.
  - Send `OnboardUpdate::Sieve(SieveAttempt { attempt, script, error: Some(error) })`.
  - Return `"compile error:\n{error}\nFix the program and call submit_sieve again."`

The model sees the error and can retry forever.

### Tool 2: `web_search`

Arguments:

```json
{
  "type": "object",
  "properties": {
    "query": { "type": "string" },
    "limit": { "type": "integer", "minimum": 1, "maximum": 8 }
  },
  "required": ["query"]
}
```

Behavior:

- Send `OnboardUpdate::Phase(format!("Researching: web search `{query}`…"))`.
- Call `client.search(query, SearchOptions { limit, sources: Some(vec![SearchSource::Web, SearchSource::News]), ..Default::default() })`.
- Map `SearchResultOrDocument::WebResult` to `{title, url, description}`.
- Map `SearchResultOrDocument::Document` to `{url, markdown}`.
- Return `ToolOutput::json(json!({ "results": items }))`.
- On error, return
  `ToolExecutionError::new(ToolErrorKind::Other, format!("web_search failed: {error}"))`
  so the model sees the real message.

### Tool 3: `read_page`

Arguments:

```json
{
  "type": "object",
  "properties": {
    "url": { "type": "string" }
  },
  "required": ["url"]
}
```

Behavior:

- Reject non-`http`/`https` URLs.
- Send `OnboardUpdate::Phase(format!("Researching: reading `{url}`…"))`.
- Call `client.scrape(url, Some(ScrapeOptions { formats: Some(vec![Format::Markdown]), only_main_content: Some(true), ..Default::default() }))`.
- Return `{url, title, markdown}` with markdown truncated to `MAX_PAGE_CHARS`.
- If markdown is empty, return a model-visible error.

### Helper

Add a local `truncate_chars` helper. Do not reuse the private Discord helper.

## 7. `onboarding.rs` changes

Remove:

- `MAX_SIEVE_ATTEMPTS`
- `SieveSynthesis`
- `Validation`
- `validate_synthesis`
- `parse_sieve_synthesis`
- `OnboardUpdate::asking_for_rule`
- The `extract_json_object` import

Change:

- `SieveAttempt` loses `max_attempts`:

```rust
pub struct SieveAttempt {
    pub attempt: usize,
    pub script: String,
    pub error: Option<String>,
}
```

- `OnboardingService` gains `firecrawl: Arc<firecrawl::Client>`.
- `OnboardingService::new` builds the Firecrawl client from
  `config.firecrawl_api_key`.
- `prepare` calls `onboarding_agent::synthesize(...)` instead of
  `self.synthesize(...)`.
- `build_sieve_user_prompt` removes the JSON response shape and ends with a
  research instruction:

```text
Research this market, then write and submit one Sieve program.
```

Keep `parse_slug`, `validate_market`, `build_market`, `get_json`, and the
activation tests.

## 8. Discord changes

`src/discord.rs`:

- Raise `ONBOARD_TIMEOUT` from 120 seconds to 600 seconds. Research plus several
  agent turns will exceed 120 seconds.
- `sieve_header` becomes:

```rust
"Attempt {n} failed"
"Attempt {n} compiled"
```

- The failure path no longer uses `max_attempts`. Use:

```text
Agent stopped without a compiling rule.
```

- The updater already handles `OnboardUpdate::Phase` and
  `OnboardUpdate::Sieve`. No new variant is needed.
- Update the Discord tests that construct `SieveAttempt` and assert headers.

## 9. Tests

### `onboarding_agent.rs`

- `submit_tool_accepts_a_compiling_program_and_records_it`
- `submit_tool_returns_the_compiler_error_and_does_not_record`
- `submit_tool_counts_attempts`
- `submit_tool_refuses_a_second_submission_after_success`
- `search_args_reject_missing_query`
- `read_page_rejects_non_http_urls`
- `truncate_chars_is_char_safe`

### `onboarding.rs`

- Keep `parse_slug`, `validate_market`, and `activate_*` tests.
- Replace the synthesis JSON tests with:
  - `user_prompt_contains_market_context_and_research_instruction`
  - `user_prompt_falls_back_to_description_when_criteria_are_missing`
- Remove `parse_synthesis_*` and `asking_for_rule_*` tests.

### `config.rs`

- Add `FIRECRAWL_API_KEY` to `required_vars`.
- Add `rejects_missing_firecrawl_key`.

### `discord.rs`

- Update `SieveAttempt` constructions.
- Update `sieve_header` expectations.
- Update `sieve_report` headers.

## 10. Documentation

- `docs/SPEC.md` §6.7: rewrite steps 3–4. Describe the agent, the three tools,
  the no-attempt-limit rule, the research budget, and the 600-second timeout.
- `docs/ARCHITECTURE.md`:
  - Update the `onboarding.rs` section.
  - Update the `/track` sequence diagram: replace the 5-attempt loop with the
    agent and its tools.
  - Update the `LlmClient` note: onboarding now uses `completion_model`.
- `AGENTS.md`: add an invariant:

```text
- Onboarding is a rig agent. It researches with Firecrawl and submits Sieve
  programs through a tool. Do not reintroduce a fixed attempt loop.
```

- `.env.example`: add `FIRECRAWL_API_KEY` and the tool-capable model note.

## 11. Implementation order (atomic commits)

1. `chore(deps): add firecrawl and enable the rig agent feature`
2. `feat(config): require FIRECRAWL_API_KEY`
3. `feat(llm): expose a rig completion model from LlmClient`
4. `feat(onboarding): add the research agent and its three tools`
5. `refactor(onboarding): replace the fixed synthesis loop with the agent`
6. `feat(discord): render agent progress and raise the onboarding timeout`
7. `test: cover the onboarding agent tools and prompts`
8. `docs: describe the onboarding research agent`

Run the three gates after each logical step:

```sh
cargo build --release
cargo test
cargo clippy -p bsky-manifold-arb --all-targets --no-deps -- -D warnings
```

## 12. Risks and decisions

| Topic | Decision | Reason |
|---|---|---|
| Firecrawl key | Required at startup | Onboarding cannot research without it. Fails loudly. |
| Attempt limit | None in `submit_sieve` | The model retries until it compiles. |
| Agent turn cap | 200 | Runaway guard. Not an attempt limit. |
| Onboarding timeout | 600 seconds | Research plus several agent turns. |
| Tool style | `DynamicTool` | No `derive` feature. Easy to add more tools. |
| Compile execution | `spawn_blocking` | Cranelift is CPU-bound and `CompiledSieve` is `!Send`. |
| Research budget | 8 searches, 5 page reads | Controls Firecrawl cost and agent time. |
| Final text | Ignored | The agent only calls tools. No summary is shown. |
| Model | Must support tool calling | The current default may not. Update `.env.example`. |

## 13. Decisions to confirm

1. **Firecrawl key**: required at startup, or optional with a degraded agent
   that has no research tools?
2. **Turn cap**: 200, or a different value?
3. **Timeout**: 600 seconds, or longer?
4. **Model**: do you want to switch `ONBOARDING_MODEL` to a known tool-calling
   model now, or keep the current one and document the requirement?
