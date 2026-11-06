//! The onboarding research agent: research a market, then submit a Sieve rule.
//!
//! Onboarding is a `rig` agent, not a fixed prompt loop. The agent is given
//! three tools:
//!
//! - `web_search` (Firecrawl) finds current sources for the market topic.
//! - `read_page` (Firecrawl) reads the most useful sources in full.
//! - `submit_sieve` compiles a candidate Sieve program and reports the compiler
//!   error back to the model.
//!
//! The agent keeps retrying `submit_sieve` until the program compiles; there is
//! no attempt limit. [`MAX_AGENT_TURNS`] is only a runaway guard. The accepted
//! program is written into a shared slot, so the caller never parses the
//! model's final text (which is ignored: the agent only calls tools).
//!
//! The module is deliberately narrow. Its tools and state are reusable for a
//! later periodic research agent, but for now research feeds only the Sieve
//! script.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use firecrawl::{
    Client, Document, Format, ScrapeOptions, SearchOptions, SearchResultOrDocument, SearchSource,
};
use rig::tool::{DynamicTool, ToolErrorKind, ToolExecutionError, ToolOutput};
use rig::message::ToolName;
use serde_json::{json, Value};
use tokio::sync::mpsc;

use crate::error::{bail, AppResult};
use crate::onboarding::{
    build_sieve_user_prompt, ManifoldSlugMarket, OnboardUpdate, SieveAttempt,
    SIEVE_SYSTEM_PROMPT,
};
use crate::sieve;

/// Runaway guard for the agent loop. This is not an attempt limit: the model
/// may call `submit_sieve` as many times as it needs.
pub const MAX_AGENT_TURNS: usize = 200;

/// Default and maximum `web_search` result count.
const MAX_SEARCH_RESULTS: u32 = 8;

/// Longest page or document excerpt returned to the model, in characters.
const MAX_PAGE_CHARS: usize = 20_000;

/// Instructions appended to [`SIEVE_SYSTEM_PROMPT`] for the agent run.
pub const AGENT_INSTRUCTIONS: &str = r##"You are the onboarding research agent for a prediction-market trader.

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
supportive and skeptical posts."##;

/// The system prompt for the onboarding agent run.
pub fn agent_preamble() -> String {
    format!("{SIEVE_SYSTEM_PROMPT}\n\n{AGENT_INSTRUCTIONS}")
}

/// Mutable state shared by the `submit_sieve` tool and [`synthesize`].
///
/// `submitted` holds the first program that compiled. Once it is set, later
/// submissions are refused so a successful run cannot be overwritten.
#[derive(Clone, Default)]
struct SubmitState {
    submitted: Arc<Mutex<Option<String>>>,
    last_error: Arc<Mutex<Option<String>>>,
    attempts: Arc<AtomicUsize>,
}

impl SubmitState {
    fn submitted(&self) -> Option<String> {
        self.submitted.lock().ok().and_then(|slot| slot.clone())
    }

    fn take_submitted(&self) -> Option<String> {
        self.submitted.lock().ok().and_then(|mut slot| slot.take())
    }

    fn record_submitted(&self, script: &str) {
        if let Ok(mut slot) = self.submitted.lock() {
            *slot = Some(script.to_owned());
        }
    }

    fn record_error(&self, error: &str) {
        if let Ok(mut slot) = self.last_error.lock() {
            *slot = Some(error.to_owned());
        }
    }

    fn last_error(&self) -> Option<String> {
        self.last_error.lock().ok().and_then(|slot| slot.clone())
    }
}

/// Researches `market` with the agent and returns the first compiling program.
///
/// The agent's final response text is ignored. The accepted program comes from
/// the `submit_sieve` slot, so a model that writes prose instead of calling the
/// tool produces an error, never a parse failure on free text.
pub async fn synthesize(
    model: rig::DynModel<rig::operation::Completion>,
    firecrawl: Arc<Client>,
    market: &ManifoldSlugMarket,
    updates: &mpsc::Sender<OnboardUpdate>,
) -> AppResult<String> {
    let state = SubmitState::default();

    let tools = vec![
        submit_sieve_tool(state.clone(), updates.clone()),
        web_search_tool(Arc::clone(&firecrawl), updates.clone()),
        read_page_tool(Arc::clone(&firecrawl), updates.clone()),
    ];

    let agent = rig::AgentBuilder::new(model)
        .preamble(agent_preamble())
        .dynamic_tools(tools)
        .default_max_turns(MAX_AGENT_TURNS)
        .build();

    let _ = updates
        .send(OnboardUpdate::Phase(
            "Researching the market topic…".to_owned(),
        ))
        .await;

    // The response is intentionally discarded: the agent communicates only
    // through its tools, and the accepted program is read from the slot.
    let _ = agent.prompt(build_sieve_user_prompt(market)).await;

    match state.take_submitted() {
        Some(script) => Ok(script),
        None => match state.last_error() {
            Some(error) => bail!(
                "onboarding agent stopped without a compiling Sieve rule. \
                 Last compiler error:\n{error}"
            ),
            None => bail!("onboarding agent stopped without submitting a Sieve rule"),
        },
    }
}

/// Builds the `submit_sieve` tool.
///
/// Each call compiles the program on a blocking thread, because Cranelift is
/// CPU-bound and [`sieve::CompiledSieve`] is `!Send`. The closure returns only
/// `Result<(), String>`, so the compiled engine never crosses the task
/// boundary. A compiler error is returned as ordinary tool text so the model
/// can read it and retry.
fn submit_sieve_tool(
    state: SubmitState,
    updates: mpsc::Sender<OnboardUpdate>,
) -> DynamicTool {
    let parameters = json!({
        "type": "object",
        "properties": {
            "sieve_program": {
                "type": "string",
                "description": "The complete Sieve program, starting with `match Market_<id> {`."
            }
        },
        "required": ["sieve_program"]
    });

    DynamicTool::new(
        tool_name("submit_sieve"),
        "Compile a Sieve program. Returns `ok` when it compiles, otherwise the compiler error.",
        parameters,
        move |args| {
            let state = state.clone();
            let updates = updates.clone();
            Box::pin(async move {
                let program = args
                    .get("sieve_program")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        ToolExecutionError::invalid_args(
                            "submit_sieve requires a string `sieve_program` argument",
                        )
                    })?
                    .to_owned();
                if program.trim().is_empty() {
                    return Err(ToolExecutionError::invalid_args(
                        "`sieve_program` must not be empty",
                    ));
                }

                if state.submitted().is_some() {
                    return Ok(ToolOutput::text(
                        "A compiling rule was already submitted. Stop calling tools.",
                    ));
                }

                let attempt = state.attempts.fetch_add(1, Ordering::SeqCst) + 1;
                let source = program.clone();
                let outcome = tokio::task::spawn_blocking(move || {
                    sieve::compile(&source).map(|_compiled| ()).map_err(|error| error.to_string())
                })
                .await
                .map_err(|error| {
                    ToolExecutionError::new(
                        ToolErrorKind::Other,
                        format!("compile task failed: {error}"),
                    )
                })?;

                match outcome {
                    Ok(()) => {
                        state.record_submitted(&program);
                        let _ = updates
                            .send(OnboardUpdate::Sieve(SieveAttempt {
                                attempt,
                                script: program,
                                error: None,
                            }))
                            .await;
                        Ok(ToolOutput::text(
                            "ok: the program compiled. Stop calling tools.",
                        ))
                    }
                    Err(error) => {
                        state.record_error(&error);
                        let _ = updates
                            .send(OnboardUpdate::Sieve(SieveAttempt {
                                attempt,
                                script: program,
                                error: Some(error.clone()),
                            }))
                            .await;
                        Ok(ToolOutput::text(format!(
                            "compile error:\n{error}\nFix the program and call submit_sieve again."
                        )))
                    }
                }
            })
        },
    )
}

/// Builds the `web_search` tool.
fn web_search_tool(client: Arc<Client>, updates: mpsc::Sender<OnboardUpdate>) -> DynamicTool {
    let parameters = json!({
        "type": "object",
        "properties": {
            "query": { "type": "string" },
            "limit": { "type": "integer", "minimum": 1, "maximum": MAX_SEARCH_RESULTS }
        },
        "required": ["query"]
    });

    DynamicTool::new(
        tool_name("web_search"),
        "Search the web and news for sources about the market topic.",
        parameters,
        move |args| {
            let client = Arc::clone(&client);
            let updates = updates.clone();
            Box::pin(async move {
                let query = args
                    .get("query")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|query| !query.is_empty())
                    .ok_or_else(|| {
                        ToolExecutionError::invalid_args(
                            "web_search requires a non-empty string `query` argument",
                        )
                    })?
                    .to_owned();

                let limit = args
                    .get("limit")
                    .and_then(Value::as_u64)
                    .map(|limit| (limit as u32).clamp(1, MAX_SEARCH_RESULTS))
                    .unwrap_or(MAX_SEARCH_RESULTS / 2);

                let _ = updates
                    .send(OnboardUpdate::Phase(format!(
                        "Researching: web search `{query}`…"
                    )))
                    .await;

                let response = client
                    .search(
                        &query,
                        SearchOptions {
                            limit: Some(limit),
                            sources: Some(vec![SearchSource::Web, SearchSource::News]),
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(|error| {
                        ToolExecutionError::new(
                            ToolErrorKind::Other,
                            format!("web_search failed: {error}"),
                        )
                    })?;

                let results: Vec<Value> = response
                    .data
                    .web
                    .unwrap_or_default()
                    .into_iter()
                    .map(map_search_result)
                    .collect();

                Ok(ToolOutput::json(json!({ "results": results })))
            })
        },
    )
}

/// Maps one Firecrawl search item to the compact shape the model reads.
fn map_search_result(result: SearchResultOrDocument) -> Value {
    match result {
        SearchResultOrDocument::WebResult(web) => json!({
            "title": web.title,
            "url": web.url,
            "description": web.description,
        }),
        SearchResultOrDocument::Document(document) => {
            let url = document
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.source_url.clone())
                .unwrap_or_default();
            json!({
                "url": url,
                "markdown": truncate_chars(
                    document.markdown.as_deref().unwrap_or_default(),
                    MAX_PAGE_CHARS,
                ),
            })
        }
    }
}

/// Builds the `read_page` tool.
fn read_page_tool(client: Arc<Client>, updates: mpsc::Sender<OnboardUpdate>) -> DynamicTool {
    let parameters = json!({
        "type": "object",
        "properties": {
            "url": { "type": "string" }
        },
        "required": ["url"]
    });

    DynamicTool::new(
        tool_name("read_page"),
        "Read one web page and return its main content as Markdown.",
        parameters,
        move |args| {
            let client = Arc::clone(&client);
            let updates = updates.clone();
            Box::pin(async move {
                let raw_url = args
                    .get("url")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|url| !url.is_empty())
                    .ok_or_else(|| {
                        ToolExecutionError::invalid_args(
                            "read_page requires a non-empty string `url` argument",
                        )
                    })?
                    .to_owned();

                let parsed = url::Url::parse(&raw_url).map_err(|error| {
                    ToolExecutionError::invalid_args(format!(
                        "read_page requires an absolute http or https URL: {error}"
                    ))
                })?;
                if !matches!(parsed.scheme(), "http" | "https") {
                    return Err(ToolExecutionError::invalid_args(format!(
                        "read_page only reads http and https URLs, got `{}`",
                        parsed.scheme()
                    )));
                }

                let _ = updates
                    .send(OnboardUpdate::Phase(format!(
                        "Researching: reading `{raw_url}`…"
                    )))
                    .await;

                let document = client
                    .scrape(
                        &raw_url,
                        ScrapeOptions {
                            formats: Some(vec![Format::Markdown]),
                            only_main_content: Some(true),
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(|error| {
                        ToolExecutionError::new(
                            ToolErrorKind::Other,
                            format!("read_page failed: {error}"),
                        )
                    })?;

                let markdown = document.markdown.as_deref().unwrap_or_default();
                if markdown.trim().is_empty() {
                    return Err(ToolExecutionError::new(
                        ToolErrorKind::Other,
                        "read_page returned no readable content for this URL",
                    ));
                }

                Ok(ToolOutput::json(json!({
                    "url": raw_url,
                    "title": document_title(&document),
                    "markdown": truncate_chars(markdown, MAX_PAGE_CHARS),
                })))
            })
        },
    )
}

/// Returns the document title, or an empty string when absent.
fn document_title(document: &Document) -> String {
    document
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.title.clone())
        .unwrap_or_default()
}

/// Truncates `text` to at most `max` characters without splitting one.
pub fn truncate_chars(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_owned();
    }
    text.chars().take(max).collect()
}

/// Builds a [`ToolName`] from a compile-time constant.
fn tool_name(name: &'static str) -> ToolName {
    ToolName::new(name).unwrap_or_else(|_| std::unreachable!("tool names are non-empty constants"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::onboarding::SieveAttempt;

    const GOOD_SIEVE: &str = r#"
        @subject = ("bitcoin" | "btc" | *"coin"+);
        match Market_test {
            @subject
        }
    "#;

    fn test_state() -> SubmitState {
        SubmitState::default()
    }

    fn tool(state: &SubmitState, sender: mpsc::Sender<OnboardUpdate>) -> DynamicTool {
        submit_sieve_tool(state.clone(), sender)
    }

    #[tokio::test]
    async fn submit_tool_accepts_a_compiling_program_and_records_it() {
        let state = test_state();
        let (tx, _keep) = mpsc::channel(16);
        let submit = tool(&state, tx);

        let output = submit
            .run(json!({ "sieve_program": GOOD_SIEVE }))
            .await
            .expect("compiling submission should succeed");

        assert!(output
            .as_text()
            .unwrap_or_default()
            .starts_with("ok: the program compiled"));
        assert_eq!(state.submitted().as_deref(), Some(GOOD_SIEVE));
        assert_eq!(state.attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn submit_tool_returns_the_compiler_error_and_does_not_record() {
        let state = test_state();
        let (tx, _keep) = mpsc::channel(16);
        let submit = tool(&state, tx);

        let output = submit
            .run(json!({ "sieve_program": "not a Sieve program" }))
            .await
            .expect("compiler errors are returned as tool text");

        let text = output.as_text().unwrap_or_default();
        assert!(text.starts_with("compile error:"), "{text}");
        assert!(text.contains("submit_sieve again"), "{text}");
        assert!(state.submitted().is_none());
        assert!(state.last_error().is_some());
    }

    #[tokio::test]
    async fn submit_tool_counts_attempts() {
        let state = test_state();
        let (tx, _keep) = mpsc::channel(16);
        let submit = tool(&state, tx);

        for _ in 0..3 {
            let _ = submit
                .run(json!({ "sieve_program": "not a Sieve program" }))
                .await;
        }

        assert_eq!(state.attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn submit_tool_refuses_a_second_submission_after_success() {
        let state = test_state();
        let (tx, _keep) = mpsc::channel(16);
        let submit = tool(&state, tx);

        let _ = submit.run(json!({ "sieve_program": GOOD_SIEVE })).await;
        let output = submit
            .run(json!({ "sieve_program": "not a Sieve program" }))
            .await
            .expect("a second submission is a normal refusal");

        assert!(output
            .as_text()
            .unwrap_or_default()
            .contains("already submitted"));
        assert_eq!(state.submitted().as_deref(), Some(GOOD_SIEVE));
        assert_eq!(state.attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn submit_tool_rejects_missing_program() {
        let state = test_state();
        let (tx, _keep) = mpsc::channel(16);
        let submit = tool(&state, tx);

        let error = submit
            .run(json!({}))
            .await
            .expect_err("missing program must fail");
        assert_eq!(error.kind(), ToolErrorKind::InvalidArgs);
    }

    #[tokio::test]
    async fn search_args_reject_missing_query() {
        let client = Arc::new(Client::new("fc-test").expect("firecrawl client"));
        let (tx, _keep) = mpsc::channel(16);
        let search = web_search_tool(client, tx);

        let error = search
            .run(json!({}))
            .await
            .expect_err("missing query must fail without a network call");
        assert_eq!(error.kind(), ToolErrorKind::InvalidArgs);
    }

    #[tokio::test]
    async fn read_page_rejects_non_http_urls() {
        let client = Arc::new(Client::new("fc-test").expect("firecrawl client"));
        let (tx, _keep) = mpsc::channel(16);
        let read = read_page_tool(client, tx);

        let error = read
            .run(json!({ "url": "ftp://example.com/page" }))
            .await
            .expect_err("non-http URL must fail without a network call");
        assert_eq!(error.kind(), ToolErrorKind::InvalidArgs);
    }

    #[test]
    fn truncate_chars_is_char_safe() {
        assert_eq!(truncate_chars("abc", 5), "abc");
        assert_eq!(truncate_chars("abcdef", 3), "abc");
        assert_eq!(truncate_chars("ééé", 2), "éé");
        assert_eq!(truncate_chars("aéz", 2), "aé");
        assert_eq!(truncate_chars("", 4), "");
    }

    #[test]
    fn preamble_contains_the_sieve_prompt_and_agent_instructions() {
        let preamble = agent_preamble();
        assert!(preamble.starts_with(SIEVE_SYSTEM_PROMPT));
        assert!(preamble.contains("onboarding research agent"));
    }

    #[test]
    fn sieve_attempt_events_carry_the_attempt_and_script() {
        let attempt = SieveAttempt {
            attempt: 2,
            script: GOOD_SIEVE.to_owned(),
            error: None,
        };
        assert_eq!(attempt.attempt, 2);
    }
}
