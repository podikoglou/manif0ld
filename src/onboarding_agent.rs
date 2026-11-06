//! The onboarding research agent: research a market, then submit a Sieve rule.
//!
//! Onboarding is a `rig` agent, not a fixed prompt loop. The agent is given
//! four tools:
//!
//! - `web_search` (Firecrawl) finds current sources for the market topic.
//! - `read_page` (Firecrawl) reads the most useful sources in full.
//! - `dry_run_sieve` compiles a candidate program and reports which sample
//!   posts it matches, plus authoring warnings, so the agent can tighten the
//!   rule against look-alike posts before it goes live.
//! - `submit_sieve` compiles a candidate program and reports the compiler
//!   error (or warnings) back to the model.
//!
//! The agent keeps retrying `submit_sieve` until the program compiles; there is
//! no attempt limit. [`MAX_AGENT_TURNS`] is only a runaway guard. The accepted
//! program is written into a shared slot, so the caller never parses the
//! model's final text (which is ignored: the agent only calls tools).
//!
//! The module is deliberately narrow. Its tools and state are reusable for a
//! later periodic research agent, but for now research feeds only the Sieve
//! script.

/// Tool-calling surface of the onboarding agent.
///
/// Compile errors are surfaced as text for the model to read and retry. The
/// raw error is an `anyhow` chain whose top context is only "invalid Sieve
/// source: <program>"; the useful message sits at the root cause, so tools
/// print [`root_cause`].
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use firecrawl::{
    Client, Document, Format, ScrapeOptions, SearchOptions, SearchResultOrDocument, SearchSource,
};
use rig::message::ToolName;
use rig::tool::{DynamicTool, ToolErrorKind, ToolExecutionError, ToolOutput};
use serde_json::{json, Value};
use tokio::sync::mpsc;

use crate::error::{bail, AppResult};
use crate::onboarding::{
    build_sieve_user_prompt, ManifoldSlugMarket, OnboardUpdate, ResearchStep, SieveAttempt,
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

/// Maximum posts one `dry_run_sieve` call may test.
const MAX_DRY_RUN_POSTS: usize = 20;

/// Longest post text one `dry_run_sieve` call accepts, in characters.
const MAX_DRY_RUN_POST_CHARS: usize = 2_000;

/// Longest post excerpt echoed back in a dry-run result.
const DRY_RUN_EXCERPT_CHARS: usize = 160;

/// Instructions appended to [`SIEVE_SYSTEM_PROMPT`] for the agent run.
pub const AGENT_INSTRUCTIONS: &str = r##"You are the onboarding research agent for a prediction-market trader.

1. Research the market topic.
   - Use `web_search` to find current, reliable sources.
   - Use `read_page` to read the most useful results.
   - Find the exact names, aliases, ticker symbols, product names, people,
     organizations, and events that Bluesky posts would use.
   - Use at most 8 searches and 5 page reads.
   - Prefer primary sources, official announcements, and recent news.

2. Test one candidate rule with `dry_run_sieve` before submitting it.
   - Write 6-10 short sample Bluesky posts: several on-topic posts that the
     market genuinely cares about, and several look-alike posts that must NOT
     match (other languages, weather or traffic bots, deal spam, posts using
     one of your keywords in an unrelated sense, airport style "AP"), plus a
     couple of clearly unrelated posts.
   - Call `dry_run_sieve` with the candidate program and those samples.
   - If an off-topic sample matched, narrow the matching arm (add context,
     tighten the window, drop the arm) and call `dry_run_sieve` again.
   - Address the warnings the tool returns; they mark fragile constructs.

3. Submit the final program with `submit_sieve`.
   - The program must start with `match Market_<id> { ... }`.
   - `submit_sieve` returns `ok` when the program compiles, or the compiler
     error. Fix and resubmit on error; there is no attempt limit.
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
        dry_run_sieve_tool(updates.clone()),
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

/// Flattens an `anyhow` chain to the message text of its deepest cause.
///
/// `error.to_string()` prints only the outermost context ("invalid Sieve
/// source: <program>"), which hides the actual compiler message from the
/// model. The deepest cause is the parser or engine message it needs.
fn root_cause(error: &anyhow::Error) -> String {
    let mut message = error.to_string();
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error.as_ref());
    while let Some(cause) = current.and_then(std::error::Error::source) {
        message = cause.to_string();
        current = Some(cause);
    }
    message
}

/// Builds the `submit_sieve` tool.
///
/// Each call compiles the program on a blocking thread, because Cranelift is
/// CPU-bound and [`sieve::CompiledSieve`] is `!Send`. The closure returns only
/// `Result<(), String>`, so the compiled engine never crosses the task
/// boundary. A compiler error is returned as ordinary tool text so the model
/// can read it and retry.
fn submit_sieve_tool(state: SubmitState, updates: mpsc::Sender<OnboardUpdate>) -> DynamicTool {
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
                    sieve::compile(&source)
                        .map(|compiled| compiled.warnings().to_vec())
                        .map_err(|error| root_cause(&error))
                })
                .await
                .map_err(|error| {
                    ToolExecutionError::new(
                        ToolErrorKind::Other,
                        format!("compile task failed: {error}"),
                    )
                })?;

                match outcome {
                    Ok(warnings) => {
                        state.record_submitted(&program);
                        let _ = updates
                            .send(OnboardUpdate::Sieve(SieveAttempt {
                                attempt,
                                script: program,
                                error: None,
                            }))
                            .await;
                        let warnings_text = if warnings.is_empty() {
                            String::new()
                        } else {
                            format!(
                                ". Warnings collected below; fix them if you can:\n{}",
                                warnings.join("\n")
                            )
                        };
                        Ok(ToolOutput::text(format!(
                            "ok: the program compiled{warnings_text}. Stop calling tools."
                        )))
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

/// Builds the `dry_run_sieve` tool.
///
/// Like `submit_sieve`, compilation runs on a blocking thread because
/// [`sieve::CompiledSieve`] holds JIT mappings and is `!Send`. The tool never
/// records a submission; it exists so the agent can observe matching behavior
/// on sample posts before anything goes live.
fn dry_run_sieve_tool(updates: mpsc::Sender<OnboardUpdate>) -> DynamicTool {
    let parameters = json!({
        "type": "object",
        "properties": {
            "sieve_program": {
                "type": "string",
                "description": "The complete candidate Sieve program."
            },
            "test_posts": {
                "type": "array",
                "items": { "type": "string" },
                "minItems": 1,
                "maxItems": MAX_DRY_RUN_POSTS,
                "description": "Short Bluesky-style sample posts to test the program against."
            }
        },
        "required": ["sieve_program", "test_posts"]
    });

    DynamicTool::new(
        tool_name("dry_run_sieve"),
        "Compile a candidate Sieve program and report which sample posts it matches, plus \
         authoring warnings. Use it to tighten the rule before submitting it.",
        parameters,
        move |args| {
            let updates = updates.clone();
            Box::pin(async move {
                let program = args
                    .get("sieve_program")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        ToolExecutionError::invalid_args(
                            "dry_run_sieve requires a string `sieve_program` argument",
                        )
                    })?
                    .to_owned();

                let posts = args
                    .get("test_posts")
                    .and_then(Value::as_array)
                    .map(|items| {
                        items
                            .iter()
                            .filter_map(Value::as_str)
                            .take(MAX_DRY_RUN_POSTS)
                            .map(|text| truncate_chars(text.trim(), MAX_DRY_RUN_POST_CHARS))
                            .filter(|text| !text.is_empty())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                if posts.is_empty() {
                    return Err(ToolExecutionError::invalid_args(
                        "dry_run_sieve requires at least one non-empty `test_posts` entry",
                    ));
                }

                let _ = updates
                    .send(OnboardUpdate::Phase(format!(
                        "Dry-running the rule against {} sample posts…",
                        posts.len()
                    )))
                    .await;

                let outcome = tokio::task::spawn_blocking(move || {
                    sieve::compile(&program).map(|engine| {
                        (
                            engine.warnings().to_vec(),
                            posts
                                .iter()
                                .map(|text| {
                                    (
                                        !engine.matches(text).is_empty(),
                                        truncate_chars(text, DRY_RUN_EXCERPT_CHARS),
                                    )
                                })
                                .collect::<Vec<(bool, String)>>(),
                        )
                    })
                })
                .await
                .map_err(|error| {
                    ToolExecutionError::new(
                        ToolErrorKind::Other,
                        format!("dry-run task failed: {error}"),
                    )
                })?;

                match outcome {
                    Ok((warnings, results)) => {
                        let total = results.len();
                        let matched = results.iter().filter(|(matched, _)| *matched).count();
                        let _ = updates
                            .send(OnboardUpdate::Activity(ResearchStep::DryRun {
                                matched,
                                total,
                                warnings: warnings.len(),
                            }))
                            .await;
                        let results: Vec<Value> = results
                            .into_iter()
                            .map(|(matched, text)| json!({ "matched": matched, "text": text }))
                            .collect();
                        Ok(ToolOutput::json(json!({
                            "warnings": warnings,
                            "results": results,
                            "next": "narrow any rule arm that matched an off-topic post"
                        })))
                    }
                    Err(error) => {
                        let message = root_cause(&error);
                        let _ = updates
                            .send(OnboardUpdate::Activity(ResearchStep::DryRunFailed {
                                error: message.clone(),
                            }))
                            .await;
                        Ok(ToolOutput::text(format!(
                            "compile error:\n{message}\nFix the program and call dry_run_sieve again."
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

                let _ = updates
                    .send(OnboardUpdate::Activity(ResearchStep::Search {
                        query: query.clone(),
                        results: results.len(),
                    }))
                    .await;

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

                let _ = updates
                    .send(OnboardUpdate::Activity(ResearchStep::Page {
                        url: raw_url.clone(),
                        chars: markdown.chars().count(),
                    }))
                    .await;

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
    async fn dry_run_tool_reports_matches_and_warnings() {
        let (tx, _keep) = mpsc::channel(16);
        let dry = dry_run_sieve_tool(tx);

        // `("talarico" | "ff")` exercises the short-literal warning too.
        let output = dry
            .run(json!({
                "sieve_program": r#"match Market_test { ("talarico" | "ff") &..4 "senate" }"#,
                "test_posts": ["talarico speaks at the senate hearing", "weather is nice today"]
            }))
            .await
            .expect("valid dry-run should succeed");

        let value = output.as_json().cloned().expect("dry-run returns JSON");
        let results = value["results"].as_array().expect("results array");
        assert!(
            results.iter().any(|r| r["matched"] == json!(true)),
            "{value}"
        );
        assert!(
            results.iter().any(|r| r["matched"] == json!(false)),
            "{value}"
        );
        let warnings = value["warnings"].as_array().expect("warnings array");
        assert!(warnings.iter().any(|w| w
            .as_str()
            .unwrap_or_default()
            .contains("under 3 characters")));
    }

    #[tokio::test]
    async fn dry_run_tool_reports_match_counts_as_activity() {
        let (tx, mut rx) = mpsc::channel(16);
        let dry = dry_run_sieve_tool(tx);

        let _ = dry
            .run(json!({
                "sieve_program": r#"match Market_test { ("talarico" | "ff") &..4 "senate" }"#,
                "test_posts": ["talarico speaks at the senate hearing", "weather is nice today"]
            }))
            .await
            .expect("valid dry-run should succeed");

        let mut activity = None;
        while let Ok(update) = rx.try_recv() {
            if let OnboardUpdate::Activity(step) = update {
                activity = Some(step);
            }
        }

        match activity {
            Some(ResearchStep::DryRun {
                matched,
                total,
                warnings,
            }) => {
                assert_eq!(matched, 1, "exactly one sample matches");
                assert_eq!(total, 2, "both samples are counted");
                assert!(warnings >= 1, "the short literal must be warned about");
            }
            other => panic!("expected a dry-run activity update, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn dry_run_tool_returns_compile_errors_as_tool_text() {
        let (tx, _keep) = mpsc::channel(16);
        let dry = dry_run_sieve_tool(tx);

        let output = dry
            .run(json!({
                "sieve_program": r#"match Market_test { "Donald Trump" }"#,
                "test_posts": ["Donald Trump spoke"]
            }))
            .await
            .expect("compile errors come back as tool text, not failures");

        let text = output.as_text().unwrap_or_default();
        assert!(text.starts_with("compile error:"), "{text}");
        assert!(text.contains("spans several tokens"), "{text}");
        assert!(!text.contains("invalid Sieve source"), "{text}");
    }

    #[tokio::test]
    async fn dry_run_tool_rejects_missing_args() {
        let (tx, _keep) = mpsc::channel(16);
        let dry = dry_run_sieve_tool(tx);

        let error = dry.run(json!({})).await.expect_err("no args must fail");
        assert_eq!(error.kind(), ToolErrorKind::InvalidArgs);

        let error = dry
            .run(json!({ "sieve_program": "match M { \"x\" }" }))
            .await
            .expect_err("no test posts must fail");
        assert_eq!(error.kind(), ToolErrorKind::InvalidArgs);
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
