//! LLM triage: one Cloudflare Workers AI call per matched post.
//!
//! Triage asks a small model whether a Bluesky post is material evidence for a
//! prediction market, and if so in which direction and how strongly. The model
//! returns a small JSON object; [`parse_triage_response`] turns it into a
//! [`TriageResult`], and [`is_material`] applies the materiality threshold from
//! the specification.
//!
//! # Transport: direct Workers AI `/ai/run`
//!
//! Every LLM call in this crate is one stateless JSON `POST` to Workers AI:
//!
//! ```text
//! POST https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/run
//! Authorization: Bearer {token}
//! cf-aig-gateway-id: {gateway_id}
//! Content-Type: application/json
//!
//! {"model": "{model}", "input": {"messages": [{"role": ..., "content": ...}]}}
//! ```
//!
//! No SDK is involved: [`LlmClient`] owns the `reqwest` transport and is shared
//! with onboarding, which uses the same endpoint for Sieve-rule synthesis.
//! The account id and token come from `CLOUDFLARE_ACCOUNT_ID` and
//! `CLOUDFLARE_API_TOKEN`; the gateway id from `CLOUDFLARE_AI_GATEWAY_ID`
//! (the legacy `CF_AI_GATEWAY_ID` name is accepted), defaulting to
//! Cloudflare's `default` gateway. [`TriageClient`] adds only the triage
//! prompt and model choice. The API key lives in the transport and never
//! appears in an error or log line.
//!
//! # JSON extraction
//!
//! Triage prompts the model for JSON and then parses it resiliently
//! ([`extract_json_object`]: Markdown fences, leading prose, and trailing text
//! are tolerated). Structured output is deliberately not used: it only works on
//! models that honor it, and the triage parse is the risk filter for the whole
//! program, so the crate does not want to depend on that capability across the
//! `@cf/…` models Workers AI serves. The resilient parse keeps the exact
//! reliability envelope this module was already tested against.
//!
//! Request timeout is 60 seconds; a non-2xx status or missing assistant text is
//! an error that carries the endpoint, the HTTP status, and a bounded body
//! excerpt (see [`snippet`]).

use std::time::Duration;

use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde::Serialize;
use serde_json::{json, Value};

use crate::config::Config;
use crate::error::{anyhow, bail, AppResult, Context};
use crate::types::{Direction, ExtractedPost, TrackedMarket, TriageResult};

/// Request timeout for one Workers AI `/ai/run` call.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Longest response excerpt included in an error message, in bytes.
const ERROR_SNIPPET_BYTES: usize = 500;

/// Maximum accepted evidence strength (`0..=3`).
const MAX_STRENGTH: u8 = 3;

/// System message from the triage prompt in the specification.
const SYSTEM_PROMPT: &str = "You evaluate Bluesky posts as signal for a prediction market.";

/// Cloudflare API root under which every account-scoped endpoint sits.
const BASE_URL: &str = "https://api.cloudflare.com/client/v4";

/// One chat message in a Workers AI request.
///
/// The role is `system`, `user`, or `assistant`; content is the raw text (or
/// JSON) of that turn. This is the crate's own tiny message type because the
/// transport posts JSON directly instead of going through an SDK.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LlmMessage {
    role: String,
    content: String,
}

impl LlmMessage {
    /// A system instruction, sent first in every request.
    pub fn system(content: impl Into<String>) -> Self {
        Self {
            role: "system".to_owned(),
            content: content.into(),
        }
    }

    /// A user turn.
    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: "user".to_owned(),
            content: content.into(),
        }
    }

    /// An assistant turn: a previous model answer replayed into the context.
    pub fn assistant(content: impl Into<String>) -> Self {
        Self {
            role: "assistant".to_owned(),
            content: content.into(),
        }
    }
}

/// Shared Workers AI `/ai/run` completion client.
///
/// Cloneable so triage and onboarding can each hold a handle; the underlying
/// `reqwest::Client` pools connections across clones.
#[derive(Clone)]
pub struct LlmClient {
    client: reqwest::Client,
    run_url: String,
    api_key: String,
    gateway_id: String,
}

impl LlmClient {
    /// Builds the client from the process environment.
    ///
    /// `CLOUDFLARE_ACCOUNT_ID` and `CLOUDFLARE_API_TOKEN` are required; the
    /// gateway id comes from `CLOUDFLARE_AI_GATEWAY_ID` (the legacy
    /// `CF_AI_GATEWAY_ID` name is accepted), falling back to Cloudflare's
    /// `default` gateway. Misconfiguration fails here (at startup) rather than
    /// as an uninformative error on the first LLM call.
    pub fn new() -> AppResult<Self> {
        let account_id = std::env::var("CLOUDFLARE_ACCOUNT_ID")
            .map_err(|_| anyhow!("CLOUDFLARE_ACCOUNT_ID is not set; run `wrangler whoami` to find it"))?;
        let account_id = account_id.trim();
        if account_id.is_empty() || account_id.contains('/') || account_id.chars().any(char::is_whitespace)
        {
            bail!(
                "CLOUDFLARE_ACCOUNT_ID must be a non-empty account id with no `/` or whitespace"
            );
        }
        let api_key = std::env::var("CLOUDFLARE_API_TOKEN")
            .map_err(|_| anyhow!("CLOUDFLARE_API_TOKEN is not set; run `wrangler auth token` to get it"))?;
        let gateway_id = std::env::var("CLOUDFLARE_AI_GATEWAY_ID")
            .or_else(|_| std::env::var("CF_AI_GATEWAY_ID"))
            .unwrap_or_else(|_| "default".to_owned());
        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .context("building the Workers AI HTTP client")?;

        Ok(Self {
            client,
            run_url: format!("{BASE_URL}/accounts/{account_id}/ai/run"),
            api_key,
            gateway_id,
        })
    }

    /// Sends one chat-completion request and returns the assistant text.
    ///
    /// Workers AI wraps the answer in several possible envelope shapes; the
    /// extraction logic accepts all of them. A JSON body the model returns is
    /// left to the caller (see [`extract_json_object`]).
    pub async fn complete(&self, model: &str, system: &str, user: &str) -> AppResult<String> {
        let messages = [LlmMessage::system(system), LlmMessage::user(user)];
        self.complete_messages(model, &messages).await
    }

    /// Sends a multi-turn chat-completion request and returns the assistant text.
    ///
    /// `turns` is the conversation so far, newest message last, without the
    /// system prompt: the request is built as `[system, turns…]`. This is how
    /// sieve synthesis replays its previous answers and compiler errors as a
    /// real conversation instead of text appended to one growing user prompt.
    pub async fn complete_chat(
        &self,
        model: &str,
        system: &str,
        turns: &[LlmMessage],
    ) -> AppResult<String> {
        let mut messages = Vec::with_capacity(turns.len() + 1);
        messages.push(LlmMessage::system(system));
        messages.extend(turns.iter().cloned());
        self.complete_messages(model, &messages).await
    }

    /// Runs one Workers AI call and returns the assistant text.
    ///
    /// A non-2xx status or a response without assistant text is an error that
    /// includes a short body excerpt. The API key never appears in an error or
    /// log line.
    async fn complete_messages(&self, model: &str, messages: &[LlmMessage]) -> AppResult<String> {
        let body = json!({
            "model": model,
            "input": {
                "messages": messages
            }
        });

        let response = self
            .client
            .post(&self.run_url)
            .header(AUTHORIZATION, format!("Bearer {}", self.api_key))
            .header("cf-aig-gateway-id", &self.gateway_id)
            .header(CONTENT_TYPE, "application/json")
            .body(body.to_string())
            .send()
            .await
            .context("sending request to Cloudflare Workers AI /ai/run")?;

        let status = response.status();
        let raw = response
            .text()
            .await
            .context("reading Cloudflare Workers AI response body")?;

        if !status.is_success() {
            bail!(
                "Cloudflare Workers AI request failed with HTTP {status}: {}",
                snippet(&raw)
            );
        }

        let payload: Value = serde_json::from_str(&raw).with_context(|| {
            format!(
                "parsing Cloudflare Workers AI response as JSON: {}",
                snippet(&raw)
            )
        })?;

        Self::extract_assistant_text(&payload).ok_or_else(|| {
            anyhow!(
                "Cloudflare Workers AI response contained no assistant text: {}",
                snippet(&raw)
            )
        })
    }

    /// Extracts assistant text from the response shapes Workers AI may return.
    ///
    /// Accepted paths, in order:
    ///
    /// - `result.response`
    /// - `response`
    /// - `result.choices[0].message.content`
    /// - `choices[0].message.content`
    pub fn extract_assistant_text(value: &Value) -> Option<String> {
        let direct = value
            .pointer("/result/response")
            .or_else(|| value.pointer("/response"))
            .and_then(Value::as_str)
            .filter(|text| !text.trim().is_empty())
            .map(str::to_owned);
        if direct.is_some() {
            return direct;
        }

        value
            .pointer("/result/choices/0/message/content")
            .or_else(|| value.pointer("/choices/0/message/content"))
            .and_then(Value::as_str)
            .filter(|text| !text.trim().is_empty())
            .map(str::to_owned)
    }

    /// Builds a client with placeholder credentials for unit tests.
    ///
    /// No test calls it, so the loopback URL is never contacted.
    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        Self {
            client: reqwest::Client::new(),
            run_url: "http://127.0.0.1/ai/run".to_owned(),
            api_key: "test-key".to_owned(),
            gateway_id: "test-gateway".to_owned(),
        }
    }
}

/// Triage client: the triage prompt plus the shared LLM client.
///
/// Cloneable so each pipeline worker can hold its own handle.
#[derive(Clone)]
pub struct TriageClient {
    ai: LlmClient,
    model: String,
}

impl TriageClient {
    /// Builds a triage client from process configuration.
    pub fn new(config: &Config) -> AppResult<Self> {
        Ok(Self {
            ai: LlmClient::new()?,
            model: config.triage_model.clone(),
        })
    }

    /// Sends one triage request and parses the model's JSON answer.
    ///
    /// `followers` is optional because the ingestion path does not resolve
    /// follower counts; the prompt renders `unknown` in that case.
    pub async fn evaluate(
        &self,
        market: &TrackedMarket,
        answers: &[crate::types::TrackedAnswer],
        post: &ExtractedPost,
        followers: Option<u64>,
        p_market: f64,
    ) -> AppResult<TriageResult> {
        let (system, user) = if answers.is_empty() {
            build_prompt(market, post, followers, p_market)
        } else {
            build_multiple_choice_prompt(market, answers, post, followers)
        };
        let text = self.ai.complete(&self.model, &system, &user).await?;
        parse_triage_response(&text)
    }
}

/// Builds the prompt for multiple-choice triage.
pub fn build_multiple_choice_prompt(
    market: &TrackedMarket,
    answers: &[crate::types::TrackedAnswer],
    post: &ExtractedPost,
    followers: Option<u64>,
) -> (String, String) {
    let close_time = format_timestamp(market.close_time);
    let posted = format_timestamp(Some(post.created_at));
    let followers = followers
        .map(|count| count.to_string())
        .unwrap_or_else(|| "unknown".to_owned());

    let mut answers_block = String::new();
    for ans in answers {
        answers_block.push_str(&format!("- [{}] \"{}\" (market prob: {:.2})\n", ans.id, ans.text, ans.p_market_last));
    }

    let user = format!(
        r#"Market question: {question}
Market description: {description}
Resolution criteria: {resolution_criteria}
Market close time: {close_time}

Possible Answers:
{answers_block}
Bluesky post:
Author: @{handle} ({followers} followers)
Posted: {posted}
Text: {text}

Evaluate this post against the possible answers. Respond as JSON only:
{{
  "material": true if the post contains genuine evidence, news, or insider commentary relevant to one or more answers; false if noise,
  "affected_answers": [
    {{
      "answer_id": "...",
      "direction": "strong_yes" | "yes" | "neutral" | "no" | "strong_no",
      "strength": integer 0-3 where 0=noise, 1=weak circumstantial, 2=strong secondary evidence, 3=definitive proof,
      "rationale": "one sentence"
    }}
  ]
}}"#,
        question = market.question,
        description = market.description,
        resolution_criteria = market.resolution_criteria,
        close_time = close_time,
        answers_block = answers_block,
        handle = post.handle,
        followers = followers,
        posted = posted,
        text = post.text,
    );

    (SYSTEM_PROMPT.to_owned(), user)
}

/// Builds the `(system, user)` messages for one triage call.
///
/// The prompt text follows the specification. Missing follower counts and close
/// times render as `unknown`; timestamps use RFC 3339 and the market probability
/// is clamped to `[0, 1]` and formatted with two decimals.
pub fn build_prompt(
    market: &TrackedMarket,
    post: &ExtractedPost,
    followers: Option<u64>,
    p_market: f64,
) -> (String, String) {
    let close_time = format_timestamp(market.close_time);
    let posted = format_timestamp(Some(post.created_at));
    let followers = followers
        .map(|count| count.to_string())
        .unwrap_or_else(|| "unknown".to_owned());
    let p_market = p_market.clamp(0.0, 1.0);

    let user = format!(
        r#"Market question: {question}
Resolution criteria: {resolution_criteria}
Market description: {description}
Market close time: {close_time}
Current market probability: {p_market:.2}

Bluesky post:
Author: @{handle} ({followers} followers)
Posted: {posted}
Text: {text}

Evaluate this post. Respond as JSON only:
{{
  "material": true if the post contains genuine evidence, news, or insider commentary that should shift a rational trader's probability; false if it is a coincidence match, joke, spam, or off-topic,
  "direction": one of "strong_yes" | "yes" | "neutral" | "no" | "strong_no",
  "strength": integer 0-3 where 0=noise, 1=weak circumstantial, 2=strong secondary evidence, 3=definitive proof,
  "rationale": one sentence
}}"#,
        question = market.question,
        resolution_criteria = market.resolution_criteria,
        description = market.description,
        close_time = close_time,
        p_market = p_market,
        handle = post.handle,
        followers = followers,
        posted = posted,
        text = post.text,
    );

    (SYSTEM_PROMPT.to_owned(), user)
}

/// Parses the assistant's JSON answer into a [`TriageResult`].
///
/// [`extract_json_object`] tolerates Markdown fences and surrounding prose. A
/// strength outside `0..=3` and a blank rationale are rejected instead of being
/// silently repaired.
pub fn parse_triage_response(raw: &str) -> AppResult<TriageResult> {
    let value = extract_json_object(raw)
        .map_err(|error| anyhow!("parsing triage response JSON: {error}"))?;
    let result: TriageResult = serde_json::from_value(value)
        .map_err(|error| anyhow!("parsing triage response JSON: {error}"))?;

    if result.strength > MAX_STRENGTH {
        bail!(
            "triage response strength {} is out of range 0-{MAX_STRENGTH}",
            result.strength
        );
    }
    for ans in &result.affected_answers {
        if ans.strength > MAX_STRENGTH {
            bail!(
                "triage answer {} strength {} is out of range 0-{MAX_STRENGTH}",
                ans.answer_id,
                ans.strength
            );
        }
    }
    if result.rationale.trim().is_empty() && result.affected_answers.is_empty() {
        bail!("triage response rationale must not be empty");
    }

    Ok(result)
}

/// Applies the materiality threshold.
///
/// A post moves belief only when the model marked it material, chose a
/// non-neutral direction, and assigned at least weak evidence strength.
pub fn is_material(result: &TriageResult) -> bool {
    if !result.material {
        return false;
    }
    if !result.affected_answers.is_empty() {
        return result
            .affected_answers
            .iter()
            .any(|a| a.direction != Direction::Neutral && a.strength >= 1);
    }
    result.direction.is_some_and(|d| d != Direction::Neutral) && result.strength >= 1
}

/// Extracts the first JSON object from an LLM answer.
///
/// The model may wrap its JSON in a Markdown fence or add a sentence before
/// it, so the parser strips fences and then takes the substring from the
/// first `{` to the last `}`.
pub(crate) fn extract_json_object(raw: &str) -> AppResult<Value> {
    let unfenced = strip_code_fence(raw.trim());
    let start = unfenced
        .find('{')
        .ok_or_else(|| anyhow!("response did not contain a JSON object"))?;
    let end = unfenced
        .rfind('}')
        .ok_or_else(|| anyhow!("response did not contain a complete JSON object"))?;
    if end < start {
        bail!("response JSON object is malformed");
    }

    serde_json::from_str(&unfenced[start..=end])
        .with_context(|| "parsing response JSON object".to_owned())
}

/// Removes a single Markdown code fence around the payload, if present.
fn strip_code_fence(raw: &str) -> &str {
    let mut text = raw.trim();

    for prefix in ["```json", "```JSON", "```"] {
        if let Some(rest) = text.strip_prefix(prefix) {
            text = rest.trim_start();
            break;
        }
    }

    if let Some(rest) = text.strip_suffix("```") {
        text = rest.trim_end();
    }

    text
}

/// Formats Unix seconds as RFC 3339, or `unknown` when absent or invalid.
fn format_timestamp(timestamp: Option<i64>) -> String {
    timestamp
        .and_then(|seconds| chrono::DateTime::<chrono::Utc>::from_timestamp(seconds, 0))
        .map(|time| time.to_rfc3339())
        .unwrap_or_else(|| "unknown".to_owned())
}

/// Truncates response text to [`ERROR_SNIPPET_BYTES`] without splitting a
/// character or exceeding the byte budget.
pub(crate) fn snippet(text: &str) -> String {
    let text = text.trim();
    if text.len() <= ERROR_SNIPPET_BYTES {
        return text.to_owned();
    }

    // Leave room for the ellipsis so the whole excerpt stays within the budget.
    let mut end = ERROR_SNIPPET_BYTES - '…'.len_utf8();
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    let mut out = text[..end].to_owned();
    out.push('…');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{MarketId, MarketStatus};
    use serde_json::json;

    fn sample_market() -> TrackedMarket {
        TrackedMarket {
            id: MarketId::from("Market_test"),
            slug: "will-the-thing-happen".to_owned(),
            question: "Will the thing happen by Friday?".to_owned(),
            description: "A market about the thing.".to_owned(),
            resolution_criteria: "Resolves YES if the thing happens.".to_owned(),
            close_time: Some(1_700_000_000),
            status: MarketStatus::Active,
            budget_mana: 100.0,
            invested_mana: 0.0,
            realized_pnl_mana: 0.0,
            yes_shares: 0.0,
            no_shares: 0.0,
            p_internal_log_odds: 0.0,
            p_market_last: 0.63,
            belief_updated_at: 1_690_000_000,
            sieve_source: "match Market_test { \"thing\" }".to_owned(),
            discord_thread_id: String::new(),
            created_at: 1_690_000_000,
        }
    }

    fn sample_post() -> ExtractedPost {
        ExtractedPost {
            uri: "at://did:plc:author/app.bsky.feed.post/3kabc".to_owned(),
            did: "did:plc:author".to_owned(),
            handle: "alice.test".to_owned(),
            text: "The thing is confirmed for Friday.".to_owned(),
            created_at: 1_699_999_000,
        }
    }

    fn valid_json() -> &'static str {
        r#"{
            "material": true,
            "direction": "yes",
            "strength": 2,
            "rationale": "A named source confirms the schedule."
        }"#
    }

    #[test]
    fn prompt_contains_market_post_and_probability_fields() {
        let (system, user) = build_prompt(&sample_market(), &sample_post(), Some(1_234), 0.63);

        assert_eq!(
            system,
            "You evaluate Bluesky posts as signal for a prediction market."
        );
        assert!(user.contains("Will the thing happen by Friday?"));
        assert!(user.contains("Resolves YES if the thing happens."));
        assert!(user.contains("A market about the thing."));
        assert!(user.contains("Market close time: 2023-11-14T22:13:20"));
        assert!(user.contains("Current market probability: 0.63"));
        assert!(user.contains("Author: @alice.test (1234 followers)"));
        assert!(user.contains("Posted: 2023-11-14T21:56:40"));
        assert!(user.contains("Text: The thing is confirmed for Friday."));
        assert!(user.contains("\"direction\": one of \"strong_yes\""));
    }

    #[test]
    fn prompt_uses_unknown_for_missing_followers_and_close_time() {
        let mut market = sample_market();
        market.close_time = None;

        let (_, user) = build_prompt(&market, &sample_post(), None, 0.5);

        assert!(user.contains("Market close time: unknown"));
        assert!(user.contains("Author: @alice.test (unknown followers)"));
    }

    #[test]
    fn prompt_clamps_and_formats_probability() {
        let (_, user) = build_prompt(&sample_market(), &sample_post(), Some(0), 1.7);
        assert!(user.contains("Current market probability: 1.00"));

        let (_, user) = build_prompt(&sample_market(), &sample_post(), Some(0), -0.2);
        assert!(user.contains("Current market probability: 0.00"));
    }

    #[test]
    fn parses_plain_json() {
        let result = parse_triage_response(valid_json()).expect("plain JSON should parse");

        assert_eq!(
            result,
            TriageResult {
                material: true,
                direction: Some(Direction::Yes),
                strength: 2,
                rationale: "A named source confirms the schedule.".to_owned(),
                affected_answers: Vec::new(),
            }
        );
    }

    #[test]
    fn parses_fenced_json() {
        let raw = format!("```json\n{}\n```", valid_json());
        let result = parse_triage_response(&raw).expect("fenced JSON should parse");
        assert_eq!(result.direction, Some(Direction::Yes));
    }

    #[test]
    fn parses_json_with_leading_prose() {
        let raw = format!("Here is my evaluation:\n{}\nThanks.", valid_json());
        let result = parse_triage_response(&raw).expect("prose around JSON should parse");
        assert_eq!(result.strength, 2);
    }

    #[test]
    fn missing_required_field_is_an_error() {
        let raw = r#"{"material": true, "strength": 2, "rationale": ""}"#;
        let error = parse_triage_response(raw).expect_err("blank rationale and no answers should fail");
        assert!(error.to_string().contains("rationale"), "{error}");
    }

    #[test]
    fn strength_above_three_is_rejected_not_clamped() {
        let raw = r#"{
            "material": true,
            "direction": "yes",
            "strength": 4,
            "rationale": "Definitive."
        }"#;
        let error = parse_triage_response(raw).expect_err("strength 4 should fail");
        assert!(error.to_string().contains("strength"), "{error}");
    }

    #[test]
    fn empty_rationale_is_rejected() {
        let raw = r#"{
            "material": true,
            "direction": "yes",
            "strength": 1,
            "rationale": "   "
        }"#;
        let error = parse_triage_response(raw).expect_err("blank rationale should fail");
        assert!(error.to_string().contains("rationale"), "{error}");
    }

    #[test]
    fn missing_json_object_is_an_error() {
        assert!(parse_triage_response("no json here").is_err());
        assert!(parse_triage_response("{\"material\": true").is_err());
    }

    #[test]
    fn is_material_requires_all_three_conditions() {
        let material = |material, direction, strength| TriageResult {
            material,
            direction: Some(direction),
            strength,
            rationale: "because".to_owned(),
            affected_answers: Vec::new(),
        };

        assert!(is_material(&material(true, Direction::Yes, 1)));
        assert!(is_material(&material(true, Direction::StrongNo, 3)));
        assert!(!is_material(&material(true, Direction::Yes, 0)));
        assert!(!is_material(&material(true, Direction::Neutral, 3)));
        assert!(!is_material(&material(false, Direction::Yes, 3)));
    }

    #[test]
    fn snippet_stays_within_the_byte_budget_and_never_splits_a_character() {
        let long = "é".repeat(400);
        let out = snippet(&long);

        assert!(out.len() <= ERROR_SNIPPET_BYTES, "{}", out.len());
        assert!(out.ends_with('…'));
    }

    #[test]
    fn extract_json_object_tolerates_fences_and_surrounding_prose() {
        let raw = format!("Here you go:\n```json\n{}\n```\nDone.", valid_json());
        let value = extract_json_object(&raw).expect("JSON object");
        assert_eq!(value["direction"], "yes");
    }

    #[test]
    fn extract_assistant_text_accepts_all_workers_ai_shapes() {
        let answer = "{\"material\":true}";
        let shapes = [
            json!({"result": {"response": answer}}),
            json!({"response": answer}),
            json!({"result": {"choices": [{"message": {"content": answer}}]}}),
            json!({"choices": [{"message": {"content": answer}}]}),
        ];

        for shape in shapes {
            assert_eq!(
                LlmClient::extract_assistant_text(&shape).as_deref(),
                Some(answer),
                "shape: {shape}"
            );
            assert!(LlmClient::extract_assistant_text(&shape).is_some());
        }

        assert_eq!(LlmClient::extract_assistant_text(&json!({"result": {}})), None);
    }

    #[test]
    fn llm_message_json_uses_the_workers_ai_envelope() {
        let messages = [
            LlmMessage::system("sys"),
            LlmMessage::user("user"),
            LlmMessage::assistant("assistant"),
        ];
        let body = json!({
            "model": "@cf/test/model",
            "input": { "messages": messages }
        });
        assert_eq!(body["model"], "@cf/test/model");
        assert_eq!(body["input"]["messages"][0]["role"], "system");
        assert_eq!(body["input"]["messages"][0]["content"], "sys");
        assert_eq!(body["input"]["messages"][1]["role"], "user");
        assert_eq!(body["input"]["messages"][1]["content"], "user");
        assert_eq!(body["input"]["messages"][2]["role"], "assistant");
        assert_eq!(body["input"]["messages"][2]["content"], "assistant");
        assert!(body["input"].get("messages").is_some());
        assert!(body.get("input").is_some(), "envelope must nest under `input`");
    }
}