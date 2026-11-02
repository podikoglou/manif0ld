//! LLM triage: one Cloudflare Workers AI call per matched post.
//!
//! Triage asks a small model whether a Bluesky post is material evidence for a
//! prediction market, and if so in which direction and how strongly. The model
//! returns a small JSON object; [`parse_triage_response`] turns it into a
//! [`TriageResult`], and [`is_material`] applies the materiality threshold from
//! the specification.
//!
//! # Why `reqwest` instead of `rig`
//!
//! This client talks directly to the Workers AI REST endpoint
//! (`/accounts/{account_id}/ai/run`) with a `{model, input: {messages}}`
//! envelope and the `cf-aig-gateway-id` header. `rig` 0.46 has no Workers AI
//! `/ai/run` provider, and its OpenAI-compatible client does not emit this
//! envelope. Triage is a single stateless JSON call, so a direct `reqwest` POST
//! is simpler and matches the documented request shape exactly.
//!
//! The API key is held in [`TriageClient`] and sent in the `Authorization`
//! header. It is never logged, and error messages contain only the endpoint,
//! HTTP status, and a short response snippet.

use std::time::Duration;

use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde_json::{json, Value};

use crate::config::Config;
use crate::error::{anyhow, bail, AppResult, Context};
use crate::types::{Direction, ExtractedPost, TrackedMarket, TriageResult};

/// Request timeout for one triage call.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Longest response snippet included in an error message.
const ERROR_SNIPPET_CHARS: usize = 400;

/// Maximum accepted evidence strength (`0..=3`).
const MAX_STRENGTH: u8 = 3;

/// System message from the triage prompt in the specification.
const SYSTEM_PROMPT: &str = "You evaluate Bluesky posts as signal for a prediction market.";

/// HTTP client for the Cloudflare Workers AI `/ai/run` endpoint.
///
/// Cloneable so each pipeline worker can hold its own handle. The underlying
/// `reqwest::Client` pools connections across clones.
#[derive(Clone)]
pub struct TriageClient {
    client: reqwest::Client,
    run_url: String,
    api_key: String,
    gateway_id: String,
    model: String,
}

impl TriageClient {
    /// Builds a triage client from process configuration.
    ///
    /// The Workers AI URL is validated here so a malformed configuration fails
    /// at startup rather than on the first matched post.
    pub fn new(config: &Config) -> AppResult<Self> {
        let run_url = config.cf_ai_run_url()?;
        let parsed = url::Url::parse(&run_url)
            .with_context(|| format!("validating Cloudflare Workers AI URL {run_url}"))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            bail!(
                "Cloudflare Workers AI URL must use http or https, got {}",
                parsed.scheme()
            );
        }

        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .context("building triage HTTP client")?;

        Ok(Self {
            client,
            run_url,
            api_key: config.cf_api_key.clone(),
            gateway_id: config.cf_ai_gateway_id.clone(),
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
        post: &ExtractedPost,
        followers: Option<u64>,
        p_market: f64,
    ) -> AppResult<TriageResult> {
        let (system, user) = build_prompt(market, post, followers, p_market);
        let body = json!({
            "model": self.model,
            "input": {
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user}
                ]
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
            .context("sending triage request to Cloudflare Workers AI")?;

        let status = response.status();
        let raw = response
            .text()
            .await
            .context("reading Cloudflare Workers AI response body")?;

        if !status.is_success() {
            bail!(
                "Cloudflare Workers AI triage failed with HTTP {status}: {}",
                snippet(&raw)
            );
        }

        let payload: Value = serde_json::from_str(&raw).with_context(|| {
            format!(
                "parsing Cloudflare Workers AI response as JSON: {}",
                snippet(&raw)
            )
        })?;

        let text = extract_assistant_text(&payload).ok_or_else(|| {
            anyhow!(
                "Cloudflare Workers AI response contained no assistant text: {}",
                snippet(&raw)
            )
        })?;

        parse_triage_response(&text)
    }
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
/// The model may wrap its JSON in a Markdown fence or add a sentence before it,
/// so the parser strips fences and then takes the substring from the first `{`
/// to the last `}`. A strength outside `0..=3` and a blank rationale are
/// rejected instead of being silently repaired.
pub fn parse_triage_response(raw: &str) -> AppResult<TriageResult> {
    let unfenced = strip_code_fence(raw.trim());
    let start = unfenced
        .find('{')
        .ok_or_else(|| anyhow!("triage response did not contain a JSON object"))?;
    let end = unfenced
        .rfind('}')
        .ok_or_else(|| anyhow!("triage response did not contain a complete JSON object"))?;
    if end < start {
        bail!("triage response JSON object is malformed");
    }

    let result: TriageResult = serde_json::from_str(&unfenced[start..=end])
        .map_err(|error| anyhow!("parsing triage response JSON: {error}"))?;

    if result.strength > MAX_STRENGTH {
        bail!(
            "triage response strength {} is out of range 0-{MAX_STRENGTH}",
            result.strength
        );
    }
    if result.rationale.trim().is_empty() {
        bail!("triage response rationale must not be empty");
    }

    Ok(result)
}

/// Applies the materiality threshold.
///
/// A post moves belief only when the model marked it material, chose a
/// non-neutral direction, and assigned at least weak evidence strength.
pub fn is_material(result: &TriageResult) -> bool {
    result.material && result.direction != Direction::Neutral && result.strength >= 1
}

/// Extracts assistant text from the response shapes Workers AI may return.
///
/// Accepted paths, in order:
///
/// - `result.response`
/// - `response`
/// - `result.choices[0].message.content`
/// - `choices[0].message.content`
fn extract_assistant_text(value: &Value) -> Option<String> {
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

/// Truncates response text for an error message without splitting a character.
fn snippet(text: &str) -> String {
    let mut out: String = text.chars().take(ERROR_SNIPPET_CHARS).collect();
    if text.chars().count() > ERROR_SNIPPET_CHARS {
        out.push('…');
    }
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
                direction: Direction::Yes,
                strength: 2,
                rationale: "A named source confirms the schedule.".to_owned(),
            }
        );
    }

    #[test]
    fn parses_fenced_json() {
        let raw = format!("```json\n{}\n```", valid_json());
        let result = parse_triage_response(&raw).expect("fenced JSON should parse");
        assert_eq!(result.direction, Direction::Yes);
    }

    #[test]
    fn parses_json_with_leading_prose() {
        let raw = format!("Here is my evaluation:\n{}\nThanks.", valid_json());
        let result = parse_triage_response(&raw).expect("prose around JSON should parse");
        assert_eq!(result.strength, 2);
    }

    #[test]
    fn missing_required_field_is_an_error() {
        let raw = r#"{"material": true, "strength": 2, "rationale": "ok"}"#;
        let error = parse_triage_response(raw).expect_err("missing direction should fail");
        assert!(error.to_string().contains("direction"), "{error}");
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
            direction,
            strength,
            rationale: "because".to_owned(),
        };

        assert!(is_material(&material(true, Direction::Yes, 1)));
        assert!(is_material(&material(true, Direction::StrongNo, 3)));
        assert!(!is_material(&material(true, Direction::Yes, 0)));
        assert!(!is_material(&material(true, Direction::Neutral, 3)));
        assert!(!is_material(&material(false, Direction::Yes, 3)));
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
                extract_assistant_text(&shape).as_deref(),
                Some(answer),
                "shape: {shape}"
            );
        }
    }

    #[test]
    fn extract_assistant_text_rejects_missing_or_blank_text() {
        assert!(extract_assistant_text(&json!({"result": {}})).is_none());
        assert!(extract_assistant_text(&json!({"result": {"response": "   "}})).is_none());
        assert!(extract_assistant_text(&json!({"choices": []})).is_none());
    }
}
