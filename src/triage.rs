//! Triage: one decision-model call per matched post.
//!
//! Triage asks a System One model four typed questions about a post: whether it
//! carries new evidence, which outcome it makes more likely, how strong that
//! evidence is, and whether the market price already reflects it. The model
//! returns calibrated probabilities instead of generated text, so there is no
//! JSON to parse and no output schema to drift.
//!
//! # Transport: the OpenRouter decisions endpoint, through `rig::typesafeai`
//!
//! Decision models are not chat models. OpenRouter serves them on
//! `POST /api/alpha/decisions` with a `{model, state, questions}` body, so the
//! chat completions provider cannot call them. The vendored `rig` ships
//! [`rig::typesafeai`], which sends exactly that body over the shared reqwest
//! transport and reads the `{answers, usage}` reply. [`TriageClient`] supplies
//! the endpoint, the key, the model id, and the question wording.
//!
//! `probabilities` and `confidence` are optional on OpenRouter's answer schema.
//! A missing `confidence` arrives as `None` through the typed
//! [`rig::typesafeai::DynamicScoreAnswer`] path; a missing `probabilities` is a
//! decode error, which fails the call closed rather than inventing a number.
//!
//! [`LlmClient`] stays for onboarding, which is still a chat agent run.
//!
//! # From probabilities to a belief impulse
//!
//! [`assess`] folds the four answers into one signed scale in `[-1, 1]`: the
//! probability of evidence, the direction, the novelty, and the relative
//! strength. [`crate::belief::apply_scaled`] multiplies that scale by `kappa`.
//! The model never prices the market; it only says what the post means.

use std::time::Duration;

use rig::error::ProviderError;
use rig::typesafeai::{
    Choice, ChoiceAnswer, Evaluate, EvaluationResult, Jev, JevConfig, Noul, NoulAnswer, Query,
    Score, ScoreAnswer,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::{parse_model_spec, Config, ModelProvider};
use crate::error::{anyhow, bail, AppResult, Context};
use crate::types::{ExtractedPost, TrackedAnswer, TrackedMarket};

/// Longest response excerpt included in an error message, in bytes.
pub(crate) const ERROR_SNIPPET_BYTES: usize = 500;

/// OpenRouter's decisions endpoint. Chat completions cannot reach it.
const DECISIONS_ENDPOINT: &str = "https://openrouter.ai/api/alpha/decisions";

/// Upper bound on one decisions call. Joey answers in tens of milliseconds.
const DECISIONS_TIMEOUT: Duration = Duration::from_secs(10);

/// Expected evidence strength, on the 0-3 rubric, below which a post is noise.
const MIN_STRENGTH: f64 = 0.5;

/// Smallest impulse scale worth writing. Below this the post is not material.
const MIN_IMPULSE: f64 = 0.01;

const EVIDENCE_INSTRUCTIONS: &str =
    "Does the post give new evidence about the market question?";

/// On a multiple-choice market the propositions live in the answers, not in the
/// question text, which is often just a title. The evidence question must point
/// at the answers or the model judges evidence against a bare title.
const EVIDENCE_INSTRUCTIONS_CHOICES: &str =
    "Does the post give new evidence for or against any of the market's answers?";

const EVIDENCE_YES: &str = "The post reports a new fact, a named source, an official statement, or \
                            insider knowledge that bears on the question.";

const EVIDENCE_NO: &str = "The post is a coincidence match, a joke, spam, an opinion, a question, \
                           or a restatement of facts that were already public.";

const SUPPORTS_YES_INSTRUCTIONS: &str = "Which outcome does the post make more likely?";

const STRENGTH_INSTRUCTIONS: &str = "How strong is the evidence in the post?";

const PRICED_INSTRUCTIONS: &str = "Does the market price already reflect this information?";

const PRICED_YES: &str = "The information was already public before the post, or the price moved \
                          after it became public.";

const PRICED_NO: &str = "The information is new to the market, or the price has not moved yet.";

const SUPPORTED_ANSWER_INSTRUCTIONS: &str =
    "Which answer does the post give evidence for?";

/// The abstain alternative offered on every multiple-choice market.
///
/// A matched post can be real, relevant-looking news that changes the
/// probability of none of the tracked answers (a race call for an untracked
/// race, for example). Without an abstain option the choice question is a
/// forced pick over N answers, and whatever it picks would receive an impulse.
/// When the model picks this label, no belief moves.
const NONE_ALTERNATIVE: &str = "None of the market's answers";

/// Outcome the post favors on a binary market.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Polarity {
    Yes,
    No,
}

/// Ordered evidence rubric. Array order is the wire position.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceLevel {
    Noise,
    Weak,
    Strong,
    Definitive,
}

/// The four questions for a binary market.
///
/// Struct field names become the wire question IDs; the default type
/// parameters are the question builders and the explicit ones are the answers.
#[derive(Debug, Deserialize, Serialize)]
struct BinaryQuestions<D = Choice<Polarity>, S = Score<EvidenceLevel>, E = Noul, P = Noul> {
    is_evidence: E,
    supports_yes: D,
    evidence_strength: S,
    already_priced: P,
}

/// The four questions for a multiple-choice market.
#[derive(Debug, Deserialize, Serialize)]
struct ChoiceQuestions<A = Choice<String>, S = Score<EvidenceLevel>, E = Noul, P = Noul> {
    is_evidence: E,
    supported_answer: A,
    evidence_strength: S,
    already_priced: P,
}

impl<D, S, E, P> Query for BinaryQuestions<D, S, E, P>
where
    D: Query,
    S: Query,
    E: Query,
    P: Query,
{
    type Response = BinaryQuestions<D::Response, S::Response, E::Response, P::Response>;
    type Output = BinaryQuestions<D::Output, S::Output, E::Output, P::Output>;

    fn decode(&self, response: Self::Response) -> Result<Self::Output, ProviderError> {
        Ok(BinaryQuestions {
            is_evidence: self.is_evidence.decode(response.is_evidence)?,
            supports_yes: self.supports_yes.decode(response.supports_yes)?,
            evidence_strength: self.evidence_strength.decode(response.evidence_strength)?,
            already_priced: self.already_priced.decode(response.already_priced)?,
        })
    }
}

impl<A, S, E, P> Query for ChoiceQuestions<A, S, E, P>
where
    A: Query,
    S: Query,
    E: Query,
    P: Query,
{
    type Response = ChoiceQuestions<A::Response, S::Response, E::Response, P::Response>;
    type Output = ChoiceQuestions<A::Output, S::Output, E::Output, P::Output>;

    fn decode(&self, response: Self::Response) -> Result<Self::Output, ProviderError> {
        Ok(ChoiceQuestions {
            is_evidence: self.is_evidence.decode(response.is_evidence)?,
            supported_answer: self.supported_answer.decode(response.supported_answer)?,
            evidence_strength: self.evidence_strength.decode(response.evidence_strength)?,
            already_priced: self.already_priced.decode(response.already_priced)?,
        })
    }
}

type BinaryQuery = BinaryQuestions<Choice<Polarity>, Score<EvidenceLevel>, Noul, Noul>;
type BinaryAnswers =
    BinaryQuestions<ChoiceAnswer<Polarity>, ScoreAnswer<EvidenceLevel>, NoulAnswer, NoulAnswer>;
type ChoiceQuery = ChoiceQuestions<Choice<String>, Score<EvidenceLevel>, Noul, Noul>;
type ChoiceAnswers =
    ChoiceQuestions<ChoiceAnswer<String>, ScoreAnswer<EvidenceLevel>, NoulAnswer, NoulAnswer>;

/// The shared state every question reads.
///
/// `market.description` is deliberately absent: it is long and it sways the
/// evidence judgment. `resolution_criteria` carries the definition instead.
#[derive(Debug, Serialize)]
struct TriageState<'a> {
    market: MarketState<'a>,
    post: PostState<'a>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    answers: Vec<AnswerState<'a>>,
}

#[derive(Debug, Serialize)]
struct MarketState<'a> {
    question: &'a str,
    resolution_criteria: &'a str,
    close_time: String,
    market_probability: f64,
}

#[derive(Debug, Serialize)]
struct PostState<'a> {
    author_handle: &'a str,
    author_followers: Option<u64>,
    posted_at: String,
    text: &'a str,
}

#[derive(Debug, Serialize)]
struct AnswerState<'a> {
    text: &'a str,
    market_probability: f64,
}

/// Which belief the post is about.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Target {
    /// The market itself.
    Binary,
    /// One answer of a multiple-choice market.
    Answer(String),
    /// No tracked answer: the model chose the abstain alternative. The post is
    /// recorded in the signal ledger but never moves a belief.
    NoAnswer,
}

/// Calibrated answers for one post, before the materiality policy runs.
#[derive(Clone, Debug, PartialEq)]
pub struct TriageOutcome {
    /// Probability that the post carries genuine evidence.
    pub p_evidence: f64,
    /// Probability mass on the single alternative the post favors.
    ///
    /// On a multiple-choice market this is renormalized over the tracked
    /// answers only: mass the model places on the abstain alternative means
    /// "the post supports no answer", not mass against the chosen one, so it
    /// must not drag the direction toward the chosen answer's opposite.
    pub p_target: f64,
    /// Number of alternatives the mass is spread over: 2 for binary markets.
    pub alternatives: usize,
    /// Expected evidence strength on the 0-3 rubric.
    pub strength: f64,
    /// Probability that the market already reflects the information.
    pub p_priced: f64,
    pub target: Target,
    /// Cost of the call in USD, when OpenRouter reports it.
    pub cost_usd: Option<f64>,
}

/// Thresholds that turn probabilities into a decision.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TriagePolicy {
    pub min_evidence: f64,
}

impl TriagePolicy {
    pub fn from_config(config: &Config) -> Self {
        Self {
            min_evidence: config.triage_min_evidence,
        }
    }
}

/// One triage result: the raw outcome and the decision code made from it.
#[derive(Clone, Debug, PartialEq)]
pub struct TriageAssessment {
    pub outcome: TriageOutcome,
    /// Signed impulse scale in `[-1, 1]`; `None` means the post is not material.
    pub impulse_scale: Option<f64>,
}

/// Turns calibrated answers into a signed impulse scale, or `None`.
///
/// `direction` is the excess probability mass on the favored alternative,
/// normalized so that a uniform answer scores zero and a certain one scores
/// one. For two alternatives it reduces to `2 * p - 1`.
pub fn assess(outcome: &TriageOutcome, policy: TriagePolicy) -> Option<f64> {
    if !outcome.p_evidence.is_finite() || !outcome.p_target.is_finite() {
        return None;
    }
    if matches!(outcome.target, Target::NoAnswer) {
        return None;
    }
    if outcome.p_evidence < policy.min_evidence {
        return None;
    }
    if outcome.strength < MIN_STRENGTH {
        return None;
    }

    let alternatives = outcome.alternatives as f64;
    if alternatives < 2.0 {
        return None;
    }
    let p = outcome.p_target.clamp(0.0, 1.0);
    let uniform = 1.0 / alternatives;
    let direction = ((p - uniform) / (1.0 - uniform)).clamp(-1.0, 1.0);
    let novelty = 1.0 - outcome.p_priced.clamp(0.0, 1.0);
    let strength = (outcome.strength / 3.0).clamp(0.0, 1.0);

    let scale = outcome.p_evidence * direction * novelty * strength;
    (scale.abs() >= MIN_IMPULSE).then_some(scale)
}

/// Triage client: the decisions endpoint plus the question wording.
///
/// Cloneable so each pipeline worker can hold its own handle; the underlying
/// reqwest client is shared per process.
#[derive(Clone)]
pub struct TriageClient {
    client: Jev,
    model: String,
    policy: TriagePolicy,
}

impl TriageClient {
    /// Builds a triage client from process configuration.
    pub fn new(config: &Config) -> AppResult<Self> {
        let (provider, model) = parse_model_spec(&config.triage_model)?;
        if provider != ModelProvider::OpenRouterDecisions {
            bail!(
                "triage model '{}' does not use the decisions endpoint; set TRIAGE_MODEL to \
                 'openrouter-decisions/baseten/joey-1'",
                config.triage_model
            );
        }
        // The rig cloudflare and openrouter providers read their own keys, but
        // the decisions client takes the key as a value, so read it here.
        let key = std::env::var("OPENROUTER_API_KEY").map_err(|_| {
            anyhow!("OPENROUTER_API_KEY is not set; the decisions endpoint needs it")
        })?;
        if key.trim().is_empty() {
            bail!("OPENROUTER_API_KEY is empty; the decisions endpoint needs it");
        }

        Ok(Self {
            client: JevConfig::new(key)
                .model(model)
                .with_endpoint(DECISIONS_ENDPOINT)
                .client(),
            model: model.to_owned(),
            policy: TriagePolicy::from_config(config),
        })
    }

    /// Sends one triage call and returns the outcome plus the decision.
    ///
    /// `followers` is optional because the ingestion path does not resolve
    /// follower counts; the state then omits the field.
    pub async fn evaluate(
        &self,
        market: &TrackedMarket,
        answers: &[TrackedAnswer],
        post: &ExtractedPost,
        followers: Option<u64>,
        p_market: f64,
    ) -> AppResult<TriageAssessment> {
        let state = build_state(market, answers, post, followers, p_market);
        let (outcome, cost_usd) = if answers.is_empty() {
            let result = self.run(&state, binary_questions()?).await?;
            (
                outcome_from_binary(result.answers)?,
                result.usage.and_then(cost),
            )
        } else {
            let result = self.run(&state, choice_questions(answers)?).await?;
            (
                outcome_from_choice(result.answers, answers)?,
                result.usage.and_then(cost),
            )
        };
        let outcome = TriageOutcome { cost_usd, ..outcome };
        let impulse_scale = assess(&outcome, self.policy);
        Ok(TriageAssessment {
            outcome,
            impulse_scale,
        })
    }

    /// Runs one decisions request with a hard timeout.
    async fn run<S, Q>(&self, state: &S, questions: Q) -> AppResult<EvaluationResult<Q::Output>>
    where
        S: Serialize + Sync,
        Q: Query,
    {
        let model = self.client.evaluation();
        let result = tokio::time::timeout(DECISIONS_TIMEOUT, model.evaluate(state, questions))
            .await
            .with_context(|| format!("decisions request to {} timed out", self.model))?
            .map_err(|error| anyhow!("decisions request to {} failed: {error}", self.model))?;
        Ok(result)
    }
}

/// Cost of the call in USD, when the provider reported one.
fn cost(usage: rig::completion::Usage) -> Option<f64> {
    usage.cost.map(|cost| cost.total)
}

fn binary_questions() -> AppResult<BinaryQuery> {
    Ok(BinaryQuestions {
        is_evidence: Noul::new(EVIDENCE_INSTRUCTIONS)?.criteria(EVIDENCE_YES, EVIDENCE_NO)?,
        supports_yes: Choice::new(
            SUPPORTS_YES_INSTRUCTIONS,
            [
                (
                    Polarity::Yes,
                    "The post makes the event in the market question more likely to happen.",
                ),
                (
                    Polarity::No,
                    "The post makes the event in the market question less likely to happen.",
                ),
            ],
        )?,
        evidence_strength: strength_question()?,
        already_priced: Noul::new(PRICED_INSTRUCTIONS)?.criteria(PRICED_YES, PRICED_NO)?,
    })
}

fn choice_questions(answers: &[TrackedAnswer]) -> AppResult<ChoiceQuery> {
    if answers.len() < 2 {
        bail!(
            "multiple-choice triage needs at least two answers, got {}",
            answers.len()
        );
    }
    let alternatives = answers
        .iter()
        .map(|answer| (answer.text.clone(), None::<Value>))
        .chain(std::iter::once((NONE_ALTERNATIVE.to_owned(), None::<Value>)))
        .collect::<Vec<_>>();

    Ok(ChoiceQuestions {
        is_evidence: Noul::new(EVIDENCE_INSTRUCTIONS_CHOICES)?.criteria(
            EVIDENCE_YES,
            EVIDENCE_NO,
        )?,
        supported_answer: Choice::try_from_iter(
            SUPPORTED_ANSWER_INSTRUCTIONS,
            alternatives,
        )
        .context("building the answer choice question; answer texts must be unique")?,
        evidence_strength: strength_question()?,
        already_priced: Noul::new(PRICED_INSTRUCTIONS)?.criteria(PRICED_YES, PRICED_NO)?,
    })
}

const STRENGTH_LEVELS: [&str; 4] = [
    "The post mentions the topic but gives no new information.",
    "The post hints at an outcome, with no named source.",
    "The post reports a new fact from a named or checkable source.",
    "The post states a definitive outcome or an official result.",
];

fn strength_question() -> AppResult<Score<EvidenceLevel>> {
    Ok(Score::new(
        STRENGTH_INSTRUCTIONS,
        [
            (EvidenceLevel::Noise, STRENGTH_LEVELS[0]),
            (EvidenceLevel::Weak, STRENGTH_LEVELS[1]),
            (EvidenceLevel::Strong, STRENGTH_LEVELS[2]),
            (EvidenceLevel::Definitive, STRENGTH_LEVELS[3]),
        ],
    )?)
}

fn build_state<'a>(
    market: &'a TrackedMarket,
    answers: &'a [TrackedAnswer],
    post: &'a ExtractedPost,
    followers: Option<u64>,
    p_market: f64,
) -> TriageState<'a> {
    TriageState {
        market: MarketState {
            question: &market.question,
            resolution_criteria: &market.resolution_criteria,
            close_time: format_timestamp(market.close_time),
            market_probability: p_market.clamp(0.0, 1.0),
        },
        post: PostState {
            author_handle: &post.handle,
            author_followers: followers,
            posted_at: format_timestamp(Some(post.created_at)),
            text: &post.text,
        },
        answers: answers
            .iter()
            .map(|answer| AnswerState {
                text: &answer.text,
                market_probability: answer.p_market_last.clamp(0.0, 1.0),
            })
            .collect(),
    }
}

fn outcome_from_binary(answers: BinaryAnswers) -> AppResult<TriageOutcome> {
    let choice = &answers.supports_yes;
    let p_yes = choice.probabilities.get(&Polarity::Yes).copied().ok_or_else(|| {
        anyhow!("triage gave no probability for the Yes outcome; refusing to invent one")
    })?;
    Ok(TriageOutcome {
        p_evidence: answers.is_evidence.noul,
        p_target: p_yes,
        alternatives: 2,
        strength: answers.evidence_strength.score,
        p_priced: answers.already_priced.noul,
        target: Target::Binary,
        cost_usd: None,
    })
}

fn outcome_from_choice(answers: ChoiceAnswers, tracked: &[TrackedAnswer]) -> AppResult<TriageOutcome> {
    let choice = &answers.supported_answer;
    if choice.choice == NONE_ALTERNATIVE {
        return Ok(TriageOutcome {
            p_evidence: answers.is_evidence.noul,
            // The abstain mass is display-only; `assess` never forms an
            // impulse for [`Target::NoAnswer`].
            p_target: choice.probabilities.get(NONE_ALTERNATIVE).copied().unwrap_or(0.0),
            alternatives: tracked.len() + 1,
            strength: answers.evidence_strength.score,
            p_priced: answers.already_priced.noul,
            target: Target::NoAnswer,
            cost_usd: None,
        });
    }

    let answer = tracked
        .iter()
        .find(|candidate| candidate.text == choice.choice)
        .ok_or_else(|| anyhow!("triage chose an answer that is not tracked: {}", choice.choice))?;

    let p_target = choice.probabilities.get(&choice.choice).copied().unwrap_or(0.0);
    let tracked_mass: f64 = tracked
        .iter()
        .filter_map(|candidate| choice.probabilities.get(&candidate.text))
        .sum();
    if !tracked_mass.is_finite() || tracked_mass <= 0.0 {
        bail!(
            "triage gave no probability mass to tracked answers (mass on `{}` was {p_target})",
            choice.choice
        );
    }

    Ok(TriageOutcome {
        p_evidence: answers.is_evidence.noul,
        p_target: (p_target / tracked_mass).clamp(0.0, 1.0),
        alternatives: tracked.len(),
        strength: answers.evidence_strength.score,
        p_priced: answers.already_priced.noul,
        target: Target::Answer(answer.id.clone()),
        cost_usd: None,
    })
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
    let mut end = ERROR_SNIPPET_BYTES;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &text[..end])
}

/// Shared chat client for onboarding, built on rig.
///
/// Cloneable so each agent run can hold a handle; the underlying rig clients
/// share connection pools.
#[derive(Clone)]
pub struct LlmClient {
    cloudflare: Option<rig::providers::openai::OpenAI>,
    openrouter: Option<rig::providers::openai::OpenAI>,
}

impl LlmClient {
    /// Builds the client from the process environment.
    ///
    /// Initializes Cloudflare and/or OpenRouter depending on the credentials
    /// present in the process environment.
    pub fn new() -> AppResult<Self> {
        let (cloudflare, cf_err) = match rig::providers::cloudflare::from_env() {
            Ok(client) => (Some(client), None),
            Err(e) => (None, Some(e)),
        };
        let (openrouter, or_err) = match rig::providers::openrouter::from_env() {
            Ok(client) => (Some(client), None),
            Err(e) => (None, Some(e)),
        };
        if cloudflare.is_none() && openrouter.is_none() {
            bail!(
                "failed to initialize LLM client; neither Cloudflare nor OpenRouter credentials were valid \
                 (Cloudflare: {}; OpenRouter: {})",
                cf_err.map_or_else(|| "none".to_string(), |e| e.to_string()),
                or_err.map_or_else(|| "none".to_string(), |e| e.to_string()),
            );
        }
        Ok(Self {
            cloudflare,
            openrouter,
        })
    }

    /// Builds a `rig` completion model for an agent run.
    ///
    /// Onboarding uses this to hand the onboarding model to
    /// [`rig::AgentBuilder`].
    pub fn completion_model(
        &self,
        raw_model: &str,
    ) -> AppResult<rig::DynModel<rig::operation::Completion>> {
        let (provider, model) = parse_model_spec(raw_model)?;
        let model: rig::DynModel<rig::operation::Completion> = match provider {
            ModelProvider::Cloudflare => self
                .cloudflare
                .as_ref()
                .ok_or_else(|| {
                    anyhow!(
                        "Cloudflare AI Gateway client is not initialized; please set \
                         CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_API_TOKEN"
                    )
                })?
                .completion(model)
                .into(),
            ModelProvider::OpenRouter => self
                .openrouter
                .as_ref()
                .ok_or_else(|| {
                    anyhow!(
                        "OpenRouter client is not initialized; please set OPENROUTER_API_KEY"
                    )
                })?
                .completion(model)
                .into(),
            ModelProvider::OpenRouterDecisions => bail!(
                "model '{raw_model}' uses the OpenRouter decisions endpoint and cannot drive an agent"
            ),
        };
        Ok(model)
    }

    /// Builds a client with placeholder credentials for unit tests.
    ///
    /// No test calls it, so the surrounding environment is never read.
    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        Self {
            cloudflare: Some(rig::providers::cloudflare::new("test-key")),
            openrouter: Some(rig::providers::openrouter::new("test-key")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rig::typesafeai::types::Answer;

    /// Answers as the wire carries them, before rubric mapping.
    type BinaryResponse = BinaryQuestions<Answer, Answer, Answer, Answer>;

    /// Answers from a live `baseten/joey-1` call on 2026-11-06, for a post that
    /// reported an official schedule slip with a named source.
    ///
    /// The score legend must echo the exact rubric text the request asked for,
    /// so it is built from [`STRENGTH_LEVELS`] instead of written by hand.
    fn live_reply() -> Value {
        serde_json::json!({
            "is_evidence": {"type": "noul", "noul": 0.9425491724737332},
            "supports_yes": {
                "type": "choice",
                "choice": "no",
                "probabilities": {"yes": 0.056535006413222184, "no": 0.9434649935867778},
                "confidence": 0.8869299871735556
            },
            "evidence_strength": {
                "type": "score",
                "score": 2.6054387268142327,
                "legend": {
                    "0": STRENGTH_LEVELS[0],
                    "1": STRENGTH_LEVELS[1],
                    "2": STRENGTH_LEVELS[2],
                    "3": STRENGTH_LEVELS[3]
                },
                "probabilities": {
                    "0": 0.02314174039905393,
                    "1": 0.028097493550774972,
                    "2": 0.26894106488705555,
                    "3": 0.6798197011631156
                },
                "confidence": 0.6054387268142327
            },
            "already_priced": {"type": "noul", "noul": 0.32696181044458317}
        })
    }

    fn live_answers() -> BinaryResponse {
        serde_json::from_value(live_reply()).expect("live reply should decode")
    }

    fn outcome(p_evidence: f64, p_target: f64, alternatives: usize, strength: f64, p_priced: f64) -> TriageOutcome {
        TriageOutcome {
            p_evidence,
            p_target,
            alternatives,
            strength,
            p_priced,
            target: Target::Binary,
            cost_usd: None,
        }
    }

    fn policy() -> TriagePolicy {
        TriagePolicy {
            min_evidence: 0.7,
        }
    }

    fn tracked_answer(id: &str, text: &str) -> TrackedAnswer {
        TrackedAnswer {
            id: id.to_owned(),
            market_id: "m".to_owned(),
            text: text.to_owned(),
            p_internal_log_odds: 0.0,
            p_market_last: 0.5,
            belief_updated_at: 0,
            yes_shares: 0.0,
            no_shares: 0.0,
            created_at: 0,
            invested_mana: 0.0,
        }
    }

    #[test]
    fn binary_questions_serialize_with_stable_ids_and_types() {
        let value = serde_json::to_value(binary_questions().expect("questions build"))
            .expect("questions serialize");

        let object = value.as_object().expect("questions are an object");
        let mut ids = object.keys().cloned().collect::<Vec<_>>();
        ids.sort();
        assert_eq!(
            ids,
            ["already_priced", "evidence_strength", "is_evidence", "supports_yes"]
        );
        assert_eq!(object["is_evidence"]["type"], "noul");
        assert_eq!(object["supports_yes"]["type"], "choice");
        assert_eq!(object["evidence_strength"]["type"], "score");
        assert_eq!(object["already_priced"]["type"], "noul");

        assert_eq!(
            object["supports_yes"]["criteria"]
                .as_object()
                .expect("choice criteria are a map")
                .len(),
            2
        );
        assert_eq!(
            object["evidence_strength"]["criteria"]
                .as_array()
                .expect("score criteria are an array")
                .len(),
            4
        );
    }

    #[test]
    fn choice_questions_use_answer_text_as_labels() {
        let answers = [
            tracked_answer("a1", "Yes, by June"),
            tracked_answer("a2", "No, it will not happen"),
        ];
        let value = serde_json::to_value(choice_questions(&answers).expect("questions build"))
            .expect("questions serialize");

        let criteria = value["supported_answer"]["criteria"]
            .as_object()
            .expect("choice criteria are a map");
        assert!(criteria.contains_key("Yes, by June"));
        assert!(criteria.contains_key("No, it will not happen"));
    }

    #[test]
    fn choice_questions_reject_duplicate_answer_texts() {
        let answers = [
            tracked_answer("a1", "Yes"),
            tracked_answer("a2", "Yes"),
        ];
        let error = choice_questions(&answers).expect_err("duplicate labels should fail");
        assert!(error.to_string().contains("unique"), "{error}");
    }

    #[test]
    fn choice_questions_need_two_answers() {
        let answers = [tracked_answer("a1", "Yes")];
        let error = choice_questions(&answers).expect_err("one answer should fail");
        assert!(error.to_string().contains("at least two"), "{error}");
    }

    #[test]
    fn decodes_a_live_joey_reply() {
        let query = binary_questions().expect("questions build");
        let answers = query.decode(live_answers()).expect("answers should validate");

        let outcome =
            outcome_from_binary(answers).expect("a complete reply should convert");
        assert!((outcome.p_evidence - 0.9425).abs() < 1e-4);
        assert!((outcome.p_target - 0.0565).abs() < 1e-4);
        assert!((outcome.strength - 2.6054).abs() < 1e-4);
        assert!((outcome.p_priced - 0.3270).abs() < 1e-4);
        assert_eq!(outcome.alternatives, 2);
        assert_eq!(outcome.target, Target::Binary);
    }

    #[test]
    fn a_missing_yes_probability_is_an_error() {
        // Layer 1: the wire decode rejects a reply whose choice probabilities
        // do not cover every requested alternative.
        let query = binary_questions().expect("questions build");
        let mut value = live_reply();
        value["supports_yes"]["probabilities"]
            .as_object_mut()
            .expect("probabilities map")
            .remove("yes");
        let error = query
            .decode(serde_json::from_value(value).expect("reply should decode"))
            .expect_err("decode must fail when the probability map is incomplete");
        assert!(error.to_string().contains("labels differ"), "{error}");

        // Layer 2: extraction refuses to invent a number even if a malformed
        // reply reaches it as typed answers.
        let typed = BinaryAnswers {
            is_evidence: NoulAnswer { noul: 0.9 },
            supports_yes: ChoiceAnswer {
                choice: Polarity::Yes,
                probabilities: [(Polarity::No, 1.0)].into_iter().collect(),
                confidence: 0.9,
            },
            evidence_strength: ScoreAnswer {
                score: 2.0,
                probabilities: Default::default(),
                confidence: 0.5,
            },
            already_priced: NoulAnswer { noul: 0.1 },
        };
        let error = outcome_from_binary(typed).expect_err("missing mass must fail");
        assert!(error.to_string().contains("no probability for the Yes"), "{error}");
    }

    #[test]
    fn live_reply_produces_a_negative_impulse() {
        let query = binary_questions().expect("questions build");
        let outcome = outcome_from_binary(query.decode(live_answers()).expect("answers validate"))
            .expect("a complete reply should convert");

        let scale = assess(&outcome, policy()).expect("strong evidence passes the policy");
        assert!(scale < 0.0, "the post favors NO, got {scale}");
        // 0.94255 * (2*0.056535 - 1) * (1 - 0.32696) * (2.60544/3)
        assert!((scale + 0.48864).abs() < 1e-4, "got {scale}");
    }

    #[test]
    fn low_evidence_is_not_material() {
        let outcome = outcome(0.4, 0.95, 2, 3.0, 0.0);
        assert_eq!(assess(&outcome, policy()), None);
    }

    #[test]
    fn weak_evidence_is_not_material() {
        let outcome = outcome(0.95, 0.95, 2, 0.2, 0.0);
        assert_eq!(assess(&outcome, policy()), None);
    }

    #[test]
    fn a_split_direction_produces_no_impulse() {
        let outcome = outcome(0.95, 0.5, 2, 3.0, 0.0);
        assert_eq!(assess(&outcome, policy()), None);
    }

    #[test]
    fn priced_evidence_is_discounted_to_nothing() {
        let outcome = outcome(0.95, 0.95, 2, 3.0, 1.0);
        assert_eq!(assess(&outcome, policy()), None);
    }

    #[test]
    fn scale_stays_inside_the_unit_range() {
        for (p_target, strength) in [(0.0, 3.0), (1.0, 3.0), (0.0, 0.6), (1.0, 2.9)] {
            let outcome = outcome(1.0, p_target, 2, strength, 0.0);
            let scale = assess(&outcome, policy()).expect("extreme answers are material");
            assert!(scale.abs() <= 1.0, "scale {scale} out of range");
        }
    }

    #[test]
    fn multiple_choice_direction_is_excess_mass() {
        // Four alternatives: a uniform answer is no impulse, a certain one is full.
        let uniform = outcome(0.95, 0.25, 4, 3.0, 0.0);
        assert_eq!(assess(&uniform, policy()), None);

        let certain = outcome(0.95, 1.0, 4, 3.0, 0.0);
        let scale = assess(&certain, policy()).expect("a certain answer is material");
        assert!((scale - 0.95).abs() < 1e-9, "got {scale}");
    }

    #[test]
    fn non_finite_probabilities_are_not_material() {
        let outcome = outcome(f64::NAN, 0.95, 2, 3.0, 0.0);
        assert_eq!(assess(&outcome, policy()), None);
    }

    #[test]
    fn unknown_favored_answer_is_an_error() {
        let answers = ChoiceAnswers {
            is_evidence: NoulAnswer { noul: 0.9 },
            supported_answer: ChoiceAnswer {
                choice: "Not tracked".to_owned(),
                probabilities: [("Not tracked".to_owned(), 0.9)].into_iter().collect(),
                confidence: 0.9,
            },
            evidence_strength: ScoreAnswer {
                score: 2.0,
                probabilities: Default::default(),
                confidence: 0.5,
            },
            already_priced: NoulAnswer { noul: 0.1 },
        };
        let tracked = [tracked_answer("a1", "Yes"), tracked_answer("a2", "No")];
        let error = outcome_from_choice(answers, &tracked).expect_err("unknown answer should fail");
        assert!(error.to_string().contains("not tracked"), "{error}");
    }

    #[test]
    fn choice_questions_offer_the_abstain_alternative() {
        let answers = [
            tracked_answer("a1", "Yes, by June"),
            tracked_answer("a2", "No, it will not happen"),
        ];
        let value = serde_json::to_value(choice_questions(&answers).expect("questions build"))
            .expect("questions serialize");

        let criteria = value["supported_answer"]["criteria"]
            .as_object()
            .expect("choice criteria are a map");
        assert!(criteria.contains_key(NONE_ALTERNATIVE), "{criteria:?}");
        assert_eq!(criteria.len(), 3); // 2 tracked answers + the abstain option
    }

    #[test]
    fn choice_evidence_question_is_answer_relative() {
        let answers = [tracked_answer("a1", "Yes"), tracked_answer("a2", "No")];
        let value = serde_json::to_value(choice_questions(&answers).expect("questions build"))
            .expect("questions serialize");
        assert_eq!(
            value["is_evidence"]["instructions"],
            EVIDENCE_INSTRUCTIONS_CHOICES
        );

        let binary = serde_json::to_value(binary_questions().expect("questions build"))
            .expect("questions serialize");
        assert_eq!(binary["is_evidence"]["instructions"], EVIDENCE_INSTRUCTIONS);
    }

    fn choice_answers_with(choice: &str, probabilities: &[(&str, f64)]) -> ChoiceAnswers {
        ChoiceAnswers {
            is_evidence: NoulAnswer { noul: 0.95 },
            supported_answer: ChoiceAnswer {
                choice: choice.to_owned(),
                probabilities: probabilities
                    .iter()
                    .map(|(text, p)| (text.to_string(), *p))
                    .collect(),
                confidence: 0.9,
            },
            evidence_strength: ScoreAnswer {
                score: 3.0,
                probabilities: Default::default(),
                confidence: 0.9,
            },
            already_priced: NoulAnswer { noul: 0.0 },
        }
    }

    #[test]
    fn an_abstain_choice_is_never_material() {
        let tracked = [tracked_answer("a1", "Yes"), tracked_answer("a2", "No")];
        let answers = choice_answers_with(
            NONE_ALTERNATIVE,
            &[(NONE_ALTERNATIVE, 0.8), ("Yes", 0.1), ("No", 0.1)],
        );

        let outcome = outcome_from_choice(answers, &tracked).expect("abstain decodes");
        assert_eq!(outcome.target, Target::NoAnswer);
        assert_eq!(assess(&outcome, policy()), None);
    }

    #[test]
    fn abstain_mass_does_not_flip_the_direction() {
        // Track mass 0.5 split evenly, abstain 0.5: renormalized p_target is a
        // draw, so no impulse — not a maximal negative one for the chosen side.
        let tracked = [tracked_answer("a1", "Yes"), tracked_answer("a2", "No")];
        let answers = choice_answers_with(
            "Yes",
            &[("Yes", 0.25), ("No", 0.25), (NONE_ALTERNATIVE, 0.5)],
        );

        let outcome = outcome_from_choice(answers, &tracked).expect("choice decodes");
        assert!((outcome.p_target - 0.5).abs() < 1e-12, "{:?}", outcome.p_target);
        assert_eq!(assess(&outcome, policy()), None);
    }

    #[test]
    fn zero_mass_on_tracked_answers_is_an_error() {
        let tracked = [tracked_answer("a1", "Yes"), tracked_answer("a2", "No")];
        let answers = choice_answers_with(
            "Yes",
            &[(NONE_ALTERNATIVE, 1.0), ("Yes", 0.0), ("No", 0.0)],
        );

        let error = outcome_from_choice(answers, &tracked).expect_err("no mass must fail");
        assert!(error.to_string().contains("no probability mass"), "{error}");
    }

    #[test]
    fn snippet_truncates_on_a_character_boundary() {
        let text = "é".repeat(ERROR_SNIPPET_BYTES);
        let snippet = snippet(&text);
        assert!(snippet.len() <= ERROR_SNIPPET_BYTES + 3);
        assert!(snippet.ends_with('…'));
    }

    #[test]
    fn format_timestamp_handles_missing_values() {
        assert_eq!(format_timestamp(None), "unknown");
        assert_eq!(format_timestamp(Some(0)), "1970-01-01T00:00:00+00:00");
    }

    // -------------------------------------------------------------------
    // Live calls. Run with `cargo test -- --ignored --nocapture`
    // -------------------------------------------------------------------

    /// Configuration with placeholder credentials, except the live OpenRouter
    /// key, which the decisions client needs.
    fn live_config() -> Config {
        dotenvy::dotenv().ok();
        // The decisions client reads OPENROUTER_API_KEY itself; fail early.
        let _key = std::env::var("OPENROUTER_API_KEY")
            .ok()
            .filter(|key| !key.is_empty())
            .expect("OPENROUTER_API_KEY must be set for the live probe");
        Config {
            jetstream_url: "wss://jetstream2.us-east.bsky.network/subscribe".to_owned(),
            cloudflare_account_id: None,
            triage_model: "openrouter-decisions/baseten/joey-1".to_owned(),
            onboarding_model: "openrouter/anthropic/claude".to_owned(),
            firecrawl_api_key: "fc-".to_owned(),
            manifold_api_key: "m-".to_owned(),
            discord_bot_token: "d-".to_owned(),
            discord_guild_id: 1,
            discord_control_channel_id: 1,
            discord_operator_user_id: 1,
            default_market_budget: 100.0,
            kelly_fraction: 0.25,
            entry_edge_threshold: 0.05,
            exit_edge_threshold: 0.01,
            min_trade_size: 1.0,
            belief_kappa: 1.1,
            belief_halflife_hours: 4.0,
            triage_min_evidence: 0.7,
            db_path: std::path::PathBuf::from("/tmp/opencode/manif0ld-live.db"),
        }
    }

    fn live_market(question: &str, criteria: &str) -> TrackedMarket {
        TrackedMarket {
            id: "live".into(),
            slug: "live".into(),
            market_url: String::new(),
            question: question.into(),
            description: String::new(),
            resolution_criteria: criteria.into(),
            close_time: None,
            status: crate::types::MarketStatus::Active,
            budget_mana: 100.0,
            invested_mana: 0.0,
            realized_pnl_mana: 0.0,
            yes_shares: 0.0,
            no_shares: 0.0,
            p_internal_log_odds: 0.0,
            p_market_last: 0.34,
            belief_updated_at: 0,
            sieve_source: String::new(),
            discord_thread_id: String::new(),
            created_at: 0,
            deleted_at: None,
            outcome_type: "BINARY".to_owned(),
            should_answers_sum_to_one: false,
        }
    }

    fn live_post(text: &str) -> ExtractedPost {
        ExtractedPost {
            uri: "at://live/app.bsky.feed.post/1".into(),
            did: "did:plc:live".into(),
            handle: "nasawatch.bsky.social".into(),
            text: text.into(),
            created_at: 1_770_000_000,
        }
    }

    fn assert_assessment_sane(assessment: &TriageAssessment, label: &str) {
        let outcome = &assessment.outcome;
        println!(
            "{label}: evidence {:.3} target {:.3} strength {:.2} priced {:.3} scale {:?}",
            outcome.p_evidence,
            outcome.p_target,
            outcome.strength,
            outcome.p_priced,
            assessment.impulse_scale,
        );
        assert!(
            (0.0..=1.0).contains(&outcome.p_evidence),
            "p_evidence {}",
            outcome.p_evidence
        );
        assert!(
            (0.0..=1.0).contains(&outcome.p_target),
            "p_target {}",
            outcome.p_target
        );
        assert!(
            (0.0..=1.0).contains(&outcome.p_priced),
            "p_priced {}",
            outcome.p_priced
        );
        assert!(
            (0.0..=3.0).contains(&outcome.strength),
            "strength {}",
            outcome.strength
        );
    }

    #[test]
    #[ignore = "network call; needs OPENROUTER_API_KEY"]
    fn evaluates_a_live_binary_market() {
        let client = TriageClient::new(&live_config()).expect("client builds");
        let market = live_market(
            "Will the Artemis III crewed landing happen before 2028?",
            "Resolves YES if NASA lands humans on the Moon before 2028-01-01.",
        );
        let post = live_post(
            "NASA officially confirms Artemis III is slipping to 2028. \
             The associate administrator said the lander will not be ready.",
        );
        let runtime = tokio::runtime::Runtime::new().expect("runtime builds");
        let assessment = runtime.block_on(client.evaluate(&market, &[], &post, Some(48_210), 0.34));
        assert_assessment_sane(&assessment.expect("live call"), "binary");
    }

    #[test]
    #[ignore = "network call; needs OPENROUTER_API_KEY"]
    fn evaluates_a_live_multiple_choice_market() {
        let client = TriageClient::new(&live_config()).expect("client builds");
        let market = live_market(
            "When will the next Artemis crewed landing happen?",
            "Resolves to the year NASA next lands humans on the Moon.",
        );
        let answers = [
            tracked_answer("a1", "2028"),
            tracked_answer("a2", "2029"),
            tracked_answer("a3", "2030 or later"),
        ];
        let post = live_post(
            "NASA officially confirms Artemis III is slipping to 2028. \
             The associate administrator said the lander will not be ready.",
        );
        let runtime = tokio::runtime::Runtime::new().expect("runtime builds");
        let assessment = runtime.block_on(client.evaluate(&market, &answers, &post, None, 0.34));
        assert_assessment_sane(&assessment.expect("live call"), "multiple-choice");
    }
}
