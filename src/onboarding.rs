//! Market onboarding: turn a Manifold URL into a tracked market.
//!
//! Onboarding has two separated phases so the Discord layer can report progress
//! and create a thread before anything becomes live:
//!
//! 1. [`OnboardingService::prepare`] fetches the market, runs the research agent
//!    in [`crate::onboarding_agent`] to write a compiling Sieve rule, and
//!    validates the rule. It performs no writes.
//! 2. [`OnboardingService::activate`] persists the draft, registers the compiled
//!    engine, and refreshes the market cache. Any failure after the database
//!    insert rolls the new row back.
//!
//! All Manifold HTTP calls use short timeouts and put the HTTP
//! status plus a bounded body excerpt into errors. The Cloudflare credential
//! lives in the rig provider and never appears in an error or log line.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::belief;
use crate::config::Config;
use crate::db::Db;
use crate::error::{anyhow, bail, AppResult, Context};
use crate::onboarding_agent;
use crate::pipeline::MarketCache;
use crate::sieve::EngineRegistry;
use crate::triage::{snippet, LlmClient};
use crate::types::{MarketStatus, TrackedMarket};

/// Manifold slug lookup endpoint; the slug is appended as a path segment.
const MANIFOLD_SLUG_URL: &str = "https://api.manifold.markets/v0/slug";

/// Timeout for Manifold lookups.
const HTTP_TIMEOUT: Duration = Duration::from_secs(20);

/// System prompt for Sieve synthesis, mirrored in the specification (§7).
pub const SIEVE_SYSTEM_PROMPT: &str = r##"You are an expert filtration engineer writing a program in the Sieve DSL.
Sieve is a declarative, zero-backtracking O(N) DSL that compiles into an
Aho-Corasick trie over case-folded tokens and native compiled proximity
state machines.

### 1. LEXICAL FACTS (handled internally by the Sieve engine)
- Case-Insensitive: "launch" matches "Launch", "LAUNCH".
- A token is a contiguous run of ASCII letters, digits, and underscores.
  Accented characters END a token: "após" tokenizes as "ap" + "s". This is
  why short literals are dangerous: a 2-letter token in a Romance-language
  post is almost always a fragment of a longer word.
- Implicit word boundaries: "bot" matches "a bot!", "#bot", but NOT "robot".
- Word content is matched verbatim apart from case folding. There is NO
  Unicode normalization of word content.

### 2. LITERAL MODIFIERS
- "word"    : exact token (case-insensitive)
- "word"+   : morphological expansion (-s, -es, -ed, -ing, -er)
- "word"~   : leetspeak/obfuscation normalization (@/4→a, 3→e, 1/!→i, 0→o, $/5→s, s.c.a.m→scam)
- "word"*   : prefix wildcard ("gpt5"* matches "gpt5turbo")
- *"word"   : suffix wildcard (*"coin" matches "bitcoin", "memecoin")
- Chainable: *"coin"+~ = suffix + morphology + leetspeak
- The case-sensitive `!` modifier is NOT supported. Do not use it.

### 3. COMBINATORS
- A | B      : OR
- A B        : adjacent sequence (ordered, distance 1)
- A ..N B    : ordered proximity (≤ N intervening tokens, 1≤N≤16)
- A &..N B   : unordered proximity window (1≤N≤16)
- A & B      : document co-occurrence
- A - B      : exception guard (match A unless B also matches)

### 4. EBNF
program     = { statement } ;
statement   = macro_def | rule_def ;
macro_def   = "@" IDENT "=" expr ";" ;
rule_def    = "match" [ IDENT ] "{" expr "}" ;
expr        = diff_expr ;
diff_expr   = and_expr { "-" and_expr } ;
and_expr    = alt_expr { "&" alt_expr } ;
alt_expr    = seq_expr { ( "|" | NEWLINE ) seq_expr } ;
seq_expr    = term { [ prox_op ] term } ;
prox_op     = ".." INT | "&.." INT ;
term        = literal | "@" IDENT | "(" expr ")" ;
literal     = [ "*" ] STRING { modifier } ;
modifier    = "+" | "~" | "*" ;

### 5. CONSTRUCTS THE COMPILER REJECTS
1. A quoted literal containing a space fails to compile ("Donald Trump"),
   because a pattern may not span tokens. Write phrases with adjacency:
   "donald" "trump", or use a single distinctive token ("trump").
2. A case-sensitive `!` literal containing uppercase letters ("SEC"!) fails
   to compile: patterns are scanned on the case-folded stream. Drop `!`.

### 6. WRITING RULES THAT DO NOT MISFIRE
1. `&..N` and `..N` bind TIGHTER than `|`. Parenthesize the group a window
   applies to: ("associated" "press" | "ap" | "reuters") &..8 ("call"+ | "win"+).
   Written without the parentheses, only the last alternative is constrained
   and the others match standalone.
2. Never include the anchor token in its own context set. `"midterms" &..8
   @terms` is vacuous when @terms contains "midterm"+: the same occurrence
   satisfies the window at distance 0.
3. Never rely on a 1-2 character token ("AP", "US", "FF") without a tight
   proximity window, and never attach `+` to one. Wire-service abbreviations
   need context: ("ap" | "associated" "press" | "nbc") &..6 ("call"+ |
   "projection"+ | "race" | "win"+).
4. Name people by their distinctive surname ("talarico"), or as an
   adjacency phrase ("mary" "peltola"). Common first names alone match
   the wrong people.
5. The `-` exception guard is document-wide: only subtract RARE words
   (airdrop, memecoin, fanfic), never common words like "job" or "call".
6. Prefer verbs/events with strong signalling value ("call"+, "win"+,
   "projection"+) over bare topic nouns.

### 7. DESIGN REQUIREMENTS FOR THIS USE CASE
1. Every @macro ends with ';'. The final `match Market_<id> { ... }` has no ';'.
2. Polysemy guard: never match ambiguous nouns (e.g., "apple", "orion") standalone;
   always bind to a domain context via `(@codenames &..8 @domain_context)`.
3. This rule's ONLY job is topical filtering — matching posts plausibly related to
   the market. Direction (YES vs NO) is determined by a downstream model, so DO NOT
   try to encode direction. Match both supportive and skeptical posts equally.
4. Always include a `- @noise` exception at the end of the match block to
   subtract rare noise words (airdrop/memecoin promo, giveaway spam,
   fanfiction tags)."##;

/// One market as returned by `GET /v0/slug/{slug}`.
///
/// Manifold returns many more fields; only the onboarding inputs are read.
/// `closeTime` is milliseconds since the Unix epoch, unlike every timestamp this
/// crate stores, which is seconds.
///
/// `probability` is absent for non-binary outcome types (e.g. `MULTIPLE_CHOICE`),
/// so it is deserialized as `Option<f64>`. `validate_market` rejects any market
/// whose `outcomeType` is not `BINARY` before the probability is accessed, so a
/// `None` here always accompanies an already-invalid market.
#[derive(Debug, Clone, Deserialize)]
pub struct ManifoldSlugMarket {
    pub id: String,
    pub question: String,
    /// Canonical page URL set by Manifold (e.g.
    /// `https://manifold.markets/Jack1/2026-melbourne-cup-winner`); fails open
    /// to the slug-only form when absent.
    #[serde(rename = "url", default)]
    pub url: String,
    #[serde(rename = "textDescription", default)]
    pub text_description: String,
    #[serde(rename = "resolutionCriteria", default)]
    pub resolution_criteria: Option<String>,
    #[serde(rename = "closeTime", default)]
    pub close_time_ms: Option<i64>,
    /// Present on `BINARY` markets; absent on `MULTIPLE_CHOICE`.
    #[serde(default)]
    pub probability: Option<f64>,
    #[serde(rename = "outcomeType")]
    pub outcome_type: String,
    #[serde(rename = "isResolved")]
    pub is_resolved: bool,
    #[serde(default)]
    pub answers: Option<Vec<ManifoldSlugAnswer>>,
}

/// One answer from a multiple choice market response.
#[derive(Debug, Clone, Deserialize)]
pub struct ManifoldSlugAnswer {
    pub id: String,
    pub text: String,
    pub probability: Option<f64>,
}

/// One program submitted to `submit_sieve` by the onboarding agent.
///
/// The agent sends one event per submission; the caller replaces its previous
/// report with the current one. `error` is `None` when the program compiled, so
/// a successful attempt still carries the final script.
#[derive(Debug, Clone, PartialEq)]
pub struct SieveAttempt {
    /// 1-based submission number within the agent run.
    pub attempt: usize,
    /// The complete program the agent submitted.
    pub script: String,
    /// The parser or compiler error, absent when the program compiled.
    pub error: Option<String>,
}

/// A live status update for the `/track` progress message.
///
/// Onboarding emits these as it advances, so the operator sees what is running
/// now instead of one frozen line until the whole flow finishes. The caller
/// replaces the previous message content with every update.
#[derive(Debug, Clone, PartialEq)]
pub enum OnboardUpdate {
    /// A coarse phase message shown while a slow step runs.
    Phase(String),
    /// One completed Sieve submission: the program and, on failure, its
    /// compiler error.
    Sieve(SieveAttempt),
}

/// A fully resolved market that has not been persisted yet.
///
/// The Discord layer creates a thread for `market`, then calls
/// [`OnboardingService::activate`] with the thread ID. Until then no row, no
/// engine, and no cache entry exists.
#[derive(Debug, Clone)]
pub struct OnboardingDraft {
    pub market: TrackedMarket,
    pub sieve_source: String,
    pub answers: Vec<crate::types::TrackedAnswer>,
}

/// Fetches, synthesizes, and activates tracked markets.
pub struct OnboardingService {
    ai: LlmClient,
    model: String,
    firecrawl: Arc<firecrawl::Client>,
    db: Db,
    registry: Arc<EngineRegistry>,
    cache: Arc<MarketCache>,
}

impl OnboardingService {
    /// Builds an onboarding service from process configuration.
    pub fn new(
        config: &Config,
        db: Db,
        registry: Arc<EngineRegistry>,
        cache: Arc<MarketCache>,
    ) -> AppResult<Self> {
        Ok(Self {
            ai: LlmClient::new()?,
            model: config.onboarding_model.clone(),
            firecrawl: Arc::new(
                firecrawl::Client::new(&config.firecrawl_api_key)
                    .context("building the Firecrawl client")?,
            ),
            db,
            registry,
            cache,
        })
    }

    /// Fetches a market and prepares a draft without persisting anything.
    ///
    /// `budget` must be a positive finite mana amount. `initial_prob` is an
    /// operator override in whole percent; it is clamped to `1..=99`. Without
    /// an override the internal belief starts at the market probability.
    ///
    /// Each Sieve generation is reported on `updates` as it happens: a phase
    /// message while a slow step runs, then the attempt itself (program plus
    /// validation error, or the compiled program). The caller renders each
    /// update immediately and replaces the previous one.
    pub async fn prepare(
        &self,
        url: &str,
        budget: f64,
        initial_prob: Option<u8>,
        updates: &mpsc::Sender<OnboardUpdate>,
    ) -> AppResult<OnboardingDraft> {
        if !budget.is_finite() || budget <= 0.0 {
            bail!("market budget must be a positive finite mana value, got {budget}");
        }

        let slug = parse_slug(url)?;
        let http = Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .context("building onboarding HTTP client")?;

        let _ = updates
            .send(OnboardUpdate::Phase(format!(
                "Fetching Manifold market `{slug}`…"
            )))
            .await;
        let market = self.fetch_market(&http, &slug).await?;
        validate_market(&market)?;

        if let Some(existing) = self.db.get_market(&market.id)? {
            if existing.status != MarketStatus::Closed {
                bail!("market {} ({slug}) is already tracked", market.id);
            }
        }

        let sieve_source = self.synthesize(&market, updates).await?;
        let tracked = build_market(&market, &slug, budget, initial_prob, sieve_source.clone());

        let now = Utc::now().timestamp();
        let answers: Vec<crate::types::TrackedAnswer> = market
            .answers
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .map(|ans| {
                let p = ans.probability.unwrap_or(0.5).clamp(0.01, 0.99);
                crate::types::TrackedAnswer {
                    id: ans.id.clone(),
                    market_id: market.id.clone(),
                    text: ans.text.clone(),
                    p_internal_log_odds: belief::logit(p),
                    p_market_last: p,
                    belief_updated_at: now,
                    yes_shares: 0.0,
                    no_shares: 0.0,
                    created_at: now,
                }
            })
            .collect();

        Ok(OnboardingDraft {
            market: tracked,
            sieve_source,
            answers,
        })
    }

    /// Persists a prepared draft and makes it live.
    ///
    /// The market row is inserted first. If engine registration or the cache
    /// update fails, the new row is removed and the engine is unregistered
    /// (best effort) before the error is returned.
    pub fn activate(&self, draft: OnboardingDraft, thread_id: &str) -> AppResult<TrackedMarket> {
        let OnboardingDraft {
            mut market,
            sieve_source,
            answers,
            ..
        } = draft;

        market.discord_thread_id = thread_id.to_owned();
        if let Some(existing) = self.db.get_market(&market.id)? {
            if existing.status == MarketStatus::Closed {
                let _ = self.db.delete_answers_for_market(&market.id);
                let _ = self.db.delete_market(&market.id);
            }
        }
        self.db.insert_market(&market)?;

        let rollback = |market_id: &str| {
            let _ = self.db.delete_answers_for_market(market_id);
            let _ = self.db.delete_market(market_id);
            let _ = self.registry.unregister(market_id);
        };

        for answer in &answers {
            if let Err(error) = self.db.upsert_answer(answer) {
                rollback(&market.id);
                return Err(error);
            }
        }

        if let Err(error) = self
            .registry
            .register_source(market.id.clone(), &sieve_source)
        {
            rollback(&market.id);
            return Err(error);
        }

        self.cache.upsert(market.clone());
        Ok(market)
    }

    /// Fetches one market by slug.
    async fn fetch_market(&self, http: &Client, slug: &str) -> AppResult<ManifoldSlugMarket> {
        let url = format!("{MANIFOLD_SLUG_URL}/{slug}");
        let value = get_json(http, &url, &[])
            .await
            .with_context(|| format!("fetching Manifold market {slug}"))?;
        serde_json::from_value(value)
            .with_context(|| format!("parsing Manifold market {slug} response"))
    }

    /// Runs the research agent for one market and returns its Sieve program.
    ///
    /// This delegates to [`crate::onboarding_agent::synthesize`], which owns the
    /// research loop, the Firecrawl tools, and the unbounded `submit_sieve`
    /// compile loop. The returned program has already compiled, and every
    /// submission was reported on `updates`.
    async fn synthesize(
        &self,
        market: &ManifoldSlugMarket,
        updates: &mpsc::Sender<OnboardUpdate>,
    ) -> AppResult<String> {
        let model = self.ai.completion_model(&self.model)?;
        onboarding_agent::synthesize(model, Arc::clone(&self.firecrawl), market, updates)
            .await
            .with_context(|| format!("synthesizing a Sieve rule with model {}", self.model))
    }
}

/// Extracts the last non-empty path segment from a Manifold market URL.
///
/// Query strings and fragments are ignored because the `url` crate parses them
/// separately. HTTP and HTTPS are accepted; other schemes and bare domains are
/// rejected.
pub fn parse_slug(url: &str) -> AppResult<String> {
    let parsed = url::Url::parse(url.trim())
        .with_context(|| format!("parsing Manifold market URL {url}"))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        bail!(
            "Manifold market URL must use http or https, got {}",
            parsed.scheme()
        );
    }

    parsed
        .path_segments()
        .and_then(|mut segments| segments.rfind(|segment| !segment.is_empty()))
        .map(str::to_owned)
        .ok_or_else(|| anyhow!("Manifold market URL must include a market slug, got {url}"))
}

/// Validates that a market is active and either BINARY or MULTIPLE_CHOICE with answers.
pub fn validate_market(market: &ManifoldSlugMarket) -> AppResult<()> {
    if market.outcome_type != "BINARY" && market.outcome_type != "MULTIPLE_CHOICE" {
        bail!(
            "market {} is neither BINARY nor MULTIPLE_CHOICE (outcomeType {}); \
             only BINARY and MULTIPLE_CHOICE markets can be traded",
            market.id,
            market.outcome_type
        );
    }
    if market.is_resolved {
        bail!("market {} is already resolved", market.id);
    }

    if market.outcome_type == "BINARY" {
        let probability = market.probability.ok_or_else(|| {
            anyhow!(
                "market {} is missing a probability (unexpected for a BINARY market)",
                market.id
            )
        })?;
        if !(probability > 0.0 && probability < 1.0) {
            bail!(
                "market {} probability {} must be strictly between 0 and 1",
                market.id,
                probability
            );
        }
    } else if market.outcome_type == "MULTIPLE_CHOICE" {
        let answers = market.answers.as_deref().unwrap_or(&[]);
        if answers.is_empty() {
            bail!("MULTIPLE_CHOICE market {} has no answers", market.id);
        }
    }
    Ok(())
}

/// Builds the user message for the onboarding research agent.
///
/// The agent instructions live in
/// [`crate::onboarding_agent::AGENT_INSTRUCTIONS`] so the DSL reference in
/// [`SIEVE_SYSTEM_PROMPT`] stays verbatim.
pub fn build_sieve_user_prompt(market: &ManifoldSlugMarket) -> String {
    let close_time = market
        .close_time_ms
        .and_then(DateTime::<Utc>::from_timestamp_millis)
        .map(|time| time.to_rfc3339())
        .unwrap_or_else(|| "unknown".to_owned());

    let description = if market.text_description.trim().is_empty() {
        "(none provided)"
    } else {
        market.text_description.as_str()
    };
    let resolution = market
        .resolution_criteria
        .as_deref()
        .filter(|text| !text.trim().is_empty())
        .unwrap_or(description);

    let answers_section = if let Some(answers) = &market.answers {
        if !answers.is_empty() {
            let names: Vec<String> = answers.iter().map(|a| format!("\"{}\"", a.text)).collect();
            format!("\nCandidate Answers / Options: {}\n", names.join(", "))
        } else {
            String::new()
        }
    } else {
        String::new()
    };

    format!(
        r#"Market id: {id}
Market question: {question}{answers_section}
Market description: {description}
Resolution criteria: {resolution}
Market close time: {close_time}

Research this market, then write and submit one Sieve program."#,
        id = market.id,
        question = market.question,
        answers_section = answers_section,
    )
}

/// Builds the tracked-market row for a validated market.
///
/// `market` must have already passed `validate_market`, so `probability` is
/// guaranteed to be `Some` with a value in `(0, 1)`.
fn build_market(
    market: &ManifoldSlugMarket,
    slug: &str,
    budget: f64,
    initial_prob: Option<u8>,
    sieve_source: String,
) -> TrackedMarket {
    let now = Utc::now().timestamp();
    // Safe: validate_market ensures probability is Some and in (0, 1).
    let market_prob = market.probability.unwrap_or(0.5);
    let p_internal = match initial_prob {
        Some(probability) => belief::logit(f64::from(probability.clamp(1, 99)) / 100.0),
        None => belief::logit(market_prob),
    };

    TrackedMarket {
        id: market.id.clone(),
        slug: slug.to_owned(),
        market_url: market.url.clone(),
        question: market.question.clone(),
        description: market.text_description.clone(),
        resolution_criteria: market
            .resolution_criteria
            .clone()
            .unwrap_or_else(|| market.text_description.clone()),
        close_time: market.close_time_ms.map(|millis| millis / 1000),
        status: MarketStatus::Active,
        budget_mana: budget,
        invested_mana: 0.0,
        realized_pnl_mana: 0.0,
        yes_shares: 0.0,
        no_shares: 0.0,
        p_internal_log_odds: p_internal,
        p_market_last: market_prob.clamp(0.01, 0.99),
        belief_updated_at: now,
        sieve_source,
        discord_thread_id: String::new(),
        created_at: now,
        deleted_at: None,
    }
}

/// GETs a JSON document, reporting status and a bounded body excerpt on error.
async fn get_json(http: &Client, url: &str, query: &[(&str, &str)]) -> AppResult<Value> {
    let response = http
        .get(url)
        .query(query)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;

    let status = response.status();
    let raw = response
        .text()
        .await
        .with_context(|| format!("reading response body from {url}"))?;

    if !status.is_success() {
        bail!("GET {url} failed with HTTP {status}: {}", snippet(&raw));
    }

    serde_json::from_str(&raw)
        .with_context(|| format!("parsing JSON from {url}: {}", snippet(&raw)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::MarketCache;
    use crate::sieve;

    const GOOD_SIEVE: &str = r#"
        @subject = ("bitcoin" | "btc" | *"coin"+);
        @event = ("launch" | "etf"+ | "approval"+);
        @noise = ("airdrop"+ | "giveaway"+ | "job"+);

        match Market_test {
            (@subject &..6 @event) - @noise
        }
    "#;

    fn sample_market() -> ManifoldSlugMarket {
        ManifoldSlugMarket {
            id: "Market_test".to_owned(),
            question: "Will the Bitcoin ETF launch by Friday?".to_owned(),
            text_description: "A market about the Bitcoin ETF approval.".to_owned(),
            resolution_criteria: Some("Resolves YES if a spot ETF launches.".to_owned()),
            url: "https://manifold.markets/Alice/will-the-thing-happen".to_owned(),
            close_time_ms: Some(1_700_000_000_000),
            probability: Some(0.63),
            outcome_type: "BINARY".to_owned(),
            is_resolved: false,
            answers: None,
        }
    }

    fn sample_draft(id: &str) -> OnboardingDraft {
        let market = TrackedMarket {
            id: id.to_owned(),
            slug: "will-btc-launch".to_owned(),
            market_url: "https://manifold.markets/Alice/will-btc-launch".to_owned(),
            question: "Will BTC launch?".to_owned(),
            description: "A market about BTC.".to_owned(),
            resolution_criteria: "Resolves YES if BTC launches.".to_owned(),
            close_time: Some(1_700_000_000),
            status: MarketStatus::Active,
            budget_mana: 50.0,
            invested_mana: 0.0,
            realized_pnl_mana: 0.0,
            yes_shares: 0.0,
            no_shares: 0.0,
            p_internal_log_odds: belief::logit(0.5),
            p_market_last: 0.5,
            belief_updated_at: 1,
            sieve_source: GOOD_SIEVE.to_owned(),
            discord_thread_id: String::new(),
            created_at: 1,
            deleted_at: None,
        };
        OnboardingDraft {
            market,
            sieve_source: GOOD_SIEVE.to_owned(),
            answers: Vec::new(),
        }
    }

    fn service(
        db: Db,
        registry: Arc<EngineRegistry>,
        cache: Arc<MarketCache>,
    ) -> OnboardingService {
        OnboardingService {
            ai: LlmClient::for_test(),
            model: "test-model".to_owned(),
            firecrawl: Arc::new(firecrawl::Client::new("fc-test").expect("test Firecrawl client")),
            db,
            registry,
            cache,
        }
    }

    #[test]
    fn parse_slug_accepts_market_urls_and_ignores_query() {
        assert_eq!(
            parse_slug("https://manifold.markets/Alice/will-it-rain").expect("normal URL"),
            "will-it-rain"
        );
        assert_eq!(
            parse_slug("https://manifold.markets/Alice/will-it-rain/").expect("trailing slash"),
            "will-it-rain"
        );
        assert_eq!(
            parse_slug("http://manifold.markets/Alice/will-it-rain?ref=x#top")
                .expect("query and fragment"),
            "will-it-rain"
        );
    }

    #[test]
    fn parse_slug_rejects_bare_domains_and_other_schemes() {
        assert!(parse_slug("https://manifold.markets").is_err());
        assert!(parse_slug("https://manifold.markets/").is_err());
        assert!(parse_slug("ftp://manifold.markets/Alice/market").is_err());
        assert!(parse_slug("not a url").is_err());
    }

    #[test]
    fn validate_market_accepts_tradable_binary_markets() {
        validate_market(&sample_market()).expect("valid binary market");
    }

    #[test]
    fn validate_market_rejects_non_binary_resolved_and_extreme_probabilities() {
        let mut non_binary_no_answers = sample_market();
        non_binary_no_answers.outcome_type = "MULTIPLE_CHOICE".to_owned();
        non_binary_no_answers.answers = None;
        assert!(validate_market(&non_binary_no_answers).is_err());

        let mut other_type = sample_market();
        other_type.outcome_type = "NUMERIC".to_owned();
        assert!(validate_market(&other_type).is_err());

        let mut multi_valid = sample_market();
        multi_valid.outcome_type = "MULTIPLE_CHOICE".to_owned();
        multi_valid.answers = Some(vec![ManifoldSlugAnswer {
            id: "ans-1".into(),
            text: "Option A".into(),
            probability: Some(0.4),
        }]);
        assert!(validate_market(&multi_valid).is_ok());

        let mut resolved = sample_market();
        resolved.is_resolved = true;
        assert!(validate_market(&resolved).is_err());

        let mut zero = sample_market();
        zero.probability = Some(0.0);
        assert!(validate_market(&zero).is_err());

        let mut one = sample_market();
        one.probability = Some(1.0);
        assert!(validate_market(&one).is_err());

        // A market missing probability with a BINARY outcomeType is caught by the probability None check.
        let mut no_prob = sample_market();
        no_prob.probability = None;
        assert!(validate_market(&no_prob).is_err());
    }

    #[test]
    fn user_prompt_contains_market_context_and_research_instruction() {
        let prompt = build_sieve_user_prompt(&sample_market());

        assert!(prompt.contains("Will the Bitcoin ETF launch by Friday?"));
        assert!(prompt.contains("A market about the Bitcoin ETF approval."));
        assert!(prompt.contains("Resolves YES if a spot ETF launches."));
        assert!(prompt.contains("Market close time: 2023-11-14T22:13:20+00:00"));
        assert!(prompt.contains("Research this market, then write and submit one Sieve program."));
        assert!(!prompt.contains("vip"));
    }

    #[test]
    fn user_prompt_falls_back_to_description_when_criteria_are_missing() {
        let mut market = sample_market();
        market.resolution_criteria = None;
        let prompt = build_sieve_user_prompt(&market);

        let resolution_lines: Vec<&str> = prompt
            .lines()
            .filter(|line| line.starts_with("Resolution criteria:"))
            .collect();
        assert_eq!(
            resolution_lines,
            vec!["Resolution criteria: A market about the Bitcoin ETF approval."]
        );
    }

    #[test]
    fn known_good_sieve_program_compiles_and_known_bad_program_fails() {
        sieve::compile(GOOD_SIEVE).expect("known-good program should compile");
        assert!(sieve::compile(r#"match Market_test { "unterminated }"#).is_err());
        assert!(sieve::compile("not a Sieve program").is_err());
    }

    #[test]
    fn activate_persists_market_engine_and_cache() {
        let db = Db::open_in_memory().expect("in-memory DB");
        let registry = Arc::new(EngineRegistry::new());
        let cache = Arc::new(MarketCache::load(&db).expect("load cache"));
        let service = service(db.clone(), Arc::clone(&registry), Arc::clone(&cache));

        let market = service
            .activate(sample_draft("Market_test"), "thread-1")
            .expect("activate draft");

        assert_eq!(market.discord_thread_id, "thread-1");

        let stored = db
            .get_market("Market_test")
            .expect("read market")
            .expect("market row");
        assert_eq!(stored.discord_thread_id, "thread-1");
        assert_eq!(stored.sieve_source, GOOD_SIEVE);

        assert!(registry.contains("Market_test"));
        assert_eq!(registry.len(), 1);

        let cached = cache.get("Market_test").expect("cached market");
        assert_eq!(cached.discord_thread_id, "thread-1");
    }

    #[test]
    fn activating_a_duplicate_id_fails_without_touching_the_registry() {
        let db = Db::open_in_memory().expect("in-memory DB");
        let existing = sample_draft("Market_test");
        db.insert_market(&existing.market).expect("insert existing");

        let registry = Arc::new(EngineRegistry::new());
        let cache = Arc::new(MarketCache::load(&db).expect("load cache"));
        let service = service(db.clone(), Arc::clone(&registry), Arc::clone(&cache));

        let error = service
            .activate(sample_draft("Market_test"), "thread-2")
            .expect_err("duplicate id must fail");
        assert!(error.to_string().contains("Market_test"), "{error}");

        assert!(!registry.contains("Market_test"));
        let stored = db
            .get_market("Market_test")
            .expect("read market")
            .expect("original row");
        assert_eq!(stored.discord_thread_id, "");
    }

    #[test]
    fn activate_replaces_previously_closed_market() {
        let db = Db::open_in_memory().expect("in-memory DB");
        let mut existing = sample_draft("Market_test");
        existing.market.status = MarketStatus::Closed;
        db.insert_market(&existing.market).expect("insert existing");

        let registry = Arc::new(EngineRegistry::new());
        let cache = Arc::new(MarketCache::load(&db).expect("load cache"));
        let service = service(db.clone(), Arc::clone(&registry), Arc::clone(&cache));

        let activated = service
            .activate(sample_draft("Market_test"), "thread-reopened")
            .expect("re-activating closed market should succeed");
        assert_eq!(activated.discord_thread_id, "thread-reopened");
        assert_eq!(activated.status, MarketStatus::Active);

        let stored = db
            .get_market("Market_test")
            .expect("read market")
            .expect("market row");
        assert_eq!(stored.discord_thread_id, "thread-reopened");
        assert_eq!(stored.status, MarketStatus::Active);
        assert!(registry.contains("Market_test"));
    }

    #[test]
    fn activate_replaces_a_previously_untracked_market() {
        let db = Db::open_in_memory().expect("in-memory DB");
        let existing = sample_draft("Market_test");
        db.insert_market(&existing.market).expect("insert existing");
        db.soft_delete_market(&existing.market.id, 1_700_000_900)
            .expect("soft delete existing");
        assert!(db.get_market("Market_test").expect("read").is_some());

        let registry = Arc::new(EngineRegistry::new());
        let cache = Arc::new(MarketCache::load(&db).expect("load cache"));
        let service = service(db.clone(), Arc::clone(&registry), Arc::clone(&cache));

        let activated = service
            .activate(sample_draft("Market_test"), "thread-retracked")
            .expect("re-tracking an untracked market should succeed");
        assert_eq!(activated.discord_thread_id, "thread-retracked");
        assert_eq!(activated.status, MarketStatus::Active);

        let stored = db
            .get_market("Market_test")
            .expect("read market")
            .expect("market row");
        assert_eq!(stored.status, MarketStatus::Active);
        assert_eq!(stored.deleted_at, None, "the soft-delete marker is cleared");
        assert!(registry.contains("Market_test"));
    }

    #[test]
    fn activate_rolls_back_the_market_when_engine_registration_fails() {
        let db = Db::open_in_memory().expect("in-memory DB");
        let registry = Arc::new(EngineRegistry::new());
        let cache = Arc::new(MarketCache::load(&db).expect("load cache"));
        let service = service(db.clone(), Arc::clone(&registry), Arc::clone(&cache));

        let mut draft = sample_draft("Market_bad");
        draft.sieve_source = "not a Sieve program".to_owned();
        draft.market.sieve_source = draft.sieve_source.clone();

        assert!(service.activate(draft, "thread-3").is_err());
        assert!(db.get_market("Market_bad").expect("read").is_none());
        assert!(!registry.contains("Market_bad"));
        assert!(cache.get("Market_bad").is_none());
    }
}
