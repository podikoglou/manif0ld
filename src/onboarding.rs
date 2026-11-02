//! Market onboarding: turn a Manifold URL into a tracked market.
//!
//! Onboarding has two separated phases so the Discord layer can report progress
//! and create a thread before anything becomes live:
//!
//! 1. [`OnboardingService::prepare`] fetches the market, asks the onboarding
//!    model for a Sieve rule, compiles it with retries, and resolves VIP actors.
//!    It performs no writes.
//! 2. [`OnboardingService::activate`] persists the draft, registers the compiled
//!    engine, and refreshes the market cache. Any failure after the database
//!    insert rolls the new row back.
//!
//! All Manifold and Bluesky HTTP calls use short timeouts and put the HTTP
//! status plus a bounded body excerpt into errors. The Cloudflare credential
//! lives in the rig provider and never appears in an error or log line.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;

use crate::belief;
use crate::config::Config;
use crate::db::Db;
use crate::error::{anyhow, bail, AppResult, Context};
use crate::pipeline::MarketCache;
use crate::sieve::{self, EngineRegistry};
use crate::triage::{extract_json_object, snippet, LlmClient};
use crate::types::{MarketStatus, TrackedMarket, VipEntry};

/// Manifold slug lookup endpoint; the slug is appended as a path segment.
const MANIFOLD_SLUG_URL: &str = "https://api.manifold.markets/v0/slug";

/// Public Bluesky AppView that serves actor search and profiles.
const BSKY_PUBLIC_API: &str = "https://public.api.bsky.app/xrpc";

/// Timeout for Manifold and Bluesky lookups.
const HTTP_TIMEOUT: Duration = Duration::from_secs(20);

/// Maximum number of LLM-suggested VIP queries to resolve.
const MAX_VIP_QUERIES: usize = 10;

/// Candidate actors requested per VIP search.
const VIP_SEARCH_LIMIT: u64 = 5;

/// LLM attempts allowed for one Sieve program.
const MAX_SIEVE_ATTEMPTS: usize = 3;

/// Minimum follower count that makes an actor a preferred VIP candidate.
const PREFERRED_VIP_FOLLOWERS: u64 = 500;

/// System prompt for Sieve synthesis, copied verbatim from the specification.
pub const SIEVE_SYSTEM_PROMPT: &str = r##"You are an expert filtration engineer writing a program in the Sieve DSL.
Sieve is a declarative, zero-backtracking O(N) DSL that compiles into a SIMD
Lookup Table, a Double-Array Aho-Corasick trie, and 64-bit Glushkov Shift-AND
proximity state words.

### 1. LEXICAL DEFAULTS (handled internally by the Sieve engine)
- Case-Insensitive: "launch" matches "Launch", "LAUNCH".
- Unicode Normalized: input is NFKC-normalized, zero-width joiners stripped.
- Implicit Word Boundaries: "bot" matches "a bot!", "#bot", but NOT "robot".

### 2. LITERAL MODIFIERS
- "word"    : exact token (case-insensitive)
- "word"+   : morphological expansion (-s, -es, -ed, -ing, -er)
- "word"~   : leetspeak/obfuscation normalization (@/4→a, 3→e, 1/!→i, 0→o, $/5→s, s.c.a.m→scam)
- "word"*   : prefix wildcard ("gpt5"* matches "gpt5turbo")
- *"word"   : suffix wildcard (*"coin" matches "bitcoin", "memecoin")
- "word"!   : case-sensitive exact match (use on short acronyms: "API"!, "SEC"!, "AGI"!)
- Chainable: *"coin"+~ = suffix + morphology + leetspeak

### 3. COMBINATORS
- A | B      : OR
- A B        : adjacent sequence
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
modifier    = "+" | "~" | "*" | "!" ;

### 5. DESIGN REQUIREMENTS FOR THIS USE CASE
1. Every @macro ends with ';'. The final `match Market_<id> { ... }` has no ';'.
2. Polysemy guard: never match ambiguous nouns (e.g., "apple", "orion") standalone;
   always bind to a domain context via `(@codenames &..8 @domain_context)`.
3. This rule's ONLY job is topical filtering — matching posts plausibly related to
   the market. Direction (YES vs NO) is determined by a downstream LLM, so DO NOT
   try to encode direction. Match both supportive and skeptical posts equally.
4. Always include a `- @noise` exception at the end of the match block to subtract
   crypto airdrops, memecoins, job postings, and fanfiction."##;

/// One market as returned by `GET /v0/slug/{slug}`.
///
/// Manifold returns many more fields; only the onboarding inputs are read.
/// `closeTime` is milliseconds since the Unix epoch, unlike every timestamp this
/// crate stores, which is seconds.
#[derive(Debug, Clone, Deserialize)]
pub struct ManifoldSlugMarket {
    pub id: String,
    pub question: String,
    #[serde(rename = "textDescription", default)]
    pub text_description: String,
    #[serde(rename = "resolutionCriteria", default)]
    pub resolution_criteria: Option<String>,
    #[serde(rename = "closeTime", default)]
    pub close_time_ms: Option<i64>,
    pub probability: f64,
    #[serde(rename = "outcomeType")]
    pub outcome_type: String,
    #[serde(rename = "isResolved")]
    pub is_resolved: bool,
}

/// A Bluesky actor that may become a VIP for one market.
#[derive(Debug, Clone, PartialEq)]
pub struct VipCandidate {
    pub did: String,
    pub handle: String,
    pub followers_count: Option<u64>,
}

/// The LLM's answer to the Sieve synthesis prompt.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct SieveSynthesis {
    #[serde(default)]
    pub sieve_program: String,
    #[serde(default)]
    pub vip_actor_queries: Vec<String>,
}

/// A fully resolved market that has not been persisted yet.
///
/// The Discord layer creates a thread for `market`, then calls
/// [`OnboardingService::activate`] with the thread ID. Until then no row, no
/// engine, and no cache entry exists.
#[derive(Debug, Clone)]
pub struct OnboardingDraft {
    pub market: TrackedMarket,
    pub vips: Vec<VipEntry>,
    pub sieve_source: String,
    pub warnings: Vec<String>,
}

/// Fetches, synthesizes, and activates tracked markets.
pub struct OnboardingService {
    ai: LlmClient,
    model: String,
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
    pub async fn prepare(
        &self,
        url: &str,
        budget: f64,
        initial_prob: Option<u8>,
    ) -> AppResult<OnboardingDraft> {
        if !budget.is_finite() || budget <= 0.0 {
            bail!("market budget must be a positive finite mana value, got {budget}");
        }

        let slug = parse_slug(url)?;
        let http = Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .context("building onboarding HTTP client")?;

        let market = self.fetch_market(&http, &slug).await?;
        validate_market(&market)?;

        if self.db.get_market(&market.id)?.is_some() {
            bail!("market {} ({slug}) is already tracked", market.id);
        }

        let synthesis = self.synthesize(&market).await?;

        let mut warnings = Vec::new();
        let mut queries = synthesis.vip_actor_queries;
        if queries.len() > MAX_VIP_QUERIES {
            warnings.push(format!(
                "LLM returned {} VIP queries; resolving only the first {MAX_VIP_QUERIES}",
                queries.len()
            ));
            queries.truncate(MAX_VIP_QUERIES);
        }
        let vips = self.resolve_vips(&http, &queries, &mut warnings).await;

        let sieve_source = synthesis.sieve_program;
        let tracked = build_market(&market, &slug, budget, initial_prob, sieve_source.clone());

        Ok(OnboardingDraft {
            market: tracked,
            vips,
            sieve_source,
            warnings,
        })
    }

    /// Persists a prepared draft and makes it live.
    ///
    /// The market row is inserted first. If engine registration, VIP insertion,
    /// or the cache update fails, the new row and VIPs are removed and the
    /// engine is unregistered (best effort) before the error is returned.
    pub fn activate(&self, draft: OnboardingDraft, thread_id: &str) -> AppResult<TrackedMarket> {
        let OnboardingDraft {
            mut market,
            vips,
            sieve_source,
            ..
        } = draft;

        market.discord_thread_id = thread_id.to_owned();
        self.db.insert_market(&market)?;

        let rollback = |market_id: &str| {
            let _ = self.db.delete_vips_for_market(market_id);
            let _ = self.db.delete_market(market_id);
            let _ = self.registry.unregister(market_id);
        };

        if let Err(error) = self
            .registry
            .register_source(market.id.clone(), &sieve_source)
        {
            rollback(&market.id);
            return Err(error);
        }

        for vip in &vips {
            if let Err(error) = self.db.add_vip(&market.id, vip) {
                rollback(&market.id);
                return Err(error);
            }
        }

        self.cache.upsert(market.clone(), vips);
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

    /// Asks the onboarding model for a compileable Sieve rule.
    ///
    /// A parse or compile failure is fed back into the next user prompt. After
    /// [`MAX_SIEVE_ATTEMPTS`] failures the error includes the last compiler (or
    /// parser) message and the raw LLM output so the operator can see what the
    /// model produced.
    async fn synthesize(&self, market: &ManifoldSlugMarket) -> AppResult<SieveSynthesis> {
        let mut user_prompt = build_sieve_user_prompt(market);
        let mut last_output = String::new();
        let mut last_error = String::new();

        for attempt in 1..=MAX_SIEVE_ATTEMPTS {
            let raw = self
                .ai
                .complete(&self.model, SIEVE_SYSTEM_PROMPT, &user_prompt)
                .await
                .with_context(|| {
                    format!("synthesizing a Sieve rule (attempt {attempt} of {MAX_SIEVE_ATTEMPTS})")
                })?;
            last_output = raw.clone();

            let outcome = match parse_sieve_synthesis(&raw) {
                Ok(synthesis) => match sieve::compile(&synthesis.sieve_program) {
                    Ok(_) => Ok(synthesis),
                    Err(error) => Err(error),
                },
                Err(error) => Err(error),
            };

            match outcome {
                Ok(synthesis) => return Ok(synthesis),
                Err(error) => {
                    last_error = error.to_string();
                    user_prompt.push_str(&format!(
                        "\n\nYour previous answer failed validation.\n\
                         Error: {last_error}\n\
                         Previous answer:\n{raw}\n\
                         Respond with corrected JSON only."
                    ));
                }
            }
        }

        bail!(
            "Sieve rule synthesis failed after {MAX_SIEVE_ATTEMPTS} attempts: {last_error}\n\
             Last LLM output:\n{last_output}"
        )
    }

    /// Resolves VIP queries through the public Bluesky AppView.
    ///
    /// Search and profile failures are warnings, not fatal errors: a market with
    /// no VIPs is still useful. Results are deduplicated by DID.
    async fn resolve_vips(
        &self,
        http: &Client,
        queries: &[String],
        warnings: &mut Vec<String>,
    ) -> Vec<VipEntry> {
        let mut vips = Vec::new();
        let mut seen_dids = HashSet::new();
        let mut seen_queries = HashSet::new();

        for raw_query in queries {
            let query = raw_query.trim().trim_start_matches('@').trim();
            if query.is_empty() {
                continue;
            }
            if !seen_queries.insert(query.to_ascii_lowercase()) {
                continue;
            }

            let actors = match search_actors(http, query).await {
                Ok(actors) if !actors.is_empty() => actors,
                Ok(_) => {
                    warnings.push(format!("VIP search for '{query}' returned no actors"));
                    continue;
                }
                Err(error) => {
                    warnings.push(format!("VIP search for '{query}' failed: {error}"));
                    continue;
                }
            };

            let mut candidates = Vec::with_capacity(actors.len());
            for actor in actors {
                candidates.push(VipCandidate {
                    followers_count: fetch_followers(http, &actor.handle).await,
                    did: actor.did,
                    handle: actor.handle,
                });
            }

            let exact = candidates
                .iter()
                .position(|candidate| candidate.handle.eq_ignore_ascii_case(query));
            let selected = match exact {
                Some(index) => Some(candidates[index].clone()),
                None => select_best_vip(&candidates),
            };

            if let Some(candidate) = selected {
                if seen_dids.insert(candidate.did.clone()) {
                    vips.push(VipEntry {
                        did: candidate.did,
                        handle: candidate.handle,
                    });
                }
            }
        }

        vips
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

/// Rejects markets that cannot be traded as a simple binary contract.
pub fn validate_market(market: &ManifoldSlugMarket) -> AppResult<()> {
    if market.outcome_type != "BINARY" {
        bail!(
            "market {} is not a binary market (outcomeType {})",
            market.id,
            market.outcome_type
        );
    }
    if market.is_resolved {
        bail!("market {} is already resolved", market.id);
    }
    if !(market.probability > 0.0 && market.probability < 1.0) {
        bail!(
            "market {} probability {} must be strictly between 0 and 1",
            market.id,
            market.probability
        );
    }
    Ok(())
}

/// Builds the user message for Sieve synthesis.
///
/// The response-shape instruction lives here rather than in
/// [`SIEVE_SYSTEM_PROMPT`] so the DSL reference stays verbatim.
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

    format!(
        r#"Market id: {id}
Market question: {question}
Market description: {description}
Resolution criteria: {resolution}
Market close time: {close_time}

Write one Sieve program that matches posts plausibly related to this market.
Respond as JSON only, using exactly this shape:
{{
  "sieve_program": "match Market_{id} {{ ... }}",
  "vip_actor_queries": ["@handle_or_name", ...]
}}"#,
        id = market.id,
        question = market.question,
    )
}

/// Parses a synthesis answer into its Sieve program and VIP queries.
pub fn parse_sieve_synthesis(raw: &str) -> AppResult<SieveSynthesis> {
    let value = extract_json_object(raw).context("parsing Sieve synthesis response")?;
    let synthesis: SieveSynthesis =
        serde_json::from_value(value).context("parsing Sieve synthesis JSON")?;

    if synthesis.sieve_program.trim().is_empty() {
        bail!("Sieve synthesis response contained an empty sieve_program");
    }

    Ok(synthesis)
}

/// Chooses the most useful VIP among the search results for one query.
///
/// Candidates on a custom domain or with at least
/// [`PREFERRED_VIP_FOLLOWERS`] followers are considered first. Within that
/// pool, the highest follower count wins; a custom domain breaks a tie, and
/// otherwise the first candidate is kept. If no candidate is preferred, the
/// same ordering is applied to the whole list.
pub fn select_best_vip(candidates: &[VipCandidate]) -> Option<VipCandidate> {
    let preferred: Vec<&VipCandidate> = candidates
        .iter()
        .filter(|candidate| is_preferred_vip(candidate))
        .collect();
    let pool: Vec<&VipCandidate> = if preferred.is_empty() {
        candidates.iter().collect()
    } else {
        preferred
    };

    let mut best: Option<&VipCandidate> = None;
    for candidate in pool {
        match best {
            None => best = Some(candidate),
            Some(current) => {
                let candidate_followers = candidate.followers_count.unwrap_or(0);
                let current_followers = current.followers_count.unwrap_or(0);
                let higher_followers = candidate_followers > current_followers;
                let custom_wins_tie = candidate_followers == current_followers
                    && is_custom_domain(candidate)
                    && !is_custom_domain(current);
                if higher_followers || custom_wins_tie {
                    best = Some(candidate);
                }
            }
        }
    }

    best.cloned()
}

fn is_preferred_vip(candidate: &VipCandidate) -> bool {
    is_custom_domain(candidate)
        || candidate
            .followers_count
            .is_some_and(|count| count >= PREFERRED_VIP_FOLLOWERS)
}

fn is_custom_domain(candidate: &VipCandidate) -> bool {
    !candidate
        .handle
        .to_ascii_lowercase()
        .ends_with(".bsky.social")
}

/// Builds the tracked-market row for a validated market.
fn build_market(
    market: &ManifoldSlugMarket,
    slug: &str,
    budget: f64,
    initial_prob: Option<u8>,
    sieve_source: String,
) -> TrackedMarket {
    let now = Utc::now().timestamp();
    let p_internal = match initial_prob {
        Some(probability) => belief::logit(f64::from(probability.clamp(1, 99)) / 100.0),
        None => belief::logit(market.probability),
    };

    TrackedMarket {
        id: market.id.clone(),
        slug: slug.to_owned(),
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
        p_market_last: market.probability.clamp(0.01, 0.99),
        belief_updated_at: now,
        sieve_source,
        discord_thread_id: String::new(),
        created_at: now,
    }
}

/// One actor from `app.bsky.actor.searchActors`.
#[derive(Debug, Deserialize)]
struct ActorRef {
    did: String,
    handle: String,
}

/// Response envelope of `app.bsky.actor.searchActors`.
#[derive(Debug, Deserialize)]
struct SearchActorsResponse {
    #[serde(default)]
    actors: Vec<ActorRef>,
}

/// Searches the public Bluesky AppView for actors.
async fn search_actors(http: &Client, query: &str) -> AppResult<Vec<ActorRef>> {
    let url = format!("{BSKY_PUBLIC_API}/app.bsky.actor.searchActors");
    let limit = VIP_SEARCH_LIMIT.to_string();
    let value = get_json(http, &url, &[("q", query), ("limit", &limit)]).await?;
    let response: SearchActorsResponse =
        serde_json::from_value(value).context("parsing Bluesky actor search response")?;
    Ok(response.actors)
}

/// Reads `followersCount` for one handle, treating any failure as unknown.
async fn fetch_followers(http: &Client, handle: &str) -> Option<u64> {
    let url = format!("{BSKY_PUBLIC_API}/app.bsky.actor.getProfile");
    let value = get_json(http, &url, &[("actor", handle)]).await.ok()?;
    value.get("followersCount").and_then(Value::as_u64)
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
            close_time_ms: Some(1_700_000_000_000),
            probability: 0.63,
            outcome_type: "BINARY".to_owned(),
            is_resolved: false,
        }
    }

    fn sample_vip(did: &str, handle: &str) -> VipEntry {
        VipEntry {
            did: did.to_owned(),
            handle: handle.to_owned(),
        }
    }

    fn sample_candidate(handle: &str, followers: Option<u64>) -> VipCandidate {
        VipCandidate {
            did: format!("did:plc:{handle}"),
            handle: handle.to_owned(),
            followers_count: followers,
        }
    }

    fn sample_draft(id: &str) -> OnboardingDraft {
        let market = TrackedMarket {
            id: id.to_owned(),
            slug: "will-btc-launch".to_owned(),
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
        };
        OnboardingDraft {
            market,
            vips: vec![
                sample_vip("did:plc:vip1", "vip1.test"),
                sample_vip("did:plc:vip2", "vip2.test"),
            ],
            sieve_source: GOOD_SIEVE.to_owned(),
            warnings: Vec::new(),
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
        let mut non_binary = sample_market();
        non_binary.outcome_type = "MULTIPLE_CHOICE".to_owned();
        assert!(validate_market(&non_binary).is_err());

        let mut resolved = sample_market();
        resolved.is_resolved = true;
        assert!(validate_market(&resolved).is_err());

        let mut zero = sample_market();
        zero.probability = 0.0;
        assert!(validate_market(&zero).is_err());

        let mut one = sample_market();
        one.probability = 1.0;
        assert!(validate_market(&one).is_err());
    }

    #[test]
    fn user_prompt_contains_market_context_and_response_shape() {
        let prompt = build_sieve_user_prompt(&sample_market());

        assert!(prompt.contains("Will the Bitcoin ETF launch by Friday?"));
        assert!(prompt.contains("A market about the Bitcoin ETF approval."));
        assert!(prompt.contains("Resolves YES if a spot ETF launches."));
        assert!(prompt.contains("Market close time: 2023-11-14T22:13:20+00:00"));
        assert!(prompt.contains("\"sieve_program\""));
        assert!(prompt.contains("\"vip_actor_queries\""));
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
    fn parse_synthesis_accepts_plain_fenced_and_prose_wrapped_json() {
        let json = r#"{
            "sieve_program": "match Market_test { \"bitcoin\" }",
            "vip_actor_queries": ["@alice.test"]
        }"#;

        for raw in [
            json.to_owned(),
            format!("```json\n{json}\n```"),
            format!("Here is the program:\n{json}\nDone."),
        ] {
            let synthesis = parse_sieve_synthesis(&raw).expect("synthesis JSON should parse");
            assert_eq!(
                synthesis.sieve_program,
                r#"match Market_test { "bitcoin" }"#
            );
            assert_eq!(synthesis.vip_actor_queries, vec!["@alice.test"]);
        }
    }

    #[test]
    fn parse_synthesis_rejects_missing_or_empty_program() {
        assert!(parse_sieve_synthesis(r#"{"vip_actor_queries": []}"#).is_err());
        assert!(
            parse_sieve_synthesis(r#"{"sieve_program": "   ", "vip_actor_queries": []}"#).is_err()
        );
        assert!(parse_sieve_synthesis("no json here").is_err());
    }

    #[test]
    fn known_good_sieve_program_compiles_and_known_bad_program_fails() {
        sieve::compile(GOOD_SIEVE).expect("known-good program should compile");
        assert!(sieve::compile(r#"match Market_test { "unterminated }"#).is_err());
        assert!(sieve::compile("not a Sieve program").is_err());
    }

    #[test]
    fn select_best_vip_prefers_custom_domains_and_large_accounts() {
        let candidates = vec![
            sample_candidate("new.bsky.social", Some(10)),
            sample_candidate("unrelated.bsky.social", Some(50)),
            sample_candidate("custom.example", Some(5)),
        ];
        let best = select_best_vip(&candidates).expect("candidate");
        assert_eq!(best.handle, "custom.example");

        let candidates = vec![
            sample_candidate("small.bsky.social", Some(10)),
            sample_candidate("big.bsky.social", Some(600)),
        ];
        let best = select_best_vip(&candidates).expect("candidate");
        assert_eq!(best.handle, "big.bsky.social");
    }

    #[test]
    fn select_best_vip_uses_highest_followers_then_custom_domain_then_first() {
        let highest = vec![
            sample_candidate("custom.example", Some(100)),
            sample_candidate("big.bsky.social", Some(1_000)),
        ];
        assert_eq!(
            select_best_vip(&highest).expect("candidate").handle,
            "big.bsky.social"
        );

        let tie = vec![
            sample_candidate("plain.bsky.social", Some(700)),
            sample_candidate("custom.example", Some(700)),
        ];
        assert_eq!(
            select_best_vip(&tie).expect("candidate").handle,
            "custom.example"
        );

        let all_equal = vec![
            sample_candidate("first.example", Some(10)),
            sample_candidate("second.example", Some(10)),
        ];
        assert_eq!(
            select_best_vip(&all_equal).expect("candidate").handle,
            "first.example"
        );

        assert!(select_best_vip(&[]).is_none());
    }

    #[test]
    fn activate_persists_market_vips_engine_and_cache() {
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

        let vips = db.list_vips("Market_test").expect("list VIPs");
        assert_eq!(vips.len(), 2);

        assert!(registry.contains("Market_test"));
        assert_eq!(registry.len(), 1);

        let cached = cache.get("Market_test").expect("cached market");
        assert_eq!(cached.discord_thread_id, "thread-1");
        assert!(cache.is_vip("Market_test", "did:plc:vip1"));
    }

    #[test]
    fn activating_a_duplicate_id_fails_without_touching_the_registry() {
        let db = Db::open_in_memory().expect("in-memory DB");
        let existing = sample_draft("Market_test");
        db.insert_market(&existing.market).expect("insert existing");
        db.add_vip("Market_test", &sample_vip("did:plc:old", "old.test"))
            .expect("add existing VIP");

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
        let vips = db.list_vips("Market_test").expect("list VIPs");
        assert_eq!(vips, vec![sample_vip("did:plc:old", "old.test")]);
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
        assert!(db.list_vips("Market_bad").expect("vips").is_empty());
        assert!(!registry.contains("Market_bad"));
        assert!(cache.get("Market_bad").is_none());
    }
}
