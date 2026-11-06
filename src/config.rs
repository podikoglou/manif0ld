//! Process configuration loaded from environment variables.
//!
//! `Config` is the single source of truth for runtime settings. Values are read
//! once at startup; downstream modules receive `&Config` rather than reading the
//! environment themselves. Secret values are never included in error messages.

use std::path::PathBuf;

use crate::error::{AppResult, Context, anyhow, bail};

const DEFAULT_JETSTREAM_URL: &str = "wss://jetstream2.us-east.bsky.network/subscribe";
const DEFAULT_DB_PATH: &str = "./data/manif0ld.db";
const DEFAULT_MARKET_BUDGET: f64 = 100.0;
const DEFAULT_KELLY_FRACTION: f64 = 0.25;
const DEFAULT_ENTRY_EDGE_THRESHOLD: f64 = 0.05;
const DEFAULT_EXIT_EDGE_THRESHOLD: f64 = 0.01;
const DEFAULT_MIN_TRADE_SIZE: f64 = 1.0;
const DEFAULT_BELIEF_KAPPA: f64 = 1.1;
const DEFAULT_BELIEF_HALFLIFE_HOURS: f64 = 4.0;
const DEFAULT_TRIAGE_MIN_EVIDENCE: f64 = 0.7;
/// Dry run is the safe default until the canary phase is approved.
const DEFAULT_TRADING_DRY_RUN: bool = true;
const DEFAULT_MC_MAX_POSITIONS: usize = 6;
const DEFAULT_MC_MAX_NEW_PER_TICK: usize = 2;
const DEFAULT_MC_MAX_ANSWER_FRACTION: f64 = 0.25;
/// Manifold allows 500 requests/minute per IP; the bucket starts at that.
const DEFAULT_RATE_LIMIT_PER_MIN: usize = 500;

/// Runtime configuration for the bot.
///
/// Construct with [`Config::from_env`] in production. The `Debug` impl prints
/// every field verbatim, so never log a `Config` value: several fields hold
/// credentials.
#[derive(Debug, Clone)]
pub struct Config {
    pub jetstream_url: String,
    pub cloudflare_account_id: Option<String>,
    pub triage_model: String,
    pub onboarding_model: String,
    /// Firecrawl credential for the onboarding research agent.
    pub firecrawl_api_key: String,
    pub manifold_api_key: String,
    pub discord_bot_token: String,
    pub discord_guild_id: u64,
    pub discord_control_channel_id: u64,
    pub discord_operator_user_id: u64,
    pub default_market_budget: f64,
    pub kelly_fraction: f64,
    pub entry_edge_threshold: f64,
    pub exit_edge_threshold: f64,
    pub min_trade_size: f64,
    pub belief_kappa: f64,
    pub belief_halflife_hours: f64,
    /// Probability of evidence a post must reach before it moves belief.
    pub triage_min_evidence: f64,
    /// When true, planned bets are sent with `dryRun: true` and nothing is
    /// persisted. Defaults to true until the canary phase is approved.
    pub trading_dry_run: bool,
    /// Maximum answers of one multiple-choice market that may hold shares.
    pub mc_max_positions: usize,
    /// Maximum new answer entries in one evaluation.
    pub mc_max_new_per_tick: usize,
    /// Maximum share of a market budget committed to one answer.
    pub mc_max_answer_fraction: f64,
    /// Manifold REST requests per minute shared by every consumer, refilled
    /// continuously; trade writes have priority over housekeeping.
    pub rate_limit_per_min: usize,
    pub db_path: PathBuf,
}

impl Config {
    /// Loads configuration from the process environment.
    ///
    /// Also loads a `.env` file when present. A missing or unreadable `.env` is
    /// not an error: the process environment may be the source of truth.
    pub fn from_env() -> AppResult<Self> {
        dotenvy::dotenv().ok();
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    /// Builds a configuration from an arbitrary key lookup.
    ///
    /// This is the testable core of [`Config::from_env`]; tests pass a closure
    /// over a map instead of mutating the process environment.
    fn from_lookup(get: impl Fn(&str) -> Option<String>) -> AppResult<Self> {
        let jetstream_url =
            optional(&get, "JETSTREAM_URL").unwrap_or_else(|| DEFAULT_JETSTREAM_URL.to_owned());

        // The rig cloudflare provider reads `CLOUDFLARE_API_TOKEN` and
        // `CLOUDFLARE_AI_GATEWAY_ID` from the process environment itself.
        // The rig openrouter provider reads `OPENROUTER_API_KEY`.
        let triage_model = required(&get, "TRIAGE_MODEL")?;
        let (triage_provider, _) = parse_model_spec(&triage_model)?;
        let onboarding_model = required(&get, "ONBOARDING_MODEL")?;
        let (onboarding_provider, _) = parse_model_spec(&onboarding_model)?;
        if onboarding_provider == ModelProvider::OpenRouterDecisions {
            bail!(
                "ONBOARDING_MODEL must be a chat model; '{onboarding_model}' uses the decisions API"
            );
        }

        let needs_cloudflare = triage_provider == ModelProvider::Cloudflare
            || onboarding_provider == ModelProvider::Cloudflare;
        let needs_openrouter = matches!(
            triage_provider,
            ModelProvider::OpenRouter | ModelProvider::OpenRouterDecisions
        ) || onboarding_provider == ModelProvider::OpenRouter;

        let cloudflare_account_id = if needs_cloudflare {
            let account_id = required(&get, "CLOUDFLARE_ACCOUNT_ID")?;
            validate_cloudflare_account_id(&account_id)?;
            Some(account_id)
        } else {
            match optional(&get, "CLOUDFLARE_ACCOUNT_ID") {
                Some(id) if id != "{account_id}" => {
                    validate_cloudflare_account_id(&id)?;
                    Some(id)
                }
                _ => None,
            }
        };

        if needs_openrouter {
            let _ = required(&get, "OPENROUTER_API_KEY")?;
        }

        let manifold_api_key = required(&get, "MANIFOLD_API_KEY")?;
        let firecrawl_api_key = required(&get, "FIRECRAWL_API_KEY")?;
        let discord_bot_token = required(&get, "DISCORD_BOT_TOKEN")?;
        let discord_guild_id = required_u64(&get, "DISCORD_GUILD_ID")?;
        let discord_control_channel_id = required_u64(&get, "DISCORD_CONTROL_CHANNEL_ID")?;
        let discord_operator_user_id = required_u64(&get, "DISCORD_OPERATOR_USER_ID")?;

        let default_market_budget = f64_or(&get, "DEFAULT_MARKET_BUDGET", DEFAULT_MARKET_BUDGET)?;
        let kelly_fraction = f64_or(&get, "KELLY_FRACTION", DEFAULT_KELLY_FRACTION)?;
        let entry_edge_threshold =
            f64_or(&get, "ENTRY_EDGE_THRESHOLD", DEFAULT_ENTRY_EDGE_THRESHOLD)?;
        let exit_edge_threshold = f64_or(&get, "EXIT_EDGE_THRESHOLD", DEFAULT_EXIT_EDGE_THRESHOLD)?;
        let min_trade_size = f64_or(&get, "MIN_TRADE_SIZE", DEFAULT_MIN_TRADE_SIZE)?;
        let belief_kappa = f64_or(&get, "BELIEF_KAPPA", DEFAULT_BELIEF_KAPPA)?;
        let belief_halflife_hours =
            f64_or(&get, "BELIEF_HALFLIFE_HOURS", DEFAULT_BELIEF_HALFLIFE_HOURS)?;
        let triage_min_evidence = f64_or(&get, "TRIAGE_MIN_EVIDENCE", DEFAULT_TRIAGE_MIN_EVIDENCE)?;
        let trading_dry_run = bool_or(&get, "TRADING_DRY_RUN", DEFAULT_TRADING_DRY_RUN)?;
        let mc_max_positions = usize_or(&get, "MC_MAX_POSITIONS", DEFAULT_MC_MAX_POSITIONS)?;
        let mc_max_new_per_tick =
            usize_or(&get, "MC_MAX_NEW_PER_TICK", DEFAULT_MC_MAX_NEW_PER_TICK)?;
        let mc_max_answer_fraction = f64_or(
            &get,
            "MC_MAX_ANSWER_FRACTION",
            DEFAULT_MC_MAX_ANSWER_FRACTION,
        )?;
        if !(mc_max_answer_fraction > 0.0 && mc_max_answer_fraction <= 1.0) {
            bail!("MC_MAX_ANSWER_FRACTION must be in (0, 1], got {mc_max_answer_fraction}");
        }

        let rate_limit_per_min = usize_or(&get, "RATE_LIMIT_PER_MIN", DEFAULT_RATE_LIMIT_PER_MIN)?;
        if rate_limit_per_min == 0 {
            bail!("RATE_LIMIT_PER_MIN must be a positive number of requests per minute");
        }

        let db_path = optional(&get, "DB_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_DB_PATH));

        let config = Self {
            jetstream_url,
            cloudflare_account_id,
            triage_model,
            onboarding_model,
            firecrawl_api_key,
            manifold_api_key,
            discord_bot_token,
            discord_guild_id,
            discord_control_channel_id,
            discord_operator_user_id,
            default_market_budget,
            kelly_fraction,
            entry_edge_threshold,
            exit_edge_threshold,
            min_trade_size,
            belief_kappa,
            belief_halflife_hours,
            triage_min_evidence,
            trading_dry_run,
            mc_max_positions,
            mc_max_new_per_tick,
            mc_max_answer_fraction,
            rate_limit_per_min,
            db_path,
        };

        Ok(config)
    }
}

/// Provider selected by a model name prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelProvider {
    Cloudflare,
    OpenRouter,
    /// OpenRouter's decisions endpoint, which serves System One models.
    OpenRouterDecisions,
}

impl ModelProvider {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cloudflare => "cloudflare",
            Self::OpenRouter => "openrouter",
            Self::OpenRouterDecisions => "openrouter-decisions",
        }
    }
}

/// Matches an ASCII prefix without splitting a multi-byte character.
fn strip_prefix_ci<'a>(text: &'a str, prefix: &str) -> Option<&'a str> {
    let head = text.get(..prefix.len())?;
    head.eq_ignore_ascii_case(prefix)
        .then(|| &text[prefix.len()..])
}

/// Parses a model name prefix to determine the provider and underlying model name.
///
/// Supported prefixes:
/// - `cloudflare/` -> Cloudflare AI Gateway
/// - `openrouter/` -> OpenRouter
/// - `openrouter-decisions/` -> OpenRouter's decisions endpoint
///
/// Models without a recognized provider prefix are rejected.
pub fn parse_model_spec(model: &str) -> AppResult<(ModelProvider, &str)> {
    let trimmed = model.trim();
    if trimmed.is_empty() {
        bail!("model name cannot be empty");
    }

    for (prefix, provider) in [
        ("openrouter-decisions/", ModelProvider::OpenRouterDecisions),
        ("openrouter/", ModelProvider::OpenRouter),
        ("cloudflare/", ModelProvider::Cloudflare),
    ] {
        if let Some(rest) = strip_prefix_ci(trimmed, prefix) {
            let rest = rest.trim();
            if rest.is_empty() {
                bail!("model name after '{prefix}' prefix cannot be empty");
            }
            return Ok((provider, rest));
        }
    }

    bail!(
        "model '{trimmed}' must begin with 'cloudflare/', 'openrouter/', or \
         'openrouter-decisions/' prefix"
    )
}

/// Rejects an account id no URL can carry.
///
/// `CLOUDFLARE_ACCOUNT_ID` is interpolated into the endpoint URL by the rig
/// cloudflare provider, which already refuses anything that could not live in
/// a path segment. The extra guard catches the literal `{account_id}`
/// placeholder from `.env.example` (previously `CF_AI_GATEWAY_URL` carried it)
/// so a setup error fails at config load rather than as a confusing 404 on the
/// first LLM call.
fn validate_cloudflare_account_id(account_id: &str) -> AppResult<()> {
    if account_id.starts_with('{')
        || account_id.contains('/')
        || account_id.chars().any(char::is_whitespace)
    {
        bail!(
            "CLOUDFLARE_ACCOUNT_ID must be a non-empty account id with no `/`, braces, or \
             whitespace; replace the `{{account_id}}` placeholder in .env.example"
        );
    }
    Ok(())
}

/// Returns a trimmed, non-empty value, or `None` when the variable is absent or
/// blank. Blank values are treated as absent so that a default applies.
fn optional(get: &impl Fn(&str) -> Option<String>, key: &str) -> Option<String> {
    get(key)
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

/// Reads a required variable, reporting only its name on failure.
fn required(get: &impl Fn(&str) -> Option<String>, key: &str) -> AppResult<String> {
    match optional(get, key) {
        Some(value) => Ok(value),
        None => Err(anyhow!(
            "missing required environment variable {key}; see .env.example"
        )),
    }
}

/// Reads a required unsigned integer, used for Discord snowflake IDs.
fn required_u64(get: &impl Fn(&str) -> Option<String>, key: &str) -> AppResult<u64> {
    let raw = required(get, key)?;
    raw.parse::<u64>()
        .with_context(|| format!("{key} must be an unsigned 64-bit integer"))
}

/// Reads an optional floating-point variable, falling back to `default`.
fn f64_or(get: &impl Fn(&str) -> Option<String>, key: &str, default: f64) -> AppResult<f64> {
    match optional(get, key) {
        None => Ok(default),
        Some(raw) => raw
            .parse::<f64>()
            .with_context(|| format!("{key} must be a number")),
    }
}

/// Reads an optional boolean variable, falling back to `default`.
///
/// Accepts `true`/`false` in any case and the digits `1`/`0`.
fn bool_or(get: &impl Fn(&str) -> Option<String>, key: &str, default: bool) -> AppResult<bool> {
    match optional(get, key) {
        None => Ok(default),
        Some(raw) => match raw.to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Ok(true),
            "false" | "0" | "no" | "off" => Ok(false),
            _ => Err(anyhow!("{key} must be true or false")),
        },
    }
}

/// Reads an optional non-negative integer variable, falling back to `default`.
fn usize_or(get: &impl Fn(&str) -> Option<String>, key: &str, default: usize) -> AppResult<usize> {
    match optional(get, key) {
        None => Ok(default),
        Some(raw) => raw
            .parse::<usize>()
            .with_context(|| format!("{key} must be a non-negative integer")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Required variables with placeholder values; numeric and path variables
    /// are intentionally omitted so defaults can be asserted.
    fn required_vars() -> Vec<(&'static str, &'static str)> {
        vec![
            ("CLOUDFLARE_ACCOUNT_ID", "test-account"),
            ("TRIAGE_MODEL", "cloudflare/@cf/google/gemini-test"),
            ("ONBOARDING_MODEL", "cloudflare/@cf/deepseek/test"),
            ("FIRECRAWL_API_KEY", "fc-test"),
            ("MANIFOLD_API_KEY", "manifold-key"),
            ("DISCORD_BOT_TOKEN", "discord-token"),
            ("DISCORD_GUILD_ID", "123456789012345678"),
            ("DISCORD_CONTROL_CHANNEL_ID", "223456789012345678"),
            ("DISCORD_OPERATOR_USER_ID", "323456789012345678"),
        ]
    }

    fn config_from(vars: &[(&str, &str)]) -> AppResult<Config> {
        Config::from_lookup(|key| {
            vars.iter()
                .find(|(name, _)| *name == key)
                .map(|(_, value)| (*value).to_owned())
        })
    }

    #[test]
    fn applies_defaults_for_optional_values() {
        let config = config_from(&required_vars()).expect("config should load");

        assert_eq!(config.jetstream_url, DEFAULT_JETSTREAM_URL);
        assert_eq!(config.default_market_budget, 100.0);
        assert_eq!(config.kelly_fraction, 0.25);
        assert_eq!(config.entry_edge_threshold, 0.05);
        assert_eq!(config.exit_edge_threshold, 0.01);
        assert_eq!(config.min_trade_size, 1.0);
        assert_eq!(config.belief_kappa, 1.1);
        assert_eq!(config.belief_halflife_hours, 4.0);
        assert_eq!(config.triage_min_evidence, 0.7);
        assert!(config.trading_dry_run, "dry run must default to true");
        assert_eq!(config.mc_max_positions, 6);
        assert_eq!(config.mc_max_new_per_tick, 2);
        assert_eq!(config.mc_max_answer_fraction, 0.25);
        assert_eq!(config.rate_limit_per_min, DEFAULT_RATE_LIMIT_PER_MIN);
        assert_eq!(config.db_path, PathBuf::from("./data/manif0ld.db"));
    }

    #[test]
    fn parses_explicit_values() {
        let mut vars = required_vars();
        vars.extend([
            ("JETSTREAM_URL", "wss://jetstream.example/subscribe"),
            ("DEFAULT_MARKET_BUDGET", "42.5"),
            ("KELLY_FRACTION", "0.5"),
            ("ENTRY_EDGE_THRESHOLD", "0.1"),
            ("EXIT_EDGE_THRESHOLD", "0.02"),
            ("MIN_TRADE_SIZE", "2.5"),
            ("BELIEF_KAPPA", "2.0"),
            ("BELIEF_HALFLIFE_HOURS", "6.5"),
            ("TRADING_DRY_RUN", "false"),
            ("MC_MAX_POSITIONS", "3"),
            ("MC_MAX_NEW_PER_TICK", "1"),
            ("MC_MAX_ANSWER_FRACTION", "0.5"),
            ("RATE_LIMIT_PER_MIN", "400"),
            ("DB_PATH", "/tmp/test.db"),
        ]);

        let config = config_from(&vars).expect("config should load");

        assert_eq!(config.jetstream_url, "wss://jetstream.example/subscribe");
        assert_eq!(config.discord_guild_id, 123_456_789_012_345_678);
        assert_eq!(config.discord_control_channel_id, 223_456_789_012_345_678);
        assert_eq!(config.discord_operator_user_id, 323_456_789_012_345_678);
        assert_eq!(config.default_market_budget, 42.5);
        assert_eq!(config.kelly_fraction, 0.5);
        assert_eq!(config.entry_edge_threshold, 0.1);
        assert_eq!(config.exit_edge_threshold, 0.02);
        assert_eq!(config.min_trade_size, 2.5);
        assert_eq!(config.belief_kappa, 2.0);
        assert_eq!(config.belief_halflife_hours, 6.5);
        assert!(!config.trading_dry_run);
        assert_eq!(config.mc_max_positions, 3);
        assert_eq!(config.mc_max_new_per_tick, 1);
        assert_eq!(config.mc_max_answer_fraction, 0.5);
        assert_eq!(config.rate_limit_per_min, 400);
        assert_eq!(config.db_path, PathBuf::from("/tmp/test.db"));
    }

    #[test]
    fn reports_missing_required_variable_by_name() {
        let mut vars = required_vars();
        vars.retain(|(name, _)| *name != "MANIFOLD_API_KEY");

        let error = config_from(&vars).expect_err("missing secret should fail");
        let message = error.to_string();

        assert!(message.contains("MANIFOLD_API_KEY"), "{message}");
    }

    #[test]
    fn treats_blank_required_value_as_missing() {
        let mut vars = required_vars();
        vars.retain(|(name, _)| *name != "DISCORD_BOT_TOKEN");
        vars.push(("DISCORD_BOT_TOKEN", "   "));

        let error = config_from(&vars).expect_err("blank secret should fail");
        assert!(error.to_string().contains("DISCORD_BOT_TOKEN"));
    }

    #[test]
    fn rejects_malformed_discord_id() {
        let mut vars = required_vars();
        vars.retain(|(name, _)| *name != "DISCORD_GUILD_ID");
        vars.push(("DISCORD_GUILD_ID", "not-a-snowflake"));

        let error = config_from(&vars).expect_err("malformed ID should fail");
        let message = error.to_string();
        assert!(message.contains("DISCORD_GUILD_ID"), "{message}");
        // The offending value must not leak into the error text.
        assert!(!message.contains("not-a-snowflake"), "{message}");
    }

    #[test]
    fn rejects_malformed_numeric_value() {
        let mut vars = required_vars();
        vars.push(("KELLY_FRACTION", "a quarter"));

        let error = config_from(&vars).expect_err("malformed number should fail");
        let message = error.to_string();
        assert!(message.contains("KELLY_FRACTION"), "{message}");
        assert!(!message.contains("a quarter"), "{message}");
    }

    #[test]
    fn rejects_a_zero_rate_limit() {
        let mut vars = required_vars();
        vars.push(("RATE_LIMIT_PER_MIN", "0"));

        let error = config_from(&vars).expect_err("zero budget should fail");
        assert!(error.to_string().contains("RATE_LIMIT_PER_MIN"), "{error}");
    }

    #[test]
    fn rejects_a_malformed_dry_run_flag() {
        let mut vars = required_vars();
        vars.push(("TRADING_DRY_RUN", "sometimes"));

        let error = config_from(&vars).expect_err("malformed flag should fail");
        assert!(error.to_string().contains("TRADING_DRY_RUN"));
    }

    #[test]
    fn rejects_an_answer_fraction_outside_the_unit_interval() {
        for value in ["0", "-0.1", "1.5"] {
            let mut vars = required_vars();
            vars.push(("MC_MAX_ANSWER_FRACTION", value));

            let error = config_from(&vars).expect_err("out-of-range fraction should fail");
            assert!(
                error.to_string().contains("MC_MAX_ANSWER_FRACTION"),
                "{error}"
            );
        }
    }

    #[test]
    fn stores_cloudflare_account_id_and_models() {
        let config = config_from(&required_vars()).expect("config should load");

        assert_eq!(
            config.cloudflare_account_id.as_deref(),
            Some("test-account")
        );
        assert_eq!(config.triage_model, "cloudflare/@cf/google/gemini-test");
        assert_eq!(config.onboarding_model, "cloudflare/@cf/deepseek/test");
    }

    #[test]
    fn parses_openrouter_configuration() {
        let mut vars = required_vars();
        vars.retain(|(name, _)| {
            *name != "CLOUDFLARE_ACCOUNT_ID"
                && *name != "TRIAGE_MODEL"
                && *name != "ONBOARDING_MODEL"
        });
        vars.push(("TRIAGE_MODEL", "openrouter/google/gemini-2.0-flash-001"));
        vars.push(("ONBOARDING_MODEL", "openrouter/anthropic/claude-3.5-sonnet"));
        vars.push(("OPENROUTER_API_KEY", "sk-or-test-key"));

        let config = config_from(&vars).expect("openrouter config should load");
        assert_eq!(config.cloudflare_account_id, None);
        assert_eq!(
            config.triage_model,
            "openrouter/google/gemini-2.0-flash-001"
        );
        assert_eq!(
            config.onboarding_model,
            "openrouter/anthropic/claude-3.5-sonnet"
        );
    }

    #[test]
    fn rejects_missing_openrouter_key() {
        let mut vars = required_vars();
        vars.retain(|(name, _)| *name != "TRIAGE_MODEL");
        vars.push(("TRIAGE_MODEL", "openrouter/google/gemini-2.0-flash-001"));

        let error = config_from(&vars).expect_err("missing OPENROUTER_API_KEY should fail");
        assert!(error.to_string().contains("OPENROUTER_API_KEY"));
    }

    #[test]
    fn rejects_missing_firecrawl_key() {
        let mut vars = required_vars();
        vars.retain(|(name, _)| *name != "FIRECRAWL_API_KEY");

        let error = config_from(&vars).expect_err("missing Firecrawl key should fail");
        assert!(error.to_string().contains("FIRECRAWL_API_KEY"));
    }

    #[test]
    fn parses_model_prefixes() {
        let (p, m) = parse_model_spec("openrouter/anthropic/claude").unwrap();
        assert_eq!(p, ModelProvider::OpenRouter);
        assert_eq!(m, "anthropic/claude");

        let (p, m) = parse_model_spec("cloudflare/@cf/meta/llama").unwrap();
        assert_eq!(p, ModelProvider::Cloudflare);
        assert_eq!(m, "@cf/meta/llama");

        let (p, m) = parse_model_spec("openrouter-decisions/baseten/joey-1").unwrap();
        assert_eq!(p, ModelProvider::OpenRouterDecisions);
        assert_eq!(m, "baseten/joey-1");

        // Models without a prefix must error
        assert!(parse_model_spec("@cf/meta/llama").is_err());
        assert!(parse_model_spec("gpt-4o").is_err());
        assert!(parse_model_spec("openrouter/").is_err());
        assert!(parse_model_spec("cloudflare/").is_err());
        assert!(parse_model_spec("openrouter-decisions/").is_err());
        assert!(parse_model_spec("").is_err());
    }

    #[test]
    fn parses_decisions_triage_configuration() {
        let mut vars = required_vars();
        vars.retain(|(name, _)| *name != "TRIAGE_MODEL");
        vars.push(("TRIAGE_MODEL", "openrouter-decisions/baseten/joey-1"));
        vars.push(("OPENROUTER_API_KEY", "sk-or-test-key"));

        let config = config_from(&vars).expect("decisions triage config should load");
        assert_eq!(config.triage_model, "openrouter-decisions/baseten/joey-1");
    }

    #[test]
    fn rejects_decisions_onboarding_model() {
        let mut vars = required_vars();
        vars.retain(|(name, _)| *name != "ONBOARDING_MODEL");
        vars.push(("ONBOARDING_MODEL", "openrouter-decisions/baseten/joey-1"));
        vars.push(("OPENROUTER_API_KEY", "sk-or-test-key"));

        let error = config_from(&vars).expect_err("decisions onboarding should fail");
        assert!(error.to_string().contains("ONBOARDING_MODEL"), "{error}");
    }

    #[test]
    fn rejects_missing_openrouter_key_for_decisions_triage() {
        let mut vars = required_vars();
        vars.retain(|(name, _)| *name != "TRIAGE_MODEL");
        vars.push(("TRIAGE_MODEL", "openrouter-decisions/baseten/joey-1"));

        let error = config_from(&vars).expect_err("missing OPENROUTER_API_KEY should fail");
        assert!(error.to_string().contains("OPENROUTER_API_KEY"), "{error}");
    }

    #[test]
    fn rejects_placeholder_account_id() {
        let mut vars = required_vars();
        vars.retain(|(name, _)| *name != "CLOUDFLARE_ACCOUNT_ID");
        vars.push(("CLOUDFLARE_ACCOUNT_ID", "{account_id}"));

        let error = config_from(&vars).expect_err("placeholder should fail");
        assert!(
            error.to_string().contains("CLOUDFLARE_ACCOUNT_ID"),
            "{error}"
        );
    }

    #[test]
    fn rejects_account_id_that_cannot_live_in_a_url() {
        for bad in ["has whitespace", "has/slash"] {
            let mut vars = required_vars();
            vars.retain(|(name, _)| *name != "CLOUDFLARE_ACCOUNT_ID");
            vars.push(("CLOUDFLARE_ACCOUNT_ID", bad));

            let error = config_from(&vars).expect_err("unusable account id should fail");
            assert!(
                error.to_string().contains("CLOUDFLARE_ACCOUNT_ID"),
                "{bad}: {error}"
            );
        }
    }

    #[test]
    fn negative_discord_id_is_rejected() {
        let mut vars = required_vars();
        vars.retain(|(name, _)| *name != "DISCORD_OPERATOR_USER_ID");
        vars.push(("DISCORD_OPERATOR_USER_ID", "-1"));

        let error = config_from(&vars).expect_err("negative snowflake should fail");
        assert!(error.to_string().contains("DISCORD_OPERATOR_USER_ID"));
    }
}
