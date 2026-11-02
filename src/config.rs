//! Process configuration loaded from environment variables.
//!
//! `Config` is the single source of truth for runtime settings. Values are read
//! once at startup; downstream modules receive `&Config` rather than reading the
//! environment themselves. Secret values are never included in error messages.

use std::path::PathBuf;

use crate::error::{anyhow, bail, AppResult, Context};

const DEFAULT_JETSTREAM_URL: &str = "wss://jetstream2.us-east.bsky.network/subscribe";
const DEFAULT_DB_PATH: &str = "./data/arb.db";
const DEFAULT_MARKET_BUDGET: f64 = 100.0;
const DEFAULT_KELLY_FRACTION: f64 = 0.25;
const DEFAULT_ENTRY_EDGE_THRESHOLD: f64 = 0.05;
const DEFAULT_EXIT_EDGE_THRESHOLD: f64 = 0.01;
const DEFAULT_MIN_TRADE_SIZE: f64 = 1.0;
const DEFAULT_BELIEF_KAPPA: f64 = 1.1;
const DEFAULT_BELIEF_HALFLIFE_HOURS: f64 = 4.0;

/// Runtime configuration for the bot.
///
/// Construct with [`Config::from_env`] in production. The `Debug` impl prints
/// every field verbatim, so never log a `Config` value: several fields hold
/// credentials.
#[derive(Debug, Clone)]
pub struct Config {
    pub jetstream_url: String,
    pub cloudflare_account_id: String,
    pub triage_model: String,
    pub onboarding_model: String,
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

        // The Workers AI transport reads `CLOUDFLARE_API_TOKEN` and the gateway
        // id from the process environment itself. This struct validates the
        // account id here so a missing or placeholder value fails at startup,
        // and owns the model names the LLM calls use.
        let cloudflare_account_id = required(&get, "CLOUDFLARE_ACCOUNT_ID")?;
        validate_cloudflare_account_id(&cloudflare_account_id)?;
        let triage_model = required(&get, "TRIAGE_MODEL")?;
        let onboarding_model = required(&get, "ONBOARDING_MODEL")?;
        let manifold_api_key = required(&get, "MANIFOLD_API_KEY")?;
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

        let db_path = optional(&get, "DB_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_DB_PATH));

        let config = Self {
            jetstream_url,
            cloudflare_account_id,
            triage_model,
            onboarding_model,
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
            db_path,
        };

        Ok(config)
    }
}

/// Rejects an account id no URL can carry.
///
/// `CLOUDFLARE_ACCOUNT_ID` is interpolated into the `/ai/run` endpoint URL by
/// the Workers AI transport in `src/triage.rs` (`LlmClient`), which already
/// refuses anything that could not live in a path segment. The extra guard
/// catches the literal `{account_id}` placeholder from `.env.example`
/// (previously `CF_AI_GATEWAY_URL` carried it) so a setup error fails at
/// config load rather than as a confusing 404 on the first LLM call.
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Required variables with placeholder values; numeric and path variables
    /// are intentionally omitted so defaults can be asserted.
    fn required_vars() -> Vec<(&'static str, &'static str)> {
        vec![
            ("CLOUDFLARE_ACCOUNT_ID", "test-account"),
            ("TRIAGE_MODEL", "@cf/google/gemini-test"),
            ("ONBOARDING_MODEL", "@cf/deepseek/test"),
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
        assert_eq!(config.db_path, PathBuf::from("./data/arb.db"));
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
    fn stores_cloudflare_account_id_and_models() {
        let config = config_from(&required_vars()).expect("config should load");

        assert_eq!(config.cloudflare_account_id, "test-account");
        assert_eq!(config.triage_model, "@cf/google/gemini-test");
        assert_eq!(config.onboarding_model, "@cf/deepseek/test");
    }

    #[test]
    fn rejects_placeholder_account_id() {
        let mut vars = required_vars();
        vars.retain(|(name, _)| *name != "CLOUDFLARE_ACCOUNT_ID");
        vars.push(("CLOUDFLARE_ACCOUNT_ID", "{account_id}"));

        let error = config_from(&vars).expect_err("placeholder should fail");
        assert!(error.to_string().contains("CLOUDFLARE_ACCOUNT_ID"), "{error}");
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
