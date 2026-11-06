//! Chat-model transport for onboarding, built on the vendored `rig`.
//!
//! [`LlmClient`] holds one `rig` client per chat provider and builds the
//! completion model that [`crate::onboarding_agent`] hands to
//! [`rig::AgentBuilder`]. Triage does not use this module: it is a decision
//! model and reaches OpenRouter's `/api/alpha/decisions` through
//! [`rig::typesafeai`] in [`crate::triage`].
//!
//! # One transport, three deadlines
//!
//! Both providers share one reqwest client built by [`chat_transport`]. rig's
//! bundled client has no request timeout at all, so the transport sets a
//! connect timeout, a read timeout, and a total deadline. The total deadline is
//! the one that ends a stalled streamed turn: OpenRouter holds a queued request
//! open with `: OPENROUTER PROCESSING` comment lines, and every comment is a
//! read that resets the read timeout.
//!
//! # What this module must not become
//!
//! Pure transport: provider construction and completion models. JSON helpers,
//! prompts, response parsers, retries, and feature logic belong to the feature
//! module that needs them — `snippet`, for example, lives in
//! [`crate::onboarding`], its only caller.

use std::time::Duration;

use crate::config::{parse_model_spec, ModelProvider};
use crate::error::{anyhow, bail, AppResult, Context};

/// Upper bound on opening one TCP/TLS connection to an LLM provider.
const LLM_CONNECT_TIMEOUT: Duration = Duration::from_secs(20);

/// Longest silence tolerated while an LLM provider answers.
///
/// `reqwest` resets this on every successful read, so it bounds a request whose
/// connection goes quiet, without capping how long a live stream may run.
/// Observed model turns answer in a few seconds, so this only ever fires on a
/// dead connection.
const LLM_READ_TIMEOUT: Duration = Duration::from_secs(45);

/// Longest one onboarding model request may run, from connect to last byte.
///
/// [`LLM_READ_TIMEOUT`] cannot end a stream that keeps arriving: OpenRouter
/// holds a queued upstream request open with `: OPENROUTER PROCESSING` comment
/// lines, and every comment counts as a read and resets that timeout. A stalled
/// turn then runs with no log line and no error until the whole `/track` run
/// hits its 600-second wall. This total deadline ends one turn of the research
/// agent instead.
const ONBOARDING_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

/// Shared chat client for onboarding, built on rig.
///
/// Cloneable so each agent run can hold a handle; the underlying rig clients
/// share connection pools.
#[derive(Clone)]
pub struct LlmClient {
    cloudflare: Option<rig::providers::openai::OpenAI>,
    openrouter: Option<rig::providers::openai::OpenAI>,
}

/// Builds the HTTP transport both chat providers share.
///
/// `total` bounds one whole request, including a streamed reply, because a
/// read timeout cannot end a stream that keeps arriving. Every request rig
/// sends for an agent turn goes through this transport.
fn chat_transport(total: Duration) -> AppResult<rig::http_client::ReqwestClient> {
    let client = rig::rig_reqwest::reqwest::Client::builder()
        .connect_timeout(LLM_CONNECT_TIMEOUT)
        .read_timeout(LLM_READ_TIMEOUT)
        .timeout(total)
        .build()
        .context("building the LLM HTTP client")?;
    Ok(rig::http_client::ReqwestClient::from(client))
}

impl LlmClient {
    /// Builds the client from the process environment.
    ///
    /// Initializes Cloudflare and/or OpenRouter depending on the credentials
    /// present in the process environment. Both providers share one transport
    /// from [`chat_transport`], because the bundled `reqwest` client rig
    /// defaults to has no request timeout at all.
    pub fn new() -> AppResult<Self> {
        let transport = chat_transport(ONBOARDING_REQUEST_TIMEOUT)?;

        let (cloudflare, cf_err) = match rig::providers::openai::OpenAIConfig::from_env_with(
            &rig::providers::cloudflare::DIALECT,
        ) {
            Ok(config) => (Some(config.connect(transport.clone())), None),
            Err(e) => (None, Some(e)),
        };
        let (openrouter, or_err) = match rig::providers::openai::OpenAIConfig::from_env_with(
            &rig::providers::openai::wire::OPENROUTER,
        ) {
            Ok(config) => (Some(config.connect(transport)), None),
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

    use rig::completion::CompletionRequest;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// A stream that only sends keepalives must fail on the total request
    /// deadline. OpenRouter holds a queued turn open with `: OPENROUTER
    /// PROCESSING` comment lines, and every comment is a read that resets the
    /// read timeout, so silence never arrives for a read timeout to catch.
    #[test]
    fn keepalive_stream_ends_on_the_request_deadline() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime builds");

        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .expect("listener binds");
        let address = listener.local_addr().expect("listener has an address");
        let server = runtime.spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("connection accepted");
            let mut buffer = [0u8; 4096];
            let head = socket.read(&mut buffer).await.expect("request read");
            assert!(head > 0, "client sent no request bytes");
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n")
                .await
                .expect("headers written");
            while socket
                .write_all(b": OPENROUTER PROCESSING\n\n")
                .await
                .is_ok()
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        let config = rig::providers::openai::OpenAIConfig {
            base_url: format!("http://{address}"),
            ..rig::providers::openai::OpenAIConfig::with_key(
                &rig::providers::openai::wire::OPENROUTER,
                "test-key",
            )
        };
        let transport = chat_transport(Duration::from_millis(200)).expect("transport builds");
        let model = config.connect(transport).completion("test-model");

        let call = runtime.block_on(async {
            tokio::time::timeout(
                Duration::from_secs(5),
                model.call(CompletionRequest::new("hi")),
            )
            .await
        });
        server.abort();
        let answer = call.expect("a stalled stream ran past the request deadline");
        assert!(answer.is_err(), "a stalled stream must fail");
    }
}
