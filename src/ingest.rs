//! Jetstream websocket ingestion.
//!
//! The ingest task owns the live connection: it builds the subscription URL,
//! reconnects with exponential backoff, converts raw events through
//! [`crate::extract`], and publishes [`ExtractedPost`] values on a broadcast
//! channel.
//!
//! Every connection starts at the live tail. Posts are never replayed: a
//! replayed post would move belief with news the market has already priced, and
//! the belief update uses the wall clock, so a burst of old posts skips the
//! decay that belongs between them. Missing a post means no trade; replaying it
//! means a false edge and a bad trade.
//!
//! Text handling is deliberately absent here. This module filters events and
//! manages the connection; it never normalizes post text.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::broadcast;
use tokio::time::Instant;
use tokio_tungstenite::{connect_async, tungstenite::Message, WebSocketStream};
use url::Url;

use crate::error::{AppResult, Context};
use crate::extract;
use crate::types::ExtractedPost;

/// Number of posts the broadcast channel buffers before dropping the oldest.
pub const CHANNEL_CAPACITY: usize = 1024;

/// Minimum gap between warnings about unparseable websocket messages.
const BAD_MESSAGE_WARN_INTERVAL: Duration = Duration::from_secs(5);

/// Upper bound for reconnect backoff.
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// Collection requested from Jetstream.
const WANTED_COLLECTIONS: &str = "app.bsky.feed.post";

/// Creates the broadcast channel used to fan posts out to consumers.
///
/// The receiver count is normally one per pipeline stage. When no receiver is
/// subscribed, `send` returns an error and the ingest loop ignores it.
pub fn channel() -> broadcast::Sender<ExtractedPost> {
    broadcast::channel(CHANNEL_CAPACITY).0
}

/// Runs the ingestion loop forever.
///
/// Connection failures, closed streams, and websocket errors are retried with
/// exponential backoff. The function returns only for errors that cannot be
/// retried, such as a malformed `JETSTREAM_URL`.
///
/// No cursor is sent, so each attempt subscribes at the live tail and a
/// reconnect costs only the posts published while the socket was down.
pub async fn run(
    config: crate::config::Config,
    tx: broadcast::Sender<ExtractedPost>,
) -> AppResult<()> {
    let mut attempt: u32 = 0;

    loop {
        let url = build_url(&config.jetstream_url)?;

        tracing::info!(url = %url, "connecting to jetstream");

        match connect_async(url.as_str()).await {
            Ok((websocket, _response)) => {
                // A completed handshake proves the endpoint is reachable, so
                // the next failure starts the backoff sequence again.
                attempt = 0;
                tracing::info!("jetstream connected");

                match consume(websocket, &tx).await {
                    Ok(()) => tracing::warn!("jetstream stream ended; reconnecting"),
                    Err(error) => tracing::error!(error = %error, "jetstream stream failed"),
                }

                let delay = backoff_delay(attempt);
                attempt = attempt.saturating_add(1);
                tracing::warn!(retry_in_secs = delay.as_secs(), "reconnecting to jetstream");
                tokio::time::sleep(delay).await;
            }
            Err(error) => {
                let delay = backoff_delay(attempt);
                attempt = attempt.saturating_add(1);
                tracing::warn!(
                    error = %error,
                    retry_in_secs = delay.as_secs(),
                    "jetstream connection failed"
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
}

/// Builds the subscription URL, preserving any query already in `base`.
pub(crate) fn build_url(base: &str) -> AppResult<String> {
    let mut url = Url::parse(base).with_context(|| format!("parsing jetstream URL {base}"))?;

    url.query_pairs_mut()
        .append_pair("wantedCollections", WANTED_COLLECTIONS);

    Ok(url.into())
}

/// Exponential reconnect delay: 1s, 2s, 4s, ... capped at 60s.
pub(crate) fn backoff_delay(attempt: u32) -> Duration {
    let seconds = 1_u64.checked_shl(attempt).unwrap_or(u64::MAX);
    Duration::from_secs(seconds.min(MAX_BACKOFF.as_secs()))
}

/// Consumes one websocket connection until it closes or fails.
async fn consume<T>(
    websocket: WebSocketStream<T>,
    tx: &broadcast::Sender<ExtractedPost>,
) -> AppResult<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let (mut sink, mut stream) = websocket.split();

    let mut last_bad_message_warn: Option<Instant> = None;

    let result = loop {
        match stream.next().await {
            None => break Ok(()),
            Some(Err(error)) => break Err(error).context("reading jetstream message"),
            Some(Ok(Message::Text(text))) => {
                ingest_event(text.as_str(), tx, &mut last_bad_message_warn);
            }
            Some(Ok(Message::Close(_))) => break Ok(()),
            // Ping, Pong, Binary, and raw frames carry no post data.
            Some(Ok(_)) => {}
        }
    };

    // Best-effort close handshake; the peer may already be gone.
    let _ = sink.close().await;
    result
}

/// Parses one websocket text message and publishes any extracted post.
///
/// Unparseable messages are ignored. The warning is rate limited so a stream of
/// garbage cannot flood the logs.
fn ingest_event(
    raw: &str,
    tx: &broadcast::Sender<ExtractedPost>,
    last_bad_message_warn: &mut Option<Instant>,
) {
    let event: serde_json::Value = match serde_json::from_str(raw) {
        Ok(event) => event,
        Err(error) => {
            let now = Instant::now();
            let should_warn = last_bad_message_warn.is_none_or(|last| {
                now.saturating_duration_since(last) >= BAD_MESSAGE_WARN_INTERVAL
            });
            if should_warn {
                tracing::warn!(error = %error, "ignoring unparseable jetstream message");
                *last_bad_message_warn = Some(now);
            }
            return;
        }
    };

    let Some(post) = extract::extract(&event) else {
        return;
    };

    // No receivers means nobody consumes the stream yet; that is not an error.
    let _ = tx.send(post);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_has_no_receivers_until_subscribed() {
        let tx = channel();
        assert_eq!(tx.receiver_count(), 0);
        let _rx = tx.subscribe();
        assert_eq!(tx.receiver_count(), 1);
    }

    #[test]
    fn build_url_without_existing_query() {
        let url = build_url("wss://jetstream.example/subscribe").expect("valid URL");

        assert_eq!(
            url,
            "wss://jetstream.example/subscribe?wantedCollections=app.bsky.feed.post"
        );
    }

    #[test]
    fn build_url_never_requests_a_replay() {
        let url = build_url("wss://jetstream.example/subscribe?compression=zstd").expect("valid URL");

        assert_eq!(
            url,
            "wss://jetstream.example/subscribe?compression=zstd&\
             wantedCollections=app.bsky.feed.post"
        );
        assert!(
            !url.contains("cursor="),
            "ingest must always subscribe at the live tail"
        );
    }

    #[test]
    fn build_url_rejects_a_malformed_base() {
        assert!(build_url("not a url").is_err());
    }

    #[test]
    fn backoff_starts_at_one_second() {
        assert_eq!(backoff_delay(0), Duration::from_secs(1));
    }

    #[test]
    fn backoff_doubles_per_attempt() {
        assert_eq!(backoff_delay(3), Duration::from_secs(8));
    }

    #[test]
    fn backoff_is_capped_at_sixty_seconds() {
        assert_eq!(backoff_delay(10), Duration::from_secs(60));
        assert_eq!(backoff_delay(u32::MAX), Duration::from_secs(60));
    }

    #[test]
    fn ingest_event_publishes_posts() {
        let (tx, mut rx) = broadcast::channel(CHANNEL_CAPACITY);
        let mut last_warn = None;

        let raw = r#"{
            "did": "did:plc:author",
            "time_us": 1725911162329308,
            "kind": "commit",
            "commit": {
                "operation": "create",
                "collection": "app.bsky.feed.post",
                "rkey": "3kabc",
                "record": { "text": "MiXeD case!" }
            }
        }"#;

        ingest_event(raw, &tx, &mut last_warn);

        let post = rx.try_recv().expect("post should be published");
        assert_eq!(post.text, "MiXeD case!");
    }

    #[test]
    fn ingest_event_ignores_garbage() {
        let (tx, _rx) = broadcast::channel(CHANNEL_CAPACITY);
        let mut last_warn = None;

        ingest_event("not json", &tx, &mut last_warn);
        ingest_event("", &tx, &mut last_warn);

        assert!(last_warn.is_some(), "the first warning should be recorded");

        // A second bad message inside the rate limit does not move the clock.
        let first_warn = last_warn;
        ingest_event("{", &tx, &mut last_warn);
        assert_eq!(last_warn, first_warn);
    }

    #[test]
    fn ingest_event_ignores_non_post_events() {
        let (tx, mut rx) = broadcast::channel(CHANNEL_CAPACITY);
        let mut last_warn = None;

        let raw = r#"{
            "did": "did:plc:author",
            "time_us": 1725911162329308,
            "kind": "commit",
            "commit": {
                "operation": "delete",
                "collection": "app.bsky.feed.post",
                "rkey": "3kabc",
                "record": { "text": "gone" }
            }
        }"#;

        ingest_event(raw, &tx, &mut last_warn);

        assert!(rx.try_recv().is_err());
    }
}
