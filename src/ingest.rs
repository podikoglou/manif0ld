//! Jetstream websocket ingestion.
//!
//! The ingest task owns the live connection: it builds the subscription URL,
//! reconnects with exponential backoff, converts raw events through
//! [`crate::extract`], publishes [`ExtractedPost`] values on a broadcast
//! channel, and persists the microsecond cursor to the `kv` table.
//!
//! Text handling is deliberately absent here. This module filters events and
//! manages the cursor; it never normalizes post text.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::broadcast;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::Message, WebSocketStream};
use url::Url;

use crate::db::Db;
use crate::error::{AppResult, Context};
use crate::extract;
use crate::types::ExtractedPost;

/// Number of posts the broadcast channel buffers before dropping the oldest.
pub const CHANNEL_CAPACITY: usize = 1024;

/// `kv` key holding the newest observed Jetstream `time_us` value.
const CURSOR_KEY: &str = "jetstream_cursor_us";

/// How often the cursor may be written to SQLite.
const PERSIST_INTERVAL: Duration = Duration::from_secs(5);

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
/// retried, such as a malformed `JETSTREAM_URL`; cursor write failures are
/// logged and retried on the next tick or reconnect.
///
/// The cursor is reloaded from the database before every connection attempt so
/// a fresh process resumes where the previous one stopped, and a reconnect
/// within this process resumes after the last persisted post.
pub async fn run(
    config: crate::config::Config,
    db: crate::db::Db,
    tx: broadcast::Sender<ExtractedPost>,
) -> AppResult<()> {
    let mut attempt: u32 = 0;

    loop {
        let cursor_us = load_cursor(&db);
        let url = build_url(&config.jetstream_url, cursor_us)?;

        tracing::info!(url = %url, "connecting to jetstream");

        match connect_async(url.as_str()).await {
            Ok((websocket, _response)) => {
                // A completed handshake proves the endpoint is reachable, so
                // the next failure starts the backoff sequence again.
                attempt = 0;
                tracing::info!("jetstream connected");

                let mut tracker = CursorTracker::new(cursor_us, Instant::now());
                let result = consume(websocket, &db, &tx, &mut tracker).await;

                match &result {
                    Ok(()) => tracing::warn!("jetstream stream ended; reconnecting"),
                    Err(error) => tracing::error!(error = %error, "jetstream stream failed"),
                }

                // Covers an abrupt error path. On a clean close `consume` has
                // already written the cursor, so this is a no-op.
                if let Err(error) = persist_cursor(&db, &mut tracker) {
                    tracing::error!(error = %error, "could not persist jetstream cursor");
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
pub(crate) fn build_url(base: &str, cursor_us: Option<i64>) -> AppResult<String> {
    let mut url = Url::parse(base).with_context(|| format!("parsing jetstream URL {base}"))?;

    {
        let mut query = url.query_pairs_mut();
        query.append_pair("wantedCollections", WANTED_COLLECTIONS);
        if let Some(cursor_us) = cursor_us {
            query.append_pair("cursor", &cursor_us.to_string());
        }
    }

    Ok(url.into())
}

/// Exponential reconnect delay: 1s, 2s, 4s, ... capped at 60s.
pub(crate) fn backoff_delay(attempt: u32) -> Duration {
    let seconds = 1_u64.checked_shl(attempt).unwrap_or(u64::MAX);
    Duration::from_secs(seconds.min(MAX_BACKOFF.as_secs()))
}

/// Tracks the newest Jetstream timestamp and when it was last persisted.
#[derive(Debug)]
struct CursorTracker {
    highest: Option<i64>,
    last_written: Option<i64>,
    last_write_at: Instant,
}

impl CursorTracker {
    /// Seeds the tracker with the cursor loaded from the database.
    ///
    /// The loaded cursor counts as already written, so startup does not rewrite
    /// an unchanged value.
    fn new(cursor_us: Option<i64>, now: Instant) -> Self {
        Self {
            highest: cursor_us,
            last_written: cursor_us,
            last_write_at: now,
        }
    }

    /// Records an observed `time_us`, keeping the maximum.
    ///
    /// Jetstream can deliver slightly out-of-order events; a lower timestamp
    /// never moves the cursor backwards.
    fn observe(&mut self, time_us: i64) {
        self.highest = Some(self.highest.map_or(time_us, |highest| highest.max(time_us)));
    }

    /// Whether the cursor changed and the write interval has elapsed.
    fn should_persist(&self, now: Instant) -> bool {
        self.highest.is_some()
            && self.highest != self.last_written
            && now.saturating_duration_since(self.last_write_at) >= PERSIST_INTERVAL
    }

    /// Records a successful write of the current highest cursor.
    fn mark_persisted(&mut self) {
        self.last_written = self.highest;
        self.last_write_at = Instant::now();
    }
}

/// Reads the persisted cursor, warning and starting fresh when it is malformed.
fn load_cursor(db: &Db) -> Option<i64> {
    match db.get_kv(CURSOR_KEY) {
        Ok(Some(raw)) => match raw.trim().parse::<i64>() {
            Ok(cursor_us) => Some(cursor_us),
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "ignoring malformed jetstream cursor; starting from the live tail"
                );
                None
            }
        },
        Ok(None) => None,
        Err(error) => {
            tracing::warn!(
                error = %error,
                "could not read jetstream cursor; starting from the live tail"
            );
            None
        }
    }
}

/// Writes the highest cursor when it changed since the last write.
///
/// This is the forced variant used on stream end, so the final few posts are
/// not lost merely because the 5 second interval has not elapsed.
fn persist_cursor(db: &Db, tracker: &mut CursorTracker) -> AppResult<()> {
    let Some(highest) = tracker.highest else {
        return Ok(());
    };

    if tracker.last_written != Some(highest) {
        db.set_kv(CURSOR_KEY, &highest.to_string())
            .context("persisting jetstream cursor")?;
    }
    tracker.mark_persisted();
    Ok(())
}

/// Consumes one websocket connection until it closes or fails.
///
/// The cursor is persisted on a 5 second ticker and once more when the stream
/// ends, so a close never loses the newest post.
async fn consume<T>(
    websocket: WebSocketStream<T>,
    db: &Db,
    tx: &broadcast::Sender<ExtractedPost>,
    tracker: &mut CursorTracker,
) -> AppResult<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let (mut sink, mut stream) = websocket.split();

    let mut ticker = tokio::time::interval(PERSIST_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // The first tick completes immediately; skip it so the first periodic
    // write happens after a full interval.
    ticker.tick().await;

    let mut last_bad_message_warn: Option<Instant> = None;

    let result = loop {
        tokio::select! {
            _ = ticker.tick() => {
                if tracker.should_persist(Instant::now()) {
                    if let Err(error) = persist_cursor(db, tracker) {
                        tracing::error!(error = %error, "could not persist jetstream cursor");
                    }
                }
            }
            message = stream.next() => {
                match message {
                    None => {
                        persist_cursor(db, tracker)?;
                        break Ok(());
                    }
                    Some(Err(error)) => {
                        break Err(error).context("reading jetstream message");
                    }
                    Some(Ok(Message::Text(text))) => {
                        ingest_event(text.as_str(), tracker, tx, &mut last_bad_message_warn);
                    }
                    Some(Ok(Message::Close(_))) => {
                        persist_cursor(db, tracker)?;
                        break Ok(());
                    }
                    // Ping, Pong, Binary, and raw frames carry no post data.
                    Some(Ok(_)) => {}
                }
            }
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
    tracker: &mut CursorTracker,
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

    if let Some(time_us) = event["time_us"].as_i64() {
        tracker.observe(time_us);
    }

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
        let url = build_url("wss://jetstream.example/subscribe", None).expect("valid URL");

        assert_eq!(
            url,
            "wss://jetstream.example/subscribe?wantedCollections=app.bsky.feed.post"
        );
    }

    #[test]
    fn build_url_with_existing_query_and_cursor() {
        let url = build_url(
            "wss://jetstream.example/subscribe?compression=zstd",
            Some(1_725_911_162_329_308),
        )
        .expect("valid URL");

        assert_eq!(
            url,
            "wss://jetstream.example/subscribe?compression=zstd&\
             wantedCollections=app.bsky.feed.post&cursor=1725911162329308"
        );
    }

    #[test]
    fn build_url_rejects_a_malformed_base() {
        assert!(build_url("not a url", None).is_err());
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
    fn cursor_persists_only_on_change_and_after_five_seconds() {
        let start = Instant::now();
        let mut tracker = CursorTracker::new(None, start);

        assert!(!tracker.should_persist(start + PERSIST_INTERVAL));

        tracker.observe(100);
        assert!(!tracker.should_persist(start));
        assert!(!tracker.should_persist(start + Duration::from_secs(4)));
        assert!(tracker.should_persist(start + PERSIST_INTERVAL));

        tracker.mark_persisted();
        assert!(
            !tracker.should_persist(Instant::now() + Duration::from_secs(30)),
            "unchanged cursor must not be rewritten"
        );
    }

    #[test]
    fn lower_out_of_order_timestamp_does_not_reduce_cursor() {
        let start = Instant::now();
        let mut tracker = CursorTracker::new(None, start);

        tracker.observe(200);
        tracker.observe(100);
        assert_eq!(tracker.highest, Some(200));

        let mut seeded = CursorTracker::new(Some(500), start);
        seeded.observe(400);
        assert_eq!(seeded.highest, Some(500));
        assert!(!seeded.should_persist(start + Duration::from_secs(10)));
    }

    #[test]
    fn ingest_event_publishes_posts_and_advances_cursor() {
        let (tx, mut rx) = broadcast::channel(CHANNEL_CAPACITY);
        let mut tracker = CursorTracker::new(None, Instant::now());
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

        ingest_event(raw, &mut tracker, &tx, &mut last_warn);

        let post = rx.try_recv().expect("post should be published");
        assert_eq!(post.text, "MiXeD case!");
        assert_eq!(tracker.highest, Some(1_725_911_162_329_308));
    }

    #[test]
    fn ingest_event_ignores_garbage_without_advancing_cursor() {
        let (tx, _rx) = broadcast::channel(CHANNEL_CAPACITY);
        let mut tracker = CursorTracker::new(None, Instant::now());
        let mut last_warn = None;

        ingest_event("not json", &mut tracker, &tx, &mut last_warn);
        ingest_event("", &mut tracker, &tx, &mut last_warn);

        assert_eq!(tracker.highest, None);
        assert!(last_warn.is_some(), "the first warning should be recorded");

        // A second bad message inside the rate limit does not move the clock.
        let first_warn = last_warn;
        ingest_event("{", &mut tracker, &tx, &mut last_warn);
        assert_eq!(last_warn, first_warn);
    }

    #[test]
    fn ingest_event_ignores_non_post_events() {
        let (tx, mut rx) = broadcast::channel(CHANNEL_CAPACITY);
        let mut tracker = CursorTracker::new(None, Instant::now());
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

        ingest_event(raw, &mut tracker, &tx, &mut last_warn);

        assert!(rx.try_recv().is_err());
        assert_eq!(tracker.highest, None);
    }
}
