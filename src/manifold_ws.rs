//! Manifold broadcast websocket client (`wss://api.manifold.markets/ws`).
//!
//! This is a live-wire-probe-verified reimplementation of the Manifold
//! stream protocol — the trading-side counterpart of [`crate::ingest`] —
//! feeding `docs/ALGO.md` §6.1's "manifold events never drop" channel.
//! Everything below is measured, not guessed from the docs:
//!
//! - **The subscription is a first-frame JSON message, not a query
//!   parameter.** `?sub=` carries nothing; a
//!   `{"type":"subscribe","txid":N,"topics":[...]}` frame right after
//!   connect is acked once inside ~100 ms with
//!   `{"type":"ack","txid":N,"success":true}`. Topics are SLASH-delimited
//!   (`global/new-bet`); comma-delimited spellings ack but broadcast dead
//!   silence.
//! - **The client must send `{"type":"ping","txid":N}` roughly every 20 s.**
//!   The server sweeps connections it has not heard from (observed kill at
//!   94 s with no client traffic) and there is no server-side ping to rely
//!   on. Any client frame refreshes that timer, so the keepalive interval's
//!   immediate first tick is harmless.
//! - **Broadcasts carry no txid:** `{"type":"broadcast","topic":...,
//!   "data":...}`. A `success:false` ack is fatal for one connection → warn
//!   and reconnect.
//! - **The bare `global` topic duplicates its subtopics**, so only the
//!   `global/...` names are subscribed.
//!
//! The module is deliberately stateless beyond the incrementing txid: every
//! reconnect re-sends the subscription, and consumers rebuild mirrored
//! state from the event stream. Events are delivered on an mpsc channel
//! that is sized generously and **never drops**: when the buffer is full
//! the socket loop itself blocks on `send`, pushing backpressure into the
//! socket read instead of discarding tradeable data.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_tungstenite::{WebSocketStream, connect_async, tungstenite::Message};

use crate::config::Config;
use crate::error::{AppResult, Context};

/// Endpoint of the Manifold broadcast websocket (verified live: no query
/// parameters, no auth for the topics we use).
const ENDPOINT: &str = "wss://api.manifold.markets/ws";

/// Events the event channel buffers before sends start to block.
///
/// Measured broadcast rates peak around 8 msg/s, so this is minutes of
/// buffering for a stalled consumer before backpressure reaches the socket.
pub const CHANNEL_CAPACITY: usize = 1024;

/// Grace period for the server to ack a fresh subscription before the
/// connection is abandoned for a reconnect.
const ACK_TIMEOUT: Duration = Duration::from_secs(5);

/// Interval between client keepalive pings. The server kills a silent
/// connection within ~94 s; pinging at 20 s keeps that competitor far away.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(20);

/// Upper bound for reconnect backoff.
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// Minimum gap between `debug!` logs about unparseable broadcast payloads.
const BAD_PAYLOAD_LOG_INTERVAL: Duration = Duration::from_secs(5);

/// Minimum gap between `warn!` logs about a full event channel. A full
/// channel is a consumer problem, the socket loop already holds back, and
/// the condition persists until it does not: log it sparsely.
const CHANNEL_FULL_LOG_INTERVAL: Duration = Duration::from_secs(30);

/// How many bytes of an unparseable payload to show in a `debug!` log line.
const BAD_PAYLOAD_SAMPLE_BYTES: usize = 240;

/// Event topics the bot subscribes to, in one immutable list.
///
/// `global/updated-contract` and `global/new-bet` mirror price and order
/// flow; `global/new-contract` is the rare sniping feed (`docs/ALGO.md` §6.1
/// tabulates the expected rates). The bare `global` topic is intentionally
/// absent: its frames duplicate the subtopics exactly.
pub fn default_topics() -> &'static [&'static str] {
    &[
        "global/new-bet",
        "global/updated-contract",
        "global/new-contract",
    ]
}

/// Creates the event channel the socket task queues parsed events onto.
///
/// The receiver must be handed out before (or at) the point [`run`] is
/// spawned, per the `App::spawn` convention, so the channel is never
/// momentarily ownerless: a missing receiver makes [`run`] stop instead of
/// reconnecting to nobody.
pub fn channel() -> (mpsc::Sender<ManifoldEvent>, mpsc::Receiver<ManifoldEvent>) {
    mpsc::channel(CHANNEL_CAPACITY)
}

/// Runs the broadcast client forever, queueing events on `tx`.
///
/// Connection failures, ack failures, ack timeouts, and closed streams are
/// retried with exponential backoff (1 s → 2 s → 4 s → … capped at 60 s),
/// mirroring [`crate::ingest`]. Every successful connection re-sends the
/// subscription; nothing is replayed, and consumers rebuild their state
/// from the event stream.
///
/// The [`Config`] parameter is currently unused — the verified endpoint is
/// the [`ENDPOINT`] constant and the topics need no auth — but stays in the
/// signature so the `App::spawn` wiring keeps a stable call shape for future
/// topic or endpoint configuration.
///
/// The function returns when no receiver of the event channel is left;
/// reconnecting to nobody would only churn.
pub async fn run(_config: Config, tx: mpsc::Sender<ManifoldEvent>) -> AppResult<()> {
    let mut attempt: u32 = 0;

    loop {
        if tx.is_closed() {
            tracing::warn!("no manifold event receiver left; stopping the socket client");
            return Ok(());
        }

        tracing::info!(
            endpoint = ENDPOINT,
            "connecting to manifold broadcast socket"
        );

        match connect_async(ENDPOINT).await {
            Ok((websocket, _response)) => {
                // A completed handshake proves the endpoint is reachable, so
                // the next failure starts the backoff sequence again.
                attempt = 0;
                tracing::info!("manifold broadcast socket connected");

                match consume(websocket, &tx).await {
                    Ok(()) => tracing::warn!("manifold stream ended; reconnecting"),
                    Err(error) => {
                        if tx.is_closed() {
                            tracing::debug!(
                                error = %error,
                                "event receiver vanished mid-stream; stopping"
                            );
                            return Ok(());
                        }
                        tracing::error!(error = %error, "manifold stream failed");
                    }
                }

                let delay = backoff_delay(attempt);
                attempt = attempt.saturating_add(1);
                tracing::warn!(retry_in_secs = delay.as_secs(), "reconnecting to manifold");
                tokio::time::sleep(delay).await;
            }
            Err(error) => {
                let delay = backoff_delay(attempt);
                attempt = attempt.saturating_add(1);
                tracing::warn!(
                    error = %error,
                    retry_in_secs = delay.as_secs(),
                    "manifold connection failed"
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
}

// -----------------------------------------------------------------------------
// Events
// -----------------------------------------------------------------------------

/// One parsed broadcast event, typed for the consumers below.
#[derive(Debug, Clone, PartialEq)]
pub enum ManifoldEvent {
    /// One bet from a `global/new-bet` batch (one event per bet).
    Bet(BetEvent),
    /// One price patch from `global/updated-contract`.
    ContractUpdate(ContractUpdateEvent),
    /// One brand-new market from `global/new-contract`.
    NewContract(NewContractEvent),
}

/// A single bet or limit order, as broadcast in `global/new-bet`.
///
/// Only fields eagerly needed downstream are kept; everything else
/// (`fills`, `fees`, `silent`, `visibility`, …) is tolerated and ignored so
/// a payload variant we have not measured cannot kill the stream.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BetEvent {
    pub id: String,
    pub user_id: String,
    pub contract_id: String,
    /// Present on multiple-choice markets only.
    pub answer_id: Option<String>,
    /// `YES` or `NO`.
    pub outcome: String,
    pub amount: f64,
    pub order_amount: f64,
    pub shares: f64,
    pub prob_before: f64,
    pub prob_after: f64,
    pub is_filled: bool,
    /// Present on limit orders, resting or partially filled.
    pub limit_prob: Option<f64>,
    pub expires_at: Option<i64>,
    pub is_cancelled: bool,
    pub is_redemption: bool,
    /// Milliseconds since the epoch.
    pub created_time: i64,
    /// Live batches omit this so far; the REST API carries it.
    pub updated_time: Option<i64>,
}

/// A partial contract snapshot from `global/updated-contract`.
///
/// The server does not transmit the full contract: no question, no
/// description, no resolution fields. It is a price patch keyed by
/// `contract_id`, leaving market metadata to the REST snapshot that created
/// the tracked market.
#[derive(Debug, Clone, PartialEq)]
pub struct ContractUpdateEvent {
    pub contract_id: String,
    /// Server broadcast timestamp, milliseconds since the epoch.
    pub broadcast_time: i64,
    pub probs: ContractProbs,
}

/// The probability payload of a [`ContractUpdateEvent`].
///
/// Which shape applies is detected structurally, because the partial patch
/// carries no `mechanism`: a top-level `prob` with no `answers` is a binary
/// (`cpmm-1`-style) market; an `answers` array is a multi (`cpmm-multi-*`)
/// market.
#[derive(Debug, Clone, PartialEq)]
pub enum ContractProbs {
    /// One consensus probability for a binary market.
    Binary { prob: f64 },
    /// Per-answer probabilities for a multiple-choice market.
    Multi { answers: Vec<AnswerProb> },
}

/// One answer's probability inside a multi-market update.
#[derive(Debug, Clone, PartialEq)]
pub struct AnswerProb {
    pub id: String,
    pub prob: f64,
}

/// A brand-new market, as broadcast in `global/new-contract`.
///
/// **Source-verified, not live-verified.** The probe never caught this topic
/// firing (hours-scale rate); the shape is documented from the backend
/// source, where `data` is `{contract, creator}`, and parsed leniently to
/// match: every field except `id` is optional, unknown fields are ignored,
/// and a small shape change degrades to fewer populated fields instead of a
/// dropped event.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContractStub {
    pub id: String,
    pub slug: Option<String>,
    pub question: Option<String>,
    pub outcome_type: Option<String>,
    pub mechanism: Option<String>,
    /// Market close time, milliseconds since the epoch.
    pub close_time: Option<i64>,
    /// Opening probability of a binary market.
    pub initial_prob: Option<f64>,
    /// Creation time, milliseconds since the epoch.
    pub created_time: Option<i64>,
    pub creator_id: Option<String>,
}

/// Wrapper carrying the contract payload of a `global/new-contract`
/// broadcast; the companion `creator` object is ignored.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NewContractEvent {
    pub contract: ContractStub,
}

// -----------------------------------------------------------------------------
// Connection lifecycle
// -----------------------------------------------------------------------------

/// Consumes one established websocket connection until it ends.
///
/// Sends the subscription, waits (bounded) for its ack, then pumps
/// broadcasts into `tx` while pinging every [`KEEPALIVE_INTERVAL`]. An ack
/// timeout or a nack returns cleanly so the caller reconnects; transport
/// errors surface as errors.
async fn consume<T>(
    websocket: WebSocketStream<T>,
    tx: &mpsc::Sender<ManifoldEvent>,
) -> AppResult<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let (mut sink, mut stream) = websocket.split();
    let mut txid: u64 = 0;

    // The subscription must be the very first client frame; the endpoint
    // broadcasts nothing on a socket that never subscribed (probe-verified,
    // including the query-parameter spellings).
    txid += 1;
    sink.send(Message::Text(subscribe_message(txid).into()))
        .await
        .context("sending the manifold subscribe frame")?;

    match tokio::time::timeout(ACK_TIMEOUT, subscribe_ack(&mut stream, txid)).await {
        Ok(AckState::Granted) => {
            tracing::info!(
                topics = ?default_topics(),
                txid,
                "manifold subscription acknowledged"
            );
        }
        Ok(AckState::Rejected(error)) => {
            tracing::warn!(error = %error, "manifold rejected the subscription; reconnecting");
            return Ok(());
        }
        Ok(AckState::Lost(why)) => {
            tracing::warn!(reason = %why, "manifold socket lost before the ack; reconnecting");
            return Ok(());
        }
        Err(_elapsed) => {
            tracing::warn!(
                timeout_millis = ACK_TIMEOUT.as_millis() as u64,
                "manifold did not ack the subscription in time; reconnecting"
            );
            return Ok(());
        }
    }

    let mut keepalive = tokio::time::interval(KEEPALIVE_INTERVAL);
    let mut parse_warn = RateLimited::new(BAD_PAYLOAD_LOG_INTERVAL);
    let mut full_warn = RateLimited::new(CHANNEL_FULL_LOG_INTERVAL);

    let result: AppResult<()> = loop {
        tokio::select! {
            // The interval's first tick fires immediately, pinging once
            // right after the ack; the server counts it as a lastSeen
            // refresh, so the extra frame costs nothing.
            _ = keepalive.tick() => {
                txid = txid.wrapping_add(1);
                if let Err(error) = sink.send(Message::Text(ping_message(txid).into())).await {
                    break Err(error).context("sending the manifold keepalive ping");
                }
            }
            message = stream.next() => match message {
                None => break Ok(()),
                Some(Err(error)) => break Err(error).context("reading a manifold message"),
                Some(Ok(Message::Text(text))) => {
                    if let Err(error) =
                        dispatch_frame(text.as_str(), tx, &mut parse_warn, &mut full_warn).await
                    {
                        break Err(error);
                    }
                }
                Some(Ok(Message::Close(_))) => break Ok(()),
                // Ping, Pong, and Binary frames carry no broadcast data.
                Some(Ok(_)) => {}
            }
        }
    };

    // Best-effort close handshake; the peer may already be gone.
    let _ = sink.close().await;
    result
}

/// Outcome of waiting for the subscribe ack.
enum AckState {
    /// `success: true` for the txid we sent.
    Granted,
    /// `success: false` for the txid we sent; carries the server's error.
    Rejected(String),
    /// The socket errored or ended before an ack could arrive.
    Lost(String),
}

/// Reads frames until the ack for `wanted_txid` arrives.
///
/// Frames other than that ack (early broadcasts, pongs, …) are consumed and
/// discarded: the connection restarts on reconnect anyway, so losing the
/// few events that arrive before an ack is the same cost every reconnect
/// already accounts for (`crate::ingest` documents the no-replay policy).
async fn subscribe_ack<T>(
    stream: &mut futures_util::stream::SplitStream<WebSocketStream<T>>,
    wanted_txid: u64,
) -> AckState
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        match stream.next().await {
            None => return AckState::Lost("the stream ended before the subscribe ack".to_owned()),
            Some(Err(error)) => return AckState::Lost(format!("websocket error: {error}")),
            Some(Ok(Message::Close(_))) => {
                return AckState::Lost("the socket closed before the subscribe ack".to_owned());
            }
            Some(Ok(Message::Text(text))) => {
                let Ok(value) = serde_json::from_str::<Value>(text.as_str()) else {
                    continue;
                };
                if value.get("type").and_then(Value::as_str) != Some("ack") {
                    continue;
                }
                if value.get("txid").and_then(Value::as_u64) != Some(wanted_txid) {
                    // An ack for some other client message; keep waiting.
                    continue;
                }
                if value.get("success").and_then(Value::as_bool) == Some(true) {
                    return AckState::Granted;
                }
                let error = value
                    .get("error")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown error")
                    .to_owned();
                return AckState::Rejected(error);
            }
            Some(Ok(_)) => {}
        }
    }
}

/// Exponential reconnect delay: 1s, 2s, 4s, ... capped at 60s.
fn backoff_delay(attempt: u32) -> Duration {
    let seconds = 1_u64.checked_shl(attempt).unwrap_or(u64::MAX);
    Duration::from_secs(seconds.min(MAX_BACKOFF.as_secs()))
}

// -----------------------------------------------------------------------------
// Frame parsing
// -----------------------------------------------------------------------------

/// Verdict for one raw text frame.
enum ParsedFrame {
    /// Connection housekeeping or a topic this module does not queue: skip
    /// silently.
    Skip,
    /// Events to queue, possibly none (an accepted but empty batch).
    Events(Vec<ManifoldEvent>),
    /// A subscribed topic whose payload could not be parsed; report once
    /// with a sample, then drop.
    Bad,
}

/// Parses one frame and queues its events on `tx`.
///
/// Parse failures never touch the connection: the frame is logged (rate
/// limited) and dropped. Delivery failures do end the loop: a dropped
/// receiver means no app is left, so returning the error lets the runner
/// stop instead of reconnecting to nobody.
async fn dispatch_frame(
    raw: &str,
    tx: &mpsc::Sender<ManifoldEvent>,
    parse_warn: &mut RateLimited,
    full_warn: &mut RateLimited,
) -> AppResult<()> {
    match parse_frame(raw) {
        ParsedFrame::Skip => Ok(()),
        ParsedFrame::Bad => {
            if parse_warn.ready() {
                tracing::debug!(
                    sample = %sample_of(raw),
                    "dropping unparseable manifold broadcast frame"
                );
            }
            Ok(())
        }
        ParsedFrame::Events(events) => {
            for event in events {
                // A bounded mpsc `send` blocks while the buffer is full, so
                // a slow consumer slows the socket loop instead of losing
                // events (`docs/ALGO.md` §6.1: manifold events are never
                // dropped).
                if tx.capacity() == 0 && full_warn.ready() {
                    tracing::warn!(
                        "manifold event channel is full; the socket loop is now blocking on sends"
                    );
                }
                tx.send(event).await.context("queueing a manifold event")?;
            }
            Ok(())
        }
    }
}

/// Parses one raw text frame into its queued events.
fn parse_frame(raw: &str) -> ParsedFrame {
    let Ok(value) = serde_json::from_str::<Value>(raw) else {
        // Non-JSON frames were never observed in the probe; treat them as
        // housekeeping noise worth one rate-limited log line at most.
        return ParsedFrame::Bad;
    };
    if value.get("type").and_then(Value::as_str) != Some("broadcast") {
        // Acks and anything else are connection housekeeping.
        return ParsedFrame::Skip;
    }

    let topic = match value.get("topic") {
        Some(Value::String(topic)) => topic.as_str(),
        _ => return ParsedFrame::Bad,
    };
    let data = value.get("data").unwrap_or(&Value::Null);

    match topic {
        "global/new-bet" => {
            let Some(bets) = data.get("bets").and_then(Value::as_array) else {
                return ParsedFrame::Bad;
            };
            // One bet failing to parse loses only that bet; a batch where
            // every bet is malformed is a shape change worth reporting.
            let events: Vec<ManifoldEvent> = bets
                .iter()
                .filter_map(|bet| serde_json::from_value::<BetEvent>(bet.clone()).ok())
                .map(ManifoldEvent::Bet)
                .collect();
            if !bets.is_empty() && events.is_empty() {
                return ParsedFrame::Bad;
            }
            ParsedFrame::Events(events)
        }
        "global/updated-contract" => {
            let Some(broadcast_time) = data.get("broadcastTime").and_then(Value::as_i64) else {
                return ParsedFrame::Bad;
            };
            let Some(contract) = data.get("contract") else {
                return ParsedFrame::Bad;
            };
            let Some(probs) = contract_probs(contract) else {
                return ParsedFrame::Bad;
            };
            let Some(contract_id) = contract.get("id").and_then(Value::as_str) else {
                return ParsedFrame::Bad;
            };
            ParsedFrame::Events(vec![ManifoldEvent::ContractUpdate(ContractUpdateEvent {
                contract_id: contract_id.to_owned(),
                broadcast_time,
                probs,
            })])
        }
        "global/new-contract" => match serde_json::from_value::<NewContractEvent>(data.clone()) {
            Ok(event) => ParsedFrame::Events(vec![ManifoldEvent::NewContract(event)]),
            Err(_) => ParsedFrame::Bad,
        },
        // Per-market topics and the other global feeds (comments, subsidy)
        // are not subscribed; if one slips through, skip without ceremony.
        _ => ParsedFrame::Skip,
    }
}

/// Reads the probability payload from one partial contract patch.
///
/// Structural detection (documented on [`ContractProbs`]): an `answers`
/// array marks a multi market, otherwise a top-level `prob` marks a binary
/// one. Neither present is an unparseable patch.
fn contract_probs(contract: &Value) -> Option<ContractProbs> {
    if let Some(answers) = contract.get("answers").and_then(Value::as_array) {
        let parsed: Vec<AnswerProb> = answers
            .iter()
            .filter_map(|answer| {
                Some(AnswerProb {
                    id: answer.get("id")?.as_str()?.to_owned(),
                    prob: answer.get("prob")?.as_f64()?,
                })
            })
            .collect();
        if !answers.is_empty() && parsed.is_empty() {
            // Every answer was malformed: a shape change, not a market
            // with no answers.
            return None;
        }
        return Some(ContractProbs::Multi { answers: parsed });
    }
    let prob = contract.get("prob").and_then(Value::as_f64)?;
    Some(ContractProbs::Binary { prob })
}

/// The subscribe frame: the first client message after connect.
///
/// Serialization of a literal txid and a static string slice cannot fail,
/// hence the `expect`.
fn subscribe_message(txid: u64) -> String {
    serde_json::to_string(&json!({
        "type": "subscribe",
        "txid": txid,
        "topics": default_topics(),
    }))
    .expect("a subscribe frame is always serializable")
}

/// The keepalive frame the server's idle sweeper measures connection life
/// by. Cleanup by construction: the same argument holds as above.
fn ping_message(txid: u64) -> String {
    serde_json::to_string(&json!({
        "type": "ping",
        "txid": txid,
    }))
    .expect("a ping frame is always serializable")
}

/// First [`BAD_PAYLOAD_SAMPLE_BYTES`] bytes of a raw frame, for debug logs.
fn sample_of(raw: &str) -> String {
    if raw.len() <= BAD_PAYLOAD_SAMPLE_BYTES {
        return raw.to_owned();
    }
    let mut end = BAD_PAYLOAD_SAMPLE_BYTES;
    while !raw.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &raw[..end])
}

/// A log throttle that allows exactly one statement per `interval`.
struct RateLimited {
    interval: Duration,
    last: Option<Instant>,
}

impl RateLimited {
    /// Builds a throttle with no statement recorded; its first call is due.
    fn new(interval: Duration) -> Self {
        Self {
            interval,
            last: None,
        }
    }

    /// Reports whether a log statement is due now, recording it if so.
    fn ready(&mut self) -> bool {
        let now = Instant::now();
        let due = self
            .last
            .is_none_or(|last| now.saturating_duration_since(last) >= self.interval);
        if due {
            self.last = Some(now);
        }
        due
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Drains the events one raw frame produces, on a fresh channel.
    async fn events(raw: &str) -> Vec<ManifoldEvent> {
        let (tx, mut rx) = mpsc::channel(CHANNEL_CAPACITY);
        let mut parse_warn = RateLimited::new(BAD_PAYLOAD_LOG_INTERVAL);
        let mut full_warn = RateLimited::new(CHANNEL_FULL_LOG_INTERVAL);
        dispatch_frame(raw, &tx, &mut parse_warn, &mut full_warn)
            .await
            .expect("the test receiver stays alive");
        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }
        events
    }

    /// Live sample: a two-bet batch from `global/new-bet`, trimmed from
    /// `/tmp/opencode/ws_probe/samples_slash.json` (a YES fill plus a
    /// redemption bet on a multiple-choice market). Fill arrays, fees,
    /// `silent`, `visibility`, and `loanAmount` are payload fields the
    /// struct does not mirror on purpose.
    const BET_BATCH_FRAME: &str = r#"{
        "type": "broadcast",
        "topic": "global/new-bet",
        "data": {
            "bets": [
                {
                    "id": "z9nsUc5OqtdP",
                    "userId": "aXZxvzETIsavm5XbMYvfa3ov4FE2",
                    "isApi": false,
                    "silent": false,
                    "betGroupId": "ncA2zO8ICAgA",
                    "orderAmount": 25,
                    "amount": 25,
                    "shares": 28.14989784768854,
                    "isFilled": true,
                    "fills": [
                        {"matchedBetId": null, "shares": 10.928878687249352, "amount": 9.705959558861451, "timestamp": 1793996509211},
                        {"matchedBetId": null, "amount": 15.294040441138549, "shares": 17.221019160439187, "timestamp": 1793996509211}
                    ],
                    "contractId": "qLqPPtnyu8",
                    "outcome": "YES",
                    "isCancelled": false,
                    "loanAmount": 0,
                    "answerId": "SQLtL0u0IP",
                    "probBefore": 0.8867252147685656,
                    "probAfter": 0.889464234829119,
                    "createdTime": 1793996509211,
                    "isRedemption": false,
                    "visibility": "public"
                },
                {
                    "id": "lpyZPhNRcugR",
                    "userId": "aXZxvzETIsavm5XbMYvfa3ov4FE2",
                    "isApi": false,
                    "betGroupId": "ncA2zO8ICAgA",
                    "contractId": "qLqPPtnyu8",
                    "outcome": "NO",
                    "orderAmount": 0,
                    "isCancelled": false,
                    "amount": 0,
                    "loanAmount": 0,
                    "shares": 0,
                    "answerId": "Ndntu2ydyC",
                    "isFilled": true,
                    "probBefore": 0.052364151442258415,
                    "probAfter": 0.050872133367511035,
                    "createdTime": 1793996509211,
                    "isRedemption": true,
                    "visibility": "public"
                }
            ]
        }
    }"#;

    /// Live sample: an unplaced limit order (`isFilled: false`, a nonzero
    /// `orderAmount`, `limitProb`, `expiresAt`, and a duplicate `betId`
    /// alias the struct ignores). Source: `samples_slash.json`, third bet.
    const LIMIT_ORDER_FRAME: &str = r#"{
        "type": "broadcast",
        "topic": "global/new-bet",
        "data": {
            "bets": [
                {
                    "id": "9ILn26ESZZpn",
                    "fills": [],
                    "isApi": true,
                    "amount": 0,
                    "shares": 0,
                    "userId": "Fxsi2an7IoMsNLyq1ODqsXZIMqC3",
                    "outcome": "YES",
                    "answerId": "IhLA85c8nL",
                    "isFilled": false,
                    "expiresAt": 1794000124231,
                    "limitProb": 0.07,
                    "probAfter": 0.1,
                    "contractId": "52qAgzUUUs",
                    "loanAmount": 0,
                    "probBefore": 0.1,
                    "visibility": "public",
                    "createdTime": 1793996524231,
                    "isCancelled": false,
                    "orderAmount": 8.5,
                    "isRedemption": false,
                    "betId": "9ILn26ESZZpn",
                    "updatedTime": 1793996524219
                }
            ]
        }
    }"#;

    /// Live sample: a multiple-choice price patch from
    /// `global/updated-contract` (answers carry the probabilities; the
    /// contract carries no top-level `prob`). Trimmed to two answers.
    const MULTI_CONTRACT_FRAME: &str = r#"{
        "type": "broadcast",
        "topic": "global/updated-contract",
        "data": {
            "contract": {
                "id": "qLqPPtnyu8",
                "lastBetTime": 1793996509211,
                "volume": 7759.776023341293,
                "lastUpdatedTime": 1793996509211,
                "uniqueBettorCount": 11,
                "answers": [
                    {
                        "id": "SQLtL0u0IP",
                        "text": "7500",
                        "prob": 0.889464234829119,
                        "poolYes": 88.130690883381,
                        "probChanges": {"day": 0.012, "week": 0.012, "month": 0.742}
                    },
                    {
                        "id": "Ndntu2ydyC",
                        "text": "8000",
                        "prob": 0.050872133367511035,
                        "poolNo": 57.87854722936545,
                        "probChanges": {"day": -0.006, "week": -0.006, "month": -0.750}
                    }
                ]
            },
            "broadcastTime": 1793996509256
        }
    }"#;

    /// Live sample: a binary price patch (top-level `prob`, `pool`, and no
    /// `answers`). Source: `frames_probe2.jsonl`, contract `AZCP6AlgPI`.
    const BINARY_CONTRACT_FRAME: &str = r#"{
        "type": "broadcast",
        "topic": "global/updated-contract",
        "data": {
            "contract": {
                "id": "AZCP6AlgPI",
                "lastBetTime": 1793997113739,
                "volume": 18454.95565262676,
                "lastUpdatedTime": 1793997113739,
                "uniqueBettorCount": 91,
                "pool": {"YES": 375.49393702938715, "NO": 2663.158845948933},
                "p": 0.500000000000001,
                "prob": 0.8764274947329298
            },
            "broadcastTime": 1793997113766
        }
    }"#;

    #[tokio::test]
    async fn parses_a_bet_batch_into_one_event_per_bet() {
        let events = events(BET_BATCH_FRAME).await;

        assert_eq!(events.len(), 2);
        let ManifoldEvent::Bet(bet) = &events[0] else {
            panic!("first event should be a bet");
        };
        assert_eq!(bet.id, "z9nsUc5OqtdP");
        assert_eq!(bet.user_id, "aXZxvzETIsavm5XbMYvfa3ov4FE2");
        assert_eq!(bet.contract_id, "qLqPPtnyu8");
        assert_eq!(bet.answer_id.as_deref(), Some("SQLtL0u0IP"));
        assert_eq!(bet.outcome, "YES");
        assert!((bet.order_amount - 25.0).abs() < 1e-9);
        assert!((bet.prob_after - 0.889_464_234_829_119).abs() < 1e-12);
        assert!(bet.is_filled);
        assert!(!bet.is_cancelled);
        assert!(!bet.is_redemption);
        assert_eq!(bet.created_time, 1_793_996_509_211);
        assert_eq!(bet.limit_prob, None, "a fill bet carries no limit");
        assert_eq!(bet.expires_at, None);
        assert_eq!(bet.updated_time, None, "the live batch omits it");

        // The redemption bet: a zero-amount NO side with no limit fields.
        let ManifoldEvent::Bet(bet) = &events[1] else {
            panic!("second event should be a bet");
        };
        assert_eq!(bet.outcome, "NO");
        assert_eq!(bet.answer_id.as_deref(), Some("Ndntu2ydyC"));
        assert_eq!(bet.amount, 0.0);
        assert_eq!(bet.shares, 0.0);
        assert!(bet.is_redemption);
        assert_eq!(bet.limit_prob, None);
        assert_eq!(bet.expires_at, None);
    }

    #[tokio::test]
    async fn parses_a_resting_limit_order() {
        let evs = events(LIMIT_ORDER_FRAME).await;

        assert_eq!(evs.len(), 1);
        let ManifoldEvent::Bet(bet) = &evs[0] else {
            panic!("should be a bet");
        };
        assert_eq!(bet.id, "9ILn26ESZZpn");
        assert_eq!(bet.limit_prob, Some(0.07));
        assert_eq!(bet.order_amount, 8.5);
        assert_eq!(bet.expires_at, Some(1_794_000_124_231));
        assert_eq!(bet.updated_time, Some(1_793_996_524_219));
        assert!(!bet.is_filled);
    }

    #[tokio::test]
    async fn parses_a_multi_contract_update() {
        let evs = events(MULTI_CONTRACT_FRAME).await;

        let ManifoldEvent::ContractUpdate(update) = &evs[0] else {
            panic!("should be a contract update");
        };
        assert_eq!(update.contract_id, "qLqPPtnyu8");
        assert_eq!(update.broadcast_time, 1_793_996_509_256);
        let ContractProbs::Multi { answers } = &update.probs else {
            panic!("answers present implies the multi shape");
        };
        assert_eq!(answers.len(), 2);
        assert_eq!(answers[0].id, "SQLtL0u0IP");
        assert!((answers[0].prob - 0.889_464_234_829_119).abs() < 1e-12);
        assert_eq!(answers[1].id, "Ndntu2ydyC");
        assert!((answers[1].prob - 0.050_872_133_367_511_035).abs() < 1e-12);
    }

    #[tokio::test]
    async fn parses_a_binary_contract_update() {
        let evs = events(BINARY_CONTRACT_FRAME).await;

        let ManifoldEvent::ContractUpdate(update) = &evs[0] else {
            panic!("should be a contract update");
        };
        assert_eq!(update.contract_id, "AZCP6AlgPI");
        assert_eq!(update.broadcast_time, 1_793_997_113_766);
        let ContractProbs::Binary { prob } = &update.probs else {
            panic!("no answers implies the binary shape");
        };
        assert!((prob - 0.876_427_494_732_929_8).abs() < 1e-12);
    }

    #[tokio::test]
    async fn unknown_fields_are_ignored() {
        // Extended from the live batch: unknown keys at every level of the
        // frame (top level, data level, and bet level).
        let raw = r#"{"type":"broadcast","worker":"w1","topic":"global/new-bet",
            "data":{"page":2,"bets":[{
                "id":"x","userId":"u","contractId":"c","outcome":"YES",
                "amount":1,"orderAmount":1,"shares":1,"probBefore":0.5,
                "probAfter":0.6,"isFilled":true,"isCancelled":false,
                "isRedemption":false,"createdTime":5,"silent":true,
                "betGroupId":"g","visibility":"private"}]}}"#;
        let evs = events(raw).await;

        let ManifoldEvent::Bet(bet) = &evs[0] else {
            panic!("should be a bet");
        };
        assert_eq!(bet.id, "x");
        assert_eq!(bet.limit_prob, None);
    }

    #[tokio::test]
    async fn a_bet_without_required_fields_is_dropped() {
        // The payload survives, the malformed bets do not; the loop itself
        // must never stop over one bad frame.
        let raw = r#"{"type":"broadcast","topic":"global/new-bet","data":{"bets":[
            {"id":"abc","userId":"u","contractId":"c","outcome":"YES"}
        ]}}"#;
        assert_eq!(events(raw).await, Vec::new());
    }

    #[tokio::test]
    async fn acks_and_foreign_topics_are_skipped_silently() {
        // Acks arrive once per connection and carry no data.
        assert!(
            events(r#"{"type":"ack","txid":1,"success":true}"#)
                .await
                .is_empty()
        );
        // Topics this module does not queue, including a per-market one.
        assert!(
            events(r#"{"type":"broadcast","topic":"global/new-comment","data":{"comment":{}}}"#)
                .await
                .is_empty()
        );
        assert!(
            events(r#"{"type":"broadcast","topic":"contract/abc/orders","data":{}}"#)
                .await
                .is_empty()
        );
    }

    #[test]
    fn an_unsubscribed_broadcast_frame_is_skipped_cleanly() {
        // One guard on shape drift in the topic table.
        assert!(matches!(
            parse_frame(r#"{"type":"broadcast","topic":"global/new-x","data":{}}"#),
            ParsedFrame::Skip
        ));
    }

    #[test]
    fn a_known_topic_with_a_broken_payload_is_reported_bad() {
        assert!(matches!(
            parse_frame(r#"{"type":"broadcast","topic":"global/new-bet","data":{}}"#),
            ParsedFrame::Bad
        ));
        assert!(matches!(
            parse_frame(r#"{"type":"broadcast","topic":"global/updated-contract","data":{}}"#),
            ParsedFrame::Bad
        ));
    }

    #[tokio::test]
    async fn non_json_frames_never_stop_the_loop() {
        assert_eq!(events("not json").await, Vec::new());
        assert_eq!(events("").await, Vec::new());
    }

    #[test]
    fn ping_frame_serialization_shape() {
        let value: Value =
            serde_json::from_str(&ping_message(7)).expect("a ping frame is valid json");
        let object = value.as_object().expect("a ping frame is an object");
        assert_eq!(object.len(), 2, "no extra keys sneak in");
        assert_eq!(value["type"], "ping");
        assert_eq!(value["txid"], 7);
    }

    #[test]
    fn subscribe_frame_serialization_shape() {
        let value: Value =
            serde_json::from_str(&subscribe_message(1)).expect("a subscribe frame is json");
        assert_eq!(value["type"], "subscribe");
        assert_eq!(value["txid"], 1);
        assert_eq!(value["topics"], json!(default_topics()));
    }

    #[test]
    fn backoff_starts_at_one_second() {
        assert_eq!(backoff_delay(0), Duration::from_secs(1));
    }

    #[test]
    fn backoff_doubles_per_attempt_and_is_capped() {
        assert_eq!(backoff_delay(3), Duration::from_secs(8));
        assert_eq!(backoff_delay(10), Duration::from_secs(60));
        assert_eq!(backoff_delay(u32::MAX), Duration::from_secs(60));
    }

    #[test]
    fn the_rate_limiter_allows_one_statement_per_interval() {
        let mut throttle = RateLimited::new(Duration::from_secs(1));
        assert!(throttle.ready(), "the first statement is due");
        assert!(!throttle.ready(), "the second is not, inside one interval");
    }

    #[test]
    fn bad_payload_samples_stay_char_safe() {
        // `héllo` spans two bytes per character; the cut must respect that.
        let long = "héllo ".repeat(80);
        let cut = sample_of(&long);
        assert!(cut.len() <= BAD_PAYLOAD_SAMPLE_BYTES + "…".len());
        assert!(cut.ends_with('…'));
        assert_eq!(sample_of("a short payload"), "a short payload");
    }
}
