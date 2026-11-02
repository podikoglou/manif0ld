//! Post-to-signal orchestration.
//!
//! The pipeline connects ingestion to the belief engine:
//!
//! ```text
//! ExtractedPost ──► Sieve match ──► per-market bounded queue
//!        ──► LLM triage ──► belief update ──► signal ledger ──► event stream
//! ```
//!
//! Each market has its own queue and one worker task. A worker processes its
//! market's jobs sequentially so belief updates follow post order. The queue is
//! a `tokio::sync::broadcast` channel of capacity 30: when a market saturates,
//! the oldest job is overwritten and the worker observes `RecvError::Lagged`.
//! This is the bounded, drop-oldest behaviour from the specification; an
//! unbounded channel would let one busy market consume memory without limit.
//!
//! [`PipelineEvent`] is the extension point for the Discord layer: the pipeline
//! never talks to Discord itself, it publishes fully-formed signal and error
//! events that a later consumer can render as embeds.
//!
//! # Locking
//!
//! The market cache and the queue map use `std::sync` locks. A guard is never
//! held across an `.await`: every method extracts owned data (`TrackedMarket`,
//! `QueueHandle`, sender clone) and drops the guard before doing async work.
//! Locks recover from poisoning because a panic in one task must not take the
//! whole signal path down.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

use chrono::Utc;
use tokio::sync::broadcast;

use crate::belief;
use crate::config::Config;
use crate::db::Db;
use crate::error::AppResult;
use crate::sieve::EngineRegistry;
use crate::triage::{self, TriageClient};
use crate::types::{
    Direction, ExtractedPost, MarketId, MarketStatus, SignalRecord, TrackedMarket,
};

/// Number of jobs one market's triage queue buffers before dropping the oldest.
const QUEUE_CAPACITY: usize = 30;

/// Capacity of the pipeline event stream consumed by Discord later.
const EVENT_CAPACITY: usize = 1024;

/// Minimum gap between saturation warnings for one market.
const QUEUE_LAG_WARN_INTERVAL: Duration = Duration::from_secs(30);

/// Snapshot of every tracked market.
///
/// The cache exists because the hot matching path needs market state without a
/// SQLite query per post. It is shared as `Arc<MarketCache>` and refreshed with
/// [`MarketCache::upsert`], [`MarketCache::remove`], or [`MarketCache::reload`]
/// when control-plane commands change a market.
pub struct MarketCache {
    markets: RwLock<HashMap<MarketId, TrackedMarket>>,
}

impl MarketCache {
    /// Loads every market row from the database.
    pub fn load(db: &Db) -> AppResult<Self> {
        Ok(Self {
            markets: RwLock::new(Self::snapshot(db)?),
        })
    }

    /// Returns a clone of one cached market.
    pub fn get(&self, id: &str) -> Option<TrackedMarket> {
        self.read().get(id).cloned()
    }

    /// Returns every active market, sorted by ID for deterministic callers.
    pub fn active_markets(&self) -> Vec<TrackedMarket> {
        let markets = self.read();
        let mut active: Vec<TrackedMarket> = markets
            .values()
            .filter(|market| market.status == MarketStatus::Active)
            .cloned()
            .collect();
        active.sort_by(|left, right| left.id.cmp(&right.id));
        active
    }

    /// Inserts or replaces one market.
    ///
    /// Used both when a market is onboarded and when the trading or control
    /// plane layer persists position, probability, or status changes.
    pub fn upsert(&self, market: TrackedMarket) {
        self.write().insert(market.id.clone(), market);
    }

    /// Removes one market from the cache. Missing IDs are ignored.
    pub fn remove(&self, id: &str) {
        self.write().remove(id);
    }

    /// Replaces the whole cache with a fresh database snapshot.
    pub fn reload(&self, db: &Db) -> AppResult<()> {
        let snapshot = Self::snapshot(db)?;
        *self.write() = snapshot;
        Ok(())
    }

    /// Updates the belief fields of one cached market after a successful write.
    ///
    /// Keeping the cache in step matters because queued jobs read the current
    /// belief state when they start, not the state at enqueue time.
    fn record_belief(&self, id: &str, log_odds: f64, p_market: f64, updated_at: i64) {
        let mut markets = self.write();
        if let Some(market) = markets.get_mut(id) {
            market.p_internal_log_odds = log_odds;
            market.p_market_last = p_market;
            market.belief_updated_at = updated_at;
        }
    }

    fn snapshot(db: &Db) -> AppResult<HashMap<MarketId, TrackedMarket>> {
        let markets = db.list_markets()?;
        Ok(markets
            .into_iter()
            .map(|market| (market.id.clone(), market))
            .collect())
    }

    fn read(&self) -> RwLockReadGuard<'_, HashMap<MarketId, TrackedMarket>> {
        self.markets
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write(&self) -> RwLockWriteGuard<'_, HashMap<MarketId, TrackedMarket>> {
        self.markets
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// One item queued for a market's triage worker.
#[derive(Debug, Clone)]
struct TriageJob {
    /// Market state at enqueue time. The worker refreshes the belief fields from
    /// the cache so sequential jobs never start from a stale snapshot.
    market: TrackedMarket,
    post: ExtractedPost,
}

/// Per-market queue handle: sender, queue depth, and saturation warning clock.
#[derive(Clone)]
struct QueueHandle {
    sender: broadcast::Sender<TriageJob>,
    depth: QueueDepth,
    last_lag_warn: Arc<Mutex<Option<Instant>>>,
}

/// Atomic counter for the number of jobs waiting in one market's queue.
///
/// Kept separate from the channel so `/status` can read depth without locking
/// the queue map for long, and so the accounting is unit-testable without a
/// network or a running worker.
#[derive(Debug, Clone, Default)]
struct QueueDepth {
    depth: Arc<AtomicUsize>,
}

impl QueueDepth {
    fn new() -> Self {
        Self::default()
    }

    /// Records a successful enqueue and returns the new depth.
    fn added(&self) -> usize {
        self.depth.fetch_add(1, Ordering::AcqRel).saturating_add(1)
    }

    /// Records one job taken by the worker and returns the new depth.
    fn consumed(&self) -> usize {
        self.decrement(1)
    }

    /// Records `dropped` jobs overwritten by a saturated queue.
    fn lagged(&self, dropped: usize) -> usize {
        self.decrement(dropped)
    }

    /// Current number of queued jobs.
    fn get(&self) -> usize {
        self.depth.load(Ordering::Acquire)
    }

    fn decrement(&self, amount: usize) -> usize {
        let mut current = self.depth.load(Ordering::Acquire);
        loop {
            let next = current.saturating_sub(amount);
            match self.depth.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return next,
                Err(actual) => current = actual,
            }
        }
    }
}

/// Events published for downstream consumers such as the Discord layer.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum PipelineEvent {
    /// A post matched a market's Sieve rule and is being sent to LLM triage.
    PostMatched {
        market_id: MarketId,
        post_uri: String,
        author_handle: String,
    },
    /// A post was triaged for a market. Emitted for non-material posts too, so
    /// the signal ledger and future Discord embeds stay complete.
    Signal {
        market_id: MarketId,
        question: String,
        post_uri: String,
        author_handle: String,
        author_did: String,
        text: String,
        direction: Direction,
        strength: u8,
        rationale: String,
        material: bool,
        impulse: f64,
        p_before: f64,
        p_after: f64,
        p_market: f64,
        created_at: i64,
    },
    /// Triage, belief persistence, queue, or ledger failure.
    Error {
        market_id: Option<MarketId>,
        message: String,
    },
}

/// Orchestrates post matching, triage, belief updates, and ledger writes.
pub struct Pipeline {
    pub config: Arc<Config>,
    pub db: Db,
    pub registry: Arc<EngineRegistry>,
    pub cache: Arc<MarketCache>,
    triage: TriageClient,
    events: broadcast::Sender<PipelineEvent>,
    queues: Mutex<HashMap<MarketId, QueueHandle>>,
}

impl Pipeline {
    /// Builds the pipeline, its triage client, and the event broadcast channel.
    pub fn new(
        config: Arc<Config>,
        db: Db,
        registry: Arc<EngineRegistry>,
        cache: Arc<MarketCache>,
    ) -> AppResult<Self> {
        let triage = TriageClient::new(&config)?;
        let (events, _) = broadcast::channel(EVENT_CAPACITY);
        Ok(Self {
            config,
            db,
            registry,
            cache,
            triage,
            events,
            queues: Mutex::new(HashMap::new()),
        })
    }

    /// Returns a sender that can be subscribed to for pipeline events.
    pub fn events(&self) -> broadcast::Sender<PipelineEvent> {
        self.events.clone()
    }

    /// Number of jobs waiting for one market.
    pub fn queue_depth(&self, market_id: &str) -> usize {
        self.queues()
            .get(market_id)
            .map(|handle| handle.depth.get())
            .unwrap_or(0)
    }

    /// Queue depth for every market that has a live queue.
    pub fn queue_depths(&self) -> HashMap<MarketId, usize> {
        self.queues()
            .iter()
            .map(|(market_id, handle)| (market_id.clone(), handle.depth.get()))
            .collect()
    }

    /// Consumes extracted posts until the ingest channel closes.
    ///
    /// Post-stream lag is logged and skipped: those posts were already dropped
    /// by the ingest broadcast channel, so there is nothing to replay.
    pub async fn run(
        self: Arc<Self>,
        mut posts: broadcast::Receiver<ExtractedPost>,
    ) -> AppResult<()> {
        loop {
            match posts.recv().await {
                Ok(post) => self.dispatch(post),
                Err(broadcast::error::RecvError::Lagged(dropped)) => {
                    tracing::warn!(
                        dropped,
                        "post stream lagged; oldest posts were dropped by ingest"
                    );
                }
                Err(broadcast::error::RecvError::Closed) => return Ok(()),
            }
        }
    }

    /// Matches one post against every engine and enqueues triage jobs.
    ///
    /// The extracted text is passed to Sieve unchanged. Markets absent from the
    /// cache or not active are skipped, matching the control-plane semantics of
    /// paused and closed markets.
    fn dispatch(self: &Arc<Self>, post: ExtractedPost) {
        let market_ids = self.registry.matching_markets(&post.text);

        for market_id in market_ids {
            let Some(market) = self.cache.get(&market_id) else {
                continue;
            };
            if market.status != MarketStatus::Active {
                continue;
            }

            let job = TriageJob {
                market,
                post: post.clone(),
            };
            self.enqueue(market_id, job);
        }
    }

    /// Sends one job to a market's queue, recreating a dead worker once.
    fn enqueue(self: &Arc<Self>, market_id: MarketId, job: TriageJob) {
        let handle = self.queue_handle(&market_id);
        match Self::try_send(&handle, job) {
            Ok(()) => {}
            Err(job) => {
                tracing::warn!(
                    market_id = %market_id,
                    "triage worker is gone; recreating it once"
                );
                let handle = self.replace_queue(&market_id);
                if Self::try_send(&handle, *job).is_err() {
                    self.emit(PipelineEvent::Error {
                        market_id: Some(market_id),
                        message: "triage queue unavailable after worker restart".to_owned(),
                    });
                }
            }
        }
    }

    /// Enqueues a job while keeping depth accounting race-free.
    ///
    /// Depth is incremented *before* the send because a worker on another thread
    /// can receive the job as soon as `send` publishes it. On a failed send the
    /// increment is rolled back and the job is returned to the caller.
    fn try_send(handle: &QueueHandle, job: TriageJob) -> Result<(), Box<TriageJob>> {
        handle.depth.added();
        match handle.sender.send(job) {
            Ok(_) => Ok(()),
            Err(broadcast::error::SendError(job)) => {
                handle.depth.consumed();
                Err(Box::new(job))
            }
        }
    }

    fn queue_handle(self: &Arc<Self>, market_id: &MarketId) -> QueueHandle {
        let mut queues = self.queues();
        if let Some(handle) = queues.get(market_id) {
            return handle.clone();
        }

        let handle = self.spawn_worker(market_id);
        queues.insert(market_id.clone(), handle.clone());
        handle
    }

    fn replace_queue(self: &Arc<Self>, market_id: &MarketId) -> QueueHandle {
        let handle = self.spawn_worker(market_id);
        self.queues().insert(market_id.clone(), handle.clone());
        handle
    }

    fn spawn_worker(self: &Arc<Self>, market_id: &MarketId) -> QueueHandle {
        let (sender, receiver) = broadcast::channel(QUEUE_CAPACITY);
        let depth = QueueDepth::new();
        let last_lag_warn = Arc::new(Mutex::new(None));

        let pipeline = Arc::clone(self);
        let worker_market_id = market_id.clone();
        let worker_depth = depth.clone();
        let worker_warn = Arc::clone(&last_lag_warn);
        // Dropping the join handle detaches the worker; its lifetime is tied to
        // the queue sender held in `self.queues`.
        drop(tokio::spawn(async move {
            pipeline
                .worker_loop(worker_market_id, receiver, worker_depth, worker_warn)
                .await;
        }));

        QueueHandle {
            sender,
            depth,
            last_lag_warn,
        }
    }

    /// Runs one market's queue sequentially.
    async fn worker_loop(
        self: Arc<Self>,
        market_id: MarketId,
        mut receiver: broadcast::Receiver<TriageJob>,
        depth: QueueDepth,
        last_lag_warn: Arc<Mutex<Option<Instant>>>,
    ) {
        loop {
            match receiver.recv().await {
                Ok(job) => {
                    depth.consumed();
                    self.process(job).await;
                }
                Err(broadcast::error::RecvError::Lagged(dropped)) => {
                    let dropped = usize::try_from(dropped).unwrap_or(usize::MAX);
                    depth.lagged(dropped);
                    warn_queue_lag(&market_id, dropped, &last_lag_warn);
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    }

    /// Triages one post and applies the resulting belief update.
    ///
    /// A triage failure leaves belief untouched. A belief persistence failure
    /// also leaves the cached and persisted state untouched, but the triage row
    /// is still written and the signal event is still emitted so the failure is
    /// visible downstream.
    async fn process(&self, job: TriageJob) {
        let TriageJob { market, post } = job;

        // Refresh belief fields from the cache: several jobs for one market may
        // have been enqueued before the first one finished, and their snapshots
        // would otherwise overwrite each other's belief updates.
        let market = self.cache.get(&market.id).unwrap_or(market);

        self.emit(PipelineEvent::PostMatched {
            market_id: market.id.clone(),
            post_uri: post.uri.clone(),
            author_handle: post.handle.clone(),
        });

        let now = Utc::now().timestamp();
        let p_market = market.p_market_last;
        let kappa = self.config.belief_kappa;
        let halflife = self.config.belief_halflife_hours;
        let p_before = belief::current_p_internal(
            market.p_internal_log_odds,
            market.belief_updated_at,
            p_market,
            now,
            halflife,
        );

        let answers = self.db.list_answers(&market.id).unwrap_or_default();
        let result = match self.triage.evaluate(&market, &answers, &post, None, p_market).await {
            Ok(result) => result,
            Err(error) => {
                self.emit(PipelineEvent::Error {
                    market_id: Some(market.id.clone()),
                    message: format!("triage failed: {error}"),
                });
                return;
            }
        };

        let material = triage::is_material(&result);

        if !answers.is_empty() {
            // Multiple-choice market processing
            for ans_triage in &result.affected_answers {
                if ans_triage.direction == Direction::Neutral || ans_triage.strength == 0 {
                    continue;
                }
                if let Ok(Some(ans)) = self.db.get_answer(&market.id, &ans_triage.answer_id) {
                    let mut log_odds = ans.p_internal_log_odds;
                    let mut updated_at = ans.belief_updated_at;
                    let ans_p_market = ans.p_market_last;
                    let applied_impulse = belief::apply_post(
                        &mut log_odds,
                        &mut updated_at,
                        ans_triage.direction,
                        ans_triage.strength,
                        ans_p_market,
                        now,
                        kappa,
                        halflife,
                    );
                    let ans_p_after = belief::sigmoid(log_odds);
                    let _ = self.db.update_answer_belief(&market.id, &ans.id, log_odds, ans_p_market, updated_at);

                    let record = SignalRecord {
                        id: 0,
                        market_id: market.id.clone(),
                        answer_id: Some(ans.id.clone()),
                        post_uri: post.uri.clone(),
                        author_handle: post.handle.clone(),
                        post_text: post.text.clone(),
                        material: true,
                        direction: ans_triage.direction,
                        strength: ans_triage.strength,
                        impulse: applied_impulse,
                        p_after: ans_p_after,
                        created_at: now,
                    };
                    let _ = self.db.insert_signal(&record);
                }
            }

            self.emit(PipelineEvent::Signal {
                market_id: market.id,
                question: market.question,
                post_uri: post.uri,
                author_handle: post.handle,
                author_did: post.did,
                text: post.text,
                direction: result.direction.unwrap_or(Direction::Neutral),
                strength: result.strength,
                rationale: result.rationale,
                material,
                impulse: 0.0,
                p_before,
                p_after: p_before,
                p_market,
                created_at: now,
            });
            return;
        }

        let direction = result.direction.unwrap_or(Direction::Neutral);
        let mut impulse = 0.0;
        let mut p_after = p_before;

        if material {
            let mut log_odds = market.p_internal_log_odds;
            let mut updated_at = market.belief_updated_at;
            let applied_impulse = belief::apply_post(
                &mut log_odds,
                &mut updated_at,
                direction,
                result.strength,
                p_market,
                now,
                kappa,
                halflife,
            );
            let candidate_after = belief::sigmoid(log_odds);

            match self
                .db
                .update_belief(&market.id, log_odds, p_market, updated_at)
            {
                Ok(()) => {
                    impulse = applied_impulse;
                    p_after = candidate_after;
                    self.cache
                        .record_belief(&market.id, log_odds, p_market, updated_at);
                }
                Err(error) => {
                    self.emit(PipelineEvent::Error {
                        market_id: Some(market.id.clone()),
                        message: format!("failed to persist belief update: {error}"),
                    });
                }
            }
        }

        let record = SignalRecord {
            id: 0,
            market_id: market.id.clone(),
            answer_id: None,
            post_uri: post.uri.clone(),
            author_handle: post.handle.clone(),
            post_text: post.text.clone(),
            material,
            direction,
            strength: result.strength,
            impulse,
            p_after,
            created_at: now,
        };

        if let Err(error) = self.db.insert_signal(&record) {
            self.emit(PipelineEvent::Error {
                market_id: Some(market.id.clone()),
                message: format!("failed to write signal ledger: {error}"),
            });
        }

        self.emit(PipelineEvent::Signal {
            market_id: market.id,
            question: market.question,
            post_uri: post.uri,
            author_handle: post.handle,
            author_did: post.did,
            text: post.text,
            direction,
            strength: result.strength,
            rationale: result.rationale,
            material,
            impulse,
            p_before,
            p_after,
            p_market,
            created_at: now,
        });
    }

    fn emit(&self, event: PipelineEvent) {
        // No subscribers is normal before the Discord consumer starts.
        let _ = self.events.send(event);
    }

    fn queues(&self) -> MutexGuard<'_, HashMap<MarketId, QueueHandle>> {
        self.queues
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Logs a saturation warning no more often than [`QUEUE_LAG_WARN_INTERVAL`].
fn warn_queue_lag(market_id: &str, dropped: usize, last_warn: &Mutex<Option<Instant>>) {
    let now = Instant::now();
    let should_warn = {
        let mut guard = last_warn
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match *guard {
            Some(previous) if now.saturating_duration_since(previous) < QUEUE_LAG_WARN_INTERVAL => {
                false
            }
            _ => {
                *guard = Some(now);
                true
            }
        }
    };

    if should_warn {
        tracing::warn!(
            market_id = %market_id,
            dropped,
            "triage queue saturated; dropped oldest jobs"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_market(id: &str, status: MarketStatus, created_at: i64) -> TrackedMarket {
        TrackedMarket {
            id: id.to_owned(),
            slug: format!("slug-{id}"),
            market_url: String::new(),
            question: format!("Question {id}?"),
            description: "Description".to_owned(),
            resolution_criteria: "Resolves YES if the thing happens.".to_owned(),
            close_time: Some(created_at + 86_400),
            status,
            budget_mana: 100.0,
            invested_mana: 0.0,
            realized_pnl_mana: 0.0,
            yes_shares: 0.0,
            no_shares: 0.0,
            p_internal_log_odds: 0.0,
            p_market_last: 0.5,
            belief_updated_at: created_at,
            sieve_source: "match Market { \"thing\" }".to_owned(),
            discord_thread_id: String::new(),
            created_at,
        }
    }

    fn market_ids(markets: &[TrackedMarket]) -> Vec<&str> {
        markets.iter().map(|market| market.id.as_str()).collect()
    }

    #[test]
    fn load_copies_markets() {
        let db = Db::open_in_memory().expect("in-memory DB");
        db.insert_market(&sample_market("m1", MarketStatus::Active, 1))
            .expect("insert m1");
        db.insert_market(&sample_market("m2", MarketStatus::Paused, 2))
            .expect("insert m2");

        let cache = MarketCache::load(&db).expect("load cache");

        assert_eq!(cache.get("m1").expect("cached m1").question, "Question m1?");
        assert_eq!(
            cache.get("m2").expect("cached m2").status,
            MarketStatus::Paused
        );
        assert!(cache.get("missing").is_none());

        assert_eq!(
            market_ids(&cache.active_markets()),
            vec!["m1"],
            "paused markets are not active"
        );
    }

    #[test]
    fn upsert_and_remove_update_the_cache() {
        let db = Db::open_in_memory().expect("in-memory DB");
        let cache = MarketCache::load(&db).expect("load cache");
        assert!(cache.get("m3").is_none());

        cache.upsert(sample_market("m3", MarketStatus::Active, 3));

        assert_eq!(
            cache.get("m3").expect("upserted market").status,
            MarketStatus::Active
        );
        assert_eq!(market_ids(&cache.active_markets()), vec!["m3"]);

        cache.remove("m3");
        assert!(cache.get("m3").is_none());
    }

    #[test]
    fn upsert_replaces_state() {
        let db = Db::open_in_memory().expect("in-memory DB");
        let cache = MarketCache::load(&db).expect("load cache");
        cache.upsert(sample_market("m1", MarketStatus::Active, 1));
        cache.upsert(sample_market("m2", MarketStatus::Active, 2));

        let mut updated = cache.get("m1").expect("cached m1");
        updated.yes_shares = 12.5;
        updated.invested_mana = 8.0;
        updated.p_market_last = 0.61;
        cache.upsert(updated);

        let cached = cache.get("m1").expect("updated m1");
        assert_eq!(cached.yes_shares, 12.5);
        assert_eq!(cached.invested_mana, 8.0);
        assert_eq!(cached.p_market_last, 0.61);

        // Upserting an unknown market inserts it.
        let mut fresh = sample_market("m3", MarketStatus::Paused, 3);
        fresh.no_shares = 4.0;
        cache.upsert(fresh);
        assert_eq!(cache.get("m3").expect("inserted m3").no_shares, 4.0);
    }

    #[test]
    fn reload_replaces_the_whole_snapshot() {
        let db = Db::open_in_memory().expect("in-memory DB");
        db.insert_market(&sample_market("m1", MarketStatus::Active, 1))
            .expect("insert m1");
        let cache = MarketCache::load(&db).expect("load cache");
        assert!(cache.get("m1").is_some());

        db.insert_market(&sample_market("m2", MarketStatus::Active, 2))
            .expect("insert m2");
        cache.reload(&db).expect("reload cache");

        assert!(cache.get("m2").is_some());

        db.delete_market("m1").expect("delete m1");
        cache.reload(&db).expect("reload after delete");
        assert!(cache.get("m1").is_none());
    }

    #[test]
    fn queue_depth_tracks_adds_consumes_and_lag() {
        let depth = QueueDepth::new();
        assert_eq!(depth.get(), 0);

        assert_eq!(depth.added(), 1);
        assert_eq!(depth.added(), 2);
        assert_eq!(depth.added(), 3);
        assert_eq!(depth.get(), 3);

        assert_eq!(depth.consumed(), 2);
        assert_eq!(depth.get(), 2);

        // A lagged worker is told how many oldest jobs were overwritten.
        assert_eq!(depth.lagged(1), 1);
        assert_eq!(depth.get(), 1);

        // Accounting saturates at zero instead of wrapping.
        assert_eq!(depth.lagged(99), 0);
        assert_eq!(depth.get(), 0);
    }
}
