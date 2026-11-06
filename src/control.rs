//! One service for the operator control plane.
//!
//! Every mutating Discord command calls one method here. The service owns the
//! ordering of the steps (sell, write the lifecycle transition, retire the
//! engine, archive the thread), and returns a small report of facts. Rendering
//! stays in the adapter.
//!
//! # Seams
//!
//! Three ports keep the service in-process and testable:
//!
//! - [`Threads`] is the Discord thread lifecycle; its real adapter lives in
//!   `discord.rs` so this module never imports the Discord client crate.
//! - [`Onboarder`] is the onboarding transaction, implemented for
//!   [`OnboardingService`].
//! - [`Trading`] is the slice of [`TradeExecutor`] the control plane needs.
//!
//! Two teardown end states exist by API: [`ControlPlane::untrack`] soft-deletes
//! the row (`CLOSED` + `deleted_at`), [`ControlPlane::close`] keeps it visible
//! (`CLOSED`, `deleted_at` NULL). Both go through one private body so the
//! ordering cannot drift.
//!
//! # Ordering
//!
//! `End::Hide` mirrors `/untrack`: unregister the engine, then write the row
//! and propagate a failure, then archive. `End::Keep` mirrors the Liquidate
//! button: write the row and log a failure, unregister, then archive. The
//! divergence is deliberate and recorded in `docs/plans/control-plane.md` §10.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::mpsc;

use crate::belief;
use crate::db::Db;
use crate::error::{bail, AppResult, Context};
use crate::market_store::MarketStore;
use crate::onboarding::{OnboardUpdate, OnboardingDraft, OnboardingService};
use crate::sieve::EngineRegistry;
use crate::trade::{LiquidationOutcome, TradeExecutor, KILL_SWITCH_KEY};
use crate::types::{MarketId, MarketStatus, TrackedMarket};

/// The public thread that belongs to one market.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ThreadId(pub u64);

/// Thread lifecycle, and nothing else.
///
/// The control plane owns the *ordering*; the adapter owns the Discord API and
/// the thread name. Archive is best effort: the adapter logs a failure, never
/// raises.
#[async_trait]
pub trait Threads: Send + Sync {
    /// Creates the market thread and returns its ID.
    async fn create_for_market(&self, budget: f64, question: &str) -> AppResult<ThreadId>;

    /// Archives a thread. Failures are the adapter's to log.
    async fn archive(&self, thread: ThreadId);
}

/// The onboarding transaction. `prepare` is read-only; `activate` is the only
/// writer.
#[async_trait]
pub trait Onboarder: Send + Sync {
    async fn prepare(
        &self,
        url: &str,
        budget: f64,
        initial_prob: Option<u8>,
        updates: mpsc::Sender<OnboardUpdate>,
    ) -> AppResult<OnboardingDraft>;

    fn activate(&self, draft: OnboardingDraft, thread_id: &str) -> AppResult<TrackedMarket>;
}

/// The trading layer, as the control plane needs it.
#[async_trait]
pub trait Trading: Send + Sync {
    /// One trade check, no trigger post.
    async fn evaluate(&self, market_id: &str) -> AppResult<()>;

    async fn liquidate(&self, market_id: &str) -> AppResult<LiquidationOutcome>;

    async fn liquidate_all(&self) -> Vec<(MarketId, anyhow::Error)>;
}

/// The onboarding service is the real [`Onboarder`].
#[async_trait]
impl Onboarder for OnboardingService {
    async fn prepare(
        &self,
        url: &str,
        budget: f64,
        initial_prob: Option<u8>,
        updates: mpsc::Sender<OnboardUpdate>,
    ) -> AppResult<OnboardingDraft> {
        OnboardingService::prepare(self, url, budget, initial_prob, &updates).await
    }

    fn activate(&self, draft: OnboardingDraft, thread_id: &str) -> AppResult<TrackedMarket> {
        OnboardingService::activate(self, draft, thread_id)
    }
}

/// The trade executor is the real [`Trading`] adapter.
#[async_trait]
impl Trading for TradeExecutor {
    async fn evaluate(&self, market_id: &str) -> AppResult<()> {
        self.evaluate_market(market_id, None).await.map(|_| ())
    }

    async fn liquidate(&self, market_id: &str) -> AppResult<LiquidationOutcome> {
        TradeExecutor::liquidate(self, market_id).await
    }

    async fn liquidate_all(&self) -> Vec<(MarketId, anyhow::Error)> {
        TradeExecutor::liquidate_all(self).await
    }
}

/// A prepared market that is now live, with its thread.
#[derive(Debug)]
pub struct Onboarded {
    /// The draft is kept for the `[ONBOARDED]` embed.
    pub draft: OnboardingDraft,
    pub thread: ThreadId,
}

/// What happened to the position during a teardown.
#[derive(Debug)]
pub enum Sale {
    /// The sale ran and the executor reported shares sold.
    Liquidated,
    /// `/untrack liquidate:false` on a live market.
    Untouched,
    /// The row was already closed, so nothing was attempted.
    AlreadyClosed,
    /// Manifold resolved the market; positions settle automatically.
    Resolved,
    /// The market held no shares.
    NoPosition,
    /// The sale failed. The text is `Display` of the executor error.
    Failed(String),
}

/// What `/untrack` found and did.
#[derive(Debug)]
pub enum UntrackReport {
    AlreadyUntracked {
        question: String,
        market_id: String,
    },
    Untracked {
        question: String,
        market_id: String,
        sale: Sale,
    },
}

/// What the Liquidate button found and did.
#[derive(Debug)]
pub enum CloseReport {
    NotTracked {
        market_id: String,
    },
    AlreadyUntracked {
        question: String,
        market_id: String,
    },
    AlreadyClosed {
        question: String,
        market_id: String,
    },
    /// The executor saw `AlreadyClosed`; nothing was closed, unregistered, or
    /// archived.
    ClosedByAnotherAction {
        question: String,
        market_id: String,
    },
    Closed {
        question: String,
        market_id: String,
        sale: Sale,
    },
}

/// One `/budget` change.
#[derive(Debug)]
pub struct BudgetChange {
    pub question: String,
    pub market_id: String,
    pub before: f64,
    pub after: f64,
}

/// One `/override` belief write.
#[derive(Debug)]
pub struct ProbabilityChange {
    pub question: String,
    pub market_id: String,
    pub log_odds: f64,
    pub clamped_p: f64,
}

/// One `/panic` run.
#[derive(Debug)]
pub struct PanicReport {
    /// `None` when `liquidate_all` was false.
    pub attempted: Option<usize>,
    pub liquidated: usize,
    /// `(market_id, Display of the error)`, untruncated.
    pub errors: Vec<(String, String)>,
}

/// Which end state a teardown leaves behind.
enum End {
    /// `/untrack`: `CLOSED` + `deleted_at`, hidden from every listing.
    Hide,
    /// Liquidate button: `CLOSED`, row visible.
    Keep,
}

/// The single writer of operator-driven lifecycle transitions.
pub struct ControlPlane {
    db: Db,
    store: Arc<MarketStore>,
    registry: Arc<EngineRegistry>,
    onboarding: Arc<dyn Onboarder>,
    trading: Arc<dyn Trading>,
}

impl ControlPlane {
    pub fn new(
        db: Db,
        store: Arc<MarketStore>,
        registry: Arc<EngineRegistry>,
        onboarding: Arc<dyn Onboarder>,
        trading: Arc<dyn Trading>,
    ) -> Self {
        Self {
            db,
            store,
            registry,
            onboarding,
            trading,
        }
    }

    /// `/track`. prepare → duplicate check → thread → activate → archive on
    /// failure. The embed is posted by the caller.
    ///
    /// `updates` is taken by value on purpose: the caller awaits the progress
    /// updater as soon as this returns, and an open sender would keep that
    /// updater waiting forever.
    pub async fn track(
        &self,
        url: &str,
        budget: f64,
        initial_prob: Option<u8>,
        updates: mpsc::Sender<OnboardUpdate>,
        threads: &dyn Threads,
    ) -> AppResult<Onboarded> {
        let draft = self
            .onboarding
            .prepare(url, budget, initial_prob, updates)
            .await?;

        if let Some(existing) = self.store.load(&draft.market.id)? {
            if existing.status != MarketStatus::Closed {
                bail!(
                    "market {} is already tracked (see /status or /untrack)",
                    draft.market.id
                );
            }
        }

        let thread = threads
            .create_for_market(budget, &draft.market.question)
            .await?;

        if let Err(error) = self
            .onboarding
            .activate(draft.clone(), &thread.0.to_string())
        {
            threads.archive(thread).await;
            return Err(error).context("activating the prepared draft");
        }

        Ok(Onboarded { draft, thread })
    }

    /// `/untrack`. Sell when asked, then `End::Hide`.
    pub async fn untrack(
        &self,
        query: &str,
        liquidate: bool,
        threads: &dyn Threads,
    ) -> AppResult<UntrackReport> {
        let market = self.resolve_any(query)?;
        let market_id = market.id.clone();

        if market.is_deleted() {
            return Ok(UntrackReport::AlreadyUntracked {
                question: market.question,
                market_id,
            });
        }

        let sale = self.sell(&market, liquidate).await;
        self.teardown(&market, End::Hide, threads).await?;

        Ok(UntrackReport::Untracked {
            question: market.question,
            market_id,
            sale,
        })
    }

    /// The Liquidate button. Sell, then `End::Keep`.
    pub async fn close(&self, market_id: &str, threads: &dyn Threads) -> AppResult<CloseReport> {
        let Some(market) = self.store.load(market_id)? else {
            return Ok(CloseReport::NotTracked {
                market_id: market_id.to_owned(),
            });
        };
        let question = market.question.clone();

        if market.is_deleted() {
            return Ok(CloseReport::AlreadyUntracked {
                question,
                market_id: market_id.to_owned(),
            });
        }
        if market.status == MarketStatus::Closed {
            return Ok(CloseReport::AlreadyClosed {
                question,
                market_id: market_id.to_owned(),
            });
        }

        let outcome = self.trading.liquidate(market_id).await;
        if let Err(error) = &outcome {
            tracing::error!(market_id = %market_id, error = %error, "liquidation failed");
        }
        if matches!(outcome, Ok(LiquidationOutcome::AlreadyClosed)) {
            // Another action closed the market while this one waited for the
            // execution lock. Do not repeat the cleanup or the archiving.
            return Ok(CloseReport::ClosedByAnotherAction {
                question,
                market_id: market_id.to_owned(),
            });
        }

        self.teardown(&market, End::Keep, threads).await?;

        let sale = match outcome {
            Ok(LiquidationOutcome::Sold) => Sale::Liquidated,
            Ok(LiquidationOutcome::AlreadyResolved) => Sale::Resolved,
            Ok(LiquidationOutcome::NoPosition) => Sale::NoPosition,
            Ok(LiquidationOutcome::AlreadyClosed) => Sale::AlreadyClosed,
            Err(error) => Sale::Failed(error.to_string()),
        };

        Ok(CloseReport::Closed {
            question,
            market_id: market_id.to_owned(),
            sale,
        })
    }

    /// The one teardown body. The end state and its failure order are data.
    async fn teardown(
        &self,
        market: &TrackedMarket,
        end: End,
        threads: &dyn Threads,
    ) -> AppResult<()> {
        match end {
            End::Hide => {
                self.registry.unregister(&market.id);
                self.store.untrack(&market.id, Utc::now().timestamp())?;
            }
            End::Keep => {
                if let Err(error) = self.store.close(&market.id) {
                    tracing::error!(
                        market_id = %market.id,
                        error = %error,
                        "failed to close the market"
                    );
                }
                self.registry.unregister(&market.id);
            }
        }

        if let Ok(channel_id) = market.discord_thread_id.trim().parse::<u64>() {
            threads.archive(ThreadId(channel_id)).await;
        }
        Ok(())
    }

    /// The sale step shared by both teardown intents.
    async fn sell(&self, market: &TrackedMarket, want_sale: bool) -> Sale {
        if market.status == MarketStatus::Closed {
            return Sale::AlreadyClosed;
        }
        if !want_sale {
            return Sale::Untouched;
        }

        match self.trading.liquidate(&market.id).await {
            Ok(_) => Sale::Liquidated,
            Err(error) => {
                tracing::error!(
                    market_id = %market.id,
                    error = %error,
                    "liquidation during /untrack failed"
                );
                Sale::Failed(error.to_string())
            }
        }
    }

    pub fn set_budget(&self, query: &str, new_budget: f64) -> AppResult<BudgetChange> {
        let market = self.resolve_tracked(query)?;
        let before = market.budget_mana;
        self.store.record_budget(&market.id, new_budget)?;
        Ok(BudgetChange {
            question: market.question,
            market_id: market.id,
            before,
            after: new_budget,
        })
    }

    pub fn set_probability(&self, query: &str, probability: f64) -> AppResult<ProbabilityChange> {
        let market = self.resolve_tracked(query)?;
        let (log_odds, clamped_p) = belief::override_probability(probability);
        let now = Utc::now().timestamp();
        // Only the belief columns move. `p_market_last` belongs to the trade
        // sync, so the next decay uses whatever price the market last reported.
        self.store.record_belief(&market.id, log_odds, now)?;
        Ok(ProbabilityChange {
            question: market.question,
            market_id: market.id,
            log_odds,
            clamped_p,
        })
    }

    /// Toggles ACTIVE <-> PAUSED. Errors carry the operator text verbatim.
    pub fn set_paused(&self, market_id: &str) -> AppResult<MarketStatus> {
        let Some(market) = self.store.load(market_id)? else {
            bail!("market {market_id} is not tracked");
        };
        if market.is_deleted() {
            bail!("market {market_id} was untracked and cannot be resumed");
        }
        if market.status == MarketStatus::Closed {
            bail!("market {market_id} is closed and cannot be resumed");
        }
        let new_status = match market.status {
            MarketStatus::Active => MarketStatus::Paused,
            MarketStatus::Paused => MarketStatus::Active,
            MarketStatus::Closed => bail!("market {market_id} is closed and cannot be resumed"),
        };
        self.store.record_status(market_id, new_status)?;
        Ok(new_status)
    }

    /// One trade check. Logs instead of raising; the caller spawns it.
    pub async fn check_now(&self, market_id: &str) {
        if let Err(error) = self.trading.evaluate(market_id).await {
            tracing::error!(
                market_id = %market_id,
                error = %error,
                "trade check after /override failed"
            );
        }
    }

    /// `/panic`. Sets the kill switch, then sweeps when asked.
    pub async fn panic(&self, liquidate_all: bool) -> AppResult<PanicReport> {
        self.db
            .set_kv(KILL_SWITCH_KEY, "true")
            .context("setting the global kill switch")?;

        if !liquidate_all {
            return Ok(PanicReport {
                attempted: None,
                liquidated: 0,
                errors: Vec::new(),
            });
        }

        let errors = self.trading.liquidate_all().await;
        let attempted = self
            .store
            .list()?
            .iter()
            .filter(|market| market.status != MarketStatus::Closed)
            .count();
        let liquidated = attempted.saturating_sub(errors.len());

        Ok(PanicReport {
            attempted: Some(attempted),
            liquidated,
            errors: errors
                .into_iter()
                .map(|(market_id, error)| (market_id, error.to_string()))
                .collect(),
        })
    }

    /// Resolves a reference against every tracked (not soft-deleted) row.
    fn resolve_tracked(&self, query: &str) -> AppResult<TrackedMarket> {
        let markets = self.store.list()?;
        let Some(market) = resolve_market(&markets, query) else {
            bail!("no unique tracked market matches {query:?}");
        };
        Ok(market.clone())
    }

    /// Resolves a reference against every row, including soft-deleted markets.
    ///
    /// `/untrack` needs this so a repeated call reports "already untracked"
    /// instead of claiming the market never existed.
    fn resolve_any(&self, query: &str) -> AppResult<TrackedMarket> {
        let markets = self.store.list_including_deleted()?;
        let Some(market) = resolve_market(&markets, query) else {
            bail!("no tracked or untracked market matches {query:?}");
        };
        Ok(market.clone())
    }
}

/// Resolves an operator-provided market reference.
///
/// The market ID or slug must match case-insensitively. A unique slug prefix is
/// accepted as a convenience. Ambiguous or missing references resolve to
/// `None` rather than guessing.
pub(crate) fn resolve_market<'a>(
    markets: &'a [TrackedMarket],
    query: &str,
) -> Option<&'a TrackedMarket> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return None;
    }
    let needle = trimmed.to_lowercase();
    let url_slug = crate::onboarding::parse_slug(trimmed)
        .or_else(|_| crate::onboarding::parse_slug(&format!("https://{trimmed}")))
        .ok()
        .map(|s| s.to_lowercase());

    let exact: Vec<&TrackedMarket> = markets
        .iter()
        .filter(|market| {
            let id = market.id.to_lowercase();
            let slug = market.slug.to_lowercase();
            id == needle
                || slug == needle
                || url_slug.as_deref() == Some(&slug)
                || url_slug.as_deref() == Some(&id)
        })
        .collect();
    if exact.len() == 1 {
        return exact.first().copied();
    }
    if !exact.is_empty() {
        return None;
    }

    let prefixes: Vec<&TrackedMarket> = markets
        .iter()
        .filter(|market| {
            let slug = market.slug.to_lowercase();
            slug.starts_with(&needle)
                || url_slug
                    .as_deref()
                    .is_some_and(|url_s| slug.starts_with(url_s))
        })
        .collect();
    if prefixes.len() == 1 {
        return prefixes.first().copied();
    }

    None
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use super::*;

    const TEST_THREAD: ThreadId = ThreadId(4_242);
    const SIEVE: &str = r#"match Market { "thing" }"#;

    fn market(id: &str, question: &str, slug: &str, status: MarketStatus) -> TrackedMarket {
        TrackedMarket {
            id: id.to_owned(),
            slug: slug.to_owned(),
            market_url: String::new(),
            question: question.to_owned(),
            description: String::new(),
            resolution_criteria: String::new(),
            close_time: None,
            status,
            budget_mana: 100.0,
            invested_mana: 0.0,
            realized_pnl_mana: 0.0,
            yes_shares: 0.0,
            no_shares: 0.0,
            p_internal_log_odds: 0.0,
            p_market_last: 0.5,
            belief_updated_at: 0,
            sieve_source: SIEVE.to_owned(),
            discord_thread_id: "12345".to_owned(),
            created_at: 0,
            deleted_at: None,
            outcome_type: "BINARY".to_owned(),
            should_answers_sum_to_one: false,
            strategy: crate::types::STRATEGY_NEWS.to_owned(),
        }
    }

    fn draft(market: TrackedMarket) -> OnboardingDraft {
        OnboardingDraft {
            market,
            sieve_source: SIEVE.to_owned(),
            answers: Vec::new(),
        }
    }

    fn store_with(markets: &[TrackedMarket]) -> (Db, Arc<MarketStore>) {
        let db = Db::open_in_memory().expect("in-memory DB");
        let store = Arc::new(MarketStore::new(&db).expect("store loads"));
        for market in markets {
            store.insert(market).expect("insert market");
        }
        (db, store)
    }

    fn registry_with(ids: &[&str]) -> Arc<EngineRegistry> {
        let registry = Arc::new(EngineRegistry::new());
        for id in ids {
            registry
                .register_source((*id).to_owned(), SIEVE)
                .expect("register engine");
        }
        registry
    }

    #[derive(Default)]
    struct RecordingThreads {
        created: Mutex<Vec<(f64, String)>>,
        archived: Mutex<Vec<ThreadId>>,
        fail_create: bool,
    }

    impl RecordingThreads {
        fn created_calls(&self) -> Vec<(f64, String)> {
            self.created.lock().expect("lock").clone()
        }

        fn archived_threads(&self) -> Vec<ThreadId> {
            self.archived.lock().expect("lock").clone()
        }
    }

    #[async_trait]
    impl Threads for RecordingThreads {
        async fn create_for_market(&self, budget: f64, question: &str) -> AppResult<ThreadId> {
            if self.fail_create {
                bail!("thread creation failed");
            }
            self.created
                .lock()
                .expect("lock")
                .push((budget, question.to_owned()));
            Ok(TEST_THREAD)
        }

        async fn archive(&self, thread: ThreadId) {
            self.archived.lock().expect("lock").push(thread);
        }
    }

    struct ScriptedOnboarder {
        draft: OnboardingDraft,
        fail_activate: bool,
        activated_thread: Mutex<Option<String>>,
    }

    impl ScriptedOnboarder {
        fn new(draft: OnboardingDraft) -> Self {
            Self {
                draft,
                fail_activate: false,
                activated_thread: Mutex::new(None),
            }
        }

        fn failing(draft: OnboardingDraft) -> Self {
            Self {
                fail_activate: true,
                ..Self::new(draft)
            }
        }

        fn activated_thread(&self) -> Option<String> {
            self.activated_thread.lock().expect("lock").clone()
        }
    }

    #[async_trait]
    impl Onboarder for ScriptedOnboarder {
        async fn prepare(
            &self,
            _url: &str,
            _budget: f64,
            _initial_prob: Option<u8>,
            updates: mpsc::Sender<OnboardUpdate>,
        ) -> AppResult<OnboardingDraft> {
            drop(updates);
            Ok(self.draft.clone())
        }

        fn activate(&self, draft: OnboardingDraft, thread_id: &str) -> AppResult<TrackedMarket> {
            *self.activated_thread.lock().expect("lock") = Some(thread_id.to_owned());
            if self.fail_activate {
                bail!("activation failed");
            }
            let mut market = draft.market;
            market.discord_thread_id = thread_id.to_owned();
            Ok(market)
        }
    }

    #[derive(Clone, Debug)]
    enum ScriptedLiquidation {
        Sold,
        Resolved,
        NoPosition,
        AlreadyClosed,
        Failed(String),
    }

    struct RecordingTrading {
        evaluated: Mutex<Vec<String>>,
        liquidated: Mutex<Vec<String>>,
        sweeps: AtomicUsize,
        liquidation: Mutex<ScriptedLiquidation>,
        sweep_errors: Mutex<Vec<String>>,
    }

    impl RecordingTrading {
        fn new(outcome: ScriptedLiquidation) -> Self {
            Self {
                evaluated: Mutex::new(Vec::new()),
                liquidated: Mutex::new(Vec::new()),
                sweeps: AtomicUsize::new(0),
                liquidation: Mutex::new(outcome),
                sweep_errors: Mutex::new(Vec::new()),
            }
        }

        fn with_sweep_error(self, market_id: &str) -> Self {
            self.sweep_errors
                .lock()
                .expect("lock")
                .push(market_id.to_owned());
            self
        }

        fn evaluated_ids(&self) -> Vec<String> {
            self.evaluated.lock().expect("lock").clone()
        }

        fn liquidated_ids(&self) -> Vec<String> {
            self.liquidated.lock().expect("lock").clone()
        }

        fn sweeps(&self) -> usize {
            self.sweeps.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl Trading for RecordingTrading {
        async fn evaluate(&self, market_id: &str) -> AppResult<()> {
            self.evaluated
                .lock()
                .expect("lock")
                .push(market_id.to_owned());
            Ok(())
        }

        async fn liquidate(&self, market_id: &str) -> AppResult<LiquidationOutcome> {
            self.liquidated
                .lock()
                .expect("lock")
                .push(market_id.to_owned());
            let script = self.liquidation.lock().expect("lock").clone();
            match script {
                ScriptedLiquidation::Sold => Ok(LiquidationOutcome::Sold),
                ScriptedLiquidation::Resolved => Ok(LiquidationOutcome::AlreadyResolved),
                ScriptedLiquidation::NoPosition => Ok(LiquidationOutcome::NoPosition),
                ScriptedLiquidation::AlreadyClosed => Ok(LiquidationOutcome::AlreadyClosed),
                ScriptedLiquidation::Failed(message) => bail!("{message}"),
            }
        }

        async fn liquidate_all(&self) -> Vec<(MarketId, anyhow::Error)> {
            self.sweeps.fetch_add(1, Ordering::SeqCst);
            self.sweep_errors
                .lock()
                .expect("lock")
                .iter()
                .map(|id| (id.clone(), anyhow::anyhow!("liquidation failed for {id}")))
                .collect()
        }
    }

    fn control(
        db: Db,
        store: Arc<MarketStore>,
        registry: Arc<EngineRegistry>,
        onboarder: Arc<ScriptedOnboarder>,
        trading: Arc<RecordingTrading>,
    ) -> ControlPlane {
        ControlPlane::new(db, store, registry, onboarder, trading)
    }

    fn sender() -> mpsc::Sender<OnboardUpdate> {
        mpsc::channel(1).0
    }

    #[tokio::test]
    async fn track_creates_the_thread_and_passes_it_to_activate() {
        let (db, store) = store_with(&[]);
        let registry = registry_with(&[]);
        let onboarder = Arc::new(ScriptedOnboarder::new(draft(market(
            "M1",
            "Will it rain?",
            "will-it-rain",
            MarketStatus::Active,
        ))));
        let trading = Arc::new(RecordingTrading::new(ScriptedLiquidation::Sold));
        let control = control(
            db.clone(),
            Arc::clone(&store),
            registry,
            Arc::clone(&onboarder),
            trading,
        );
        let threads = RecordingThreads::default();

        let onboarded = control
            .track("url", 50.0, Some(60), sender(), &threads)
            .await
            .expect("track succeeds");

        assert_eq!(onboarded.thread, TEST_THREAD);
        assert_eq!(onboarded.draft.market.id, "M1");
        assert_eq!(
            threads.created_calls(),
            vec![(50.0, "Will it rain?".to_owned())]
        );
        assert_eq!(onboarder.activated_thread(), Some("4242".to_owned()));
        assert!(threads.archived_threads().is_empty());
    }

    #[tokio::test]
    async fn track_archives_the_thread_when_activation_fails() {
        let (db, store) = store_with(&[]);
        let registry = registry_with(&[]);
        let onboarder = Arc::new(ScriptedOnboarder::failing(draft(market(
            "M1",
            "Will it rain?",
            "will-it-rain",
            MarketStatus::Active,
        ))));
        let trading = Arc::new(RecordingTrading::new(ScriptedLiquidation::Sold));
        let control = control(
            db.clone(),
            Arc::clone(&store),
            registry,
            Arc::clone(&onboarder),
            trading,
        );
        let threads = RecordingThreads::default();

        let error = control
            .track("url", 50.0, None, sender(), &threads)
            .await
            .expect_err("activation fails");

        assert!(
            format!("{error:#}").contains("activation failed"),
            "{error:?}"
        );
        assert_eq!(threads.created_calls().len(), 1);
        assert_eq!(threads.archived_threads(), vec![TEST_THREAD]);
    }

    #[tokio::test]
    async fn track_refuses_a_market_that_is_already_tracked() {
        let existing = market("M1", "Will it rain?", "will-it-rain", MarketStatus::Active);
        let (db, store) = store_with(std::slice::from_ref(&existing));
        let registry = registry_with(&[]);
        let onboarder = Arc::new(ScriptedOnboarder::new(draft(existing)));
        let trading = Arc::new(RecordingTrading::new(ScriptedLiquidation::Sold));
        let control = control(
            db.clone(),
            Arc::clone(&store),
            registry,
            Arc::clone(&onboarder),
            trading,
        );
        let threads = RecordingThreads::default();

        let error = control
            .track("url", 50.0, None, sender(), &threads)
            .await
            .expect_err("already tracked");

        assert!(error.to_string().contains("already tracked"), "{error:?}");
        assert!(threads.created_calls().is_empty());
        assert_eq!(onboarder.activated_thread(), None);
    }

    #[tokio::test]
    async fn track_replaces_a_market_that_is_closed() {
        let existing = market("M1", "Will it rain?", "will-it-rain", MarketStatus::Closed);
        let (db, store) = store_with(std::slice::from_ref(&existing));
        let registry = registry_with(&[]);
        let onboarder = Arc::new(ScriptedOnboarder::new(draft(existing)));
        let trading = Arc::new(RecordingTrading::new(ScriptedLiquidation::Sold));
        let control = control(
            db.clone(),
            Arc::clone(&store),
            registry,
            Arc::clone(&onboarder),
            trading,
        );
        let threads = RecordingThreads::default();

        let onboarded = control
            .track("url", 50.0, None, sender(), &threads)
            .await
            .expect("closed market is replaceable");

        assert_eq!(onboarded.thread, TEST_THREAD);
        assert_eq!(threads.created_calls().len(), 1);
        assert_eq!(onboarder.activated_thread(), Some("4242".to_owned()));
    }

    #[tokio::test]
    async fn untrack_hides_the_row_and_unregisters_the_engine() {
        let existing = market("M1", "Will it rain?", "will-it-rain", MarketStatus::Active);
        let (db, store) = store_with(&[existing]);
        let registry = registry_with(&["M1"]);
        let onboarder = Arc::new(ScriptedOnboarder::new(draft(market(
            "M1",
            "Will it rain?",
            "will-it-rain",
            MarketStatus::Active,
        ))));
        let trading = Arc::new(RecordingTrading::new(ScriptedLiquidation::Sold));
        let control = control(
            db.clone(),
            Arc::clone(&store),
            Arc::clone(&registry),
            onboarder,
            Arc::clone(&trading),
        );
        let threads = RecordingThreads::default();

        let report = control
            .untrack("M1", true, &threads)
            .await
            .expect("untrack succeeds");

        assert!(matches!(
            report,
            UntrackReport::Untracked {
                sale: Sale::Liquidated,
                ..
            }
        ));
        assert_eq!(trading.liquidated_ids(), vec!["M1".to_owned()]);
        let stored = store.load("M1").expect("load").expect("row");
        assert_eq!(stored.status, MarketStatus::Closed);
        assert!(stored.deleted_at.is_some());
        assert!(store.list().expect("list").is_empty());
        assert!(!registry.contains("M1"));
        assert_eq!(threads.archived_threads(), vec![ThreadId(12_345)]);
    }

    #[tokio::test]
    async fn close_keeps_the_row_visible_and_unregisters_the_engine() {
        let existing = market("M1", "Will it rain?", "will-it-rain", MarketStatus::Active);
        let (db, store) = store_with(&[existing]);
        let registry = registry_with(&["M1"]);
        let onboarder = Arc::new(ScriptedOnboarder::new(draft(market(
            "M1",
            "Will it rain?",
            "will-it-rain",
            MarketStatus::Active,
        ))));
        let trading = Arc::new(RecordingTrading::new(ScriptedLiquidation::Sold));
        let control = control(
            db.clone(),
            Arc::clone(&store),
            Arc::clone(&registry),
            onboarder,
            trading,
        );
        let threads = RecordingThreads::default();

        let report = control.close("M1", &threads).await.expect("close succeeds");

        assert!(matches!(
            report,
            CloseReport::Closed {
                sale: Sale::Liquidated,
                ..
            }
        ));
        let stored = store.load("M1").expect("load").expect("row");
        assert_eq!(stored.status, MarketStatus::Closed);
        assert_eq!(stored.deleted_at, None);
        assert_eq!(store.list().expect("list").len(), 1);
        assert!(!registry.contains("M1"));
        assert_eq!(threads.archived_threads(), vec![ThreadId(12_345)]);
    }

    #[tokio::test]
    async fn untrack_without_liquidation_sells_nothing() {
        let existing = market("M1", "Will it rain?", "will-it-rain", MarketStatus::Active);
        let (db, store) = store_with(&[existing]);
        let registry = registry_with(&["M1"]);
        let onboarder = Arc::new(ScriptedOnboarder::new(draft(market(
            "M1",
            "Will it rain?",
            "will-it-rain",
            MarketStatus::Active,
        ))));
        let trading = Arc::new(RecordingTrading::new(ScriptedLiquidation::Sold));
        let control = control(
            db.clone(),
            Arc::clone(&store),
            Arc::clone(&registry),
            onboarder,
            Arc::clone(&trading),
        );
        let threads = RecordingThreads::default();

        let report = control
            .untrack("M1", false, &threads)
            .await
            .expect("untrack succeeds");

        assert!(matches!(
            report,
            UntrackReport::Untracked {
                sale: Sale::Untouched,
                ..
            }
        ));
        assert!(trading.liquidated_ids().is_empty());
        assert_eq!(
            store.load("M1").expect("load").expect("row").status,
            MarketStatus::Closed
        );
        assert!(!registry.contains("M1"));
        assert_eq!(threads.archived_threads(), vec![ThreadId(12_345)]);
    }

    #[tokio::test]
    async fn untrack_continues_when_the_sale_fails() {
        let existing = market("M1", "Will it rain?", "will-it-rain", MarketStatus::Active);
        let (db, store) = store_with(&[existing]);
        let registry = registry_with(&["M1"]);
        let onboarder = Arc::new(ScriptedOnboarder::new(draft(market(
            "M1",
            "Will it rain?",
            "will-it-rain",
            MarketStatus::Active,
        ))));
        let trading = Arc::new(RecordingTrading::new(ScriptedLiquidation::Failed(
            "boom".to_owned(),
        )));
        let control = control(
            db.clone(),
            Arc::clone(&store),
            Arc::clone(&registry),
            onboarder,
            trading,
        );
        let threads = RecordingThreads::default();

        let report = control
            .untrack("M1", true, &threads)
            .await
            .expect("untrack continues");

        match report {
            UntrackReport::Untracked {
                sale: Sale::Failed(message),
                ..
            } => assert_eq!(message, "boom"),
            other => panic!("unexpected report: {other:?}"),
        }
        let stored = store.load("M1").expect("load").expect("row");
        assert_eq!(stored.status, MarketStatus::Closed);
        assert!(stored.deleted_at.is_some());
        assert!(!registry.contains("M1"));
        assert_eq!(threads.archived_threads(), vec![ThreadId(12_345)]);
    }

    #[tokio::test]
    async fn untrack_reports_an_already_untracked_market() {
        let mut existing = market("M1", "Will it rain?", "will-it-rain", MarketStatus::Closed);
        existing.deleted_at = Some(7);
        let (db, store) = store_with(&[existing]);
        let registry = registry_with(&["M1"]);
        let onboarder = Arc::new(ScriptedOnboarder::new(draft(market(
            "M1",
            "Will it rain?",
            "will-it-rain",
            MarketStatus::Closed,
        ))));
        let trading = Arc::new(RecordingTrading::new(ScriptedLiquidation::Sold));
        let control = control(
            db.clone(),
            Arc::clone(&store),
            Arc::clone(&registry),
            onboarder,
            Arc::clone(&trading),
        );
        let threads = RecordingThreads::default();

        let report = control
            .untrack("M1", true, &threads)
            .await
            .expect("untrack reports");

        assert!(matches!(report, UntrackReport::AlreadyUntracked { .. }));
        assert!(trading.liquidated_ids().is_empty());
        assert!(threads.archived_threads().is_empty());
        assert_eq!(
            store.load("M1").expect("load").expect("row").deleted_at,
            Some(7)
        );
        assert!(registry.contains("M1"), "nothing was unregistered");
    }

    #[tokio::test]
    async fn untrack_skips_an_empty_thread_id() {
        let mut existing = market("M1", "Will it rain?", "will-it-rain", MarketStatus::Active);
        existing.discord_thread_id = String::new();
        let (db, store) = store_with(&[existing]);
        let registry = registry_with(&["M1"]);
        let onboarder = Arc::new(ScriptedOnboarder::new(draft(market(
            "M1",
            "Will it rain?",
            "will-it-rain",
            MarketStatus::Active,
        ))));
        let trading = Arc::new(RecordingTrading::new(ScriptedLiquidation::Sold));
        let control = control(
            db.clone(),
            Arc::clone(&store),
            Arc::clone(&registry),
            onboarder,
            trading,
        );
        let threads = RecordingThreads::default();

        control
            .untrack("M1", true, &threads)
            .await
            .expect("untrack succeeds");

        assert!(store.list().expect("list").is_empty());
        assert!(threads.archived_threads().is_empty());
    }

    #[tokio::test]
    async fn close_reports_a_market_that_is_already_closed() {
        let existing = market("M1", "Will it rain?", "will-it-rain", MarketStatus::Closed);
        let (db, store) = store_with(&[existing]);
        let registry = registry_with(&["M1"]);
        let onboarder = Arc::new(ScriptedOnboarder::new(draft(market(
            "M1",
            "Will it rain?",
            "will-it-rain",
            MarketStatus::Closed,
        ))));
        let trading = Arc::new(RecordingTrading::new(ScriptedLiquidation::Sold));
        let control = control(
            db.clone(),
            Arc::clone(&store),
            Arc::clone(&registry),
            onboarder,
            Arc::clone(&trading),
        );
        let threads = RecordingThreads::default();

        let report = control.close("M1", &threads).await.expect("close reports");

        assert!(matches!(report, CloseReport::AlreadyClosed { .. }));
        assert!(trading.liquidated_ids().is_empty());
        assert!(threads.archived_threads().is_empty());
        assert!(registry.contains("M1"));
    }

    #[tokio::test]
    async fn close_does_not_archive_when_the_executor_reports_already_closed() {
        let existing = market("M1", "Will it rain?", "will-it-rain", MarketStatus::Active);
        let (db, store) = store_with(&[existing]);
        let registry = registry_with(&["M1"]);
        let onboarder = Arc::new(ScriptedOnboarder::new(draft(market(
            "M1",
            "Will it rain?",
            "will-it-rain",
            MarketStatus::Active,
        ))));
        let trading = Arc::new(RecordingTrading::new(ScriptedLiquidation::AlreadyClosed));
        let control = control(
            db.clone(),
            Arc::clone(&store),
            Arc::clone(&registry),
            onboarder,
            trading,
        );
        let threads = RecordingThreads::default();

        let report = control.close("M1", &threads).await.expect("close reports");

        assert!(matches!(report, CloseReport::ClosedByAnotherAction { .. }));
        assert_eq!(
            store.load("M1").expect("load").expect("row").status,
            MarketStatus::Active
        );
        assert!(threads.archived_threads().is_empty());
        assert!(registry.contains("M1"));
    }

    #[tokio::test]
    async fn close_reports_a_resolved_market_and_an_empty_position() {
        for (outcome, expected) in [
            (ScriptedLiquidation::Resolved, "resolved"),
            (ScriptedLiquidation::NoPosition, "empty"),
        ] {
            let existing = market("M1", "Will it rain?", "will-it-rain", MarketStatus::Active);
            let (db, store) = store_with(&[existing]);
            let registry = registry_with(&["M1"]);
            let onboarder = Arc::new(ScriptedOnboarder::new(draft(market(
                "M1",
                "Will it rain?",
                "will-it-rain",
                MarketStatus::Active,
            ))));
            let trading = Arc::new(RecordingTrading::new(outcome));
            let control = control(
                db.clone(),
                Arc::clone(&store),
                Arc::clone(&registry),
                onboarder,
                trading,
            );
            let threads = RecordingThreads::default();

            let report = control.close("M1", &threads).await.expect("close succeeds");

            match (expected, report) {
                (
                    "resolved",
                    CloseReport::Closed {
                        sale: Sale::Resolved,
                        ..
                    },
                ) => {}
                (
                    "empty",
                    CloseReport::Closed {
                        sale: Sale::NoPosition,
                        ..
                    },
                ) => {}
                (_, other) => panic!("unexpected report for {expected}: {other:?}"),
            }
            assert_eq!(
                store.load("M1").expect("load").expect("row").status,
                MarketStatus::Closed
            );
            assert_eq!(threads.archived_threads(), vec![ThreadId(12_345)]);
        }
    }

    #[tokio::test]
    async fn set_paused_toggles_between_active_and_paused() {
        let existing = market("M1", "Will it rain?", "will-it-rain", MarketStatus::Active);
        let (db, store) = store_with(&[existing]);
        let registry = registry_with(&[]);
        let onboarder = Arc::new(ScriptedOnboarder::new(draft(market(
            "M1",
            "Will it rain?",
            "will-it-rain",
            MarketStatus::Active,
        ))));
        let trading = Arc::new(RecordingTrading::new(ScriptedLiquidation::Sold));
        let control = control(db, Arc::clone(&store), registry, onboarder, trading);

        assert_eq!(
            control.set_paused("M1").expect("pause"),
            MarketStatus::Paused
        );
        assert_eq!(
            store.load("M1").expect("load").expect("row").status,
            MarketStatus::Paused
        );
        assert_eq!(
            control.set_paused("M1").expect("resume"),
            MarketStatus::Active
        );
        assert_eq!(
            store.load("M1").expect("load").expect("row").status,
            MarketStatus::Active
        );
    }

    #[tokio::test]
    async fn set_paused_rejects_a_closed_or_untracked_market() {
        let mut untracked = market("M2", "Gone?", "gone", MarketStatus::Paused);
        untracked.deleted_at = Some(3);
        let closed = market("M3", "Closed?", "closed", MarketStatus::Closed);
        let (db, store) = store_with(&[untracked, closed]);
        let registry = registry_with(&[]);
        let onboarder = Arc::new(ScriptedOnboarder::new(draft(market(
            "M1",
            "Will it rain?",
            "will-it-rain",
            MarketStatus::Active,
        ))));
        let trading = Arc::new(RecordingTrading::new(ScriptedLiquidation::Sold));
        let control = control(db, store, registry, onboarder, trading);

        assert_eq!(
            control
                .set_paused("missing")
                .expect_err("unknown")
                .to_string(),
            "market missing is not tracked"
        );
        assert_eq!(
            control.set_paused("M2").expect_err("untracked").to_string(),
            "market M2 was untracked and cannot be resumed"
        );
        assert_eq!(
            control.set_paused("M3").expect_err("closed").to_string(),
            "market M3 is closed and cannot be resumed"
        );
    }

    #[tokio::test]
    async fn set_budget_reports_the_old_and_the_new_value() {
        let existing = market("M1", "Will it rain?", "will-it-rain", MarketStatus::Active);
        let (db, store) = store_with(&[existing]);
        let registry = registry_with(&[]);
        let onboarder = Arc::new(ScriptedOnboarder::new(draft(market(
            "M1",
            "Will it rain?",
            "will-it-rain",
            MarketStatus::Active,
        ))));
        let trading = Arc::new(RecordingTrading::new(ScriptedLiquidation::Sold));
        let control = control(db, Arc::clone(&store), registry, onboarder, trading);

        let change = control.set_budget("M1", 250.0).expect("set budget");

        assert_eq!(change.question, "Will it rain?");
        assert_eq!(change.market_id, "M1");
        assert_eq!(change.before, 100.0);
        assert_eq!(change.after, 250.0);
        assert_eq!(
            store.load("M1").expect("load").expect("row").budget_mana,
            250.0
        );
    }

    #[tokio::test]
    async fn set_probability_writes_the_belief_and_leaves_the_price() {
        let mut existing = market("M1", "Will it rain?", "will-it-rain", MarketStatus::Active);
        existing.p_market_last = 0.4;
        let (db, store) = store_with(&[existing]);
        let registry = registry_with(&[]);
        let onboarder = Arc::new(ScriptedOnboarder::new(draft(market(
            "M1",
            "Will it rain?",
            "will-it-rain",
            MarketStatus::Active,
        ))));
        let trading = Arc::new(RecordingTrading::new(ScriptedLiquidation::Sold));
        let control = control(db, Arc::clone(&store), registry, onboarder, trading);

        let change = control.set_probability("M1", 0.7).expect("set belief");
        let (log_odds, clamped_p) = belief::override_probability(0.7);

        assert_eq!(change.log_odds, log_odds);
        assert_eq!(change.clamped_p, clamped_p);
        let stored = store.load("M1").expect("load").expect("row");
        assert_eq!(stored.p_internal_log_odds, log_odds);
        assert_eq!(stored.p_market_last, 0.4);
    }

    #[tokio::test]
    async fn check_now_runs_one_evaluation() {
        let (db, store) = store_with(&[]);
        let registry = registry_with(&[]);
        let onboarder = Arc::new(ScriptedOnboarder::new(draft(market(
            "M1",
            "Will it rain?",
            "will-it-rain",
            MarketStatus::Active,
        ))));
        let trading = Arc::new(RecordingTrading::new(ScriptedLiquidation::Sold));
        let control = control(db, store, registry, onboarder, Arc::clone(&trading));

        control.check_now("M1").await;

        assert_eq!(trading.evaluated_ids(), vec!["M1".to_owned()]);
    }

    #[tokio::test]
    async fn panic_sets_the_kill_switch_and_counts_the_sweep() {
        let active = market("M1", "First?", "first", MarketStatus::Active);
        let paused = market("M2", "Second?", "second", MarketStatus::Paused);
        let closed = market("M3", "Third?", "third", MarketStatus::Closed);
        let (db, store) = store_with(&[active, paused, closed]);
        let registry = registry_with(&[]);
        let onboarder = Arc::new(ScriptedOnboarder::new(draft(market(
            "M1",
            "First?",
            "first",
            MarketStatus::Active,
        ))));
        let trading =
            Arc::new(RecordingTrading::new(ScriptedLiquidation::Sold).with_sweep_error("M1"));
        let control = control(
            db.clone(),
            Arc::clone(&store),
            registry,
            onboarder,
            Arc::clone(&trading),
        );

        let report = control.panic(true).await.expect("panic succeeds");

        assert_eq!(
            db.get_kv(KILL_SWITCH_KEY).expect("read kv"),
            Some("true".to_owned())
        );
        assert_eq!(report.attempted, Some(2));
        assert_eq!(report.liquidated, 1);
        assert_eq!(
            report.errors,
            vec![("M1".to_owned(), "liquidation failed for M1".to_owned())]
        );
        assert_eq!(trading.sweeps(), 1);
    }

    #[tokio::test]
    async fn panic_without_liquidate_all_sells_nothing() {
        let active = market("M1", "First?", "first", MarketStatus::Active);
        let (db, store) = store_with(&[active]);
        let registry = registry_with(&[]);
        let onboarder = Arc::new(ScriptedOnboarder::new(draft(market(
            "M1",
            "First?",
            "first",
            MarketStatus::Active,
        ))));
        let trading = Arc::new(RecordingTrading::new(ScriptedLiquidation::Sold));
        let control = control(db.clone(), store, registry, onboarder, Arc::clone(&trading));

        let report = control.panic(false).await.expect("panic succeeds");

        assert_eq!(
            db.get_kv(KILL_SWITCH_KEY).expect("read kv"),
            Some("true".to_owned())
        );
        assert_eq!(report.attempted, None);
        assert_eq!(report.liquidated, 0);
        assert!(report.errors.is_empty());
        assert_eq!(trading.sweeps(), 0);
    }

    #[test]
    fn resolves_a_market_by_case_insensitive_id_or_slug() {
        let markets = vec![
            market("AbC123", "First?", "first-question", MarketStatus::Active),
            market("DeF456", "Second?", "second-question", MarketStatus::Active),
        ];

        assert_eq!(
            resolve_market(&markets, "abc123").map(|market| market.id.as_str()),
            Some("AbC123")
        );
        assert_eq!(
            resolve_market(&markets, "SECOND-QUESTION").map(|market| market.id.as_str()),
            Some("DeF456")
        );
        assert_eq!(
            resolve_market(&markets, "second").map(|market| market.id.as_str()),
            Some("DeF456")
        );
        assert_eq!(
            resolve_market(&markets, "https://manifold.markets/Alice/second-question")
                .map(|market| market.id.as_str()),
            Some("DeF456")
        );
        assert_eq!(
            resolve_market(&markets, "https://manifold.markets/Alice/second-question/")
                .map(|market| market.id.as_str()),
            Some("DeF456")
        );
        assert_eq!(
            resolve_market(
                &markets,
                "http://manifold.markets/Alice/second-question?ref=me"
            )
            .map(|market| market.id.as_str()),
            Some("DeF456")
        );
        assert_eq!(
            resolve_market(&markets, "manifold.markets/Alice/second-question")
                .map(|market| market.id.as_str()),
            Some("DeF456")
        );
        assert!(resolve_market(&markets, "missing").is_none());
        assert!(resolve_market(&markets, "").is_none());
    }

    #[test]
    fn ambiguous_slug_matches_resolve_to_none() {
        let markets = vec![
            market("M1", "First?", "shared-prefix-one", MarketStatus::Active),
            market("M2", "Second?", "shared-prefix-two", MarketStatus::Active),
        ];

        assert!(resolve_market(&markets, "shared").is_none());
    }
}
