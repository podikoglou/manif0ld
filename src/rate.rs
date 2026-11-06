//! Global token bucket for the Manifold REST API.
//!
//! Everything we ever do against `api.manifold.markets` — trade writes,
//! snapshots, reconciliation, discovery — shares one IP-level limit of 500
//! requests/minute. This module turns that limit into a shared budget
//! (`docs/ALGO.md` §2) instead of hoping the consumers stay small:
//!
//! - **Continuous refill, lazily applied.** On every acquire the bucket
//!   tops up from a stored timestamp, so time passes while the bot is idle
//!   and no background task is needed. The core is synchronous
//!   (`std::sync::Mutex`): there is no tokio and no await inside it.
//! - **Priority by threshold, not by queue.** Trades are rare but
//!   latency-critical; housekeeping (reconcile, discovery) is periodic and
//!   can always wait for the next pass. Housekeeping classes must keep a
//!   reserve (12% of capacity) unspent, so a breaking-news trade burst can
//!   drain the spendable space without queueing behind housekeeping.
//!   Trades themselves may spend down to a small floor (1% of capacity)
//!   and no further, so the bucket always keeps a token of headroom.
//! - **Refusals are skips, not errors.** An acquire that fails means
//!   "skip this call, retry on the next pass", which is harmless for
//!   housekeeping and the intended shape for anything lower priority
//!   than a trade.
//!
//! The clock is injected so tests advance time by calling
//! [`TestClock::advance`] instead of sleeping; refill is verified against
//! injected time, never wall-clock.

#[cfg(test)]
use std::sync::Arc;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

/// Fraction of capacity that housekeeping classes must leave unspent.
///
/// `docs/ALGO.md` §2 budgets trade writes at ≤60/min of a 500/min bucket,
/// i.e. 60/500 = 12%: that is the headroom a trade burst can rely on.
const HOUSEKEEPING_RESERVE_FRACTION: f64 = 0.12;

/// Fraction of capacity to which a Trade-class acquire may drain the bucket.
///
/// Small on purpose: trades may use the whole reserve, but the bucket never
/// refunds below 1% of capacity, so back-to-back bursts stop before the
/// bucket is empty.
const TRADE_FLOOR_FRACTION: f64 = 0.01;

/// Cost of one REST write, in tokens.
///
/// Every Manifold request counts one request of the per-minute limit, so one
/// token per call is the natural unit.
pub const WRITE_COST_TOKENS: f64 = 1.0;

/// A monotonic time source for the bucket.
///
/// Only differences between two `now()` calls are ever read, so the origin is
/// arbitrary.
pub trait Clock: Send + Sync {
    /// Time elapsed from a fixed origin.
    fn now(&self) -> Duration;
}

/// Wall-clock implementation of [`Clock`].
///
/// `std::time::Instant` has a process-defined origin, so the anchor is fixed
/// at first use and only deltas are ever read.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Duration {
        static ORIGIN: OnceLock<Instant> = OnceLock::new();
        Instant::now().saturating_duration_since(*ORIGIN.get_or_init(Instant::now))
    }
}

// -----------------------------------------------------------------------------
// Bucket
// -----------------------------------------------------------------------------

/// Priority classes that share the Manifold budget, strongest first.
///
/// The threshold each class pays up to is selected by [`priority_threshold`];
/// the classes exist so call sites declare intent ("a trade" vs "a sweep")
/// rather than numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priority {
    /// Manifold writes (bet, sell, cancel): always wins. May spend down to the
    /// trade floor and no further.
    Trade,
    /// Slow reconciliation reads: the batched price refresh and the portfolio
    /// position snapshot in `TradeExecutor`'s 5-minute backstop. Yields when
    /// the bucket is at or below the reserve so a trade burst can always get
    /// through.
    Reconcile,
    /// Discovery sweeps: lowest class, same reserve behaviour as
    /// [`Priority::Reconcile`]. Spent by the S2 new-market market fetch
    /// (`crate::snipe`); later used by the discovery sweeps of `docs/ALGO.md`
    /// §2.
    Discovery,
}

/// Selects the level a class may spend down to.
///
/// Trade may drain the bucket to the trade floor; housekeeping must leave the
/// reserve in place, so a low bucket refuses housekeeping long before it
/// refuses trades.
fn priority_threshold(class: Priority, capacity: f64) -> f64 {
    match class {
        Priority::Trade => TRADE_FLOOR_FRACTION * capacity,
        Priority::Reconcile | Priority::Discovery => HOUSEKEEPING_RESERVE_FRACTION * capacity,
    }
}

/// A successful acquire. `remaining` is the bucket level after the spend.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Granted {
    pub remaining: f64,
}

/// A refused acquire.
///
/// `remaining` is the bucket level at refusal and `threshold` the level the
/// requested class demanded; both are for log lines only. The caller is
/// expected to skip the call and retry on the next pass.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Denied {
    pub remaining: f64,
    pub threshold: f64,
}

struct BucketState {
    tokens: f64,
    last: Duration,
}

/// The shared token bucket.
///
/// Cloning is not supported: the only instance is built once (in the trade
/// executor for now) and shared by reference. Refill is computed on every
/// [`RateLimiter::try_acquire`], so there is no async, no task, and no tick.
pub struct RateLimiter {
    clock: Box<dyn Clock>,
    capacity: f64,
    refill_per_second: f64,
    state: Mutex<BucketState>,
}

impl RateLimiter {
    /// Builds a bucket that refills `per_minute` tokens across every minute,
    /// i.e. continuously rather than in minute-aligned bursts.
    ///
    /// This is the production constructor: capacity and refill rate are the
    /// same per-minute budget, expressed once.
    pub fn per_minute(per_minute: usize, clock: Box<dyn Clock>) -> Self {
        Self::new(per_minute as f64, per_minute as f64 / 60.0, clock)
    }

    /// Builds a bucket with an explicit capacity and refill rate.
    ///
    /// `refill_per_second` may be below the per-minute-equivalent when a test
    /// wants a slower top-up; zero disables refill.
    ///
    /// # Panics
    ///
    /// Panics on a non-positive capacity or a negative refill rate. These are
    /// construction bugs, not runtime conditions.
    pub fn new(capacity: f64, refill_per_second: f64, clock: Box<dyn Clock>) -> Self {
        assert!(
            capacity.is_finite() && capacity > 0.0,
            "capacity must be positive"
        );
        assert!(
            refill_per_second.is_finite() && refill_per_second >= 0.0,
            "refill rate must be non-negative"
        );

        Self {
            clock,
            capacity,
            refill_per_second,
            state: Mutex::new(BucketState {
                tokens: capacity,
                last: Duration::ZERO,
            }),
        }
    }

    /// Attempts to spend `cost` tokens for one request of `class`.
    ///
    /// Succeeds, charging `cost`, exactly when the post-spend level stays at
    /// or above the class threshold (trade floor for `Priority::Trade`,
    /// housekeeping reserve otherwise). A zero or negative cost is a no-op
    /// acquire: it succeeds without spending and never moves the level.
    pub fn try_acquire(&self, class: Priority, cost: f64) -> Result<Granted, Denied> {
        let cost = cost.max(0.0);
        let threshold = priority_threshold(class, self.capacity);

        // Poison recovery is safe by construction: the state is two plain
        // numbers, and the lock guards nothing that a panicking holder could
        // have left inconsistent.
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let now = self.clock.now();
        let elapsed = now.saturating_sub(state.last);
        state.last = now;
        state.tokens =
            (state.tokens + elapsed.as_secs_f64() * self.refill_per_second).min(self.capacity);

        if state.tokens < threshold + cost {
            return Err(Denied {
                remaining: state.tokens,
                threshold,
            });
        }

        state.tokens -= cost;
        Ok(Granted {
            remaining: state.tokens,
        })
    }
}

// -----------------------------------------------------------------------------
// Test clock
// -----------------------------------------------------------------------------

/// A [`Clock`] whose time only moves when [`TestClock::advance`] is called.
#[cfg(test)]
#[derive(Debug, Clone, Default)]
pub struct TestClock(Arc<Mutex<Duration>>);

#[cfg(test)]
impl TestClock {
    /// Builds a clock standing at zero.
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(Duration::ZERO)))
    }

    /// Moves the clock forward; the bucket refill sees the sum of every call.
    pub fn advance(&self, by: Duration) {
        let mut current = self.0.lock().unwrap_or_else(|p| p.into_inner());
        *current += by;
    }
}

#[cfg(test)]
impl Clock for TestClock {
    fn now(&self) -> Duration {
        *self.0.lock().unwrap_or_else(|p| p.into_inner())
    }
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a bucket whose capacity equals its per-minute budget, and whose
    /// clocks only move when a test moves them.
    fn limiter(per_minute: usize) -> (RateLimiter, TestClock) {
        let clock = TestClock::new();
        (
            RateLimiter::per_minute(per_minute, Box::new(clock.clone())),
            clock,
        )
    }

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-9,
            "expected {expected}, got {actual}"
        );
    }

    #[test]
    fn refills_continuously_over_injected_time() {
        let (limiter, clock) = limiter(100);
        // Drain below the reserve (12 tokens) with trades only: trades may
        // spend housekeeping's headroom, down to the trade floor.
        for _ in 0..89 {
            assert!(
                limiter.try_acquire(Priority::Trade, 1.0).is_ok(),
                "should drain below the reserve"
            );
        }

        for _ in 0..3 {
            clock.advance(Duration::from_secs(2));
        }

        // 100/60 tokens per second across 6 seconds adds 10 tokens on top of
        // the 11 left in the bucket; the spend takes one.
        let granted = limiter
            .try_acquire(Priority::Trade, 1.0)
            .expect("refill should restore one trade");
        assert_close(granted.remaining, 20.0);
    }

    #[test]
    fn refill_is_capped_at_capacity() {
        let (limiter, clock) = limiter(100);
        clock.advance(Duration::from_secs(10_000));

        // The bucket may have sat full for a long time; it cannot exceed its
        // capacity, so 99 trade-sized acquires drain it to the floor again.
        for _ in 0..99 {
            assert!(limiter.try_acquire(Priority::Trade, 1.0).is_ok());
        }
        assert!(
            limiter.try_acquire(Priority::Trade, 1.0).is_err(),
            "the floor must stop the 100th acquire"
        );
    }

    #[test]
    fn trade_succeeds_while_reconcile_is_refused_below_the_reserve() {
        let (limiter, _clock) = limiter(100);
        // Reserve is 12 tokens. Drain below it with trades only.
        for _ in 0..88 {
            limiter.try_acquire(Priority::Trade, 1.0).expect("drain");
        }

        let denied = limiter
            .try_acquire(Priority::Reconcile, 1.0)
            .expect_err("housekeeping must yield once the reserve is spent");
        assert_close(denied.remaining, 12.0);
        assert_close(denied.threshold, 12.0);

        // The same bucket still serves a trade: it may spend down to the
        // floor, exactly the headroom the reserve is meant to protect.
        let granted = limiter
            .try_acquire(Priority::Trade, 1.0)
            .expect("trade must win below the reserve");
        assert_close(granted.remaining, 11.0);
    }

    #[test]
    fn housekeeping_succeeds_when_the_bucket_is_full() {
        let (limiter, _clock) = limiter(100);

        let granted = limiter
            .try_acquire(Priority::Reconcile, 1.0)
            .expect("reconcile should pass with a full bucket");
        let discovery = limiter
            .try_acquire(Priority::Discovery, 1.0)
            .expect("discovery should pass with a full bucket");

        assert_close(granted.remaining, 99.0);
        assert_close(discovery.remaining, 98.0);
    }

    #[test]
    fn trade_drains_to_the_floor_and_stops() {
        let (limiter, clock) = limiter(100);
        // Floor is 1% of 100 = 1 token: 99 acquires are allowed, no more.
        let mut spent = 0.0;
        while let Ok(granted) = limiter.try_acquire(Priority::Trade, 1.0) {
            spent += 1.0;
            assert!(granted.remaining >= 1.0, "never below the floor");
        }
        assert_close(spent, 99.0);

        // A trade burst can be no bigger than the spendable space; refilling
        // buys the next burst.
        clock.advance(Duration::from_secs(60));
        assert!(limiter.try_acquire(Priority::Trade, 1.0).is_ok());
    }

    #[test]
    fn costs_are_metric_amounts() {
        let (limiter, _clock) = limiter(100);

        let granted = limiter
            .try_acquire(Priority::Trade, 2.5)
            .expect("a partial-cost acquire");
        assert_close(granted.remaining, 97.5);

        // A fractional cost is also a real spend, not a fee discount.
        let granted = limiter
            .try_acquire(Priority::Trade, 0.25)
            .expect("fractional cost should succeed");
        assert_close(granted.remaining, 97.25);

        // A cost larger than the spendable space is refused without spending.
        let denied = limiter
            .try_acquire(Priority::Trade, 200.0)
            .expect_err("a cost over the capacity cannot be granted");
        assert_close(denied.remaining, 97.25);

        // A zero or negative cost is a free acquire.
        let granted = limiter
            .try_acquire(Priority::Trade, 0.0)
            .expect("zero cost");
        assert_close(granted.remaining, 97.25);
        let granted = limiter
            .try_acquire(Priority::Trade, -3.0)
            .expect("negative cost is clamped to zero");
        assert_close(granted.remaining, 97.25);
    }

    #[test]
    fn acquires_from_multiple_threads_share_one_bucket() {
        // Refill is zero so the total spend is exact regardless of wall clock.
        let limiter = RateLimiter::new(100.0, 0.0, Box::new(SystemClock));
        let limiter = Arc::new(limiter);

        let granted_total: usize = std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for _ in 0..4 {
                let limiter = Arc::clone(&limiter);
                handles.push(scope.spawn(move || {
                    let mut granted = 0;
                    while limiter.try_acquire(Priority::Trade, 1.0).is_ok() {
                        granted += 1;
                    }
                    granted
                }));
            }
            handles.into_iter().map(|h| h.join().expect("thread")).sum()
        });

        // Floor is 1 token, capacity 100: 99 acquires across four threads, no
        // more, no matter how they interleave.
        assert_eq!(granted_total, 99);
    }
}
