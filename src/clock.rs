//! Time abstraction for rate-limiting and reconciliation decisions.
//!
//! The auto-resolving engines use a [`Clock`] to debounce re-executions
//! per captured query. The trait is intentionally minimal: a single
//! `now_micros()` method returning a monotonically non-decreasing
//! microsecond counter from some implementation-defined origin. A
//! [`Timer`] also wakes a task at a reading, which the drift guard's
//! reconciliation awaits.
//!
//! [`StdClock`] is the default, available under the `std` feature. It
//! reads `std::time::Instant`. [`ManualClock`] is for tests and ticks
//! only when the test calls [`ManualClock::advance`]. `TokioTimer`, under
//! the `pg-streaming` feature, reads and sleeps on tokio's clock.

use alloc::sync::Arc;
use core::future::{poll_fn, Future};
use core::sync::atomic::{AtomicU64, Ordering};
use core::task::Poll;
use core::time::Duration;

/// Monotonic time source.
///
/// Implementations must return values that are non-decreasing across calls
/// from the same instance, but the origin of the count is up to the
/// implementation (process start, an absolute epoch, a test-supplied
/// counter, etc.). Engines use the trait only for *deltas* between calls.
///
/// `Send + Sync` so the trait works in both single-threaded sync engines
/// and multi-threaded async runtimes. Subql stores clocks behind
/// [`Arc<dyn Clock>`](alloc::sync::Arc) so the same clock can be shared
/// across multiple engines without ownership games.
pub trait Clock: Send + Sync {
    /// Current monotonic position in microseconds from this clock's
    /// origin. Calls are non-decreasing for a given clock instance.
    fn now_micros(&self) -> u64;
}

/// Default clock for `std` builds: reads `std::time::Instant`.
#[cfg(feature = "std")]
#[derive(Debug)]
pub struct StdClock {
    origin: std::time::Instant,
}

#[cfg(feature = "std")]
impl StdClock {
    /// Construct a clock anchored at "now" of the calling thread.
    #[must_use]
    pub fn new() -> Self {
        Self {
            origin: std::time::Instant::now(),
        }
    }
}

#[cfg(feature = "std")]
impl Default for StdClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "std")]
impl Clock for StdClock {
    fn now_micros(&self) -> u64 {
        let elapsed = std::time::Instant::now().saturating_duration_since(self.origin);
        u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX)
    }
}

/// Test-controlled clock. Starts at zero (or a caller-supplied origin)
/// and only advances when [`advance`](Self::advance) is called.
///
/// Thread-safe via an atomic counter. Useful in async tests that want
/// deterministic timing across `block_on` boundaries.
#[derive(Debug)]
pub struct ManualClock {
    micros: AtomicU64,
}

impl ManualClock {
    /// Create a clock starting at `start_micros`.
    ///
    /// # Examples
    ///
    /// ```
    /// use subql::{Clock, ManualClock};
    ///
    /// let clock = ManualClock::new(1_000_000);
    /// assert_eq!(clock.now_micros(), 1_000_000);
    /// ```
    #[must_use]
    pub const fn new(start_micros: u64) -> Self {
        Self {
            micros: AtomicU64::new(start_micros),
        }
    }

    /// Advance the clock by `by`. Saturates at `u64::MAX`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use subql::{Clock, ManualClock};
    ///
    /// let clock = ManualClock::new(0);
    /// clock.advance(Duration::from_millis(250));
    /// assert_eq!(clock.now_micros(), 250_000);
    /// ```
    pub fn advance(&self, by: Duration) {
        let add = u64::try_from(by.as_micros()).unwrap_or(u64::MAX);
        self.micros.fetch_add(add, Ordering::SeqCst);
    }
}

impl Default for ManualClock {
    fn default() -> Self {
        Self::new(0)
    }
}

impl Clock for ManualClock {
    fn now_micros(&self) -> u64 {
        self.micros.load(Ordering::SeqCst)
    }
}

/// A [`Clock`] that wakes a waiting task once it reaches a reading.
///
/// [`DriftGuard`](crate::drift::DriftGuard) awaits one to start each
/// reconciliation run, so a run starts on an idle stream too.
pub trait Timer: Clock {
    /// Complete once [`Clock::now_micros`] reaches `deadline_micros`.
    fn sleep_until(&self, deadline_micros: u64) -> impl Future<Output = ()> + Send + '_;
}

impl<C: Clock + ?Sized> Clock for Arc<C> {
    fn now_micros(&self) -> u64 {
        (**self).now_micros()
    }
}

impl<T: Timer> Timer for Arc<T> {
    fn sleep_until(&self, deadline_micros: u64) -> impl Future<Output = ()> + Send + '_ {
        (**self).sleep_until(deadline_micros)
    }
}

/// A manual clock has no task to wake when it advances, so its sleep asks
/// to be polled again until the reading reaches the deadline.
impl Timer for ManualClock {
    fn sleep_until(&self, deadline_micros: u64) -> impl Future<Output = ()> + Send + '_ {
        poll_fn(move |context| {
            if self.now_micros() >= deadline_micros {
                Poll::Ready(())
            } else {
                context.waker().wake_by_ref();
                Poll::Pending
            }
        })
    }
}

/// A [`Timer`] on tokio's clock, so it follows `tokio::time::pause`.
#[cfg(feature = "pg-streaming")]
#[derive(Debug)]
pub struct TokioTimer {
    origin: tokio::time::Instant,
}

#[cfg(feature = "pg-streaming")]
impl TokioTimer {
    /// A timer whose readings count from now.
    #[must_use]
    pub fn new() -> Self {
        Self {
            origin: tokio::time::Instant::now(),
        }
    }
}

#[cfg(feature = "pg-streaming")]
impl Default for TokioTimer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "pg-streaming")]
impl Clock for TokioTimer {
    fn now_micros(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_micros()).unwrap_or(u64::MAX)
    }
}

/// The furthest a [`TokioTimer`] sleeps, which every platform's `Instant`
/// reaches from its origin and no run waits for.
#[cfg(feature = "pg-streaming")]
const FAR_FUTURE: Duration = Duration::from_hours(30 * 365 * 24);

#[cfg(feature = "pg-streaming")]
impl Timer for TokioTimer {
    fn sleep_until(&self, deadline_micros: u64) -> impl Future<Output = ()> + Send + '_ {
        tokio::time::sleep_until(
            self.origin + Duration::from_micros(deadline_micros).min(FAR_FUTURE),
        )
    }
}

/// Compute the elapsed [`Duration`] between two `now_micros` readings.
///
/// Saturates at zero when the clock somehow went backwards (the trait's
/// monotonicity contract should make this impossible, but the saturation
/// keeps debounce logic robust against badly-behaved custom impls).
#[must_use]
pub const fn duration_between(earlier_micros: u64, later_micros: u64) -> Duration {
    Duration::from_micros(later_micros.saturating_sub(earlier_micros))
}

/// Shared clock handle: an `Arc<dyn Clock>` typedef used by the engines
/// to store and clone clocks cheaply.
pub type ClockHandle = Arc<dyn Clock>;

#[cfg(all(test, feature = "pg-streaming"))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_tokio_timer_wakes_at_its_deadline_and_not_before() {
        let timer = TokioTimer::default();
        let deadline = timer.now_micros() + 2_000;
        timer.sleep_until(deadline).await;
        assert!(timer.now_micros() >= deadline);
        let far = tokio::time::timeout(Duration::from_millis(5), timer.sleep_until(u64::MAX));
        assert!(
            far.await.is_err(),
            "a deadline past the far future never comes"
        );
    }
}
