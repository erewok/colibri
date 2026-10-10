//! Millisecond clocks used by the rate limiters.
//!
//! Every time-dependent decision in `limiters` reads one of the two functions
//! here, which gives tests a single seam for controlling time. With the
//! `test-clock` feature enabled, a thread may install a [`virtual_clock`] that
//! replaces both clocks for that thread until its guard is dropped; threads
//! without one see real time. Without the feature, the seam compiles away.

use std::sync::OnceLock;
use std::time::Instant;

use chrono::Utc;

/// Wall-clock milliseconds since the Unix epoch.
///
/// Used by `TokenBucket`, whose `last_call` is serialized and imported by
/// other nodes, so timestamps must be comparable across processes.
pub fn wall_ms() -> i64 {
    #[cfg(feature = "test-clock")]
    if let Some(now) = virtual_clock::now() {
        return virtual_clock::WALL_EPOCH_MS + now + virtual_clock::wall_offset();
    }
    Utc::now().timestamp_millis()
}

static PROCESS_START: OnceLock<Instant> = OnceLock::new();

/// Monotonic millisecond clock for all elapsed-time decisions in `DistributedBucket`.
///
/// Backed by `std::time::Instant`, which is guaranteed not to go backwards
/// regardless of NTP corrections, leap seconds, or operator clock resets.
/// The epoch is anchored at the first call (effectively process start) and is
/// process-local — values are not meaningful across process restarts or node
/// boundaries. That is fine: `DistributedBucket` state is in-memory only, and
/// the fields that use this clock (`last_call`, `InternalRequestEntry::timestamp_ms`)
/// are never included in the gossip wire format.
pub fn monotonic_ms() -> i64 {
    #[cfg(feature = "test-clock")]
    if let Some(now) = virtual_clock::now() {
        return now;
    }
    PROCESS_START
        .get_or_init(Instant::now)
        .elapsed()
        .as_millis() as i64
}

/// A per-thread virtual clock for deterministic tests.
///
/// ```ignore
/// let clock = colibri::clock::virtual_clock::install();
/// // ... time is frozen for this thread ...
/// clock.advance(1_500);
/// ```
#[cfg(feature = "test-clock")]
pub mod virtual_clock {
    use std::cell::Cell;
    use std::marker::PhantomData;

    /// Offset added to virtual time to produce plausible wall-clock values.
    pub(super) const WALL_EPOCH_MS: i64 = 1_700_000_000_000;

    thread_local! {
        static NOW_MS: Cell<Option<i64>> = const { Cell::new(None) };
        /// How far the wall clock has been stepped away from monotonic time.
        static WALL_OFFSET_MS: Cell<i64> = const { Cell::new(0) };
    }

    pub(super) fn now() -> Option<i64> {
        NOW_MS.with(Cell::get)
    }

    pub(super) fn wall_offset() -> i64 {
        WALL_OFFSET_MS.with(Cell::get)
    }

    /// Restores real time for the installing thread when dropped.
    /// Not `Send`: the clock belongs to the thread that installed it.
    pub struct VirtualClockGuard {
        _not_send: PhantomData<*const ()>,
    }

    /// Freeze time at zero for the current thread.
    ///
    /// Panics if this thread already has a virtual clock installed.
    pub fn install() -> VirtualClockGuard {
        NOW_MS.with(|now| {
            assert!(now.get().is_none(), "virtual clock already installed");
            now.set(Some(0));
        });
        VirtualClockGuard {
            _not_send: PhantomData,
        }
    }

    impl VirtualClockGuard {
        /// Move this thread's virtual time forward.
        pub fn advance(&self, ms: u32) {
            NOW_MS.with(|now| now.set(now.get().map(|t| t + i64::from(ms))));
        }

        /// Step the wall clock by `ms` (negative steps it back), as NTP or an
        /// operator might. Monotonic time is unaffected.
        pub fn step_wall(&self, ms: i64) {
            WALL_OFFSET_MS.with(|offset| offset.set(offset.get() + ms));
        }

        /// Current virtual monotonic time in milliseconds.
        pub fn now_ms(&self) -> i64 {
            now().expect("guard exists, so the clock is installed")
        }
    }

    impl Drop for VirtualClockGuard {
        fn drop(&mut self) {
            NOW_MS.with(|now| now.set(None));
            WALL_OFFSET_MS.with(|offset| offset.set(0));
        }
    }
}
