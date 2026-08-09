//! Explicit sleep budget for the bitemporal lifecycle conformance path.
//!
//! Spec 018 AC6 bounds the explicit test sleeps to ten seconds. The budget
//! distinguishes two kinds of sleep:
//!
//! - `sleep` records a deliberate timing sleep. A deliberate sleep exists so
//!   that wall-clock time passes for a proof, such as live lease expiry or a
//!   lease renewal. The AC6 assertion counts these sleeps.
//! - `poll_sleep` records a conditional-wait sleep. A conditional-wait sleep
//!   paces a poll loop that exits as soon as async worker progress makes the
//!   condition true. Each poll loop has its own deadline and asserts that
//!   deadline, so the recorded poll total is bounded by the sum of the poll
//!   deadlines and does not grow with machine speed. The AC6 assertion does
//!   not count these sleeps; a separate loose bound guards them.

use std::sync::Mutex;
use std::time::Duration;

static RECORDED: Mutex<Vec<Duration>> = Mutex::new(Vec::new());
static POLL_RECORDED: Mutex<Vec<Duration>> = Mutex::new(Vec::new());

/// Records and performs a deliberate timing sleep.
pub(crate) async fn sleep(duration: Duration) {
    RECORDED
        .lock()
        .expect("sleep budget mutex poisoned")
        .push(duration);
    tokio::time::sleep(duration).await;
}

/// Records and performs one pacing sleep inside a conditional-wait poll.
pub(crate) async fn poll_sleep(duration: Duration) {
    POLL_RECORDED
        .lock()
        .expect("sleep budget mutex poisoned")
        .push(duration);
    tokio::time::sleep(duration).await;
}

pub(crate) fn reset() {
    RECORDED
        .lock()
        .expect("sleep budget mutex poisoned")
        .clear();
    POLL_RECORDED
        .lock()
        .expect("sleep budget mutex poisoned")
        .clear();
}

/// Sum of the deliberate timing sleeps.
pub(crate) fn total() -> Duration {
    RECORDED
        .lock()
        .expect("sleep budget mutex poisoned")
        .iter()
        .copied()
        .sum()
}

/// Sum of the conditional-wait poll sleeps.
pub(crate) fn poll_total() -> Duration {
    POLL_RECORDED
        .lock()
        .expect("sleep budget mutex poisoned")
        .iter()
        .copied()
        .sum()
}
