//! Explicit sleep budget for the bitemporal lifecycle conformance path.
//!
//! Spec 018 AC6 bounds the explicit test sleeps to ten seconds. Every
//! explicit sleep on this path goes through `sleep`, and the lifecycle test
//! asserts the recorded total at the end.

use std::sync::Mutex;
use std::time::Duration;

static RECORDED: Mutex<Vec<Duration>> = Mutex::new(Vec::new());

pub(crate) async fn sleep(duration: Duration) {
    RECORDED
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
}

pub(crate) fn total() -> Duration {
    RECORDED
        .lock()
        .expect("sleep budget mutex poisoned")
        .iter()
        .copied()
        .sum()
}
