use std::{
    future::Future,
    pin::Pin,
    time::{Duration, Instant},
};

/// Represents tokio timer utilities
#[cfg(feature = "tokio-comp")]
mod tokio_timer;

/// Represents async-std timer utilities
#[cfg(feature = "async-std-comp")]
mod async_std_timer;

/// Runtime agnostic sleep and timer utils
pub mod timer {
    #[cfg(feature = "async-std-comp")]
    pub use crate::utils::async_std_timer::AsyncStdTimer;
    #[cfg(feature = "tokio-comp")]
    pub use crate::utils::tokio_timer::TokioTimer;
}

/// A timer which provides timer-like functions.
pub trait Timer {
    /// Return a future that resolves in `duration` time.
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Sleep>>;

    /// Return a future that resolves at `deadline`.
    fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn Sleep>>;

    /// Reset a future to resolve at `new_deadline` instead.
    fn reset(&self, sleep: &mut Pin<Box<dyn Sleep>>, new_deadline: Instant) {
        *sleep = self.sleep_until(new_deadline);
    }
}

/// A future returned by a `Timer`.
pub trait Sleep: Send + Sync + Future<Output = ()> {}
