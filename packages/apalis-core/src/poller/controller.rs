use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use super::{PLUGGED, STOPPED, UNPLUGGED};

/// The `Controller` struct represents a thread-safe state manager.
/// It uses `AtomicUsize` for state to ensure safe concurrent access.
/// Used to control polling
#[derive(Debug, Clone)]
pub struct Controller {
    pub(super) state: Arc<AtomicUsize>,
}

impl Controller {
    /// Constructs a new `Controller` instance with an initial state.
    pub fn new() -> Self {
        Controller {
            state: Arc::new(AtomicUsize::new(PLUGGED)),
        }
    }

    /// Sets the state of the controller to `PLUGGED`.
    pub fn plug(&self) {
        self.state.store(PLUGGED, Ordering::Relaxed);
    }

    /// Sets the state of the controller to `UNPLUGGED`.
    pub fn unplug(&self) {
        self.state.store(UNPLUGGED, Ordering::Relaxed);
    }

    /// Returns `true` if the current state is `PLUGGED`.
    pub fn is_plugged(&self) -> bool {
        self.state.load(Ordering::Relaxed) == PLUGGED
    }

    /// Sets the state of the controller to `Stopped`.
    pub fn stop(&self) {
        self.state.store(STOPPED, Ordering::Relaxed);
    }

    /// Returns `true` if the current state is `STOPPED`.
    pub fn is_stopped(&self) -> bool {
        self.state.load(Ordering::Relaxed) == STOPPED
    }
}

impl Default for Controller {
    fn default() -> Self {
        Self::new()
    }
}
