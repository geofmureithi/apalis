use std::sync::{atomic::{AtomicUsize, Ordering}, Arc};

use super::{PLUGGED, UNPLUGGED};

/// The `Control` struct represents a thread-safe state manager.
/// It uses `AtomicUsize` for state to ensure safe concurrent access.
#[derive(Debug, Clone)]
pub struct Control {
    pub(super) state: Arc<AtomicUsize>,
}

impl Control {
    /// Constructs a new `Controller` instance with an initial state.
    pub fn new() -> Self {
        Control {
            state: Arc::new(AtomicUsize::new(PLUGGED)),
        }
    }

    /// Sets the state of the controller to `PLUGGED`.
    pub fn plug(&self) {
        self.state.store(PLUGGED, Ordering::SeqCst);
    }

    /// Sets the state of the controller to `UNPLUGGED`.
    pub fn unplug(&self) {
        self.state.store(UNPLUGGED, Ordering::SeqCst);
    }

    /// Returns `true` if the current state is `PLUGGED`.
    pub fn is_plugged(&self) -> bool {
        self.state.load(Ordering::SeqCst) == PLUGGED
    }
}

impl Default for Control {
    fn default() -> Self {
        Self::new()
    }
}

pub trait Controller {
    fn plug(&self);

    fn unplug(&self);

    fn is_plugged(&self) -> bool;
}

impl Controller for Control {
    fn unplug(&self) {
        self.unplug()
    }
    fn is_plugged(&self) -> bool {
        self.is_plugged()
    }
    fn plug(&self) {
        self.plug()
    }
}

impl Controller for () {
    fn is_plugged(&self) -> bool {
        true
    }
    fn plug(&self) {}
    fn unplug(&self) {}
}
