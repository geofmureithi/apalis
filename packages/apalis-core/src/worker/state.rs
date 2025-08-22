use std::{sync::atomic::AtomicUsize, sync::atomic::Ordering};

use crate::error::{WorkerError, WorkerStateError};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[repr(usize)]
pub(super) enum InnerWorkerState {
    #[default]
    Pending,
    Running,
    Paused,
    Stopped,
}

impl TryFrom<usize> for InnerWorkerState {
    type Error = WorkerError;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(InnerWorkerState::Pending),
            1 => Ok(InnerWorkerState::Running),
            2 => Ok(InnerWorkerState::Paused),
            3 => Ok(InnerWorkerState::Stopped),
            v => Err(WorkerError::StateError(WorkerStateError::InvalidState(
                format!("{v} not a valid state"),
            ))),
        }
    }
}

/// Represents the state of a worker
#[derive(Debug, Default)]
pub (super) struct WorkerState {
    inner: AtomicUsize,
}

impl WorkerState {
    pub (super) fn new(state: InnerWorkerState) -> Self {
        Self {
            inner: AtomicUsize::new(state as usize),
        }
    }

    pub(super) fn load(&self, order: Ordering) -> InnerWorkerState {
        InnerWorkerState::try_from(self.inner.load(order)).expect("Invalid enum value")
    }

    pub(super) fn store(&self, state: InnerWorkerState, order: Ordering) {
        self.inner.store(state as usize, order);
    }
}
