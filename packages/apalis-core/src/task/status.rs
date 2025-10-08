//! The status of a task
//!
//! ## Overview
//!
//! The `Status` enum defines the various states
//! a task can be in, such as `Pending`, `Running`, `Done`, `Failed`, etc.
//!
//! - It includes functionality for parsing a `Status` from a string and
//! formatting it for display.
//! - This is useful for tracking the lifecycle of tasks.
use core::fmt;
use std::{
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
};

/// Represents the state of a task
#[repr(u8)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum Status {
    /// Task is pending
    Pending,
    /// Task is queued for execution, but no worker has picked it up
    Queued,
    /// Task is running
    Running,
    /// Task was done successfully
    Done,
    /// Task has failed.
    Failed,
    /// Task has been killed
    Killed,
}

impl Default for Status {
    fn default() -> Self {
        Status::Pending
    }
}

/// Errors that can occur when parsing a `Status` from a string
#[derive(Debug, thiserror::Error)]
pub enum StatusError {
    #[error("Unknown state: {0}")]
    /// Unknown state error
    UnknownState(String),
}

impl FromStr for Status {
    type Err = StatusError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Pending" => Ok(Status::Pending),
            "Queued" => Ok(Status::Queued),
            "Running" => Ok(Status::Running),
            "Done" => Ok(Status::Done),
            "Failed" => Ok(Status::Failed),
            "Killed" => Ok(Status::Killed),
            _ => Err(StatusError::UnknownState(s.to_owned())),
        }
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            Status::Pending => write!(f, "Pending"),
            Status::Queued => write!(f, "Queued"),
            Status::Running => write!(f, "Running"),
            Status::Done => write!(f, "Done"),
            Status::Failed => write!(f, "Failed"),
            Status::Killed => write!(f, "Killed"),
        }
    }
}

impl Status {
    fn from_u8(val: u8) -> Option<Self> {
        match val {
            0 => Some(Status::Pending),
            1 => Some(Status::Queued),
            2 => Some(Status::Running),
            3 => Some(Status::Done),
            4 => Some(Status::Failed),
            5 => Some(Status::Killed),
            _ => None,
        }
    }
}

/// Atomic version of `Status` for concurrent scenarios
#[repr(transparent)]
#[derive(Debug, Clone, Default)]
pub struct AtomicStatus(Arc<AtomicU8>);

impl AtomicStatus {
    /// Create a new `AtomicStatus` with the given initial status
    pub fn new(status: Status) -> Self {
        Self(Arc::new(AtomicU8::new(status as u8)))
    }

    /// Load the current status
    pub fn load(&self) -> Status {
        Status::from_u8(self.0.load(Ordering::Acquire)).unwrap()
    }

    /// Store a new status
    pub fn store(&self, status: Status) {
        self.0.store(status as u8, Ordering::Release);
    }
    /// Swap the current status with a new one, returning the old status
    pub fn swap(&self, status: Status) -> Status {
        Status::from_u8(self.0.swap(status as u8, Ordering::AcqRel)).unwrap()
    }
}

impl Into<Status> for AtomicStatus {
    fn into(self) -> Status {
        self.load()
    }
}

impl Into<AtomicStatus> for Status {
    fn into(self) -> AtomicStatus {
        AtomicStatus::new(self)
    }
}

#[cfg(feature = "serde")]
mod serde_impl {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::*;

    // Custom serialization function
    fn serialize<S>(status: &AtomicStatus, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let value = status.load();
        serializer.serialize_str(&value.to_string())
    }

    // Custom deserialization function
    fn deserialize<'de, D>(deserializer: D) -> Result<AtomicStatus, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        let status = Status::from_str(&value).map_err(serde::de::Error::custom)?;
        Ok(AtomicStatus::new(status))
    }

    impl Serialize for AtomicStatus {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serialize(self, serializer)
        }
    }

    impl<'de> Deserialize<'de> for AtomicStatus {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserialize(deserializer)
        }
    }
}
