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
use std::str::FromStr;

/// Represents the state of a task
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
