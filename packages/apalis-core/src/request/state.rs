use core::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Represents the state of a task
#[non_exhaustive]
#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, std::cmp::Eq)]
pub enum State {
    /// Task is pending
    #[serde(alias = "Latest")]
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

impl Default for State {
    fn default() -> Self {
        State::Pending
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StateError {
    #[error("Unknown state: {0}")]
    UnknownState(String),
}

impl FromStr for State {
    type Err = StateError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Pending" => Ok(State::Pending),
            "Queued" => Ok(State::Queued),
            "Running" => Ok(State::Running),
            "Done" => Ok(State::Done),
            "Failed" => Ok(State::Failed),
            "Killed" => Ok(State::Killed),
            _ => Err(StateError::UnknownState(s.to_owned())),
        }
    }
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            State::Pending => write!(f, "Pending"),
            State::Queued => write!(f, "Queued"),
            State::Running => write!(f, "Running"),
            State::Done => write!(f, "Done"),
            State::Failed => write!(f, "Failed"),
            State::Killed => write!(f, "Killed"),
        }
    }
}
