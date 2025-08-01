/// A unique ID that can be used by a backend
use std::{
    fmt::{Debug, Display},
    hash::Hash,
    str::FromStr,
    time::SystemTime,
};

use ulid::Ulid;


/// A wrapper type that defines a task id.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct TaskId(Ulid);

impl TaskId {
    /// Generate a new [`TaskId`]
    pub fn new() -> Self {
        Self(Ulid::new())
    }
    /// Get the inner [`Ulid`]
    pub fn inner(&self) -> Ulid {
        self.0
    }

    pub fn from_system_time(datetime: SystemTime) -> Self {
        TaskId(Ulid::from_datetime(datetime))
    }
}

impl Default for TaskId {
    fn default() -> Self {
        Self::new()
    }
}

impl FromStr for TaskId {
    type Err = ulid::DecodeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(TaskId(Ulid::from_str(s)?))
    }
}

impl Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}
