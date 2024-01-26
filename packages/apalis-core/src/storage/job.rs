use std::{
    fmt::{self, Debug, Display},
    str::FromStr,
};

use serde::{de::Visitor, Deserialize, Deserializer, Serialize, Serializer};
use ulid::Ulid;

use crate::error::Error;

/// A wrapper type that defines a job id.
///
/// Job id's are prefixed by `JID-` followed by a [`ulid::Ulid`].
/// This makes [`JobId`]s orderable
///
#[derive(Debug, Clone)]
pub struct JobId(Ulid);

impl JobId {
    /// Generate a new [`JobId`]
    pub fn new() -> Self {
        Self(Ulid::new())
    }
    /// Get the inner [`Ulid`]
    pub fn inner(&self) -> Ulid {
        self.0
    }
}

impl Default for JobId {
    fn default() -> Self {
        Self::new()
    }
}

impl FromStr for JobId {
    type Err = ulid::DecodeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let prefix = &s[..4];
        if prefix != "JID-" {
            return Err(ulid::DecodeError::InvalidChar);
        }
        Ok(JobId(Ulid::from_str(&s[4..])?))
    }
}

impl Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("JID-")?;
        Display::fmt(&self.0, f)
    }
}

impl Serialize for JobId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for JobId {
    fn deserialize<D>(deserializer: D) -> Result<JobId, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(JobIdVisitor)
    }
}

struct JobIdVisitor;

impl<'de> Visitor<'de> for JobIdVisitor {
    type Value = JobId;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a prefix of `JID-` followed by the `ulid`")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        JobId::from_str(value).map_err(serde::de::Error::custom)
    }
}

/// Represents the state of a [Request]
#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, std::cmp::Eq)]
pub enum State {
    /// Job is pending
    #[serde(alias = "Latest")]
    Pending,
    /// Job is running
    Running,
    /// Job was done successfully
    Done,
    /// Retry Job
    Retry,
    /// Job has failed. Check `last_error`
    Failed,
    /// Job has been killed
    Killed,
}

impl Default for State {
    fn default() -> Self {
        State::Pending
    }
}

impl FromStr for State {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Pending" | "Latest" => Ok(State::Pending),
            "Running" => Ok(State::Running),
            "Done" => Ok(State::Done),
            "Retry" => Ok(State::Retry),
            "Failed" => Ok(State::Failed),
            "Killed" => Ok(State::Killed),
            _ => Err(Error::InvalidContext("Invalid Job state".to_string())),
        }
    }
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            State::Pending => write!(f, "Pending"),
            State::Running => write!(f, "Running"),
            State::Done => write!(f, "Done"),
            State::Retry => write!(f, "Retry"),
            State::Failed => write!(f, "Failed"),
            State::Killed => write!(f, "Killed"),
        }
    }
}

/// Trait representing a job.
///
///
/// # Example
/// ```rust
/// # use apalis_core::storage::job::Job;
/// # struct Email;
/// impl Job for Email {
///     const NAME: &'static str = "apalis::Email";
/// }
/// ```
pub trait Job {
    /// Represents the name for job.
    const NAME: &'static str;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_parse_job_id() {
        let id = "JID-01GWSGFS40RHST0FFZ6V1E1116";
        JobId::from_str(id).unwrap();
    }
}
