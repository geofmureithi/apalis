/// A unique ID that can be used by a backend
use std::{
    convert::Infallible,
    fmt::{Debug, Display},
    hash::Hash,
    str::FromStr,
};

use serde::{de::Visitor, Deserialize, Deserializer, Serialize, Serializer};
use ulid::Ulid;

use crate::{request::Request, service_fn::from_request::FromRequest};

/// A wrapper type that defines a task id.
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

impl Serialize for TaskId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for TaskId {
    fn deserialize<D>(deserializer: D) -> Result<TaskId, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(TaskIdVisitor)
    }
}

impl<Req: Sync, Ctx: Sync> FromRequest<Request<Req, Ctx>> for TaskId {
    type Error = Infallible;
    async fn from_request(req: &Request<Req, Ctx>) -> Result<Self, Self::Error> {
        Ok(req.parts.task_id.clone())
    }
}

struct TaskIdVisitor;

impl Visitor<'_> for TaskIdVisitor {
    type Value = TaskId;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a `ulid`")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        TaskId::from_str(value).map_err(serde::de::Error::custom)
    }
}
