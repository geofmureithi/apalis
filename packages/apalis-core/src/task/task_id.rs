/// A unique ID that can be used by a backend
use std::{
    convert::Infallible,
    fmt::{Debug, Display},
    hash::Hash,
    str::FromStr,
    time::SystemTime,
};

use ulid::Ulid;

use crate::{service_fn::from_request::FromRequest, task::Task};

/// A wrapper type that defines a task id.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Copy, Clone, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct TaskId<IdType = Ulid>(IdType);

impl<IdType> TaskId<IdType> {
    /// Generate a new [`TaskId`]
    pub fn new(id: IdType) -> Self {
        Self(id)
    }
    /// Get the inner [`IdType`]
    pub fn inner(&self) -> &IdType {
        &self.0
    }
}

impl TaskId<Ulid> {
    pub fn from_system_time(datetime: SystemTime) -> Self {
        TaskId(Ulid::from_datetime(datetime))
    }
}

impl<IdType: Default> Default for TaskId<IdType> {
    fn default() -> Self {
        Self::new(IdType::default())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TaskIdError<E> {
    #[error("could not decode task_id: `{0}`")]
    Decode(E),
}

impl<IdType: FromStr> FromStr for TaskId<IdType> {
    type Err = TaskIdError<IdType::Err>;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(TaskId(
            IdType::from_str(s).map_err(|e| TaskIdError::Decode(e))?,
        ))
    }
}

impl<IdType: FromStr> TryFrom<&'_ str> for TaskId<IdType> {
    type Error = TaskIdError<IdType::Err>;

    fn try_from(value: &'_ str) -> Result<Self, Self::Error> {
        Self::from_str(value)
    }
}

impl<IdType: Display> Display for TaskId<IdType> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl<Req: Sync, Ctx: Sync, IdType: Clone + Sync> FromRequest<Task<Req, Ctx, IdType>>
    for TaskId<IdType>
{
    type Error = Infallible;
    async fn from_request(req: &Task<Req, Ctx, IdType>) -> Result<Self, Self::Error> {
        Ok(req.meta.task_id.clone())
    }
}
