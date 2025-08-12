/// A unique ID that can be used by a backend
use std::{
    convert::Infallible,
    fmt::{Debug, Display},
    hash::Hash,
    str::FromStr,
    sync::Arc,
    time::SystemTime,
};

pub use ulid::Ulid;

use crate::{
    service_fn::from_request::FromRequest,
    task::{data::MissingDataError, Task},
};

pub use unique_id::UniqueId;

/// A wrapper type that defines a task id.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct TaskId<IdType = Ulid>(Arc<IdType>);

impl<IdType> Clone for TaskId<IdType> {
    fn clone(&self) -> Self {
        TaskId(self.0.clone())
    }
}

impl<IdType> TaskId<IdType> {
    /// Generate a new [`TaskId`]
    pub fn new(id: IdType) -> Self {
        Self(Arc::new(id))
    }
    /// Get the inner [`IdType`]
    pub fn inner(&self) -> &IdType {
        &self.0
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
        Ok(TaskId::new(
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

impl<Args: Sync, Ctx: Sync, IdType: Sync + Send> FromRequest<Task<Args, Ctx, IdType>>
    for TaskId<IdType>
{
    type Error = MissingDataError;
    async fn from_request(req: &Task<Args, Ctx, IdType>) -> Result<Self, Self::Error> {
        Ok(req
            .meta
            .task_id
            .clone()
            .ok_or(MissingDataError::MissingTaskIdentifier(
                std::any::type_name::<Ctx>(),
            ))?)
    }
}

mod unique_id {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    const ALPHABET: &[u8] = b"abcdefghijkmnopqrstuvwxyz23456789-";
    const BASE: u64 = 34;
    const TIME_LEN: usize = 6;
    const RANDOM_LEN: usize = 5;

    /// Consider using a ulid/uuid/nanoid in backend implementation
    pub struct UniqueId(String);

    impl Default for UniqueId {
        fn default() -> Self {
            UniqueId(unique_id())
        }
    }

    // Atomic counter to ensure uniqueness within same millisecond
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    /// Converts a number to base-64 using the NanoID alphabet.
    fn encode_base64(mut value: u64, length: usize) -> String {
        let mut buf = vec![b'A'; length];
        for i in (0..length).rev() {
            buf[i] = ALPHABET[(value % BASE) as usize];
            value /= BASE;
        }
        String::from_utf8(buf).unwrap()
    }

    /// Generates a unique, time-ordered NanoID-style string (zero-deps).
    pub fn unique_id() -> String {
        let timestamp = current_time_millis();
        let time_str = encode_base64(timestamp, TIME_LEN);

        // Counter ensures uniqueness across fast calls
        let count = COUNTER.fetch_add(1, Ordering::Relaxed);
        let rand_part = encode_base64(xorshift64(timestamp ^ count), RANDOM_LEN);

        format!("{time_str}{rand_part}")
    }

    /// Returns current time in milliseconds since UNIX epoch.
    fn current_time_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    /// Simple xorshift PRNG
    fn xorshift64(mut x: u64) -> u64 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    }
}
