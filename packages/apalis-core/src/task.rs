/// A unique tracker for number of attempts
pub mod attempt {
    use std::{
        convert::Infallible,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::{request::Request, service_fn::FromRequest};

    /// A wrapper to keep count of the attempts tried by a task
    #[derive(Debug, Clone)]
    pub struct Attempt(Arc<AtomicUsize>);

    // Custom serialization function
    fn serialize<S>(attempt: &Attempt, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let value = attempt.0.load(Ordering::SeqCst);
        serializer.serialize_u64(value as u64)
    }

    // Custom deserialization function
    fn deserialize<'de, D>(deserializer: D) -> Result<Attempt, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u64::deserialize(deserializer)?;
        Ok(Attempt(Arc::new(AtomicUsize::new(value as usize))))
    }

    impl Serialize for Attempt {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serialize(self, serializer)
        }
    }

    impl<'de> Deserialize<'de> for Attempt {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserialize(deserializer)
        }
    }

    impl Default for Attempt {
        fn default() -> Self {
            Self(Arc::new(AtomicUsize::new(0)))
        }
    }

    impl Attempt {
        /// Build a new tracker
        pub fn new() -> Self {
            Self::default()
        }

        /// Build a tracker from an existing value
        pub fn new_with_value(value: usize) -> Self {
            Self(Arc::new(AtomicUsize::from(value)))
        }

        /// Get the current value
        pub fn current(&self) -> usize {
            self.0.load(std::sync::atomic::Ordering::Relaxed)
        }

        /// Increase the current value
        pub fn increment(&self) -> usize {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        }
    }

    impl<Req: Sync, Ctx: Sync> FromRequest<Request<Req, Ctx>> for Attempt {
        type Error = Infallible;
        async fn from_request(req: &Request<Req, Ctx>) -> Result<Self, Self::Error> {
            Ok(req.parts.attempt.clone())
        }
    }
}
/// A unique ID that can be used by a backend
pub mod task_id {
    use std::{
        convert::Infallible,
        fmt::{Debug, Display},
        hash::Hash,
        str::FromStr,
    };

    use serde::{de::Visitor, Deserialize, Deserializer, Serialize, Serializer};
    use ulid::Ulid;

    use crate::{data::MissingDataError, request::Request, service_fn::FromRequest};

    /// A wrapper type that defines a task id.
    #[derive(Debug, Clone, Eq, Hash, PartialEq)]
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
}
