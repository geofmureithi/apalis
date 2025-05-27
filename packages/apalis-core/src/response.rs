use std::{fmt::Debug, sync::Arc};

use crate::{
    error::{BoxDynError, Error},
    task::{attempt::Attempt, task_id::TaskId},
};

/// A generic `Response` struct that wraps the result of a task, containing the outcome (`Ok` or `Err`),
/// task metadata such as `task_id`, `attempt`, and an internal marker field for future extensions.
///
/// # Type Parameters
/// - `Res`: The successful result type of the response.
///
/// # Fields
/// - `inner`: A `Result` that holds either the success value of type `Res` or an `Error` on failure.
/// - `task_id`: A `TaskId` representing the unique identifier for the task.
/// - `attempt`: An `Attempt` representing how many attempts were made to complete the task.
/// - `_priv`: A private marker field to prevent external construction of the `Response`.
#[derive(Debug, Clone)]
pub struct Response<Res> {
    /// The result from a task
    pub inner: Result<Res, Error>,
    /// The task id
    pub task_id: TaskId,
    /// The current attempt
    pub attempt: Attempt,
    pub(crate) _priv: (),
}

impl<Res> Response<Res> {
    /// Creates a new `Response` instance.
    ///
    /// # Arguments
    /// - `inner`: A `Result` holding either a successful response of type `Res` or an `Error`.
    /// - `task_id`: A `TaskId` representing the unique identifier for the task.
    /// - `attempt`: The attempt count when creating this response.
    ///
    /// # Returns
    /// A new `Response` instance.
    pub fn new(inner: Result<Res, Error>, task_id: TaskId, attempt: Attempt) -> Self {
        Response {
            inner,
            task_id,
            attempt,
            _priv: (),
        }
    }

    /// Constructs a successful `Response`.
    ///
    /// # Arguments
    /// - `res`: The success value of type `Res`.
    /// - `task_id`: A `TaskId` representing the unique identifier for the task.
    /// - `attempt`: The attempt count when creating this response.
    ///
    /// # Returns
    /// A `Response` instance containing the success value.
    pub fn success(res: Res, task_id: TaskId, attempt: Attempt) -> Self {
        Self::new(Ok(res), task_id, attempt)
    }

    /// Constructs a failed `Response`.
    ///
    /// # Arguments
    /// - `error`: The `Error` that occurred.
    /// - `task_id`: A `TaskId` representing the unique identifier for the task.
    /// - `attempt`: The attempt count when creating this response.
    ///
    /// # Returns
    /// A `Response` instance containing the error.
    pub fn failure(error: Error, task_id: TaskId, attempt: Attempt) -> Self {
        Self::new(Err(error), task_id, attempt)
    }

    /// Checks if the `Response` contains a success (`Ok`).
    ///
    /// # Returns
    /// `true` if the `Response` is successful, `false` otherwise.
    pub fn is_success(&self) -> bool {
        self.inner.is_ok()
    }

    /// Checks if the `Response` contains a failure (`Err`).
    ///
    /// # Returns
    /// `true` if the `Response` is a failure, `false` otherwise.
    pub fn is_failure(&self) -> bool {
        self.inner.is_err()
    }

    /// Maps the success value (`Res`) of the `Response` to another type using the provided function.
    ///
    /// # Arguments
    /// - `f`: A function that takes a reference to the success value and returns a new value of type `T`.
    ///
    /// # Returns
    /// A new `Response` with the transformed success value or the same error.
    ///
    /// # Type Parameters
    /// - `F`: A function or closure that takes a reference to a value of type `Res` and returns a value of type `T`.
    /// - `T`: The new type of the success value after mapping.
    pub fn map<F, T>(&self, f: F) -> Response<T>
    where
        F: FnOnce(&Res) -> T,
    {
        Response {
            inner: self.inner.as_ref().map(f).map_err(|e| e.clone()),
            task_id: self.task_id.clone(),
            attempt: self.attempt.clone(),
            _priv: (),
        }
    }
}

/// Helper for Job Responses
pub trait IntoResponse {
    /// The final result of the job
    type Result;
    /// converts self into a Result
    fn into_response(self) -> Self::Result;
}

impl IntoResponse for bool {
    type Result = std::result::Result<Self, Error>;
    fn into_response(self) -> std::result::Result<Self, Error> {
        match self {
            true => Ok(true),
            false => Err(Error::Failed(Arc::new(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Job returned false",
            ))))),
        }
    }
}

impl<T, E: Into<BoxDynError>> IntoResponse for std::result::Result<T, E> {
    type Result = Result<T, Error>;
    fn into_response(self) -> Result<T, Error> {
        match self {
            Ok(value) => Ok(value),
            Err(e) => {
                let e = e.into();
                if let Some(custom_error) = e.downcast_ref::<Error>() {
                    return Err(custom_error.clone());
                }
                Err(Error::Failed(Arc::new(e)))
            }
        }
    }
}

macro_rules! SIMPLE_JOB_RESULT {
    ($type:ty) => {
        impl IntoResponse for $type {
            type Result = std::result::Result<$type, Error>;
            fn into_response(self) -> std::result::Result<$type, Error> {
                Ok(self)
            }
        }
    };
}

SIMPLE_JOB_RESULT!(());
SIMPLE_JOB_RESULT!(u8);
SIMPLE_JOB_RESULT!(u16);
SIMPLE_JOB_RESULT!(u32);
SIMPLE_JOB_RESULT!(u64);
SIMPLE_JOB_RESULT!(usize);
SIMPLE_JOB_RESULT!(i8);
SIMPLE_JOB_RESULT!(i16);
SIMPLE_JOB_RESULT!(i32);
SIMPLE_JOB_RESULT!(i64);
SIMPLE_JOB_RESULT!(isize);
SIMPLE_JOB_RESULT!(f32);
SIMPLE_JOB_RESULT!(f64);
SIMPLE_JOB_RESULT!(String);
SIMPLE_JOB_RESULT!(&'static str);
