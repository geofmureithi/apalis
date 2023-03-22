use std::any::Any;

use tower::BoxError;

use crate::error::JobError;

/// Helper for Job Responses
pub trait IntoResponse {
    /// The final result of the job
    type Result;
    /// converts self into a Result
    fn into_response(self) -> Self::Result;
}

impl IntoResponse for bool {
    type Result = std::result::Result<Self, JobError>;
    fn into_response(self) -> std::result::Result<Self, JobError> {
        match self {
            true => Ok(true),
            false => Err(JobError::Failed(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Job returned false",
            )))),
        }
    }
}

impl<T: Any, E: Into<BoxError> + Send + Sync> IntoResponse for std::result::Result<T, E> {
    type Result = Result<T, JobError>;
    fn into_response(self) -> Result<T, JobError> {
        match self {
            Ok(value) => Ok(value),
            Err(e) => Err(JobError::Failed(e.into())),
        }
    }
}

macro_rules! SIMPLE_JOB_RESULT {
    ($type:ty) => {
        impl IntoResponse for $type {
            type Result = std::result::Result<$type, JobError>;
            fn into_response(self) -> std::result::Result<$type, JobError> {
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
