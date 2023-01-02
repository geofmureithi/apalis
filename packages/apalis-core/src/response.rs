use crate::error::JobError;
use std::{any::Any, error::Error, time::Duration};

/// Represents a non-error result for a [Job] or [JobFn] service.
///
/// Any job should return this as a result to control a jobs outcome.
#[derive(Debug, Clone)]
pub enum JobResult {
    /// Job successfully completed
    Success,
    /// Job needs to be manually retried.
    Retry,
    /// Job was complete as a result of being killed
    Kill,
    /// Return job back and process it in [Duration]
    Reschedule(Duration),
}

impl std::fmt::Display for JobResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let text = match self {
            JobResult::Success => "Success",
            JobResult::Retry => "Retry",
            JobResult::Kill => "Kill",
            JobResult::Reschedule(_) => "Reschedule",
        };
        f.write_str(text)
    }
}

/// Helper for Job Responses
pub trait IntoJobResponse {
    /// converts self into a Result
    fn into_response(self) -> Result<JobResult, JobError>;
}

impl IntoJobResponse for bool {
    fn into_response(self) -> Result<JobResult, JobError> {
        match self {
            true => Ok(JobResult::Success),
            false => Err(JobError::Unknown),
        }
    }
}

impl<T: Any, E: 'static + Error + Send + Sync> IntoJobResponse for Result<T, E> {
    fn into_response(self) -> Result<JobResult, JobError> {
        match self {
            Ok(value) => {
                let value_any = &value as &dyn Any;
                match value_any.downcast_ref::<JobResult>() {
                    Some(res) => Ok(res.clone()),
                    None => Ok(JobResult::Success),
                }
            }
            Err(e) => Err(JobError::Failed(Box::new(e))),
        }
    }
}

macro_rules! SIMPLE_JOB_RESULT {
    ($type:ty) => {
        impl IntoJobResponse for $type {
            fn into_response(self) -> Result<JobResult, JobError> {
                Ok(JobResult::Success)
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
