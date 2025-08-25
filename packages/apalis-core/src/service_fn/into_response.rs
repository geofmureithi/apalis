use std::convert::Infallible;

use crate::error::BoxDynError;

/// Helper for Job Responses
pub trait IntoResponse {
    /// The final result of the job
    type Output;

    /// converts self into a Result
    fn into_response(self) -> Result<Self::Output, BoxDynError>;
}

impl IntoResponse for bool {
    type Output = bool;
    fn into_response(self) -> Result<bool, BoxDynError> {
        match self {
            true => Ok(true),
            false => Err("Task returned false".into()),
        }
    }
}

impl<T> IntoResponse for Option<T> {
    type Output = Option<T>;
    fn into_response(self) -> Result<Option<T>, BoxDynError> {
        Ok(self)
    }
}

impl<T, E: Into<BoxDynError> + Send + 'static> IntoResponse for std::result::Result<T, E> {
    type Output = T;
    fn into_response(self) -> Result<T, BoxDynError> {
        match self {
            Ok(value) => Ok(value),
            Err(err) => Err(err.into()),
        }
    }
}

macro_rules! SIMPLE_JOB_RESULT {
    ($type:ty) => {
        impl IntoResponse for $type {
            type Output = $type;
            fn into_response(self) -> std::result::Result<$type, BoxDynError> {
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
