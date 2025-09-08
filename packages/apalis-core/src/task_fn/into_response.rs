//! Traits for converting types into responses for task functions.

use crate::error::BoxDynError;

/// Trait for generating responses.
///
/// Types that implement `IntoResponse` can be returned from a task fn.
///
/// # Implementing `IntoResponse`
///
/// You generally shouldn't have to implement `IntoResponse` manually, as apalis
/// provides implementations for many common types.
///
/// However it might be necessary if you have a custom error type that you want
/// to return from a task fn:
///
/// ```rust
/// # use apalis_core::service_fn::into_response::IntoResponse;
/// # use apalis_core::error::BoxDynError;
/// enum CustomResult {
///     Success(String),
///     Recoverable(BoxDynError),
///     Failure(BoxDynError),
/// }
/// impl IntoResponse for MyError {
///     type Output = String;
///     fn into_response(self) -> Result<Self::Output, BoxDynError> {
///         match self {
///             CustomResult::Success(val) => Ok(val),
///             CustomResult::Recoverable(err) => Err(err),
///             CustomResult::Failure(err) => Err(err),
///         }
///    }    
/// }
///
/// async fn my_task() -> CustomResult {
///     CustomResult::Success("All good".to_string())
/// }
/// ```
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
