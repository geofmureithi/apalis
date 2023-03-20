use futures::future;
use tower::retry::Policy;

/// Re-export from [tower::retry::RetryLayer]
pub use tower::retry::RetryLayer;

use crate::{error::JobError, request::JobRequest};

type Req<T> = JobRequest<T>;
type Err = JobError;

/// Retries a job instantly until max_attempts
#[derive(Clone, Debug)]
pub struct DefaultRetryPolicy;

impl<T, Res> Policy<Req<T>, Res, Err> for DefaultRetryPolicy
where
    T: Clone,
{
    type Future = future::Ready<Self>;

    fn retry(&self, req: &Req<T>, result: Result<&Res, &Err>) -> Option<Self::Future> {
        match result {
            Ok(_) => {
                // Treat all `Response`s as success,
                // so don't retry...
                None
            }
            Err(_) if (req.max_attempts() - req.attempts() > 0) => {
                Some(future::ready(DefaultRetryPolicy))
            }
            Err(_) => None,
        }
    }

    fn clone_request(&self, req: &Req<T>) -> Option<Req<T>> {
        let mut req = req.clone();
        req.record_attempt();
        Some(req)
    }
}
