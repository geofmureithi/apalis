use futures::future;
use tower::retry::Policy;

/// Re-export from [`tower::retry::RetryLayer`]
pub use tower::retry::RetryLayer;

use crate::{error::Error, request::Request, storage::context::Context};

type Req<T> = Request<T>;
type Err = Error;

/// Retries a job instantly until `max_attempts`
#[derive(Clone, Debug)]
pub struct DefaultRetryPolicy;

impl<T, Res> Policy<Req<T>, Res, Err> for DefaultRetryPolicy
where
    T: Clone,
{
    type Future = future::Ready<Self>;

    fn retry(&self, req: &Req<T>, result: Result<&Res, &Err>) -> Option<Self::Future> {
        let ctx = req.get::<Context>()?;
        match result {
            Ok(_) => {
                // Treat all `Response`s as success,
                // so don't retry...
                None
            }
            Err(_) if (ctx.max_attempts() - ctx.attempts() > 0) => {
                Some(future::ready(DefaultRetryPolicy))
            }
            Err(_) => None,
        }
    }

    fn clone_request(&self, req: &Req<T>) -> Option<Req<T>> {
        let mut req = req.clone();
        let ctx = req.get_mut::<Context>()?;
        ctx.record_attempt();
        Some(req)
    }
}
