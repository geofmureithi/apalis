use futures::future;
use tower::retry::Policy;

use apalis_core::{error::Error, request::Request};
/// Re-export from [`RetryLayer`]
///
/// [`RetryLayer`]: tower::retry::RetryLayer
pub use tower::retry::RetryLayer;

type Req<T, Ctx> = Request<T, Ctx>;
type Err = Error;

/// Retries a task instantly for `retries`
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    retries: usize,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self { retries: 25 }
    }
}

impl RetryPolicy {
    /// Set the number of replies
    pub fn retries(count: usize) -> Self {
        Self { retries: count }
    }
}

impl<T, Res, Ctx> Policy<Req<T, Ctx>, Res, Err> for RetryPolicy
where
    T: Clone,
    Ctx: Clone,
{
    type Future = future::Ready<Self>;

    fn retry(&self, req: &Req<T, Ctx>, result: Result<&Res, &Err>) -> Option<Self::Future> {
        let attempt = &req.parts.attempt;
        match result {
            Ok(_) => {
                // Treat all `Response`s as success,
                // so don't retry...
                None
            }
            Err(_) if self.retries == 0 => None,
            Err(_) if (self.retries - attempt.current() > 0) => Some(future::ready(self.clone())),
            Err(_) => None,
        }
    }

    fn clone_request(&self, req: &Req<T, Ctx>) -> Option<Req<T, Ctx>> {
        let req = req.clone();
        req.parts.attempt.increment();
        Some(req)
    }
}
