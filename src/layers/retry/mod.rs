use futures::future;
use tower::retry::Policy;

/// Re-export from [`RetryLayer`]
///
/// [`RetryLayer`]: tower::retry::RetryLayer
pub use tower::retry::RetryLayer;

use apalis_core::task::attempt::Attempt;
use apalis_core::{error::Error, request::Request};

type Req<T> = Request<T>;
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

impl<T, Res> Policy<Req<T>, Res, Err> for RetryPolicy
where
    T: Clone,
{
    type Future = future::Ready<Self>;

    fn retry(&self, req: &Req<T>, result: Result<&Res, &Err>) -> Option<Self::Future> {
        let ctx = req.get::<Attempt>().cloned().unwrap_or_default();
        match result {
            Ok(_) => {
                // Treat all `Response`s as success,
                // so don't retry...
                None
            }
            Err(_) if (self.retries - ctx.current() > 0) => Some(future::ready(self.clone())),
            Err(_) => None,
        }
    }

    fn clone_request(&self, req: &Req<T>) -> Option<Req<T>> {
        let mut req = req.clone();
        let value = req
            .get::<Attempt>()
            .cloned()
            .map(|attempt| {
                attempt.increment();
                attempt
            })
            .unwrap_or_default();
        req.insert(value);
        Some(req)
    }
}
