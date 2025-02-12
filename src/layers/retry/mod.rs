use futures::{future::BoxFuture, FutureExt};
use std::fmt;
use std::sync::Arc;
use tower::retry::backoff::Backoff;

use apalis_core::{error::Error, request::Request};

/// Re-exports from [`tower::retry`]
pub use tower::retry::*;
/// Re-exports from [`tower::util`]
pub use tower::util::rng::HasherRng;

type Req<T, Ctx> = Request<T, Ctx>;
type Err = Error;

/// Retries a task with backoff
#[derive(Clone, Debug)]
pub struct BackoffRetryPolicy<B> {
    retries: usize,
    backoff: B,
}

impl<B> BackoffRetryPolicy<B> {
    /// Build a new retry policy with backoff
    pub fn new(retries: usize, backoff: B) -> Self {
        Self { retries, backoff }
    }
}

impl<T, Res, Ctx, B> Policy<Req<T, Ctx>, Res, Err> for BackoffRetryPolicy<B>
where
    T: Clone,
    Ctx: Clone,
    B: Backoff,
    B::Future: Send + 'static,
{
    type Future = BoxFuture<'static, ()>;

    fn retry(
        &mut self,
        req: &mut Req<T, Ctx>,
        result: &mut Result<Res, Err>,
    ) -> Option<Self::Future> {
        let attempt = req.parts.attempt.current();
        match result.as_mut() {
            Ok(_) => {
                // Treat all `Response`s as success,
                // so don't retry...
                None
            }
            Err(Err::Abort(_)) => return None,
            Err(err) => {
                if self.retries == 0 {
                    *err = Err::Abort(Arc::new(Box::new(RetryPolicyError::ZeroRetries(
                        err.clone(),
                    ))));
                    return None;
                } else if self.retries >= attempt {
                    let counter = req.parts.attempt.clone();
                    return Some(Box::pin(self.backoff.next_backoff().map(move |_| {
                        counter.increment();
                    })));
                } else {
                    *err = Err::Abort(Arc::new(Box::new(RetryPolicyError::OutOfRetries {
                        current_attempt: attempt,
                        inner: err.clone(),
                    })));
                    return None;
                }
            }
        }
    }

    fn clone_request(&mut self, req: &Req<T, Ctx>) -> Option<Req<T, Ctx>> {
        let req = req.clone();
        Some(req)
    }
}

/// Retries a task instantly for `retries`
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    retries: usize,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self { retries: 5 }
    }
}

impl RetryPolicy {
    /// Set the number of replies
    pub fn retries(retries: usize) -> Self {
        Self { retries }
    }

    /// Include a backoff to the retry policy
    pub fn with_backoff<B: Backoff>(self, backoff: B) -> BackoffRetryPolicy<B> {
        BackoffRetryPolicy {
            retries: self.retries,
            backoff,
        }
    }
}

impl<T, Res, Ctx> Policy<Req<T, Ctx>, Res, Err> for RetryPolicy
where
    T: Clone,
    Ctx: Clone,
{
    type Future = std::future::Ready<()>;

    fn retry(
        &mut self,
        req: &mut Req<T, Ctx>,
        result: &mut Result<Res, Err>,
    ) -> Option<Self::Future> {
        let attempt = req.parts.attempt.current();
        match result.as_mut() {
            Ok(_) => {
                // Treat all `Response`s as success,
                // so don't retry...
                None
            }
            Err(Err::Abort(_)) => return None,
            Err(err) => {
                if self.retries == 0 {
                    *err = Err::Abort(Arc::new(Box::new(RetryPolicyError::ZeroRetries(
                        err.clone(),
                    ))));
                    return None;
                } else if self.retries >= attempt {
                    req.parts.attempt.increment();
                    return Some(std::future::ready(()));
                } else {
                    *err = Err::Abort(Arc::new(Box::new(RetryPolicyError::OutOfRetries {
                        current_attempt: attempt,
                        inner: err.clone(),
                    })));
                    return None;
                }
            }
        }
    }

    fn clone_request(&mut self, req: &Req<T, Ctx>) -> Option<Req<T, Ctx>> {
        let req = req.clone();
        Some(req)
    }
}

/// The error returned if the [RetryPolicy] forces the task to abort
/// Works by wrapping the inner error [Error::Abort]
#[derive(Debug)]
pub enum RetryPolicyError {
    /// Attempted all the retries allocated
    OutOfRetries {
        /// The current attempt
        current_attempt: usize,
        /// The last error to occur
        inner: Error,
    },
    /// Retries forbidden
    ZeroRetries(Error),
}

impl fmt::Display for RetryPolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RetryPolicyError::OutOfRetries {
                current_attempt,
                inner,
            } => {
                write!(
                    f,
                    "RetryPolicyError: Out of retries after {} attempts: {}",
                    current_attempt, inner
                )
            }
            RetryPolicyError::ZeroRetries(inner) => {
                write!(f, "RetryPolicyError: Zero retries allowed: {}", inner)
            }
        }
    }
}

impl std::error::Error for RetryPolicyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            RetryPolicyError::OutOfRetries { inner, .. } => inner.source(),
            RetryPolicyError::ZeroRetries(inner) => inner.source(),
        }
    }
}
