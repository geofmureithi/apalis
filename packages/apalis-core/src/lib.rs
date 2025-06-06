#![crate_name = "apalis_core"]
#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub,
    bad_style,
    dead_code,
    improper_ctypes,
    non_shorthand_field_patterns,
    no_mangle_generic_items,
    overflowing_literals,
    path_statements,
    patterns_in_fns_without_body,
    unconditional_recursion,
    unused,
    unused_allocation,
    unused_comparisons,
    unused_parens,
    while_true
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
//! # apalis-core
//! Utilities for building job and message processing tools.
/// Represent utilities for creating worker instances.
pub mod builder;

/// Represents a task source eg Postgres or Redis
pub mod backend;
/// Includes all possible error types.
pub mod error;
/// Represents monitoring of running workers
pub mod monitor;
/// Represents the request to be processed.
pub mod request;
/// Represents different possible responses.
pub mod response;
/// Represents a service that is created from a function.
pub mod service_fn;
/// Represents the utils for building workers.
pub mod worker;

pub mod shared;

/// Represents the utils needed to extend a task's context.
pub mod data;

/// In-memory utilities for testing and mocking
pub mod memory;

/// Task management utilities
pub mod task;

/// Codec for handling data
pub mod codec;

/// Allows stepped tasks
pub mod stepped;

/// Shutdown utilities
pub mod shutdown;

pub mod ext;

/// Sleep utilities
#[cfg(feature = "sleep")]
pub async fn sleep(duration: std::time::Duration) {
    futures_timer::Delay::new(duration).await;
}

#[cfg(feature = "sleep")]
/// Interval utilities
pub mod interval {
    use std::fmt;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use futures::future::BoxFuture;
    use futures::Stream;

    use crate::sleep;
    /// Creates a new stream that yields at a set interval.
    pub fn interval(duration: Duration) -> Interval {
        Interval {
            timer: Box::pin(sleep(duration)),
            interval: duration,
        }
    }

    /// A stream representing notifications at fixed interval
    #[must_use = "streams do nothing unless polled or .awaited"]
    pub struct Interval {
        timer: BoxFuture<'static, ()>,
        interval: Duration,
    }

    impl fmt::Debug for Interval {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("Interval")
                .field("interval", &self.interval)
                .field("timer", &"a future represented `apalis_core::sleep`")
                .finish()
        }
    }

    impl Stream for Interval {
        type Item = ();

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            match Pin::new(&mut self.timer).poll(cx) {
                Poll::Ready(_) => {}
                Poll::Pending => return Poll::Pending,
            };
            let interval = self.interval;
            let fut = std::mem::replace(&mut self.timer, Box::pin(sleep(interval)));
            drop(fut);
            Poll::Ready(Some(()))
        }
    }
}

#[cfg(feature = "test-utils")]
#[allow(unused)]
/// Test utilities that allows you to test backends
pub mod test_utils;
