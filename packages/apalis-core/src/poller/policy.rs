use std::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use futures::Stream;
use pin_project_lite::pin_project;

/// A trait representing how a backend fetches the next batch of items
pub trait FetchNext<Item> {
    /// The type of error that returned by the fetch next operation
    type Error;

    /// The future to be polled to get the next batch of items
    type Future: Future<Output = Result<Vec<Item>, Self::Error>> + Send;

    /// Provides the future to poll the next batch of items
    /// If None is returned, the stream is considered as exhausted
    fn fetch_next(&mut self) -> Option<Self::Future>;
}

/// A trait defining when `fetch_next` should be called.
pub trait FetchPolicy {
    /// The output of our fetch next operation
    type Item;
    /// Returns true if the next fetch operation should be called
    fn should_fetch(&mut self, cx: &mut Context<'_>) -> bool;

    /// Allows the policy adjust appropriately based on the result
    #[allow(unused_variables)]
    fn on_fetch(&mut self, items: &Vec<Self::Item>) {
        // we don't have to always care
    }
}

pin_project! {
    /// A Stream that polls based on the FetchPolicy.
    pub struct FetchStream<N, P>
    where
        N: FetchNext<P::Item>,
        P: FetchPolicy,
    {
        fetcher: N,
        policy: P,
        #[pin]
        pending_future: Option<N::Future>,
    }
}

impl<N, P> FetchStream<N, P>
where
    N: FetchNext<P::Item> + Unpin,
    P: FetchPolicy,
{
    /// Create a new stream controlled by the policy
    pub fn new(fetcher: N, policy: P) -> Self {
        Self {
            fetcher,
            policy,
            pending_future: None,
        }
    }
}

impl<N, P, E> Stream for FetchStream<N, P>
where
    N: FetchNext<P::Item, Error = E> + Unpin,
    P: FetchPolicy,
{
    type Item = Result<Vec<P::Item>, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if let Some(fut) = this.pending_future.as_mut().as_pin_mut() {
            match fut.poll(cx) {
                Poll::Ready(Ok(items)) => {
                    this.pending_future.set(None);
                    this.policy.on_fetch(&items);

                    return Poll::Ready(Some(Ok(items)));
                }
                Poll::Ready(Err(e)) => {
                    this.pending_future.set(None);

                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        if this.policy.should_fetch(cx) {
            if let Some(fut) = this.fetcher.fetch_next() {
                this.pending_future.set(Some(fut));
            } else {
                // We have exhausted our stream
                // or our policy wants to stop fetching
                return Poll::Ready(None);
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// A backoff-based schedule.
#[derive(Debug, Clone)]
pub struct BackoffSchedule<Item> {
    base_delay: Duration,
    current_delay: Duration,
    last_fetch: Option<Instant>,
    item: PhantomData<Item>,
}

impl<Item> BackoffSchedule<Item> {
    /// Create a basic backoff approach
    pub fn new(base_delay: Duration) -> Self {
        Self {
            base_delay,
            current_delay: base_delay,
            last_fetch: None,
            item: PhantomData,
        }
    }
}

impl<Item> FetchPolicy for BackoffSchedule<Item> {
    type Item = Item;

    fn should_fetch(&mut self, cx: &mut Context<'_>) -> bool {
        if let Some(last) = self.last_fetch {
            if last.elapsed() < self.current_delay {
                cx.waker().wake_by_ref();
                return false;
            }
        }
        true
    }

    fn on_fetch(&mut self, items: &Vec<Self::Item>) {
        match items.len() > 0 {
            true => {
                self.current_delay = self.base_delay;
                self.last_fetch = Some(Instant::now());
            }
            false => {
                self.current_delay *= 2;
            }
        }
    }
}
