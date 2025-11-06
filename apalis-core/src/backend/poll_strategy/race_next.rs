use core::fmt;
use futures_core::Stream;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

/// A stream that polls multiple streams, always returning the first ready item,
/// and skipping one item from all other streams each round.
pub struct RaceNext<T> {
    streams: Vec<Option<Pin<Box<dyn Stream<Item = T> + Send>>>>,
    pending_skips: Vec<bool>,
}

impl<T> fmt::Debug for RaceNext<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RaceNext")
            .field("active_streams", &self.active_count())
            .finish()
    }
}

impl<T: 'static + Send> RaceNext<T> {
    /// Create a new RaceNext stream from a vector of streams
    pub fn new(streams: Vec<Pin<Box<dyn Stream<Item = T> + Send>>>) -> Self {
        let len = streams.len();
        Self {
            streams: streams.into_iter().map(Some).collect(),
            pending_skips: vec![false; len],
        }
    }
}

impl<T: 'static + Send> Stream for RaceNext<T> {
    type Item = (usize, T);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();

        // First, handle any pending skips from the previous round
        for i in 0..this.streams.len() {
            if this.pending_skips[i] {
                if let Some(ref mut stream) = this.streams[i] {
                    match stream.as_mut().poll_next(cx) {
                        Poll::Ready(Some(_)) => {
                            // Successfully skipped an item
                            this.pending_skips[i] = false;
                        }
                        Poll::Ready(None) => {
                            // Stream ended while trying to skip
                            this.streams[i] = None;
                            this.pending_skips[i] = false;
                        }
                        Poll::Pending => {
                            // Still waiting to skip, continue to next stream
                            continue;
                        }
                    }
                }
            }
        }

        // Now poll for the next ready item
        let mut any_pending = false;
        for i in 0..this.streams.len() {
            // Skip streams that are still pending a skip operation
            if this.pending_skips[i] {
                any_pending = true;
                continue;
            }

            if let Some(ref mut stream) = this.streams[i] {
                match stream.as_mut().poll_next(cx) {
                    Poll::Ready(Some(item)) => {
                        // Found a ready item! Mark other streams for skipping
                        for j in 0..this.streams.len() {
                            if j != i && this.streams[j].is_some() {
                                this.pending_skips[j] = true;
                            }
                        }
                        return Poll::Ready(Some((i, item)));
                    }
                    Poll::Ready(None) => {
                        // This stream ended, remove it
                        this.streams[i] = None;
                    }
                    Poll::Pending => {
                        any_pending = true;
                    }
                }
            }
        }

        // Check if all streams are exhausted
        if this.streams.iter().all(|s| s.is_none()) {
            return Poll::Ready(None);
        }

        if any_pending {
            Poll::Pending
        } else {
            // All remaining streams are exhausted
            Poll::Ready(None)
        }
    }
}

impl<T> RaceNext<T> {
    /// Returns the number of active streams remaining
    pub fn active_count(&self) -> usize {
        self.streams.iter().filter(|s| s.is_some()).count()
    }

    /// Checks if any streams are still active
    pub fn has_active_streams(&self) -> bool {
        self.streams.iter().any(|s| s.is_some())
    }
}
