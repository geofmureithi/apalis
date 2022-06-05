use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use tokio::time::Interval;

use crate::storage::StorageWorkerPulse;

use super::worker::KeepAlive;

pub(crate) struct HeartbeatStream {
    interval: Interval,
    beat: StorageWorkerPulse,
}

impl HeartbeatStream {
    pub(crate) fn new(beat: StorageWorkerPulse, interval: Interval) -> Self {
        HeartbeatStream { beat, interval }
    }
}
impl Stream for HeartbeatStream {
    type Item = StorageWorkerPulse;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let beat = self.beat.clone();
        self.get_mut().interval.poll_tick(cx).map(|_| Some(beat))
    }
}

pub(crate) struct KeepAliveStream {
    interval: Interval,
}

impl KeepAliveStream {
    pub(crate) fn new(interval: Interval) -> Self {
        KeepAliveStream { interval }
    }
}

impl Stream for KeepAliveStream {
    type Item = KeepAlive;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut()
            .interval
            .poll_tick(cx)
            .map(|_| Some(KeepAlive))
    }
}
