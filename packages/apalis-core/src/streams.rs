use std::{
    pin::Pin,
    task::{Context, Poll},
};

use actix::clock::Interval;
use futures::Stream;

use crate::worker::WorkerPulse;

pub struct HeartbeatStream {
    interval: Interval,
    beat: WorkerPulse,
}

impl HeartbeatStream {
    pub fn new(beat: WorkerPulse, interval: Interval) -> Self {
        HeartbeatStream { beat, interval }
    }
}
impl Stream for HeartbeatStream {
    type Item = WorkerPulse;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let beat = self.beat.clone();
        self.get_mut().interval.poll_tick(cx).map(|_| Some(beat))
    }
}

pub struct FetchJobStream {
    interval: Interval,
}

pub struct FetchJob;

impl FetchJobStream {
    pub fn new(interval: Interval) -> Self {
        FetchJobStream { interval }
    }
}

impl Stream for FetchJobStream {
    type Item = FetchJob;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut()
            .interval
            .poll_tick(cx)
            .map(|_| Some(FetchJob))
    }
}
