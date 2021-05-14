use crate::consumer::RedisConsumer;
use actix::prelude::*;
use actix::Context;
use apalis::{Job, JobHandler};

use log::{debug, warn};

use actix_rt::time::Interval;
use std::pin::Pin;
use std::task::{Context as StdContext, Poll};

#[derive(Message)]
#[rtype(result = "()")]
pub struct HeartBeat;

pub struct HeartBeatStream {
    interval: Interval,
}

impl HeartBeatStream {
    pub fn new(interval: Interval) -> Self {
        HeartBeatStream { interval }
    }
}

impl Stream for HeartBeatStream {
    type Item = HeartBeat;

    fn poll_next(self: Pin<&mut Self>, cx: &mut StdContext<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut()
            .interval
            .poll_tick(cx)
            .map(|_| Some(HeartBeat))
    }
}

impl<J: 'static + Unpin + JobHandler<Self>> StreamHandler<HeartBeat> for RedisConsumer<J>
where
    J: Job,
{
    fn handle(&mut self, _: HeartBeat, _: &mut Context<RedisConsumer<J>>) {
        debug!(
            "Received heartbeat for RedisConsumer: {} in {:?}",
            self.id(),
            std::thread::current().id()
        );
    }

    fn finished(&mut self, _: &mut Self::Context) {
        warn!("Heartbeat for consumer: {:?} stopped", self.id());
    }
}
