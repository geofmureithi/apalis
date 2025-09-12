use std::{
    fmt::Display,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::SystemTime,
};

use apalis_core::{
    backend::{Backend, TaskStream},
    features_table,
    layers::Identity,
    task::{builder::TaskBuilder, task_id::TaskId, Task},
    timer::Delay,
    worker::context::WorkerContext,
};
use chrono::{DateTime, TimeZone, Utc};
use futures_util::{
    stream::{self, BoxStream},
    Stream, StreamExt, TryStreamExt,
};
use ulid::Ulid;

use crate::{context::CronContext, error::CronStreamError, schedule::Schedule, tick::Tick};

/// Represents a stream from a cron schedule with a timezone
#[doc = features_table! {
    setup = unreachable!();,
    TaskSink => not_supported("You cannot push tasks to a cron stream"),
}]
#[derive(Debug)]
pub struct CronStream<S: Schedule<Timezone>, Timezone: chrono::TimeZone> {
    schedule: S,
    timezone: Timezone,
    next_tick: Option<DateTime<Timezone>>,
    delay: Option<Delay>,
}

impl<S: Schedule<Utc>> CronStream<S, Utc> {
    /// Build a new cron stream from a schedule using the UTC timezone
    pub fn new(schedule: S) -> Self {
        Self::new_with_timezone(schedule, Utc)
    }
}

impl<S: Schedule<Tz>, Tz: chrono::TimeZone> CronStream<S, Tz> {
    /// Build a new cron stream from a schedule and timezone
    pub fn new_with_timezone(schedule: S, timezone: Tz) -> Self {
        Self {
            schedule,
            timezone,
            next_tick: None,
            delay: None,
        }
    }
}

impl<S: Schedule<Tz> + Unpin, Tz: TimeZone + Unpin> Stream for CronStream<S, Tz>
where
    Tz::Offset: Unpin,
{
    type Item = Result<Tick<Tz>, CronStreamError<Tz>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();
        loop {
            match &mut this.next_tick {
                Some(next) => {
                    // If we haven't set the delay yet, set it now.
                    if this.delay.is_none() {
                        let now = Utc::now();
                        let td = next.clone().signed_duration_since(now);
                        let duration = match td.to_std() {
                            Ok(d) => d,
                            Err(e) => {
                                return Poll::Ready(Some(Err(CronStreamError::OutOfRangeError {
                                    inner: e,
                                    tick: next.clone(),
                                })))
                            }
                        };
                        this.delay = Some(Delay::new(duration));
                    }

                    // Poll the delay future
                    match Pin::new(this.delay.as_mut().unwrap()).poll(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(()) => {
                            let fired = next.clone();

                            // Update next_tick and delay.
                            let next_tick = this.schedule.next_tick(&this.timezone);
                            self.next_tick = next_tick;
                            self.delay = None;
                            return Poll::Ready(Some(Ok(Tick::new(fired))));
                        }
                    }
                }
                None => {
                    let next_tick = this.schedule.next_tick(&this.timezone);
                    match next_tick {
                        Some(next) => this.next_tick = Some(next),
                        None => return Poll::Ready(None),
                    }
                }
            }
        }
    }
}

impl<S: Schedule<Tz> + Unpin + Send + Sync + 'static, Tz: Unpin> Backend<Tick<Tz>>
    for CronStream<S, Tz>
where
    Tz: TimeZone + Send + Sync + 'static,
    Tz::Offset: Send + Sync + Unpin + Display,
{
    type Context = CronContext<S>;
    type Codec = ();
    type Error = CronStreamError<Tz>;
    type Stream = TaskStream<Task<Tick<Tz>, Self::Context, Ulid>, CronStreamError<Tz>>;

    type Layer = Identity;

    type IdType = Ulid;

    type Beat = BoxStream<'static, Result<(), Self::Error>>;

    fn heartbeat(&self, _: &WorkerContext) -> Self::Beat {
        stream::once(async { Ok(()) }).boxed()
    }
    fn middleware(&self) -> Self::Layer {
        Identity::new()
    }

    fn poll(self, _: &WorkerContext) -> Self::Stream {
        let ctx = CronContext::new(self.schedule.clone());
        let stream = TryStreamExt::and_then(self, move |tick| {
            let ctx = ctx.clone();
            async move {
                let timestamp: SystemTime = tick.get_timestamp().clone().into();
                let task_id = Ulid::from_datetime(timestamp);
                let task = TaskBuilder::new(tick)
                    .with_ctx(ctx.clone())
                    .with_task_id(TaskId::new(task_id))
                    .build();

                Ok(Some(task))
            }
        });
        stream.boxed()
    }
}
