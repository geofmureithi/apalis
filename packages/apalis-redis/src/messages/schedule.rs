use crate::consumer::RedisConsumer;
use actix::prelude::*;
use actix_rt::time::Interval;
use apalis_core::{Error, Job, JobHandler};
use chrono::prelude::*;
use log::*;
use std::pin::Pin;
use std::task::{Context as StdContext, Poll};

#[derive(Message)]
#[rtype(result = "()")]
pub struct Schedule;

pub struct ScheduleStream {
    interval: Interval,
}

impl ScheduleStream {
    pub fn new(interval: Interval) -> Self {
        ScheduleStream { interval }
    }
}

impl Stream for ScheduleStream {
    type Item = Schedule;

    fn poll_next(self: Pin<&mut Self>, cx: &mut StdContext<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut()
            .interval
            .poll_tick(cx)
            .map(|_| Some(Schedule))
    }
}

impl<J: 'static + Unpin + JobHandler<Self>> StreamHandler<Schedule> for RedisConsumer<J>
where
    J: Job,
{
    fn handle(&mut self, _: Schedule, ctx: &mut Context<RedisConsumer<J>>) {
        let conn = self.queue.storage.clone();
        let enqueue_jobs = redis::Script::new(include_str!("../../lua/enqueue_scheduled_jobs.lua"));
        let scheduled_jobs_set = self.queue.scheduled_jobs_set.to_string();
        let active_jobs_list = self.queue.active_jobs_list.to_string();
        let signal_list = self.queue.signal_list.to_string();
        let timestamp = Utc::now().timestamp();
        let fut = async move {
            let mut conn = conn.get_connection().await.unwrap();
            let res: Result<i8, Error> = enqueue_jobs
                .key(scheduled_jobs_set)
                .key(active_jobs_list)
                .key(signal_list)
                .arg(timestamp)
                .arg("10".to_string()) //Enque 10 jobs
                .invoke_async(&mut conn)
                .await
                .map_err(|_| Error::Failed);
            match res {
                Ok(count) => {
                    if count > 0 {
                        info!("Jobs to enqueue: {:?}", count);
                    }
                }
                Err(e) => {
                    error!("Unable to Enqueue jobs, Error: {:?}", e);
                }
            }
        };
        let fut = actix::fut::wrap_future::<_, Self>(fut);
        ctx.spawn(fut);
    }

    fn finished(&mut self, _: &mut Self::Context) {
        warn!("Scheduler for consumer: {:?} stopped", self.id());
    }
}
