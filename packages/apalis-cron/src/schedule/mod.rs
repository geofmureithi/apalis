use chrono::DateTime;

use crate::tick::Tick;
mod builder;
#[cfg(feature = "cron")]
mod cron;
#[cfg(feature = "english")]
mod english;

pub trait Schedule<Timezone: chrono::TimeZone>: Sized {
    fn next_tick(&self, timezone: &Timezone) -> Option<DateTime<Timezone>>;
}

#[cfg(test)]
mod tests {
    use apalis_core::{
        backend::{self, memory::MemoryStorage},
        error::BoxDynError,
        task::{builder::TaskBuilder, task_id::TaskId},
        worker::{builder::WorkerBuilder, event::Event, ext::event_listener::EventListenerExt},
    };
    use cron::Schedule;
    use futures_util::{stream, SinkExt, StreamExt};
    use ulid::Ulid;

    use crate::tick::Tick;

    use std::{str::FromStr, time::Duration};

    #[tokio::test]
    async fn eager_worker() {
        let schedule = Schedule::from_str("1/1 * * * * *").unwrap();
        let five_ticks = schedule.upcoming(chrono::Utc).take(5);
        let mut tasks = stream::iter(five_ticks.map(|s| {
            let ts = s.timestamp() as u64;
            let task = TaskBuilder::new(Tick::new(s)).run_at_timestamp(ts).build();
            Ok(task)
        }));

        let mut memory = MemoryStorage::new();

        memory.send_all(&mut tasks).await.unwrap();

        async fn send_reminder(job: Tick, id: TaskId) -> Result<(), BoxDynError> {
            println!("Running cronjob for timestamp: {:?} with id {}", job, id);
            tokio::time::sleep(Duration::from_secs(1)).await;
            Err("All failing".into())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(memory)
            .on_event(move |ctx, ev| {
                println!("{:?}", ev);
                let ctx = ctx.clone();
                if matches!(ev, Event::Error(_)) {
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_secs(3)).await;
                        ctx.stop().unwrap();
                    });
                }
            })
            .build(send_reminder);
        worker.run().await.unwrap();
    }
}
