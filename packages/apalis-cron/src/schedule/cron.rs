use chrono::DateTime;

use crate::schedule::Schedule;

impl<Tz: chrono::TimeZone> Schedule<Tz> for cron::Schedule {
    fn next_tick(&self, timezone: &Tz) -> Option<DateTime<Tz>> {
        self.upcoming(timezone.clone()).next()
    }
}

#[cfg(test)]
mod tests {
    use apalis_core::{
        error::BoxDynError,
        task::task_id::TaskId,
        worker::{builder::WorkerBuilder, event::Event, ext::event_listener::EventListenerExt},
    };
    use cron::Schedule;
    use ulid::Ulid;

    use crate::{backend::CronStream, tick::Tick};

    use std::{str::FromStr, time::Duration};

    #[tokio::test]
    async fn basic_worker() {
        let schedule = Schedule::from_str("1/1 * * * * *").unwrap();
        let backend = CronStream::new(schedule);

        async fn send_reminder(job: Tick, id: TaskId<Ulid>) -> Result<(), BoxDynError> {
            println!("Running cronjob for timestamp: {:?} with id {}", job, id);
            tokio::time::sleep(Duration::from_secs(1)).await;
            Err("All failing".into())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
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
