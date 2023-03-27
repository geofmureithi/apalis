pub mod timer;

use std::{future::Future, str::FromStr};

use anyhow::Result;
use apalis::{
    cron::{CronStream, Schedule},
    layers::TraceLayer,
    prelude::*,
};
use chrono::{DateTime, Utc};
use timer::SmolTimer;
use tracing::debug;

#[derive(Default, Debug, Clone)]
struct Reminder(DateTime<Utc>);

impl From<DateTime<Utc>> for Reminder {
    fn from(t: DateTime<Utc>) -> Self {
        Reminder(t)
    }
}

impl Job for Reminder {
    const NAME: &'static str = "reminder::DailyReminder";
}
async fn send_reminder(job: Reminder, _ctx: JobContext) {
    debug!("Called at {job:?}")
}

/// Spawns futures.
#[derive(Clone)]
struct SmolExecutor;

impl Executor for SmolExecutor {
    type JoinHandle = ();
    fn spawn(&self, fut: impl Future<Output = ()> + Send + 'static) -> Self::JoinHandle {
        smol::spawn(async { fut.await }).detach()
    }
}

fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    tracing_subscriber::fmt::init();

    smol::block_on(async {
        let schedule = Schedule::from_str("1/1 * * * * *").unwrap();
        let worker = WorkerBuilder::new("daily-cron-worker")
            .stream(CronStream::new(schedule, SmolTimer).to_stream())
            .layer(TraceLayer::new())
            .build(job_fn(send_reminder));

        Monitor::new()
            .executor(SmolExecutor)
            .register(worker)
            .run()
            .await?;
        Ok(())
    })
}
