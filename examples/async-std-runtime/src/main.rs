use std::{str::FromStr, time::Duration};

use anyhow::Result;
use apalis::{
    cron::{CronStream, Schedule},
    layers::TraceLayer,
    prelude::{timer::SleepTimer, Timer, *},
};
use chrono::{DateTime, Utc};
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
    debug!("Called at {job:?}");
    let timer = SleepTimer;
    timer.sleep(Duration::from_secs(3)).await;
}

#[async_std::main]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    tracing_subscriber::fmt::init();
    let (s, ctrl_c) = async_channel::bounded(100);
    let handle = move || {
        s.try_send(()).ok();
    };
    ctrlc::set_handler(handle).unwrap();

    let schedule = Schedule::from_str("1/1 * * * * *").unwrap();
    let worker = WorkerBuilder::new("daily-cron-worker")
        .stream(CronStream::new(schedule).to_stream())
        .layer(TraceLayer::new())
        .build(job_fn(send_reminder));

    Monitor::new()
        .register(worker)
        .run_with_signal(async {
            ctrl_c.recv().await.ok();
            Ok(())
        })
        .await?;
    Ok(())
}
