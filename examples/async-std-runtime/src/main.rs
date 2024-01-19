use std::{future::Future, str::FromStr, time::Duration};

use anyhow::Result;
use apalis::{
    cron::{CronStream, Schedule},
    layers::Data,
    prelude::*,
};
use chrono::{DateTime, Utc};
use tracing::{debug, info};

#[derive(Default, Debug, Clone)]
struct Reminder(DateTime<Utc>);

impl From<DateTime<Utc>> for Reminder {
    fn from(t: DateTime<Utc>) -> Self {
        Reminder(t)
    }
}

async fn send_reminder(reminder: Reminder) {
    debug!("Called at {reminder:?}");
}

#[async_std::main]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    tracing_subscriber::fmt::init();
    let (s, ctrl_c) = async_channel::bounded(1);
    let handle = move || {
        s.try_send(()).ok();
    };
    ctrlc::set_handler(handle)?;

    let schedule = Schedule::from_str("1/1 * * * * *").unwrap();
    let worker = WorkerBuilder::new("daily-cron-worker")
        // .layer(TraceLayer::new())
        .stream(CronStream::new(schedule).into_stream())
        .build_fn(send_reminder);

    Monitor::<AsyncStdExecutor>::new()
        // .executor(AsyncStdExecutor::new())
        .register_with_count(3, worker)
        // .run()
        .run_with_signal(async {
            ctrl_c.recv().await.ok();
            info!("Shutting down");
            Ok(())
        })
        .await?;
    Ok(())
}

#[derive(Clone, Debug, Default)]
pub struct AsyncStdExecutor;

impl AsyncStdExecutor {
    /// A new async-std executor
    pub fn new() -> Self {
        Self
    }
}

impl Executor for AsyncStdExecutor {
    fn spawn(&self, fut: impl Future<Output = ()> + Send + 'static) {
        async_std::task::spawn(async { fut.await });
    }
}
