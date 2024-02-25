use std::{future::Future, str::FromStr, time::Duration};

use anyhow::Result;
use apalis::{
    cron::{CronStream, Schedule},
    layers::{retry::RetryLayer, retry::RetryPolicy, tracing::MakeSpan, tracing::TraceLayer},
    prelude::*,
};

use chrono::{DateTime, Utc};
use tracing::{debug, info, Instrument, Level, Span};

type WorkerCtx = Context<AsyncStdExecutor>;

#[derive(Default, Debug, Clone)]
struct Reminder(DateTime<Utc>);

impl From<DateTime<Utc>> for Reminder {
    fn from(t: DateTime<Utc>) -> Self {
        Reminder(t)
    }
}

async fn send_in_background(reminder: Reminder) {
    apalis_core::sleep(Duration::from_secs(2)).await;
    debug!("Called at {reminder:?}");
}
async fn send_reminder(reminder: Reminder, worker: WorkerCtx) -> bool {
    // this will happen in the workers background and wont block the next tasks
    worker.spawn(send_in_background(reminder).in_current_span());
    false
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
        .layer(RetryLayer::new(RetryPolicy::retries(5)))
        .layer(TraceLayer::new().make_span_with(ReminderSpan::new()))
        .stream(CronStream::new(schedule).into_stream())
        .build_fn(send_reminder);

    Monitor::<AsyncStdExecutor>::new()
        .register_with_count(2, worker)
        .on_event(|e| debug!("Worker event: {e:?}"))
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
        async_std::task::spawn(fut);
    }
}

#[derive(Debug, Clone)]
pub struct ReminderSpan {
    level: Level,
}

impl Default for ReminderSpan {
    fn default() -> Self {
        Self::new()
    }
}

impl ReminderSpan {
    /// Create a new `ReminderSpan`.
    pub fn new() -> Self {
        Self {
            level: Level::DEBUG,
        }
    }
}

impl<B> MakeSpan<B> for ReminderSpan {
    fn make_span(&mut self, req: &Request<B>) -> Span {
        let task_id: &TaskId = req.get().unwrap();
        let attempts: Attempt = req.get().cloned().unwrap_or_default();
        let span = Span::current();
        macro_rules! make_span {
            ($level:expr) => {
                tracing::span!(
                    parent: span,
                    $level,
                    "reminder",
                    task_id = task_id.to_string(),
                    attempt = attempts.current().to_string(),
                )
            };
        }

        match self.level {
            Level::ERROR => {
                make_span!(Level::ERROR)
            }
            Level::WARN => {
                make_span!(Level::WARN)
            }
            Level::INFO => {
                make_span!(Level::INFO)
            }
            Level::DEBUG => {
                make_span!(Level::DEBUG)
            }
            Level::TRACE => {
                make_span!(Level::TRACE)
            }
        }
    }
}
