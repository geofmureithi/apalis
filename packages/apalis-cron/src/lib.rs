#![crate_name = "apalis_cron"]
#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]
//!
//! ## Feature flags
#![cfg_attr(
    feature = "docsrs",
    cfg_attr(doc, doc = ::document_features::document_features!())
)]

mod backend;
mod context;
mod error;
mod schedule;
mod tick;
mod timezone;

const FORMAT: &str = "%Y-%m-%d %H:%M:%S";

pub use {backend::*, context::*, error::*, schedule::*, tick::*, timezone::*};

#[cfg(test)]
mod tests {
    use std::{str::FromStr, time::Duration};

    use apalis_core::{
        backend::{json::JsonStorage, pipe::PipeExt},
        error::BoxDynError,
        task::task_id::TaskId,
        worker::{builder::WorkerBuilder, event::Event, ext::event_listener::EventListenerExt},
    };
    use chrono::Utc;
    use cron::Schedule;
    use tower::{limit::ConcurrencyLimitLayer, load_shed::LoadShedLayer};

    use crate::{backend::CronStream, context::CronContext, tick::Tick};

    #[tokio::test]
    async fn basic_worker() {
        let schedule = Schedule::from_str("1/1 * * * * *").unwrap();
        let stream = CronStream::new_with_timezone(schedule, Utc);

        async fn send_reminder(
            tick: Tick<Utc>,
            meta: CronContext<Schedule>,
        ) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(1)).await;
            println!(
                "Running cronjob for timestamp: {} with meta: {:?}",
                tick.get_timestamp(),
                meta
            );
            Err("Failed".into())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(stream)
            .on_event(move |ctx, ev| {
                println!("{:?}", ev);
                let ctx = ctx.clone();
                if matches!(ev, Event::Start) {
                    tokio::spawn(async move {
                        if ctx.is_running() {
                            tokio::time::sleep(Duration::from_millis(900)).await;
                            ctx.stop().unwrap();
                        }
                    });
                }
            })
            .build(send_reminder);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn load_shedding_worker() {
        // We are generating a new cron job every second
        let schedule = Schedule::from_str("1/1 * * * * *").unwrap();
        let stream = CronStream::new_with_timezone(schedule, Utc);

        // But a single job can take longer than a second to complete
        async fn send_reminder(
            tick: Tick<Utc>,
            ctx: CronContext<Schedule>,
        ) -> Result<(), BoxDynError> {
            println!(
                "Running cronjob for timestamp: {} with ctx: {:?}",
                tick.get_timestamp(),
                ctx.schedule().unwrap().to_string()
            );
            tokio::time::sleep(Duration::from_secs(2)).await;
            Err("Failed".into())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(stream)
            // Drop requests if there is existing one
            .layer(LoadShedLayer::new())
            // Limit concurrent requests
            .layer(ConcurrencyLimitLayer::new(1))
            .on_event(move |ctx, ev| {
                println!("{:?}", ev);
                let ctx = ctx.clone();
                if matches!(ev, Event::Start) {
                    tokio::spawn(async move {
                        if ctx.is_running() {
                            tokio::time::sleep(Duration::from_millis(3000)).await;
                            ctx.stop().unwrap();
                        }
                    });
                }
            })
            .build(send_reminder);
        // This might log
        //
        // $ Running cronjob for timestamp: 2025-09-04 23:28:08 UTC with ctx: "1/1 * * * * *"
        // $ Error("Failed")
        // $ Error(Overloaded)
        // $ Running cronjob for timestamp: 2025-09-04 23:28:10 UTC with ctx: "1/1 * * * * *"
        worker.run().await.unwrap();
    }

    #[cfg(feature = "serde")]
    #[tokio::test]
    async fn piped_worker() {
        let schedule = Schedule::from_str("1/1 * * * * *").unwrap();
        let stream = CronStream::new(schedule);
        let in_memory = JsonStorage::new_temp().unwrap();

        let backend = stream.pipe_to(in_memory);

        async fn send_reminder(job: Tick, id: TaskId) -> Result<(), BoxDynError> {
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
