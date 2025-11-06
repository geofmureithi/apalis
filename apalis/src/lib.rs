#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]
//! ## Feature flags
#![cfg_attr(
    feature = "docsrs",
    cfg_attr(doc, doc = ::document_features::document_features!())
)]
//!
//! [`Service`]: https://docs.rs/tower/latest/tower/trait.Service.html
//! [`tower`]: https://crates.io/crates/tower
//! [`tower-http`]: https://crates.io/crates/tower-http
//! [`Layer`]: https://docs.rs/tower/latest/tower/trait.Layer.html
//! [`Stream`]: https://docs.rs/futures/latest/futures/stream/trait.Stream.html
#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
/// apalis fully supports middleware via [`Layer`](https://docs.rs/tower/latest/tower/trait.Layer.html)
pub mod layers;

/// Common imports
pub mod prelude {
    pub use crate::layers::WorkerBuilderExt;
    pub use apalis_core::{
        backend::{
            Backend, FetchById, ListTasks, ListWorkers, Metrics, RegisterWorker, Reschedule,
            ResumeAbandoned, ResumeById, TaskSink, Update, WaitForCompletion,
        },
        backend::{
            TaskResult, TaskStream, codec::*, custom::*, memory::MemoryStorage, pipe::*,
            poll_strategy::*, shared::MakeShared,
        },
        error::*,
        layers::*,
        monitor::{ExitError, Monitor, MonitorError, MonitoredWorkerError, shutdown::Shutdown},
        task::Parts,
        task::Task,
        task::attempt::Attempt,
        task::builder::TaskBuilder,
        task::data::{AddExtension, Data},
        task::extensions::Extensions,
        task::metadata::MetadataExt,
        task::task_id::TaskId,
        task_fn::{FromRequest, IntoResponse, TaskFn, task_fn},
        worker::builder::*,
        worker::ext::{ack::*, circuit_breaker::*, event_listener::*, long_running::*},
        worker::{Worker, context::WorkerContext, event::Event},
    };
}
