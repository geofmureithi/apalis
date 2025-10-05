//! # File based Backend using JSON
//!
//! A JSON file-based backend for persisting tasks and results.
//!
//! ## Features
//!
//! - **Sink support**: Ability to push new tasks.
//! - **Codec Support**: Serialization support for arguments using JSON.
//! - **Workflow Support**: Flexible enough to support workflows.
//! - **Ack Support**: Allows acknowledgement of task completion.
//! - **WaitForCompletion**: Wait for tasks to complete without blocking.
//!
//! ## Usage Example
//!
//! ```rust
//! # use apalis_core::backend::json::JsonStorage;
//! # use apalis_core::worker::builder::WorkerBuilder;
//! # use std::time::Duration;
//! # use apalis_core::worker::context::WorkerContext;
//! # use apalis_core::backend::TaskSink;
//! # use apalis_core::error::BoxDynError;
//! # use apalis_core::worker::ext::event_listener::EventListenerExt;
//!
//! #[tokio::main]
//! async fn main() {
//!     let mut json_store = JsonStorage::new_temp().unwrap();
//!     json_store.push(42).await.unwrap();

//!
//!     async fn task(task: u32, ctx: WorkerContext) -> Result<(), BoxDynError> {
//!         tokio::time::sleep(Duration::from_secs(1)).await;
//!         ctx.stop().unwrap();
//!         Ok(())
//!     }
//!
//!     let worker = WorkerBuilder::new("rango-tango")
//!         .backend(json_store)
//!         .on_event(|ctx, ev| {
//!             println!("On Event = {:?}", ev);
//!         })
//!         .build(task);
//!     worker.run().await.unwrap();
//! }
//! ```
//!
//! ## Implementation Notes
//!
//! - Tasks are stored in a file, each line representing a serialized task entry.
//! - All operations are thread-safe using `RwLock`.
//! - Data is atomically persisted to disk to avoid corruption.
//! - Supports temporary storage for testing and ephemeral use cases.
//!
use serde_json::Value;
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{BufRead, Write},
    path::PathBuf,
    sync::{Arc, RwLock},
};

use self::{
    meta::JsonMapMetadata,
    util::{TaskKey, TaskWithMeta},
};
use crate::{
    features_table,
    task::{
        Task,
        status::Status,
        task_id::{RandomId, TaskId},
    },
};
use std::io::{BufReader, BufWriter};

mod backend;
mod meta;
mod shared;
mod sink;
mod util;

pub use self::shared::SharedJsonStore;

/// A backend that persists to a file using json encoding
///
#[doc = features_table! {
    setup = {
        use apalis_core::backend::json::JsonStorage;
        JsonStorage::new_temp().unwrap()
    };,
    TaskSink => supported("Ability to push new tasks"),
    Serialization => limited("Serialization support for arguments. Only accepts `json`", false),
    FetchById => not_implemented("Allow fetching a task by its ID"),
    RegisterWorker => not_supported("Allow registering a worker with the backend"),
    PipeExt => supported("Allow other backends to pipe to this backend", false),
    MakeShared => supported("Share the same JSON storage across multiple workers", false),
    Workflow => supported("Flexible enough to support workflows", false),
    WaitForCompletion => supported("Wait for tasks to complete without blocking", false),
    ResumeById => not_implemented("Resume a task by its ID"),
    ResumeAbandoned => not_implemented("Resume abandoned tasks"),
    ListWorkers => not_supported("List all workers registered with the backend"),
    ListTasks => not_implemented("List all tasks in the backend"),
}]
#[derive(Debug)]
pub struct JsonStorage<Args> {
    tasks: Arc<RwLock<BTreeMap<TaskKey, TaskWithMeta>>>,
    buffer: Vec<Task<Args, JsonMapMetadata>>,
    path: PathBuf,
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone)]
struct StorageEntry {
    task_id: TaskId,
    status: Status,
    task: TaskWithMeta,
}

impl<Args> JsonStorage<Args> {
    /// Creates a new `JsonStorage` instance using the specified file path.
    pub fn new(path: impl Into<PathBuf>) -> std::io::Result<Self> {
        let path = path.into();
        let mut data = BTreeMap::new();

        if path.exists() {
            let file = File::open(&path)?;
            let reader = BufReader::new(file);

            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }

                if let Ok(entry) = serde_json::from_str::<StorageEntry>(&line) {
                    let key = TaskKey {
                        status: entry.status,
                        task_id: entry.task_id,
                        namespace: std::any::type_name::<Args>().to_owned(),
                    };
                    data.insert(key, entry.task);
                }
            }
        }

        Ok(JsonStorage {
            path,
            tasks: Arc::new(RwLock::new(data)),
            buffer: Vec::new(),
        })
    }

    /// Creates a new temporary `JsonStorage` instance.
    pub fn new_temp() -> Result<JsonStorage<Args>, std::io::Error> {
        let p = std::env::temp_dir().join(format!("apalis-json-store-{}", RandomId::default()));
        Self::new(p)
    }

    fn insert(&mut self, k: &TaskKey, v: TaskWithMeta) -> Result<(), std::io::Error> {
        self.tasks.try_write().unwrap().insert(k.clone(), v);
        Ok(())
    }

    /// Removes a task from the storage.
    pub fn remove(&mut self, key: &TaskKey) -> std::io::Result<Option<TaskWithMeta>> {
        let removed = self.tasks.try_write().unwrap().remove(key);

        if removed.is_some() {
            self.persist_to_disk()?;
        }

        Ok(removed)
    }

    /// Persist all current data to disk by rewriting the file
    fn persist_to_disk(&self) -> std::io::Result<()> {
        let tmp_path = self.path.with_extension("tmp");

        {
            let tmp_file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)?;
            let mut writer = BufWriter::new(tmp_file);

            for (key, value) in self.tasks.try_read().unwrap().iter() {
                let entry = StorageEntry {
                    status: key.status.clone(),
                    task_id: key.task_id.clone(),
                    task: value.clone(),
                };
                let line = serde_json::to_string(&entry)?;
                writeln!(writer, "{}", line)?;
            }

            writer.flush()?;
        } // BufWriter is dropped here, ensuring all data is written

        // Atomically replace the old file with the new one
        std::fs::rename(tmp_path, &self.path)?;
        Ok(())
    }
    /// Reload data from disk, useful if the file was modified externally
    pub fn reload(&mut self) -> std::io::Result<()> {
        let mut new_data = BTreeMap::new();

        if self.path.exists() {
            let file = File::open(&self.path)?;
            let reader = BufReader::new(file);

            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }

                if let Ok(entry) = serde_json::from_str::<StorageEntry>(&line) {
                    let key = TaskKey {
                        status: entry.status,
                        task_id: entry.task_id,
                        namespace: std::any::type_name::<Args>().to_owned(),
                    };
                    new_data.insert(key, entry.task);
                }
            }
        }

        *self.tasks.try_write().unwrap() = new_data;
        Ok(())
    }
    /// Clear all data from memory and file
    pub fn clear(&mut self) -> std::io::Result<()> {
        self.tasks.try_write().unwrap().clear();

        // Create an empty file
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.path)?;
        drop(file);

        Ok(())
    }

    /// Update the status of an existing key
    pub fn update_status(
        &mut self,
        old_key: &TaskKey,
        new_status: Status,
    ) -> std::io::Result<bool> {
        let mut tasks = self.tasks.try_write().unwrap();
        if let Some(value) = tasks.remove(old_key) {
            let new_key = TaskKey {
                status: new_status,
                task_id: old_key.task_id.clone(),
                namespace: old_key.namespace.clone(),
            };
            tasks.insert(new_key, value);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Retrieves a task from the storage.
    pub fn get(&self, key: &TaskKey) -> Option<TaskWithMeta> {
        let tasks = self.tasks.try_read().unwrap();
        let res = tasks.get(key);
        res.cloned()
    }

    fn update_result(&self, key: &TaskKey, status: Status, val: Value) -> std::io::Result<bool> {
        let mut tasks = self.tasks.try_write().unwrap();
        if let Some(mut task) = tasks.remove(key) {
            let new_key = TaskKey {
                status,
                task_id: key.task_id.clone(),
                namespace: key.namespace.clone(),
            };
            task.result = Some(val);

            tasks.insert(new_key, task);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl<Args> Clone for JsonStorage<Args> {
    fn clone(&self) -> Self {
        Self {
            tasks: self.tasks.clone(),
            buffer: Vec::new(),
            path: self.path.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::{
        backend::{TaskSink, json::JsonStorage},
        error::BoxDynError,
        worker::{
            builder::WorkerBuilder, context::WorkerContext, ext::event_listener::EventListenerExt,
        },
    };

    const ITEMS: u32 = 100;

    #[tokio::test]
    async fn basic_worker() {
        let mut json_store = JsonStorage::new_temp().unwrap();
        for i in 0..ITEMS {
            json_store.push(i).await.unwrap();
        }

        async fn task(task: u32, ctx: WorkerContext) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if task == ITEMS - 1 {
                ctx.stop().unwrap();
                return Err("Worker stopped!")?;
            }
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(json_store)
            .on_event(|ctx, ev| {
                println!("On Event = {:?} from = {}", ev, ctx.name());
            })
            .build(task);
        worker.run().await.unwrap();
    }
}
