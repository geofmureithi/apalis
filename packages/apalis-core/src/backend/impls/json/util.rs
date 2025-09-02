use std::{cmp::Ordering, collections::BTreeMap, fmt::Debug, marker::PhantomData};

use futures_channel::mpsc::SendError;
use futures_core::stream::BoxStream;
use futures_util::{stream, FutureExt, StreamExt};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;

use crate::{
    backend::{
        impls::json::{meta::JsonMapMetadata, JsonStorage},
        TaskResult, WaitForCompletion,
    }, error::BoxDynError, task::{
        status::Status,
        task_id::{RandomId, TaskId},
    }, worker::ext::ack::Acknowledge
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskKey {
    pub(super) task_id: TaskId,
    pub(super) namespace: String,
    pub(super) status: Status,
}

impl PartialEq for TaskKey {
    fn eq(&self, other: &Self) -> bool {
        self.task_id == other.task_id && self.namespace == other.namespace
    }
}

impl Eq for TaskKey {}

impl PartialOrd for TaskKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TaskKey {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.task_id.cmp(&other.task_id) {
            Ordering::Equal => self.namespace.cmp(&other.namespace),
            ord => ord,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskWithMeta {
    pub(super) args: Value,
    pub(super) meta: JsonMapMetadata,
    pub(super) result: Option<Value>,
}

pub struct JsonAck<Args> {
    pub(crate) inner: JsonStorage<Args>,
}

impl<Args> Clone for JsonAck<Args> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<Args: Send + 'static + Debug, Res: Serialize, Meta: Sync> Acknowledge<Res, Meta, RandomId>
    for JsonAck<Args>
{
    type Error = serde_json::Error;

    type Future = futures_core::future::BoxFuture<'static, Result<(), Self::Error>>;

    fn ack(
        &mut self,
        res: &Result<Res, BoxDynError>,
        ctx: &crate::task::ExecutionContext<Meta, RandomId>,
    ) -> Self::Future {
        let mut store = self.inner.clone();
        let val = serde_json::to_value(res.as_ref().map_err(|e| e.to_string())).unwrap();
        let task_id = ctx.task_id.clone().unwrap();
        async move {
            let key = TaskKey {
                task_id: task_id.clone(),
                namespace: std::any::type_name::<Args>().to_owned(),
                status: Status::Running,
            };

            let _ = store.update_result(&key, Status::Done, val).unwrap();

            store.persist_to_disk().unwrap();

            Ok(())
        }
        .boxed()
    }
}

impl<Res: 'static + DeserializeOwned + Send, Compact: 'static + Sync>
    WaitForCompletion<Res, Compact> for JsonStorage<Compact>
where
    Compact: Send + DeserializeOwned + 'static + Unpin,
{
    type ResultStream = BoxStream<'static, Result<TaskResult<Res>, SendError>>;
    fn wait_for(
        &self,
        task_ids: impl IntoIterator<Item = TaskId<Self::IdType>>,
    ) -> Self::ResultStream {
        use std::{collections::HashSet, time::Duration};

        let task_ids: HashSet<_> = task_ids.into_iter().collect();
        struct PollState<T, Compact> {
            vault: JsonStorage<Compact>,
            pending_tasks: HashSet<TaskId>,
            namespace: String,
            poll_interval: Duration,
            _phantom: std::marker::PhantomData<T>,
        }
        let state = PollState {
            vault: self.clone(),
            pending_tasks: task_ids,
            namespace: std::any::type_name::<Compact>().to_owned(),
            poll_interval: Duration::from_millis(100),
            _phantom: std::marker::PhantomData,
        };
        stream::unfold(state, |mut state: PollState<Res, Compact>| {
            async move {
                // panic!( "{}", state.pending_tasks.len());
                // If no pending tasks, we're done
                if state.pending_tasks.is_empty() {
                    return None;
                }

                loop {
                    // Check for completed tasks
                    let vault = &state.vault;
                    let completed_task = state.pending_tasks.iter().find_map(|task_id| {
                        let key = TaskKey {
                            task_id: task_id.clone(),
                            namespace: state.namespace.clone(),
                            status: Status::Pending,
                        };

                        vault
                            .get(&key)
                            .map(|value| (task_id.clone(), value.result.unwrap()))
                    });

                    if let Some((task_id, result)) = completed_task {
                        state.pending_tasks.remove(&task_id);
                        let result: Result<Res, String> = serde_json::from_value(result).unwrap();
                        return Some((
                            Ok(TaskResult {
                                task_id: task_id,
                                status: Status::Done,
                                result,
                            }),
                            state,
                        ));
                    }

                    // No completed tasks, wait and try again
                    crate::timer::sleep(state.poll_interval).await;
                }
            }
        })
        .boxed()
    }

    async fn check_status(
        &self,
        task_ids: impl IntoIterator<Item = TaskId<Self::IdType>> + Send,
    ) -> Result<Vec<TaskResult<Res>>, Self::Error> {
        use crate::task::status::Status;
        use std::collections::HashSet;
        let task_ids: HashSet<_> = task_ids.into_iter().collect();
        let mut results = Vec::new();
        for task_id in task_ids {
            let key = TaskKey {
                task_id: task_id.clone(),
                namespace: std::any::type_name::<Compact>().to_owned(),
                status: Status::Pending,
            };
            if let Some(value) = self.get(&key) {
                let result =
                    match serde_json::from_value::<Result<Res, String>>(value.result.unwrap()) {
                        Ok(result) => TaskResult {
                            task_id: task_id.clone(),
                            status: Status::Done,
                            result,
                        },
                        Err(e) => TaskResult {
                            task_id: task_id.clone(),
                            status: Status::Failed,
                            result: Err(format!("Deserialization error: {}", e)),
                        },
                    };
                results.push(result);
            }
        }
        Ok(results)
    }
}

/// Find the first item that meets the requirements
pub trait FindFirstWith<K, V> {
    fn find_first_with<F>(&self, predicate: F) -> Option<(&K, &V)>
    where
        F: FnMut(&K, &V) -> bool;
}

impl<K, V> FindFirstWith<K, V> for BTreeMap<K, V>
where
    K: Ord + Clone,
{
    fn find_first_with<F>(&self, mut predicate: F) -> Option<(&K, &V)>
    where
        F: FnMut(&K, &V) -> bool,
    {
        if let Some(key) = self.iter().find(|(k, v)| predicate(k, v)).map(|(k, _)| k) {
            self.get_key_value(key)
        } else {
            None
        }
    }
}
