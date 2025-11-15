#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]
#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]

use apalis_core::{error::BoxDynError, task::Task};

use crate::router::{GoTo, StepResult};

type BoxedService<Input, Output> = tower::util::BoxService<Input, Output, BoxDynError>;
type SteppedService<Compact, Ctx, IdType> =
    BoxedService<Task<Compact, Ctx, IdType>, GoTo<StepResult<Compact, IdType>>>;

/// combinator for sequential workflow execution.
pub mod and_then;
/// combinator for chaining multiple workflows.
pub mod chain;
/// utilities for workflow context management.
pub mod context;
/// utilities for directed acyclic graph workflows.
#[allow(unused)]
pub mod dag;
/// utilities for introducing delays in workflow execution.
pub mod delay;
/// combinator for filtering and mapping workflow items.
pub mod filter_map;
/// combinator for folding over workflow items.
pub mod fold;
mod id_generator;
/// utilities for workflow routing.
pub mod router;
/// utilities for workflow service orchestration.
pub mod service;
/// utilities for workflow sinks.
pub mod sink;
/// utilities for workflow steps.
pub mod step;
/// workflow definitions.
pub mod workflow;

pub use {dag::DagExecutor, dag::DagFlow, sink::WorkflowSink, workflow::Workflow};

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, time::Duration};

    use apalis_core::{
        backend::json::JsonStorage,
        task::{builder::TaskBuilder, task_id::TaskId},
        task_fn::task_fn,
        worker::{
            builder::WorkerBuilder, context::WorkerContext, event::Event,
            ext::event_listener::EventListenerExt,
        },
    };
    use futures::SinkExt;
    use serde_json::Value;

    use crate::{and_then::AndThen, workflow::Workflow};

    use super::*;

    #[tokio::test]
    async fn basic_workflow() {
        let workflow = Workflow::new("and-then-workflow")
            .and_then(async |input: u32| (input) as usize)
            .delay_for(Duration::from_secs(1))
            .and_then(async |input: usize| (input) as usize)
            .delay_for(Duration::from_secs(1))
            // .delay_with(|_: Task<usize, _, _>| Duration::from_secs(1))
            .add_step(AndThen::new(task_fn(async |input: usize| {
                Ok::<_, BoxDynError>(input.to_string())
            })))
            .and_then(async |input: String, _task_id: TaskId| input.parse::<usize>())
            .and_then(async |res: usize| {
                Ok::<_, BoxDynError>((0..res).enumerate().collect::<HashMap<_, _>>())
            })
            .filter_map(async |(index, input): (usize, usize)| {
                if input % 2 == 0 {
                    Some(index.to_string())
                } else {
                    None
                }
            })
            .fold(
                async move |(acc, item): (usize, String), _wrk: WorkerContext| {
                    println!("Folding item {item} with acc {acc}");
                    let item = item.parse::<usize>().unwrap();
                    let acc = acc + item;
                    acc
                },
            )
            .and_then(async |res: usize, wrk: WorkerContext| {
                wrk.stop().unwrap();
                println!("Completed with {res:?}");
            });

        let mut in_memory: JsonStorage<Value> = JsonStorage::new_temp().unwrap();

        in_memory
            .send(TaskBuilder::new(Value::from(17)).build())
            .await
            .unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(in_memory)
            .on_event(|ctx, ev| {
                println!("On Event = {ev:?}");
                if matches!(ev, Event::Error(_)) {
                    ctx.stop().unwrap();
                }
            })
            .build(workflow);
        worker.run().await.unwrap();
    }
}
