# apalis-workflow

This crate provides a flexible and composable workflow engine for [apalis](https://github.com/geofmureithi/apalis). Can be used for building old school workflows or advanced LLM workflows.

## Overview

The workflow engine allows you to define a sequence of steps in a workflow.
Workflows are built by composing steps, and can be executed using supported backends

## Features

- Compose workflows from reusable steps.
- Durable and resumable workflows.
- Steps are processed in a distributed manner.
- Parallel execution of steps.
- Extensible via the `Step` trait.
- Integration with `apalis` backends and workers
- Strongly typed steps and context handling

## Example

```rust,no_run
use apalis_workflow::*;
use apalis_core::backend::json::JsonStorage;
use apalis_core::worker::builder::WorkerBuilder;
use std::time::Duration;
use apalis_core::worker::ext::event_listener::EventListenerExt;

#[tokio::main]
async fn main() {
   let workflow = WorkFlow::new("odd-numbers-workflow")
       .delay_for(Duration::from_millis(1000))
       .then(|a: usize| async move { Ok::<_, WorkflowError>((0..a).collect::<Vec<_>>()) })
       .filter_map(|x| async move { if x % 2 != 0 { Some(x) } else { None } })
       .then(|a: Vec<usize>| async move {
           println!("Sum: {}", a.iter().sum::<usize>());
           Ok::<_, WorkflowError>(())
        });

   let mut in_memory = JsonStorage::new_temp().unwrap();

   in_memory.push_start(10).await.unwrap();

   let worker = WorkerBuilder::new("rango-tango")
       .backend(in_memory)
       .on_event(|ctx, ev| {
           println!("On Event = {:?}", ev);
       })
       .build(workflow);
   worker.run().await.unwrap();
}
```

## Observability

You can track your workflows using [apalis-board](https://github.com/apalis-dev/apalis-board).
![Task](https://github.com/apalis-dev/apalis-board/raw/master/screenshots/task.png)

## Backend Support

- [x] JSONStorage
- [x] SqliteStorage
- [x] RedisStorage
- [x] PostgresStorage
- [ ] MysqlStorage
- [ ] RsMq

## Roadmap

- [x] Then: Sequential execution
- [x] Delay: Delay execution
- [x] FilterMap: MapReduce
- [ ] Fold
- [ ] Repeater
- [ ] Subflow
- [ ] DAG

## See also:

- [Underway](https://github.com/maxcountryman/underway): Postgres-only solution that inspired some parts of this crate

## License

Licensed under MIT or Apache-2.0.
