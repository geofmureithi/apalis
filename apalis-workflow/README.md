# apalis-workflow

This crate provides a flexible and composable workflow engine for [apalis](https://github.com/apalis-dev/apalis). Can be used for building general workflows or advanced LLM workflows.

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
- Compile-time guarantees for workflows.

## Example

```rust,ignore
use apalis::prelude::*;
use apalis_workflow::*;
use apalis_core::backend::json::JsonStorage;

#[tokio::main]
async fn main() {
   let workflow = Workflow::new("odd-numbers-workflow")
       .and_then(|a: usize| async move { Ok::<_, BoxDynError>((0..a).collect::<Vec<_>>()) })
       .filter_map(|x| async move { if x % 2 != 0 { Some(x) } else { None } })
       .and_then(|a: Vec<usize>| async move {
           println!("Sum: {}", a.iter().sum::<usize>());
           Ok::<_, BoxDynError>(())
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

- [x] [JSONStorage](https://docs.rs/apalis-core/1.0.0-beta.2/apalis_core/backend/json/struct.JsonStorage.html)
- [x] [SqliteStorage](https://docs.rs/apalis-sqlite#workflow-example)
- [x] [RedisStorage](https://docs.rs/apalis-redis#workflow-example)
- [x] [PostgresStorage](https://docs.rs/apalis-postgres#workflow-example)
- [ ] MysqlStorage
- [ ] RsMq

## Roadmap

- [x] AndThen: Sequential execution on success
- [x] Delay: Delay execution
- [x] FilterMap: MapReduce
- [x] Fold
- [-] Repeater
- [-] Subflow
- [-] DAG

## Inspirations:

- [Underway](https://github.com/maxcountryman/underway): Postgres-only `stepped` solution
- [dagx](https://github.com/swaits/dagx): blazing fast in-memory `dag` solution

## License

Licensed under MIT or Apache-2.0.
