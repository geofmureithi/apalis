 # apalis-workflow

 This crate provides a flexible and composable workflow engine for [apalis].

 ## Overview

 The workflow engine allows you to define a sequence of steps, each represented as a service, that can process tasks in a customizable manner. Each step can have pre-processing, main execution, and post-processing hooks, enabling advanced control over task execution flow.

 Workflows are built by composing steps, and can be executed using supported backends. The engine supports asynchronous execution, error handling, and integration with the `apalis` worker system.

 ## Key Concepts

 - **Step**: Represents a single unit of work in the workflow. Each step can define pre, run, and post hooks.
 - **WorkFlow**: Manages a sequence of steps and orchestrates their execution.
 - **TaskFlowSink**: Trait for pushing tasks into the workflow, supporting step indexing and context metadata.
 - **CompositeService**: Wraps a step as a boxed Tower service for uniform execution.
 - **WorkflowError**: Unified error type for workflow operations.

 ## Features

 - Compose workflows from reusable steps
 - Asynchronous execution of steps
 - Customizable hooks for pre, run, and post step logic
 - Integration with `apalis` backends and workers
 - Strongly typed task and context handling
 - Extensible error handling

 ## Example

 ```rust
 let workflow = WorkFlow::new("simple-workflow")
     .then(|a: usize| async move { Ok::<_, BoxDynError>(a - 2) })
     .delay_for(Duration::from_millis(1000))
     .then(|a| async move { Ok::<_, BoxDynError>(a + 3) });
 ```

 ## Usage

 1. Define your steps by implementing the `Step` trait.
 2. Compose them into a workflow using `WorkFlow`.
 3. Push tasks into the workflow using a compatible backend.
 4. Run the workflow with an `apalis` worker.

 ## License

 Licensed under MIT or Apache-2.0.
