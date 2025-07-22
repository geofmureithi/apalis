use std::collections::HashMap;
use std::future::{ready, Future};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::{BoxFuture, FutureExt};
use futures::lock::Mutex;
use serde::{Deserialize, Serialize};
use tower::util::BoxService;
use tower::{Layer, Service, ServiceExt};

use crate::backend::codec::{Decoder, Encoder};
use crate::backend::{Backend, TaskSink};
use crate::error::BoxDynError;
use crate::request::task_id::TaskId;
use crate::request::Request;
use crate::service_fn::{service_fn, ServiceFn};
use crate::worker::builder::WorkerFactory;
use crate::workflow::GoTo;

type BoxedService<Input, Output> = tower::util::BoxService<Input, Output, BoxDynError>;

type BranchedService<Compact, Ctx> = BoxedService<Request<Compact, Ctx>, GoTo<Compact>>;

type BranchRouteService<Compact, Ctx> =
    BoxedService<Request<BranchRequest<Compact>, Ctx>, GoTo<Compact>>;

/// Represents a request for a specific branch in a conditional workflow.
///
/// This struct carries the data for a branch, its identifier,
/// workflow state, and the execution context.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct BranchRequest<T> {
    /// The identifier of the branch to execute.
    pub branch_id: String,
    /// The data associated with the branch.
    pub data: T,
    /// The shared workflow state.
    pub workflow_state: WorkflowState,
    /// The execution context of the workflow.
    pub execution_ctx: ExecutionContext,
}

/// The execution context for a workflow, containing information about previous steps.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ExecutionContext {
    /// A map of step indices to the `TaskId` of the worker that executed them.
    previous_nodes: HashMap<usize, TaskId>,
}

impl<T> BranchRequest<T> {
    /// Creates a new `BranchRequest`.
    pub fn new(
        branch_id: String,
        data: T,
        workflow_state: WorkflowState,
        execution_ctx: ExecutionContext,
    ) -> Self {
        Self {
            branch_id,
            data,
            workflow_state,
            execution_ctx,
        }
    }
}

/// Represents the result of a branch condition evaluation.
#[derive(Clone, Debug)]
pub enum BranchChoice {
    /// Execute a specific branch by ID.
    Branch(String),
    /// Execute multiple branches in parallel.
    Parallel(Vec<String>),
    /// Skip all branches and proceed with the given data.
    Skip,
    /// Loop back to a specific node by ID.
    LoopTo(String),
}

/// Represents shared workflow state that persists across node executions.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct WorkflowState {
    /// Shared data accessible by all nodes.
    pub shared: HashMap<String, serde_json::Value>,
    /// The original query or input that started the workflow.
    pub query: Option<String>,
}

impl WorkflowState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_query(query: String) -> Self {
        Self {
            shared: HashMap::new(),
            query: Some(query),
        }
    }

    pub fn get<T>(&self, key: &str) -> Option<T>
    where
        T: serde::de::DeserializeOwned,
    {
        self.shared
            .get(key)
            .and_then(|v| serde_json::from_value(v.clone()).ok())
    }

    pub fn set<T>(&mut self, key: String, value: T)
    where
        T: serde::Serialize,
    {
        if let Ok(json_value) = serde_json::to_value(value) {
            self.shared.insert(key, json_value);
        }
    }
}

/// A trait for defining branching logic with access to workflow state.
pub trait BranchCondition<T> {
    type Error: Into<BoxDynError>;

    /// Evaluates the condition and returns which branch(es) to execute.
    fn evaluate(&self, data: &T, state: &WorkflowState) -> Result<BranchChoice, Self::Error>;
}

/// A simple function-based branch condition with state access.
pub struct FnBranchCondition<F> {
    condition_fn: F,
}

impl<F> FnBranchCondition<F> {
    pub fn new(condition_fn: F) -> Self {
        Self { condition_fn }
    }
}

impl<F, T, E> BranchCondition<T> for FnBranchCondition<F>
where
    F: Fn(&T, &WorkflowState) -> Result<BranchChoice, E>,
    E: Into<BoxDynError>,
{
    type Error = E;

    fn evaluate(&self, data: &T, state: &WorkflowState) -> Result<BranchChoice, Self::Error> {
        (self.condition_fn)(data, state)
    }
}

/// A builder for creating branched workflows.
///
/// `BranchBuilder` is used to define multiple conditional branches
/// that can be executed based on runtime conditions.
pub struct BranchBuilder<Input, Current, Svc, Codec, Compact, Ctx> {
    branches: HashMap<String, Svc>,
    condition: Option<Box<dyn BranchCondition<Current, Error = BoxDynError> + Send + Sync>>,
    input: PhantomData<Input>,
    current: PhantomData<Current>,
    codec: PhantomData<Codec>,
    fallback: Option<
        Box<
            dyn FnMut(
                    Request<Compact, Ctx>,
                ) -> BoxFuture<'static, Result<GoTo<Compact>, BoxDynError>>
                + Send
                + Sync
                + 'static,
        >,
    >,
}

impl<Input, Compact, Ctx, Codec>
    BranchBuilder<Input, Input, BranchedService<Compact, Ctx>, Codec, Compact, Ctx>
{
    /// Creates a new `BranchBuilder`.
    pub fn new() -> BranchBuilder<Input, Input, BranchedService<Compact, Ctx>, Codec, Compact, Ctx>
    {
        BranchBuilder {
            branches: HashMap::new(),
            condition: None,
            input: PhantomData,
            current: PhantomData,
            codec: PhantomData,
            fallback: None,
        }
    }
}

impl<Input, Current, Compact, Ctx, Codec>
    BranchBuilder<Input, Current, BranchedService<Compact, Ctx>, Codec, Compact, Ctx>
{
    /// Sets the branching condition that determines which branch to execute.
    pub fn condition<C>(mut self, condition: C) -> Self
    where
        C: BranchCondition<Current, Error = BoxDynError> + Send + Sync + 'static,
    {
        self.condition = Some(Box::new(condition));
        self
    }

    /// Sets a condition using a closure with state access.
    pub fn condition_fn<F, E>(self, condition_fn: F) -> Self
    where
        F: Fn(&Current, &WorkflowState) -> Result<BranchChoice, E> + Send + Sync + 'static,
        E: Into<BoxDynError>,
    {
        self.condition(FnBranchCondition::new(move |data, state| {
            condition_fn(data, state).map_err(Into::into)
        }))
    }

    /// Sets a fallback service to be called when a branch is not found or condition fails.
    pub fn fallback<
        F: Future<Output = Result<GoTo<Compact>, E>> + Send + 'static,
        E: Into<BoxDynError>,
    >(
        mut self,
        mut fun: impl FnMut(Request<Compact, Ctx>) -> F + Send + Sync + 'static,
    ) -> Self {
        self.fallback = Some(Box::new(move |r| fun(r).map_err(|e| e.into()).boxed()));
        self
    }
}

/// A service that wraps a single branch in a workflow, handling encoding and decoding.
pub struct SingleBranchService<S, Codec, Current, Next, Compact, Ctx> {
    inner: S,
    _marker: PhantomData<(Codec, Current, Next, Compact, Ctx)>,
}

impl<S, Codec, Current, Next, Compact, Ctx>
    SingleBranchService<S, Codec, Current, Next, Compact, Ctx>
{
    /// Creates a new `SingleBranchService`.
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }
}

impl<S, Codec, Current, Next, Compact, Ctx, E> Service<Request<Compact, Ctx>>
    for SingleBranchService<S, Codec, Current, Next, Compact, Ctx>
where
    S: Service<Request<Current, Ctx>, Response = GoTo<Next>, Error = E> + Send + 'static,
    S::Future: Send + 'static,
    Codec: Decoder<Current, Compact = Compact> + Encoder<Next, Compact = Compact>,
    <Codec as Decoder<Current>>::Error: std::error::Error + Send + Sync + 'static,
    <Codec as Encoder<Next>>::Error: std::error::Error + Send + Sync + 'static,
    E: Into<BoxDynError>,
    Compact: Send + 'static,
{
    type Response = GoTo<Compact>;
    type Error = BoxDynError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: Request<Compact, Ctx>) -> Self::Future {
        // Decode request
        let decoded = Codec::decode(&req.args);
        match decoded {
            Ok(decoded) => {
                let req = Request::new_with_parts(decoded, req.parts);
                let fut = self.inner.call(req);

                Box::pin(async move {
                    let res = fut.await.map_err(Into::into)?;

                    let encoded = match res {
                        GoTo::Next(next) => GoTo::Next(Codec::encode(&next).map_err(|e| {
                            BoxDynError::from(BranchError::CodecError(
                                e.into(),
                                std::any::type_name::<Next>(),
                            ))
                        })?),
                        GoTo::Delay { next, delay } => GoTo::Delay {
                            next: Codec::encode(&next).map_err(|e| {
                                BoxDynError::from(BranchError::CodecError(
                                    e.into(),
                                    std::any::type_name::<Next>(),
                                ))
                            })?,
                            delay,
                        },
                        GoTo::Done(done) => GoTo::Done(Codec::encode(&done).map_err(|e| {
                            BoxDynError::from(BranchError::CodecError(
                                e.into(),
                                std::any::type_name::<Next>(),
                            ))
                        })?),
                    };

                    Ok(encoded)
                })
            }
            Err(e) => {
                let current = std::any::type_name::<Current>();
                ready(Err(BoxDynError::from(BranchError::CodecError(
                    e.into(),
                    current,
                ))))
                .boxed()
            }
        }
    }
}

// Example usage demonstrating the Python workflow pattern
#[cfg(test)]
mod example {
    use crate::backend::codec::json::JsonCodec;

    use super::*;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct SearchDecision {
        action: String,
        reason: String,
        search_term: Option<String>,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct SearchResult {
        term: String,
        results: String,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    enum NodeInput {
        Query(String),
        Decision(SearchDecision),
        SearchResult(SearchResult),
    }

    // Simulate LLM call for deciding action
    fn decide_action(query: &str, context: &str) -> SearchDecision {
        // Simple logic - if context is empty, search; otherwise answer
        if context.is_empty() || context == "No previous search" {
            SearchDecision {
                action: "search".to_string(),
                reason: "No previous context available".to_string(),
                search_term: Some(format!("Nobel Prize Physics 2024")),
            }
        } else {
            SearchDecision {
                action: "answer".to_string(),
                reason: "Sufficient context available".to_string(),
                search_term: None,
            }
        }
    }

    // Simulate web search
    fn search_web(term: &str) -> String {
        format!("Search results for '{}': The 2024 Nobel Prize in Physics was awarded to John Hopfield and Geoffrey Hinton for their work on machine learning.", term)
    }

    // Simulate LLM call for generating answer
    fn generate_answer(query: &str, context: &str) -> String {
        format!("Based on the context: {}\n\nAnswer: The 2024 Nobel Prize in Physics was awarded to John Hopfield and Geoffrey Hinton for their pioneering work in machine learning and artificial neural networks.", context)
    }

    pub fn create_workflow_example() -> BranchBuilder<
        NodeInput,
        NodeInput,
        BranchedService<serde_json::Value, ()>,
        JsonCodec<Vec<u8>>,
        serde_json::Value,
        (),
    > {
        BranchBuilder::new()
            .condition_fn(
                |input: &NodeInput, state: &WorkflowState| -> Result<BranchChoice, BoxDynError> {
                    match input {
                        NodeInput::Query(_) => {
                            Ok(BranchChoice::Branch("decide_action".to_string()))
                        }
                        NodeInput::Decision(decision) => {
                            if decision.action == "search" {
                                Ok(BranchChoice::Branch("search_web".to_string()))
                            } else {
                                Ok(BranchChoice::Branch("direct_answer".to_string()))
                            }
                        }
                        NodeInput::SearchResult(_) => {
                            Ok(BranchChoice::LoopTo("decide_action".to_string()))
                        }
                    }
                },
            )
            // DecideAction node
            .branch_fn("decide_action", |req: Request<NodeInput, ()>| async move {
                let query = match &req.args {
                    NodeInput::Query(q) => q.clone(),
                    _ => req
                        .get::<WorkflowState>()
                        .and_then(|state| state.query.clone())
                        .unwrap_or_default(),
                };

                let state = req.get::<WorkflowState>().unwrap_or_default();
                let context = state
                    .get::<String>("context")
                    .unwrap_or_else(|| "No previous search".to_string());

                let decision = decide_action(&query, &context);

                // Update workflow state
                let mut new_state = state;
                if let Some(search_term) = &decision.search_term {
                    new_state.set("search_term".to_string(), search_term.clone());
                }

                Ok(GoTo::Next(NodeInput::Decision(decision)))
            })
            // SearchWeb node
            .branch_fn("search_web", |req: Request<NodeInput, ()>| async move {
                let state = req.get::<WorkflowState>().unwrap_or_default();
                let search_term = state.get::<String>("search_term").unwrap_or_default();

                let results = search_web(&search_term);

                // Update context in workflow state
                let mut new_state = state;
                let mut prev_searches: Vec<SearchResult> =
                    new_state.get("context").unwrap_or_default();
                prev_searches.push(SearchResult {
                    term: search_term.clone(),
                    results: results.clone(),
                });
                new_state.set(
                    "context".to_string(),
                    serde_json::to_string(&prev_searches).unwrap_or_default(),
                );

                Ok(GoTo::Next(NodeInput::SearchResult(SearchResult {
                    term: search_term,
                    results,
                })))
            })
            // DirectAnswer node
            .branch_fn("direct_answer", |req: Request<NodeInput, ()>| async move {
                let state = req.get::<WorkflowState>().unwrap_or_default();
                let query = state.query.unwrap_or_default();
                let context = state
                    .get::<String>("context")
                    .unwrap_or_else(|| "".to_string());

                let answer = generate_answer(&query, &context);

                // Update workflow state with final answer
                let mut new_state = state;
                new_state.set("answer".to_string(), answer.clone());

                println!("Answer: {}", answer);

                Ok(GoTo::Done(NodeInput::Query(answer)))
            })
            .fallback(|req| async move {
                println!("Fallback: Unknown branch or condition");
                Ok(GoTo::Done(req.args))
            })
    }

    #[tokio::test]
    async fn test_workflow_example() {
        let workflow = create_workflow_example();

        // This would be used with your backend/sink system:
        // let backend = YourBackend::new();
        // let service = workflow.service(&backend);

        // Example of how the workflow would process:
        // 1. Start with Query -> routes to "decide_action"
        // 2. DecideAction sees no context -> returns Decision("search") -> routes to "search_web"
        // 3. SearchWeb returns SearchResult -> loops back to "decide_action"
        // 4. DecideAction sees context -> returns Decision("answer") -> routes to "direct_answer"
        // 5. DirectAnswer generates final response

        let initial_state =
            WorkflowState::with_query("Who won the Nobel Prize in Physics 2024?".to_string());
        let request = BranchRequest::new(
            "".to_string(), // Empty branch_id triggers condition evaluation
            NodeInput::Query("Who won the Nobel Prize in Physics 2024?".to_string()),
            initial_state,
            ExecutionContext::default(),
        );

        println!("Workflow would process: {:?}", request);
    }
}

impl<Input, Current, Compact, Codec, Ctx>
    BranchBuilder<Input, Current, BranchedService<Compact, Ctx>, Codec, Compact, Ctx>
where
    Ctx: Send + 'static,
    Current: Send + 'static,
    Compact: Send + 'static,
{
    /// Adds a new branch to the workflow.
    ///
    /// Branches are `tower::Service`s that process the job conditionally.
    pub fn branch<S, Next, E: Into<BoxDynError>>(
        mut self,
        branch_id: impl Into<String>,
        service: S,
    ) -> BranchBuilder<Input, Next, BranchedService<Compact, Ctx>, Codec, Compact, Ctx>
    where
        S: Service<Request<Current, Ctx>, Response = GoTo<Next>, Error = E> + Send + 'static,
        S::Future: Send + 'static,
        Codec: Encoder<Next, Compact = Compact>
            + Decoder<Current, Compact = Compact>
            + Encoder<Next, Compact = Compact>,
        <Codec as Decoder<Current>>::Error: std::error::Error + Send + Sync + 'static,
        <Codec as Encoder<Next>>::Error: std::error::Error + Send + Sync + 'static,
        Next: Send + 'static,
        Codec: Send + 'static,
    {
        self.branches.insert(
            branch_id.into(),
            BoxedService::new(
                SingleBranchService::<S, Codec, Current, Next, Compact, Ctx>::new(service),
            ),
        );
        BranchBuilder {
            branches: self.branches,
            condition: self.condition,
            input: self.input,
            current: PhantomData,
            codec: PhantomData,
            fallback: self.fallback,
        }
    }

    /// Adds a new branch to the workflow from a function.
    ///
    /// This is a convenience method that wraps a function into a `tower::Service`.
    pub fn branch_fn<F, Next, E: Into<BoxDynError>, FnArgs>(
        self,
        branch_id: impl Into<String>,
        branch_fn: F,
    ) -> BranchBuilder<Input, Next, BranchedService<Compact, Ctx>, Codec, Compact, Ctx>
    where
        Codec: Encoder<Next, Compact = Compact> + Decoder<Current, Compact = Compact>,
        <Codec as Decoder<Current>>::Error: std::error::Error + Send + Sync + 'static,
        <Codec as Encoder<Next>>::Error: std::error::Error + Send + Sync + 'static,
        F: Send + 'static,
        ServiceFn<F, Current, Ctx, FnArgs>:
            Service<Request<Current, Ctx>, Response = GoTo<Next>, Error = E>,
        FnArgs: std::marker::Send + 'static,
        Current: std::marker::Send + 'static,
        Ctx: Send + 'static,
        <ServiceFn<F, Current, Ctx, FnArgs> as Service<Request<Current, Ctx>>>::Future:
            Send + 'static,
        Next: Send + 'static,
        Codec: Send + 'static,
    {
        self.branch(branch_id, service_fn::<F, Current, Ctx, FnArgs>(branch_fn))
    }
}

impl<Compact, Ctx, B, M, Input, Current, Cdc>
    WorkerFactory<BranchRequest<Compact>, Ctx, BranchRouteService<Compact, Ctx>, B, M>
    for BranchBuilder<Input, Current, BranchedService<Compact, Ctx>, Cdc, Compact, Ctx>
where
    B: Backend<BranchRequest<Compact>, Ctx>,
    M: Layer<BranchRouteService<Compact, Ctx>>,
    Input: Send + 'static,
    Current: Send + 'static + Clone,
    Cdc: Send
        + 'static
        + Decoder<BranchRequest<Compact>, Compact = Compact>
        + Encoder<BranchRequest<Compact>, Compact = Compact>
        + Decoder<Current, Compact = Compact>,
    <Cdc as Encoder<BranchRequest<Compact>>>::Error: std::error::Error + Send + Sync + 'static,
    <Cdc as Decoder<BranchRequest<Compact>>>::Error: std::error::Error + Send + Sync + 'static,
    <Cdc as Decoder<Current>>::Error: std::error::Error + Send + Sync + 'static,
    Ctx: 'static,
    Compact: 'static + Send + Clone,
    B::Sink:
        TaskSink<BranchRequest<Compact>, Codec = Cdc, Compact = Compact, Context = Ctx> + 'static,
    <<B as Backend<BranchRequest<Compact>, Ctx>>::Sink as TaskSink<BranchRequest<Compact>>>::Error:
        std::error::Error + Send + Sync,
{
    fn service(self, backend: &B) -> BranchRouteService<Compact, Ctx> {
        let sink = backend.sink();
        BoxedService::new(RoutedBranchService {
            inner: self,
            sink: Arc::new(Mutex::new(sink)),
        })
    }
}

/// Errors that can occur during the execution of a branched workflow.
#[derive(Debug, thiserror::Error)]
pub enum BranchError {
    /// Occurs when a branch ID is not found.
    #[error("The branch `{0}` is not available")]
    BranchNotFound(String),
    /// Occurs when no condition is set for conditional branching.
    #[error("No condition set for conditional branching")]
    NoCondition,
    /// Occurs when condition evaluation fails.
    #[error("Condition evaluation failed: {0}")]
    ConditionError(BoxDynError),
    /// Occurs when encoding or decoding of a branch's data fails.
    #[error("Codec failed for branch expecting type {1}: {0}")]
    CodecError(BoxDynError, &'static str),
}

/// A service that routes a `BranchRequest` to the appropriate branch service.
pub struct RoutedBranchService<Inner, SinkT> {
    inner: Inner,
    sink: Arc<Mutex<SinkT>>,
}

impl<Compact, Ctx, Input, Current, Cdc, SinkT> Service<Request<BranchRequest<Compact>, Ctx>>
    for RoutedBranchService<
        BranchBuilder<Input, Current, BranchedService<Compact, Ctx>, Cdc, Compact, Ctx>,
        SinkT,
    >
where
    Compact: Send + 'static + Clone,
    Current: Send + 'static + Clone,
    Cdc: Decoder<BranchRequest<Compact>, Compact = Compact>
        + Encoder<BranchRequest<Compact>, Compact = Compact>
        + Decoder<Current, Compact = Compact>,
    <Cdc as Encoder<BranchRequest<Compact>>>::Error: std::error::Error + Send + Sync + 'static,
    <Cdc as Decoder<BranchRequest<Compact>>>::Error: std::error::Error + Send + Sync + 'static,
    <Cdc as Decoder<Current>>::Error: std::error::Error + Send + Sync + 'static,
    SinkT: TaskSink<BranchRequest<Compact>, Compact = Compact, Codec = Cdc> + 'static,
    SinkT::Error: std::error::Error + Send + Sync + 'static,
{
    type Response = GoTo<Compact>;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<GoTo<Compact>, BoxDynError>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<BranchRequest<Compact>, Ctx>) -> Self::Future {
        let branch_id = request.args.branch_id.clone();
        let task_id = request.parts.task_id.clone();
        let ctx = request.args.execution_ctx.clone();

        // If this is a direct branch request, execute the specified branch
        if !branch_id.is_empty() {
            let mut req = request.map(|s| s.data);
            req.insert(ctx.clone());

            let svc = self.inner.branches.get_mut(&branch_id);
            match svc {
                Some(svc) => {
                    let sink = self.sink.clone();
                    let fut = svc.call(req).and_then(move |res| async move {
                        let mut sink = sink.lock().await;
                        let mut ctx = ctx.clone();
                        ctx.previous_nodes.insert(0, task_id); // Use 0 as branch execution marker

                        match &res {
                            GoTo::Next(next) => {
                                let req = Request::new(BranchRequest::new(
                                    "".to_string(),
                                    next.clone(),
                                    request.args.workflow_state.clone(),
                                    ctx,
                                ));
                                let _res = sink.push_request(req).await?;
                            }
                            GoTo::Delay { next, delay } => {
                                let req = Request::new(BranchRequest::new(
                                    "".to_string(),
                                    next.clone(),
                                    request.args.workflow_state.clone(),
                                    ctx,
                                ));
                                let _res = sink.schedule_request(req, *delay).await?;
                            }
                            GoTo::Done(_) => {}
                        }
                        Ok(res)
                    });

                    fut.boxed()
                }
                None => {
                    // Try fallback if available
                    if let Some(ref mut fallback) = self.inner.fallback {
                        let req = request.map(|s| s.data);
                        fallback(req).boxed()
                    } else {
                        ready(Err(BoxDynError::from(BranchError::BranchNotFound(
                            branch_id,
                        ))))
                        .boxed()
                    }
                }
            }
        } else {
            // This is a conditional branch request - evaluate condition
            let condition = match &self.inner.condition {
                Some(condition) => condition,
                None => return ready(Err(BoxDynError::from(BranchError::NoCondition))).boxed(),
            };

            // Decode the data to evaluate condition
            let decoded_data = match Cdc::decode(&request.args.data) {
                Ok(data) => data,
                Err(e) => {
                    return ready(Err(BoxDynError::from(BranchError::CodecError(
                        e.into(),
                        std::any::type_name::<Current>(),
                    ))))
                    .boxed()
                }
            };

            let choice = match condition.evaluate(&decoded_data, &request.args.workflow_state) {
                Ok(choice) => choice,
                Err(e) => {
                    return ready(Err(BoxDynError::from(BranchError::ConditionError(e)))).boxed()
                }
            };

            let sink = self.sink.clone();
            let ctx = request.args.execution_ctx.clone();
            let task_id = request.parts.task_id.clone();
            let data = request.args.data.clone();
            let mut workflow_state = request.args.workflow_state.clone();

            match choice {
                BranchChoice::Branch(branch_id) => {
                    // Execute single branch
                    let req =
                        Request::new(BranchRequest::new(branch_id, data, workflow_state, ctx));
                    let sink_fut = async move {
                        let mut sink = sink.lock().await;
                        sink.push_request(req).await.map_err(Into::into)
                    };

                    async move {
                        sink_fut.await?;
                        Ok(GoTo::Done(data))
                    }
                    .boxed()
                }
                BranchChoice::Parallel(branch_ids) => {
                    // Execute multiple branches in parallel
                    let sink_fut = async move {
                        let mut sink = sink.lock().await;
                        for branch_id in branch_ids {
                            let req = Request::new(BranchRequest::new(
                                branch_id,
                                data.clone(),
                                workflow_state.clone(),
                                ctx.clone(),
                            ));
                            sink.push_request(req)
                                .await
                                .map_err(|e| -> BoxDynError { e.into() })?;
                        }
                        Ok(())
                    };

                    async move {
                        sink_fut.await?;
                        Ok(GoTo::Done(data))
                    }
                    .boxed()
                }
                BranchChoice::LoopTo(node_id) => {
                    // Loop back to a specific node
                    let req = Request::new(BranchRequest::new(node_id, data, workflow_state, ctx));
                    let sink_fut = async move {
                        let mut sink = sink.lock().await;
                        sink.push_request(req).await.map_err(Into::into)
                    };

                    async move {
                        sink_fut.await?;
                        Ok(GoTo::Done(data))
                    }
                    .boxed()
                }
                BranchChoice::Skip => {
                    // Skip all branches and continue
                    ready(Ok(GoTo::Done(data))).boxed()
                }
            }
        }
    }
}
