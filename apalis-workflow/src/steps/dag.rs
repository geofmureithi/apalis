use std::{
    collections::{HashMap, HashSet},
    fmt::{Debug, Display},
    marker::PhantomData,
    sync::Arc,
    task::{Context, Poll},
};

use apalis_core::{
    backend::TaskSink,
    error::BoxDynError,
    task::{Task, builder::TaskBuilder, metadata::MetadataExt},
    worker::builder::IntoWorkerService,
};
use futures::TryFutureExt;
use futures::{future::BoxFuture, lock::Mutex};
use petgraph::{
    Direction,
    algo::toposort,
    graph::{DiGraph, NodeIndex},
    visit::EdgeRef,
};
use serde::{Deserialize, Serialize};
use tower::{Service, ServiceBuilder};

use crate::{BoxedService, SteppedService};

pub struct DagFlow<Compact, Ctx, IdType, Codec> {
    graph: DiGraph<SteppedService<Compact, Ctx, IdType>, ()>,
    node_mapping: HashMap<String, NodeIndex>,
    codec: PhantomData<Codec>,
}

impl<Compact, Ctx, IdType, Codec> Default for DagFlow<Compact, Ctx, IdType, Codec> {
    fn default() -> Self {
        Self {
            graph: DiGraph::new(),
            node_mapping: HashMap::new(),
            codec: PhantomData,
        }
    }
}

impl<Compact, Ctx, IdType, Codec> DagFlow<Compact, Ctx, IdType, Codec> {
    pub fn new_typed<T>() -> DagFlow<T, Ctx, IdType, Codec> {
        DagFlow {
            graph: DiGraph::new(),
            node_mapping: HashMap::new(),
            codec: PhantomData,
        }
    }
}

impl<Compact, Ctx, IdType, Codec> DagFlow<Compact, Ctx, IdType, Codec>
where
    Compact: Debug,
{
    /// Add a node to the DAG
    pub fn node<S, Current, Output, E: Into<BoxDynError>>(mut self, name: &str, service: S) -> Self
    where
        S: Service<Task<Current, Ctx, IdType>, Response = Output, Error = E> + Send + 'static,
        S::Future: Send + 'static,
        Output: Send + Sync + 'static,
        Codec: apalis_core::backend::codec::Codec<Output, Compact = Compact>
            + apalis_core::backend::codec::Codec<Current, Compact = Compact>,
        <Codec as apalis_core::backend::codec::Codec<Current>>::Error: Debug,
        <Codec as apalis_core::backend::codec::Codec<Output>>::Error: Debug,
    {
        let current = self.node_mapping.get_mut(name);

        match current {
            Some(exists) => {
                // Ensure it exists
                let _ = self.graph.node_weight_mut(*exists).unwrap();
            }
            None => {
                let dag_service = ServiceBuilder::new()
                    .map_request(|req: Task<Compact, Ctx, IdType>| {
                        req.map(|args| {
                            let c: Current = Codec::decode(&args).unwrap_or_else(|_| {
                                panic!(
                                    "Could not decode node, expecting {}",
                                    std::any::type_name::<Current>()
                                )
                            });
                            c
                        })
                    })
                    .map_response(|res: Output| {
                        // .map_err(|e| WorkflowError::CodecError(e.into()))?;
                        Codec::encode(&res).unwrap()
                    })
                    .map_err(|e: E| e.into())
                    .service(service);

                let node_id = self.graph.add_node(BoxedService::new(dag_service));
                self.node_mapping.insert(name.to_owned(), node_id);
            }
        }

        self
    }
    /// Add an edge to the DAG
    pub fn edge(mut self, from: &str, to: &str) -> Self {
        let from_node = self
            .node_mapping
            .get(from)
            .ok_or_else(|| format!("Node '{from}' not found"))
            .unwrap();
        let to_node = self
            .node_mapping
            .get(to)
            .ok_or_else(|| format!("Node '{to}' not found"))
            .unwrap();

        self.graph.add_edge(*from_node, *to_node, ());
        self
    }
    /// Build the DAG executor
    pub fn build(self) -> Result<DagExecutor<Compact, Ctx, IdType>, String> {
        // Validate DAG (check for cycles)
        let sorted = toposort(&self.graph, None).map_err(|_| "DAG contains cycles")?;

        Ok(DagExecutor {
            graph: self.graph,
            node_mapping: self.node_mapping,
            topological_order: sorted,
        })
    }
}

pub struct RoutedDagService<Inner, SinkT> {
    inner: Inner,
    sink: Arc<Mutex<SinkT>>,
}

/// Request for DAG node execution
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DagRequest {
    pub node_id: NodeIndex,
    pub execution_context: ExecutionContext,
}

impl DagRequest {
    pub fn new(node_id: NodeIndex, context: ExecutionContext) -> Self {
        Self {
            node_id,
            execution_context: context,
        }
    }
}

/// Context for tracking DAG execution state
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ExecutionContext {
    pub nodes: HashMap<NodeIndex, String>,
    pub completed_nodes: HashSet<NodeIndex>,
}

/// Executor for DAG workflows
pub struct DagExecutor<Compact, Ctx, IdType> {
    graph: DiGraph<SteppedService<Compact, Ctx, IdType>, ()>,
    node_mapping: HashMap<String, NodeIndex>,
    topological_order: Vec<NodeIndex>,
}

impl<Compact, Ctx, Cdc, SinkT, IdType> Service<Task<Compact, Ctx, IdType>>
    for RoutedDagService<DagExecutor<Compact, Ctx, IdType>, SinkT>
where
    Compact: Send + 'static + Clone,
    SinkT: TaskSink<Compact, Compact = Compact, Codec = Cdc, IdType = IdType, Context = Ctx>
        + 'static
        + Send,
    Ctx: MetadataExt<DagRequest> + Send + Sync + 'static + Default,
    IdType: Send + 'static + Display,
    <Ctx as MetadataExt<DagRequest>>::Error: Debug,
{
    type Response = Compact;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, task: Task<Compact, Ctx, IdType>) -> Self::Future {
        let meta: DagRequest = task.parts.ctx.extract().unwrap_or(DagRequest {
            node_id: NodeIndex::new(0),
            execution_context: ExecutionContext::default(),
        });
        let mut execution_context = meta.execution_context.clone();
        let index = meta.node_id;
        let predecessors: Vec<NodeIndex> = self
            .inner
            .graph
            .neighbors_directed(index, Direction::Incoming)
            .collect();

        // Panic? if not all predecessors are completed
        if !predecessors
            .iter()
            .all(|&pred| execution_context.completed_nodes.contains(&pred))
        {
            return async move {
                Err(
                    format!("Cannot execute node {index:?} before its predecessors are completed")
                        .into(),
                )
            }
            .boxed();
        }
        let targets: HashSet<_> = self.inner.graph.edges(index).map(|s| s.target()).collect();

        let target_edges = targets
            .into_iter()
            .map(|s| {
                (
                    s,
                    self.inner
                        .graph
                        .neighbors_directed(s, Direction::Incoming)
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<HashMap<_, _>>();
        let svc = self.inner.graph.node_weight_mut(index).unwrap();

        let sink = self.sink.clone();
        use futures::FutureExt;
        let task_id = task.parts.task_id.as_ref().unwrap().to_string();
        let fut = svc.call(task).and_then(move |res| async move {
            let mut sink = sink.lock().await;
            execution_context.completed_nodes.insert(index);
            for (node_index, targets) in target_edges {
                // Check if all predecessors of the target node are completed
                let all_predecessors_completed = targets
                    .iter()
                    .all(|pred| execution_context.completed_nodes.contains(pred));

                // if all_predecessors_completed {

                // }
                let mut ctx = Ctx::default();
                ctx.inject(DagRequest {
                    node_id: node_index,
                    execution_context: execution_context.clone(),
                })
                .unwrap();
                let req = TaskBuilder::new(res.clone()).with_ctx(ctx).build();
                let _res = sink.push_task(req).await;
            }
            Ok(res.clone())
        });

        fut.boxed()
    }
}

impl<FlowSink, Compact>
    IntoWorkerService<FlowSink, RoutedDagService<Self, FlowSink>, Compact, FlowSink::Context>
    for DagExecutor<Compact, FlowSink::Context, FlowSink::IdType>
where
    FlowSink::Context: MetadataExt<DagRequest> + Send + Sync + 'static + Default,
    FlowSink::IdType: Send + 'static,
    <FlowSink::Context as MetadataExt<DagRequest>>::Error: Debug,
    FlowSink: TaskSink<Compact, Args = Compact, Compact = Compact> + Send + Clone + 'static,
    Compact: Send + Clone + 'static,
    FlowSink::IdType: Send + 'static + Display,
{
    fn into_service(self, sink: &FlowSink) -> RoutedDagService<Self, FlowSink> {
        RoutedDagService {
            inner: self,
            sink: Arc::new(Mutex::new(sink.clone())),
        }
    }
}

#[cfg(test)]
mod tests {
    use apalis_core::{
        backend::{WeakTaskSink, codec::json::JsonCodec, json::JsonStorage},
        error::BoxDynError,
        task::{builder::TaskBuilder, metadata::MetadataExt, task_id::RandomId},
        task_fn::task_fn,
        worker::{
            builder::WorkerBuilder, context::WorkerContext, event::Event,
            ext::event_listener::EventListenerExt,
        },
    };
    use petgraph::graph::NodeIndex;
    use serde_json::{Map, Value};

    use crate::steps::dag::{DagExecutor, DagFlow, DagRequest};

    #[tokio::test]
    async fn simple_dag_flow() {
        let dag: DagExecutor<Value, Map<String, serde_json::Value>, RandomId> =
            DagFlow::<Value, Map<String, serde_json::Value>, RandomId, JsonCodec<Value>>::default()
                .node(
                    "step1",
                    task_fn(|task: String| async move { format!("Hello, {task}") }),
                )
                .node(
                    "step2",
                    task_fn(|task: String, wrk: WorkerContext| async move {
                        wrk.stop().unwrap();
                        Ok::<String, BoxDynError>(format!("Goodbye, {task}"))
                    }),
                )
                .edge("step1", "step2")
                .build()
                .unwrap();
        let mut in_memory = JsonStorage::new_temp().unwrap();

        in_memory.push("Some task data".to_owned()).await.unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(in_memory)
            .on_event(|ctx, ev| {
                println!("On Event = {ev:?}");
                if matches!(ev, Event::Error(_)) {
                    ctx.stop().unwrap();
                }
            })
            .build(dag);
        worker.run().await.unwrap();
    }

    // ## Workflow Diagram
    // ```text
    //     EXTRACT          TRANSFORM        COMBINE        AGGREGATE
    //   ┌──────────┐     ┌──────────┐
    //   │ LoadData │────►│Transform │────┐
    //   │    A     │     │   (×2)   │    │
    //   └──────────┘     └──────────┘    │
    //                                     ├──►┌─────────┐───►┌─────────┐───►┌─────────┐
    //                                     │   │ Combine │    │ Average │    │ Report  │
    //                                     └──►└─────────┘    └─────────┘    └─────────┘
    //   ┌──────────┐     ┌──────────┐    ┌──►                    │              │
    //   │ LoadData │────►│Transform │────┘                       │              │
    //   │    B     │     │   (×3)   │                            └──────────────┘
    //   └──────────┘     └──────────┘
    // ```
    #[tokio::test]
    async fn complex_dag_flow() {
        let dag: DagExecutor<Value, Map<String, serde_json::Value>, RandomId> =
            DagFlow::<Value, Map<String, serde_json::Value>, RandomId, JsonCodec<Value>>::default()
                .node(
                    "load_data_a",
                    task_fn(|_: ()| async move { Ok::<Vec<u32>, BoxDynError>(vec![1, 2, 3]) }),
                )
                .node(
                    "load_data_b",
                    task_fn(|_: ()| async move { Ok::<Vec<u32>, BoxDynError>(vec![4, 5, 6]) }),
                )
                .node(
                    "transform_a",
                    task_fn(|task: Vec<u32>| async move {
                        Ok::<u32, BoxDynError>(task.into_iter().map(|x| x * 2).sum())
                    }),
                )
                .node(
                    "transform_b",
                    task_fn(|task: Vec<u32>| async move {
                        Ok::<u32, BoxDynError>(task.into_iter().map(|x| x * 3).sum())
                    }),
                )
                .edge("load_data_a", "transform_a")
                .edge("load_data_b", "transform_b")
                .node(
                    "combine",
                    task_fn(|task: Vec<u32>| async move {
                        Ok::<u32, BoxDynError>(task.into_iter().sum())
                    }),
                )
                .edge("transform_a", "combine")
                .edge("transform_b", "combine")
                .node(
                    "average",
                    task_fn(|task: usize| async move { Ok::<f64, BoxDynError>(task as f64 / 2.0) }),
                )
                .edge("combine", "average")
                .node(
                    "report",
                    task_fn(|task: f64| async move {
                        println!("Average: {task}");
                        Ok::<(), BoxDynError>(())
                    }),
                )
                .edge("average", "report")
                .build()
                .unwrap();
        let mut in_memory = JsonStorage::new_temp().unwrap();

        let mut ctx = Map::default();

        ctx.inject(DagRequest::new(NodeIndex::new(0), Default::default()))
            .unwrap();

        let node_0 = TaskBuilder::new(()).with_ctx(ctx).build();
        in_memory.push_task(node_0).await.unwrap();

        let mut ctx = Map::default();

        ctx.inject(DagRequest::new(NodeIndex::new(1), Default::default()))
            .unwrap();

        let node_1 = TaskBuilder::new(()).with_ctx(ctx).build();

        in_memory.push_task(node_1).await.unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(in_memory)
            .on_event(|ctx, ev| {
                println!("On Event = {ev:?}");
                if matches!(ev, Event::Error(_)) {
                    ctx.stop().unwrap();
                }
            })
            .build(dag);
        worker.run().await.unwrap();
    }

    fn test_typed_dag_flow() {
        let dag = DagFlow::<String, (), u64, JsonCodec<Vec<u8>>>::new_typed()
            .node(
                "step1",
                task_fn(|task: String| async move {
                    Ok::<String, BoxDynError>(format!("Hello, {task}"))
                }),
            )
            .node(
                "step2",
                // TODO: Enforce type mismatch here
                task_fn(|task: Vec<u8>| async move {
                    Ok::<String, BoxDynError>(format!("Goodbye, {task:?}"))
                }),
            )
            .edge("step1", "step2");
    }
}
