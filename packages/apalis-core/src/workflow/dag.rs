use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use futures::{future::BoxFuture, lock::Mutex, FutureExt, Sink, TryFutureExt};
use petgraph::{
    algo::toposort,
    graph::{DiGraph, NodeIndex},
    visit::EdgeRef,
    Direction,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tower::{util::BoxService, Layer, Service, ServiceBuilder};

use crate::{
    backend::{
        codec::{json::JsonCodec, Decoder, Encoder},
        Backend, TaskSink,
    },
    error::BoxDynError,
    request::{task_id::TaskId, Parts, Request},
    service_fn::{service_fn, ServiceFn},
    worker::{
        builder::{WorkerBuilder, WorkerFactory},
        Worker,
    },
    workflow::GoTo,
};

type BoxedService<Input, Output> = tower::util::BoxService<Input, Output, BoxDynError>;

type DagService<Compact, Ctx> = BoxedService<Request<Compact, Ctx>, GoTo<Compact>>;

type RouteService<Compact, Ctx> = BoxedService<Request<Compact, Ctx>, ()>;

/// Request for DAG node execution
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DagRequest<T> {
    pub node_id: NodeIndex,
    pub inputs: HashMap<NodeIndex, T>,
    pub execution_context: ExecutionContext,
}

/// Context for tracking DAG execution state
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ExecutionContext {
    pub completed_nodes: std::collections::HashSet<NodeIndex>,
}

impl<T> DagRequest<T> {
    pub fn new(
        node_id: NodeIndex,
        inputs: HashMap<NodeIndex, T>,
        context: ExecutionContext,
    ) -> Self {
        Self {
            node_id,
            inputs,
            execution_context: context,
        }
    }
}

/// Builder for creating DAG workflows
// #[derive(Clone)]
pub struct DagBuilder<Compact, Ctx, Codec = JsonCodec<Value>> {
    graph: DiGraph<DagService<Compact, Ctx>, ()>,
    node_mapping: HashMap<String, NodeIndex>,
    compact_type: PhantomData<Compact>,
    codec: PhantomData<Codec>,
}

impl<Compact, Ctx> Default for DagBuilder<Compact, Ctx> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Compact, Ctx> DagBuilder<Compact, Ctx> {
    pub fn new() -> Self {
        Self {
            graph: DiGraph::new(),
            node_mapping: HashMap::new(),
            compact_type: PhantomData,
            codec: PhantomData,
        }
    }
}

impl<Compact, Ctx, Codec> DagBuilder<Compact, Ctx, Codec>
where
    Compact: Debug,
{
    pub fn add_node<F, Current, Output, Args, FnArgs, E>(self, node_fn: F) -> Self
    where
        Output: Send + Sync + 'static,
        Codec: Encoder<Output, Compact = Compact> + Decoder<Current, Compact = Compact>,
        <Codec as Decoder<Current>>::Error: Debug,
        <Codec as Encoder<Output>>::Error: Debug,
        F: Send,
        Args: Send,
        Ctx: Send,
        FnArgs: Send,
        ServiceFn<F, Args, Ctx, FnArgs>:
            Service<Request<Current, Ctx>, Response = GoTo<Output>, Error = E> + Send + 'static,
        E: Into<BoxDynError>,
        <ServiceFn<F, Args, Ctx, FnArgs> as Service<Request<Current, Ctx>>>::Future: Send + 'static,
    {
        let name = std::any::type_name::<F>();
        if name.ends_with("{{closure}}") {
            panic!("closures are not allowed as nodes")
        }
        self.add_node_inner::<_, Current, Output, _>(
            &name,
            service_fn::<F, Args, Ctx, FnArgs>(node_fn),
        )
    }
    /// Add a node to the DAG
    fn add_node_inner<S, Current, Output, E: Into<BoxDynError>>(
        mut self,
        name: &str,
        service: S,
    ) -> Self
    where
        S: Service<Request<Current, Ctx>, Response = GoTo<Output>, Error = E> + Send + 'static,
        S::Future: Send + 'static,
        Output: Send + Sync + 'static,
        Codec: Encoder<Output, Compact = Compact> + Decoder<Current, Compact = Compact>,
        <Codec as Decoder<Current>>::Error: Debug,
        <Codec as Encoder<Output>>::Error: Debug,
    {
        let current = self.node_mapping.get_mut(name);

        match current {
            Some(exists) => {
                // Ensure it exists
                let _ = self.graph.node_weight_mut(*exists).unwrap();
            }
            None => {
                let dag_service = ServiceBuilder::new()
                    .map_request(|req: Request<Compact, Ctx>| {
                        Request::new_with_parts(
                            Codec::decode(&req.args).expect(&format!(
                                "Could not decode node, expecting {}",
                                std::any::type_name::<Current>()
                            )),
                            req.parts,
                        )
                    })
                    .map_response(|res: GoTo<Output>| match res {
                        GoTo::Next(results) => GoTo::Next(
                            Codec::encode(&results)
                                .expect(&format!("Could not encode output for node")),
                        ),
                        GoTo::Delay { next, delay } => GoTo::Delay {
                            next: Codec::encode(&next)
                                .expect(&format!("Could not encode output for node")),
                            delay,
                        },
                        GoTo::Done(output) => {
                            let encoded =
                                Codec::encode(&output).expect("Could not encode final output");
                            GoTo::Done(encoded)
                        }
                    })
                    .map_err(|e: E| e.into())
                    .service(service);

                let node_id = self.graph.add_node(BoxedService::new(dag_service));
                self.node_mapping.insert(name.to_owned(), node_id);
            }
        }

        self
    }

    /// Add an edge between two nodes
    pub fn add_edge<F1, F2>(&mut self, from: F1, to: F2) -> Result<(), String> {
        let from = std::any::type_name_of_val(&from);
        let from_node = self
            .node_mapping
            .get(from)
            .ok_or_else(|| format!("Node '{}' not found", from))?;
        let to = std::any::type_name_of_val(&to);
        let to_node = self
            .node_mapping
            .get(to)
            .ok_or_else(|| format!("Node '{}' not found", to))?;

        self.graph.add_edge(*from_node, *to_node, ());
        Ok(())
    }

    fn add_edge_inner(&mut self, from: &str, to: &str) -> Result<(), String> {
        let from_node = self
            .node_mapping
            .get(from)
            .ok_or_else(|| format!("Node '{}' not found", from))?;
        let to_node = self
            .node_mapping
            .get(to)
            .ok_or_else(|| format!("Node '{}' not found", to))?;

        self.graph.add_edge(*from_node, *to_node, ());
        Ok(())
    }

    /// Build the DAG executor
    pub fn build(self) -> Result<DagExecutor<Compact, Ctx>, String> {
        // Validate DAG (check for cycles)
        let sorted = toposort(&self.graph, None).map_err(|_| "DAG contains cycles")?;

        Ok(DagExecutor {
            graph: self.graph,
            node_mapping: self.node_mapping,
            topological_order: sorted,
        })
    }
}

/// Executor for DAG workflows
pub struct DagExecutor<Compact, Ctx> {
    graph: DiGraph<DagService<Compact, Ctx>, ()>,
    node_mapping: HashMap<String, NodeIndex>,
    topological_order: Vec<NodeIndex>,
}

impl<Compact, Ctx> DagExecutor<Compact, Ctx> {
    /// Get the execution order of nodes
    pub fn get_execution_order(&self) -> &[NodeIndex] {
        &self.topological_order
    }

    /// Get node by name
    pub fn get_node_id(&self, name: &str) -> Option<NodeIndex> {
        self.node_mapping.get(name).copied()
    }
}

impl<Ctx, Compact, Cdc, B, M>
    WorkerFactory<DagRequest<Compact>, Ctx, RouteService<DagRequest<Compact>, Ctx>, B, M>
    for DagExecutor<Compact, Ctx>
where
    B: Backend<DagRequest<Compact>, Ctx>,
    B::Sink: TaskSink<DagRequest<Compact>, Codec = Cdc, Compact = Compact> + 'static,
    M: Layer<RouteService<Compact, Ctx>>,
    Cdc: Decoder<DagRequest<Compact>, Compact = Compact>
        + Encoder<DagRequest<Compact>, Compact = Compact>,
    <Cdc as Encoder<DagRequest<Compact>>>::Error: Debug,
    <Cdc as Decoder<DagRequest<Compact>>>::Error: Debug,
    Ctx: 'static,
    Compact: 'static + Send + Clone,
{
    fn service(self, backend: &B) -> RouteService<DagRequest<Compact>, Ctx> {
        let sink = backend.sink();
        let svc = BoxService::new(RoutedDagService {
            inner: self,
            sink: Arc::new(Mutex::new(sink)),
        });
        svc
    }
}

pub struct RoutedDagService<Inner, SinkT> {
    inner: Inner,
    sink: Arc<Mutex<SinkT>>,
}

impl<Compact, Ctx, Cdc, SinkT> Service<Request<DagRequest<Compact>, Ctx>>
    for RoutedDagService<DagExecutor<Compact, Ctx>, SinkT>
where
    Compact: Send + 'static + Clone,
    Cdc: Decoder<DagRequest<Compact>, Compact = Compact>
        + Encoder<DagRequest<Compact>, Compact = Compact>,
    <Cdc as Encoder<DagRequest<Compact>>>::Error: Debug,
    <Cdc as Decoder<DagRequest<Compact>>>::Error: Debug,
    SinkT: TaskSink<DagRequest<Compact>, Compact = Compact, Codec = Cdc> + 'static,
{
    type Response = ();
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<(), BoxDynError>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<DagRequest<Compact>, Ctx>) -> Self::Future {
        let mut inputs = req.args.inputs.clone();
        let mut execution_context = req.args.execution_context.clone();
        let index = req.args.node_id;
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
            panic!("Missing some predecessors")
        }
        let targets: HashSet<_> = self
            .inner
            .graph
            .edges(index)
            .into_iter()
            .map(|s| s.target())
            .collect();
        let svc = self.inner.graph.node_weight_mut(index).unwrap();

        let sink = self.sink.clone();
        let fut = svc
            .call(req.map(|mut r| r.inputs.remove(&index).unwrap()))
            .and_then(move |res| async move {
                let mut sink = sink.lock().await;
                match res {
                    GoTo::Next(res) => {
                        execution_context.completed_nodes.insert(index);
                        for node_index in targets {
                            inputs.insert(node_index, res.clone());
                            let req = Request::new(DagRequest::new(
                                node_index,
                                inputs.clone(),
                                execution_context.clone(),
                            ))
                            .map(|req| Cdc::encode(&req).unwrap());
                            let _res = sink.push_raw_request(req).await;
                        }

                        Ok(())
                    }
                    GoTo::Delay { next, delay } => todo!(),
                    GoTo::Done(_) => Ok(()),
                }
            });

        fut.boxed()
    }
}

pub trait AddServiceFlow<EntryFn, NextFn, Entry, Next, Compact, Ctx>: Sized {
    fn add_service_flow(self, entry: EntryFn, next: NextFn) -> Self;
}

pub trait NamedService {
    fn named_service(&self) -> &str;
}

impl<F, Args, Ctx, FnArgs> NamedService for ServiceFn<F, Args, Ctx, FnArgs> {
    fn named_service(&self) -> &str {
        std::any::type_name::<F>()
    }
}

impl<Entry, Next, Output, Compact, Ctx, Codec, EntrySvc, NextSvc>
    AddServiceFlow<EntrySvc, NextSvc, Entry, Next, Compact, Ctx> for DagBuilder<Compact, Ctx, Codec>
where
    Codec: Decoder<Entry, Compact = Compact>
        + Encoder<Next, Compact = Compact>
        + Decoder<Next, Compact = Compact>
        + Encoder<Output, Compact = Compact>
        + 'static,
    Compact: Debug,
    EntrySvc: NamedService + Service<Request<Entry, Ctx>, Response = GoTo<Next>> + Send + 'static,
    Next: Send + Sync + 'static,
    <Codec as Decoder<Entry>>::Error: Debug,
    <Codec as Decoder<Next>>::Error: Debug,
    <Codec as Encoder<Next>>::Error: Debug,
    <Codec as Encoder<Output>>::Error: Debug,
    Output: Send + Sync + 'static,
    EntrySvc::Future: Send + 'static,
    EntrySvc::Error: Into<BoxDynError>,
    NextSvc: NamedService + Send + Service<Request<Next, Ctx>, Response = GoTo<Output>> + 'static,
    NextSvc::Future: Send + 'static,
    NextSvc::Error: Into<BoxDynError>,
{
    fn add_service_flow(self, entry: EntrySvc, next: NextSvc) -> Self {
        let entry_name = entry.named_service().to_owned();
        let next_name = next.named_service().to_owned();
        let mut builder = self
            .add_node_inner::<EntrySvc, Entry, Next, _>(&entry_name, entry)
            .add_node_inner::<NextSvc, Next, Output, _>(&next_name, next);
        builder.add_edge_inner(&entry_name, &next_name).unwrap();
        builder
    }
}

pub trait AddFlow<EntryFn, NextFn, Ctx, Args1, FnArgs1, Args2, FnArgs2, Compact>: Sized {
    fn add_flow(self, entry: EntryFn, next: NextFn) -> Self;
}

pub trait AddSubFlow<EntryFn, NextFn, Ctx, Args, FnArgs, Compact, Output>: Sized {
    fn add_sub_flow(self, entry: EntryFn, next: DagExecutor<Compact, Ctx>) -> Self;
}

impl<E, N, Ctx, Args1, FnArgs1, Compact, Codec, Output, Err: Into<BoxDynError>>
    AddSubFlow<E, N, Ctx, Args1, FnArgs1, Compact, Output> for DagBuilder<Compact, Ctx, Codec>
where
    E: NamedService
        + Service<Request<N, Ctx>, Response = GoTo<Output>, Error = Err>
        + Send
        + 'static,
    Compact: Debug,
    E::Future: Send + 'static,
    Output: Send + Sync + 'static,
    Codec: Encoder<Output, Compact = Compact> + Decoder<N, Compact = Compact>,
    <Codec as Decoder<N>>::Error: Debug,
    <Codec as Encoder<Output>>::Error: Debug,
{
    fn add_sub_flow(self, entry: E, next: DagExecutor<Compact, Ctx>) -> Self {
        let entry_name = entry.named_service().to_owned();
        let (nodes, edges) = next.graph.into_nodes_edges();
        let builder = self.add_node_inner(&entry_name, entry);
        let node_mappings = next.node_mapping;
        builder
    }
}

impl<E, N, Ctx, Args1, FnArgs1, Args2, FnArgs2, Compact, Codec>
    AddFlow<E, N, Ctx, Args1, FnArgs1, Args2, FnArgs2, Compact> for DagBuilder<Compact, Ctx, Codec>
where
    DagBuilder<Compact, Ctx, Codec>: AddServiceFlow<
        ServiceFn<E, Args1, Ctx, FnArgs1>,
        ServiceFn<N, Args2, Ctx, FnArgs2>,
        Args1,
        Args2,
        Compact,
        Ctx,
    >,
{
    fn add_flow(self, entry: E, next: N) -> Self {
        self.add_service_flow(service_fn(entry), service_fn(next))
    }
}

pub trait DagSinkExt<Compact, Ctx, T> {
    type Error;

    // fn push_inputs(
    //     &mut self,
    //     index: NodeIndex,
    //     value: Input,
    // ) -> impl Future<Output = Result<Parts<Ctx>, Self::Error>>;

    fn start_flow(&mut self, value: &T) -> impl Future<Output = Result<Parts<Ctx>, Self::Error>>;
}

impl<S, Compact, Ctx, Err, T> DagSinkExt<Compact, Ctx, T> for S
where
    S: TaskSink<DagRequest<Compact>, Compact = Compact, Context = Ctx, Error = Err>,
    Err: std::error::Error,
    S::Codec: Encoder<DagRequest<Compact>, Compact = Compact> + Encoder<T, Compact = Compact>,
    Ctx: Default,
    <<S as TaskSink<DagRequest<Compact>>>::Codec as Encoder<T>>::Error: Debug,
{
    type Error = Err;

    async fn start_flow(&mut self, input: &T) -> Result<Parts<Ctx>, Self::Error> {
        let mut inputs = HashMap::new();
        inputs.insert(NodeIndex::new(0), S::Codec::encode(input).unwrap());
        self.push(DagRequest::new(
            NodeIndex::new(0),
            inputs,
            Default::default(),
        ))
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use futures::StreamExt;
    use tower::{
        layer::util::{Identity, Stack},
        util::BoxService,
    };

    use crate::{
        backend::{
            codec::json::JsonCodec,
            memory::{JsonMemory, MemoryStorage},
        },
        // service_fn::service_fn,
        worker::{
            context::WorkerContext,
            event::Event,
            ext::event_listener::{EventListenerExt, EventListenerLayer},
        },
    };

    use super::*;
    use serde_json::Value;

    #[tokio::test]
    async fn test_dag_execution() {
        use tower::service_fn;
        // Create a simple DAG: A -> B -> C
        let mut builder = DagBuilder::<Value, ()>::new();

        builder = builder.add_node_inner(
            "node_a",
            service_fn(|req: Request<String, ()>| async move {
                dbg!(&req);
                Ok::<_, BoxDynError>(GoTo::Next(req.args.to_uppercase()))
            }),
        );

        builder = builder.add_node_inner(
            "node_b",
            service_fn(|req: Request<String, ()>| async move {
                dbg!(&req);
                Ok::<_, BoxDynError>(GoTo::Next(req.args.to_lowercase()))
            }),
        );

        builder = builder.add_node_inner(
            "node_c",
            service_fn(|_req: Request<String, ()>| async move {
                dbg!(_req);
                Ok::<_, BoxDynError>(GoTo::Done("final_result".to_string()))
            }),
        );

        builder.add_edge_inner("node_a", "node_b").unwrap();
        builder.add_edge_inner("node_b", "node_c").unwrap();

        let mut executor = builder.build().unwrap();

        // let initial_inputs = [(
        //     "node_a".to_string(),
        //     JsonCodec::<Value>::encode(&"initial_input".to_owned()).unwrap(),
        // )]
        // .into();
        // let result = executor.execute(initial_inputs).await.unwrap();

        // match result {
        //     GoTo::Done(final_result) => {
        //         assert_eq!(final_result, "\"final_result\"");
        //     }
        //     _ => panic!("Expected Done result"),
        // }
    }

    #[tokio::test]
    async fn it_works() {
        #[derive(Debug, Serialize, Deserialize, Default)]
        struct Order {
            id: u64,
            items: Vec<String>,
            payment_valid: bool,
            in_stock: bool,
            tracking_id: Option<String>,
        }
        async fn validate_payment(order: Order) -> Result<GoTo<Order>, io::Error> {
            println!("Validating payment for order {}", order.id);
            if order.payment_valid {
                Ok(GoTo::Next(order))
            } else {
                Err(io::Error::new(io::ErrorKind::Other, "Payment Rejected"))
            }
        }

        async fn process_payment(mut order: Order) -> Result<GoTo<Order>, io::Error> {
            println!("Processing payment for order {}", order.id);
            // Payment logic...
            Ok(GoTo::Next(order))
        }

        async fn confirm_payment(order: Order) -> Result<GoTo<Order>, io::Error> {
            println!("Payment confirmed for order {}", order.id);
            Ok(GoTo::Next(order))
        }

        async fn check_stock(mut order: Order) -> Result<GoTo<Order>, io::Error> {
            println!("Checking stock for order {}", order.id);
            if order.in_stock {
                Ok(GoTo::Next(order))
            } else {
                Err(io::Error::new(io::ErrorKind::Other, "Out of stock"))
            }
        }

        async fn reserve_items(order: Order) -> Result<GoTo<Order>, io::Error> {
            println!("Reserving items for order {}", order.id);
            Ok(GoTo::Next(order))
        }

        async fn update_inventory(order: Order) -> Result<GoTo<Order>, io::Error> {
            println!("Updating inventory for order {}", order.id);
            Ok(GoTo::Next(order))
        }

        async fn create_label(mut order: Order) -> Result<GoTo<Order>, io::Error> {
            println!("Creating shipping label for order {}", order.id);
            order.tracking_id = Some(format!("TRK{}", order.id));
            Ok(GoTo::Next(order))
        }

        async fn assign_carrier(order: Order) -> Result<GoTo<Order>, io::Error> {
            println!("Assigning carrier for order {}", order.id);
            Ok(GoTo::Next(order))
        }

        async fn schedule_pickup(order: Order) -> Result<GoTo<Order>, io::Error> {
            println!("Scheduling pickup for order {}", order.id);
            Ok(GoTo::Next(order))
        }

        async fn log_event(order: Order) -> Result<GoTo<Order>, io::Error> {
            println!("Logging event for order {}", order.id);
            Ok(GoTo::Next(order))
        }

        async fn notify_customer(order: Order, ctx: WorkerContext) -> Result<GoTo<()>, io::Error> {
            println!("Notifying customer for order {}", order.id);
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(2)).await;
                ctx.stop().unwrap();
            });
            Ok(GoTo::Done(()))
        }

        let process_payment: DagExecutor<Value, ()> = DagBuilder::new()
            // Payment Flow
            .add_flow(validate_payment, process_payment)
            .add_flow(process_payment, confirm_payment)
            .build()
            .unwrap();

        let dag = DagBuilder::new()
            // Payment Flow
            // .add_sub_flow(service_fn(validate_payment), process_payment)
            // Inventory Flow
            .add_flow(confirm_payment, check_stock)
            .add_flow(check_stock, reserve_items)
            .add_flow(reserve_items, update_inventory)
            // Shipping Flow
            .add_flow(update_inventory, create_label)
            .add_flow(create_label, assign_carrier)
            .add_flow(assign_carrier, schedule_pickup)
            // Logging & Notification Flow
            .add_flow(schedule_pickup, log_event)
            .add_flow(log_event, notify_customer)
            .build()
            .unwrap();

        let in_memory = MemoryStorage::new_with_json();
        let mut sink = in_memory.sink();
        let _p = sink
            .start_flow(&Order {
                payment_valid: true,
                in_stock: true,
                ..Default::default()
            })
            .await
            .unwrap();

        fn check_dag<
            W: WorkerFactory<
                DagRequest<Value>,
                (),
                RouteService<DagRequest<Value>, ()>,
                MemoryStorage<JsonMemory<DagRequest<Value>>>,
                Stack<EventListenerLayer, Identity>,
            >,
        >(
            dag: &W,
        ) {
        }

        check_dag(&dag);

        let worker = WorkerBuilder::new("rango-tango")
            .backend(in_memory)
            .on_event(|ctx, ev| {
                println!("Worker {:?}, On Event = {:?}", ctx.name(), ev);
            })
            .build(dag);
        let mut event_stream = worker.stream();
        while let Some(Ok(ev)) = event_stream.next().await {
            println!("On Event = {:?}", ev);
            if matches!(ev, Event::Error(_)) {
                break;
            }
        }
    }

    #[tokio::test]
    async fn test_task_dag_execution() {
        // Create a simple DAG: A -> B -> C

        async fn task_1(_req: String) -> Result<GoTo<String>, BoxDynError> {
            dbg!(_req);
            Ok::<_, BoxDynError>(GoTo::Next("result_a".to_string()))
        }

        async fn task_2(_req: String) -> Result<GoTo<String>, BoxDynError> {
            dbg!(_req);
            Ok::<_, BoxDynError>(GoTo::Next("result_b".to_string()))
        }

        async fn task_3(_req: String) -> Result<GoTo<String>, BoxDynError> {
            dbg!(_req);
            Ok::<_, BoxDynError>(GoTo::Next("result_b".to_string()))
        }

        async fn final_task(_req: HashMap<NodeIndex, String>) -> Result<GoTo<String>, BoxDynError> {
            dbg!(_req);
            Ok::<_, BoxDynError>(GoTo::Done("final_result".to_string()))
        }

        let mut executor = DagBuilder::<Value, ()>::new()
            .add_flow(task_1, task_2)
            .add_flow(task_1, task_3)
            // .add_flow(task_2, final_task)
            // .add_flow(task_3, final_task)
            .build()
            .unwrap();

        // let initial_inputs = [(
        //     "node_a".to_string(),
        //     JsonCodec::<String>::encode(&"initial_input".to_owned()).unwrap(),
        // )]
        // .into();
        // let result = executor.execute(initial_inputs).await.unwrap();

        // match result {
        //     GoTo::Done(final_result) => {
        //         assert_eq!(final_result, "\"final_result\"");
        //     }
        //     _ => panic!("Expected Done result"),
        // }
    }
}
