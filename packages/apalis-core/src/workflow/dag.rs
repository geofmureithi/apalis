use std::{
    collections::HashMap,
    fmt::Debug,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::FutureExt;
use petgraph::{
    algo::toposort,
    graph::{DiGraph, NodeIndex},
    Direction,
};
use serde::{Deserialize, Serialize};
use tower::{Service, ServiceBuilder};

use crate::{
    backend::codec::{json::JsonCodec, Decoder, Encoder},
    error::BoxDynError,
    request::{task_id::TaskId, Request},
    service_fn::{service_fn, ServiceFn},
};

type BoxedService<Input, Output> = tower::util::BoxService<Input, Output, BoxDynError>;

type DagService<Compact, Ctx> = BoxedService<Request<DagRequest<Compact>, Ctx>, DagResult<Compact>>;

/// Control flow for DAG execution
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum DagResult<T = ()> {
    /// Continue to specified nodes
    Continue {
        /// Results to pass to next nodes
        results: HashMap<NodeIndex, T>,
    },
    /// Delay execution and continue to specified nodes
    Delay {
        /// Results to pass to next nodes after delay
        results: HashMap<NodeIndex, T>,
        /// The period to delay
        delay: Duration,
    },
    /// Complete execution with final result
    Done(T),
}

/// Request for DAG node execution
#[derive(Clone, Debug)]
pub struct DagRequest<T> {
    pub node_id: NodeIndex,
    pub inputs: HashMap<NodeIndex, T>,
    pub execution_context: ExecutionContext,
}

/// Context for tracking DAG execution state
#[derive(Clone, Debug)]
pub struct ExecutionContext {
    pub completed_nodes: std::collections::HashSet<NodeIndex>,
    pub pending_results: HashMap<NodeIndex, Vec<u8>>, // Serialized results
    pub execution_id: String,
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
pub struct DagBuilder<Input, Compact, Ctx, Codec = JsonCodec<String>> {
    graph: DiGraph<DagService<Compact, Ctx>, ()>,
    node_mapping: HashMap<String, NodeIndex>,
    input_type: PhantomData<Input>,
    compact_type: PhantomData<Compact>,
    codec: PhantomData<Codec>,
}

impl<Input, Compact, Ctx> Default for DagBuilder<Input, Compact, Ctx> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Input, Compact, Ctx> DagBuilder<Input, Compact, Ctx> {
    pub fn new() -> Self {
        Self {
            graph: DiGraph::new(),
            node_mapping: HashMap::new(),
            input_type: PhantomData,
            compact_type: PhantomData,
            codec: PhantomData,
        }
    }
}

impl<Input, Compact, Ctx, Codec> DagBuilder<Input, Compact, Ctx, Codec>
where
    Codec: Encoder<Input, Compact = Compact> + Decoder<Input, Compact = Compact> + Clone + 'static,
    <Codec as Decoder<Input>>::Error: Debug,
    <Codec as Encoder<Input>>::Error: Debug,
    Compact: Debug,
{
    pub fn add_node<F, Output, Args, FnArgs, E>(self, node_fn: F) -> Self
    where
        Output: Clone + Send + Sync + 'static,
        Codec: Encoder<Output, Compact = Compact> + Decoder<Output, Compact = Compact>,
        <Codec as Decoder<Output>>::Error: Debug,
        <Codec as Encoder<Output>>::Error: Debug,
        F: Send,
        Args: Send,
        Ctx: Send,
        FnArgs: Send,
        ServiceFn<F, Args, Ctx, FnArgs>: Service<Request<HashMap<NodeIndex, Input>, Ctx>, Response = DagResult<Output>, Error = E>
            + Copy
            + Send
            + 'static,
        E: Into<BoxDynError>,
        <ServiceFn<F, Args, Ctx, FnArgs> as Service<Request<HashMap<NodeIndex, Input>, Ctx>>>::Future: Send + 'static
    {
        let name = std::any::type_name::<F>().to_owned();
        if name.ends_with("{{closure}}") {
            panic!("closures are not allowed as nodes")
        }
        self.add_node_inner::<_, Output, _>(name, service_fn::<F, Args, Ctx, FnArgs>(node_fn))
    }
    /// Add a node to the DAG
    fn add_node_inner<S, Output, E: Into<BoxDynError>>(mut self, name: String, service: S) -> Self
    where
        S: Service<
                Request<HashMap<NodeIndex, Input>, Ctx>,
                Response = DagResult<Output>,
                Error = E,
            > + Send
            + 'static,
        S::Future: Send + 'static,
        Output: Clone + Send + Sync + 'static,
        Codec: Encoder<Output, Compact = Compact> + Decoder<Output, Compact = Compact>,
        <Codec as Decoder<Output>>::Error: Debug,
        <Codec as Encoder<Output>>::Error: Debug,
    {
        let dag_service = ServiceBuilder::new()
            .map_request(|req: Request<DagRequest<Compact>, Ctx>| {
                // dbg!(&req);
                let decoded_inputs = req
                    .args
                    .inputs
                    .into_iter()
                    .map(|(node_id, compact_data)| {
                        dbg!(&compact_data, std::any::type_name::<Input>());
                        let decoded = Codec::decode(&compact_data)
                            .expect(&format!("Could not decode input for node {:?}", node_id));
                        (node_id, decoded)
                    })
                    .collect();

                Request::new_with_parts(decoded_inputs, req.parts)
            })
            .map_response(|res: DagResult<Output>| match res {
                DagResult::Continue { results } => {
                    let encoded_results = results
                        .into_iter()
                        .map(|(node_id, output)| {
                            let encoded = Codec::encode(&output)
                                .expect(&format!("Could not encode output for node {:?}", node_id));
                            (node_id, encoded)
                        })
                        .collect();
                    DagResult::Continue {
                        results: encoded_results,
                    }
                }
                DagResult::Delay { results, delay } => {
                    let encoded_results = results
                        .into_iter()
                        .map(|(node_id, output)| {
                            let encoded = Codec::encode(&output)
                                .expect(&format!("Could not encode output for node {:?}", node_id));
                            (node_id, encoded)
                        })
                        .collect();
                    DagResult::Delay {
                        results: encoded_results,
                        delay,
                    }
                }
                DagResult::Done(output) => {
                    let encoded = Codec::encode(&output).expect("Could not encode final output");
                    DagResult::Done(encoded)
                }
            })
            .map_err(|e: E| e.into())
            .service(service);

        let node_id = self.graph.add_node(BoxedService::new(dag_service));
        self.node_mapping.insert(name, node_id);
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
pub struct DagExecutor<Compact, Ctx = ()> {
    graph: DiGraph<DagService<Compact, Ctx>, ()>,
    node_mapping: HashMap<String, NodeIndex>,
    topological_order: Vec<NodeIndex>,
}

impl<Compact> DagExecutor<Compact>
where
    Compact: Clone + Send + Sync + 'static,
{
    /// Execute the DAG starting from root nodes
    pub async fn execute(
        &mut self,
        initial_inputs: HashMap<String, Compact>,
    ) -> Result<DagResult<Compact>, BoxDynError> {
        let mut execution_context = ExecutionContext {
            completed_nodes: std::collections::HashSet::new(),
            pending_results: HashMap::new(),
            execution_id: TaskId::new().to_string(),
        };

        // Convert string keys to node indices
        let mut node_inputs: HashMap<NodeIndex, HashMap<NodeIndex, Compact>> = HashMap::new();

        // Initialize root nodes with initial inputs
        for (node_name, input_data) in initial_inputs {
            if let Some(&node_id) = self.node_mapping.get(&node_name) {
                node_inputs
                    .entry(node_id)
                    .or_default()
                    .insert(node_id, input_data);
            }
        }

        // Execute nodes in topological order
        for &node_id in &self.topological_order {
            // Check if this node has all required inputs
            let predecessors: Vec<NodeIndex> = self
                .graph
                .neighbors_directed(node_id, Direction::Incoming)
                .collect();

            // Skip if not all predecessors are completed
            if !predecessors
                .iter()
                .all(|&pred| execution_context.completed_nodes.contains(&pred))
            {
                continue;
            }

            // Get inputs for this node
            let inputs = node_inputs.remove(&node_id).unwrap_or_default();

            // Execute the node
            if let Some(service) = self.graph.node_weight_mut(node_id) {
                let request =
                    Request::new(DagRequest::new(node_id, inputs, execution_context.clone()));
                let result = service.call(request).await?;

                match result {
                    DagResult::Continue { results } => {
                        // Distribute results to successor nodes
                        for (target_node, result_data) in results {
                            node_inputs
                                .entry(target_node)
                                .or_default()
                                .insert(node_id, result_data);
                        }
                        execution_context.completed_nodes.insert(node_id);
                    }
                    DagResult::Delay { results, delay } => {
                        // Handle delay (in a real implementation, you might want to use a scheduler)
                        // tokio::time::sleep(delay).await;

                        // Distribute results to successor nodes
                        for (target_node, result_data) in results {
                            node_inputs
                                .entry(target_node)
                                .or_default()
                                .insert(node_id, result_data);
                        }
                        execution_context.completed_nodes.insert(node_id);
                    }
                    DagResult::Done(final_result) => {
                        return Ok(DagResult::Done(final_result));
                    }
                }
            }
        }

        // If we get here, execution completed without a Done result
        // Return the results from leaf nodes
        let leaf_nodes: Vec<NodeIndex> = self
            .graph
            .node_indices()
            .filter(|&node| {
                self.graph
                    .neighbors_directed(node, Direction::Outgoing)
                    .next()
                    .is_none()
            })
            .collect();
        dbg!(&leaf_nodes);
        if let Some(&leaf_node) = leaf_nodes.first() {
            dbg!(&leaf_node);
            // Return results from the first leaf node (you might want to handle this differently)
            Ok(DagResult::Continue {
                results: HashMap::new(),
            })
        } else {
            Ok(DagResult::Continue {
                results: HashMap::new(),
            })
        }
    }

    /// Get the execution order of nodes
    pub fn get_execution_order(&self) -> &[NodeIndex] {
        &self.topological_order
    }

    /// Get node by name
    pub fn get_node_id(&self, name: &str) -> Option<NodeIndex> {
        self.node_mapping.get(name).copied()
    }
}

impl<Ctx, Compact: 'static> Service<Request<DagRequest<Compact>, Ctx>>
    for DagExecutor<Compact, Ctx>
{
    type Response = DagResult<Compact>;
    type Error = BoxDynError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<DagRequest<Compact>, Ctx>) -> Self::Future {
        let node_id = &req.args.node_id;
        let execution_context = &req.args.execution_context;
        let predecessors: Vec<NodeIndex> = self
            .graph
            .neighbors_directed(*node_id, Direction::Incoming)
            .collect();

        // Skip if not all predecessors are completed
        if !predecessors
            .iter()
            .all(|&pred| execution_context.completed_nodes.contains(&pred))
        {
            panic!("Missing some predecessors")
        }

        if let Some(service) = self.graph.node_weight_mut(*node_id) {
            return service.call(req).boxed();
        }

        unreachable!("not really unreachable")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::service_fn;

    #[tokio::test]
    async fn test_dag_execution() {
        // Create a simple DAG: A -> B -> C
        let mut builder = DagBuilder::<String, String, ()>::new();

        builder = builder.add_node_inner(
            "node_a".to_string(),
            service_fn(|_req: Request<HashMap<NodeIndex, String>, ()>| async {
                dbg!(_req);
                Ok::<_, BoxDynError>(DagResult::Continue {
                    results: [(NodeIndex::new(1), "result_a".to_string())].into(),
                })
            }),
        );

        builder = builder.add_node_inner(
            "node_b".to_string(),
            service_fn(|_req: Request<HashMap<NodeIndex, String>, ()>| async {
                dbg!(_req);
                Ok::<_, BoxDynError>(DagResult::Continue {
                    results: [(NodeIndex::new(2), "result_b".to_string())].into(),
                })
            }),
        );

        builder = builder.add_node_inner(
            "node_c".to_string(),
            service_fn(|_req: Request<HashMap<NodeIndex, String>, ()>| async {
                dbg!(_req);
                Ok::<_, BoxDynError>(DagResult::Done("final_result".to_string()))
            }),
        );

        builder.add_edge_inner("node_a", "node_b").unwrap();
        builder.add_edge_inner("node_b", "node_c").unwrap();

        let mut executor = builder.build().unwrap();

        let initial_inputs = [(
            "node_a".to_string(),
            JsonCodec::<String>::encode(&"initial_input".to_owned()).unwrap(),
        )]
        .into();
        let result = executor.execute(initial_inputs).await.unwrap();

        match result {
            DagResult::Done(final_result) => {
                assert_eq!(final_result, "\"final_result\"");
            }
            _ => panic!("Expected Done result"),
        }
    }

    #[tokio::test]
    async fn test_task_dag_execution() {
        // Create a simple DAG: A -> B -> C

        async fn task_1(
            _req: HashMap<NodeIndex, String>,
        ) -> Result<DagResult<String>, BoxDynError> {
            dbg!(_req);
            Ok::<_, BoxDynError>(DagResult::Continue {
                results: [(NodeIndex::new(1), "result_a".to_string())].into(),
            })
        }

        async fn task_2(
            _req: HashMap<NodeIndex, String>,
        ) -> Result<DagResult<String>, BoxDynError> {
            dbg!(_req);
            Ok::<_, BoxDynError>(DagResult::Continue {
                results: [(NodeIndex::new(2), "result_b".to_string())].into(),
            })
        }

        async fn task_3(
            _req: HashMap<NodeIndex, String>,
        ) -> Result<DagResult<String>, BoxDynError> {
            dbg!(_req);
            Ok::<_, BoxDynError>(DagResult::Continue {
                results: [(NodeIndex::new(3), "result_c".to_string())].into(),
            })
        }

        async fn final_task(
            _req: HashMap<NodeIndex, String>,
        ) -> Result<DagResult<String>, BoxDynError> {
            dbg!(_req);
            Ok::<_, BoxDynError>(DagResult::Done("final_result".to_string()))
        }

        let mut builder = DagBuilder::<String, String, ()>::new()
            .add_node(task_1)
            .add_node(task_2)
            .add_node(task_3)
            .add_node(final_task);

        builder.add_edge(task_1, task_2).unwrap();
        builder.add_edge(task_1, task_3).unwrap();
        builder.add_edge(task_2, final_task).unwrap();
        builder.add_edge(task_3, final_task).unwrap();

        let mut executor = builder.build().unwrap();

        let initial_inputs = [(
            "node_a".to_string(),
            JsonCodec::<String>::encode(&"initial_input".to_owned()).unwrap(),
        )]
        .into();
        let result = executor.execute(initial_inputs).await.unwrap();

        match result {
            DagResult::Done(final_result) => {
                assert_eq!(final_result, "\"final_result\"");
            }
            _ => panic!("Expected Done result"),
        }
    }
    // Approach 2: Parallel Execution with Selective Results
    #[tokio::test]
    async fn test_parallel_selective_workflow() {
        async fn dispatcher(
            req: HashMap<NodeIndex, String>,
        ) -> Result<DagResult<String>, BoxDynError> {
            dbg!("Dispatcher executing", &req);
            let input = req.values().next().unwrap();

            // Send to multiple nodes but they'll decide whether to process
            let mut results = HashMap::new();
            results.insert(NodeIndex::new(1), input.clone()); // to task_2
            results.insert(NodeIndex::new(2), input.clone()); // to task_3

            Ok(DagResult::Continue { results })
        }

        async fn task_2_conditional(
            req: HashMap<NodeIndex, String>,
        ) -> Result<DagResult<String>, BoxDynError> {
            dbg!("Task_2 evaluating", &req);
            let input = req.values().next().unwrap();

            if input.contains("execute_task2") {
                dbg!("Task_2 executing");
                Ok(DagResult::Continue {
                    results: [(NodeIndex::new(3), "processed_by_task2".to_string())].into(),
                })
            } else {
                dbg!("Task_2 skipping");
                // Return empty results to effectively skip
                Ok(DagResult::Continue {
                    results: HashMap::new(),
                })
            }
        }

        async fn task_3_conditional(
            req: HashMap<NodeIndex, String>,
        ) -> Result<DagResult<String>, BoxDynError> {
            dbg!("Task_3 evaluating", &req);
            let input = req.values().next().unwrap();

            if input.contains("execute_task3") {
                dbg!("Task_3 executing");
                Ok(DagResult::Continue {
                    results: [(NodeIndex::new(3), "processed_by_task3".to_string())].into(),
                })
            } else {
                dbg!("Task_3 skipping");
                Ok(DagResult::Continue {
                    results: HashMap::new(),
                })
            }
        }

        async fn collector(
            req: HashMap<NodeIndex, String>,
        ) -> Result<DagResult<String>, BoxDynError> {
            dbg!("Collector executing", &req);
            let results: Vec<String> = req.values().cloned().collect();
            let combined_result = if results.is_empty() {
                "no_processing_occurred".to_string()
            } else {
                results.join(" + ")
            };
            Ok(DagResult::Done(combined_result))
        }

        let mut builder = DagBuilder::<String, String, ()>::new()
            .add_node(dispatcher)
            .add_node(task_2_conditional)
            .add_node(task_3_conditional)
            .add_node(collector);

        builder.add_edge(dispatcher, task_2_conditional).unwrap();
        builder.add_edge(dispatcher, task_3_conditional).unwrap();
        builder.add_edge(task_2_conditional, collector).unwrap();
        builder.add_edge(task_3_conditional, collector).unwrap();

        let mut executor = builder.build().unwrap();

        // Test with task_2 execution
        let dispatcher_name = std::any::type_name_of_val(&dispatcher);
        let initial_inputs = [(dispatcher_name.to_string(), JsonCodec::<String>::encode(&"execute_task2".to_owned()).unwrap(),)].into();

        let result = executor.execute(initial_inputs).await.unwrap();

        match result {
            DagResult::Done(final_result) => {
                assert!(final_result.contains("task2"));
                println!("Task 2 was executed: {}", final_result);
            }
            _ => panic!("Expected Done result"),
        }
    }
}
