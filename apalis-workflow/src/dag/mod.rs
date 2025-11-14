use std::{collections::HashMap, marker::PhantomData, sync::Mutex};

use apalis_core::{
    error::BoxDynError,
    task::Task,
    task_fn::{TaskFn, task_fn},
};
use petgraph::{
    Direction,
    algo::toposort,
    dot::Config,
    graph::{DiGraph, EdgeIndex, NodeIndex},
};
use tower::{Service, ServiceBuilder};

use crate::{BoxedService, SteppedService};

/// Directed Acyclic Graph (DAG) workflow builder
#[derive(Debug)]
pub struct DagFlow<Input = (), Output = ()> {
    graph: Mutex<DiGraph<SteppedService<(), (), ()>, ()>>,
    node_mapping: Mutex<HashMap<String, NodeIndex>>,
    _marker: PhantomData<(Input, Output)>,
}

impl DagFlow {
    /// Create a new DAG workflow builder
    pub fn new() -> Self {
        Self {
            graph: Mutex::new(DiGraph::new()),
            node_mapping: Mutex::new(HashMap::new()),
            _marker: PhantomData,
        }
    }

    /// Add a node to the DAG
    #[must_use]
    pub fn add_node<S, Input>(&self, name: &str, service: S) -> NodeBuilder<'_, Input, S::Response>
    where
        S: Service<Task<Input, (), ()>> + Send + 'static,
        S::Future: Send + 'static,
    {
        let svc = ServiceBuilder::new()
            .map_request(|r: Task<(), (), ()>| todo!())
            .map_response(|r: S::Response| todo!())
            .map_err(|_e: S::Error| {
                let boxed: BoxDynError = todo!();
                boxed
            })
            .service(service);
        let node = self.graph.lock().unwrap().add_node(BoxedService::new(svc));
        self.node_mapping
            .lock()
            .unwrap()
            .insert(name.to_string(), node);
        NodeBuilder {
            id: node,
            dag: self,
            io: PhantomData,
        }
    }

    /// Add a task function node to the DAG
    pub fn node<F, Input, O, FnArgs>(&self, node: F) -> NodeBuilder<'_, Input, O>
    where
        TaskFn<F, Input, (), FnArgs>: Service<Task<Input, (), ()>, Response = O>,
        F: Send + 'static,
        Input: Send + 'static,
        FnArgs: Send + 'static,
        <TaskFn<F, Input, (), FnArgs> as Service<Task<Input, (), ()>>>::Future: Send + 'static,
    {
        self.add_node(std::any::type_name::<F>(), task_fn(node))
    }

    /// Add a routing node to the DAG
    pub fn route<F, Input, O, FnArgs>(&self, router: F) -> NodeBuilder<'_, Input, O>
    where
        TaskFn<F, Input, (), FnArgs>: Service<Task<Input, (), ()>, Response = O>,
        F: Send + 'static,
        Input: Send + 'static,
        FnArgs: Send + 'static,
        <TaskFn<F, Input, (), FnArgs> as Service<Task<Input, (), ()>>>::Future: Send + 'static,
        O: Into<NodeIndex>,
    {
        self.add_node::<TaskFn<F, Input, (), FnArgs>, Input>(
            std::any::type_name::<F>(),
            task_fn(router),
        )
    }

    /// Build the DAG executor
    pub fn build(self) -> Result<DagExecutor, String> {
        // Validate DAG (check for cycles)
        let sorted =
            toposort(&*self.graph.lock().unwrap(), None).map_err(|_| "DAG contains cycles")?;

        fn find_edge_nodes<N, E>(graph: &DiGraph<N, E>, direction: Direction) -> Vec<NodeIndex> {
            graph
                .node_indices()
                .filter(|&n| graph.neighbors_directed(n, direction).count() == 0)
                .collect()
        }

        let graph = self.graph.into_inner().unwrap();

        Ok(DagExecutor {
            start_nodes: find_edge_nodes(&graph, Direction::Incoming),
            end_nodes: find_edge_nodes(&graph, Direction::Outgoing),
            graph,
            node_mapping: self.node_mapping.into_inner().unwrap(),
            topological_order: sorted,
        })
    }
}

/// Executor for DAG workflows
#[derive(Debug)]
pub struct DagExecutor<Ctx = (), IdType = ()> {
    graph: DiGraph<SteppedService<(), Ctx, IdType>, ()>,
    node_mapping: HashMap<String, NodeIndex>,
    topological_order: Vec<NodeIndex>,
    start_nodes: Vec<NodeIndex>,
    end_nodes: Vec<NodeIndex>,
}

impl DagExecutor {
    /// Get a node by name
    pub fn get_node_by_name_mut(&mut self, name: &str) -> Option<&mut SteppedService<(), (), ()>> {
        self.node_mapping
            .get(name)
            .and_then(|&idx| self.graph.node_weight_mut(idx))
    }

    /// Export the DAG to DOT format
    pub fn to_dot(&self) -> String {
        let names = self
            .node_mapping
            .iter()
            .map(|(name, &idx)| (idx, name.clone()))
            .collect::<HashMap<_, _>>();
        let get_node_attributes = |_, (index, _)| {
            format!(
                "label=\"{}\"",
                names.get(&index).cloned().unwrap_or_default()
            )
        };
        let dot = petgraph::dot::Dot::with_attr_getters(
            &self.graph,
            &[Config::NodeNoLabel, Config::EdgeNoLabel],
            &|_, _| String::new(),
            &get_node_attributes,
        );
        format!("{:?}", dot)
    }
}

/// Builder for a node in the DAG
#[derive(Clone, Debug)]
pub struct NodeBuilder<'a, Input, Output = ()> {
    pub(crate) id: NodeIndex,
    pub(crate) dag: &'a DagFlow,
    pub(crate) io: PhantomData<(Input, Output)>,
}

impl<Input, Output> NodeBuilder<'_, Input, Output> {
    /// Specify dependencies for this node
    pub fn depends_on<'a, D>(self, deps: D) -> NodeHandle<Input, Output>
    where
        D: DepsCheck<Input>,
    {
        let mut edges = Vec::new();
        for dep in deps.to_node_ids() {
            edges.push(self.dag.graph.lock().unwrap().add_edge(dep, self.id, ()));
        }
        NodeHandle {
            id: self.id,
            edges,
            _phantom: PhantomData,
        }
    }
}

/// Handle for a node in the DAG
#[derive(Clone, Debug)]
pub struct NodeHandle<Input, Output = ()> {
    pub(crate) id: NodeIndex,
    pub(crate) edges: Vec<EdgeIndex>,
    pub(crate) _phantom: PhantomData<(Input, Output)>,
}

/// Trait for converting dependencies into node IDs
pub trait DepsCheck<Input> {
    /// Convert dependencies to node IDs
    fn to_node_ids(&self) -> Vec<NodeIndex>;
}

impl DepsCheck<()> for () {
    fn to_node_ids(&self) -> Vec<NodeIndex> {
        Vec::new()
    }
}

impl<'a, Input, Output> DepsCheck<Output> for &NodeBuilder<'a, Input, Output> {
    fn to_node_ids(&self) -> Vec<NodeIndex> {
        vec![self.id]
    }
}

impl<Input, Output> DepsCheck<Output> for &NodeHandle<Input, Output> {
    fn to_node_ids(&self) -> Vec<NodeIndex> {
        vec![self.id]
    }
}

impl<Input, Output> DepsCheck<Output> for (&NodeHandle<Input, Output>,) {
    fn to_node_ids(&self) -> Vec<NodeIndex> {
        vec![self.0.id]
    }
}

impl<'a, Input, Output> DepsCheck<Output> for (&NodeBuilder<'a, Input, Output>,) {
    fn to_node_ids(&self) -> Vec<NodeIndex> {
        vec![self.0.id]
    }
}

impl<'a, Output, T: DepsCheck<Output>> DepsCheck<Vec<Output>> for Vec<T> {
    fn to_node_ids(&self) -> Vec<NodeIndex> {
        self.iter().flat_map(|item| item.to_node_ids()).collect()
    }
}

macro_rules! impl_deps_check {
    ($( $len:literal => ( $( $in:ident $out:ident $idx:tt ),+ ) ),+ $(,)?) => {
        $(
            impl<'a, $( $in, )+ $( $out, )+> DepsCheck<( $( $out, )+ )>
                for ( $( &NodeBuilder<'a, $in, $out>, )+ )
            {
                fn to_node_ids(&self) -> Vec<NodeIndex> {
                    vec![ $( self.$idx.id ),+ ]
                }
            }

            impl<$( $in, )+ $( $out, )+> DepsCheck<( $( $out, )+ )>
                for ( $( &NodeHandle<$in, $out>, )+ )
            {
                fn to_node_ids(&self) -> Vec<NodeIndex> {
                    vec![ $( self.$idx.id ),+ ]
                }
            }
        )+
    };
}

impl_deps_check! {
    1 => (Input1 Output1 0),
    2 => (Input1 Output1 0, Input2 Output2 1),
    3 => (Input1 Output1 0, Input2 Output2 1, Input3 Output3 2),
    4 => (Input1 Output1 0, Input2 Output2 1, Input3 Output3 2, Input4 Output4 3),
    5 => (Input1 Output1 0, Input2 Output2 1, Input3 Output3 2, Input4 Output4 3, Input5 Output5 4),
    6 => (Input1 Output1 0, Input2 Output2 1, Input3 Output3 2, Input4 Output4 3, Input5 Output5 4, Input6 Output6 5),
    7 => (Input1 Output1 0, Input2 Output2 1, Input3 Output3 2, Input4 Output4 3, Input5 Output5 4, Input6 Output6 5, Input7 Output7 6),
    8 => (Input1 Output1 0, Input2 Output2 1, Input3 Output3 2, Input4 Output4 3, Input5 Output5 4, Input6 Output6 5, Input7 Output7 6, Input8 Output8 7),
}

mod tests {
    use std::{
        collections::HashMap, marker::PhantomData, num::ParseIntError, ops::Range, time::Duration,
    };

    use apalis_core::{
        backend::json::JsonStorage, error::BoxDynError, task::Task, task_fn::task_fn,
        worker::context::WorkerContext,
    };
    use petgraph::graph::NodeIndex;
    use serde_json::Value;

    use crate::{step::Identity, workflow::Workflow};

    use super::*;

    #[test]
    fn test_basic_workflow() {
        let dag = DagFlow::new();

        let entry1 = dag.add_node("entry1", task_fn(|task: u32| async move { task as usize }));
        let entry2 = dag.add_node("entry2", task_fn(|task: u32| async move { task as usize }));
        let entry3 = dag.add_node("entry3", task_fn(|task: u32| async move { task as usize }));

        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        enum EntryRoute {
            Entry1(NodeIndex),
            Entry2(NodeIndex),
            Entry3(NodeIndex),
        }

        impl Into<NodeIndex> for EntryRoute {
            fn into(self) -> NodeIndex {
                match self {
                    EntryRoute::Entry1(idx) => idx,
                    EntryRoute::Entry2(idx) => idx,
                    EntryRoute::Entry3(idx) => idx,
                }
            }
        }

        impl DepsCheck<usize> for EntryRoute {
            fn to_node_ids(&self) -> Vec<NodeIndex> {
                vec![(*self).into()]
            }
        }

        async fn collect(task: (usize, usize, usize)) -> usize {
            task.0 + task.1 + task.2
        }
        let collector = dag.node(collect).depends_on((&entry1, &entry2, &entry3));

        async fn vec_collect(task: Vec<usize>, worker: WorkerContext) -> usize {
            task.iter().sum::<usize>()
        }

        let vec_collector = dag
            .node(vec_collect)
            .depends_on(vec![&entry1, &entry2, &entry3]);

        async fn exit(task: (usize, usize)) -> Result<u32, ParseIntError> {
            (task.0.to_string() + &task.1.to_string()).parse()
        }

        let on_collect = dag.node(exit).depends_on((&collector, &vec_collector));

        async fn check_approval(task: u32) -> Result<EntryRoute, BoxDynError> {
            match task % 3 {
                0 => Ok(EntryRoute::Entry1(NodeIndex::new(0))),
                1 => Ok(EntryRoute::Entry2(NodeIndex::new(1))),
                2 => Ok(EntryRoute::Entry3(NodeIndex::new(2))),
                _ => Err(BoxDynError::from("Invalid task")),
            }
        }

        dag.route(check_approval).depends_on(&on_collect);

        let dag_executor = dag.build().unwrap();
        assert_eq!(dag_executor.topological_order.len(), 7);

        println!("Start nodes: {:?}", dag_executor.start_nodes);
        println!("End nodes: {:?}", dag_executor.end_nodes);

        println!(
            "DAG Topological Order: {:?}",
            dag_executor.topological_order
        );

        println!("DAG in DOT format:\n{}", dag_executor.to_dot());

        // let inner_basic: Workflow<u32, usize, JsonStorage<Value>, _> = Workflow::new("basic")
        //     .and_then(async |input: u32| (input + 1) as usize)
        //     .and_then(async |input: usize| input.to_string())
        //     .and_then(async |input: String| input.parse::<usize>());

        // let workflow = Workflow::new("example_workflow")
        //     .and_then(async |input: u32| Ok::<Range<u32>, BoxDynError>(input..100))
        //     .filter_map(
        //         async |input: u32| {
        //             if input > 50 { Some(input) } else { None }
        //         },
        //     )
        //     .and_then(async |items: Vec<u32>| Ok::<_, BoxDynError>(items))
        //     .fold(0, async |(acc, item): (u32, u32)| {
        //         Ok::<_, BoxDynError>(item + acc)
        //     })
        //     // .delay_for(Duration::from_secs(2))
        //     // .delay_with(|_| Duration::from_secs(1))
        //     // .and_then(async |items: Range<u32>| Ok::<_, BoxDynError>(items.sum::<u32>()))
        //     // .repeat_until(async |i: u32| {
        //     //     if i < 20 {
        //     //         Ok::<_, BoxDynError>(Some(i))
        //     //     } else {
        //     //         Ok(None)
        //     //     }
        //     // })
        //     // .chain(inner_basic)
        //     // .chain(
        //     //     Workflow::new("sub_workflow")
        //     //         .and_then(async |input: usize| Ok::<_, BoxDynError>(input as u32 * 2))
        //     //         .and_then(async |input: u32| Ok::<_, BoxDynError>(input + 10)),
        //     // )
        //     // .chain(dag_executor)
        //     .build();
    }
}
