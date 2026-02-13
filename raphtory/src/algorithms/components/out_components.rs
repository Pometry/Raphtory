use crate::{
    core::entities::VID,
    db::{
        api::{
            state::{ops::Const, Index, NodeState},
            view::{Filter, NodeViewOps, StaticGraphViewOps},
        },
        graph::{
            node::NodeView,
            nodes::Nodes,
            views::filter::{CreateFilter, Unfiltered},
        },
        task::{
            context::Context,
            node::eval_node::EvalNodeView,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use indexmap::IndexSet;
use itertools::Itertools;
use std::collections::{hash_map::Entry, HashMap, HashSet, VecDeque};

#[derive(Clone, Debug, Default)]
struct OutState {
    out_components: Vec<VID>,
}

/// Computes the out components of each node in the graph
///
/// # Arguments
///
/// - `g` - A reference to the graph
/// - `threads` - Number of threads to use
///
pub fn out_components<G>(g: &G, threads: Option<usize>) -> NodeState<'static, Nodes<'static, G>, G>
where
    G: StaticGraphViewOps,
{
    out_components_filtered(g, threads, Unfiltered).expect("Unfiltered should never fail")
}

/// Computes the out components of each node in the filtered graph
///
/// # Arguments
///
/// - `g` - A reference to the graph
/// - `threads` - Number of threads to use
/// - `filter` - Filter
///
pub fn out_components_filtered<G, F>(
    g: &G,
    threads: Option<usize>,
    filter: F,
) -> Result<NodeState<'static, Nodes<'static, G>, G>, GraphError>
where
    G: StaticGraphViewOps,
    F: CreateFilter + 'static,
    F::EntityFiltered<'static, F::FilteredGraph<'static, G>>: StaticGraphViewOps,
{
    let filtered = g.filter(filter)?;
    let ctx: Context<_, _> = (&filtered).into();

    let step1 = ATask::new(move |vv: &mut EvalNodeView<_, OutState>| {
        let mut out_components = HashSet::new();
        let mut to_check_stack = Vec::new();
        vv.out_neighbours().iter().for_each(|node| {
            let id = node.node;
            if out_components.insert(id) {
                to_check_stack.push(id);
            }
        });
        while let Some(neighbour_id) = to_check_stack.pop() {
            if let Some(neighbour) = vv.graph().node(neighbour_id) {
                neighbour.out_neighbours().iter().for_each(|node| {
                    let id = node.node;
                    if out_components.insert(id) {
                        to_check_stack.push(id);
                    }
                });
            }
        }

        let state: &mut OutState = vv.get_mut();
        state.out_components = out_components.into_iter().collect();
        Step::Done
    });

    let mut runner = TaskRunner::new(ctx);
    let index = Index::for_graph(g);

    Ok(runner.run_with_index(
        index,
        vec![Job::new(step1)],
        vec![],
        None,
        |_, _, _, local: Vec<OutState>, index| {
            NodeState::new_from_eval_mapped_with_index(g.clone(), local, index, |v| {
                Nodes::new_filtered(
                    g.clone(),
                    g.clone(),
                    Const(true),
                    Index::from_iter(v.out_components),
                )
            })
        },
        threads,
        1,
        None,
        None,
    ))
}

/// Computes the out-component of a given node in the graph
///
/// # Arguments:
///
/// - `node` - The node whose out-component we wish to calculate
///
/// # Returns:
///
/// Nodes in the out-component with their distances from the starting node.
///
pub fn out_component<'graph, G>(node: NodeView<'graph, G>) -> NodeState<'graph, usize, G>
where
    G: GraphViewOps<'graph>,
{
    out_component_filtered(node, Unfiltered).expect("Unfiltered should never fail")
}

/// Computes the out-component of a given node in the filtered graph
///
/// # Arguments:
///
/// - `node` - The node whose out-component we wish to calculate
/// - `filter` - Filter
///
/// # Returns:
///
/// Filtered nodes in the out-component with their distances from the starting node.
///
pub fn out_component_filtered<'graph, G, F>(
    node: NodeView<'graph, G>,
    filter: F,
) -> Result<NodeState<'graph, usize, G>, GraphError>
where
    G: GraphViewOps<'graph>,
    F: CreateFilter + 'graph,
    F::EntityFiltered<'graph, F::FilteredGraph<'graph, G>>: GraphViewOps<'graph>,
{
    let mut out_components = HashMap::new();
    let mut to_check_stack = VecDeque::new();
    let filtered = node.filter(filter)?;
    filtered.out_neighbours().iter().for_each(|node| {
        let id = node.node;
        out_components.insert(id, 1usize);
        to_check_stack.push_back((id, 1usize));
    });
    while let Some((neighbour_id, d)) = to_check_stack.pop_front() {
        let d = d + 1;
        if let Some(neighbour) = (&&filtered.graph).node(neighbour_id) {
            neighbour.out_neighbours().iter().for_each(|node| {
                let id = node.node;
                if let Entry::Vacant(entry) = out_components.entry(id) {
                    entry.insert(d);
                    to_check_stack.push_back((id, d));
                }
            });
        }
    }

    let (nodes, distances): (IndexSet<_, ahash::RandomState>, Vec<_>) =
        out_components.into_iter().sorted().unzip();
    Ok(NodeState::new(
        node.graph.clone(),
        distances.into(),
        Index::Partial(nodes.into()),
    ))
}
