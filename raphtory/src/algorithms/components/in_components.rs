use crate::{
    core::{entities::VID, state::compute_state::ComputeStateVec},
    db::{
        api::{
            state::{Index, NodeState},
            view::{NodeViewOps, StaticGraphViewOps},
        },
        graph::{node::NodeView, nodes::Nodes},
        task::{
            context::Context,
            node::eval_node::EvalNodeView,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
        },
    },
    prelude::GraphViewOps,
};
use indexmap::IndexSet;
use itertools::Itertools;
use std::collections::{hash_map::Entry, HashMap, HashSet, VecDeque};

#[derive(Clone, Debug, Default)]
struct InState {
    in_components: Vec<VID>,
}

/// Computes the in components of each node in the graph
///
/// # Arguments
///
/// - `g` - A reference to the graph
/// - `threads` - Number of threads to use
///
/// # Returns
///
/// An [AlgorithmResult] containing the mapping from each node to a vector of node ids (the nodes in component)
///
pub fn in_components<G>(g: &G, threads: Option<usize>) -> NodeState<'static, Nodes<'static, G>, G>
where
    G: StaticGraphViewOps,
{
    let ctx: Context<G, ComputeStateVec> = g.into();
    let step1 = ATask::new(move |vv: &mut EvalNodeView<G, InState>| {
        let mut in_components = HashSet::new();
        let mut to_check_stack = Vec::new();
        vv.in_neighbours().iter().for_each(|node| {
            let id = node.node;
            if in_components.insert(id) {
                to_check_stack.push(id);
            }
        });
        while let Some(neighbour_id) = to_check_stack.pop() {
            if let Some(neighbour) = vv.graph().node(neighbour_id) {
                neighbour.in_neighbours().iter().for_each(|node| {
                    let id = node.node;
                    if in_components.insert(id) {
                        to_check_stack.push(id);
                    }
                });
            }
        }

        let state: &mut InState = vv.get_mut();
        state.in_components = in_components.into_iter().collect();
        Step::Done
    });

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);

    runner.run(
        vec![Job::new(step1)],
        vec![],
        None,
        |_, _, _, local: Vec<InState>| {
            NodeState::new_from_eval_mapped(g.clone(), local, |v| {
                Nodes::new_filtered(
                    g.clone(),
                    g.clone(),
                    Some(Index::from_iter(v.in_components)),
                    None,
                )
            })
        },
        threads,
        1,
        None,
        None,
    )
}

/// Computes the in-component of a given node in the graph
///
/// # Arguments:
///
/// - `node` - The node whose in-component we wish to calculate
///
/// # Returns:
///
/// The nodes within the given nodes in-component and their distances from the starting node.
///
pub fn in_component<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>(
    node: NodeView<'graph, G, GH>,
) -> NodeState<'graph, usize, G> {
    let mut in_components = HashMap::new();
    let mut to_check_stack = VecDeque::new();
    node.in_neighbours().iter().for_each(|node| {
        let id = node.node;
        in_components.insert(id, 1usize);
        to_check_stack.push_back((id, 1usize));
    });
    while let Some((neighbour_id, d)) = to_check_stack.pop_front() {
        let d = d + 1;
        if let Some(neighbour) = (&&node.graph).node(neighbour_id) {
            neighbour.in_neighbours().iter().for_each(|node| {
                let id = node.node;
                if let Entry::Vacant(entry) = in_components.entry(id) {
                    entry.insert(d);
                    to_check_stack.push_back((id, d));
                }
            });
        }
    }

    let (nodes, distances): (IndexSet<_, ahash::RandomState>, Vec<_>) =
        in_components.into_iter().sorted().unzip();
    NodeState::new(
        node.base_graph.clone(),
        node.base_graph.clone(),
        distances.into(),
        Some(Index::new(nodes)),
    )
}
