use crate::{
    core::{entities::VID, state::compute_state::ComputeStateVec},
    db::{
        api::{
            state::{GenericNodeState, Index, NodeStateOutputType, TypedNodeState},
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
use serde::{Deserialize, Serialize};
use std::collections::{hash_map::Entry, HashMap, HashSet, VecDeque};

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, Default)]
pub struct OutState {
    pub out_components: Vec<VID>,
}

#[derive(Clone, Debug)]
pub struct TransformedOutState<'graph, G, GH = G>
where
    G: GraphViewOps<'graph>,
    GH: GraphViewOps<'graph>,
{
    pub out_components: Nodes<'graph, G, GH>,
}

impl OutState {
    pub fn node_transform<'graph, G>(
        state: &GenericNodeState<'graph, G>,
        value: OutState,
    ) -> TransformedOutState<'graph, G>
    where
        G: GraphViewOps<'graph>,
    {
        TransformedOutState {
            out_components: Nodes::new_filtered(
                state.base_graph.clone(),
                state.graph.clone(),
                Some(Index::from_iter(value.out_components)),
                None,
            ),
        }
    }
}

/// Computes the out components of each node in the graph
///
/// # Arguments
///
/// - `g` - A reference to the graph
/// - `threads` - Number of threads to use
///
/// # Returns
///
/// An [AlgorithmResult] containing the mapping from each node to a vector of node ids (the nodes out component)
///
pub fn out_components<G>(
    g: &G,
    threads: Option<usize>,
) -> TypedNodeState<'static, OutState, G, G, TransformedOutState<'static, G>>
where
    G: StaticGraphViewOps,
{
    let ctx: Context<G, ComputeStateVec> = g.into();
    let step1 = ATask::new(move |vv: &mut EvalNodeView<G, OutState>| {
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

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);

    runner.run(
        vec![Job::new(step1)],
        vec![],
        None,
        |_, _, _, local: Vec<OutState>| {
            TypedNodeState::new_mapped(
                GenericNodeState::new_from_eval(
                    g.clone(),
                    local,
                    Some(HashMap::from([(
                        "out_components".to_string(),
                        (NodeStateOutputType::Nodes, None, None),
                    )])),
                ),
                OutState::node_transform,
            )
        },
        threads,
        1,
        None,
        None,
    )
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, Default)]
pub struct OutComponentState {
    pub distance: usize,
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
pub fn out_component<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>(
    node: NodeView<'graph, G, GH>,
) -> TypedNodeState<'graph, OutComponentState, G> {
    let mut out_components = HashMap::new();
    let mut to_check_stack = VecDeque::new();
    node.out_neighbours().iter().for_each(|node| {
        let id = node.node;
        out_components.insert(id, 1usize);
        to_check_stack.push_back((id, 1usize));
    });
    while let Some((neighbour_id, d)) = to_check_stack.pop_front() {
        let d = d + 1;
        if let Some(neighbour) = (&&node.graph).node(neighbour_id) {
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
    TypedNodeState::new(GenericNodeState::new_from_eval_with_index(
        node.base_graph.clone(),
        node.base_graph.clone(),
        distances
            .into_iter()
            .map(|value| OutComponentState { distance: value })
            .collect(),
        Some(Index::new(nodes)),
        None,
    ))
}
