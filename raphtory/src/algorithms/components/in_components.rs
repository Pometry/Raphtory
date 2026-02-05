use crate::{
    core::entities::VID,
    db::{
        api::{
            state::{ops::Const, GenericNodeState, Index, NodeStateOutputType, TypedNodeState},
            storage::graph,
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
use serde::{Deserialize, Serialize};
use std::{
    collections::{hash_map::Entry, HashMap, HashSet, VecDeque},
    fmt::Debug,
};

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, Default)]
pub struct InState {
    pub in_components: Vec<VID>,
}

#[derive(Clone, Debug)]
pub struct TransformedInState<'graph, G>
where
    G: GraphViewOps<'graph>,
{
    pub in_components: Nodes<'graph, G, G>,
}

impl InState {
    pub fn node_transform<'graph, G>(
        state: &GenericNodeState<'graph, G>,
        value: Self,
    ) -> TransformedInState<'graph, G>
    where
        G: GraphViewOps<'graph>,
    {
        TransformedInState {
            in_components: Nodes::new_filtered(
                state.base_graph.clone(),
                state.base_graph.clone(),
                Const(true),
                Some(Index::from_iter(value.in_components)),
            ),
        }
    }
}

/// Computes the in components of each node in the graph
///
/// # Arguments
///
/// - `g` - A reference to the graph
/// - `threads` - Number of threads to use
///
pub fn in_components<G>(
    g: &G,
    threads: Option<usize>,
) -> TypedNodeState<'static, InState, G, TransformedInState<'static, G>>
where
    G: StaticGraphViewOps,
{
    in_components_filtered(g, threads, Unfiltered).expect("Unfiltered should never fail")
}

/// Computes the in components of each node in the filtered graph
///
/// # Arguments
///
/// - `g` - A reference to the graph
/// - `threads` - Number of threads to use
/// - `filter` - Filter
///
pub fn in_components_filtered<G, F>(
    g: &G,
    threads: Option<usize>,
    filter: F,
) -> Result<TypedNodeState<'static, InState, G, TransformedInState<'static, G>>, GraphError>
where
    G: StaticGraphViewOps,
    F: CreateFilter + 'static,
    F::EntityFiltered<'static, F::FilteredGraph<'static, G>>: StaticGraphViewOps,
{
    let filtered = g.filter(filter)?;
    let ctx: Context<_, _> = (&filtered).into();

    let step1 = ATask::new(move |vv: &mut EvalNodeView<_, InState>| {
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

    let mut runner = TaskRunner::new(ctx);

    Ok(runner.run(
        vec![Job::new(step1)],
        vec![],
        None,
        |_, _, _, local: Vec<InState>| {
            TypedNodeState::new_mapped(
                GenericNodeState::new_from_eval(
                    g.clone(),
                    local,
                    Some(HashMap::from([(
                        "in_components".to_string(),
                        (NodeStateOutputType::Nodes, None),
                    )])),
                ),
                InState::node_transform,
            )
        },
        threads,
        1,
        None,
        None,
    ))
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, Default)]
pub struct InComponentState {
    pub distance: usize,
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
pub fn in_component<'graph, G>(
    node: NodeView<'graph, G>,
) -> TypedNodeState<'graph, InComponentState, G>
where
    G: GraphViewOps<'graph>,
{
    in_component_filtered(node, Unfiltered).expect("Unfiltered should never fail")
}

/// Computes the in-component of a given node in the filtered graph
///
/// # Arguments:
///
/// - `node` - The node whose in-component we wish to calculate
/// - `filter` - Filter
///
/// # Returns:
///
/// The filtered nodes within the given nodes in-component and their distances from the starting node.
///
pub fn in_component_filtered<'graph, G, F>(
    node: NodeView<'graph, G>,
    filter: F,
) -> Result<TypedNodeState<'graph, InComponentState, G>, GraphError>
where
    G: GraphViewOps<'graph>,
    F: CreateFilter + 'graph,
    F::EntityFiltered<'graph, F::FilteredGraph<'graph, G>>: GraphViewOps<'graph>,
{
    let mut in_components = HashMap::new();
    let mut to_check_stack = VecDeque::new();
    let filtered = node.filter(filter)?;
    filtered.in_neighbours().iter().for_each(|node| {
        let id = node.node;
        in_components.insert(id, 1usize);
        to_check_stack.push_back((id, 1usize));
    });
    while let Some((neighbour_id, d)) = to_check_stack.pop_front() {
        let d = d + 1;
        if let Some(neighbour) = (&&filtered.graph).node(neighbour_id) {
            neighbour.in_neighbours().iter().for_each(|node| {
                let id = node.node;
                if let Entry::Vacant(entry) = in_components.entry(id) {
                    entry.insert(d);
                    to_check_stack.push_back((id, d));
                }
            });
        }
    }

    Ok(TypedNodeState::new(GenericNodeState::new_from_map(
        node.graph.clone(),
        in_components,
        |distance| InComponentState { distance },
        None,
    )))
}
