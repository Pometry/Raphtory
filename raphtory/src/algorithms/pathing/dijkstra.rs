use crate::db::graph::edge::EdgeView;
/// Dijkstra's algorithm
use crate::{core::entities::nodes::node_ref::AsNodeRef, db::api::view::StaticGraphViewOps};
use crate::{
    core::entities::nodes::node_ref::NodeRef,
    db::{
        api::state::{ops::filter::NO_FILTER, Index, NodeState},
        graph::nodes::Nodes,
    },
    errors::GraphError,
    prelude::*,
};
use indexmap::IndexSet;
use raphtory_api::core::{
    entities::{
        properties::prop::{PropType, PropUnwrap},
        VID,
    },
    Direction,
};
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
};
use super::to_prop;

/// A state in the Dijkstra algorithm with a cost and a node name.
#[derive(PartialEq)]
struct State {
    cost: Prop,
    node: VID,
}

impl Eq for State {}

impl Ord for State {
    fn cmp(&self, other: &State) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl PartialOrd for State {
    fn partial_cmp(&self, other: &State) -> Option<Ordering> {
        other.cost.partial_cmp(&self.cost)
    }
}

/// Finds the shortest paths from a single source to multiple targets in a graph.
///
/// # Arguments
///
/// * `graph`: The graph to search in.
/// * `source`: The source node.
/// * `targets`: A vector of target nodes.
/// * `weight`: Option, The name of the weight property for the edges. If not set then defaults all edges to weight=1.
/// * `direction`: The direction of the edges of the shortest path. Defaults to both directions (undirected graph).
///
/// # Returns
///
/// Returns a `HashMap` where the key is the target node and the value is a tuple containing
/// the total cost and a vector of nodes representing the shortest path.

pub fn dijkstra_single_source_shortest_paths<G: StaticGraphViewOps, T: AsNodeRef>(
    g: &G,
    source: T,
    targets: Vec<T>,
    weight: Option<&str>,
    direction: Direction,
) -> Result<NodeState<'static, (f64, Nodes<'static, G>), G>, GraphError> {
    let cost_val = to_prop(g, weight, 0.0)?;
    let max_val = to_prop(g, weight, f64::MAX)?;
    let weight_fn = |edge: &EdgeView<G>| -> Option<Prop> {
        let edge_val = match weight{
            None => Some(Prop::U8(1)),
            Some(weight) => match edge.properties().get(weight) {
                Some(prop) => Some(prop),
                _ => None
            }
         };
         edge_val
    };
    let targets_len = targets.len();
    dijkstra_single_source_shortest_paths_algorithm(g, source, Some(targets), direction, targets_len, cost_val, max_val, weight_fn)
}

pub(crate) fn dijkstra_single_source_shortest_paths_algorithm<G: StaticGraphViewOps, T: AsNodeRef, F: Fn(&EdgeView<G>) -> Option<Prop>>(
    g: &G,
    source: T,
    targets: Option<Vec<T>>,
    direction: Direction,
    k: usize,
    cost_val: Prop,
    max_val: Prop,
    weight_fn: F
) -> Result<NodeState<'static, (f64, Nodes<'static, G>), G>, GraphError> {
    let mut k = k;
    let source_ref = source.as_node_ref();
    let source_node = match g.node(source_ref) {
        Some(src) => src,
        None => {
            let gid = match source_ref {
                NodeRef::Internal(vid) => g.node_id(vid),
                NodeRef::External(gid) => gid.to_owned(),
            };
            return Err(GraphError::NodeMissingError(gid));
        }
    };
    let target_nodes = if let Some(targets) = targets {
        let mut target_nodes = vec![false; g.unfiltered_num_nodes()];
        for target in targets {
            if let Some(target_node) = g.node(target) {
                target_nodes[target_node.node.index()] = true;
            }
        }
        target_nodes
    } else {
        vec![true; g.unfiltered_num_nodes()]
    };

    let mut heap = BinaryHeap::new();

    heap.push(State {
        cost: cost_val.clone(),
        node: source_node.node,
    });

    let mut dist: HashMap<VID, Prop> = HashMap::new();
    let mut predecessor: HashMap<VID, VID> = HashMap::new();
    let mut visited: HashSet<VID> = HashSet::new();
    let mut paths: HashMap<VID, (f64, IndexSet<VID, ahash::RandomState>)> = HashMap::new();

    dist.insert(source_node.node, cost_val.clone());

    while let Some(State {
        cost,
        node: node_vid,
    }) = heap.pop()
    {
        if target_nodes[node_vid.index()] && !paths.contains_key(&node_vid) {
            let mut path = IndexSet::default();
            path.insert(node_vid);
            let mut current_node_id = node_vid;
            while let Some(prev_node) = predecessor.get(&current_node_id) {
                path.insert(*prev_node);
                current_node_id = *prev_node;
            }
            path.reverse();
            paths.insert(node_vid, (cost.as_f64().unwrap(), path));
            k -= 1;
            if k == 0 {
                break;
            }
        }
        if !visited.insert(node_vid) {
            continue;
        }

        let edges = match direction {
            Direction::OUT => g.node(node_vid).unwrap().out_edges(),
            Direction::IN => g.node(node_vid).unwrap().in_edges(),
            Direction::BOTH => g.node(node_vid).unwrap().edges(),
        };

        // Replace this loop with your actual logic to iterate over the outgoing edges
        for edge in edges {
            let next_node_vid = edge.nbr().node;

            let edge_val = if let Some(w) = weight_fn(&edge) {
                w
            } else {
                continue;
            };
            let next_cost = cost.clone().add(edge_val).unwrap();
            if next_cost < *dist.entry(next_node_vid).or_insert(max_val.clone()) {
                heap.push(State {
                    cost: next_cost.clone(),
                    node: next_node_vid,
                });
                dist.insert(next_node_vid, next_cost);
                predecessor.insert(next_node_vid, node_vid);
            }
        }
    }
    let (index, values): (IndexSet<_, ahash::RandomState>, Vec<_>) = paths
        .into_iter()
        .map(|(id, (cost, path))| {
            let nodes =
                Nodes::new_filtered(g.clone(), g.clone(), NO_FILTER, Some(Index::new(path)));
            (id, (cost, nodes))
        })
        .unzip();
    Ok(NodeState::new(
        g.clone(),
        values.into(),
        Some(Index::new(index)),
    ))
}
