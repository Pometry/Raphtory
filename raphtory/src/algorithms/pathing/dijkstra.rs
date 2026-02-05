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
use std::usize;
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
};
use super::to_prop;

pub trait GraphMap<T: Clone> {
    fn new(n_nodes: usize, s: usize, default: T) -> Self;
    
    fn get_item(&self, vid: VID) -> T;
    
    fn set_item(&mut self, vid: VID, value: T);
}

impl<T: Clone> GraphMap<T> for Vec<T> {
    fn new(n_nodes: usize, s: usize, default: T) -> Self {
        vec![default; n_nodes] 
    }

    fn get_item(&self, vid: VID) -> T {
        self[vid.index()].clone()
    }

    fn set_item(&mut self, vid: VID, value: T) {
        self[vid.index()] = value; 
    }
}

impl<T: Clone> GraphMap<T> for HashMap<VID, T> {
    fn new(n_nodes: usize, s: usize, default: T) -> Self {
        HashMap::with_capacity(s) 
    }

    fn get_item(&self, vid: VID) -> T {
        self.get(&vid).cloned().unwrap()
    }

    fn set_item(&mut self, vid: VID, value: T) {
        self.insert(vid, value);
    }
}

pub trait GraphSet {
    fn new(n_nodes: usize, s: usize) -> Self;
    fn mark_visited(&mut self, vid: VID);
    fn is_visited(&self, vid: VID) -> bool;
}

impl GraphSet for Vec<bool> {
    fn new(n_nodes: usize, s: usize) -> Self {
        vec![false; n_nodes]
    }
    fn mark_visited(&mut self, vid: VID) {
        self[vid.index()] = true;
    }
    fn is_visited(&self, vid: VID) -> bool {
        self[vid.index()]
    }
}

impl GraphSet for HashSet<VID> {
    fn new(n_nodes: usize, s: usize) -> Self {
        HashSet::with_capacity(s)
    }
    fn mark_visited(&mut self, vid: VID) {
        self.insert(vid);
    }
    fn is_visited(&self, vid: VID) -> bool {
        self.contains(&vid)
    }
}

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
    let n_nodes = g.count_nodes();
    let (distances, predecessor) = dijkstra_single_source_shortest_paths_algorithm::<G, T, _, Vec<Prop>, Vec<VID>, Vec<bool>>(g, source, direction, n_nodes - 1, cost_val, max_val, weight_fn)?;
    let mut paths: HashMap<VID, (f64, IndexSet<VID, ahash::RandomState>)> = HashMap::new();
    for target in targets.into_iter() {
        let target_ref = target.as_node_ref();
        let target_node = match g.node(target_ref) {
            Some(tgt) => tgt,
            None => {
                let gid = match target_ref {
                    NodeRef::Internal(vid) => g.node_id(vid),
                    NodeRef::External(gid) => gid.to_owned(),
                };
                return Err(GraphError::NodeMissingError(gid));
            }
        };
        let mut path = IndexSet::default();
        let node_vid = target_node.node;
        path.insert(node_vid);
        let mut current_node_id = node_vid;
        while let Some(prev_node) = predecessor.get(current_node_id.index()) {
            if *prev_node == current_node_id {
                break;
            }
            path.insert(*prev_node);
            current_node_id = *prev_node;
        }
        path.reverse();
        paths.insert(node_vid, (distances[node_vid.index()].as_f64().unwrap(), path));
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

pub(crate) fn dijkstra_single_source_shortest_paths_algorithm<G: StaticGraphViewOps, T: AsNodeRef, F: Fn(&EdgeView<G>) -> Option<Prop>,D: GraphMap<Prop>, P: GraphMap<VID>, V: GraphSet>(
    g: &G,
    source: T,
    direction: Direction,
    k: usize,
    cost_val: Prop,
    max_val: Prop,
    weight_fn: F
) -> Result<(D, P), GraphError> {
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
    let n_nodes = g.count_nodes();

    let mut heap = BinaryHeap::new();

    heap.push(State {
        cost: cost_val.clone(),
        node: source_node.node,
    });
    let s = n_nodes.min(k + 1);
    let mut dist = D::new(n_nodes, s, max_val.clone());
    dist.set_item(source_node.node, cost_val);
    let mut predecessor = P::new(n_nodes, s, VID(usize::MAX));
    let mut visited = V::new(n_nodes, s);
    let mut visited_count = 0;

    while let Some(State {
        cost,
        node: node_vid,
    }) = heap.pop()
    {
        // accounts for source node
        if visited_count == k + 1 {
            break;
        }
        if visited.is_visited(node_vid) {
            continue;
        } else {
            visited.mark_visited(node_vid);
            visited_count += 1;
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
            if next_cost < dist.get_item(next_node_vid) {
                heap.push(State {
                    cost: next_cost.clone(),
                    node: next_node_vid,
                });
                dist.set_item(next_node_vid, next_cost);
                predecessor.set_item(next_node_vid, node_vid);
            }
        }
    }
    Ok((dist, predecessor))
}
