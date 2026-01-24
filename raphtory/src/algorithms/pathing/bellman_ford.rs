/// Bellman-Ford algorithm
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
    collections::{HashMap},
};


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
///
pub fn bellman_ford_single_source_shortest_paths<G: StaticGraphViewOps, T: AsNodeRef>(
    g: &G,
    source: T,
    targets: Vec<T>,
    weight: Option<&str>,
    direction: Direction,
) -> Result<NodeState<'static, (f64, Nodes<'static, G>), G>, GraphError> {
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
    let mut weight_type = PropType::U8;
    if let Some(weight) = weight {
        if let Some((_, dtype)) = g.edge_meta().get_prop_id_and_type(weight, false) {
            weight_type = dtype;
        } else {
            return Err(GraphError::PropertyMissingError(weight.to_string()));
        }
    }

    let mut target_nodes = vec![false; g.unfiltered_num_nodes()];
    for target in targets {
        if let Some(target_node) = g.node(target) {
            target_nodes[target_node.node.index()] = true;
        }
    }

    // Turn below into a generic function, then add a closure to ensure the prop is correctly unwrapped
    // after the calc is done
    let cost_val = match weight_type {
        PropType::F32 => Prop::F32(0f32),
        PropType::F64 => Prop::F64(0f64),
        PropType::U8 => Prop::U8(0u8),
        PropType::U16 => Prop::U16(0u16),
        PropType::U32 => Prop::U32(0u32),
        PropType::U64 => Prop::U64(0u64),
        PropType::I32 => Prop::I32(0i32),
        PropType::I64 => Prop::I64(0i64),
        p_type => {
            return Err(GraphError::InvalidProperty {
                reason: format!("Weight type: {:?}, not supported", p_type),
            })
        }
    };
    let max_val = match weight_type {
        PropType::F32 => Prop::F32(f32::MAX),
        PropType::F64 => Prop::F64(f64::MAX),
        PropType::U8 => Prop::U8(u8::MAX),
        PropType::U16 => Prop::U16(u16::MAX),
        PropType::U32 => Prop::U32(u32::MAX),
        PropType::U64 => Prop::U64(u64::MAX),
        PropType::I32 => Prop::I32(i32::MAX),
        PropType::I64 => Prop::I64(i64::MAX),
        p_type => {
            return Err(GraphError::InvalidProperty {
                reason: format!("Weight type: {:?}, not supported", p_type),
            })
        }
    };
    let mut shortest_paths: HashMap<VID, HashMap<usize, (Prop, IndexSet<VID, ahash::RandomState>)>> = HashMap::new();
    let n_nodes = g.count_nodes();
    let mut source_shortest_paths_hashmap = HashMap::new();
    let mut source_path = IndexSet::default();
    source_path.insert(source_node.node);
    for i in 0..n_nodes {
        source_shortest_paths_hashmap.insert(i, (cost_val.clone(), source_path.clone()));
    }
    shortest_paths.insert(source_node.node, source_shortest_paths_hashmap);

    for node in g.nodes() {
        if node.node == source_node.node {
            continue;
        }
        let mut node_shortest_paths_hashmap = HashMap::new();
        node_shortest_paths_hashmap.insert(0, (max_val.clone(), IndexSet::default()));
        shortest_paths.insert(node.node, node_shortest_paths_hashmap);
    }

    for i in 1..(n_nodes) {
        for node in g.nodes() {
            if node.node == source_node.node {
                continue;
            }
            let (mut min_cost, mut min_path) = shortest_paths.get(&node.node).unwrap().get(&(i - 1)).unwrap().clone();
            let edges = match direction {
                Direction::IN => node.out_edges(),
                Direction::OUT => node.in_edges(),
                Direction::BOTH => node.edges(),
            };
            for edge in edges {
                let edge_val = match weight {
                    None => Prop::U8(1),
                    Some(weight) => match edge.properties().get(weight) {
                        Some(prop) => prop,
                        _ => continue,
                    },
                };
                let neighbor_vid = edge.nbr().node;
                let neighbor_shortest_paths = shortest_paths.get(&neighbor_vid).unwrap();
                let (neighbor_shortest_path_cost, neighbor_shortest_path) =
                    neighbor_shortest_paths.get(&(i - 1)).unwrap();
                if neighbor_shortest_path_cost == &max_val {
                    continue;
                }
                let new_cost = neighbor_shortest_path_cost.clone().add(edge_val).unwrap();
                if new_cost < min_cost {
                    min_cost = new_cost;
                    min_path = neighbor_shortest_path.clone();
                    min_path.insert(node.node);
                }
            }
            shortest_paths.get_mut(&node.node).unwrap().insert(i, (min_cost, min_path));
        }
    }

    let (index, values): (IndexSet<_, ahash::RandomState>, Vec<_>) = shortest_paths
        .iter_mut()
        .filter_map(|(id, nodes_path_hashmap)| {
            if !target_nodes[id.index()] {
                return None;
            }
            let (cost, path) = nodes_path_hashmap.remove(&(n_nodes - 1)).unwrap();
            let nodes =
                Nodes::new_filtered(g.clone(), g.clone(), NO_FILTER, Some(Index::new(path)));
            Some((id, (cost.as_f64().unwrap(), nodes)))
        })
        .unzip();
    
    Ok(NodeState::new(
        g.clone(),
        values.into(),
        Some(Index::new(index)),
    ))
}
