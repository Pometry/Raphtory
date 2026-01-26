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
/// the total dist and a vector of nodes representing the shortest path.
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

    // Turn below into a generic function, then add a closure to ensure the prop is correctly unwrapped
    // after the calc is done
    let dist_val = match weight_type {
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
    let mut shortest_paths: HashMap<VID, (f64, IndexSet<VID, ahash::RandomState>)> = HashMap::new();
    let mut dist: HashMap<VID, Prop> = HashMap::new(); 
    let mut predecessor: HashMap<VID, VID> = HashMap::new();

    let n_nodes = g.count_nodes();
   
    for node in g.nodes() {
        predecessor.insert(node.node, node.node);   
        if node.node == source_node.node {
            dist.insert(source_node.node, dist_val.clone());
        } else {
            dist.insert(node.node, max_val.clone());
        }
    }

    for i in 1..n_nodes {
        for node in g.nodes() {
            if node.node == source_node.node {
                continue;
            }
            let mut min_dist = dist.get(&node.node).unwrap().clone();
            let mut min_node = predecessor.get(&node.node).unwrap().clone();
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
                let neighbor_dist = dist.get(&neighbor_vid).unwrap(); 
                if neighbor_dist == &max_val {
                    continue;
                }
                let new_dist = neighbor_dist.clone().add(edge_val).unwrap();
                if new_dist < min_dist {
                    min_dist = new_dist;
                    min_node = neighbor_vid;
                }
            }
            dist.insert(node.node, min_dist);
            predecessor.insert(node.node, min_node);
        }
    }

    for node in g.nodes() {
        let edges = match direction {
            Direction::IN => node.out_edges(),
            Direction::OUT => node.in_edges(),
            Direction::BOTH => node.edges(),
        };
        let node_dist = dist.get(&node.node).unwrap();
        for edge in edges {
            let edge_val = match weight {
                None => Prop::U8(1),
                Some(weight) => match edge.properties().get(weight) {
                    Some(prop) => prop,
                    _ => continue,
                },
            };
            let neighbor_vid = edge.nbr().node;
            let neighbor_dist = dist.get(&neighbor_vid).unwrap(); 
            if neighbor_dist == &max_val {
                continue;
            }
            let new_dist = neighbor_dist.clone().add(edge_val).unwrap();
            if new_dist < *node_dist {
                return Err(GraphError::InvalidProperty { reason: "Negative cycle detected".to_string() });
            }
        }
    } 

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
        path.insert(target_node.node);
        let mut current_node_id = target_node.node;
        while let Some(prev_node) = predecessor.get(&current_node_id) {
            if *prev_node == current_node_id {
                break;
            }
            path.insert(*prev_node);
            current_node_id = *prev_node;
        }
        path.reverse();
        shortest_paths.insert(
            target_node.node,
            (dist.get(&target_node.node).unwrap().as_f64().unwrap(), path),
        );
    } 

    let (index, values): (IndexSet<_, ahash::RandomState>, Vec<_>) = shortest_paths
        .into_iter()
        .map(|(id, (dist, path))| {
            let nodes =
                Nodes::new_filtered(g.clone(), g.clone(), NO_FILTER, Some(Index::new(path)));
            (id, (dist, nodes))
        })
        .unzip();
    
    Ok(NodeState::new(
        g.clone(),
        values.into(),
        Some(Index::new(index)),
    ))
}
