use crate::db::graph::edge::EdgeView;
use crate::db::graph::node::NodeView;
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
use std::hash::Hash;
use std::{
    collections::{HashMap},
};
use super::{to_prop, get_prop_val};


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
    // Turn below into a generic function, then add a closure to ensure the prop is correctly unwrapped
    // after the calc is done
    let dist_val = to_prop(g, weight, 0.0)?;
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
    let (distances, predecessor) = bellman_ford_single_source_shortest_paths_algorithm(g, Some(source), direction, dist_val, max_val, weight_fn)?;
    let mut shortest_paths: HashMap<VID, (f64, IndexSet<VID, ahash::RandomState>)> = HashMap::new();
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
        while let Some(prev_node) = predecessor.get(current_node_id.index()) {
            if *prev_node == current_node_id {
                break;
            }
            path.insert(*prev_node);
            current_node_id = *prev_node;
        }
        path.reverse();
        shortest_paths.insert(
            target_node.node,
            (distances[target_node.node.index()].as_f64().unwrap(), path),
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

pub(crate) fn bellman_ford_single_source_shortest_paths_algorithm<G: StaticGraphViewOps, T: AsNodeRef, F: Fn(&EdgeView<G>) -> Option<Prop>>(
    g: &G,
    source: Option<T>,
    direction: Direction,
    dist_val: Prop,
    max_val: Prop,
    weight_fn: F
) -> Result<(Vec<Prop>, Vec<VID>), GraphError> {
    let max_bound = get_prop_val(max_val.dtype(), f64::MAX)?;
    let mut dummy_node = false;
    // creates a dummy node if source node is none
    let source_node_vid = if let Some(source) = source {  
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
        source_node.node
    } else {
        dummy_node = true;
        VID(usize::MAX)
    };
    let n_nodes = g.count_nodes();
    let mut dist: Vec<Prop> = vec![max_val.clone(); n_nodes];
    let mut predecessor: Vec<VID> = vec![VID(usize::MAX); n_nodes];

    let n_nodes = g.count_nodes();
   
    for node in g.nodes() {
        let node_idx = node.node.index(); 
        if dummy_node {
            predecessor[node_idx] = source_node_vid;   
        } else {
            predecessor[node_idx] = node.node;   
            if node.node == source_node_vid {
                dist[source_node_vid.index()] = dist_val.clone();
            } else {
                dist[node_idx] = max_val.clone();
            }
        }
    }

    for _ in 1..n_nodes {
        let mut changed = false;
        for node in g.nodes() {
            if node.node == source_node_vid {
                continue;
            }
            let node_idx = node.node.index();
            let mut min_dist = dist[node_idx].clone();
            let mut min_node = predecessor[node_idx];
            let edges = match direction {
                Direction::IN => node.out_edges(),
                Direction::OUT => node.in_edges(),
                Direction::BOTH => node.edges(),
            };
            for edge in edges {
                let edge_val = if let Some(w) = weight_fn(&edge) {
                    w
                } else {
                    continue;
                };
                let neighbor_vid = edge.nbr().node;
                let neighbor_dist = dist[neighbor_vid.index()].clone();
                if neighbor_dist == max_bound {
                    continue;
                }
                let new_dist = neighbor_dist.clone().add(edge_val).unwrap();
                if new_dist < min_dist {
                    min_dist = new_dist;
                    min_node = neighbor_vid;
                    changed = true;
                }
            }
            dist[node_idx] = min_dist;
            predecessor[node_idx] = min_node;
        }
        if !changed {
            break;
        }
    }

    for node in g.nodes() {
        let edges = match direction {
            Direction::IN => node.out_edges(),
            Direction::OUT => node.in_edges(),
            Direction::BOTH => node.edges(),
        };
        let node_dist = &dist[node.node.index()];
        for edge in edges {
            let edge_val = if let Some(w) = weight_fn(&edge) {
                w
            } else {
                continue;
            };
            let neighbor_vid = edge.nbr().node;
            let neighbor_dist = &dist[neighbor_vid.index()];
            if *neighbor_dist == max_bound {
                continue;
            }
            let new_dist = neighbor_dist.clone().add(edge_val).unwrap();
            if new_dist < *node_dist {
                return Err(GraphError::InvalidProperty { reason: "Negative cycle detected".to_string() });
            }
        }
    } 
    Ok((dist, predecessor))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{api::mutation::AdditionOps, graph::graph::Graph};
    use raphtory_api::core::Direction;

    fn load_graph(edges: Vec<(i64, &str, &str, Vec<(&str, f32)>)>) -> Graph {
        let graph = Graph::new();
        for (t, src, dst, props) in edges {
            graph.add_edge(t, src, dst, props, None).unwrap();
        }
        graph
    }

    fn graph_with_negative_weights() -> Graph {
        load_graph(vec![
            (0, "A", "B", vec![("weight", 4.0f32)]),
            (1, "A", "C", vec![("weight", 2.0f32)]),
            (2, "B", "C", vec![("weight", -3.0f32)]),
            (3, "C", "D", vec![("weight", 2.0f32)]),
            (4, "B", "D", vec![("weight", 5.0f32)]),
        ])
    }

    #[test]
    fn test_bellman_ford_with_virtual_source() {
        // Test with source = None and max_val = 0 (same as dist_val)
        // This simulates adding a virtual source node with zero-weight edges to all nodes
        // Used in Johnson's algorithm to compute potential function h(v)
        let graph = graph_with_negative_weights();

        let dist_val = Prop::F32(0.0);
        let max_val = Prop::F32(0.0);
        
        let weight_fn = |edge: &EdgeView<Graph>| -> Option<Prop> {
            edge.properties().get("weight")
        };

        let result = bellman_ford_single_source_shortest_paths_algorithm(
            &graph,
            None::<NodeRef>,
            Direction::OUT,
            dist_val,
            max_val,
            weight_fn,
        );

        assert!(result.is_ok(), "Bellman-Ford with virtual source should succeed");
        
        let (distances, predecessors) = result.unwrap();
        
        // All nodes should have distances computed from virtual source
        assert_eq!(distances.len(), graph.count_nodes());
        assert_eq!(predecessors.len(), graph.count_nodes());
        
        // All distances should be non-infinite (reachable from virtual source)
        for (i, dist) in distances.iter().enumerate() {
            let dist_f64 = dist.as_f64().unwrap();
            assert!(
                dist_f64.is_finite(),
                "Node index {} should have finite distance, got: {}",
                i,
                dist_f64
            );
        }
        
        // Check that reweighted edges would be non-negative
        // w'(u,v) = w(u,v) + h(u) - h(v) where h = distances from virtual source
        for edge in graph.edges() {
            let u_idx = edge.src().node.index();
            let v_idx = edge.dst().node.index();
            
            let h_u = distances[u_idx].as_f64().unwrap();
            let h_v = distances[v_idx].as_f64().unwrap();
            let w_uv = edge.properties().get("weight").unwrap().as_f64().unwrap();
            
            let reweighted = w_uv + h_u - h_v;
            
            assert!(
                reweighted >= -1e-10,
                "Reweighted edge {} -> {} should be non-negative: {} + {} - {} = {}",
                edge.src().name(),
                edge.dst().name(),
                w_uv,
                h_u,
                h_v,
                reweighted
            );
        }
    }

    #[test]
    fn test_bellman_ford_virtual_source_with_positive_weights() {
        let graph = load_graph(vec![
            (0, "A", "B", vec![("weight", 1.0f32)]),
            (1, "B", "C", vec![("weight", 2.0f32)]),
            (2, "A", "C", vec![("weight", 4.0f32)]),
        ]);

        let dist_val = Prop::F32(0.0);
        let max_val = Prop::F32(0.0);
        
        let weight_fn = |edge: &EdgeView<Graph>| -> Option<Prop> {
            edge.properties().get("weight")
        };

        let result = bellman_ford_single_source_shortest_paths_algorithm(
            &graph,
            None::<NodeRef>,
            Direction::OUT,
            dist_val,
            max_val,
            weight_fn,
        );

        assert!(result.is_ok());
        
        let (distances, _) = result.unwrap();
        
        // With all positive weights and virtual source at 0,
        // all distances should be <= 0 (since we're finding minimum distances)
        for dist in distances.iter() {
            let dist_f64 = dist.as_f64().unwrap();
            assert!(dist_f64 <= 1e-10, "Distance should be <= 0, got: {}", dist_f64);
        }
    }

    #[test]
    fn test_bellman_ford_virtual_source_detects_negative_cycle() {
        // Create a graph with a negative cycle
        let graph = load_graph(vec![
            (0, "A", "B", vec![("weight", 1.0f32)]),
            (1, "B", "C", vec![("weight", -2.0f32)]),
            (2, "C", "A", vec![("weight", -1.0f32)]),
        ]);

        let dist_val = Prop::F32(0.0);
        let max_val = Prop::F32(0.0);
        
        let weight_fn = |edge: &EdgeView<Graph>| -> Option<Prop> {
            edge.properties().get("weight")
        };

        let result = bellman_ford_single_source_shortest_paths_algorithm(
            &graph,
            None::<NodeRef>,
            Direction::OUT,
            dist_val,
            max_val,
            weight_fn,
        );

        assert!(result.is_err(), "Should detect negative cycle");
        
        if let Err(GraphError::InvalidProperty { reason }) = result {
            assert!(reason.contains("Negative cycle"));
        } else {
            panic!("Expected InvalidProperty error with negative cycle message");
        }
    }

    #[test]
    fn test_bellman_ford_virtual_source_with_integer_weights() {
        let edges = vec![
            (0, 1, 2, vec![("weight", 4i64)]),
            (1, 1, 3, vec![("weight", 2i64)]),
            (2, 2, 3, vec![("weight", -3i64)]),
            (3, 3, 4, vec![("weight", 2i64)]),
        ];

        let graph = Graph::new();
        for (t, src, dst, props) in edges {
            graph.add_edge(t, src, dst, props, None).unwrap();
        }

        let dist_val = Prop::I64(0);
        let max_val = Prop::I64(0);
        
        let weight_fn = |edge: &EdgeView<Graph>| -> Option<Prop> {
            edge.properties().get("weight")
        };

        let result = bellman_ford_single_source_shortest_paths_algorithm(
            &graph,
            None::<NodeRef>,
            Direction::OUT,
            dist_val,
            max_val,
            weight_fn,
        );

        assert!(result.is_ok());
        
        let (distances, _) = result.unwrap();
        
        // Check all distances are finite
        for dist in distances.iter() {
            let dist_f64 = dist.as_f64().unwrap();
            assert!(
                dist_f64.abs() < i64::MAX as f64 / 2.0,
                "Distance should be finite, got: {}",
                dist_f64
            );
        }
    }
}
