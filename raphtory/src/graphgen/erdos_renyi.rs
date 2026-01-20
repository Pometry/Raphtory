//! Generates a graph using the erodos renyl model
//!
//! # Examples
//!
//! ```
//! use raphtory::prelude::*;
//! use raphtory::graphgen::erdos_renyi::erdos_renyi;
//! let graph = Graph::new();
//! erdos_renyi(&graph, 1000, 0.1, None);
//! ```

use super::next_id;
use crate::{
    db::{
        api::{mutation::AdditionOps, view::*},
        graph::graph::Graph,
    },
    prelude::{NO_PROPS, NodeStateOps},
};
use rand::{rngs::StdRng, Rng, SeedableRng};
use raphtory_api::core::storage::timeindex::AsTime;
use raphtory_core::entities::GID;
use rayon::iter::ParallelIterator;
use tracing::error;

/// Generates an Erdos-Renyi random graph in the provided `graph`.
///
/// # Arguments
/// * `graph` - The graph to populate with nodes and edges.
/// * `n_nodes` - Number of nodes to create in the graph.
/// * `p` - Probability of edge creation between any two nodes (0.0 = no edges, 1.0 = fully connected).
/// * `seed` - Optional 32-byte seed for deterministic random generation. If `None`, uses entropy.
///
/// # Behavior
/// - Adds `n_nodes` nodes to the graph.
/// - For each pair of distinct nodes, adds a directed edge with probability `p`.
/// - Uses the provided seed for reproducibility if given.
///
/// # Example
/// ```
/// let graph = Graph::new();
/// erdos_renyi(&graph, 10, 0.2, None);
/// ```
pub fn erdos_renyi(graph: &Graph, nodes_to_add: usize, p: f64, seed: Option<u64>) {
    let mut rng;
    if let Some(seed_value) = seed {
        rng = StdRng::seed_from_u64(seed_value);
    } else {
        rng = StdRng::from_entropy();
    }
    let mut ids = graph.nodes().id().par_iter_values().collect::<Vec<GID>>();
    let mut latest_time = graph.latest_time().map_or(0, |t| t.t());
    let mut max_id = next_id(graph, graph.nodes().id().iter_values().max());
    for _ in 0..nodes_to_add {
        max_id = next_id(graph, Some(max_id));
        latest_time += 1;
        graph
            .add_node(latest_time, &max_id, NO_PROPS, None)
            .map_err(|err| error!("{:?}", err))
            .ok();
        ids.push(max_id.clone()); 
    }
    for i in 0..ids.len() {
        for j in (i + 1)..ids.len() {
            let create_edge = rng.gen_bool(p);  
            if create_edge {
                latest_time += 1;
                graph.add_edge(latest_time, &ids[i], &ids[j], NO_PROPS, None).expect("Not able to add edge");
            }
        }
    }  
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;

    #[test]
    fn test_erdos_renyi_small_graph() {
        let graph = Graph::new();
        let n_nodes = 20;
        let p = 0.5;
        let seed = Some(42);
        erdos_renyi(&graph, n_nodes, p, seed);
        let node_count = graph.nodes().id().iter_values().count();
        assert_eq!(node_count, n_nodes);
        let edge_count = graph.edges().into_iter().count();
        assert!(edge_count > 0);
        assert!(edge_count <= n_nodes * (n_nodes - 1));
    }

    #[test]
    fn test_erdos_renyi_zero_probability() {
        let graph = Graph::new();
        let n_nodes = 20;
        let p = 0.0;
        let seed = Some(42); 
        erdos_renyi(&graph, n_nodes, p, seed);
        let edge_count = graph.edges().into_iter().count();
        assert_eq!(edge_count, 0);
    }

    #[test]
    fn test_erdos_renyi_full_probability() {
        let graph = Graph::new();
        let n_nodes = 20;
        let p = 1.0;
        let seed = Some(42);
        erdos_renyi(&graph, n_nodes, p, seed);
        let edge_count = graph.edges().into_iter().count();
        assert_eq!(edge_count, (n_nodes * (n_nodes - 1))/2);
    }

    
}