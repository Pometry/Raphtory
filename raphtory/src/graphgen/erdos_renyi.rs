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
    prelude::{NodeStateOps, NO_PROPS},
};
use rand::{rngs::StdRng, Rng, SeedableRng};
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
pub fn erdos_renyi(graph: &Graph, n_nodes: usize, p: f64, seed: Option<[u8; 32]>) {
    let mut rng: StdRng;
    if let Some(seed_value) = seed {
        rng = StdRng::from_seed(seed_value);
    } else {
        rng = StdRng::from_entropy();
    }
    let mut latest_time = graph.latest_time().unwrap_or(0);
    let ids = graph.nodes().id().iter_values().collect::<Vec<_>>();
    let mut max_id = next_id(graph, ids.iter().max().cloned());
    for _ in 0..n_nodes {
        max_id = next_id(graph, Some(max_id));
        latest_time += 1;
        graph
            .add_node(latest_time, &max_id, NO_PROPS, None)
            .map_err(|err| error!("{:?}", err))
            .ok();
    }
    let all_ids = graph.nodes().id().iter_values().collect::<Vec<_>>();
    for id in all_ids.iter() {
        for other_id in all_ids.iter() {
            if id != other_id && rng.gen::<f64>() < p {
                latest_time += 1;
                graph
                    .add_edge(latest_time, id, other_id, NO_PROPS, None)
                    .expect("Not able to add edge");
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
        let n_nodes = 5;
        let p = 0.5;
        let seed = Some([1u8; 32]);
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
        let n_nodes = 4;
        let p = 0.0;
        let seed = Some([2u8; 32]);
        erdos_renyi(&graph, n_nodes, p, seed);
        let edge_count = graph.edges().into_iter().count();
        assert_eq!(edge_count, 0);
    }

    #[test]
    fn test_erdos_renyi_full_probability() {
        let graph = Graph::new();
        let n_nodes = 3;
        let p = 1.0;
        let seed = Some([3u8; 32]);
        erdos_renyi(&graph, n_nodes, p, seed);
        let edge_count = graph.edges().into_iter().count();
        assert_eq!(edge_count, n_nodes * (n_nodes - 1));
    }
}
