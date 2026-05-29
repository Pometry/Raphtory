//! Generates a graph using the Erdős-Rényi model
//!
//! # Examples
//!
//! ```
//! use raphtory::graphgen::erdos_renyi::erdos_renyi;
//! let graph = erdos_renyi(1000, 0.1, None).unwrap();
//! ```

use crate::{
    db::{
        api::{mutation::AdditionOps, view::*},
        graph::graph::Graph,
    },
    errors::GraphError,
    prelude::NO_PROPS,
};
use rand::{rngs::StdRng, Rng, SeedableRng};
use raphtory_api::core::storage::timeindex::AsTime;
use raphtory_core::entities::GID;

/// Generates an Erdős-Rényi random graph and returns it.
///
/// The Erdős-Rényi model creates a random graph by connecting each pair of nodes
/// with a given probability. This implementation creates an undirected graph.
///
/// # Arguments
/// * `nodes_to_add` - Number of nodes to create in the graph.
/// * `p` - Probability of edge creation between any two nodes (0.0 = no edges, 1.0 = fully connected).
/// * `seed` - Optional 64-bit seed for deterministic random generation. If `None`, uses entropy.
///
/// # Returns
/// * `Result<Graph, GraphError>` - A new graph with the generated nodes and edges.
///
/// # Behavior
/// - Creates a new graph and adds `nodes_to_add` nodes with sequential u64 IDs (0, 1, 2, ...).
/// - For each pair of distinct nodes, adds an undirected edge with probability `p`.
/// - Uses the provided seed for reproducible random generation if given.
/// - All nodes and edges are timestamped with incrementing time values.
///
/// # Example
/// ```
/// use raphtory::graphgen::erdos_renyi::erdos_renyi;
///
/// // Create a random graph with 10 nodes and 20% edge probability
/// let graph = erdos_renyi(10, 0.2, Some(42)).unwrap();
/// ```
pub fn erdos_renyi(nodes_to_add: usize, p: f64, seed: Option<u64>) -> Result<Graph, GraphError> {
    let graph = Graph::new();
    let mut rng;
    if let Some(seed_value) = seed {
        rng = StdRng::seed_from_u64(seed_value);
    } else {
        rng = StdRng::from_os_rng();
    }
    let mut latest_time = graph.latest_time().map_or(0, |t| t.t());
    for i in 0..nodes_to_add {
        let id = GID::U64(i as u64);
        latest_time += 1;
        graph.add_node(latest_time, &id, NO_PROPS, None, None)?;
    }
    for i in 0..nodes_to_add {
        let source_id = GID::U64(i as u64);
        for j in (i + 1)..nodes_to_add {
            let dst_id = GID::U64(j as u64);
            let create_edge = rng.random_bool(p);
            if create_edge {
                latest_time += 1;
                graph.add_edge(latest_time, &source_id, &dst_id, NO_PROPS, None)?;
                graph.add_edge(latest_time, &dst_id, &source_id, NO_PROPS, None)?;
            }
        }
    }
    Ok(graph)
}

#[cfg(test)]
mod tests {
    use crate::{graphgen::erdos_renyi::erdos_renyi, prelude::*};

    #[test]
    fn test_erdos_renyi_half_probability() {
        let n_nodes = 20;
        let p = 0.5;
        let seed = Some(42);
        let graph = erdos_renyi(n_nodes, p, seed).unwrap();
        let node_count = graph.nodes().id().iter_values().count();
        let edge_count = graph.edges().into_iter().count();
        assert_eq!(node_count, n_nodes);
        assert!(edge_count > 0);
        assert!(edge_count <= n_nodes * (n_nodes - 1));
    }

    #[test]
    fn test_erdos_renyi_zero_probability() {
        let n_nodes = 20;
        let p = 0.0;
        let seed = Some(42);
        let graph = erdos_renyi(n_nodes, p, seed).unwrap();
        let edge_count = graph.edges().into_iter().count();
        let node_count = graph.nodes().id().iter_values().count();
        assert_eq!(node_count, n_nodes);
        assert_eq!(edge_count, 0);
    }

    #[test]
    fn test_erdos_renyi_full_probability() {
        let n_nodes = 20;
        let p = 1.0;
        let seed = Some(42);
        let graph = erdos_renyi(n_nodes, p, seed).unwrap();
        let edge_count = graph.edges().into_iter().count();
        let node_count = graph.nodes().id().iter_values().count();
        assert_eq!(node_count, n_nodes);
        assert_eq!(edge_count, (n_nodes * (n_nodes - 1)));
    }
}
