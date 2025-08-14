//! This module contains the preferential attachment graph generation model.
//!
//! This function is a graph generation model based upon:
//! Barabási, Albert-László, and Réka Albert. "Emergence of scaling in random networks." science 286.5439 (1999): 509-512.
//! # Examples
//!
//! ```
//! use raphtory::prelude::*;
//! use raphtory::graphgen::preferential_attachment::ba_preferential_attachment;
//!
//! let graph = Graph::new();
//! ba_preferential_attachment(&graph, 1000, 10, None);
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
use raphtory_api::core::storage::timeindex::AsTime;
use std::collections::HashSet;
use tracing::error;

/// Generates a graph using the preferential attachment model.
///
/// Given a graph this function will add a user defined number of nodes, each with a user
/// defined number of edges.
/// This is an iterative algorithm where at each `step` a node is added and its neighbours are
/// chosen from the pool of nodes already within the network.
/// For this model the neighbours are chosen proportionally based upon their degree, favouring
/// nodes with higher degree (more connections).
/// This sampling is conducted without replacement.
///
/// **Note:**  If the provided graph doesnt have enough nodes/edges for the initial sample,
/// the min number of both will be added before generation begins.
///
/// # Arguments
/// * `graph` - The graph you wish to add nodes and edges to
/// * `nodes_to_add` - The amount of nodes you wish to add to the graph (steps)
/// * `edges_per_step` - The amount of edges a joining node should add to the graph
/// * `seed` - an optional byte array for the seed used in rng, can be None
/// # Examples
///
/// ```
/// use raphtory::prelude::*;
/// use raphtory::graphgen::preferential_attachment::ba_preferential_attachment;
///
/// let graph = Graph::new();
/// ba_preferential_attachment(&graph, 1000, 10, None);
/// ```
pub fn ba_preferential_attachment(
    graph: &Graph,
    nodes_to_add: usize,
    edges_per_step: usize,
    seed: Option<[u8; 32]>,
) {
    let mut rng: StdRng;
    if let Some(seed_value) = seed {
        rng = StdRng::from_seed(seed_value);
    } else {
        rng = StdRng::from_entropy();
    }
    let mut latest_time = graph.latest_time().map_or(0, |t| t.t());
    let view = graph;
    let mut ids = graph.nodes().id().iter_values().collect::<Vec<_>>();
    let mut degrees: Vec<usize> = view.nodes().degree().iter_values().collect();
    let mut edge_count: usize = degrees.iter().sum();

    let mut max_id = next_id(view, ids.iter().max().cloned());

    while ids.len() < edges_per_step {
        max_id = next_id(view, Some(max_id));
        graph
            .add_node(latest_time, &max_id, NO_PROPS, None)
            .map_err(|err| error!("{:?}", err))
            .ok();
        degrees.push(0);
        ids.push(max_id.clone());
    }

    if graph.count_edges() < edges_per_step {
        for pos in 1..ids.len() {
            graph
                .add_edge(latest_time, &ids[pos], &ids[pos - 1], NO_PROPS, None)
                .expect("Not able to add edge");
            edge_count += 2;
            degrees[pos] += 1;
            degrees[pos - 1] += 1;
        }
    }

    for _ in 0..nodes_to_add {
        max_id = next_id(view, Some(max_id));
        latest_time += 1;
        let mut normalisation = edge_count;
        let mut positions_to_skip: HashSet<usize> = HashSet::new();

        for _ in 0..edges_per_step {
            let mut sum = 0;
            let rand_num = rng.gen_range(1..=normalisation);
            for pos in 0..ids.len() {
                if !positions_to_skip.contains(&pos) {
                    sum += degrees[pos];
                    if sum >= rand_num {
                        positions_to_skip.insert(pos);
                        normalisation -= degrees[pos];
                        break;
                    }
                }
            }
        }
        for pos in positions_to_skip {
            let dst = &ids[pos];
            degrees[pos] += 1;
            graph
                .add_edge(latest_time, &max_id, dst, NO_PROPS, None)
                .expect("Not able to add edge");
        }
        ids.push(max_id.clone());
        degrees.push(edges_per_step);
        edge_count += edges_per_step * 2;
    }
}

//TODO need to benchmark the creation of these networks
#[cfg(test)]
mod preferential_attachment_tests {
    use super::*;
    use crate::graphgen::random_attachment::random_attachment;
    use raphtory_api::core::utils::logging::global_info_logger;
    #[test]
    fn blank_graph() {
        let graph = Graph::new();
        ba_preferential_attachment(&graph, 1000, 10, None);
        assert_eq!(graph.count_edges(), 10009);
        assert_eq!(graph.count_nodes(), 1010);
    }

    #[test]
    fn only_nodes() {
        global_info_logger();
        let graph = Graph::new();
        for i in 0..10 {
            graph
                .add_node(i, i as u64, NO_PROPS, None)
                .map_err(|err| error!("{:?}", err))
                .ok();
        }

        ba_preferential_attachment(&graph, 1000, 5, None);
        assert_eq!(graph.count_edges(), 5009);
        assert_eq!(graph.count_nodes(), 1010);
    }

    #[test]
    fn prior_graph() {
        let graph = Graph::new();
        random_attachment(&graph, 1000, 3, None);
        ba_preferential_attachment(&graph, 500, 4, None);
        assert_eq!(graph.count_edges(), 5000);
        assert_eq!(graph.count_nodes(), 1503);
    }
}
