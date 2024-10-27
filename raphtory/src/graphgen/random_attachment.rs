//! Generates a graph using the random attachment model
//!
//! This function is a graph generation model based upon:
//! Callaway, Duncan S., et al. "Are randomly grown graphs really random?."
//! Physical Review E 64.4 (2001): 041902.
//!
//! # Examples
//!
//! ```
//! use raphtory::prelude::*;
//! use raphtory::graphgen::random_attachment::random_attachment;
//! let graph = Graph::new();
//! random_attachment(&graph, 1000, 10, None);
//! ```

use crate::{
    db::{
        api::{mutation::AdditionOps, view::*},
        graph::graph::Graph,
    },
    prelude::{NodeStateOps, NO_PROPS},
};
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
use tracing::error;

use super::next_id;

/// Given a graph this function will add a user defined number of nodes, each with a
/// user defined number of edges.
/// This is an iterative algorithm where at each `step` a node is added and its neighbours
/// are chosen from the pool of nodes already within the network.
/// For this model the neighbours are chosen purely at random. This sampling is done
/// without replacement.
///
/// **Note:**  If the provided graph doesnt have enough nodes/edges for the initial sample,
/// the min number of both will be added before generation begins.
///
/// # Arguments
/// * `graph` - The graph you wish to add nodes and edges to
/// * `nodes_to_add` - The amount of nodes you wish to add to the graph (steps)
/// * `edges_per_step` - The amount of edges a joining node should add to the graph
/// * `seed` - (Optional) An array of u8 bytes to be used as the input seed, Default None
/// # Examples
///
/// ```
/// use raphtory::prelude::*;
/// use raphtory::graphgen::random_attachment::random_attachment;
/// let graph = Graph::new();
/// random_attachment(&graph, 1000, 10, None);
/// ```
pub fn random_attachment(
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
    let mut latest_time = graph.latest_time().unwrap_or(0);
    let mut ids = graph.nodes().id().values().collect::<Vec<_>>();
    let mut max_id = next_id(graph, ids.iter().max().cloned());

    while ids.len() < edges_per_step {
        max_id = next_id(graph, Some(max_id));
        latest_time += 1;
        graph
            .add_node(latest_time, &max_id, NO_PROPS, None)
            .map_err(|err| error!("{:?}", err))
            .ok();
        ids.push(max_id.clone());
    }

    for _ in 0..nodes_to_add {
        let edges = ids.choose_multiple(&mut rng, edges_per_step);
        max_id = next_id(graph, Some(max_id));
        latest_time += 1;
        edges.for_each(|neighbour| {
            graph
                .add_edge(latest_time, &max_id, neighbour, NO_PROPS, None)
                .expect("Not able to add edge");
        });
        ids.push(max_id.clone());
    }
}

#[cfg(test)]
mod random_graph_test {
    use super::*;
    use crate::graphgen::preferential_attachment::ba_preferential_attachment;
    use raphtory_api::core::utils::logging::global_info_logger;
    #[test]
    fn blank_graph() {
        let graph = Graph::new();
        random_attachment(&graph, 100, 20, None);
        assert_eq!(graph.count_edges(), 2000);
        assert_eq!(graph.count_nodes(), 120);
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

        random_attachment(&graph, 1000, 5, None);
        assert_eq!(graph.count_edges(), 5000);
        assert_eq!(graph.count_nodes(), 1010);
    }

    #[test]
    fn prior_graph() {
        let graph = Graph::new();
        ba_preferential_attachment(&graph, 300, 7, None);
        random_attachment(&graph, 4000, 12, None);
        assert_eq!(graph.count_edges(), 50106);
        assert_eq!(graph.count_nodes(), 4307);
    }
}
