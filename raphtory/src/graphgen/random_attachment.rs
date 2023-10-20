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
    prelude::NO_PROPS,
};
use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};

/// Given a graph this function will add a user defined number of vertices, each with a
/// user defined number of edges.
/// This is an iterative algorithm where at each `step` a vertex is added and its neighbours
/// are chosen from the pool of nodes already within the network.
/// For this model the neighbours are chosen purely at random. This sampling is done
/// without replacement.
///
/// **Note:**  If the provided graph doesnt have enough nodes/edges for the initial sample,
/// the min number of both will be added before generation begins.
///
/// # Arguments
/// * `graph` - The graph you wish to add vertices and edges to
/// * `vertices_to_add` - The amount of vertices you wish to add to the graph (steps)
/// * `edges_per_step` - The amount of edges a joining vertex should add to the graph
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
    vertices_to_add: usize,
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
    let mut ids: Vec<u64> = graph.vertices().id().collect();
    let mut max_id = ids.iter().max().copied().unwrap_or(0);

    while ids.len() < edges_per_step {
        max_id += 1;
        latest_time += 1;
        graph
            .add_vertex(latest_time, max_id, NO_PROPS)
            .map_err(|err| println!("{:?}", err))
            .ok();
        ids.push(max_id);
    }

    for _ in 0..vertices_to_add {
        let edges = ids.choose_multiple(rng, edges_per_step);
        max_id += 1;
        latest_time += 1;
        edges.for_each(|neighbour| {
            graph
                .add_edge(latest_time, max_id, *neighbour, NO_PROPS, None)
                .expect("Not able to add edge");
        });
        ids.push(max_id);
    }
}

#[cfg(test)]
mod random_graph_test {
    use super::*;
    use crate::graphgen::preferential_attachment::ba_preferential_attachment;
    #[test]
    fn blank_graph() {
        let graph = Graph::new();
        random_attachment(&graph, 100, 20, None);
        assert_eq!(graph.count_edges(), 2000);
        assert_eq!(graph.count_vertices(), 120);
    }

    #[test]
    fn only_nodes() {
        let graph = Graph::new();
        for i in 0..10 {
            graph
                .add_vertex(i, i as u64, NO_PROPS)
                .map_err(|err| println!("{:?}", err))
                .ok();
        }

        random_attachment(&graph, 1000, 5, None);
        assert_eq!(graph.count_edges(), 5000);
        assert_eq!(graph.count_vertices(), 1010);
    }

    #[test]
    fn prior_graph() {
        let graph = Graph::new();
        ba_preferential_attachment(&graph, 300, 7, None);
        random_attachment(&graph, 4000, 12, None);
        assert_eq!(graph.count_edges(), 50106);
        assert_eq!(graph.count_vertices(), 4307);
    }
}
