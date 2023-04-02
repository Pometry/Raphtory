//! Generates a graph using the random attachment model
//!
//! This function is a graph generation model based upon:
//! Callaway, Duncan S., et al. "Are randomly grown graphs really random?."
//! Physical Review E 64.4 (2001): 041902.
//!
//! # Examples
//!
//! ```
//! use docbrown_db::graph::Graph;
//! use docbrown_db::graphgen::random_attachment::random_attachment;
//! let graph = Graph::new(2);
//! random_attachment(&graph, 1000, 10);
//! ```

use crate::graph::Graph;
use crate::view_api::*;
use docbrown_core::tgraph_shard::errors::GraphError;
use rand::seq::SliceRandom;

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
/// # Examples
///
/// ```
/// use docbrown_db::graph::Graph;
/// use docbrown_db::graphgen::random_attachment::random_attachment;
/// let graph = Graph::new(2);
/// random_attachment(&graph, 1000, 10);
/// ```
pub fn random_attachment(graph: &Graph, vertices_to_add: usize, edges_per_step: usize) {
    let rng = &mut rand::thread_rng();
    let mut latest_time = match graph.latest_time() {
        None => 0,
        Some(time) => time,
    };
    let mut ids: Vec<u64> = graph.vertices().id().collect();
    let mut max_id = match ids.iter().max() {
        Some(id) => *id,
        None => 0,
    };

    while ids.len() < edges_per_step {
        max_id += 1;
        latest_time += 1;
        graph
            .add_vertex(latest_time, max_id, &vec![])
            .map_err(|err| println!("{:?}", err))
            .ok();
        ids.push(max_id);
    }

    for _ in 0..vertices_to_add {
        let edges = ids.choose_multiple(rng, edges_per_step);
        max_id += 1;
        latest_time += 1;
        edges.for_each(|neighbour| {
            graph.add_edge(latest_time, max_id, *neighbour, &vec![]);
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
        let graph = Graph::new(2);
        random_attachment(&graph, 100, 20);
        assert_eq!(graph.num_edges(), 2000);
        assert_eq!(graph.num_vertices(), 120);
    }

    #[test]
    fn only_nodes() {
        let graph = Graph::new(2);
        for i in 0..10 {
            graph
                .add_vertex(i, i as u64, &vec![])
                .map_err(|err| println!("{:?}", err))
                .ok();
        }

        random_attachment(&graph, 1000, 5);
        assert_eq!(graph.num_edges(), 5000);
        assert_eq!(graph.num_vertices(), 1010);
    }

    #[test]
    fn prior_graph() {
        let graph = Graph::new(2);
        ba_preferential_attachment(&graph, 300, 7);
        random_attachment(&graph, 4000, 12);
        assert_eq!(graph.num_edges(), 50106);
        assert_eq!(graph.num_vertices(), 4307);
    }
}
