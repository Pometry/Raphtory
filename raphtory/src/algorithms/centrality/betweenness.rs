use crate::{
    algorithms::algorithm_result::AlgorithmResult,
    core::entities::VID,
    db::graph::vertex::VertexView,
    prelude::{GraphViewOps, VertexViewOps},
};
use ordered_float::OrderedFloat;
use std::collections::{HashMap, VecDeque};

/// Computes the betweenness centrality for nodes in a given graph.
///
/// # Parameters
///
/// - `g`: A reference to the graph.
/// - `k`: An `Option<usize>` specifying the number of nodes to consider for the centrality computation. Defaults to all nodes if `None`.
/// - `normalized`: A `Option<bool>` indicating whether to normalize the centrality values.
///
/// # Returns
///
/// Returns an `AlgorithmResult` containing the betweenness centrality of each node.
pub fn betweenness_centrality<G: GraphViewOps>(
    g: &G,
    k: Option<usize>,
    normalized: Option<bool>,
) -> AlgorithmResult<G, f64, OrderedFloat<f64>> {
    // Initialize a hashmap to store betweenness centrality values.
    let mut betweenness: HashMap<usize, f64> = HashMap::new();

    // Get the vertices and the total number of vertices in the graph.
    let nodes = g.vertices();
    let n = g.count_vertices();
    let k_sample = k.unwrap_or(n);

    // Main loop over each node to compute betweenness centrality.
    for node in nodes.iter().take(k_sample) {
        let mut stack = Vec::new();
        let mut predecessors: HashMap<usize, Vec<usize>> = HashMap::new();
        let mut sigma: HashMap<usize, f64> = HashMap::new();
        let mut dist: HashMap<usize, i64> = HashMap::new();
        let mut queue = VecDeque::new();

        // Initialize distance and sigma values for each node.
        for node in nodes.iter() {
            dist.insert(node.vertex.0, -1);
            sigma.insert(node.vertex.0, 0.0);
        }
        dist.insert(node.vertex.0, 0);
        sigma.insert(node.vertex.0, 1.0);
        queue.push_back(node.vertex.0);

        // BFS loop to find shortest paths.
        while let Some(current_node_id) = queue.pop_front() {
            stack.push(current_node_id);
            for neighbor in
                VertexView::new_internal(g.clone(), VID::from(current_node_id)).out_neighbours()
            {
                // Path discovery
                if dist[&neighbor.vertex.0] < 0 {
                    queue.push_back(neighbor.vertex.0);
                    dist.insert(neighbor.vertex.0, dist[&current_node_id] + 1);
                }
                // Path counting
                if dist[&neighbor.vertex.0] == dist[&current_node_id] + 1 {
                    sigma.insert(
                        neighbor.vertex.0,
                        sigma[&neighbor.vertex.0] + sigma[&current_node_id],
                    );
                    predecessors
                        .entry(neighbor.vertex.0)
                        .or_insert_with(Vec::new)
                        .push(current_node_id);
                }
            }
        }

        let mut delta: HashMap<usize, f64> = HashMap::new();
        for node in nodes.iter() {
            delta.insert(node.vertex.0, 0.0);
        }

        // Accumulation
        while let Some(w) = stack.pop() {
            for v in predecessors.get(&w).unwrap_or(&Vec::new()) {
                let coeff = (sigma[v] / sigma[&w]) * (1.0 + delta[&w]);
                let new_delta_v = delta[v] + coeff;
                delta.insert(*v, new_delta_v);
            }
            if w != node.vertex.0 {
                let updated_betweenness = betweenness.entry(w).or_insert(0.0);
                *updated_betweenness += delta[&w];
            }
        }
    }

    // Normalization
    if let Some(true) = normalized {
        let factor = 1.0 / ((n as f64 - 1.0) * (n as f64 - 2.0));
        for node in nodes.iter() {
            if betweenness.contains_key(&node.vertex.0) {
                betweenness.insert(node.vertex.0, betweenness[&node.vertex.0] * factor);
            } else {
                betweenness.insert(node.vertex.0, 0.0f64);
            }
        }
    } else {
        for node in nodes.iter() {
            if !betweenness.contains_key(&node.vertex.0) {
                betweenness.insert(node.vertex.0, 0.0f64);
            }
        }
    }

    // Construct and return the AlgorithmResult
    let results_type = std::any::type_name::<f64>();
    AlgorithmResult::new(g.clone(), "Betweenness", results_type, betweenness)
}

#[cfg(test)]
mod betweenness_centrality_test {
    use super::*;
    use crate::prelude::*;

    #[test]
    fn test_betweenness_centrality() {
        let graph = Graph::new();
        let vs = vec![
            (1, 2),
            (1, 3),
            (1, 4),
            (2, 3),
            (2, 4),
            (2, 5),
            (3, 4),
            (3, 5),
            (3, 6),
            (4, 3),
            (4, 2),
            (4, 4),
        ];
        for (src, dst) in &vs {
            graph.add_edge(0, *src, *dst, NO_PROPS, None).unwrap();
        }
        let mut expected: HashMap<String, Option<f64>> = HashMap::new();
        expected.insert("1".to_string(), Some(0.0));
        expected.insert("2".to_string(), Some(1.0));
        expected.insert("3".to_string(), Some(4.0));
        expected.insert("4".to_string(), Some(1.0));
        expected.insert("5".to_string(), Some(0.0));
        expected.insert("6".to_string(), Some(0.0));

        let res = betweenness_centrality(&graph, None, Some(false));
        assert_eq!(res.get_all_with_names(), expected);

        let mut expected: HashMap<String, Option<f64>> = HashMap::new();
        expected.insert("1".to_string(), Some(0.0));
        expected.insert("2".to_string(), Some(0.05));
        expected.insert("3".to_string(), Some(0.2));
        expected.insert("4".to_string(), Some(0.05));
        expected.insert("5".to_string(), Some(0.0));
        expected.insert("6".to_string(), Some(0.0));
        let res = betweenness_centrality(&graph, None, Some(true));
        assert_eq!(res.get_all_with_names(), expected);
    }
}
