use crate::{
    algorithms::algorithm_result::AlgorithmResult,
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
    let mut betweenness: HashMap<u64, f64> = HashMap::new();

    // Get the vertices and the total number of vertices in the graph.
    let nodes = g.vertices();
    let n = g.count_vertices();
    let k_sample = k.unwrap_or(n);

    // Main loop over each node to compute betweenness centrality.
    for node in nodes.iter().take(k_sample) {
        let mut stack = Vec::new();
        let mut predecessors: HashMap<u64, Vec<u64>> = HashMap::new();
        let mut sigma: HashMap<u64, f64> = HashMap::new();
        let mut dist: HashMap<u64, i64> = HashMap::new();
        let mut queue = VecDeque::new();

        // Initialize distance and sigma values for each node.
        for node in nodes.iter() {
            dist.insert(node.id(), -1);
            sigma.insert(node.id(), 0.0);
        }
        dist.insert(node.id(), 0);
        sigma.insert(node.id(), 1.0);
        queue.push_back(node.id());

        // BFS loop to find shortest paths.
        while let Some(current_node_id) = queue.pop_front() {
            stack.push(current_node_id);
            for neighbor in g.vertex(current_node_id).unwrap().out_neighbours() {
                // Path discovery
                if dist[&neighbor.id()] < 0 {
                    queue.push_back(neighbor.id());
                    dist.insert(neighbor.id(), dist[&current_node_id] + 1);
                }
                // Path counting
                if dist[&neighbor.id()] == dist[&current_node_id] + 1 {
                    sigma.insert(
                        neighbor.id(),
                        sigma[&neighbor.id()] + sigma[&current_node_id],
                    );
                    predecessors
                        .entry(neighbor.id())
                        .or_insert_with(Vec::new)
                        .push(current_node_id);
                }
            }
        }

        let mut delta: HashMap<u64, f64> = HashMap::new();
        for node in nodes.iter() {
            delta.insert(node.id(), 0.0);
        }

        // Accumulation
        while let Some(w) = stack.pop() {
            for v in predecessors.get(&w).unwrap_or(&Vec::new()) {
                let coeff = (sigma[v] / sigma[&w]) * (1.0 + delta[&w]);
                let new_delta_v = delta[v] + coeff;
                delta.insert(*v, new_delta_v);
            }
            if w != node.id() {
                let updated_betweenness = betweenness.entry(w).or_insert(0.0);
                *updated_betweenness += delta[&w];
            }
        }
    }

    // Normalization
    if let Some(true) = normalized {
        let factor = 1.0 / ((n as f64 - 1.0) * (n as f64 - 2.0));
        for node in nodes.iter() {
            if betweenness.contains_key(&node.id()) {
                betweenness.insert(node.id(), betweenness[&node.id()] * factor);
            } else {
                betweenness.insert(node.id(), 0.0f64);
            }
        }
    } else {
        for node in nodes.iter() {
            if !betweenness.contains_key(&node.id()) {
                betweenness.insert(node.id(), 0.0f64);
            }
        }
    }

    // Construct and return the AlgorithmResult
    let results_type = std::any::type_name::<f64>();
    let result = betweenness
        .into_iter()
        .map(|(k, v)| (g.vertex(k).unwrap().vertex.0, v))
        .collect();
    AlgorithmResult::new(g.clone(), "Betweenness", results_type, result)
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
        assert_eq!(res.get_with_names(), expected);

        let mut expected: HashMap<String, Option<f64>> = HashMap::new();
        expected.insert("1".to_string(), Some(0.0));
        expected.insert("2".to_string(), Some(0.05));
        expected.insert("3".to_string(), Some(0.2));
        expected.insert("4".to_string(), Some(0.05));
        expected.insert("5".to_string(), Some(0.0));
        expected.insert("6".to_string(), Some(0.0));
        let res = betweenness_centrality(&graph, None, Some(true));
        assert_eq!(res.get_with_names(), expected);
    }
}
