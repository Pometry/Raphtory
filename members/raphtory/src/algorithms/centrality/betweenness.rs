use crate::{
    core::entities::VID,
    db::{api::state::NodeState, graph::node::NodeView},
    prelude::{GraphViewOps, NodeViewOps},
};
use std::collections::{HashMap, VecDeque};

/// Computes the betweenness centrality for nodes in a given graph.
///
/// # Arguments
///
/// - `g`: A reference to the graph.
/// - `k`: An `Option<usize>` specifying the number of nodes to consider for the centrality computation. Defaults to all nodes if `None`.
/// - `normalized`: If `true` normalize the centrality values.
///
/// # Returns
///
/// A NodeState containing the betweenness centrality of each node.
pub fn betweenness_centrality<'graph, G: GraphViewOps<'graph>>(
    g: &G,
    k: Option<usize>,
    normalized: bool,
) -> NodeState<'graph, f64, G> {
    // Initialize a hashmap to store betweenness centrality values.
    let mut betweenness: Vec<f64> = vec![0.0; g.unfiltered_num_nodes()];

    // Get the nodes and the total number of nodes in the graph.
    let nodes = g.nodes();
    let n = g.count_nodes();
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
            dist.insert(node.node.0, -1);
            sigma.insert(node.node.0, 0.0);
        }
        dist.insert(node.node.0, 0);
        sigma.insert(node.node.0, 1.0);
        queue.push_back(node.node.0);

        // BFS loop to find shortest paths.
        while let Some(current_node_id) = queue.pop_front() {
            stack.push(current_node_id);
            for neighbor in
                NodeView::new_internal(g.clone(), VID::from(current_node_id)).out_neighbours()
            {
                // Path discovery
                if dist[&neighbor.node.0] < 0 {
                    queue.push_back(neighbor.node.0);
                    dist.insert(neighbor.node.0, dist[&current_node_id] + 1);
                }
                // Path counting
                if dist[&neighbor.node.0] == dist[&current_node_id] + 1 {
                    sigma.insert(
                        neighbor.node.0,
                        sigma[&neighbor.node.0] + sigma[&current_node_id],
                    );
                    predecessors
                        .entry(neighbor.node.0)
                        .or_default()
                        .push(current_node_id);
                }
            }
        }

        let mut delta: HashMap<usize, f64> = HashMap::new();
        for node in nodes.iter() {
            delta.insert(node.node.0, 0.0);
        }

        // Accumulation
        while let Some(w) = stack.pop() {
            for v in predecessors.get(&w).unwrap_or(&Vec::new()) {
                let coeff = (sigma[v] / sigma[&w]) * (1.0 + delta[&w]);
                let new_delta_v = delta[v] + coeff;
                delta.insert(*v, new_delta_v);
            }
            if w != node.node.0 {
                betweenness[w] += delta[&w];
            }
        }
    }

    // Normalization
    if normalized {
        let factor = 1.0 / ((n as f64 - 1.0) * (n as f64 - 2.0));
        for node in nodes.iter() {
            betweenness[node.node.index()] *= factor;
        }
    }

    NodeState::new_from_eval(g.clone(), betweenness)
}

#[cfg(test)]
mod betweenness_centrality_test {
    use super::*;
    use crate::{prelude::*, test_storage};

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

        test_storage!(&graph, |graph| {
            let mut expected: HashMap<String, f64> = HashMap::new();
            expected.insert("1".to_string(), 0.0);
            expected.insert("2".to_string(), 1.0);
            expected.insert("3".to_string(), 4.0);
            expected.insert("4".to_string(), 1.0);
            expected.insert("5".to_string(), 0.0);
            expected.insert("6".to_string(), 0.0);

            let res = betweenness_centrality(graph, None, false);
            assert_eq!(res, expected);

            let mut expected: HashMap<String, f64> = HashMap::new();
            expected.insert("1".to_string(), 0.0);
            expected.insert("2".to_string(), 0.05);
            expected.insert("3".to_string(), 0.2);
            expected.insert("4".to_string(), 0.05);
            expected.insert("5".to_string(), 0.0);
            expected.insert("6".to_string(), 0.0);
            let res = betweenness_centrality(graph, None, true);
            assert_eq!(res, expected);
        });
    }
}
