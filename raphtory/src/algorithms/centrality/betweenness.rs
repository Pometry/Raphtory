use crate::{
    core::entities::VID,
    db::{
        api::state::{Index, NodeState},
        graph::node::NodeView,
    },
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
    let index = Index::for_graph(g);
    // Initialize a hashmap to store betweenness centrality values.
    let mut betweenness: Vec<f64> = vec![0.0; g.count_nodes()];

    // Get the nodes and the total number of nodes in the graph.
    let nodes = g.nodes();
    let n = g.count_nodes();
    let k_sample = k.unwrap_or(n);

    // Main loop over each node to compute betweenness centrality.
    for node in nodes.iter().take(k_sample) {
        let mut stack: Vec<VID> = Vec::new();
        let mut predecessors: HashMap<VID, Vec<VID>> = HashMap::new();
        let mut sigma: HashMap<VID, f64> = HashMap::new();
        let mut dist: HashMap<VID, i64> = HashMap::new();
        let mut queue = VecDeque::new();

        // Initialize distance and sigma values for each node.
        for node in nodes.iter() {
            dist.insert(node.node, -1);
            sigma.insert(node.node, 0.0);
        }
        dist.insert(node.node, 0);
        sigma.insert(node.node, 1.0);
        queue.push_back(node.node);

        // BFS loop to find shortest paths.
        while let Some(current_node_id) = queue.pop_front() {
            stack.push(current_node_id);
            for neighbor in NodeView::new_internal(g.clone(), current_node_id).out_neighbours() {
                // Path discovery
                if dist[&neighbor.node] < 0 {
                    queue.push_back(neighbor.node);
                    dist.insert(neighbor.node, dist[&current_node_id] + 1);
                }
                // Path counting
                if dist[&neighbor.node] == dist[&current_node_id] + 1 {
                    sigma.insert(
                        neighbor.node,
                        sigma[&neighbor.node] + sigma[&current_node_id],
                    );
                    predecessors
                        .entry(neighbor.node)
                        .or_default()
                        .push(current_node_id);
                }
            }
        }

        let mut delta: HashMap<VID, f64> = HashMap::new();
        for node in nodes.iter() {
            delta.insert(node.node, 0.0);
        }

        // Accumulation
        while let Some(w) = stack.pop() {
            for v in predecessors.get(&w).unwrap_or(&Vec::new()) {
                let coeff = (sigma[v] / sigma[&w]) * (1.0 + delta[&w]);
                let new_delta_v = delta[v] + coeff;
                delta.insert(*v, new_delta_v);
            }
            if w != node.node {
                let pos = index.index(&w).unwrap();
                betweenness[pos] += delta[&w];
            }
        }
    }

    // Normalization
    if normalized {
        let factor = 1.0 / ((n as f64 - 1.0) * (n as f64 - 2.0));
        for node in nodes.iter() {
            let pos = index.index(&node.node).unwrap();
            betweenness[pos] *= factor;
        }
    }

    NodeState::new_from_eval(g.clone(), betweenness)
}
