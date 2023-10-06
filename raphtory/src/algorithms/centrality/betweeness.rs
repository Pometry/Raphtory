use crate::{
    algorithms::algorithm_result::AlgorithmResult,
    prelude::{GraphViewOps, VertexViewOps},
};
use ordered_float::OrderedFloat;
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, VecDeque},
};

#[derive(Eq, PartialEq)]
struct Reverse<T>(T);

impl<T: Ord> PartialOrd for Reverse<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        other.0.partial_cmp(&self.0)
    }
}

impl<T: Ord> Ord for Reverse<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        other.0.cmp(&self.0)
    }
}

pub fn betweenness_centrality<G: GraphViewOps>(g: &G, k: Option<usize>, normalized: bool) {
    let mut betweenness: HashMap<u64, f64> = HashMap::new();
    let nodes = g.vertices();
    let n = g.count_vertices();
    let k_sample = k.unwrap_or(n);

    for node in nodes.iter().take(k_sample) {
        let mut stack = Vec::new();
        let mut predecessors: HashMap<u64, Vec<u64>> = HashMap::new();
        let mut sigma: HashMap<u64, f64> = HashMap::new();
        let mut dist: HashMap<u64, i64> = HashMap::new();
        let mut queue = VecDeque::new();

        for node in nodes.iter() {
            dist.insert(node.id(), -1);
            sigma.insert(node.id(), 0.0);
        }
        dist.insert(node.id(), 0);
        sigma.insert(node.id(), 1.0);
        queue.push_back(node.id());

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
    if normalized {
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

    // Output the betweenness centrality of each node
    println!("{:?}", betweenness);
}

#[cfg(test)]
mod betweenness_centrality_test {
    use super::*;
    use crate::prelude::*;

    #[test]
    fn test_betweenness_centrality() {
        let graph = Graph::new();
        let vs = vec![
            (0, 1),
            (0, 2),
            (0, 3),
            (1, 2),
            (1, 3),
            (1, 4),
            (2, 3),
            (2, 4),
            (2, 5),
            (3, 2),
            (3, 1),
            (3, 3),
        ];
        for (src, dst) in &vs {
            graph.add_edge(0, *src, *dst, NO_PROPS, None).unwrap();
        }
        betweenness_centrality(&graph, None, false);
        betweenness_centrality(&graph, None, true)
    }
}
