use rand::{rngs::StdRng, seq::SliceRandom, thread_rng, SeedableRng};
use std::collections::{BTreeMap, HashMap, HashSet};

use crate::{
    db::{api::view::StaticGraphViewOps, graph::node::NodeView},
    prelude::*,
};

/// Computes components using a label propagation algorithm
///
/// # Arguments
///
/// * `g` - A reference to the graph
/// * `seed` - (Optional) Array of 32 bytes of u8 which is set as the rng seed
///
/// Returns:
///
/// A vector of hashsets each containing nodes
///
pub fn label_propagation<G>(
    graph: &G,
    seed: Option<[u8; 32]>,
) -> Result<Vec<HashSet<NodeView<G>>>, &'static str>
where
    G: StaticGraphViewOps,
{
    let mut labels: HashMap<NodeView<G>, u64> = HashMap::new();
    for node in graph.nodes() {
        labels.insert(node.clone(), node.id());
    }

    let nodes = graph.nodes();
    let mut shuffled_nodes: Vec<NodeView<G>> = nodes.iter().collect();
    if let Some(seed_value) = seed {
        let mut rng = StdRng::from_seed(seed_value);
        shuffled_nodes.shuffle(&mut rng);
    } else {
        let mut rng = thread_rng();
        shuffled_nodes.shuffle(&mut rng);
    }
    let mut changed = true;
    while changed {
        changed = false;
        for node in &shuffled_nodes {
            let neighbors = node.neighbours();
            let mut label_count: BTreeMap<u64, f64> = BTreeMap::new();

            for neighbour in neighbors {
                *label_count.entry(labels[&neighbour.clone()]).or_insert(0.0) += 1.0;
            }

            if let Some(max_label) = find_max_label(&label_count) {
                if max_label != labels[&node] {
                    labels.insert(node.clone(), max_label);
                    changed = true;
                }
            }
        }
    }

    // Group nodes by their labels to form communities
    let mut communities: HashMap<u64, HashSet<NodeView<G>>> = HashMap::new();
    for (node, label) in labels {
        communities.entry(label).or_default().insert(node.clone());
    }

    Ok(communities.values().cloned().collect())
}

fn find_max_label(label_count: &BTreeMap<u64, f64>) -> Option<u64> {
    label_count
        .iter()
        .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
        .map(|(label, _)| *label)
}

#[cfg(test)]
mod lpa_tests {
    use super::*;
    use crate::test_storage;

    #[test]
    fn lpa_test() {
        let graph: Graph = Graph::new();
        let edges = vec![
            (1, "R1", "R2"),
            (1, "R2", "R3"),
            (1, "R3", "G"),
            (1, "G", "B1"),
            (1, "G", "B3"),
            (1, "B1", "B2"),
            (1, "B2", "B3"),
            (1, "B2", "B4"),
            (1, "B3", "B4"),
            (1, "B3", "B5"),
            (1, "B4", "B5"),
        ];
        for (ts, src, dst) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let seed = Some([5; 32]);
            let result = label_propagation(graph, seed).unwrap();
            let expected = vec![
                HashSet::from([
                    graph.node("R1").unwrap(),
                    graph.node("R2").unwrap(),
                    graph.node("R3").unwrap(),
                ]),
                HashSet::from([
                    graph.node("G").unwrap(),
                    graph.node("B1").unwrap(),
                    graph.node("B2").unwrap(),
                    graph.node("B3").unwrap(),
                    graph.node("B4").unwrap(),
                    graph.node("B5").unwrap(),
                ]),
            ];
            for hashset in expected {
                assert!(result.contains(&hashset));
            }
        });
    }
}
