use rand::{rng, rngs::StdRng, seq::SliceRandom, SeedableRng};
use raphtory_api::core::entities::GID;
use std::collections::{BTreeMap, HashMap, HashSet};

use crate::{
    db::{api::view::StaticGraphViewOps, graph::node::NodeView},
    prelude::*,
};

/// Computes components using a label propagation algorithm
///
/// # Arguments
///
/// - `g` - A reference to the graph
/// - `seed` - (Optional) Array of 32 bytes of u8 which is set as the rng seed
///
/// # Returns
///
/// A vector of hashsets each containing nodes
///
pub fn label_propagation<G>(
    g: &G,
    seed: Option<[u8; 32]>,
) -> Result<Vec<HashSet<NodeView<'static, G>>>, &'static str>
where
    G: StaticGraphViewOps,
{
    let mut labels: HashMap<NodeView<&G>, GID> = HashMap::new();
    let nodes = &g.nodes();
    for node in nodes.iter() {
        labels.insert(node, node.id());
    }

    let mut shuffled_nodes: Vec<NodeView<&G>> = nodes.iter().collect();
    if let Some(seed_value) = seed {
        let mut rng = StdRng::from_seed(seed_value);
        shuffled_nodes.shuffle(&mut rng);
    } else {
        let mut rng = rng();
        shuffled_nodes.shuffle(&mut rng);
    }
    let mut changed = true;
    while changed {
        changed = false;
        for node in &shuffled_nodes {
            let neighbors = node.neighbours();
            let mut label_count: BTreeMap<GID, f64> = BTreeMap::new();

            for neighbour in neighbors {
                *label_count.entry(labels[&neighbour].clone()).or_insert(0.0) += 1.0;
            }

            if let Some(max_label) = find_max_label(&label_count) {
                if max_label != labels[node] {
                    labels.insert(*node, max_label);
                    changed = true;
                }
            }
        }
    }

    // Group nodes by their labels to form communities
    let mut communities: HashMap<GID, HashSet<NodeView<'static, G>>> = HashMap::new();
    for (node, label) in labels {
        communities.entry(label).or_default().insert(node.cloned());
    }

    Ok(communities.values().cloned().collect())
}

fn find_max_label(label_count: &BTreeMap<GID, f64>) -> Option<GID> {
    label_count
        .iter()
        .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
        .map(|(label, _)| label.clone())
}
