use crate::{
    db::api::{
        state::{GenericNodeState, TypedNodeState},
        view::StaticGraphViewOps,
    },
    prelude::*,
};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, PartialEq, Serialize, Deserialize, Debug)]
pub struct CentralityScore {
    #[serde(rename = "degree_centrality")]
    pub score: f64,
}

impl PartialEq<f64> for CentralityScore {
    fn eq(&self, other: &f64) -> bool {
        self.score == *other
    }
}

/// Computes the degree centrality of all nodes in the graph. The values are normalized
/// by dividing each result with the maximum possible degree. Graphs with self-loops can have
/// values of centrality greater than 1.
///
/// # Arguments
///
/// - `g`: A reference to the graph.
///
/// # Returns
///
/// An [AlgorithmResult] containing the degree centrality of each node.
pub fn degree_centrality<G: StaticGraphViewOps>(
    g: &G,
) -> TypedNodeState<'static, CentralityScore, G> {
    // NodeState<'static, f64, G> {
    let max_degree = match g.nodes().degree().max() {
        None => return TypedNodeState::new(GenericNodeState::new_empty(g.clone())),
        Some(v) => v,
    };

    let values: Vec<_> = if max_degree == 0 {
        vec![CentralityScore { score: 0.0 }; g.count_nodes()]
    } else {
        g.nodes()
            .degree()
            .into_par_iter_values()
            .map(|v| CentralityScore {
                score: (v as f64) / max_degree as f64,
            })
            .collect()
    };
    TypedNodeState::new(GenericNodeState::new_from_eval(g.clone(), values, None))
}

#[cfg(test)]
mod test {
    use crate::{algorithms::centrality::degree_centrality::degree_centrality, prelude::*};

    #[test]
    fn test_empty_edges() {
        let g = Graph::new();
        for i in 0..10 {
            g.add_node(0, i, NO_PROPS, None, None).unwrap();
        }
        let c = degree_centrality(&g);
        assert_eq!(c.values_to_rows(), vec![0.0; 10])
    }
}
