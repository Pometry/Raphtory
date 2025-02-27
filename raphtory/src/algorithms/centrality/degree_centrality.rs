use crate::{
    db::api::{state::NodeState, view::StaticGraphViewOps},
    prelude::*,
};
use rayon::prelude::*;

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
pub fn degree_centrality<G: StaticGraphViewOps>(g: &G) -> NodeState<'static, f64, G> {
    let max_degree = match g.nodes().degree().max() {
        None => return NodeState::new_empty(g.clone()),
        Some(v) => v,
    };

    let values: Vec<_> = g
        .nodes()
        .degree()
        .into_par_iter_values()
        .map(|v| (v as f64) / max_degree as f64)
        .collect();

    NodeState::new_from_values(g.clone(), values)
}

#[cfg(test)]
mod degree_centrality_test {
    use crate::{
        algorithms::centrality::degree_centrality::degree_centrality,
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::NO_PROPS,
        test_storage,
    };
    use std::collections::HashMap;

    #[test]
    fn test_degree_centrality() {
        let graph = Graph::new();
        let vs = vec![(1, 2), (1, 3), (1, 4), (2, 3), (2, 4)];
        for (src, dst) in &vs {
            graph.add_edge(0, *src, *dst, NO_PROPS, None).unwrap();
        }
        test_storage!(&graph, |graph| {
            let mut expected: HashMap<String, f64> = HashMap::new();
            expected.insert("1".to_string(), 1.0);
            expected.insert("2".to_string(), 1.0);
            expected.insert("3".to_string(), 2.0 / 3.0);
            expected.insert("4".to_string(), 2.0 / 3.0);

            let res = degree_centrality(graph);
            assert_eq!(res, expected);
        });
    }
}
