use crate::{
    core::entities::nodes::node_ref::AsNodeRef,
    db::api::{
        state::{Index, NodeState},
        view::*,
    },
};
use indexmap::IndexSet;
use itertools::Itertools;
use rayon::prelude::*;

/// Local clustering coefficient (batch, intersection) - measures the degree to which one or multiple nodes in a graph tend to cluster together.
/// Uses path-counting for its triangle-counting step.
///
/// # Arguments
/// - `graph`: Raphtory graph, can be directed or undirected but will be treated as undirected.
/// - `v`: vec of node ids, if empty, will return results for every node in the graph
///
/// # Returns
/// the local clustering coefficient of node v in g.
pub fn local_clustering_coefficient_batch<G: StaticGraphViewOps, V: AsNodeRef>(
    graph: &G,
    v: Vec<V>,
) -> NodeState<'static, f64, G> {
    let (index, values): (IndexSet<_, ahash::RandomState>, Vec<_>) = v
        .par_iter()
        .filter_map(|n| {
            let s = (&graph).node(n)?;
            let triangle_count = s
                .neighbours()
                .iter()
                .filter(|nbor| nbor.degree() > 1 && nbor.node != s.node)
                .combinations(2)
                .filter(|nb| graph.has_edge(nb[0], nb[1]) || graph.has_edge(nb[1], nb[0]))
                .count() as f64;
            let mut degree = s.degree() as f64;
            if graph.has_edge(s.node, s.node) {
                degree -= 1.0;
            }
            Some((
                s.node,
                if degree <= 1.0 {
                    0.0
                } else {
                    (2.0 * triangle_count) / (degree * (degree - 1.0))
                },
            ))
        })
        .unzip();
    let result: Option<_> = Some(Index::new(index));
    NodeState::new(graph.clone(), graph.clone(), values.into(), result)
}

#[cfg(test)]
mod clustering_coefficient_tests {
    use super::local_clustering_coefficient_batch;
    use crate::{
        db::{
            api::{mutation::AdditionOps, view::*},
            graph::graph::Graph,
        },
        prelude::NO_PROPS,
        test_storage,
    };
    use std::collections::HashMap;

    #[test]
    fn clusters_of_triangles() {
        let graph = Graph::new();
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (3, 2, 1),
            (4, 3, 2),
            (5, 1, 4),
            (6, 4, 5),
            (6, 1, 1),
            (6, 5, 5),
        ];

        for (t, src, dst) in &vs {
            graph.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let expected = [0.3333333333333333, 1.0, 1.0, 0.0, 0.0];
            let expected: HashMap<String, f64> =
                (1..=5).map(|v| (v.to_string(), expected[v - 1])).collect();
            let windowed_graph = graph.window(0, 7);
            let actual = local_clustering_coefficient_batch(&windowed_graph, (1..=5).collect());
            let actual: HashMap<String, f64> = (1..=5)
                .map(|v| (v.to_string(), actual.values()[v - 1]))
                .collect();
            assert_eq!(expected, actual);
        });
    }
}
