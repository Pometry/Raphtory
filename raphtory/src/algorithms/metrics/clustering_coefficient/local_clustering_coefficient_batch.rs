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
