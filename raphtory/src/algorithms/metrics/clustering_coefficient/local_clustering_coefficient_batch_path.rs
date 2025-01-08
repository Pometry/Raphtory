use crate::{
    algorithms::{
        algorithm_result::AlgorithmResult, metrics::clustering_coefficient::filter_nodes,
    },
    core::{entities::nodes::node_ref::AsNodeRef, state::compute_state::ComputeStateVec},
    db::{
        api::view::*,
        graph::views::node_subgraph::NodeSubgraph,
        task::{
            context::Context,
            node::eval_node::EvalNodeView,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
        },
    },
};
use itertools::Itertools;
use ordered_float::OrderedFloat;
use raphtory_api::core::entities::VID;
use rayon::iter::ParallelIterator;
use rustc_hash::FxHashSet;

/// Local clustering coefficient (batch, intersection) - measures the degree to which one or multiple nodes in a graph tend to cluster together.
/// Uses path-counting for its triangle-counting step.
///
/// # Arguments
/// - `graph`: Raphtory graph, can be directed or undirected but will be treated as undirected.
/// - `v`: vec of node ids, if empty, will return results for every node in the graph
/// - `threads`: number of threads to use
///
/// # Returns
/// the local clustering coefficient of node v in g.
pub fn local_clustering_coefficient_batch_path<G: StaticGraphViewOps, V: AsNodeRef>(
    graph: &G,
    v: Vec<V>,
    threads: Option<usize>,
) -> AlgorithmResult<G, f64, OrderedFloat<f64>> {
    #[derive(Clone, Debug, Default)]
    struct LCCState {
        lcc: f64,
        src_node: bool,
    }

    let all_src_nodes: bool = v.len() == 0;
    let nodes: FxHashSet<VID>;
    let mut src_nodes: FxHashSet<VID> = FxHashSet::default();
    let g;
    if all_src_nodes == false {
        (nodes, src_nodes) = filter_nodes(graph, &v);
        g = graph.subgraph(nodes);
    } else {
        g = graph.subgraph(graph.nodes());
    }
    let ctx: Context<NodeSubgraph<G>, ComputeStateVec> = Context::from(&g);

    let step1 = ATask::new(move |s: &mut EvalNodeView<NodeSubgraph<G>, LCCState>| {
        let src_node = src_nodes.contains(&s.node) || all_src_nodes == true;
        s.get_mut().src_node = src_node;
        let degree = s.degree();
        if degree < 2 || (degree == 2 && g.has_edge(&s.node, &s.node)) {
            s.get_mut().lcc = 0.0;
            return Step::Continue;
        }
        let triangle_count = s
            .neighbours()
            .iter()
            .filter(|nbor| nbor.degree() > 1 && nbor.node != s.node)
            .combinations(2)
            .filter_map(|nb| match g.has_edge(nb[0].id(), nb[1].id()) {
                true => Some(1),
                false => match g.has_edge(nb[1].id(), nb[0].id()) {
                    true => Some(1),
                    false => None,
                },
            })
            .count() as f64;
        let mut degree = s.degree() as f64;
        if g.has_edge(s.node, s.node) {
            degree -= 1.0;
        }
        s.get_mut().lcc = (2.0 * triangle_count) / (degree * (degree - 1.0));
        Step::Continue
    });

    let tasks = vec![Job::new(step1)];

    let mut runner: TaskRunner<NodeSubgraph<G>, _> = TaskRunner::new(ctx);

    let res = runner.run(
        vec![],
        tasks,
        None,
        |_, _, _, local: Vec<LCCState>| {
            graph
                .nodes()
                .par_iter()
                .filter_map(|node| {
                    let VID(id) = node.node;
                    if local[id].src_node == true {
                        Some((id, local[id].lcc))
                    } else {
                        None
                    }
                })
                .collect()
        },
        threads,
        1,
        None,
        None,
    );

    let results_type = std::any::type_name::<f64>();
    AlgorithmResult::new(graph.clone(), "Triangle Count (Batch)", results_type, res)
}

#[cfg(test)]
mod clustering_coefficient_tests {
    use super::local_clustering_coefficient_batch_path;
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
            let expected = vec![0.3333333333333333, 1.0, 1.0, 0.0, 0.0];
            let expected: HashMap<String, f64> =
                (1..=5).map(|v| (v.to_string(), expected[v - 1])).collect();
            let windowed_graph = graph.window(0, 7);
            let actual =
                local_clustering_coefficient_batch_path(&windowed_graph, (1..=5).collect(), None)
                    .get_all_with_names();
            assert_eq!(actual, expected);
        });
    }
}
