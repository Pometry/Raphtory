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
use ordered_float::OrderedFloat;
use raphtory_api::core::entities::VID;
use rayon::iter::ParallelIterator;
use rustc_hash::FxHashSet;

/// Local clustering coefficient (batch, intersection) - measures the degree to which one or multiple nodes in a graph tend to cluster together.
/// Uses cached sets of neighbours to handle triangle-counting.
///
/// # Arguments
/// - `graph`: Raphtory graph, can be directed or undirected but will be treated as undirected.
/// - `v`: vec of node ids, if empty, will return results for every node in the graph
/// - `threads`: number of threads to use
///
/// # Returns
/// the local clustering coefficient of node v in g.
pub fn local_clustering_coefficient_batch_intersection<G: StaticGraphViewOps, V: AsNodeRef>(
    graph: &G,
    v: Vec<V>,
    threads: Option<usize>,
) -> AlgorithmResult<G, f64, OrderedFloat<f64>> {
    #[derive(Clone, Debug, Default)]
    struct NborState {
        lower_nbors: FxHashSet<VID>,
        higher_nbors: FxHashSet<VID>,
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

    let step1 = ATask::new(move |s: &mut EvalNodeView<NodeSubgraph<G>, NborState>| {
        // filter out non-eligible verts
        let src_node = src_nodes.contains(&s.node) || all_src_nodes == true;
        s.get_mut().src_node = src_node;
        s.get_mut().lcc = -1.0;
        let degree = s.degree();
        if degree < 2 || (degree == 2 && g.has_edge(&s.node, &s.node)) {
            return Step::Continue;
        }
        s.get_mut().lcc = 0.0;
        let mut lower_nbors = FxHashSet::default();
        let mut higher_nbors = FxHashSet::default();
        for t in s.neighbours() {
            if t.node == s.node {
                continue;
            }
            if s.node > t.node {
                lower_nbors.insert(t.node);
            } else if src_node == true {
                higher_nbors.insert(t.node);
            }
        }
        let state = s.get_mut();
        state.lower_nbors = lower_nbors;
        state.higher_nbors = higher_nbors;
        Step::Continue
    });

    let step2 = ATask::new(move |s: &mut EvalNodeView<NodeSubgraph<G>, NborState>| {
        // filter down to eligible src_nodes
        if s.get().src_node == false {
            return Step::Continue;
        } else if s.get().lcc == -1.0 {
            s.get_mut().lcc = 0.0;
            return Step::Continue;
        }
        let mut intersection_count = 0;
        let lower_nbors = &s.get().lower_nbors;
        let higher_nbors = &s.get().higher_nbors;
        for t in s.neighbours() {
            if t.node == s.node {
                continue;
            }
            intersection_count += higher_nbors.intersection(&t.prev().lower_nbors).count();
            intersection_count += lower_nbors.intersection(&t.prev().lower_nbors).count();
        }
        let degree = (lower_nbors.len() + higher_nbors.len()) as f64;
        s.get_mut().lcc = (2.0 * intersection_count as f64) / (degree * (degree - 1.0));
        Step::Continue
    });

    let init_tasks = vec![Job::new(step1)];
    let tasks = vec![Job::new(step2)];

    let mut runner: TaskRunner<NodeSubgraph<G>, _> = TaskRunner::new(ctx);

    let res = runner.run(
        init_tasks,
        tasks,
        None,
        |_, _, _, local: Vec<NborState>| {
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
    use super::local_clustering_coefficient_batch_intersection;
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
            let actual = local_clustering_coefficient_batch_intersection(
                &windowed_graph,
                (1..=5).collect(),
                None,
            )
            .get_all_with_names();
            assert_eq!(actual, expected);
        });
    }
}
