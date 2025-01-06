//! Local Clustering coefficient - measures the degree to which nodes in a graph tend to cluster together.
//!
//! It is calculated by dividing the number of triangles (sets of three nodes that are all
//! connected to each other) in the graph by the total number of possible triangles.
//! The resulting value is a number between 0 and 1 that represents the density of
//! clustering in the graph.
//!
//! A high clustering coefficient indicates that nodes tend to be
//! connected to nodes that are themselves connected to each other, while a low clustering
//! coefficient indicates that nodes tend to be connected to nodes that are not connected
//! to each other.
//!
//! In a social network of a particular community_detection, we can compute the clustering
//! coefficient of each node to get an idea of how strongly connected and cohesive
//! that node's neighborhood is.
//!
//! A high clustering coefficient for a node in a social network indicates that the
//! node's neighbors tend to be strongly connected with each other, forming a tightly-knit
//! group or community_detection. In contrast, a low clustering coefficient for a node indicates that
//! its neighbors are relatively less connected with each other, suggesting a more fragmented
//! or diverse community_detection.
//!
//! # Examples
//!
//! ```rust
//! use raphtory::algorithms::metrics::local_clustering_coefficient::local_clustering_coefficient;
//! use raphtory::prelude::*;
//!
//! let g = Graph::new();
//! let windowed_graph = g.window(0, 7);
//! let vs = vec![
//!     (1, 1, 2),
//!     (2, 1, 3),
//!     (3, 2, 1),
//!     (4, 3, 2),
//!     (5, 1, 4),
//!     (6, 4, 5),
//! ];
//!
//! for (t, src, dst) in &vs {
//!     g.add_edge(*t, *src, *dst, NO_PROPS, None);
//! }
//!
//! let actual = (1..=5)
//! .map(|v| local_clustering_coefficient(&windowed_graph, v))
//! .collect::<Vec<_>>();
//!
//! println!("local clustering coefficient of all nodes: {:?}", actual);
//! ```

use crate::{
    algorithms::{
        algorithm_result::AlgorithmResult, cores::k_core::k_core_set,
        motifs::local_triangle_count::local_triangle_count,
    },
    core::{
        entities::nodes::node_ref::AsNodeRef,
        state::{accumulator_id::accumulators, compute_state::ComputeStateVec},
    },
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
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use rustc_hash::FxHashSet;

fn filter_nodes<G: StaticGraphViewOps, V: AsNodeRef>(
    graph: &G,
    v: &Vec<V>,
) -> (FxHashSet<VID>, FxHashSet<VID>) {
    // figure out optimal way to apply these operations
    let src_nodes: FxHashSet<VID> = v
        .into_iter()
        .map(|node| graph.node(node).unwrap().node)
        .collect();

    let neighbours: FxHashSet<VID> = src_nodes
        .par_iter()
        .map(|node| {
            graph
                .node(node)
                .unwrap()
                .neighbours()
                .iter()
                .map(|nbor| nbor.node)
                .collect()
        })
        .reduce(
            || FxHashSet::default(),
            |mut acc, neighbors| {
                acc.extend(neighbors);
                acc
            },
        );
    (src_nodes.union(&neighbours).copied().collect(), src_nodes)
}

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
    let mut nodes: FxHashSet<VID> = FxHashSet::default();
    let mut src_nodes: FxHashSet<VID> = FxHashSet::default();
    let mut g;
    if all_src_nodes == false {
        (nodes, src_nodes) = filter_nodes(graph, &v);
        g = graph.subgraph(nodes);
    } else {
        g = graph.subgraph(graph.nodes());
    }
    let ctx: Context<NodeSubgraph<G>, ComputeStateVec> = Context::from(&g);

    let step1 = ATask::new(move |s: &mut EvalNodeView<NodeSubgraph<G>, NborState>| {
        // filter out non-eligible verts
        let src_node = (src_nodes.contains(&s.node) || all_src_nodes == true) && s.degree() > 1;
        s.get_mut().src_node = src_node;
        if s.degree() < 2 {
            return Step::Continue;
        }
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
    let mut nodes: FxHashSet<VID> = FxHashSet::default();
    let mut src_nodes: FxHashSet<VID> = FxHashSet::default();
    let mut g;
    if all_src_nodes == false {
        (nodes, src_nodes) = filter_nodes(graph, &v);
        g = graph.subgraph(nodes);
    } else {
        g = graph.subgraph(graph.nodes());
    }
    let ctx: Context<NodeSubgraph<G>, ComputeStateVec> = Context::from(&g);

    let step1 = ATask::new(move |s: &mut EvalNodeView<NodeSubgraph<G>, LCCState>| {
        let src_node = (src_nodes.contains(&s.node) || all_src_nodes == true) && s.degree() > 1;
        s.get_mut().src_node = src_node;
        if src_node == false {
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

/// measures the degree to which nodes in a graph tend to cluster together
pub fn local_clustering_coefficient<G: StaticGraphViewOps, V: AsNodeRef>(
    graph: &G,
    v: V,
) -> Option<f64> {
    let v = v.as_node_ref();
    if let Some(node) = graph.node(v) {
        if let Some(triangle_count) = local_triangle_count(graph, v) {
            let triangle_count = triangle_count as f64;
            let mut degree = node.degree() as f64;
            if graph.has_edge(node.node, node.node) {
                degree -= 1.0;
            }
            if degree > 1.0 {
                Some((2.0 * triangle_count) / (degree * (degree - 1.0)))
            } else {
                Some(0.0)
            }
        } else {
            None
        }
    } else {
        None
    }
}

#[cfg(test)]
mod clustering_coefficient_tests {
    use super::{
        local_clustering_coefficient, local_clustering_coefficient_batch_intersection,
        local_clustering_coefficient_batch_path,
    };
    use crate::{
        db::{
            api::{mutation::AdditionOps, view::*},
            graph::graph::Graph,
        },
        prelude::NO_PROPS,
        test_storage,
    };

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
        ];

        for (t, src, dst) in &vs {
            graph.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let expected = vec![0.3333333333333333, 1.0, 1.0, 0.0, 0.0];
            let windowed_graph = graph.window(0, 7);
            let actual = (1..=5)
                .map(|v| local_clustering_coefficient(&windowed_graph, v).unwrap())
                .collect::<Vec<_>>();
            let res1 = local_clustering_coefficient_batch_intersection(
                &windowed_graph,
                (1..=2).collect(),
                None,
            )
            .get_all_with_names();
            let res2 =
                local_clustering_coefficient_batch_path(&windowed_graph, (1..=2).collect(), None)
                    .get_all_with_names();
            println!("res1: {:?}, res2: {:?}", res1, res2);
            assert_eq!(actual, expected);
        });
    }
}
