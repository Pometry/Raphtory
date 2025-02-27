use crate::{
    algorithms::community_detection::modularity::{ModularityFunction, Partition},
    core::entities::VID,
    db::api::state::NodeState,
    prelude::GraphViewOps,
};
use rand::prelude::SliceRandom;

/// Louvain algorithm for community detection
///
/// # Arguments
///
/// - `g` (GraphView): the graph view
/// - `resolution` (float): the resolution parameter for modularity
/// - `weight_prop` (str | None): the edge property to use for weights (has to be float)
/// - `tol` (None | float): the floating point tolerance for deciding if improvements are significant (default: 1e-8)
///
/// # Returns
///
///  An [AlgorithmResult] containing a mapping of vertices to cluster ID.
pub fn louvain<'graph, M: ModularityFunction, G: GraphViewOps<'graph>>(
    g: &G,
    resolution: f64,
    weight_prop: Option<&str>,
    tol: Option<f64>,
) -> NodeState<'graph, usize, G> {
    let tol = tol.unwrap_or(1e-8);
    let mut rng = rand::thread_rng();
    let mut modularity_state = M::new(
        g,
        weight_prop,
        resolution,
        Partition::new_singletons(g.count_nodes()),
        tol,
    );

    let mut global_partition: Vec<_> = (0..g.count_nodes()).collect();

    let mut outer_moved = true;
    while outer_moved {
        outer_moved = false;
        let mut inner_moved = true;
        let mut nodes: Vec<_> = modularity_state.nodes().collect();
        while inner_moved {
            inner_moved = false;
            nodes.shuffle(&mut rng);
            for v in nodes.iter() {
                if let Some((best_c, delta)) = modularity_state
                    .candidate_moves(v)
                    .map(|c| (c, modularity_state.move_delta(v, c)))
                    .max_by(|(_, delta1), (_, delta2)| delta1.total_cmp(delta2))
                {
                    let old_c = modularity_state.partition().com(v);
                    if best_c != old_c && delta > tol {
                        inner_moved = true;
                        outer_moved = true;
                        modularity_state.move_node(v, best_c);
                    }
                }
            }
        }
        let partition = modularity_state.aggregate();
        for c in global_partition.iter_mut() {
            *c = partition.com(&VID(*c)).index();
        }
    }
    NodeState::new_from_values(g.clone(), global_partition)
}

#[cfg(test)]
mod test {
    use crate::{
        algorithms::community_detection::{louvain::louvain, modularity::ModularityUnDir},
        prelude::*,
        test_storage,
    };
    use proptest::prelude::*;

    #[cfg(feature = "io")]
    use raphtory_api::core::utils::logging::global_info_logger;

    #[test]
    fn test_louvain() {
        let edges = vec![
            (100, 200, 2.0f64),
            (100, 300, 3.0f64),
            (200, 300, 8.5f64),
            (300, 400, 1.0f64),
            (400, 500, 1.5f64),
            (600, 800, 0.5f64),
            (700, 900, 3.5f64),
            (100, 600, 1.5f64),
        ];
        // for _ in 0..100 {
        test_all_nodes_assigned_inner(edges)
        // }
    }

    fn test_all_nodes_assigned_inner(edges: Vec<(u64, u64, f64)>) {
        let graph = Graph::new();
        for (src, dst, weight) in edges {
            graph
                .add_edge(1, src, dst, [("weight", weight)], None)
                .unwrap();
            graph
                .add_edge(1, dst, src, [("weight", weight)], None)
                .unwrap();
        }

        test_storage!(&graph, |graph| {
            let result = louvain::<ModularityUnDir, _>(graph, 1.0, Some("weight"), None);
            assert!(graph
                .nodes()
                .iter()
                .all(|n| result.get_by_node(n).is_some()));
        });
    }

    fn test_all_nodes_assigned_inner_unweighted(edges: Vec<(u64, u64)>) {
        let graph = Graph::new();
        for (src, dst) in edges {
            graph.add_edge(1, src, dst, NO_PROPS, None).unwrap();
            graph.add_edge(1, dst, src, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let result = louvain::<ModularityUnDir, _>(graph, 1.0, None, None);
            assert!(graph
                .nodes()
                .iter()
                .all(|n| result.get_by_node(n).is_some()));
        });
    }

    proptest! {
        #[test]
        fn test_all_nodes_in_communities(edges in any::<Vec<(u64, u64, f64)>>().prop_map(|mut v| {v.iter_mut().for_each(|(_, _, w)| *w = w.abs()); v})) {
            test_all_nodes_assigned_inner(edges)
        }

        #[test]
        fn test_all_nodes_assigned_unweighted(edges in any::<Vec<(u8, u8)>>().prop_map(|v| v.into_iter().map(|(s, d)|  (s as u64, d as u64)).collect::<Vec<_>>())) {
            test_all_nodes_assigned_inner_unweighted(edges)
        }
    }

    #[cfg(feature = "io")]
    #[test]
    fn lfr_test() {
        use crate::io::csv_loader::CsvLoader;
        use serde::{Deserialize, Serialize};
        use std::path::PathBuf;
        global_info_logger();
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("resources/test");
        let loader = CsvLoader::new(d.join("test.csv")).set_delimiter(",");
        let graph = Graph::new();

        #[derive(Deserialize, Serialize, Debug)]
        struct CsvEdge {
            src: u64,
            dst: u64,
        }

        loader
            .load_into_graph(&graph, |e: CsvEdge, g| {
                g.add_edge(1, e.src, e.dst, NO_PROPS, None).unwrap();
            })
            .unwrap();

        test_storage!(&graph, |graph| {
            let _ = louvain::<ModularityUnDir, _>(graph, 1.0, None, None);
            // TODO: Add assertions
        });
    }
}
