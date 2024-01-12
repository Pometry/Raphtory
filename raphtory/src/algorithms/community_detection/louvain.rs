use crate::{
    algorithms::{
        algorithm_result::AlgorithmResult,
        community_detection::modularity::{ModularityFunction, Partition},
    },
    core::entities::VID,
    prelude::GraphViewOps,
};
use rand::prelude::SliceRandom;
use std::collections::HashMap;

pub fn louvain<'graph, M: ModularityFunction, G: GraphViewOps<'graph>>(
    graph: &G,
    resolution: f64,
    weight_prop: Option<&str>,
    tol: Option<f64>,
) -> AlgorithmResult<G, usize> {
    let tol = tol.unwrap_or(1e-8);
    let mut rng = rand::thread_rng();
    let mut modularity_state = M::new(
        graph,
        weight_prop,
        resolution,
        Partition::new_singletons(graph.count_nodes()),
        tol,
    );
    let mut global_partition: HashMap<_, _> = graph
        .nodes()
        .iter()
        .enumerate()
        .map(|(ci, node)| (node.node.index(), ci))
        .collect();

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
                    let old_c = modularity_state.partition().com(&v);
                    if best_c != old_c && delta > tol {
                        inner_moved = true;
                        outer_moved = true;
                        modularity_state.move_node(v, best_c);
                    }
                }
            }
        }
        let partition = modularity_state.aggregate();
        for c in global_partition.values_mut() {
            *c = partition.com(&VID(*c)).index();
        }
    }
    AlgorithmResult::new(graph.clone(), "louvain", "usize", global_partition)
}

#[cfg(test)]
mod test {
    use crate::{
        algorithms::community_detection::{louvain::louvain, modularity::ModularityUnDir},
        prelude::*,
    };
    use proptest::prelude::*;

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
        assert!(test_all_nodes_assigned_inner(edges))
        // }
    }

    fn test_all_nodes_assigned_inner(edges: Vec<(u64, u64, f64)>) -> bool {
        let g = Graph::new();
        for (src, dst, weight) in edges {
            g.add_edge(1, src, dst, [("weight", weight)], None).unwrap();
            g.add_edge(1, dst, src, [("weight", weight)], None).unwrap();
        }
        let result = louvain::<ModularityUnDir, _>(&g, 1.0, Some("weight"), None);
        g.nodes().iter().all(|n| result.get(n).is_some())
    }

    fn test_all_nodes_assigned_inner_unweighted(edges: Vec<(u64, u64)>) -> bool {
        let g = Graph::new();
        for (src, dst) in edges {
            g.add_edge(1, src, dst, NO_PROPS, None).unwrap();
            g.add_edge(1, dst, src, NO_PROPS, None).unwrap();
        }
        let result = louvain::<ModularityUnDir, _>(&g, 1.0, None, None);
        g.nodes().iter().all(|n| result.get(n).is_some())
    }

    proptest! {
        #[test]
        fn test_all_nodes_in_communities(edges in any::<Vec<(u64, u64, f64)>>().prop_map(|mut v| {v.iter_mut().for_each(|(_, _, w)| *w = w.abs()); v})) {
            prop_assert!(test_all_nodes_assigned_inner(edges))
        }

        #[test]
        fn test_all_nodes_assigned_unweighted(edges in any::<Vec<(u8, u8)>>().prop_map(|v| v.into_iter().map(|(s, d)|  (s as u64, d as u64)).collect::<Vec<_>>())) {
            prop_assert!(test_all_nodes_assigned_inner_unweighted(edges))
        }
    }

    #[cfg(feature = "io")]
    #[test]
    fn lfr_test() {
        use crate::graph_loader::source::csv_loader::CsvLoader;
        use serde::{Deserialize, Serialize};
        use std::path::PathBuf;

        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("resources/test");
        let loader = CsvLoader::new(d.join("test.csv")).set_delimiter(",");
        let g = Graph::new();

        #[derive(Deserialize, Serialize, Debug)]
        struct CsvEdge {
            src: u64,
            dst: u64,
        }

        loader
            .load_into_graph(&g, |e: CsvEdge, g| {
                g.add_edge(1, e.src, e.dst, NO_PROPS, None).unwrap();
            })
            .unwrap();

        let result = louvain::<ModularityUnDir, _>(&g, 1.0, None, None);
        println!("{result:?}")
    }
}
