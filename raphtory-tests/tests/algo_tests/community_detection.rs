use raphtory::{
    algorithms::community_detection::{
        label_propagation::label_propagation,
        louvain::louvain,
        modularity::{ComID, ModularityFunction, ModularityUnDir, Partition},
    },
    core::entities::VID,
    graphgen::random_attachment::random_attachment,
    logging::global_info_logger,
    prelude::*,
};
use raphtory_tests::test_storage;
use std::collections::{HashMap, HashSet};
use tracing::info;

fn group_by_value(map: &HashMap<String, usize>) -> Vec<HashSet<String>> {
    let mut grouped: HashMap<usize, HashSet<String>> = HashMap::new();

    for (key, &value) in map {
        grouped
            .entry(value)
            .or_insert_with(HashSet::new)
            .insert(key.clone());
    }

    grouped.into_values().collect()
}

#[test]
fn lpa_test() {
    let graph: Graph = Graph::new();
    let edges = vec![
        (1, "R1", "R2"),
        (1, "R1", "R3"),
        (1, "R2", "R3"),
        (1, "R3", "G"),
        (1, "G", "B1"),
        (1, "G", "B3"),
        (1, "B1", "B2"),
        (1, "B2", "B3"),
        (1, "B2", "B4"),
        (1, "B3", "B4"),
        (1, "B3", "B5"),
        (1, "B4", "B5"),
    ];
    for (ts, src, dst) in edges {
        graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
    }
    test_storage!(&graph, |graph| {
        let seed = Some(8); // NB: different seeds affect the partition reached
        let result = label_propagation(graph, 20, seed, None, None, None, None)
            .to_hashmap(|value| value.community_id);
        println!("{:?}", result);
        let result = group_by_value(&result);

        let expected = vec![
            HashSet::from(["R1".to_string(), "R2".to_string(), "R3".to_string()]),
            HashSet::from([
                "G".to_string(),
                "B1".to_string(),
                "B2".to_string(),
                "B3".to_string(),
                "B4".to_string(),
                "B5".to_string(),
            ]),
        ];
        for hashset in expected {
            assert!(result.contains(&hashset));
        }
    });
}

/// The vote share behind each label.
///
/// The graph is small enough to count by hand: X neighbours two A-seeds and one B-seed, Y neighbours
/// the B-seed alone, and Z—W is a component no seed can reach.
#[test]
fn lpa_vote_share() {
    let graph: Graph = Graph::new();
    for (src, dst) in [
        ("A1", "X"),
        ("A2", "X"),
        ("B1", "X"),
        ("B1", "Y"),
        ("Z", "W"),
    ] {
        graph.add_edge(1, src, dst, NO_PROPS, None).unwrap();
    }
    test_storage!(&graph, |graph| {
        let vid = |name: &str| graph.node(name).unwrap().node.0;
        let seeds: HashMap<usize, usize> =
            HashMap::from([(vid("A1"), 0), (vid("A2"), 0), (vid("B1"), 1)]);
        let out = label_propagation(graph, 20, Some(6), None, Some(seeds), None, None)
            .to_hashmap(|value| (value.community_id, value.confidence));

        let close = |got: (usize, f64), label: usize, share: f64| {
            assert_eq!(got.0, label, "{got:?}");
            assert!((got.1 - share).abs() < 1e-9, "{got:?} wanted {share}");
        };

        // X has no label of its own to add on its first pass, so it divides the seeds' three votes.
        close(out["X"], 0, 2.0 / 3.0);
        // Y hears from one seed and nothing else.
        assert_eq!(out["Y"], (1, 1.0));
        // The A-seeds re-evaluate once X has a label and find it agrees with them.
        assert_eq!(out["A1"], (0, 1.0));
        // The B-seed weighs X's A-label against its own vote and Y's, and keeps its label on 2 of 3.
        close(out["B1"], 1, 2.0 / 3.0);
        // Unreachable: never cast or received a vote, which is what the 0.0 means.
        assert_eq!(out["Z"], (usize::MAX, 0.0));
        assert_eq!(out["W"], (usize::MAX, 0.0));
    });
}

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
    test_all_nodes_assigned_inner(edges)
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
        let result = louvain::<ModularityUnDir, _>(graph, 1.0, Some("weight"), None, Some(42));
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
        let result = louvain::<ModularityUnDir, _>(graph, 1.0, None, None, Some(42));
        assert!(graph
            .nodes()
            .iter()
            .all(|n| result.get_by_node(n).is_some()));
    });
}

proptest! {
    #[test]
    fn test_all_nodes_in_communities_proptest(edges in any::<Vec<(u64, u64, f64)>>().prop_map(|mut v| {v.iter_mut().for_each(|(_, _, w)| *w = w.abs()); v})) {
        test_all_nodes_assigned_inner(edges)
    }

    #[test]
    fn test_all_nodes_assigned_unweighted_proptest(edges in any::<Vec<(u8, u8)>>().prop_map(|v| v.into_iter().map(|(s, d)|  (s as u64, d as u64)).collect::<Vec<_>>())) {
        test_all_nodes_assigned_inner_unweighted(edges)
    }
}

#[test]
fn lfr_test() {
    use raphtory::io::csv_loader::CsvLoader;
    use raphtory_api::core::utils::logging::global_info_logger;
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
        let _ = louvain::<ModularityUnDir, _>(graph, 1.0, None, None, Some(42));
        // TODO: Add assertions
    });
}

#[test]
fn test_louvain_deterministic() {
    let graph = Graph::new();
    random_attachment(&graph, 10_000, 5, Some([7; 32]));

    test_storage!(&graph, |graph| {
        let seed = Some(42);
        let first = louvain::<ModularityUnDir, _>(graph, 1.0, None, None, seed);

        for _ in 0..100 {
            let result = louvain::<ModularityUnDir, _>(graph, 1.0, None, None, seed);
            assert!(
                result == first,
                "louvain produced different clusters across runs with the same seed"
            );
        }
    });
}

#[test]
fn test_delta() {
    global_info_logger();
    let graph = Graph::new();
    graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
    graph.add_edge(0, 2, 1, NO_PROPS, None).unwrap();

    test_storage!(&graph, |graph| {
        let mut m = ModularityUnDir::new(
            graph,
            None,
            1.0,
            Partition::new_singletons(graph.count_nodes()),
            1e-8,
        );
        let old_value = m.value();
        assert_eq!(old_value, -0.5);
        let delta = m.move_delta(&VID(0), ComID(1));
        info!("delta: {delta}");
        m.move_node(&VID(0), ComID(1));
        assert_eq!(m.value(), old_value + delta)
    });
}

#[test]
fn test_aggregation() {
    global_info_logger();
    let graph = Graph::new();
    graph.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
    graph.add_edge(0, 1, 0, NO_PROPS, None).unwrap();
    graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
    graph.add_edge(0, 2, 1, NO_PROPS, None).unwrap();
    graph.add_edge(0, 0, 3, NO_PROPS, None).unwrap();
    graph.add_edge(0, 3, 0, NO_PROPS, None).unwrap();

    test_storage!(&graph, |graph| {
        let partition = Partition::from_iter([0usize, 0, 1, 1]);
        let mut m = ModularityUnDir::new(graph, None, 1.0, partition, 1e-8);
        let value_before = m.value();
        let _ = m.aggregate();
        let value_after = m.value();
        info!("before: {value_before}, after: {value_after}");
        assert_eq!(value_after, value_before);
        let delta = m.move_delta(&VID(0), ComID(1));
        m.move_node(&VID(0), ComID(1));
        let value_merged = m.value();
        assert_eq!(value_merged, 0.0);
        assert!((value_merged - (value_after + delta)).abs() < 1e-8);
    });
}
