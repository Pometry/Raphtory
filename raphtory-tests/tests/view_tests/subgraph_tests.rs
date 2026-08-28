use ahash::HashSet;
use itertools::Itertools;
use proptest::{proptest, sample::subsequence};
use raphtory::{
    algorithms::{components::weakly_connected_components, motifs::triangle_count::triangle_count},
    db::graph::{assertions::assert_graph_equal, views::deletion_graph::PersistentGraph},
    prelude::*,
};
use raphtory_storage::mutation::addition_ops::InternalAdditionOps;
use raphtory_tests::{
    test_storage,
    utils::{build_graph, build_graph_strat},
};
use serde_json::json;
use std::collections::BTreeSet;

#[test]
fn test_materialize_no_edges() {
    let graph = Graph::new();

    graph.add_node(1, 1, NO_PROPS, None, None).unwrap();
    graph.add_node(2, 2, NO_PROPS, None, None).unwrap();

    test_storage!(&graph, |graph| {
        let sg = graph.subgraph([1, 2, 1]); // <- duplicated nodes should have no effect

        let actual = sg.materialize().unwrap().into_events().unwrap();
        assert_graph_equal(&actual, &sg);
    });
}

#[test]
fn test_remove_degree1_triangle_count() {
    let graph = Graph::new();
    let edges = vec![
        (1, 2, 1),
        (1, 3, 2),
        (1, 4, 3),
        (3, 1, 4),
        (3, 4, 5),
        (3, 5, 6),
        (4, 5, 7),
        (5, 6, 8),
        (5, 8, 9),
        (7, 5, 10),
        (8, 5, 11),
        (1, 9, 12),
        (9, 1, 13),
        (6, 3, 14),
        (4, 8, 15),
        (8, 3, 16),
        (5, 10, 17),
        (10, 5, 18),
        (10, 8, 19),
        (1, 11, 20),
        (11, 1, 21),
        (9, 11, 22),
        (11, 9, 23),
    ];
    for (src, dst, ts) in edges {
        graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
    }
    test_storage!(&graph, |graph| {
        let subgraph = graph.subgraph(graph.nodes().into_iter().filter(|v| v.degree() > 1));
        let ts = triangle_count(&subgraph, None);
        let tg = triangle_count(graph, None);
        assert_eq!(ts, tg)
    });
}

#[test]
fn layer_materialize() {
    let graph = Graph::new();
    graph.add_edge(0, 1, 2, NO_PROPS, Some("1")).unwrap();
    graph.add_edge(0, 3, 4, NO_PROPS, Some("2")).unwrap();

    test_storage!(&graph, |graph| {
        let sg = graph.subgraph([1, 2]);
        let sgm = sg.materialize().unwrap();
        assert_eq!(
            sg.unique_layers().collect_vec(),
            sgm.unique_layers().collect_vec()
        );
    });
}

#[test]
fn test_cc() {
    let graph = Graph::new();
    graph.add_node(0, 0, NO_PROPS, None, None).unwrap();
    graph.add_node(0, 3, NO_PROPS, None, None).unwrap();
    graph.add_node(1, 2, NO_PROPS, None, None).unwrap();
    graph.add_node(1, 4, NO_PROPS, None, None).unwrap();
    graph.add_edge(0, 0, 1, NO_PROPS, Some("1")).unwrap();
    graph.add_edge(1, 3, 4, NO_PROPS, Some("1")).unwrap();
    let sg = graph.subgraph([0, 1, 3, 4]);
    let cc = weakly_connected_components(&sg);
    let groups = cc.groups();
    let group_sets = groups
        .iter()
        .map(|(_, g)| {
            g.iter()
                .map(|node| node.id())
                .sorted()
                .collect::<BTreeSet<_>>()
        })
        .collect::<HashSet<_>>();
    assert_eq!(
        group_sets,
        HashSet::from_iter([
            BTreeSet::from([GID::U64(0), GID::U64(1)]),
            BTreeSet::from([GID::U64(3), GID::U64(4)])
        ])
    );
}

#[test]
fn test_layer_edges() {
    let graph = Graph::new();
    graph.add_edge(0, 0, 1, NO_PROPS, Some("1")).unwrap();
    graph.add_edge(1, 0, 1, NO_PROPS, Some("2")).unwrap();

    assert_eq!(
        graph.subgraph([0, 1]).edges().id().collect_vec(),
        [(GID::U64(0), GID::U64(1))]
    );
    assert_eq!(
        graph
            .subgraph([0, 1])
            .valid_layers("1")
            .edges()
            .id()
            .collect_vec(),
        [(GID::U64(0), GID::U64(1))]
    );
}

#[test]
fn nodes_without_updates_are_filtered() {
    let g = Graph::new();
    g.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
    let expected = Graph::new();
    expected.resolve_layer(None).unwrap();
    let subgraph = g.subgraph([0]);
    assert_graph_equal(&subgraph, &expected);
}

#[test]
fn materialize_proptest() {
    proptest!(|(graph in build_graph_strat(10, 10, 10, 10, false), nodes in subsequence((0..10).collect::<Vec<_>>(), 0..10))| {
        let graph = Graph::from(build_graph(&graph));
        let subgraph = graph.subgraph(nodes);
        assert_graph_equal(&subgraph, &subgraph.materialize().unwrap());
    })
}

#[test]
fn materialize_proptest_failure() {
    let graph_f = serde_json::from_value(json!({"nodes":{},"edges":[[[1,1,"a"],{"props":{"t_props":[[0,[]]],"c_props":[]},"deletions":[]}],[[0,0,null],{"props":{"t_props":[[0,[]]],"c_props":[]},"deletions":[]}]]})).unwrap();
    let graph = Graph::from(build_graph(&graph_f));
    let subgraph = graph.subgraph([1]);
    let nodes = subgraph.default_layer().nodes().id().collect_vec();
    dbg!(nodes);
    assert_eq!(subgraph.default_layer().count_nodes(), 0);
    assert_eq!(subgraph.count_edges(), 1);
    let materialised = subgraph.materialize().unwrap();
    assert_graph_equal(&subgraph, &materialised);
}

#[test]
fn materialize_persistent_proptest() {
    proptest!(|(graph in build_graph_strat(10, 10, 10, 10, true), nodes in subsequence((0..10).collect::<Vec<_>>(), 0..10))| {
        let graph = PersistentGraph::from(build_graph(&graph));
        let subgraph = graph.subgraph(nodes);
        assert_graph_equal(&subgraph, &subgraph.materialize().unwrap());
    })
}

#[test]
fn test_subgraph_only_deletion() {
    let g = PersistentGraph::new();
    g.delete_edge(0, 0, 1, None).unwrap();
    let sg = g.subgraph([0]);
    let expected = PersistentGraph::new();
    expected.resolve_layer(None).unwrap();
    assert_graph_equal(&sg, &expected);
}
