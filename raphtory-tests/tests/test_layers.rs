use itertools::Itertools;
use proptest::proptest;
use raphtory::{
    db::graph::{graph::assert_graph_equal, views::deletion_graph::PersistentGraph},
    prelude::*,
};
use raphtory_api::core::entities::GID;
use raphtory_tests::{
    test_storage,
    utils::{build_graph, build_graph_layer, build_graph_strat, GraphFixture},
};
use serde_json::json;

#[test]
fn proptest_layering() {
    proptest!(|(graph_f in build_graph_strat(10, 10, 10, 10, false), layer in proptest::sample::subsequence(&["_default", "a", "b"], 0..3))| {
        let g_layer_expected = Graph::from(build_graph_layer(&graph_f, &layer));
        let g = Graph::from(build_graph(&graph_f));
            let g_layer = g.valid_layers(layer.clone());
            assert_graph_equal(&g_layer, &g_layer_expected);
    })
}

#[test]
fn test_node_explicit_node_additions() {
    let graph_f: GraphFixture = serde_json::from_value(json!({"nodes":{"10":{"props":{"t_props":[[0,[]]],"c_props":[]},"node_type":null}},"edges":[]})).unwrap();
    let layer = [];
    let g_layer_expected = Graph::from(build_graph_layer(&graph_f, &layer));
    let g = Graph::from(build_graph(&graph_f));
    let g_layer = g.valid_layers(layer.clone());

    assert_graph_equal(&g_layer, &g_layer_expected);
}

#[test]
fn test_failure() {
    let graph_f: GraphFixture = serde_json::from_value(json!({"nodes":{},"edges":[[[0,0,"a"],{"props":{"t_props":[[0,[]]],"c_props":[]},"deletions":[]}],[[3,9,"b"],{"props":{"t_props":[[0,[]]],"c_props":[]},"deletions":[]}],[[9,3,"b"],{"props":{"t_props":[[0,[]]],"c_props":[]},"deletions":[]}],[[0,0,null],{"props":{"t_props":[[0,[]]],"c_props":[]},"deletions":[]}]]})).unwrap();
    let layer = ["_default", "b"];
    let g_layer_expected = Graph::from(build_graph_layer(&graph_f, &layer));
    let g = Graph::from(build_graph(&graph_f));
    let g_layer = g.valid_layers(layer.clone());

    assert_graph_equal(&g_layer, &g_layer_expected);
}

#[test]
fn test_failure2() {
    let graph_f: GraphFixture = serde_json::from_value(json!({"nodes":{},"edges":[[[0,0,null],{"props":{"t_props":[[0,[]]],"c_props":[]},"deletions":[]}],[[0,0,"a"],{"props":{"t_props":[[0,[]]],"c_props":[]},"deletions":[]}],[[0,0,"b"],{"props":{"t_props":[[0,[]]],"c_props":[]},"deletions":[]}]]})).unwrap();
    let layer = ["_default", "b"];
    let g_layer_expected = Graph::from(build_graph_layer(&graph_f, &layer));
    let g = Graph::from(build_graph(&graph_f));
    let g_layer = g.valid_layers(layer.clone());

    assert_graph_equal(&g_layer, &g_layer_expected);
}

#[test]
fn test_failure3() {
    let graph_f: GraphFixture = serde_json::from_value(json!({"nodes":{},"edges":[[[0,0,null],{"props":{"t_props":[[0,[]]],"c_props":[]},"deletions":[]}],[[0,0,"b"],{"props":{"t_props":[[0,[]]],"c_props":[]},"deletions":[]}],[[0,1,"a"],{"props":{"t_props":[[0,[]]],"c_props":[]},"deletions":[]}]]})).unwrap();
    let layer = ["_default", "b"];
    let g_layer_expected = Graph::from(build_graph_layer(&graph_f, &layer));
    let g = Graph::from(build_graph(&graph_f));
    let g_layer = g.valid_layers(layer.clone());

    assert_graph_equal(&g_layer, &g_layer_expected);
}

// Regression for the build_graph_layer node-layer filter
#[test]
fn test_node_layer_visibility_under_valid_layers() {
    let graph_f: GraphFixture = serde_json::from_value(json!({
        "nodes": {
            "1": {"props":{"t_props":[[0,[]]],"c_props":[]}, "node_type": null, "node_layer": null},
            "2": {"props":{"t_props":[[0,[]]],"c_props":[]}, "node_type": null, "node_layer": "a"},
            "3": {"props":{"t_props":[[0,[]]],"c_props":[]}, "node_type": null, "node_layer": "b"}
        },
        "edges": []
    }))
    .unwrap();

    let layer = ["b"];
    let g_layer_expected = Graph::from(build_graph_layer(&graph_f, &layer));
    let g = Graph::from(build_graph(&graph_f));
    let g_layer = g.valid_layers(layer.clone());

    assert_graph_equal(&g_layer, &g_layer_expected);
}

#[test]
fn proptest_layering_persistent_graph() {
    proptest!(|(graph_f in build_graph_strat(10, 10, 10, 10, true), layer in proptest::sample::subsequence(&["_default", "a", "b"], 0..3))| {
        let g_layer_expected = PersistentGraph::from(build_graph_layer(&graph_f, &layer));
        let g = PersistentGraph::from(build_graph(&graph_f));
        let g_layer = g.valid_layers(layer);
        assert_graph_equal(&g_layer, &g_layer_expected);
    })
}

#[test]
fn test_layer_node() {
    let graph = Graph::new();

    graph.add_edge(0, 1, 2, NO_PROPS, Some("layer1")).unwrap();
    graph.add_edge(0, 2, 3, NO_PROPS, Some("layer2")).unwrap();
    graph.add_edge(3, 2, 4, NO_PROPS, Some("layer1")).unwrap();
    graph.add_edge(1, 4, 1, NO_PROPS, Some("layer3")).unwrap();

    test_storage!(&graph, |graph| {
        let neighbours = graph
            .layers(vec!["layer1", "layer2"])
            .unwrap()
            .node(1)
            .unwrap()
            .neighbours()
            .into_iter()
            .collect_vec();
        assert_eq!(
            neighbours[0]
                .layers("layer2")
                .unwrap()
                .edges()
                .id()
                .collect_vec(),
            vec![(GID::U64(2), GID::U64(3))]
        );
        assert_eq!(
            graph
                .layers("layer2")
                .unwrap()
                .node(neighbours[0].name())
                .unwrap()
                .edges()
                .id()
                .collect_vec(),
            vec![(GID::U64(2), GID::U64(3))]
        );
        let mut edges = graph
            .layers("layer1")
            .unwrap()
            .node(neighbours[0].name())
            .unwrap()
            .edges()
            .id()
            .filter_map(|(a, b)| a.to_u64().zip(b.to_u64()))
            .collect_vec();
        edges.sort();
        assert_eq!(edges, vec![(1, 2), (2, 4)]);
        let mut edges = graph
            .layers("layer1")
            .unwrap()
            .edges()
            .id()
            .filter_map(|(a, b)| a.to_u64().zip(b.to_u64()))
            .collect_vec();
        edges.sort();
        assert_eq!(edges, vec![(1, 2), (2, 4)]);
        let mut edges = graph
            .layers(vec!["layer1", "layer2"])
            .unwrap()
            .edges()
            .id()
            .filter_map(|(a, b)| a.to_u64().zip(b.to_u64()))
            .collect_vec();
        edges.sort();
        assert_eq!(edges, vec![(1, 2), (2, 3), (2, 4)]);

        let mut edges = graph
            .layers(["layer1", "layer3"])
            .unwrap()
            .window(0, 2)
            .edges()
            .id()
            .filter_map(|(a, b)| a.to_u64().zip(b.to_u64()))
            .collect_vec();
        edges.sort();
        assert_eq!(edges, vec![(1, 2), (4, 1)]);
    });
}

#[test]
fn layering_tests() {
    let graph = Graph::new();
    let e1 = graph.add_edge(0, 1, 2, NO_PROPS, Some("1")).unwrap();
    graph.add_edge(1, 1, 2, NO_PROPS, Some("2")).unwrap();

    println!("edge: {e1:?}");
    // FIXME: this is weird, see issue #1458
    assert!(e1.has_layer("2"));
    let history = e1.layers("2").unwrap().history();
    println!("history: {:?}", history);
    assert!(e1.layers("2").unwrap().history().is_empty());

    test_storage!(&graph, |graph| {
        let e = graph.edge(1, 2).unwrap();
        // layers with non-existing layers errors
        assert!(e.layers(["1", "3"]).is_err());
        // valid_layers ignores non-existing layers
        assert_eq!(e.valid_layers(["1", "3"]).layer_names(), ["1"]);
        assert!(e.has_layer("1"));
        assert!(e.has_layer("2"));
        assert!(!e.has_layer("3"));
        assert!(e.valid_layers("1").has_layer("1"));
        assert!(!e.valid_layers("1").has_layer("2"));
    });
}
