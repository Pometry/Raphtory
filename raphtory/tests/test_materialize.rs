use crate::test_utils::{build_edge_list, build_graph_from_edge_list};
use proptest::{arbitrary::any, proptest};
use raphtory::{
    db::graph::graph::{assert_graph_equal, assert_graph_equal_timestamps},
    prelude::*,
};
use raphtory_api::core::storage::arc_str::OptionAsStr;
use raphtory_storage::core_ops::CoreGraphOps;
use std::ops::Range;

pub mod test_utils;

#[test]
fn test_materialize() {
    let g = Graph::new();
    g.add_edge(0, 1, 2, [("layer1", "1")], Some("1")).unwrap();
    g.add_edge(0, 1, 2, [("layer2", "2")], Some("2")).unwrap();

    let gm = g.materialize().unwrap();

    assert_graph_equal(&g, &gm);
    assert_eq!(
        gm.nodes().name().iter_values().collect::<Vec<String>>(),
        vec!["1", "2"]
    );

    assert!(g
        .layers("2")
        .unwrap()
        .edge(1, 2)
        .unwrap()
        .properties()
        .temporal()
        .get("layer1")
        .and_then(|prop| prop.latest())
        .is_none());
    assert!(gm
        .into_events()
        .unwrap()
        .layers("2")
        .unwrap()
        .edge(1, 2)
        .unwrap()
        .properties()
        .temporal()
        .get("layer1")
        .and_then(|prop| prop.latest())
        .is_none());
}

#[test]
fn test_graph_properties() {
    let g = Graph::new();
    g.add_properties(1, [("test", "test")]).unwrap();
    g.add_metadata([("test_metadata", "test2")]).unwrap();

    test_storage!(&g, |g| {
        let gm = g.materialize().unwrap();
        assert_graph_equal(&g, &gm);
    });
}

#[test]
fn materialize_prop_test() {
    proptest!(|(edges in build_edge_list(100, 100), w in any::<Range<i64>>())| {
        let g = build_graph_from_edge_list(&edges);
        test_storage!(&g, |g| {
            let gm = g.materialize().unwrap();
            assert_graph_equal_timestamps(&g, &gm);
            let gw = g.window(w.start, w.end);
            let gmw = gw.materialize().unwrap();
            assert_graph_equal_timestamps(&gw, &gmw);
        });
    })
}

#[test]
fn test_subgraph() {
    let g = Graph::new();
    g.add_node(0, 1, NO_PROPS, None).unwrap();
    g.add_node(0, 2, NO_PROPS, None).unwrap();
    g.add_node(0, 3, NO_PROPS, None).unwrap();
    g.add_node(0, 4, NO_PROPS, None).unwrap();
    g.add_node(0, 5, NO_PROPS, None).unwrap();

    let nodes_subgraph = g.subgraph(vec![4, 5]);
    assert_eq!(
        nodes_subgraph
            .nodes()
            .name()
            .iter_values()
            .collect::<Vec<String>>(),
        vec!["4", "5"]
    );
    let gm = nodes_subgraph.materialize().unwrap();
    assert_graph_equal(&nodes_subgraph, &gm);
}

#[test]
fn test_exclude_nodes() {
    let g = Graph::new();
    g.add_node(0, 1, NO_PROPS, None).unwrap();
    g.add_node(0, 2, NO_PROPS, None).unwrap();
    g.add_node(0, 3, NO_PROPS, None).unwrap();
    g.add_node(0, 4, NO_PROPS, None).unwrap();
    g.add_node(0, 5, NO_PROPS, None).unwrap();

    let exclude_nodes_subgraph = g.exclude_nodes(vec![4, 5]);
    assert_eq!(
        exclude_nodes_subgraph
            .nodes()
            .name()
            .iter_values()
            .collect::<Vec<String>>(),
        vec!["1", "2", "3"]
    );
    let gm = exclude_nodes_subgraph.materialize().unwrap();
    assert_graph_equal(&exclude_nodes_subgraph, &gm);
}

#[test]
fn testing_node_types() {
    let graph = Graph::new();
    graph.add_node(0, "A", NO_PROPS, None).unwrap();
    graph.add_node(1, "B", NO_PROPS, Some("H")).unwrap();

    test_storage!(&graph, |graph| {
        let node_a = graph.node("A").unwrap();
        let node_b = graph.node("B").unwrap();
        let node_a_type = node_a.node_type();
        let node_a_type_str = node_a_type.as_str();

        assert_eq!(node_a_type_str, None);
        assert_eq!(node_b.node_type().as_str(), Some("H"));
    });

    // Nodes with No type can be overwritten
    let node_a = graph.add_node(1, "A", NO_PROPS, Some("TYPEA")).unwrap();
    assert_eq!(node_a.node_type().as_str(), Some("TYPEA"));

    // Check that overwriting a node type returns an error
    assert!(graph.add_node(2, "A", NO_PROPS, Some("TYPEB")).is_err());
    // Double check that the type did not actually change
    assert_eq!(graph.node("A").unwrap().node_type().as_str(), Some("TYPEA"));
    // Check that the update is not added to the graph
    let all_node_types = graph.get_all_node_types();
    assert_eq!(all_node_types.len(), 2);
}

#[test]
fn changing_property_type_errors() {
    let g = Graph::new();
    let props_0 = [("test", Prop::U64(1))];
    let props_1 = [("test", Prop::F64(0.1))];
    g.add_properties(0, props_0.clone()).unwrap();
    assert!(g.add_properties(1, props_1.clone()).is_err());

    g.add_node(0, 1, props_0.clone(), None).unwrap();
    assert!(g.add_node(1, 1, props_1.clone(), None).is_err());

    g.add_edge(0, 1, 2, props_0.clone(), None).unwrap();
    assert!(g.add_edge(1, 1, 2, props_1.clone(), None).is_err());
}

#[test]
fn test_edge_layer_properties() {
    let g = Graph::new();
    g.add_edge(1, "A", "B", [("greeting", "howdy")], Some("layer 1"))
        .unwrap();
    g.add_edge(2, "A", "B", [("greeting", "ola")], Some("layer 2"))
        .unwrap();
    g.add_edge(2, "A", "B", [("greeting", "hello")], Some("layer 2"))
        .unwrap();
    g.add_edge(3, "A", "B", [("greeting", "namaste")], Some("layer 3"))
        .unwrap();

    let edge_ab = g.edge("A", "B").unwrap();
    let props = edge_ab
        .properties()
        .iter()
        .filter_map(|(k, v)| v.map(move |v| (k.to_string(), v.to_string())))
        .collect::<Vec<_>>();
    assert_eq!(props, vec![("greeting".to_string(), "namaste".to_string())]);
}
