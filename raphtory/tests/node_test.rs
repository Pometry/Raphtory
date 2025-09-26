use raphtory::prelude::*;
use raphtory_api::core::storage::arc_str::ArcStr;
use raphtory_core::storage::timeindex::AsTime;
use std::collections::HashMap;

use crate::test_utils::test_graph;

pub mod test_utils;

#[test]
fn test_earliest_time() {
    let graph = Graph::new();
    graph.add_node(0, 1, NO_PROPS, None).unwrap();
    graph.add_node(1, 1, NO_PROPS, None).unwrap();
    graph.add_node(2, 1, NO_PROPS, None).unwrap();

    // FIXME: Node add without properties not showing up (Issue #46)
    test_graph(&graph, |graph| {
        let view = graph.before(2);
        assert_eq!(view.node(1).expect("v").earliest_time().unwrap(), 0);
        assert_eq!(view.node(1).expect("v").latest_time().unwrap(), 1);

        let view = graph.before(3);
        assert_eq!(view.node(1).expect("v").earliest_time().unwrap(), 0);
        assert_eq!(view.node(1).expect("v").latest_time().unwrap(), 2);

        let view = graph.after(0);
        assert_eq!(view.node(1).expect("v").earliest_time().unwrap(), 1);
        assert_eq!(view.node(1).expect("v").latest_time().unwrap(), 2);

        let view = graph.after(2);
        assert_eq!(view.node(1), None);
        assert_eq!(view.node(1), None);

        let view = graph.at(1);
        assert_eq!(view.node(1).expect("v").earliest_time().unwrap(), 1);
        assert_eq!(view.node(1).expect("v").latest_time().unwrap(), 1);
    });
}

#[test]
fn test_properties() {
    let graph = Graph::new();
    let props = [("test", "test")];
    graph.add_node(0, 1, NO_PROPS, None).unwrap();
    graph.add_node(2, 1, props, None).unwrap();

    // FIXME: Node add without properties not showing up (Issue #46)
    test_graph(&graph, |graph| {
        let v1 = graph.node(1).unwrap();
        let v1_w = graph.window(0, 1).node(1).unwrap();
        assert_eq!(
            v1.properties().as_map(),
            [(ArcStr::from("test"), Prop::str("test"))].into()
        );
        assert_eq!(v1_w.properties().as_map(), HashMap::default())
    });
}

#[test]
fn test_property_additions() {
    let graph = Graph::new();
    let props = [("test", "test")];
    let v1 = graph.add_node(0, 1, NO_PROPS, None).unwrap();
    v1.add_updates(2, props).unwrap();
    let v1_w = v1.window(0, 1);
    assert_eq!(
        v1.properties().as_map(),
        props
            .into_iter()
            .map(|(k, v)| (k.into(), v.into_prop()))
            .collect()
    );
    assert_eq!(v1_w.properties().as_map(), HashMap::default())
}

#[test]
fn test_metadata_additions() {
    let g = Graph::new();
    let v1 = g.add_node(0, 1, NO_PROPS, None).unwrap();
    v1.add_metadata([("test", "test")]).unwrap();
    assert_eq!(v1.metadata().get("test"), Some("test".into()))
}

#[test]
fn test_metadata_updates() {
    let g = Graph::new();
    let v1 = g.add_node(0, 1, NO_PROPS, None).unwrap();
    v1.add_metadata([("test", "test")]).unwrap();
    v1.update_metadata([("test", "test2")]).unwrap();
    assert_eq!(v1.metadata().get("test"), Some("test2".into()))
}

#[test]
fn test_string_deduplication() {
    let g = Graph::new();
    let v1 = g
        .add_node(0, 1, [("test1", "test"), ("test2", "test")], None)
        .unwrap();
    let s1 = v1.properties().get("test1").unwrap_str();
    let s2 = v1.properties().get("test2").unwrap_str();

    assert_eq!(s1.as_ptr(), s2.as_ptr())
}

#[test]
fn test_edge_history_and_timestamps() {
    let graph = Graph::new();

    // Add nodes
    graph.add_node(0, 1, NO_PROPS, None).unwrap();
    graph.add_node(0, 2, NO_PROPS, None).unwrap();
    graph.add_node(0, 3, NO_PROPS, None).unwrap();

    // Add edges at different times
    graph.add_edge(10, 1, 2, NO_PROPS, None).unwrap();
    graph.add_edge(20, 1, 3, NO_PROPS, None).unwrap();
    graph.add_edge(30, 2, 1, NO_PROPS, None).unwrap();
    graph.add_edge(5, 1, 3, NO_PROPS, None).unwrap(); // Earlier edge to same pair

    test_storage!(&graph, |graph| {
        let node1 = graph.node(1).unwrap();

        // Test edge_history (chronological order)
        let history: Vec<_> = node1.edge_history().map(|(t, _)| t.t()).collect();
        assert_eq!(history, vec![5, 10, 20, 30]);

        // Test edge_history_rev (reverse chronological order)
        let history_rev: Vec<_> = node1.edge_history_rev().map(|(t, _)| t.t()).collect();
        assert_eq!(history_rev, vec![30, 20, 10, 5]);

        // Test earliest_edge_time
        assert_eq!(node1.earliest_edge_time(), Some(5));

        // Test latest_edge_time
        assert_eq!(node1.latest_edge_time(), Some(30));
    });
}

#[test]
fn test_edge_timestamps_with_windows() {
    let graph = Graph::new();

    // Add nodes
    graph.add_node(0, 1, NO_PROPS, None).unwrap();
    graph.add_node(0, 2, NO_PROPS, None).unwrap();
    graph.add_node(0, 3, NO_PROPS, None).unwrap();

    // Add edges at different times
    graph.add_edge(5, 1, 2, NO_PROPS, None).unwrap();
    graph.add_edge(15, 1, 3, NO_PROPS, None).unwrap();
    graph.add_edge(25, 2, 1, NO_PROPS, None).unwrap();
    graph.add_edge(35, 1, 3, NO_PROPS, None).unwrap();

    test_graph(&graph, |graph| {
        // Test window 0-20
        let windowed = graph.window(0, 20);
        let node1 = windowed.node(1).unwrap();

        let history: Vec<_> = node1.edge_history().map(|(t, _)| t.t()).collect();
        assert_eq!(history, vec![5, 15]);

        assert_eq!(node1.earliest_edge_time(), Some(5));
        assert_eq!(node1.latest_edge_time(), Some(15));

        // Test window 10-30
        let windowed = graph.window(10, 30);
        let node1 = windowed.node(1).unwrap();

        let history: Vec<_> = node1.edge_history().map(|(t, _)| t.t()).collect();
        assert_eq!(history, vec![15, 25]);

        assert_eq!(node1.earliest_edge_time(), Some(15));
        assert_eq!(node1.latest_edge_time(), Some(25));

        // Test window after all edges
        let windowed = graph.after(40);
        assert_eq!(windowed.node(1), None); // Node has no edges in this window
    });
}

#[test]
fn test_edge_timestamps_with_layers() {
    let graph = Graph::new();

    // Add nodes
    graph.add_node(0, 1, NO_PROPS, None).unwrap();
    graph.add_node(0, 2, NO_PROPS, None).unwrap();
    graph.add_node(0, 3, NO_PROPS, None).unwrap();

    // Add edges on different layers
    graph.add_edge(10, 1, 2, NO_PROPS, Some("layer1")).unwrap();
    graph.add_edge(20, 1, 3, NO_PROPS, Some("layer2")).unwrap();
    graph.add_edge(30, 2, 1, NO_PROPS, Some("layer1")).unwrap();
    graph.add_edge(5, 1, 3, NO_PROPS, Some("layer2")).unwrap();

    test_graph(&graph, |graph| {
        // Test all layers
        let node1 = graph.node(1).unwrap();
        let history: Vec<_> = node1.edge_history().map(|(t, _)| t.t()).collect();
        assert_eq!(history, vec![5, 10, 20, 30]);
        assert_eq!(node1.earliest_edge_time(), Some(5));
        assert_eq!(node1.latest_edge_time(), Some(30));

        // Test layer1 only
        let layer1_graph = graph.layers(vec!["layer1"]).unwrap();
        let node1_layer1 = layer1_graph.node(1).unwrap();
        let history: Vec<_> = node1_layer1.edge_history().map(|(t, _)| t.t()).collect();
        assert_eq!(history, vec![10, 30]);
        assert_eq!(node1_layer1.earliest_edge_time(), Some(10));
        assert_eq!(node1_layer1.latest_edge_time(), Some(30));

        // Test layer2 only
        let layer2_graph = graph.layers(vec!["layer2"]).unwrap();
        let node1_layer2 = layer2_graph.node(1).unwrap();
        let history: Vec<_> = node1_layer2.edge_history().map(|(t, _)| t.t()).collect();
        assert_eq!(history, vec![5, 20]);
        assert_eq!(node1_layer2.earliest_edge_time(), Some(5));
        assert_eq!(node1_layer2.latest_edge_time(), Some(20));
    });
}

#[test]
fn test_edge_timestamps_overlapping_windows_and_layers() {
    let graph = Graph::new();

    // Add nodes
    graph.add_node(0, 1, NO_PROPS, None).unwrap();
    graph.add_node(0, 2, NO_PROPS, None).unwrap();
    graph.add_node(0, 3, NO_PROPS, None).unwrap();
    graph.add_node(0, 4, NO_PROPS, None).unwrap();

    // Add edges with overlapping time ranges across multiple layers
    graph.add_edge(5, 1, 2, NO_PROPS, Some("layer1")).unwrap();
    graph.add_edge(10, 1, 3, NO_PROPS, Some("layer1")).unwrap();
    graph.add_edge(15, 1, 4, NO_PROPS, Some("layer2")).unwrap();
    graph.add_edge(20, 2, 1, NO_PROPS, Some("layer1")).unwrap();
    graph.add_edge(25, 3, 1, NO_PROPS, Some("layer2")).unwrap();
    graph.add_edge(30, 1, 2, NO_PROPS, Some("layer1")).unwrap();
    graph.add_edge(35, 1, 4, NO_PROPS, Some("layer2")).unwrap();

    test_graph(&graph, |graph| {
        // Test overlapping window (8-22) with layer1
        let windowed_layer1 = graph.window(8, 22).layers(vec!["layer1"]).unwrap();
        let node1 = windowed_layer1.node(1).unwrap();

        let history: Vec<_> = node1.edge_history().map(|(t, _)| t.t()).collect();
        assert_eq!(history, vec![10, 20]);
        assert_eq!(node1.earliest_edge_time(), Some(10));
        assert_eq!(node1.latest_edge_time(), Some(20));

        // Test overlapping window (12-28) with layer2
        let windowed_layer2 = graph.window(12, 28).layers(vec!["layer2"]).unwrap();
        let node1 = windowed_layer2.node(1).unwrap();

        let history: Vec<_> = node1.edge_history().map(|(t, _)| t.t()).collect();
        assert_eq!(history, vec![15, 25]);
        assert_eq!(node1.earliest_edge_time(), Some(15));
        assert_eq!(node1.latest_edge_time(), Some(25));

        // Test overlapping window (18-32) with both layers
        let windowed_both = graph
            .window(18, 32)
            .layers(vec!["layer1", "layer2"])
            .unwrap();
        let node1 = windowed_both.node(1).unwrap();

        let history: Vec<_> = node1.edge_history().map(|(t, _)| t.t()).collect();
        assert_eq!(history, vec![20, 25, 30]);
        assert_eq!(node1.earliest_edge_time(), Some(20));
        assert_eq!(node1.latest_edge_time(), Some(30));

        // Test edge case: window with no edges for the node
        let empty_window = graph.window(50, 60);
        assert_eq!(empty_window.node(1), None);
    });
}

#[test]
fn test_edge_timestamps_no_edges() {
    let graph = Graph::new();

    // Add a node but no edges
    graph.add_node(10, 1, NO_PROPS, None).unwrap();

    test_graph(&graph, |graph| {
        let node1 = graph.node(1).unwrap();

        // Node exists but has no edges
        assert_eq!(node1.edge_history().count(), 0);
        assert_eq!(node1.edge_history_rev().count(), 0);
        assert_eq!(node1.earliest_edge_time(), None);
        assert_eq!(node1.latest_edge_time(), None);
    });
}
