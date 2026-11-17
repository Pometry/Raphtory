use itertools::Itertools;
use raphtory::prelude::*;
use raphtory_api::core::storage::timeindex::AsTime;
use raphtory_tests::test_storage;

#[test]
fn test_add_node_properties_ordered_by_event_id() {
    let graph: Graph = Graph::new();
    graph
        .add_node((0, 3), 0, [("prop", "1")], None, None)
        .unwrap();
    graph
        .add_node((0, 2), 0, [("prop", "2")], None, None)
        .unwrap();
    graph
        .add_node((0, 1), 0, [("prop", "3")], None, None)
        .unwrap();

    let props = graph
        .node("0")
        .map(|node| {
            node.properties()
                .temporal()
                .get("prop")
                .unwrap()
                .values()
                .map(|x| x.to_string())
                .collect_vec()
        })
        .unwrap();

    assert_eq!(
        props,
        vec!["3".to_string(), "2".to_string(), "1".to_string()]
    );
}

#[test]
fn test_add_node_properties_overwritten_for_same_event_id() {
    let graph: Graph = Graph::new();
    graph
        .add_node((0, 1), 0, [("prop", "1")], None, None)
        .unwrap();
    graph
        .add_node((0, 1), 0, [("prop", "2")], None, None)
        .unwrap();
    graph
        .add_node((0, 1), 0, [("prop", "3")], None, None)
        .unwrap();

    let props = graph
        .node(0)
        .map(|node| {
            node.properties()
                .temporal()
                .get("prop")
                .unwrap()
                .values()
                .map(|x| x.to_string())
                .collect_vec()
        })
        .unwrap();

    assert_eq!(props, vec!["3".to_string()]);

    let graph: Graph = Graph::new();
    graph
        .add_node((0, 1), 0, [("prop", "1")], None, None)
        .unwrap();
    graph
        .add_node((0, 2), 0, [("prop", "2")], None, None)
        .unwrap();
    graph
        .add_node((0, 2), 0, [("prop", "3")], None, None)
        .unwrap();

    let props = graph
        .node(0)
        .map(|node| {
            node.properties()
                .temporal()
                .get("prop")
                .unwrap()
                .values()
                .map(|x| x.to_string())
                .collect_vec()
        })
        .unwrap();

    assert_eq!(props, vec!["1".to_string(), "3".to_string()]);
}

#[test]
fn test_create_node_properties_ordered_by_event_id() {
    let graph: Graph = Graph::new();
    graph
        .create_node((0, 3), 0, [("prop", "1")], None, None)
        .unwrap();
    graph
        .add_node((0, 2), 0, [("prop", "2")], None, None)
        .unwrap();
    graph
        .add_node((0, 1), 0, [("prop", "3")], None, None)
        .unwrap();

    let props = graph
        .node("0")
        .map(|node| {
            node.properties()
                .temporal()
                .get("prop")
                .unwrap()
                .values()
                .map(|x| x.to_string())
                .collect_vec()
        })
        .unwrap();

    assert_eq!(
        props,
        vec!["3".to_string(), "2".to_string(), "1".to_string()]
    );
}

#[test]
fn test_create_node_properties_overwritten_for_same_event_id() {
    let graph: Graph = Graph::new();
    graph
        .create_node((0, 1), 0, [("prop", "1")], None, None)
        .unwrap();
    graph
        .add_node((0, 1), 0, [("prop", "2")], None, None)
        .unwrap();
    graph
        .add_node((0, 1), 0, [("prop", "3")], None, None)
        .unwrap();

    let props = graph
        .node(0)
        .map(|node| {
            node.properties()
                .temporal()
                .get("prop")
                .unwrap()
                .values()
                .map(|x| x.to_string())
                .collect_vec()
        })
        .unwrap();

    assert_eq!(props, vec!["3".to_string()]);

    let graph: Graph = Graph::new();
    graph
        .create_node((0, 1), 0, [("prop", "1")], None, None)
        .unwrap();
    graph
        .add_node((0, 2), 0, [("prop", "2")], None, None)
        .unwrap();
    graph
        .add_node((0, 2), 0, [("prop", "3")], None, None)
        .unwrap();

    let props = graph
        .node(0)
        .map(|node| {
            node.properties()
                .temporal()
                .get("prop")
                .unwrap()
                .values()
                .map(|x| x.to_string())
                .collect_vec()
        })
        .unwrap();

    assert_eq!(props, vec!["1".to_string(), "3".to_string()]);
}

#[test]
fn test_add_edge_properties_ordered_by_event_id() {
    let graph: Graph = Graph::new();
    graph.add_edge((0, 3), 0, 1, [("prop", "1")], None).unwrap();
    graph.add_edge((0, 2), 0, 1, [("prop", "2")], None).unwrap();
    graph.add_edge((0, 1), 0, 1, [("prop", "3")], None).unwrap();

    let props = graph
        .edge(0, 1)
        .map(|edge| {
            edge.properties()
                .temporal()
                .get("prop")
                .unwrap()
                .values()
                .map(|x| x.to_string())
                .collect_vec()
        })
        .unwrap();

    assert_eq!(
        props,
        vec!["3".to_string(), "2".to_string(), "1".to_string()]
    );
}

#[test]
fn test_add_edge_properties_overwritten_for_same_event_id() {
    let graph: Graph = Graph::new();
    graph.add_edge((0, 1), 0, 1, [("prop", "1")], None).unwrap();
    graph.add_edge((0, 1), 0, 1, [("prop", "2")], None).unwrap();
    graph.add_edge((0, 1), 0, 1, [("prop", "3")], None).unwrap();

    let props = graph
        .edge(0, 1)
        .map(|edge| {
            edge.properties()
                .temporal()
                .get("prop")
                .unwrap()
                .values()
                .map(|x| x.to_string())
                .collect_vec()
        })
        .unwrap();

    assert_eq!(props, vec!["3".to_string()]);

    let graph: Graph = Graph::new();
    graph.add_edge((0, 1), 0, 1, [("prop", "1")], None).unwrap();
    graph.add_edge((0, 2), 0, 1, [("prop", "2")], None).unwrap();
    graph.add_edge((0, 2), 0, 1, [("prop", "3")], None).unwrap();

    let props = graph
        .edge(0, 1)
        .map(|edge| {
            edge.properties()
                .temporal()
                .get("prop")
                .unwrap()
                .values()
                .map(|x| x.to_string())
                .collect_vec()
        })
        .unwrap();

    assert_eq!(props, vec!["1".to_string(), "3".to_string()]);
}

#[test]
fn test_add_properties_properties_ordered_by_event_id() {
    let graph: Graph = Graph::new();
    graph.add_properties((0, 3), [("prop", "1")]).unwrap();
    graph.add_properties((0, 2), [("prop", "2")]).unwrap();
    graph.add_properties((0, 1), [("prop", "3")]).unwrap();

    let props = graph
        .properties()
        .temporal()
        .get("prop")
        .unwrap()
        .values()
        .map(|x| x.to_string())
        .collect_vec();

    assert_eq!(
        props,
        vec!["3".to_string(), "2".to_string(), "1".to_string()]
    );
}

#[test]
fn test_add_properties_properties_overwritten_for_same_event_id() {
    let graph: Graph = Graph::new();
    graph.add_properties((0, 1), [("prop", "1")]).unwrap();
    graph.add_properties((0, 1), [("prop", "2")]).unwrap();
    graph.add_properties((0, 1), [("prop", "3")]).unwrap();

    let props = graph
        .properties()
        .temporal()
        .get("prop")
        .unwrap()
        .values()
        .map(|x| x.to_string())
        .collect_vec();

    assert_eq!(props, vec!["3".to_string()]);

    let graph: Graph = Graph::new();
    graph.add_edge((0, 1), 0, 1, NO_PROPS, None).unwrap();
    graph.add_edge((0, 2), 0, 1, NO_PROPS, None).unwrap();
    graph.add_edge((0, 2), 0, 1, NO_PROPS, None).unwrap();

    graph.add_properties((0, 1), [("prop", "1")]).unwrap();
    graph.add_properties((0, 2), [("prop", "2")]).unwrap();
    graph.add_properties((0, 2), [("prop", "3")]).unwrap();

    let props = graph
        .properties()
        .temporal()
        .get("prop")
        .unwrap()
        .values()
        .map(|x| x.to_string())
        .collect_vec();

    assert_eq!(props, vec!["1".to_string(), "3".to_string()]);
}

#[test]
fn test_node_add_updates_properties_ordered_by_event_id() {
    let graph: Graph = Graph::new();
    graph.add_node(0, 0, NO_PROPS, None, None).unwrap();

    graph
        .node(0)
        .unwrap()
        .add_updates((0, 3), [("prop", "1")], None)
        .unwrap();
    graph
        .node(0)
        .unwrap()
        .add_updates((0, 2), [("prop", "2")], None)
        .unwrap();
    graph
        .node(0)
        .unwrap()
        .add_updates((0, 1), [("prop", "3")], None)
        .unwrap();

    let props = graph
        .node("0")
        .map(|node| {
            node.properties()
                .temporal()
                .get("prop")
                .unwrap()
                .values()
                .map(|x| x.to_string())
                .collect_vec()
        })
        .unwrap();

    assert_eq!(
        props,
        vec!["3".to_string(), "2".to_string(), "1".to_string()]
    );
}

#[test]
fn test_node_add_updates_properties_overwritten_for_same_event_id() {
    let graph: Graph = Graph::new();
    graph.add_node(0, 0, NO_PROPS, None, None).unwrap();
    graph.add_node(0, 0, NO_PROPS, None, None).unwrap();
    graph.add_node(0, 0, NO_PROPS, None, None).unwrap();

    graph
        .node(0)
        .unwrap()
        .add_updates((0, 1), [("prop", "1")], None)
        .unwrap();
    graph
        .node(0)
        .unwrap()
        .add_updates((0, 1), [("prop", "2")], None)
        .unwrap();
    graph
        .node(0)
        .unwrap()
        .add_updates((0, 1), [("prop", "3")], None)
        .unwrap();

    let props = graph
        .node("0")
        .map(|node| {
            node.properties()
                .temporal()
                .get("prop")
                .unwrap()
                .values()
                .map(|x| x.to_string())
                .collect_vec()
        })
        .unwrap();

    assert_eq!(props, vec!["3".to_string()]);

    let graph: Graph = Graph::new();
    graph.add_node(0, 0, NO_PROPS, None, None).unwrap();
    graph.add_node(0, 0, NO_PROPS, None, None).unwrap();
    graph.add_node(0, 0, NO_PROPS, None, None).unwrap();

    graph.add_node(0, 0, NO_PROPS, None, None).unwrap();
    graph.add_node(0, 0, NO_PROPS, None, None).unwrap();
    graph.add_node(0, 0, NO_PROPS, None, None).unwrap();

    graph
        .node(0)
        .unwrap()
        .add_updates((0, 1), [("prop", "1")], None)
        .unwrap();
    graph
        .node(0)
        .unwrap()
        .add_updates((0, 2), [("prop", "2")], None)
        .unwrap();
    graph
        .node(0)
        .unwrap()
        .add_updates((0, 2), [("prop", "3")], None)
        .unwrap();

    let props = graph
        .node("0")
        .map(|node| {
            node.properties()
                .temporal()
                .get("prop")
                .unwrap()
                .values()
                .map(|x| x.to_string())
                .collect_vec()
        })
        .unwrap();

    assert_eq!(props, vec!["1".to_string(), "3".to_string()]);
}

#[test]
fn test_edge_add_updates_properties_ordered_by_event_id() {
    let graph: Graph = Graph::new();
    graph.add_edge(0, 0, 1, NO_PROPS, None).unwrap();

    graph
        .edge(0, 1)
        .unwrap()
        .add_updates((0, 3), [("prop", "1")], None)
        .unwrap();
    graph
        .edge(0, 1)
        .unwrap()
        .add_updates((0, 2), [("prop", "2")], None)
        .unwrap();
    graph
        .edge(0, 1)
        .unwrap()
        .add_updates((0, 1), [("prop", "3")], None)
        .unwrap();

    let props = graph
        .edge(0, 1)
        .map(|edge| {
            edge.properties()
                .temporal()
                .get("prop")
                .unwrap()
                .values()
                .map(|x| x.to_string())
                .collect_vec()
        })
        .unwrap();

    assert_eq!(
        props,
        vec!["3".to_string(), "2".to_string(), "1".to_string()]
    );
}

#[test]
fn test_edge_add_updates_properties_overwritten_for_same_event_id() {
    let graph: Graph = Graph::new();
    graph.add_edge(0, 0, 1, NO_PROPS, None).unwrap();

    graph
        .edge(0, 1)
        .unwrap()
        .add_updates((0, 1), [("prop", "1")], None)
        .unwrap();
    graph
        .edge(0, 1)
        .unwrap()
        .add_updates((0, 1), [("prop", "2")], None)
        .unwrap();
    graph
        .edge(0, 1)
        .unwrap()
        .add_updates((0, 1), [("prop", "3")], None)
        .unwrap();

    let props = graph
        .edge(0, 1)
        .map(|edge| {
            edge.properties()
                .temporal()
                .get("prop")
                .unwrap()
                .values()
                .map(|x| x.to_string())
                .collect_vec()
        })
        .unwrap();

    assert_eq!(props, vec!["3".to_string()]);

    let graph: Graph = Graph::new();
    graph.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
    graph.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
    graph.add_edge(0, 0, 1, NO_PROPS, None).unwrap();

    graph
        .edge(0, 1)
        .unwrap()
        .add_updates((0, 1), [("prop", "1")], None)
        .unwrap();
    graph
        .edge(0, 1)
        .unwrap()
        .add_updates((0, 2), [("prop", "2")], None)
        .unwrap();
    graph
        .edge(0, 1)
        .unwrap()
        .add_updates((0, 2), [("prop", "3")], None)
        .unwrap();

    let props = graph
        .edge(0, 1)
        .map(|edge| {
            edge.properties()
                .temporal()
                .get("prop")
                .unwrap()
                .values()
                .map(|x| x.to_string())
                .collect_vec()
        })
        .unwrap();

    assert_eq!(props, vec!["1".to_string(), "3".to_string()]);
}

#[test]
fn test_exploded_edges() {
    let graph: Graph = Graph::new();
    graph.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
    graph.add_edge(1, 0, 1, NO_PROPS, None).unwrap();
    graph.add_edge(2, 0, 1, NO_PROPS, None).unwrap();
    graph.add_edge(3, 0, 1, NO_PROPS, None).unwrap();
    test_storage!(&graph, |graph| {
        assert_eq!(graph.count_temporal_edges(), 4)
    });
}

fn edge_updated_at_1_3_5_7<G: AdditionOps>(graph: &G) {
    for t in [1i64, 3, 5, 7] {
        graph.add_edge(t, "a", "b", [("w", t)], None).unwrap();
    }
}

/// `w` read on each exploded edge through `window(start, end)`, applied both to the exploded
/// collection and to each exploded edge on its own.
fn windowed_w<'graph, G: GraphViewOps<'graph>>(
    graph: G,
    start: i64,
    end: i64,
) -> Vec<Option<Prop>> {
    let on_collection = graph
        .edges()
        .explode()
        .window(start, end)
        .iter()
        .map(|e| e.properties().get("w"))
        .collect_vec();
    let on_each = graph
        .edges()
        .explode()
        .iter()
        .map(|e| e.window(start, end).properties().get("w"))
        .collect_vec();
    assert_eq!(on_collection, on_each);
    on_collection
}

#[test]
fn an_exploded_edge_outside_a_window_has_no_property_value() {
    let graph = Graph::new();
    edge_updated_at_1_3_5_7(&graph);
    assert_eq!(
        windowed_w(&graph, 3, 6),
        vec![None, Some(Prop::I64(3)), Some(Prop::I64(5)), None]
    );
}

#[test]
fn an_exploded_edge_carried_into_a_persistent_window_keeps_its_property_value() {
    let graph = PersistentGraph::new();
    edge_updated_at_1_3_5_7(&graph);
    assert_eq!(
        windowed_w(&graph, 3, 6),
        vec![None, Some(Prop::I64(3)), Some(Prop::I64(5)), None]
    );
    assert_eq!(
        windowed_w(&graph, 2, 3),
        vec![Some(Prop::I64(1)), None, None, None]
    );
    let exploded = graph.edges().explode();
    let carried_in = exploded.iter().next().unwrap().window(2, 3);
    assert!(!carried_in.is_active());
    assert_eq!(
        carried_in
            .properties()
            .temporal()
            .get("w")
            .unwrap()
            .iter()
            .map(|(t, v)| (t.t(), v))
            .collect_vec(),
        vec![(1, Prop::I64(1))]
    );
    assert_eq!(
        carried_in.properties().temporal().get("w").unwrap().at(2),
        Some(Prop::I64(1))
    );
}

/// a→b added @1 and deleted @5: `window(3,6)` carries the addition in, so it
/// still has its value, though a deletion inside the window makes it invalid.
#[test]
fn an_exploded_edge_carried_in_and_deleted_inside_a_persistent_window_keeps_its_value() {
    let graph = PersistentGraph::new();
    graph.add_edge(1, "a", "b", [("w", 1i64)], None).unwrap();
    graph.delete_edge(5, "a", "b", None).unwrap();
    assert_eq!(windowed_w(&graph, 3, 6), vec![Some(Prop::I64(1))]);
    let exploded = graph.edges().explode();
    let carried_in = exploded.iter().next().unwrap().window(3, 6);
    assert!(!carried_in.is_valid());
    assert_eq!(windowed_w(&graph, 6, 9), vec![None]);
}

#[test]
fn an_exploded_edge_starting_after_a_persistent_window_is_not_valid_in_it() {
    let graph = PersistentGraph::new();
    edge_updated_at_1_3_5_7(&graph);
    let valid = |start: i64, end: i64| {
        graph
            .edges()
            .explode()
            .window(start, end)
            .iter()
            .map(|e| e.is_valid())
            .collect_vec()
    };
    assert_eq!(valid(3, 6), vec![false, false, true, false]);
    assert_eq!(valid(2, 3), vec![true, false, false, false]);
}

/// a→b added @1 and deleted @2: a window that opens after the deletion never sees the
/// edge end, so the deletion is not in it; a window that holds the deletion still reports it.
#[test]
fn a_deletion_before_a_persistent_window_is_not_in_it() {
    let graph = PersistentGraph::new();
    graph.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
    graph.delete_edge(2, "a", "b", None).unwrap();
    let exploded = graph.edges().explode();
    let added = exploded.iter().next().unwrap();
    let deleted_in = |start: i64, end: i64| {
        let w = added.window(start, end);
        (
            w.is_deleted(),
            w.deletions().iter().map(|t| t.t()).collect_vec(),
        )
    };
    assert_eq!(deleted_in(3, 6), (false, vec![]));
    assert_eq!(deleted_in(2, 9), (true, vec![2]));
}
