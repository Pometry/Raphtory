use itertools::Itertools;
use raphtory::prelude::*;
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

#[test]
fn open_exploded_edge_latest_time_is_the_view_end_not_a_sentinel() {
    use raphtory::db::graph::views::deletion_graph::PersistentGraph;
    use raphtory_api::core::storage::timeindex::{AsTime, EventTime};

    let p = PersistentGraph::new();
    p.add_edge(1, "a", "b", NO_PROPS, None).unwrap(); // still in force
    p.add_edge(2, "c", "d", NO_PROPS, None).unwrap();
    p.delete_edge(5, "c", "d", None).unwrap(); // deleted

    let open_edge = |g: &PersistentGraph| {
        g.edges()
            .explode()
            .iter()
            .find(|e| e.src().name() == "a")
            .unwrap()
            .latest_time()
            .unwrap()
    };
    let deleted_edge = p
        .edges()
        .explode()
        .iter()
        .find(|e| e.src().name() == "c")
        .unwrap()
        .latest_time()
        .unwrap();

    let unwindowed = open_edge(&p);
    // the end of the view, with the same convention as the windowed clamp: never usize::MAX
    assert_eq!(unwindowed, EventTime::start(5));
    assert_ne!(unwindowed.i(), usize::MAX);
    let windowed = p
        .window(0, 10)
        .edges()
        .explode()
        .iter()
        .find(|e| e.src().name() == "a")
        .unwrap()
        .latest_time()
        .unwrap();
    assert_eq!(windowed.i(), unwindowed.i());
    assert_eq!(windowed.t(), 10);
    // the deleted edge still reports the deletion's own event id, not 0
    assert_eq!(deleted_edge.t(), 5);
    assert_eq!(deleted_edge.i(), 2);

    // a graph with a single open edge shows the same convention
    let single = PersistentGraph::new();
    single.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
    assert_eq!(open_edge(&single), EventTime::start(1));
}
