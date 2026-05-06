use itertools::Itertools;
use raphtory::{prelude::*, test_storage};

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
