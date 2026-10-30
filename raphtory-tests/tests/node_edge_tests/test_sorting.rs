use itertools::Itertools;
use raphtory::{
    db::api::view::{EdgeSortBy, EdgeSortKey, NodeSortBy, NodeSortKey, SortByTime},
    errors::GraphError,
    prelude::*,
};
use raphtory_api::core::{storage::timeindex::AsTime, Direction};

fn node_key(key: NodeSortKey, reverse: bool) -> NodeSortBy {
    NodeSortBy { reverse, key }
}

fn edge_key(key: EdgeSortKey, reverse: bool) -> EdgeSortBy {
    EdgeSortBy { reverse, key }
}

fn three_edge_graph() -> Graph {
    let g = Graph::new();
    g.add_edge(3, "c", "a", [("w", 1i64)], None).unwrap();
    g.add_edge(1, "a", "b", [("w", 3i64)], None).unwrap();
    g.add_edge(2, "b", "c", [("w", 2i64)], None).unwrap();
    g
}

fn star_graph() -> Graph {
    let g = Graph::new();
    g.add_edge(1, "hub", "a", NO_PROPS, None).unwrap();
    g.add_edge(1, "hub", "b", NO_PROPS, None).unwrap();
    g.add_edge(1, "hub", "c", NO_PROPS, None).unwrap();
    g.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
    g
}

fn names<'graph, G: GraphViewOps<'graph>>(
    nodes: &raphtory::db::graph::nodes::Nodes<'graph, G>,
) -> Vec<String> {
    nodes.iter().map(|n| n.name()).collect()
}

#[test]
fn test_edges_try_sorted_respects_max() {
    let g = three_edge_graph();
    let keys = [edge_key(EdgeSortKey::Property("w".into()), false)];
    let err = g.edges().try_sorted(&keys, Some(2)).unwrap_err();
    assert!(matches!(err, GraphError::SortTooLarge { max: 2 }), "{err}");
    assert_eq!(
        err.to_string(),
        "sorting more than 2 items isn't supported; narrow the window, layers or filter"
    );

    let capped = g.edges().try_sorted(&keys, Some(3)).unwrap();
    let unbounded = g.edges().sorted(&keys);
    let ids = |e: &raphtory::db::graph::edges::Edges<_>| {
        e.iter()
            .map(|e| (e.src().name(), e.dst().name()))
            .collect_vec()
    };
    assert_eq!(ids(&capped), ids(&unbounded));
    assert_eq!(
        ids(&capped),
        vec![
            ("c".to_string(), "a".to_string()),
            ("b".to_string(), "c".to_string()),
            ("a".to_string(), "b".to_string())
        ]
    );
}

#[test]
fn test_nodes_try_sorted_respects_max() {
    let g = three_edge_graph();
    let keys = [node_key(NodeSortKey::Name, true)];
    let err = g.nodes().try_sorted(&keys, Some(2)).unwrap_err();
    assert!(matches!(err, GraphError::SortTooLarge { max: 2 }), "{err}");

    let capped = g.nodes().try_sorted(&keys, Some(3)).unwrap();
    assert_eq!(names(&capped), names(&g.nodes().sorted(&keys)));
    assert_eq!(names(&capped), vec!["c", "b", "a"]);
}

#[test]
fn test_exploded_edges_try_sorted_respects_max() {
    let g = Graph::new();
    g.add_edge(2, "a", "b", NO_PROPS, None).unwrap();
    g.add_edge(3, "a", "b", NO_PROPS, None).unwrap();
    g.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
    let exploded = g.edges().explode();
    let keys = [edge_key(EdgeSortKey::Time(SortByTime::Earliest), true)];
    assert!(matches!(
        exploded.try_sorted(&keys, Some(2)),
        Err(GraphError::SortTooLarge { max: 2 })
    ));
    let capped = exploded.try_sorted(&keys, Some(3)).unwrap();
    let times = capped
        .iter()
        .map(|e| e.earliest_time().unwrap().t())
        .collect_vec();
    assert_eq!(times, vec![3, 2, 1]);
    assert_eq!(
        times,
        exploded
            .sorted(&keys)
            .iter()
            .map(|e| e.earliest_time().unwrap().t())
            .collect_vec()
    );
}

#[test]
fn test_sort_ties_fall_through_and_missing_values_compare_equal() {
    let g = Graph::new();
    g.add_node(1, "b", [("score", 1i64)], None, None).unwrap();
    g.add_node(1, "d", NO_PROPS, None, None).unwrap();
    g.add_node(1, "a", [("score", 1i64)], None, None).unwrap();
    g.add_node(1, "c", [("score", 0i64)], None, None).unwrap();
    let keys = [
        node_key(NodeSortKey::Property("score".into()), true),
        node_key(NodeSortKey::Name, false),
    ];
    assert_eq!(
        names(&g.nodes().try_sorted(&keys, None).unwrap()),
        vec!["a", "b", "c", "d"]
    );
}

#[test]
fn test_sort_by_degree() {
    let g = star_graph();
    let by = |dir| {
        names(&g.nodes().sorted(&[
            node_key(NodeSortKey::Degree(dir), true),
            node_key(NodeSortKey::Name, false),
        ]))
    };
    assert_eq!(by(Direction::BOTH), vec!["hub", "a", "b", "c"]);
    assert_eq!(by(Direction::IN), vec!["b", "a", "c", "hub"]);
    assert_eq!(by(Direction::OUT), vec!["hub", "a", "b", "c"]);

    let ascending = names(&g.nodes().sorted(&[
        node_key(NodeSortKey::Degree(Direction::BOTH), false),
        node_key(NodeSortKey::Name, false),
    ]));
    assert_eq!(ascending, vec!["c", "a", "b", "hub"]);
}

#[test]
fn test_edges_sorted_by_endpoint_degree() {
    let g = star_graph();
    let sorted = g.edges().sorted(&[
        edge_key(
            EdgeSortKey::Dst(node_key(NodeSortKey::Degree(Direction::IN), true)),
            false,
        ),
        edge_key(EdgeSortKey::Dst(node_key(NodeSortKey::Name, false)), false),
        edge_key(EdgeSortKey::Src(node_key(NodeSortKey::Name, false)), true),
    ]);
    let pairs = sorted
        .iter()
        .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
        .collect_vec();
    assert_eq!(pairs, vec!["a->b", "hub->b", "hub->a", "hub->c"]);
}
