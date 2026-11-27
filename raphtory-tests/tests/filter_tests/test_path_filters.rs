use raphtory::{
    db::{
        api::view::Select,
        graph::views::filter::model::{
            graph_filter::GraphFilter, ComposableFilter, PropertyExprFactory, ViewWrapOps,
        },
    },
    prelude::*,
};

fn test_graph() -> Graph {
    let g = Graph::new();
    g.add_node(0, 0, [("p1", 0), ("p2", 1)], Some("0"), None)
        .unwrap();
    g.add_node(1, 1, [("p1", 1), ("p2", 2)], Some("1"), None)
        .unwrap();
    g.add_node(0, 100, [("p1", 100), ("p2", 101)], None, None)
        .unwrap();

    g.add_edge(0, 0, 1, [("p1", 1), ("p2", 12)], Some("0"))
        .unwrap();
    g.add_edge(1, 1, 0, [("p1", 10), ("p2", 21)], None).unwrap();
    g.add_edge(100, 0, 100, [("p1", 100), ("p2", 1100)], None)
        .unwrap();
    g
}

#[test]
fn test_path_from_node_mixed_and() {
    let g = test_graph();
    let n = g.node(0).unwrap();
    let ids: Vec<_> = n
        .out_neighbours()
        .select(
            NodeFilter
                .property("p1")
                .eq(1)
                .and(EdgeFilter.property("p2").lt(100)),
        )
        .unwrap()
        .id()
        .collect();
    assert_eq!(ids, [1]);
}

#[test]
fn test_path_from_node_mixed_or() {
    let g = test_graph();
    let n = g.node(0).unwrap();
    let ids: Vec<_> = n
        .out_neighbours()
        .select(
            NodeFilter
                .property("p1")
                .eq(100)
                .or(EdgeFilter.property("p2").lt(100)),
        )
        .unwrap()
        .id()
        .collect();
    assert_eq!(ids, [1, 100]);
}

#[test]
fn test_path_from_node_window() {
    let g = test_graph();
    let n = g.node(0).unwrap();
    let ids: Vec<_> = n
        .out_neighbours()
        .select(GraphFilter.window(0, 2))
        .unwrap()
        .id()
        .collect();
    assert_eq!(ids, [1]);
}
