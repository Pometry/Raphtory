use raphtory::{
    db::{
        api::view::{
            filter_ops::{Filter, Select},
            StaticGraphViewOps,
        },
        graph::{
            edges::{EdgeItem, Edges},
            views::{
                deletion_graph::PersistentGraph,
                filter::model::{
                    edge_filter::EdgeFilter,
                    exploded_edge_filter::ExplodedEdgeFilter,
                    graph_filter::GraphFilter,
                    node_filter::{NodeFilter, NodeFilterFactory},
                    EdgeViewFilterOps, PropertyExprFactory, ViewWrapOps,
                },
            },
        },
    },
    prelude::{
        AdditionOps, DeletionOps, EdgeViewOps, EntityExprFilterOps, Graph, GraphViewOps,
        NodeViewOps, PropertyAdditionOps, TimeOps,
    },
};
use raphtory_api::core::storage::timeindex::AsTime;

/// a→b @1 [x] w=1 · a→b @7 [y] w=7 · b→c @2 [x] w=2
fn graph() -> Graph {
    let g = Graph::new();
    g.add_edge(1, "a", "b", [("w", 1i64)], Some("x")).unwrap();
    g.add_edge(7, "a", "b", [("w", 7i64)], Some("y")).unwrap();
    g.add_edge(2, "b", "c", [("w", 2i64)], Some("x")).unwrap();
    g
}

fn exploded<'graph, G: GraphViewOps<'graph>, K: EdgeItem>(
    edges: &Edges<'graph, G, K>,
) -> Vec<(String, String, i64)> {
    edges
        .explode()
        .iter()
        .map(|e| (e.src().name(), e.dst().name(), e.time().unwrap().t()))
        .collect()
}

#[test]
fn plain_node_predicate_is_decided_per_edge() {
    let g = graph();
    let selected = g
        .edges()
        .select(NodeFilter.name().ne("c"))
        .unwrap()
        .explode()
        .iter()
        .map(|e| (e.src().name(), e.dst().name(), e.time().unwrap().t()))
        .collect::<Vec<_>>();
    assert_eq!(
        selected,
        vec![
            ("a".to_string(), "b".to_string(), 1),
            ("a".to_string(), "b".to_string(), 7)
        ]
    );
}

/// A view used as an edge predicate decides membership once per edge: the
/// layers and exploded instances of a kept edge all pass, so exploding
/// after the selection lists every instance, including those outside the
/// window. Narrowing the instances is what a graph view is for.
#[test]
fn a_view_predicate_decides_per_edge() {
    let g = graph();
    let selected = g
        .edges()
        .select(GraphFilter.window(0, 5))
        .unwrap()
        .explode()
        .iter()
        .map(|e| (e.src().name(), e.dst().name(), e.time().unwrap().t()))
        .collect::<Vec<_>>();
    assert_eq!(
        selected,
        vec![
            ("a".to_string(), "b".to_string(), 1),
            ("a".to_string(), "b".to_string(), 7),
            ("b".to_string(), "c".to_string(), 2)
        ]
    );

    let narrowed = g
        .window(0, 5)
        .edges()
        .explode()
        .iter()
        .map(|e| (e.src().name(), e.dst().name(), e.time().unwrap().t()))
        .collect::<Vec<_>>();
    assert_eq!(
        narrowed,
        vec![
            ("a".to_string(), "b".to_string(), 1),
            ("b".to_string(), "c".to_string(), 2)
        ]
    );
}

/// `select` asks its question of the items of the collection it is called
/// on: once per edge on edges, once per exploded edge on exploded edges.
/// The predicate's kind plays no part.
#[test]
fn select_asks_about_the_items_of_the_collection() {
    let g = graph();
    let ab = |t| ("a".to_string(), "b".to_string(), t);
    let bc = |t| ("b".to_string(), "c".to_string(), t);

    // a view on a collection of edges keeps the edges active in it, and
    // every exploded edge of a kept edge; on exploded edges it keeps the
    // exploded edges inside it
    let window = || GraphFilter.window(0, 5);
    let edges = g.edges().select(window()).unwrap();
    assert_eq!(exploded(&edges), vec![ab(1), ab(7), bc(2)]);
    let instances = g.edges().explode().select(window()).unwrap();
    assert_eq!(exploded(&instances), vec![ab(1), bc(2)]);

    // an exploded-edge predicate on a collection of edges keeps the edges
    // with a passing exploded edge, and all of their exploded edges
    let seven = || ExplodedEdgeFilter.property("w").eq(7i64);
    let edges = g.edges().select(seven()).unwrap();
    assert_eq!(exploded(&edges), vec![ab(1), ab(7)]);
    let instances = g.edges().explode().select(seven()).unwrap();
    assert_eq!(exploded(&instances), vec![ab(7)]);

    // an edge predicate reads the edge's latest value on edges, and each
    // exploded edge's own value on exploded edges
    let latest_seven = || EdgeFilter.property("w").eq(7i64);
    let edges = g.edges().select(latest_seven()).unwrap();
    assert_eq!(exploded(&edges), vec![ab(1), ab(7)]);
    let instances = g.edges().explode().select(latest_seven()).unwrap();
    assert_eq!(exploded(&instances), vec![ab(7)]);
}

/// A view on a collection is the `exists()` test of the collection's items,
/// read through the same views: once per edge on edges, once per exploded
/// edge on exploded edges.
#[test]
fn a_view_on_a_collection_is_its_exists_test() {
    let g = graph();
    let ab = |t| ("a".to_string(), "b".to_string(), t);
    let bc = |t| ("b".to_string(), "c".to_string(), t);

    let view = g.edges().select(GraphFilter.window(0, 5)).unwrap();
    let exists = g.edges().select(EdgeFilter.window(0, 5).exists()).unwrap();
    assert_eq!(exploded(&view), vec![ab(1), ab(7), bc(2)]);
    assert_eq!(exploded(&exists), exploded(&view));
    let view = g.edges().select(GraphFilter.layers("y")).unwrap();
    let exists = g.edges().select(EdgeFilter.layers("y").exists()).unwrap();
    assert_eq!(exploded(&view), vec![ab(1), ab(7)]);
    assert_eq!(exploded(&exists), exploded(&view));

    let view = g
        .edges()
        .explode()
        .select(GraphFilter.window(0, 5))
        .unwrap();
    let exists = g
        .edges()
        .explode()
        .select(ExplodedEdgeFilter.window(0, 5).exists())
        .unwrap();
    assert_eq!(exploded(&view), vec![ab(1), bc(2)]);
    assert_eq!(exploded(&exists), exploded(&view));
    let view = g.edges().explode().select(GraphFilter.layers("y")).unwrap();
    let exists = g
        .edges()
        .explode()
        .select(ExplodedEdgeFilter.layers("y").exists())
        .unwrap();
    assert_eq!(exploded(&view), vec![ab(7)]);
    assert_eq!(exploded(&exists), exploded(&view));
}

/// `exists()` asks about the views written before it, so with no view it is
/// refused as an empty view is: on nodes, edges and exploded edges.
#[test]
fn exists_without_a_view_is_refused() {
    let g = graph();
    let errors = [
        g.nodes().select(NodeFilter.exists()).err(),
        g.edges().select(EdgeFilter.exists()).err(),
        g.edges()
            .explode()
            .select(ExplodedEdgeFilter.exists())
            .err(),
    ];
    for (entity, error) in ["node", "edge", "exploded edge"].into_iter().zip(errors) {
        let error = error.map(|e| e.to_string());
        assert!(
            error.as_deref().is_some_and(|e| e.contains("view")),
            "{entity}: {error:?}"
        );
    }
}

/// a→b w=1 @1, w=3 @3 · b→c w=2 @2, both edges with metadata m=1
fn scoped_fixture<G: StaticGraphViewOps + AdditionOps + PropertyAdditionOps>(g: G) -> G {
    g.add_edge(1, "a", "b", [("w", 1i64)], None).unwrap();
    g.add_edge(3, "a", "b", [("w", 3i64)], None).unwrap();
    g.add_edge(2, "b", "c", [("w", 2i64)], None).unwrap();
    for (src, dst) in [("a", "b"), ("b", "c")] {
        g.edge(src, dst)
            .unwrap()
            .add_metadata([("m", 1i64)], None)
            .unwrap();
    }
    g
}

fn check_scoped_exploded_reads<G: StaticGraphViewOps + AdditionOps + PropertyAdditionOps>(g: G) {
    let g = scoped_fixture(g);
    let ab = |t| ("a".to_string(), "b".to_string(), t);
    let none: Vec<(String, String, i64)> = vec![];

    let cases = [
        (
            ExplodedEdgeFilter.window(0, 2).property("w").is_some(),
            vec![ab(1)],
        ),
        (
            ExplodedEdgeFilter.window(0, 2).property("w").eq(3i64),
            none.clone(),
        ),
        (
            ExplodedEdgeFilter.at(3).property("w").eq(1i64),
            none.clone(),
        ),
        (ExplodedEdgeFilter.at(3).property("w").eq(3i64), vec![ab(3)]),
        (
            ExplodedEdgeFilter.window(0, 2).metadata("m").is_some(),
            vec![ab(1)],
        ),
        (ExplodedEdgeFilter.window(0, 2).is_active(), vec![ab(1)]),
        // A scoped state predicate is judged per exploded edge
        (ExplodedEdgeFilter.at(3).is_active(), vec![ab(3)]),
    ];
    for (filter, expected) in cases {
        let filtered = g.filter(filter.clone()).unwrap();
        assert_eq!(exploded(&filtered.edges()), expected, "filter {filter}");
        let selected = g.edges().explode().select(filter.clone()).unwrap();
        assert_eq!(exploded(&selected), expected, "select {filter}");
    }

    let edge_filter = EdgeFilter.window(0, 2).property("w").is_some();
    let selected = g.edges().explode().select(edge_filter).unwrap();
    assert_eq!(exploded(&selected), vec![ab(1)]);
}

/// A property or metadata read of an exploded edge through a view has an
/// answer only when the view includes that exploded edge: a window or `at`
/// that leaves the update out reads nothing, as `is_active` already does.
#[test]
fn a_scoped_exploded_property_reads_only_inside_its_view() {
    check_scoped_exploded_reads(Graph::new());
    check_scoped_exploded_reads(PersistentGraph::new());
}

/// ```text
/// time          1    2    3    4    5    6    7    8    9   10
/// a→b           ●+                  ●-        ●+
/// window(6,10)                           [===================)
/// ```
/// a→b added @1, deleted @5, added again @7.
fn added_deleted_readded<G: StaticGraphViewOps + AdditionOps + DeletionOps>(g: G) -> G {
    g.add_edge(1, "a", "b", [("w", 1i64)], None).unwrap();
    g.delete_edge(5, "a", "b", None).unwrap();
    g.add_edge(7, "a", "b", [("w", 1i64)], None).unwrap();
    g
}

/// On an event graph each exploded edge is an addition and none of them
/// is itself deleted, so `is_deleted()` keeps none, though the edge as a whole
/// has a deletion.
#[test]
fn exploded_is_deleted_is_judged_per_exploded_edge() {
    let g = added_deleted_readded(Graph::new());
    assert!(g.edge("a", "b").unwrap().is_deleted());
    let none: Vec<(String, String, i64)> = vec![];
    let filter = ExplodedEdgeFilter.is_deleted();
    let filtered = g.filter(filter.clone()).unwrap();
    assert_eq!(exploded(&filtered.edges()), none);
    let selected = g.edges().explode().select(filter).unwrap();
    assert_eq!(exploded(&selected), none);
}

/// On a persistent graph `window(6, 10).is_valid()` keeps the
/// addition @7, valid in the window, and not the addition @1, whose edge was
/// deleted @5.
#[test]
fn exploded_is_valid_in_a_window_is_judged_per_exploded_edge() {
    let g = added_deleted_readded(PersistentGraph::new());
    let ab7 = vec![("a".to_string(), "b".to_string(), 7)];
    let filter = ExplodedEdgeFilter.window(6, 10).is_valid();
    let filtered = g.filter(filter.clone()).unwrap();
    assert_eq!(exploded(&filtered.edges()), ab7);
    let selected = g.edges().explode().select(filter).unwrap();
    assert_eq!(exploded(&selected), ab7);
}
