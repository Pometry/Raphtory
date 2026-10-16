use raphtory::{
    db::{
        api::view::{DynamicGraph, Filter, IntoDynamic},
        graph::views::filter::model::{
            expr::{EdgeLeaf, Expr, FilterExpr, ViewOp},
            filter_operator::BinaryOp,
        },
    },
    prelude::{
        AdditionOps, EdgeViewOps, Graph, GraphViewOps, LayerOps, NodeViewOps, TimeOps, NO_PROPS,
    },
};
use raphtory_api::core::{
    entities::properties::prop::{IntoProp, Prop},
    storage::timeindex::{AsTime, EventTime},
};

/// ```text
/// time        0    1    2    3    4    5    6
///             |    |    |    |    |    |    |
/// a           ● added
/// a.score          ●(9)                     ●(2)
/// a→b.w            ●(3)                     ●(7)
/// window(0,4) [===================)
/// ```
fn graph() -> Graph {
    let g = Graph::new();
    g.add_node(0, "a", NO_PROPS, None, None).unwrap();
    g.add_node(1, "a", [("score", 9i64.into_prop())], None, None)
        .unwrap();
    g.add_node(6, "a", [("score", 2i64.into_prop())], None, None)
        .unwrap();
    g.add_edge(1, "a", "b", [("w", 3i64)], None).unwrap();
    g.add_edge(6, "a", "b", [("w", 7i64)], None).unwrap();
    g
}

fn window(start: i64, end: i64) -> FilterExpr {
    FilterExpr::View(vec![ViewOp::Window {
        start: EventTime::start(start),
        end: EventTime::start(end),
    }])
}

fn layers(names: &[&str]) -> FilterExpr {
    FilterExpr::View(vec![ViewOp::Layers(names.to_vec().into())])
}

fn w_gt(v: i64) -> FilterExpr {
    FilterExpr::Edge(Expr::Cmp(
        BinaryOp::Gt,
        Box::new(Expr::Term(EdgeLeaf::Property {
            views: vec![],
            name: "w".into(),
            temporal: false,
        })),
        Box::new(Expr::Const(v.into())),
    ))
}

fn and(operands: Vec<FilterExpr>) -> FilterExpr {
    FilterExpr::And(operands)
}

fn filtered(g: &Graph, f: FilterExpr) -> DynamicGraph {
    g.filter(f).unwrap().into_dynamic()
}

fn node_names(g: &DynamicGraph) -> Vec<String> {
    let mut names: Vec<String> = g.nodes().iter().map(|n| n.name()).collect();
    names.sort();
    names
}

fn edge_names(g: &DynamicGraph) -> Vec<String> {
    let mut names: Vec<String> = g
        .edges()
        .iter()
        .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
        .collect();
    names.sort();
    names
}

fn node_history(g: &DynamicGraph, name: &str) -> Vec<i64> {
    g.node(name).unwrap().history().t().collect()
}

fn edge_updates(g: &DynamicGraph, src: &str, dst: &str) -> Vec<i64> {
    g.edge(src, dst)
        .unwrap()
        .explode()
        .iter()
        .map(|e| e.time().unwrap().t())
        .collect()
}

fn node_score(g: &DynamicGraph, name: &str) -> Vec<Prop> {
    g.node(name)
        .unwrap()
        .properties()
        .temporal()
        .get("score")
        .unwrap()
        .values()
        .collect()
}

fn edge_w(g: &DynamicGraph, src: &str, dst: &str) -> Vec<Prop> {
    g.edge(src, dst)
        .unwrap()
        .properties()
        .temporal()
        .get("w")
        .unwrap()
        .values()
        .collect()
}

/// The fixture above, `window(0,4) & w > 0`: everything after t=4 is gone.
#[test]
fn a_window_under_and_cuts_node_histories() {
    let g = graph();
    let and = filtered(&g, and(vec![window(0, 4), w_gt(0)]));
    let chained = g.window(0, 4).filter(w_gt(0)).unwrap().into_dynamic();
    for (form, result) in [("and", &and), ("chained", &chained)] {
        assert_eq!(edge_names(result), ["a->b"], "{form}");
        assert_eq!(edge_updates(result, "a", "b"), [1], "{form}");
        assert_eq!(node_score(result, "a"), [Prop::I64(9)], "{form}");
        let earliest = result.earliest_time().map(|t| t.t());
        let latest = result.latest_time().map(|t| t.t());
        assert_eq!(earliest, Some(0), "{form}");
        assert_eq!(latest, Some(1), "{form}");
    }
    assert_eq!(node_history(&and, "a"), node_history(&chained, "a"));
    // t=1 appears twice: the score update and the a→b update; t=6 is gone
    assert_eq!(node_history(&and, "a"), [0, 1, 1]);
}

/// The fixture above, `window(0,4) & w > 5`: the predicate reads the latest
/// value overall (7, at t=6) and keeps a→b, but the kept edge only shows
/// what falls in the window.
#[test]
fn a_window_under_and_with_a_predicate_reading_outside_it() {
    let g = graph();
    let result = filtered(&g, and(vec![window(0, 4), w_gt(5)]));
    assert_eq!(edge_names(&result), ["a->b"]);
    assert_eq!(edge_updates(&result, "a", "b"), [1]);
    assert_eq!(edge_w(&result, "a", "b"), [Prop::I64(3)]);
}

/// ```text
/// time        0    1    2    3    4    5    6    7    8    9
///             |    |    |    |    |    |    |    |    |    |
/// a           ●    ●              ●         ●                  (plus the fixture above)
/// a→b.w            ●(3)                     ●(7)
/// window(0,5) [========================)
/// window(3,9)                [==============================)
/// overlap                    [=========)
/// ```
#[test]
fn nested_ands_window_to_the_overlap() {
    let g = graph();
    g.add_node(4, "a", NO_PROPS, None, None).unwrap();
    let inner = filtered(&g, and(vec![window(3, 9), w_gt(0)]));
    // t=6 appears twice: the score update and the a→b update
    assert_eq!(node_history(&inner, "a"), [4, 6, 6]);
    let result = filtered(
        &g,
        and(vec![window(0, 5), and(vec![window(3, 9), w_gt(0)])]),
    );
    assert_eq!(result.start().map(|t| t.t()), Some(3));
    assert_eq!(result.end().map(|t| t.t()), Some(5));
    assert_eq!(node_history(&result, "a"), [4]);
    assert_eq!(node_names(&result), ["a"]);
    assert!(edge_names(&result).is_empty());
}

/// ```text
/// time        0    1    2    3    4    5    6    7    8    9
/// window(0,2) [=========)
/// window(6,9)                               [==============)
/// overlap     (none)
/// ```
#[test]
fn an_empty_overlap_keeps_nothing() {
    let g = graph();
    let result = filtered(&g, and(vec![window(0, 2), window(6, 9)]));
    assert!(node_names(&result).is_empty());
    assert!(edge_names(&result).is_empty());
}

/// ```text
/// time         0    3    7    12   25
///              |    |    |    |    |
/// a, d         ●
/// a→d friends            ●
/// a→d work                         ●
/// window(3,12)      [=========)
/// ```
#[test]
fn two_views_under_and_intersect_per_update_for_nodes() {
    let g = Graph::new();
    g.add_node(0, "a", NO_PROPS, None, None).unwrap();
    g.add_node(0, "d", NO_PROPS, None, None).unwrap();
    g.add_edge(7, "a", "d", NO_PROPS, Some("friends")).unwrap();
    g.add_edge(25, "a", "d", NO_PROPS, Some("work")).unwrap();

    let work = filtered(&g, and(vec![window(3, 12), layers(&["work"])]));
    assert!(node_names(&work).is_empty(), "{:?}", node_names(&work));
    assert!(edge_names(&work).is_empty(), "{:?}", edge_names(&work));

    let friends = filtered(&g, and(vec![window(3, 12), layers(&["friends"])]));
    assert_eq!(node_names(&friends), ["a", "d"]);
    assert_eq!(edge_names(&friends), ["a->d"]);
    assert_eq!(edge_updates(&friends, "a", "d"), [7]);
    let layered = g.window(3, 12).layers("friends").unwrap().into_dynamic();
    assert_eq!(node_names(&layered), node_names(&friends));
}
