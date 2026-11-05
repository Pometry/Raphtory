//! An `or` or `not` of views alone is one view: its times and layers combine as
//! sets and the graph is seen through the result, with the time semantics to
//! match. Every expectation is written from the fixture, never read back from
//! another composite.
//!
//! ```text
//! time         0    1    2    3    4    5    6    7    8    9
//! a→b [x]           ●(1)           ●(4)                ●(8)        w at 1, 4, 8
//! c→d [y]                     ●(9)           ●(2)                  w at 3, 6
//! e→f [x]                               ●(7)                       w at 5
//!
//! window(3,6)                 [==============)
//! ~window(3,6)  =====...======)              [===============...
//! ```
use raphtory::{
    db::{
        api::view::{DynamicGraph, Filter, IntoDynamic, Select},
        graph::views::filter::{
            model::{
                edge_filter::EdgeFilter, graph_filter::GraphFilter, ComposableFilter,
                PropertyExprFactory, ViewWrapOps,
            },
            CreateFilter,
        },
    },
    errors::GraphError,
    prelude::*,
};
use raphtory_api::core::storage::timeindex::{AsTime, EventTime};
use std::collections::BTreeMap;

fn graph() -> Graph {
    let g = Graph::new();
    for (t, w) in [(1, 1i64), (4, 4), (8, 8)] {
        g.add_edge(t, "a", "b", [("w", w)], Some("x")).unwrap();
    }
    for (t, w) in [(3, 9i64), (6, 2)] {
        g.add_edge(t, "c", "d", [("w", w)], Some("y")).unwrap();
    }
    g.add_edge(5, "e", "f", [("w", 7i64)], Some("x")).unwrap();
    g
}

fn bases() -> [(&'static str, DynamicGraph); 2] {
    let g = graph();
    [
        ("event", g.clone().into_dynamic()),
        ("persistent", g.persistent_graph().into_dynamic()),
    ]
}

/// Each edge `src→dst` with the times of its updates.
fn histories<'a, G: GraphViewOps<'a>>(g: &G) -> BTreeMap<String, Vec<i64>> {
    g.edges()
        .iter()
        .map(|e| {
            (
                format!("{}→{}", e.src().name(), e.dst().name()),
                e.history().iter().map(|t| t.t()).collect(),
            )
        })
        .collect()
}

fn filtered(g: &DynamicGraph, filter: impl CreateFilter) -> BTreeMap<String, Vec<i64>> {
    histories(&g.filter(filter).unwrap())
}

fn refused(g: &DynamicGraph, filter: impl CreateFilter) -> String {
    match g.filter(filter) {
        Err(GraphError::InvalidFilter(message)) => message,
        Err(other) => panic!("expected an invalid filter, got {other}"),
        Ok(_) => panic!("expected the filter to be refused"),
    }
}

fn expected(entries: &[(&str, &[i64])]) -> BTreeMap<String, Vec<i64>> {
    entries
        .iter()
        .map(|(edge, times)| (edge.to_string(), times.to_vec()))
        .collect()
}

#[test]
fn not_of_a_window_keeps_the_events_outside_it() {
    let g = graph().into_dynamic();
    assert_eq!(
        filtered(&g, GraphFilter.window(3, 6).not()),
        expected(&[("a→b", &[1, 8]), ("c→d", &[6])])
    );
    let outside = g.filter(GraphFilter.window(3, 6).not()).unwrap();
    assert_eq!(outside.earliest_time().map(|t| t.t()), Some(1));
    assert_eq!(outside.latest_time().map(|t| t.t()), Some(8));
}

/// On both graph kinds the result is the union of the views on either side of
/// the window, which is what `before` and `window` give on their own.
#[test]
fn not_of_a_window_is_the_union_of_the_two_sides_on_both_graph_kinds() {
    for (kind, g) in bases() {
        let mut want = histories(&g.before(3));
        for (edge, times) in histories(&g.window(6, 100)) {
            want.entry(edge).or_default().extend(times);
        }
        assert_eq!(filtered(&g, GraphFilter.window(3, 6).not()), want, "{kind}");
    }
}

#[test]
fn or_of_windows_keeps_the_events_in_either_and_none_in_the_gap() {
    let g = graph().into_dynamic();
    assert_eq!(
        filtered(&g, GraphFilter.window(0, 2).or(GraphFilter.window(7, 9))),
        expected(&[("a→b", &[1, 8])])
    );
}

#[test]
fn not_of_an_and_of_windows_is_the_complement_of_their_overlap() {
    let g = graph().into_dynamic();
    // window(0,5) & window(3,9) is [3,5); everything else is kept.
    assert_eq!(
        filtered(
            &g,
            GraphFilter.window(0, 5).and(GraphFilter.window(3, 9)).not()
        ),
        expected(&[("a→b", &[1, 8]), ("c→d", &[6]), ("e→f", &[5])])
    );
}

#[test]
fn not_of_a_layer_keeps_the_other_layers() {
    let g = graph().into_dynamic();
    assert_eq!(
        filtered(&g, GraphFilter.layers("x").not()),
        expected(&[("c→d", &[3, 6])])
    );
    assert_eq!(
        filtered(&g, GraphFilter.layers("x").or(GraphFilter.layers("y"))),
        histories(&g)
    );
}

/// The base graph's own view bounds the result: the complement is taken inside
/// it, not over the whole graph.
#[test]
fn a_negated_view_stays_inside_the_view_the_filter_is_given() {
    let g = graph().into_dynamic();
    assert_eq!(
        filtered(
            &g.window(0, 7).into_dynamic(),
            GraphFilter.window(3, 6).not()
        ),
        expected(&[("a→b", &[1]), ("c→d", &[6])])
    );
    // A layer view on a windowed base is still a layer view alone.
    assert_eq!(
        filtered(
            &g.window(0, 7).into_dynamic(),
            GraphFilter.layers("x").not()
        ),
        expected(&[("c→d", &[3, 6])])
    );
}

/// A multi-window operand of the top-level `and` bounds the result to its
/// ranges; the predicate is still read on the graph the filter is given.
#[test]
fn and_with_an_or_of_windows_sees_only_their_ranges() {
    let g = graph().into_dynamic();
    // w > 5 keeps a→b (latest 8) and e→f (7); only a→b has events in the ranges.
    assert_eq!(
        filtered(
            &g,
            EdgeFilter
                .property("w")
                .gt(5i64)
                .and(GraphFilter.window(0, 2).or(GraphFilter.window(7, 9)),)
        ),
        expected(&[("a→b", &[1, 8])])
    );
}

/// On a collection, `not` of a view tests existence in the graph that `not`
/// keeps.
#[test]
fn a_negated_view_on_a_collection_is_existence_outside_it() {
    let g = graph();
    let mut edges: Vec<String> = g
        .edges()
        .select(GraphFilter.window(3, 6).not())
        .unwrap()
        .iter()
        .map(|e| format!("{}→{}", e.src().name(), e.dst().name()))
        .collect();
    edges.sort();
    assert_eq!(edges, ["a→b", "c→d"]);
    let mut nodes: Vec<String> = g
        .nodes()
        .select(GraphFilter.window(3, 6).not())
        .unwrap()
        .name()
        .collect();
    nodes.sort();
    assert_eq!(nodes, ["a", "b", "c", "d"]);
}

#[test]
fn shapes_that_are_not_one_view_are_still_refused() {
    let g = graph().into_dynamic();
    // A view beside a predicate keeps different times for different edges.
    assert!(refused(
        &g,
        EdgeFilter
            .property("w")
            .gt(5i64)
            .or(GraphFilter.window(3, 6))
    )
    .contains("only with other views"));
    assert!(refused(
        &g,
        EdgeFilter
            .property("w")
            .gt(5i64)
            .and(GraphFilter.window(3, 6))
            .not()
    )
    .contains("only with other views"));
    // Time and layers restricted differently are not one (time, layers) pair.
    assert!(
        refused(&g, GraphFilter.window(0, 2).or(GraphFilter.layers("y")))
            .contains("both time and layers")
    );
    assert!(refused(
        &g,
        GraphFilter.window(0, 2).and(GraphFilter.layers("y")).not()
    )
    .contains("both time and layers"));
    // A view that restricts nodes or edges, not time or layers, has no place in the
    // value; the refusal names the view.
    assert_eq!(
        refused(&g, GraphFilter.valid().not()),
        "only time and layer views combine under `or` or `not`, not VALID"
    );
    assert_eq!(
        refused(&g, GraphFilter.subgraph(["a", "b"]).not()),
        "only time and layer views combine under `or` or `not`, not SUBGRAPH([a, b])"
    );
}

/// A view's times are resolved when it is built, from the bounds the views
/// state, not from which events they happen to cut, so a filtered graph keeps
/// the same times as the graph grows.
#[test]
fn a_resolved_view_keeps_its_meaning_as_events_arrive() {
    let g = graph();
    // window(0,100) cuts nothing yet: every event lies inside it.
    let outside = g.filter(GraphFilter.window(0, 100).not()).unwrap();
    assert!(histories(&outside).is_empty());
    let within = g
        .window(0, 8)
        .filter(GraphFilter.window(3, 6).not())
        .unwrap();
    g.add_edge(200, "x", "y", [("w", 1i64)], Some("x")).unwrap();
    g.add_edge(7, "x", "y", [("w", 1i64)], Some("x")).unwrap();
    // The event at 200 is outside window(0,100) ...
    assert_eq!(histories(&outside), expected(&[("x→y", &[200])]));
    // ... and outside window(0,8), so the negation inside it never sees it.
    assert_eq!(
        histories(&within),
        expected(&[("a→b", &[1]), ("c→d", &[6]), ("x→y", &[7])])
    );
}

/// Validity and deletion follow the base semantics across ranges: on an event
/// graph they ask whether some range holds an addition or a deletion, on a
/// persistent graph they are the state at the end of the last range. Each
/// answer is checked against the single windows.
#[test]
fn validity_and_deletion_over_ranges_follow_the_graph_kind() {
    let g = Graph::new();
    g.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
    g.delete_edge(6, "a", "b", None).unwrap();
    for (kind, g) in [
        ("event", g.clone().into_dynamic()),
        ("persistent", g.persistent_graph().into_dynamic()),
    ] {
        let either = g
            .filter(GraphFilter.window(0, 2).or(GraphFilter.window(5, 7)))
            .unwrap();
        let edge = either.edge("a", "b").unwrap();
        let first = g.window(0, 2).edge("a", "b").unwrap();
        let last = g.window(5, 7).edge("a", "b").unwrap();
        if kind == "event" {
            // The addition at 1 is in the first range only, the deletion at 6
            // in the last only: the view holds both.
            assert!(first.is_valid() && !last.is_valid());
            assert!(!first.is_deleted() && last.is_deleted());
            assert!(edge.is_valid() && edge.is_deleted(), "{kind}");
        } else {
            assert_eq!(edge.is_valid(), last.is_valid(), "{kind}");
            assert_eq!(edge.is_deleted(), last.is_deleted(), "{kind}");
        }
    }
}

/// A complement that runs to the earliest or latest time has no bound there,
/// as a graph with no window has none, so windowing over it terminates.
#[test]
fn a_complement_reaching_the_ends_of_time_has_no_bound_there() {
    let g = graph().into_dynamic();
    let outside = g.filter(GraphFilter.window(3, 6).not()).unwrap();
    assert_eq!((outside.start(), outside.end()), (None, None));
    let before = g.filter(GraphFilter.after(5).not()).unwrap();
    assert_eq!(before.start(), None);
    assert_eq!(before.end().map(|t| t.t()), Some(6));
    // Unbounded like the graph, with the same earliest and latest events, so
    // the same rolling windows.
    assert_eq!(
        outside.rolling(2, None).unwrap().count(),
        g.rolling(2, None).unwrap().count()
    );
}

/// An `and` of time and layer views alone is one view too: the overlap, with
/// no operand graphs behind it. Both operands and the overlap are checked
/// against the chained views.
#[test]
fn and_of_views_alone_is_the_overlap_view() {
    for (kind, g) in bases() {
        let both = g
            .filter(GraphFilter.window(0, 5).and(GraphFilter.window(3, 9)))
            .unwrap();
        assert_eq!(histories(&both), histories(&g.window(3, 5)), "{kind}");
        assert_eq!(
            (both.start(), both.end()),
            (Some(EventTime::start(3)), Some(EventTime::start(5))),
            "{kind}"
        );
        let layered = g
            .filter(GraphFilter.window(0, 5).and(GraphFilter.layers("x")))
            .unwrap();
        assert_eq!(
            histories(&layered),
            histories(&g.window(0, 5).valid_layers(vec!["x"])),
            "{kind}"
        );
    }
    // Disjoint windows leave nothing: no edges, and no graph property either,
    // as an empty view is a filtered one.
    let g = graph();
    g.add_properties(4, [("p", 1i64)]).unwrap();
    let none = g
        .filter(GraphFilter.window(0, 2).and(GraphFilter.window(7, 9)))
        .unwrap();
    assert!(histories(&none).is_empty());
    assert!(none.properties().temporal().keys().next().is_none());
    assert_eq!(
        g.window(0, 2).properties().temporal().keys().count(),
        0,
        "the single windows hide it too"
    );
}

/// A view with no layers keeps no edges but still keeps the graph's temporal
/// properties in its times, so it is not a view that keeps nothing.
///
/// ```text
/// time         0    1    2    3    4    5    6    7    8    9
/// p                      ●(1)                     ●(2)
/// window(0,5)  [========================)
/// window(5,10)                          [=========================)
/// ```
#[test]
fn an_or_of_views_with_no_layers_keeps_the_graph_properties_of_either() {
    let g = graph();
    g.add_properties(2, [("p", 1i64)]).unwrap();
    g.add_properties(7, [("p", 2i64)]).unwrap();
    let no_layers = || GraphFilter.exclude_layers(vec!["x", "y"]);
    let either = g
        .filter(
            GraphFilter
                .window(0, 5)
                .and(no_layers())
                .or(GraphFilter.window(5, 10).and(no_layers())),
        )
        .unwrap();
    assert!(histories(&either).is_empty());
    let p = either.properties().temporal().get("p").unwrap();
    assert_eq!(p.values().collect::<Vec<_>>(), [Prop::I64(1), Prop::I64(2)]);
    // With every layer in one window and none in the other, the property at 7
    // is kept but the edges between 5 and 10 are not: not one view.
    assert!(refused(
        &g.into_dynamic(),
        GraphFilter
            .window(0, 5)
            .or(GraphFilter.window(5, 10).and(no_layers()))
    )
    .contains("both time and layers"));
}

/// On a base that already keeps some layers, `not` of a layer keeps the base's
/// other layers only.
#[test]
fn a_negated_layer_stays_inside_the_layers_the_filter_is_given() {
    let g = graph();
    g.add_edge(9, "g", "h", [("w", 1i64)], Some("z")).unwrap();
    let base = g.valid_layers(vec!["x", "y"]).into_dynamic();
    assert_eq!(
        filtered(&base, GraphFilter.layers("x").not()),
        expected(&[("c→d", &[3, 6])])
    );
}
