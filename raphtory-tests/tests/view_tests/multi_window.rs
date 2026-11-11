//! A multi-window view is the union of the windowed views over its ranges.
//! Every expectation below is computed from the single windows, never from the
//! view under test.
//!
//! ```text
//! time:  0    1    2    3    4    5    6    7    8    9   10
//! a→b         ●                             ●               events 1 and 7
//! c→d                        ●                              event 4
//! g→h                             ●                         event 5: in the GAP
//! e→f                                  ●                    event 6
//!
//! ranges [0,5) [6,10)  [==================)    [===================)
//! ```
use proptest::prelude::*;
use raphtory::{
    db::{
        api::view::{
            internal::{GraphTimeSemanticsOps, TimeRanges, TimeSemantics},
            DynamicGraph, IntoDynamic, StaticGraphViewOps,
        },
        graph::{
            edge::EdgeView,
            views::{deletion_graph::PersistentGraph, window_graph::WindowedGraph},
        },
    },
    prelude::*,
};
use raphtory_api::core::{
    entities::edges::edge_ref::EdgeRef,
    storage::timeindex::{AsTime, EventTime},
};
use std::{collections::BTreeSet, ops::Range};

fn build<G: StaticGraphViewOps + AdditionOps>(g: &G) {
    g.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
    g.add_edge(7, "a", "b", NO_PROPS, None).unwrap();
    g.add_edge(4, "c", "d", NO_PROPS, None).unwrap();
    g.add_edge(5, "g", "h", NO_PROPS, None).unwrap();
    g.add_edge(6, "e", "f", NO_PROPS, None).unwrap();
}

/// The fixture with a graph property `p` at 2, 5 and 8, as both graph kinds.
fn bases() -> [(&'static str, DynamicGraph); 2] {
    let g = Graph::new();
    build(&g);
    for (t, p) in [(2, 2i64), (5, 5), (8, 8)] {
        g.add_properties(t, [("p", p)]).unwrap();
    }
    [
        ("event", g.clone().into_dynamic()),
        ("persistent", g.persistent_graph().into_dynamic()),
    ]
}

fn ranges(v: &[Range<i64>]) -> TimeRanges {
    TimeRanges::new(v.iter().cloned().map(EventTime::range).collect())
}

fn edge_ids<'a, G: GraphViewOps<'a>>(g: &G) -> BTreeSet<(String, String)> {
    g.edges()
        .iter()
        .map(|e| (e.src().name(), e.dst().name()))
        .collect()
}

fn node_names<'a, G: GraphViewOps<'a>>(g: &G) -> BTreeSet<String> {
    g.nodes().iter().map(|n| n.name()).collect()
}

fn history<'a, G: GraphViewOps<'a>>(g: &G, src: &str, dst: &str) -> Vec<i64> {
    g.edge(src, dst)
        .map(|e| e.history().iter().map(|t| t.t()).collect())
        .unwrap_or_default()
}

fn exploded<'a, G: GraphViewOps<'a>>(e: Option<EdgeView<G>>) -> usize {
    e.map(|e| e.explode().iter().count()).unwrap_or(0)
}

/// What each single window keeps, combined as the multi-window view over them
/// should be: edges and nodes as a union, each edge's history and the exploded
/// counts range by range in order, and the extremes of the earliest and latest
/// times, to the millisecond (not the event id: the unfiltered fast path
/// invents one, #2883).
fn check_union<'a, G: GraphViewOps<'a>>(g: G, set: &TimeRanges) -> Result<(), TestCaseError> {
    let windows: Vec<_> = set.iter().map(|r| g.window(r.start, r.end)).collect();
    let multi = WindowedGraph::from_ranges(g.clone(), set.clone());

    let edges: BTreeSet<_> = windows.iter().flat_map(edge_ids).collect();
    prop_assert_eq!(edge_ids(&multi), edges.clone());
    let nodes: BTreeSet<_> = windows.iter().flat_map(node_names).collect();
    prop_assert_eq!(node_names(&multi), nodes);
    for (src, dst) in &edges {
        let want: Vec<i64> = windows.iter().flat_map(|w| history(w, src, dst)).collect();
        prop_assert_eq!(
            history(&multi, src, dst),
            want,
            "history of {}->{}",
            src,
            dst
        );
        let want: usize = windows.iter().map(|w| exploded(w.edge(src, dst))).sum();
        prop_assert_eq!(
            exploded(multi.edge(src, dst)),
            want,
            "explode {}->{}",
            src,
            dst
        );
    }
    let millis = |t: Option<EventTime>| t.map(|t| t.t());
    prop_assert_eq!(
        millis(multi.earliest_time()),
        windows
            .iter()
            .filter_map(|w| millis(w.earliest_time()))
            .min()
    );
    prop_assert_eq!(
        millis(multi.latest_time()),
        windows.iter().filter_map(|w| millis(w.latest_time())).max()
    );
    Ok(())
}

const DOMAIN: i64 = 20;

/// Updates on a few edges among four nodes: `(time, src, dst, deletes)`.
fn arb_updates() -> impl Strategy<Value = Vec<(i64, u64, u64, bool)>> {
    prop::collection::vec((0..DOMAIN, 0..4u64, 0..4u64, any::<bool>()), 0..24)
}

/// Any ranges, overlapping or touching ones included: the set normalises them.
fn arb_ranges() -> impl Strategy<Value = TimeRanges> {
    prop::collection::vec((0..DOMAIN, 0..DOMAIN), 0..4)
        .prop_map(|v| TimeRanges::new(v.into_iter().map(|(a, b)| EventTime::range(a..b)).collect()))
}

fn fill<G: AdditionOps + DeletionOps>(g: &G, updates: &[(i64, u64, u64, bool)]) {
    for &(t, src, dst, deletes) in updates {
        if deletes {
            g.delete_edge(t, src, dst, None).unwrap();
        } else {
            g.add_edge(t, src, dst, NO_PROPS, None).unwrap();
        }
    }
}

proptest! {
    #[test]
    fn multi_window_is_the_union_of_its_windows_on_an_event_graph(
        updates in arb_updates(),
        set in arb_ranges(),
    ) {
        let g = Graph::new();
        fill(&g, &updates);
        check_union(g, &set)?;
    }

    #[test]
    fn multi_window_is_the_union_of_its_windows_on_a_persistent_graph(
        updates in arb_updates(),
        set in arb_ranges(),
    ) {
        let g = PersistentGraph::new();
        fill(&g, &updates);
        check_union(g, &set)?;
    }
}

#[test]
fn persistent_edge_alive_across_the_gap_is_present_in_both_ranges() {
    // An edge added before the first range and never deleted is alive at the
    // start of each range, so each contributes the boundary event the single
    // window reports.
    let g = PersistentGraph::new();
    g.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
    let w1 = g.window(3, 5);
    let w2 = g.window(6, 10);
    let multi = WindowedGraph::from_ranges(g.clone(), ranges(&[3..5, 6..10]));
    let single = |w: &WindowedGraph<PersistentGraph>| exploded(w.edge("a", "b"));
    assert_eq!(single(&w1), 1);
    assert_eq!(single(&w2), 1);
    assert_eq!(exploded(multi.edge("a", "b")), single(&w1) + single(&w2));
}

#[test]
fn the_parent_window_is_folded_in_once_at_construction() {
    // window(0,8) then {[0,5), [6,10)} is {[0,5), [6,8)}.
    let g = Graph::new();
    build(&g);
    let view = WindowedGraph::from_ranges(g.window(0, 8), ranges(&[0..5, 6..10]));
    match view.edge_time_semantics() {
        TimeSemantics::MultiWindow(m) => assert_eq!(m.windows(), &ranges(&[0..5, 6..8])),
        other => panic!("expected MultiWindow, got {other:?}"),
    }
    // Ranges that leave a single interval are a plain window.
    let view =
        WindowedGraph::from_ranges(g.window(0, 8), TimeRanges::single(EventTime::range(3..20)));
    assert!(matches!(
        view.edge_time_semantics(),
        TimeSemantics::Window(_)
    ));
}

/// A window over a multi-window view sees only the ranges inside it, never the
/// gap between them, at the graph level as well as per edge.
#[test]
fn a_window_over_a_multi_window_view_keeps_out_of_the_gap() {
    for (kind, g) in bases() {
        let multi = WindowedGraph::from_ranges(g.clone(), ranges(&[0..5, 6..10]));
        let clipped = WindowedGraph::from_ranges(
            g.clone(),
            ranges(&[0..5, 6..10]).clipped_to(&EventTime::range(5..7)),
        );

        // [5,7) over {[0,5), [6,10)} is [6,7): the gap event at 5 stays out.
        let windowed = multi.window(5, 7);
        assert_eq!(windowed.earliest_time().map(|t| t.t()), Some(6), "{kind}");
        assert_eq!(windowed.earliest_time(), clipped.earliest_time(), "{kind}");
        assert_eq!(edge_ids(&windowed), edge_ids(&clipped), "{kind}");

        // [4,6) over the ranges is [4,5): the latest event is 4, not 5.
        let windowed = multi.window(4, 6);
        assert_eq!(windowed.latest_time().map(|t| t.t()), Some(4), "{kind}");
        // The graph-level questions a window asks of the view below it get the
        // same answers: the window is resolved against the ranges, not the hull.
        assert_eq!(multi.window(5, 7).earliest_time_global(), Some(6), "{kind}");
        assert_eq!(multi.window(4, 6).latest_time_global(), Some(4), "{kind}");

        // The graph property's history is the two windows' histories in order:
        // on an event graph the update in the gap is in neither; a persistent
        // window starts from the value carried in, so there it reappears at 6.
        fn values<'a, V: GraphViewOps<'a>>(g: &V) -> Vec<i64> {
            g.properties()
                .temporal()
                .get("p")
                .map(|p| p.values().filter_map(|v| v.into_i64()).collect())
                .unwrap_or_default()
        }
        let mut want = values(&g.window(0, 5));
        want.extend(values(&g.window(6, 10)));
        assert_eq!(values(&multi.window(0, 10)), want, "{kind}");
        if kind == "event" {
            assert_eq!(want, [2, 8]);
        }
    }
}

/// An edge's layers over the ranges are the union of its layers in each
/// window, each once: a layer the edge only touches in the gap is not one of
/// them.
/// The ranges a view reports are the ones it keeps, read from its bounds: a
/// multi-window view its own ranges, a window over it those ranges cut to the
/// window, gap and all. A filter resolved from a view reads these.
#[test]
fn a_view_reports_the_ranges_it_keeps() {
    for (kind, g) in bases() {
        let set = ranges(&[0..5, 6..10]);
        let multi = WindowedGraph::from_ranges(g.clone(), set.clone());
        assert_eq!(multi.view_ranges(), set, "{kind}");
        for (start, end) in [(4, 7), (5, 7), (0, 10)] {
            assert_eq!(
                multi.window(start, end).view_ranges(),
                set.clipped_to(&EventTime::range(start..end)),
                "{kind}: window({start},{end})"
            );
        }
        assert_eq!(g.view_ranges(), TimeRanges::all(), "{kind}");
        assert_eq!(g.window(2, 3).view_ranges(), ranges(&[2..3]), "{kind}");
    }
}

#[test]
fn edge_layers_are_the_union_over_the_ranges() {
    let g = Graph::new();
    g.add_edge(1, "a", "b", NO_PROPS, Some("x")).unwrap();
    g.add_edge(5, "a", "b", NO_PROPS, Some("gap")).unwrap();
    g.add_edge(7, "a", "b", NO_PROPS, Some("y")).unwrap();
    g.add_edge(8, "a", "b", NO_PROPS, Some("x")).unwrap();
    for (kind, g) in [
        ("event", g.clone().into_dynamic()),
        ("persistent", g.persistent_graph().into_dynamic()),
    ] {
        let multi = WindowedGraph::from_ranges(g.clone(), ranges(&[0..5, 6..10]));
        let layers = |g: &DynamicGraph| -> BTreeSet<String> {
            g.edge("a", "b")
                .map(|e| e.layer_names().into_iter().map(|l| l.to_string()).collect())
                .unwrap_or_default()
        };
        let want: BTreeSet<String> = layers(&g.window(0, 5).into_dynamic())
            .union(&layers(&g.window(6, 10).into_dynamic()))
            .cloned()
            .collect();
        let got: BTreeSet<String> = multi
            .edge("a", "b")
            .unwrap()
            .layer_names()
            .into_iter()
            .map(|l| l.to_string())
            .collect();
        assert_eq!(got, want, "{kind}");
        if kind == "event" {
            assert_eq!(
                got,
                ["x", "y"]
                    .into_iter()
                    .map(String::from)
                    .collect::<BTreeSet<_>>()
            );
        }
    }
}

/// ```text
/// time:  0    1    2    3    4    5    6    7
/// a→b         ●         ●         ●         ●     w = the update's time
/// ranges [0,2) [4,6)  [====)        [====)
/// ```
/// An exploded edge read through the multi-window view answers as it does
/// through whichever single window holds it: an event graph only inside a
/// range, a persistent graph also for the update a range carries in.
#[test]
fn an_exploded_edge_read_through_a_multi_window_answers_as_its_windows_do() {
    let g = Graph::new();
    for t in [1i64, 3, 5, 7] {
        g.add_edge(t, "a", "b", [("w", t)], None).unwrap();
    }
    let some = |t: i64| Some(Prop::I64(t));
    let bases = [
        (
            "event",
            g.clone().into_dynamic(),
            vec![some(1), None, some(5), None],
        ),
        (
            "persistent",
            g.persistent_graph().into_dynamic(),
            vec![some(1), some(3), some(5), None],
        ),
    ];
    for (kind, g, expected) in bases {
        let (w1, w2) = (g.window(0, 2), g.window(4, 6));
        let multi = WindowedGraph::from_ranges(g.clone(), ranges(&[0..2, 4..6]));
        let read = |view: DynamicGraph, e: EdgeRef| EdgeView::new(view, e).properties().get("w");
        let exploded = g.edges().explode();
        let from_windows: Vec<_> = exploded
            .iter()
            .map(|e| {
                read(w1.clone().into_dynamic(), e.edge).or(read(w2.clone().into_dynamic(), e.edge))
            })
            .collect();
        let from_multi: Vec<_> = exploded
            .iter()
            .map(|e| read(multi.clone().into_dynamic(), e.edge))
            .collect();
        assert_eq!(from_windows, expected, "{kind}: single windows");
        assert_eq!(from_multi, expected, "{kind}: multi-window");
    }
}

/// ```text
/// time:  0    1    2    3    4    5    6    7
/// a→b         ●+        ●-                         added @1, deleted @3 (in the gap)
/// c→d         ●+                  ●-               added @1, deleted @5 (in a range)
/// ranges [0,2) [4,6)  [====)        [====)
/// ```
/// A deletion is in a multi-window view exactly when some range sees it on its own:
/// one in the gap is not, one inside a range is.
#[test]
fn an_exploded_deletion_is_in_a_multi_window_as_its_windows_see_it() {
    let g = PersistentGraph::new();
    g.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
    g.delete_edge(3, "a", "b", None).unwrap();
    g.add_edge(1, "c", "d", NO_PROPS, None).unwrap();
    g.delete_edge(5, "c", "d", None).unwrap();
    let g = g.into_dynamic();
    let (w1, w2) = (g.window(0, 2), g.window(4, 6));
    let multi = WindowedGraph::from_ranges(g.clone(), ranges(&[0..2, 4..6])).into_dynamic();
    let deleted = |view: DynamicGraph, e: EdgeRef| EdgeView::new(view, e).is_deleted();
    let exploded = g.edges().explode();
    let from_windows: Vec<_> = exploded
        .iter()
        .map(|e| {
            deleted(w1.clone().into_dynamic(), e.edge) || deleted(w2.clone().into_dynamic(), e.edge)
        })
        .collect();
    let from_multi: Vec<_> = exploded
        .iter()
        .map(|e| deleted(multi.clone(), e.edge))
        .collect();
    assert_eq!(from_windows, vec![false, true], "single windows");
    assert_eq!(from_multi, from_windows, "multi-window");
}

/// ```text
/// time     0    1    2    3    4    5    6    7    8    9
/// 0→0                                ●-                    deleted @6, never added
/// ranges   [=============================)    [====)
/// ```
/// A node is in a multi-window view when any range has it: node 0 is in the
/// first window only, through its edge's deletion there.
#[test]
fn a_node_in_any_range_is_in_the_multi_window_view() {
    let g = PersistentGraph::new();
    g.delete_edge(6, 0, 0, None).unwrap();
    let multi = WindowedGraph::from_ranges(g.clone(), ranges(&[0..7, 8..9]));
    assert_eq!(
        node_names(&g.window(0, 7)),
        BTreeSet::from(["0".to_string()])
    );
    assert_eq!(node_names(&multi), node_names(&g.window(0, 7)));
}
