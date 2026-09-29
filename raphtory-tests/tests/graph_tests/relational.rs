//! Checks the claims about current behaviour made in `relational/*.md`
//! (subgraph.md, window.md). Each test names the section it verifies.
//! A failing test means the document is wrong, not the code.

use itertools::Itertools;
use raphtory::{
    db::{
        api::view::internal::{DynamicGraph, GraphTimeSemanticsOps, IntoDynamic},
        graph::views::deletion_graph::PersistentGraph,
    },
    prelude::*,
};
use raphtory_api::core::{
    entities::GID,
    storage::timeindex::{AsTime, EventTime},
};

fn ts(times: Vec<EventTime>) -> Vec<i64> {
    times.into_iter().map(|t| t.t()).collect()
}

fn both(pg: &PersistentGraph) -> [DynamicGraph; 2] {
    [pg.clone().into_dynamic(), pg.event_graph().into_dynamic()]
}

// ---------------------------------------------------------------------------------------------
// subgraph.md
// ---------------------------------------------------------------------------------------------

/// subgraph.md §2.1 / §4.1: input is deduplicated keeping first occurrence, and
/// `nodes().id()` follows that order.
#[test]
fn subgraph_node_order_is_first_occurrence_of_input() {
    let g = Graph::new();
    for v in [1, 2, 3] {
        g.add_node(0, v, NO_PROPS, None, None).unwrap();
    }
    let sg = g.subgraph([3, 1, 2, 1]);
    assert_eq!(
        sg.nodes().id().collect_vec(),
        [GID::U64(3), GID::U64(1), GID::U64(2)]
    );
    assert_eq!(sg.count_nodes(), 3);
}

/// subgraph.md §2.2 / §2.3: `exclude_nodes` runs the existence rule, so a node whose only
/// neighbours are excluded (and that has no updates of its own) disappears.
#[test]
fn exclude_nodes_drops_nodes_whose_only_neighbours_are_excluded() {
    let pg = PersistentGraph::new();
    pg.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
    pg.add_edge(0, 2, 3, NO_PROPS, None).unwrap();
    pg.add_node(0, 4, NO_PROPS, None, None).unwrap();
    for g in both(&pg) {
        let ex = g.exclude_nodes([2]);
        assert_eq!(ex.nodes().id().collect_vec(), [GID::U64(4)]);
    }
}

/// subgraph.md §2.2: an incident edge with only a deletion keeps both endpoints alive when
/// both are requested (event rule: adjacency; persistent rule: the deletion is an event).
/// subgraph.md §4.7: that edge counts in `count_edges`.
#[test]
fn subgraph_deletion_only_edge_keeps_nodes_and_counts() {
    let pg = PersistentGraph::new();
    pg.delete_edge(0, 5, 6, None).unwrap();
    for g in both(&pg) {
        let sg = g.subgraph([5, 6]);
        assert_eq!(sg.count_nodes(), 2);
        assert_eq!(sg.count_edges(), 1);
        assert_eq!(sg.edges().id().collect_vec().len(), 1);
    }
}

/// subgraph.md §4.5: degree counts distinct neighbours across layers and directions, and
/// ignores neighbours outside `Kept`.
#[test]
fn subgraph_degree_counts_distinct_kept_neighbours() {
    let g = Graph::new();
    g.add_edge(0, 1, 2, NO_PROPS, Some("a")).unwrap();
    g.add_edge(0, 1, 2, NO_PROPS, Some("b")).unwrap();
    g.add_edge(0, 2, 1, NO_PROPS, None).unwrap();
    g.add_edge(0, 1, 3, NO_PROPS, None).unwrap();
    let sg = g.subgraph([1, 2]);
    let n = sg.node(1).unwrap();
    assert_eq!(n.degree(), 1);
    assert_eq!(n.out_degree(), 1);
    assert_eq!(n.in_degree(), 1);
    assert_eq!(g.node(1).unwrap().degree(), 2);
}

/// subgraph.md §4.10 ⚠: `v.history()` includes incident edge deletion times (both semantics),
/// while `e.history()` lists additions only. Same on the base graph.
#[test]
fn node_history_includes_edge_deletions() {
    let pg = PersistentGraph::new();
    pg.add_edge(1, 1, 2, NO_PROPS, None).unwrap();
    pg.delete_edge(3, 1, 2, None).unwrap();
    for g in both(&pg) {
        let sg = g.subgraph([1, 2]);
        assert_eq!(ts(sg.node(1).unwrap().history().collect()), [1, 3]);
        assert_eq!(ts(sg.edge(1, 2).unwrap().history().collect()), [1]);
        assert_eq!(ts(g.node(1).unwrap().history().collect()), [1, 3]);
        assert_eq!(ts(g.edge(1, 2).unwrap().history().collect()), [1]);
    }
}

/// subgraph.md §4.10: events of edges leaving `Kept` are removed from the node's history.
#[test]
fn subgraph_node_history_drops_events_of_removed_edges() {
    let pg = PersistentGraph::new();
    pg.add_edge(1, 1, 2, NO_PROPS, None).unwrap();
    pg.add_edge(2, 1, 3, NO_PROPS, None).unwrap();
    for g in both(&pg) {
        let n = g.subgraph([1, 2]).node(1).unwrap();
        assert_eq!(ts(n.history().collect()), [1]);
        assert_eq!(n.latest_time().map(|t| t.t()), Some(1));
    }
}

/// subgraph.md §4.15 ⚠: graph properties are not restricted by the subgraph and leak into
/// `earliest_time` / `latest_time`.
#[test]
fn subgraph_graph_props_leak_into_graph_time() {
    let pg = PersistentGraph::new();
    pg.add_properties(0, [("p", 1i64)]).unwrap();
    pg.add_edge(5, 1, 2, NO_PROPS, None).unwrap();
    pg.add_edge(10, 3, 4, NO_PROPS, None).unwrap();
    pg.add_properties(20, [("p", 2i64)]).unwrap();
    for g in both(&pg) {
        let sg = g.subgraph([1, 2]);
        assert_eq!(sg.earliest_time().map(|t| t.t()), Some(0));
        assert_eq!(sg.latest_time().map(|t| t.t()), Some(20));
        // the node-level answer is restricted
        assert_eq!(sg.node(1).unwrap().earliest_time().map(|t| t.t()), Some(5));
    }
}

/// subgraph.md §4.14 ⚠: an open persistent exploded edge ends at the *base* graph's latest time.
#[test]
fn persistent_subgraph_open_exploded_edge_uses_base_latest_time() {
    let pg = PersistentGraph::new();
    pg.add_edge(1, 1, 2, NO_PROPS, None).unwrap();
    pg.add_edge(100, 3, 4, NO_PROPS, None).unwrap();
    let sg = pg.subgraph([1, 2]);
    assert_eq!(sg.latest_time().map(|t| t.t()), Some(1));
    let ends = sg
        .edge(1, 2)
        .unwrap()
        .explode()
        .latest_time()
        .map(|t| t.map(|t| t.t()))
        .collect_vec();
    assert_eq!(ends, [Some(100)]);
}

/// subgraph.md §4.16 ⚠ / window.md §5.13 ⚠: unwindowed persistent edge `latest` / `at(t)`
/// ignore deletions, while the windowed variants reset at deletions.
#[test]
fn persistent_edge_prop_latest_ignores_deletion_unless_windowed() {
    let pg = PersistentGraph::new();
    pg.add_edge(1, 1, 2, [("x", 1i64)], None).unwrap();
    pg.delete_edge(5, 1, 2, None).unwrap();

    let e = pg.edge(1, 2).unwrap();
    assert_eq!(e.properties().get("x"), Some(Prop::I64(1)));
    assert_eq!(
        e.properties().temporal().get("x").and_then(|p| p.at(6)),
        Some(Prop::I64(1))
    );

    let we = pg.window(i64::MIN, i64::MAX).edge(1, 2).unwrap();
    assert_eq!(we.properties().get("x"), None);
    let we = pg.window(0, 10).edge(1, 2).unwrap();
    assert_eq!(
        we.properties().temporal().get("x").and_then(|p| p.at(6)),
        None
    );
}

/// subgraph.md §4.17 ⚠: under event semantics a deletion-only edge exists (counts in
/// `count_edges`) but has no metadata; under persistent semantics it has metadata.
#[test]
fn event_deletion_only_edge_has_no_metadata() {
    let pg = PersistentGraph::new();
    let e = pg.delete_edge(1, 1, 2, None).unwrap();
    e.add_metadata([("m", 1i64)], None).unwrap();

    let g = pg.event_graph();
    let sg = g.subgraph([1, 2]);
    assert_eq!(sg.count_edges(), 1);
    assert_eq!(sg.edge(1, 2).unwrap().metadata().get("m"), None);
    assert_eq!(g.edge(1, 2).unwrap().metadata().get("m"), None);

    let psg = pg.subgraph([1, 2]);
    assert_eq!(psg.count_edges(), 1);
    assert_eq!(
        psg.edge(1, 2).unwrap().metadata().get("m"),
        Some(Prop::I64(1))
    );
}

// ---------------------------------------------------------------------------------------------
// window.md
// ---------------------------------------------------------------------------------------------

/// window.md §1.1 / §1.3: constructor ranges, windows of windows intersect, and inverted
/// windows are clamped to empty.
#[test]
fn window_constructor_ranges() {
    let pg = PersistentGraph::new();
    pg.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
    pg.add_edge(7, 1, 2, NO_PROPS, None).unwrap();
    let g = pg.event_graph();
    let t = |x: Option<EventTime>| x.map(|t| t.as_tuple());

    let w = g.window(0, 10).window(5, 20);
    assert_eq!(t(w.start()), Some((5, 0)));
    assert_eq!(t(w.end()), Some((10, 0)));

    let w = g.window(10, 5);
    assert_eq!(t(w.start()), Some((10, 0)));
    assert_eq!(t(w.end()), Some((10, 0)));
    assert_eq!(w.count_nodes(), 0);

    assert_eq!(t(g.at(3).start()), Some((3, 0)));
    assert_eq!(t(g.at(3).end()), Some((4, 0)));
    assert_eq!(t(g.after(3).start()), Some((4, 0)));
    assert_eq!(t(g.after(3).end()), None);
    assert_eq!(t(g.before(3).end()), Some((3, 0)));
    assert_eq!(t(g.latest().start()), Some((7, 0)));
    assert_eq!(t(g.latest().end()), Some((8, 0)));

    // snapshot_at: event = before(t+1), persistent = at(t)
    assert_eq!(t(g.snapshot_at(5).start()), None);
    assert_eq!(t(g.snapshot_at(5).end()), Some((6, 0)));
    assert_eq!(t(pg.snapshot_at(5).start()), Some((5, 0)));
    assert_eq!(t(pg.snapshot_at(5).end()), Some((6, 0)));
}

/// window.md §3.1 ⚠: a persistent node that was `add_node`-ed exists in every later window;
/// a node that only exists through a deleted edge does not. Event semantics has neither.
#[test]
fn persistent_node_lifetime_depends_on_add_node() {
    let pg = PersistentGraph::new();
    pg.add_node(0, 1, NO_PROPS, None, None).unwrap();
    pg.add_edge(0, 2, 3, NO_PROPS, None).unwrap();
    pg.delete_edge(1, 2, 3, None).unwrap();

    let w = pg.window(100, 200);
    assert!(w.has_node(1));
    assert!(!w.has_node(2));
    assert_eq!(w.count_nodes(), 1);

    let ew = pg.event_graph().window(100, 200);
    assert!(!ew.has_node(1));
    assert_eq!(ew.count_nodes(), 0);
}

/// window.md §3.2 ⚠ (event): an edge whose only event in the window is a deletion is in the
/// window, but has no history and no exploded edges.
#[test]
fn event_window_deletion_only_edge_is_present() {
    let g = Graph::new();
    g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
    g.delete_edge(5, 1, 2, None).unwrap();

    let w = g.window(4, 6);
    assert_eq!(w.count_edges(), 1);
    assert_eq!(w.count_nodes(), 2);
    assert_eq!(w.count_temporal_edges(), 0);
    let e = w.edge(1, 2).unwrap();
    assert!(!e.is_valid());
    assert!(e.is_deleted());
    assert!(e.history().is_empty());
    assert_eq!(e.explode().iter().count(), 0);
}

/// window.md §5.6 ⚠: the persistent persisted row is in `explode` / `count_temporal_edges`
/// but not in `history`.
#[test]
fn persistent_persisted_row_in_explode_not_history() {
    let pg = PersistentGraph::new();
    pg.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
    pg.add_edge(2, 1, 2, NO_PROPS, None).unwrap();
    pg.add_edge(4, 1, 2, NO_PROPS, None).unwrap();

    let w = pg.window(1, 5);
    let e = w.edge(1, 2).unwrap();
    assert_eq!(w.count_temporal_edges(), 3);
    assert_eq!(e.explode().iter().count(), 3);
    assert_eq!(ts(e.history().collect()), [2, 4]);
}

/// window.md §5.10 ⚠ / §5.11: persistent node history has no persisted row and includes a
/// deletion at the start tick, while node `earliest_time` is clamped to `lo` and edge
/// deletions exclude the start tick.
#[test]
fn persistent_node_history_vs_earliest_time_at_window_start() {
    let pg = PersistentGraph::new();
    pg.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
    let n = pg.window(2, 5).node(1).unwrap();
    assert!(n.history().is_empty());
    assert_eq!(n.earliest_time().map(|t| t.t()), Some(2));
    assert_eq!(n.latest_time().map(|t| t.t()), Some(2));

    let pg = PersistentGraph::new();
    pg.add_node(0, 1, NO_PROPS, None, None).unwrap();
    pg.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
    pg.delete_edge(2, 1, 2, None).unwrap();
    let w = pg.window(2, 5);
    let n = w.node(1).unwrap();
    assert_eq!(ts(n.history().collect()), [2]);
    assert_eq!(n.earliest_time().map(|t| t.t()), Some(2));
    assert!(!w.has_edge(1, 2));
    assert!(pg.edge(1, 2).unwrap().window(2, 5).deletions().is_empty());
}

/// window.md §5.14 ⚠: the persisted row's `EventTime` differs by entity: exactly `lo` for
/// node and graph props, `(lo.t, i_addition)` for edge props and exploded edges.
#[test]
fn persistent_persisted_row_event_time_differs_by_entity() {
    let pg = PersistentGraph::new();
    pg.add_properties(0, [("z", 1i64)]).unwrap();
    pg.add_node(0, 1, [("x", 1i64)], None, None).unwrap();
    pg.add_edge(0, 1, 2, [("y", 1i64)], None).unwrap();
    let add_i = pg.edge(1, 2).unwrap().history().collect()[0].1;
    assert!(
        add_i > 0,
        "setup: the edge addition must not have event id 0"
    );

    let w = pg.window(5, 10);
    let lo = EventTime::new(5, 0);
    let persisted_edge = EventTime::new(5, add_i);

    let node_prop = w.node(1).unwrap().properties().temporal().get("x").unwrap();
    assert_eq!(node_prop.history().collect(), [lo]);

    let graph_prop = w.properties().temporal().get("z").unwrap();
    assert_eq!(graph_prop.history().collect(), [lo]);

    let e = w.edge(1, 2).unwrap();
    let edge_prop = e.properties().temporal().get("y").unwrap();
    assert_eq!(edge_prop.history().collect(), [persisted_edge]);
    let exploded = e
        .explode()
        .time_and_event_id()
        .map(|t| t.unwrap())
        .collect_vec();
    assert_eq!(exploded, [persisted_edge]);
}

/// window.md §5.14 ⚠: across layers, the persisted node prop keeps a single value (the latest),
/// while edges keep one persisted value per layer.
#[test]
fn persistent_multilayer_persisted_node_prop_is_single() {
    let pg = PersistentGraph::new();
    pg.add_node(0, 1, [("x", 1i64)], None, Some("a")).unwrap();
    pg.add_node(1, 1, [("x", 2i64)], None, Some("b")).unwrap();
    pg.add_edge(0, 3, 4, [("y", 1i64)], Some("a")).unwrap();
    pg.add_edge(1, 3, 4, [("y", 2i64)], Some("b")).unwrap();

    let w = pg.window(5, 10);
    let node_values = w
        .node(1)
        .unwrap()
        .properties()
        .temporal()
        .get("x")
        .unwrap()
        .values()
        .collect_vec();
    assert_eq!(node_values, [Prop::I64(2)]);

    let edge_values = w
        .edge(3, 4)
        .unwrap()
        .properties()
        .temporal()
        .get("y")
        .unwrap()
        .values()
        .collect_vec();
    assert_eq!(edge_values, [Prop::I64(1), Prop::I64(2)]);
}

/// window.md §5.12 ⚠: the persistent graph-level `earliest/latest_time_global` of a window are
/// approximations and disagree with `earliest_time()` when nothing is alive in the window.
#[test]
fn persistent_window_time_statistics_are_approximate() {
    let pg = PersistentGraph::new();
    pg.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
    pg.delete_edge(1, 1, 2, None).unwrap();

    let w = pg.window(5, 10);
    assert_eq!(w.count_nodes(), 0);
    assert_eq!(w.count_edges(), 0);
    assert_eq!(w.earliest_time(), None);
    assert_eq!(w.latest_time(), None);
    assert_eq!(w.earliest_time_global(), Some(5));
    assert_eq!(w.latest_time_global(), Some(5));
}

/// window.md §5.15 ⚠ (boundary bug): a graph property's `latest` in a window includes an
/// update at exactly the exclusive end `hi`, because event ids are insertion-ordered.
#[test]
fn window_graph_prop_latest_includes_exclusive_end() {
    let pg = PersistentGraph::new();
    pg.add_properties(10, [("x", 1i64)]).unwrap(); // first event: id 0 → (10, 0)
    pg.add_properties(5, [("x", 2i64)]).unwrap(); // (5, 1)
    let hist = pg
        .properties()
        .temporal()
        .get("x")
        .unwrap()
        .history()
        .collect();
    assert_eq!(hist, [EventTime::new(5, 1), EventTime::new(10, 0)]);

    for g in both(&pg) {
        let w = g.window(0, 10); // hi = (10, 0), exclusive
        let prop = w.properties().temporal().get("x").unwrap();
        // the history correctly excludes the update at hi ...
        assert_eq!(prop.values().collect_vec(), [Prop::I64(2)]);
        // ... but latest() returns it
        assert_eq!(prop.latest(), Some(Prop::I64(1)));
        assert_eq!(w.properties().get("x"), Some(Prop::I64(1)));
    }
}

// ---------------------------------------------------------------------------------------------
// layers.md
// ---------------------------------------------------------------------------------------------

fn sorted_ids(ids: Vec<GID>) -> Vec<GID> {
    ids.into_iter().sorted().collect()
}

/// layers.md §1.3: `layers(A).layers(B)` is `A ∩ B`; an empty intersection is an empty view,
/// not an error.
#[test]
fn layers_of_layers_intersect() {
    let g = Graph::new();
    g.add_edge(0, 1, 2, NO_PROPS, Some("a")).unwrap();
    g.add_edge(0, 3, 4, NO_PROPS, Some("b")).unwrap();
    g.add_edge(0, 5, 6, NO_PROPS, Some("c")).unwrap();

    let ab_b = g.layers(["a", "b"]).unwrap().layers("b").unwrap();
    assert_eq!(
        ab_b.edges().id().collect_vec(),
        [(GID::U64(3), GID::U64(4))]
    );

    let a_b = g.layers("a").unwrap().layers("b").unwrap();
    assert_eq!(a_b.count_edges(), 0);
    assert_eq!(a_b.count_nodes(), 0);
    assert_eq!(a_b.unique_layers().count(), 0);
}

/// layers.md §3.1: a node is in a layer view if it has an entry in a selected layer, or has
/// layer-less node updates.
#[test]
fn layer_node_existence_rule() {
    let pg = PersistentGraph::new();
    pg.add_node(0, 1, NO_PROPS, None, None).unwrap();
    pg.add_edge(0, 2, 3, NO_PROPS, Some("b")).unwrap();
    pg.add_node(0, 4, NO_PROPS, None, Some("a")).unwrap();
    for g in both(&pg) {
        let ids = |v: &DynamicGraph| sorted_ids(v.nodes().id().collect_vec());
        assert_eq!(
            ids(&g.layers("b").unwrap().into_dynamic()),
            [GID::U64(1), GID::U64(2), GID::U64(3)]
        );
        assert_eq!(
            ids(&g.layers("a").unwrap().into_dynamic()),
            [GID::U64(1), GID::U64(4)]
        );
        assert_eq!(
            ids(&g.valid_layers(Layer::None).into_dynamic()),
            [GID::U64(1)]
        );
    }
}

/// layers.md §1.1 ⚠: layer-less edges live in "_default"; layer-less nodes live in the static
/// layer and are visible in every selection.
#[test]
fn default_layer_is_not_static_layer() {
    let g = Graph::new();
    g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
    g.add_edge(0, 4, 5, NO_PROPS, Some("b")).unwrap();
    g.add_node(0, 3, NO_PROPS, None, None).unwrap();

    let d = g.default_layer();
    assert_eq!(d.edges().id().collect_vec(), [(GID::U64(1), GID::U64(2))]);
    assert_eq!(
        sorted_ids(d.nodes().id().collect_vec()),
        [GID::U64(1), GID::U64(2), GID::U64(3)]
    );

    let b = g.layers("b").unwrap();
    assert_eq!(
        sorted_ids(b.nodes().id().collect_vec()),
        [GID::U64(3), GID::U64(4), GID::U64(5)]
    );
}

/// layers.md §1.2 ⚠: the layer set is resolved at construction. Naming every existing layer
/// normalises to `All`, which also sees layers created later; `exclude_layers` materialises
/// the complement and does not.
#[test]
fn layer_set_is_resolved_at_construction() {
    let g = Graph::new();
    g.add_edge(0, 1, 2, NO_PROPS, Some("a")).unwrap();
    g.add_edge(0, 3, 4, NO_PROPS, Some("b")).unwrap();

    let all_named = g.layers(["a", "b"]).unwrap();
    let only_a = g.layers("a").unwrap();
    let not_a = g.exclude_layers("a").unwrap();

    g.add_edge(0, 5, 6, NO_PROPS, Some("c")).unwrap();

    assert_eq!(all_named.count_edges(), 3); // sees the new layer "c"
    assert_eq!(only_a.count_edges(), 1);
    assert_eq!(not_a.count_edges(), 1); // only "b": "c" is not visible
    assert_eq!(g.exclude_layers("a").unwrap().count_edges(), 2); // rebuilt view sees "c"
}

/// layers.md §5.2 / §5.4: degree uses per-layer adjacency; deletion-only edges count in the
/// layer they were deleted in.
#[test]
fn layer_degree_and_edges() {
    let pg = PersistentGraph::new();
    pg.add_edge(0, 1, 2, NO_PROPS, Some("a")).unwrap();
    pg.add_edge(0, 1, 3, NO_PROPS, Some("b")).unwrap();
    pg.delete_edge(0, 1, 4, Some("c")).unwrap();
    for g in both(&pg) {
        assert_eq!(g.layers("a").unwrap().node(1).unwrap().degree(), 1);
        assert_eq!(g.layers(["a", "b"]).unwrap().node(1).unwrap().degree(), 2);
        assert_eq!(g.node(1).unwrap().degree(), 3);
        let c = g.layers("c").unwrap();
        assert_eq!(c.count_edges(), 1);
        assert_eq!(c.edges().id().collect_vec(), [(GID::U64(1), GID::U64(4))]);
        assert_eq!(c.count_temporal_edges(), 0);
    }
}

/// layers.md §5.7 / §5.9: node history and node props include layer-less (static) updates and
/// selected layers only.
#[test]
fn layer_node_history_and_props_include_static_layer() {
    let pg = PersistentGraph::new();
    pg.add_node(0, 1, [("x", 1i64)], None, None).unwrap();
    pg.add_node(1, 1, [("x", 2i64)], None, Some("a")).unwrap();
    pg.add_edge(2, 1, 2, NO_PROPS, Some("b")).unwrap();
    for g in both(&pg) {
        let n = g.layers("b").unwrap().node(1).unwrap();
        assert_eq!(ts(n.history().collect()), [0, 2]);
        assert_eq!(n.properties().get("x"), Some(Prop::I64(1)));
        let n = g.layers("a").unwrap().node(1).unwrap();
        assert_eq!(ts(n.history().collect()), [0, 1]);
        assert_eq!(n.properties().get("x"), Some(Prop::I64(2)));
    }
}

/// layers.md §5.8 ⚠: graph properties are not layer-restricted and set the view's time bounds.
#[test]
fn layer_graph_props_leak_into_graph_time() {
    let pg = PersistentGraph::new();
    pg.add_properties(0, [("p", 1i64)]).unwrap();
    pg.add_edge(5, 1, 2, NO_PROPS, Some("a")).unwrap();
    pg.add_edge(10, 3, 4, NO_PROPS, Some("b")).unwrap();
    for g in both(&pg) {
        let a = g.layers("a").unwrap();
        assert_eq!(a.earliest_time().map(|t| t.t()), Some(0));
        assert_eq!(a.latest_time().map(|t| t.t()), Some(5));
    }
}

/// layers.md §4 / §5.6: persistent validity combines only over the selected layers.
#[test]
fn persistent_validity_depends_on_selected_layers() {
    let pg = PersistentGraph::new();
    pg.add_edge(0, 1, 2, NO_PROPS, Some("a")).unwrap();
    pg.add_edge(0, 1, 2, NO_PROPS, Some("b")).unwrap();
    pg.delete_edge(1, 1, 2, Some("b")).unwrap();

    let e = pg.edge(1, 2).unwrap();
    assert!(e.is_valid());
    assert!(!e.is_deleted());

    let eb = pg.layers("b").unwrap().edge(1, 2).unwrap();
    assert!(!eb.is_valid());
    assert!(eb.is_deleted());

    let ea = pg.layers("a").unwrap().edge(1, 2).unwrap();
    assert!(ea.is_valid());
    assert!(!ea.is_deleted());
}

/// layers.md §5.9 ⚠: edge metadata is a map over layers with several layers in view, and the
/// plain value with one.
#[test]
fn edge_metadata_shape_depends_on_layer_count() {
    let g = Graph::new();
    g.add_edge(0, 1, 2, NO_PROPS, Some("a"))
        .unwrap()
        .add_metadata([("m", 1i64)], Some("a"))
        .unwrap();
    g.add_edge(0, 1, 2, NO_PROPS, Some("b"))
        .unwrap()
        .add_metadata([("m", 2i64)], Some("b"))
        .unwrap();

    let all = g.edge(1, 2).unwrap().metadata().get("m");
    assert!(matches!(all, Some(Prop::Map(_))), "got {all:?}");
    assert_eq!(
        g.layers("a")
            .unwrap()
            .edge(1, 2)
            .unwrap()
            .metadata()
            .get("m"),
        Some(Prop::I64(1))
    );
}

/// layers.md §1.4: an edge reference pinned to one layer answers empty under other layers.
#[test]
fn layer_pinned_edge_ref_hides_other_layers() {
    let g = Graph::new();
    let pinned = g.add_edge(0, 1, 2, NO_PROPS, Some("a")).unwrap();
    g.add_edge(1, 1, 2, NO_PROPS, Some("b")).unwrap();

    assert!(pinned.layers("b").unwrap().history().is_empty());
    let unpinned = g.edge(1, 2).unwrap();
    assert_eq!(ts(unpinned.layers("b").unwrap().history().collect()), [1]);
}
