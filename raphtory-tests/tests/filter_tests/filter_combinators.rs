use raphtory::{
    db::{
        api::view::{internal::IntoDynGraphArc, DynamicGraph, Filter, IntoDynamic},
        graph::views::filter::{
            and_filtered_graph::AndFilteredGraph,
            model::{
                edge_filter::EdgeFilter, exploded_edge_filter::ExplodedEdgeFilter,
                node_filter::NodeFilter, ComposableFilter, EntityExprFilterOps,
                PropertyExprFactory,
            },
            or_filtered_graph::OrFilteredGraph,
        },
    },
    prelude::{
        AdditionOps, EdgeViewOps, Graph, GraphViewOps, LayerOps, NodeViewOps, TimeOps, NO_PROPS,
    },
};
use raphtory_api::core::{entities::properties::prop::IntoProp, storage::timeindex::AsTime};
use std::{collections::BTreeSet, sync::Arc};

/// a.score 9@1 · b.score 6@7 · a→b [x] @3 · b→c [y] @7
fn graph() -> Graph {
    let g = Graph::new();
    g.add_node(1, "a", [("score", 9i64.into_prop())], None, None)
        .unwrap();
    g.add_node(7, "b", [("score", 6i64.into_prop())], None, None)
        .unwrap();
    g.add_edge(3, "a", "b", NO_PROPS, Some("x")).unwrap();
    g.add_edge(7, "b", "c", NO_PROPS, Some("y")).unwrap();
    g
}

/// The graph as an event graph and as a persistent graph.
fn bases() -> [(&'static str, DynamicGraph); 2] {
    let g = graph();
    [
        ("event", g.clone().into_dynamic()),
        ("persistent", g.persistent_graph().into_dynamic()),
    ]
}

/// The nodes with a score above 4.
fn scored(base: &DynamicGraph) -> DynamicGraph {
    base.filter(NodeFilter.property("score").gt(4i64))
        .unwrap()
        .into_dynamic()
}

/// Each view an operand can carry, by name.
fn view_operands(base: &DynamicGraph) -> Vec<(&'static str, DynamicGraph)> {
    vec![
        ("window", base.window(0, 2).into_dynamic()),
        ("at", base.at(1).into_dynamic()),
        ("latest", base.latest().into_dynamic()),
        ("snapshot_at", base.snapshot_at(1).into_dynamic()),
        ("layers", base.layers("x").unwrap().into_dynamic()),
    ]
}

fn names<'graph, G: GraphViewOps<'graph>>(g: &G) -> BTreeSet<String> {
    g.nodes().iter().map(|n| n.name()).collect()
}

fn edge_ids<'graph, G: GraphViewOps<'graph>>(g: &G) -> BTreeSet<String> {
    g.edges()
        .iter()
        .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
        .collect()
}

fn set(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|s| s.to_string()).collect()
}

#[test]
fn and_keeps_what_both_operands_keep_whatever_view_an_operand_carries() {
    for (kind, base) in bases() {
        let pred = scored(&base);
        for (view, operand) in view_operands(&base) {
            let and = AndFilteredGraph::new(
                base.clone(),
                Arc::from([
                    operand.clone().into_dyn_graph_arc(),
                    pred.clone().into_dyn_graph_arc(),
                ]),
            );
            let label = format!("{kind} {view} & score>4");
            assert_eq!(
                names(&and),
                &names(&operand) & &names(&pred),
                "{label}: nodes"
            );
            assert_eq!(
                edge_ids(&and),
                &edge_ids(&operand) & &edge_ids(&pred),
                "{label}: edges"
            );
        }
    }
}

#[test]
fn and_with_a_window_operand_keeps_only_what_is_in_the_window() {
    for (kind, base) in bases() {
        let and = AndFilteredGraph::new(
            base.clone(),
            Arc::from([
                base.window(0, 2).into_dyn_graph_arc(),
                scored(&base).into_dyn_graph_arc(),
            ]),
        );
        assert_eq!(names(&and), set(&["a"]), "{kind}: nodes");
        assert_eq!(edge_ids(&and), set(&[]), "{kind}: edges");
    }
}

#[test]
fn or_keeps_what_either_operand_keeps_whatever_view_an_operand_carries() {
    for (kind, base) in bases() {
        let pred = scored(&base);
        for (view, operand) in view_operands(&base) {
            let or = OrFilteredGraph::new(
                base.clone(),
                Arc::from([
                    operand.clone().into_dyn_graph_arc(),
                    pred.clone().into_dyn_graph_arc(),
                ]),
            );
            assert_eq!(
                names(&or),
                &names(&operand) | &names(&pred),
                "{kind} {view} | score>4: nodes"
            );
        }
    }
}

#[test]
fn or_with_a_window_operand_adds_the_window_to_the_predicate() {
    for (kind, base) in bases() {
        let or = OrFilteredGraph::new(
            base.clone(),
            Arc::from([
                base.window(0, 2).into_dyn_graph_arc(),
                scored(&base).into_dyn_graph_arc(),
            ]),
        );
        assert_eq!(names(&or), set(&["a", "b"]), "{kind}: nodes");
        assert_eq!(edge_ids(&or), set(&["a->b"]), "{kind}: edges");
    }
}

#[test]
fn not_of_a_node_predicate_keeps_the_nodes_it_drops() {
    let g = graph();
    g.add_node(7, "c", NO_PROPS, None, None).unwrap();
    g.add_node(2, "d", NO_PROPS, None, None).unwrap();
    for (kind, base) in [
        ("event", g.clone().into_dynamic()),
        ("persistent", g.persistent_graph().into_dynamic()),
    ] {
        let not = base
            .filter(NodeFilter.property("score").gt(4i64).not())
            .unwrap();
        assert_eq!(names(&not), set(&["c", "d"]), "{kind}: nodes");
        assert_eq!(edge_ids(&not), set(&[]), "{kind}: edges");
    }
}

#[test]
fn not_of_an_edge_predicate_keeps_the_edges_it_drops() {
    let g = Graph::new();
    for (t, src, dst, w) in [(1, "a", "b", 1i64), (2, "b", "c", 9)] {
        g.add_edge(t, src, dst, [("w", w.into_prop())], None)
            .unwrap();
    }
    for (kind, base) in [
        ("event", g.clone().into_dynamic()),
        ("persistent", g.persistent_graph().into_dynamic()),
    ] {
        let not = base
            .filter(EdgeFilter.property("w").gt(5i64).not())
            .unwrap();
        assert_eq!(edge_ids(&not), set(&["a->b"]), "{kind}: edges");
        assert_eq!(names(&not), set(&["a", "b"]), "{kind}: nodes");
    }
}

/// a→b w=1 [x] @1 · w=9 [y] @3 · w=2 [x] @7
fn updates_graph() -> Graph {
    let g = Graph::new();
    for (t, layer, w) in [(1, "x", 1i64), (3, "y", 9), (7, "x", 2)] {
        g.add_edge(t, "a", "b", [("w", w.into_prop())], Some(layer))
            .unwrap();
    }
    g
}

/// Operands that restrict updates and layers: a window, a layer and an update
/// predicate.
fn update_operands(base: &DynamicGraph) -> Vec<(&'static str, DynamicGraph)> {
    vec![
        ("window", base.window(0, 4).into_dynamic()),
        ("layers", base.layers("x").unwrap().into_dynamic()),
        (
            "w>5",
            base.filter(ExplodedEdgeFilter.property("w").gt(5i64))
                .unwrap()
                .into_dynamic(),
        ),
    ]
}

fn updates<'graph, G: GraphViewOps<'graph>>(g: &G) -> BTreeSet<(String, String, i64, String)> {
    g.edges()
        .explode()
        .iter()
        .map(|e| {
            (
                e.src().name(),
                e.dst().name(),
                e.time().unwrap().t(),
                e.layer_name().unwrap().to_string(),
            )
        })
        .collect()
}

fn edge_layers<'graph, G: GraphViewOps<'graph>>(g: &G) -> BTreeSet<(String, String, String)> {
    g.edges()
        .iter()
        .flat_map(|e| {
            let (src, dst) = (e.src().name(), e.dst().name());
            e.layer_names()
                .into_iter()
                .map(move |l| (src.clone(), dst.clone(), l.to_string()))
        })
        .collect()
}

fn update_bases() -> [(&'static str, DynamicGraph); 2] {
    let g = updates_graph();
    [
        ("event", g.clone().into_dynamic()),
        ("persistent", g.persistent_graph().into_dynamic()),
    ]
}

#[test]
fn and_keeps_the_updates_and_layers_both_operands_keep() {
    for (kind, base) in update_bases() {
        let operands = update_operands(&base);
        for (lv, left) in &operands {
            for (rv, right) in &operands {
                let and = AndFilteredGraph::new(
                    base.clone(),
                    Arc::from([
                        left.clone().into_dyn_graph_arc(),
                        right.clone().into_dyn_graph_arc(),
                    ]),
                );
                let label = format!("{kind} {lv} & {rv}");
                assert_eq!(
                    updates(&and),
                    &updates(left) & &updates(right),
                    "{label}: updates"
                );
                assert_eq!(
                    edge_layers(&and),
                    &edge_layers(left) & &edge_layers(right),
                    "{label}: layers"
                );
            }
        }
    }
}

#[test]
fn or_keeps_the_updates_and_layers_either_operand_keeps() {
    for (kind, base) in update_bases() {
        let operands = update_operands(&base);
        for (lv, left) in &operands {
            for (rv, right) in &operands {
                let or = OrFilteredGraph::new(
                    base.clone(),
                    Arc::from([
                        left.clone().into_dyn_graph_arc(),
                        right.clone().into_dyn_graph_arc(),
                    ]),
                );
                let label = format!("{kind} {lv} | {rv}");
                assert_eq!(
                    updates(&or),
                    &updates(left) | &updates(right),
                    "{label}: updates"
                );
                assert_eq!(
                    edge_layers(&or),
                    &edge_layers(left) | &edge_layers(right),
                    "{label}: layers"
                );
            }
        }
    }
}

#[test]
fn not_of_an_update_predicate_keeps_the_updates_it_drops() {
    let base = updates_graph().into_dynamic();
    let not = base
        .filter(ExplodedEdgeFilter.property("w").gt(5i64).not())
        .unwrap();
    let kept = |t: i64| ("a".to_string(), "b".to_string(), t, "x".to_string());
    assert_eq!(updates(&not), BTreeSet::from([kept(1), kept(7)]));
    let layer = ("a".to_string(), "b".to_string(), "x".to_string());
    assert_eq!(edge_layers(&not), BTreeSet::from([layer]));
}

#[test]
fn not_of_an_update_predicate_keeps_every_layer_on_a_persistent_graph() {
    let base = updates_graph().persistent_graph().into_dynamic();
    let not = base
        .filter(ExplodedEdgeFilter.property("w").gt(5i64).not())
        .unwrap();
    let kept = |t: i64| ("a".to_string(), "b".to_string(), t, "x".to_string());
    assert_eq!(updates(&not), BTreeSet::from([kept(1), kept(7)]));
    // The rejected update at 3 reads as a deletion on a persistent graph, so layer y stays.
    let layer = |l: &str| ("a".to_string(), "b".to_string(), l.to_string());
    assert_eq!(edge_layers(&not), BTreeSet::from([layer("x"), layer("y")]));
}
