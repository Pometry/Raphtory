use super::*;
use crate::{
    db::{
        api::{state::ops::Id, view::filter_ops::Select},
        graph::views::filter::{
            model::{
                filter_operator::BinaryOp, node_filter::NodeFilter, PropertyExprFactory,
                ViewWrapOps,
            },
            CreateFilter,
        },
    },
    prelude::{AdditionOps, EntityExprFilterOps, Graph, GraphViewOps, NodeViewOps, NO_PROPS},
};
use raphtory_api::core::{
    entities::{
        properties::prop::{IntoProp, Prop},
        GID,
    },
    Direction,
};

// Test graph: a→b, a→c, b→c
// All nodes have total degree 2; in-degrees: a=0, b=1, c=2
fn build_test_graph() -> Graph {
    let g = Graph::new();
    g.add_edge(0, "a", "b", NO_PROPS, None).unwrap();
    g.add_edge(0, "a", "c", NO_PROPS, None).unwrap();
    g.add_edge(0, "b", "c", NO_PROPS, None).unwrap();
    g
}

fn filtered_names<F>(filter: F, g: Graph) -> Vec<String>
where
    F: CreateFilter,
    for<'graph> F::EntityFiltered<'graph, Graph, F::FilteredGraph<'graph, Graph>>:
        GraphViewOps<'graph>,
{
    let fg = filter.filter_graph_view(g.clone()).unwrap();
    let mut names: Vec<String> = filter
        .create_filter(g, fg)
        .unwrap()
        .nodes()
        .iter()
        .map(|n| n.name())
        .collect();
    names.sort();
    names
}

// ── DegreeExpr comparison operators ──────────────────────────────────────

#[test]
fn degree_ge_2_keeps_all_nodes() {
    let g = build_test_graph();
    assert_eq!(
        filtered_names(
            DegreeExpr {
                dir: Direction::BOTH,
                view_expr: NodeFilter
            }
            .ge(2usize),
            g
        ),
        vec!["a", "b", "c"]
    );
}

#[test]
fn degree_eq_1_keeps_no_nodes() {
    let g = build_test_graph();
    assert!(filtered_names(
        DegreeExpr {
            dir: Direction::BOTH,
            view_expr: NodeFilter
        }
        .eq(1usize),
        g
    )
    .is_empty());
}

#[test]
fn degree_le_2_keeps_all_nodes() {
    let g = build_test_graph();
    assert_eq!(
        filtered_names(
            DegreeExpr {
                dir: Direction::BOTH,
                view_expr: NodeFilter
            }
            .le(2usize),
            g
        ),
        vec!["a", "b", "c"]
    );
}

#[test]
fn degree_gt_2_keeps_no_nodes() {
    let g = build_test_graph();
    assert!(filtered_names(
        DegreeExpr {
            dir: Direction::BOTH,
            view_expr: NodeFilter
        }
        .gt(2usize),
        g
    )
    .is_empty());
}

#[test]
fn degree_ne_2_keeps_no_nodes_when_all_are_2() {
    let g = build_test_graph();
    assert!(filtered_names(
        DegreeExpr {
            dir: Direction::BOTH,
            view_expr: NodeFilter
        }
        .ne(2usize),
        g
    )
    .is_empty());
}

// ── expression-vs-expression: RHS can be another NodeExpr ────────────────

#[test]
fn total_gt_in_degree_selects_nodes_with_outgoing_edges() {
    // total=2, in-degrees: a=0, b=1, c=2 → total > in for a and b only
    let g = build_test_graph();
    assert_eq!(
        filtered_names(
            DegreeExpr {
                dir: Direction::BOTH,
                view_expr: NodeFilter
            }
            .gt(DegreeExpr {
                dir: Direction::IN,
                view_expr: NodeFilter
            }),
            g
        ),
        vec!["a", "b"]
    );
}

// ── ConstExpr for custom output types ────────────────────────────────────

#[test]
fn const_expr_works() {
    let filter = BinaryCmpExpr::new(
        ConstExpr(2usize),
        BinaryOp::Eq,
        ConstExpr(2usize),
        NodeFilter,
    );
    let g = build_test_graph();
    assert_eq!(filtered_names(filter, g), vec!["a", "b", "c"]);
}

#[test]
fn test_id_filter_expr() {
    let g = Graph::new();
    g.add_node(0, 1, NO_PROPS, None, None).unwrap();
    g.add_node(0, 6, NO_PROPS, None, None).unwrap();
    let filter = Id.ge(GID::U64(5u64));

    assert_eq!(g.nodes().select(filter).unwrap().id(), [6u64])
}

// ── Temporal property helpers ─────────────────────────────────────────────

/// Graph with three nodes; "alice" has scores [1, 5, 10] at times 1, 2, 3
///                           "bob"   has scores [2, 3]    at times 1, 2
///                           "carol" has no score property
fn build_temporal_graph() -> Graph {
    let g = Graph::new();
    g.add_node(1, "alice", [("score", 1i64.into_prop())], None, None)
        .unwrap();
    g.add_node(2, "alice", [("score", 5i64.into_prop())], None, None)
        .unwrap();
    g.add_node(3, "alice", [("score", 10i64.into_prop())], None, None)
        .unwrap();
    g.add_node(1, "bob", [("score", 2i64.into_prop())], None, None)
        .unwrap();
    g.add_node(2, "bob", [("score", 3i64.into_prop())], None, None)
        .unwrap();
    g.add_node(1, "carol", NO_PROPS, None, None).unwrap();
    let _ = NodeFilter; // suppress unused warning
    g
}

fn temporal_filtered_names<F>(filter: F, g: Graph) -> Vec<String>
where
    F: CreateFilter,
    for<'graph> F::EntityFiltered<'graph, Graph, F::FilteredGraph<'graph, Graph>>:
        GraphViewOps<'graph>,
{
    let fg = filter.filter_graph_view(g.clone()).unwrap();
    let mut names: Vec<String> = filter
        .create_filter(g, fg)
        .unwrap()
        .nodes()
        .iter()
        .map(|n| n.name())
        .collect();
    names.sort();
    names
}

// ── any() quantifier ─────────────────────────────────────────────────────

#[test]
fn temporal_any_eq_selects_nodes_with_matching_value() {
    // alice has 1, 5, 10; bob has 2, 3; carol has none
    // any == 5 → alice only
    let g = build_temporal_graph();
    let filter = NodeFilter.property("score").temporal().eq(5i64).any();
    assert_eq!(temporal_filtered_names(filter, g), vec!["alice"]);
}

#[test]
fn temporal_any_gt_selects_nodes_with_at_least_one_value_above_threshold() {
    // any > 4 → alice (has 5, 10), not bob (max 3), not carol (none)
    let g = build_temporal_graph();
    let filter = NodeFilter.property("score").temporal().gt(4i64).any();
    assert_eq!(temporal_filtered_names(filter, g), vec!["alice"]);
}

#[test]
fn temporal_any_gt_both_nodes_qualify() {
    // any > 1 → alice (5, 10), bob (2, 3) — both qualify
    let g = build_temporal_graph();
    let filter = NodeFilter.property("score").temporal().gt(1i64).any();
    assert_eq!(temporal_filtered_names(filter, g), vec!["alice", "bob"]);
}

// ── all() quantifier ─────────────────────────────────────────────────────

#[test]
fn temporal_all_gt_requires_every_value() {
    // all > 0 → alice (1,5,10 all > 0 ✓), bob (2,3 all > 0 ✓), carol excluded (empty)
    let g = build_temporal_graph();
    let filter = NodeFilter.property("score").temporal().gt(0i64).all();
    assert_eq!(temporal_filtered_names(filter, g), vec!["alice", "bob"]);
}

#[test]
fn temporal_all_gt_rejects_if_any_value_fails() {
    // all > 4 → alice (1 fails) not included, bob (2, 3 fail) not included
    let g = build_temporal_graph();
    let filter = NodeFilter.property("score").temporal().gt(4i64).all();
    assert!(temporal_filtered_names(filter, g).is_empty());
}

#[test]
fn temporal_all_requires_non_empty_sequence() {
    // carol has no score → "all" over empty sequence returns false
    let g = build_temporal_graph();
    let filter = NodeFilter.property("score").temporal().ge(0i64).all();
    let names = temporal_filtered_names(filter, g);
    assert!(!names.contains(&"carol".to_string()));
}

// ── sum() aggregator ──────────────────────────────────────────────────────

#[test]
fn temporal_sum_gt_threshold() {
    // alice sum = 16, bob sum = 5 → sum > 10 → alice only
    let g = build_temporal_graph();
    let filter = NodeFilter.property("score").temporal().sum().gt(10i64);
    assert_eq!(temporal_filtered_names(filter, g), vec!["alice"]);
}

#[test]
fn temporal_sum_eq() {
    // bob sum = 5 → sum == 5 → bob only
    let g = build_temporal_graph();
    let filter = NodeFilter.property("score").temporal().sum().eq(5i64);
    assert_eq!(temporal_filtered_names(filter, g), vec!["bob"]);
}

// ── first() / last() aggregators ─────────────────────────────────────────

#[test]
fn temporal_first_value() {
    // alice first = 1, bob first = 2 → first == 1 → alice only
    let g = build_temporal_graph();
    let filter = NodeFilter.property("score").temporal().first().eq(1i64);
    assert_eq!(temporal_filtered_names(filter, g), vec!["alice"]);
}

#[test]
fn temporal_last_value() {
    // alice last = 10 → last > 9 → alice only
    let g = build_temporal_graph();
    let filter = NodeFilter.property("score").temporal().last().gt(9i64);
    assert_eq!(temporal_filtered_names(filter, g), vec!["alice"]);
}

// ── len() aggregator ──────────────────────────────────────────────────────

#[test]
fn temporal_len_count() {
    // alice has 3 updates, bob has 2 → len == 3 → alice only
    let g = build_temporal_graph();
    let filter = NodeFilter.property("score").temporal().len().eq(3usize);
    assert_eq!(temporal_filtered_names(filter, g), vec!["alice"]);
}

#[test]
fn temporal_len_ge_2() {
    // alice (3), bob (2) both have len >= 2; carol has 0
    let g = build_temporal_graph();
    let filter = NodeFilter.property("score").temporal().len().ge(2usize);
    assert_eq!(temporal_filtered_names(filter, g), vec!["alice", "bob"]);
}

// ── NodeFilter entry point ────────────────────────────────────────────────

#[test]
fn node_filter_temporal_property_entry_point() {
    let g = build_temporal_graph();
    let filter = NodeFilter.property("score").temporal().eq(5i64).any();
    assert_eq!(temporal_filtered_names(filter, g), vec!["alice"]);
}

// ── TemporalExprOps blanket ───────────────────────────────────────────────

#[test]
fn temporal_expr_ops_blanket_any() {
    // Using EntityAggOps / EntityExprFilterOps on TemporalExpr directly
    let g = build_temporal_graph();
    let filter = NodeFilter.property("score").temporal().eq(10i64).any();
    assert_eq!(temporal_filtered_names(filter, g), vec!["alice"]);
}

// ── Windowed temporal filter ──────────────────────────────────────────────

/// Apply a windowed temporal filter directly (view is embedded in the expression).
fn windowed_filtered_names<F>(filter: F, g: Graph) -> Vec<String>
where
    F: CreateFilter,
    for<'graph> F::EntityFiltered<'graph, Graph, F::FilteredGraph<'graph, Graph>>:
        GraphViewOps<'graph>,
{
    let fg = filter.filter_graph_view(g.clone()).unwrap();
    let mut names: Vec<String> = filter
        .create_filter(g, fg)
        .unwrap()
        .nodes()
        .iter()
        .map(|n| n.name())
        .collect();
    names.sort();
    names
}

#[test]
fn windowed_temporal_any_restricts_to_window() {
    // alice scores: t1=1, t2=5, t3=10
    // window [1, 2) → only t=1 visible → score=1 only
    // any == 5 in window [1,2) → false for all nodes
    let g = build_temporal_graph();
    let filter = NodeFilter
        .window(1, 2)
        .property("score")
        .temporal()
        .eq(5i64)
        .any();
    // window [1,2) shows t=1 only → alice has score=1, not 5
    assert!(windowed_filtered_names(filter, g).is_empty());
}

#[test]
fn windowed_temporal_any_matches_in_window() {
    // window [2, 3) → alice has score=5 (t=2), bob has score=3 (t=2)
    let g = build_temporal_graph();
    let filter = NodeFilter
        .window(2, 3)
        .property("score")
        .temporal()
        .eq(5i64)
        .any();
    assert_eq!(windowed_filtered_names(filter, g), vec!["alice"]);
}

// ── Layered temporal filter ───────────────────────────────────────────────

/// Graph where temporal "score" updates are split across two named layers.
///
/// alice: score [1, 5, 10] at t=1,2,3 — all added in "layer_a"
/// bob:   score [2, 3]     at t=1,2   — all added in "layer_b"
/// carol: no score property            — added in "layer_a" (makes her visible there)
///
/// Because updates added without an explicit layer go into the static layer
/// (and are always visible regardless of the active LayeredGraph), we must use
/// an explicit layer on every `add_node` call that carries a property we want
/// to isolate.
fn build_layered_temporal_graph() -> Graph {
    let g = Graph::new();
    g.add_node(
        1,
        "alice",
        [("score", 1i64.into_prop())],
        None,
        Some("layer_a"),
    )
    .unwrap();
    g.add_node(
        2,
        "alice",
        [("score", 5i64.into_prop())],
        None,
        Some("layer_a"),
    )
    .unwrap();
    g.add_node(
        3,
        "alice",
        [("score", 10i64.into_prop())],
        None,
        Some("layer_a"),
    )
    .unwrap();
    g.add_node(
        1,
        "bob",
        [("score", 2i64.into_prop())],
        None,
        Some("layer_b"),
    )
    .unwrap();
    g.add_node(
        2,
        "bob",
        [("score", 3i64.into_prop())],
        None,
        Some("layer_b"),
    )
    .unwrap();
    g.add_node(1, "carol", NO_PROPS, None, Some("layer_a"))
        .unwrap();
    g
}

/// Apply a layered temporal filter directly (view is embedded in the expression).
fn layered_filtered_names<F>(filter: F, g: Graph) -> Vec<String>
where
    F: CreateFilter,
    for<'graph> F::EntityFiltered<'graph, Graph, F::FilteredGraph<'graph, Graph>>:
        GraphViewOps<'graph>,
{
    let fg = filter.filter_graph_view(g.clone()).unwrap();
    let mut names: Vec<String> = filter
        .create_filter(g, fg)
        .unwrap()
        .nodes()
        .iter()
        .map(|n| n.name())
        .collect();
    names.sort();
    names
}

#[test]
fn layered_temporal_any_restricts_to_layer_a_updates() {
    // layer_a view: alice has scores [1, 5, 10], carol has none, bob has none
    // any == 5 → only alice qualifies
    let g = build_layered_temporal_graph();
    let filter = NodeFilter
        .layer("layer_a")
        .property("score")
        .temporal()
        .eq(5i64)
        .any();
    assert_eq!(layered_filtered_names(filter, g), vec!["alice"]);
}

#[test]
fn layered_temporal_any_restricts_to_layer_b_updates() {
    // layer_b view: bob has scores [2, 3], alice has none, carol has none
    // any > 2 → bob qualifies (score=3 > 2), alice and carol do not
    let g = build_layered_temporal_graph();
    let filter = NodeFilter
        .layer("layer_b")
        .property("score")
        .temporal()
        .gt(2i64)
        .any();
    assert_eq!(layered_filtered_names(filter, g), vec!["bob"]);
}

#[test]
fn layered_temporal_sum_is_layer_scoped() {
    // layer_a: alice sum = 1+5+10 = 16; layer_b: bob sum = 2+3 = 5
    // layer_a sum > 10 → alice (16 > 10); carol (no score) excluded
    let g = build_layered_temporal_graph();
    let filter = NodeFilter
        .layer("layer_a")
        .property("score")
        .temporal()
        .sum()
        .gt(10i64);
    assert_eq!(layered_filtered_names(filter, g), vec!["alice"]);
}

// ── is_true() / is_false() ───────────────────────────────────────────────

/// Graph with bool "active" property:
///   "on"  — active = true
///   "off" — active = false
///   "na"  — no active property
fn build_bool_graph() -> Graph {
    let g = Graph::new();
    g.add_node(0, "on", [("active", true.into_prop())], None, None)
        .unwrap();
    g.add_node(0, "off", [("active", false.into_prop())], None, None)
        .unwrap();
    g.add_node(0, "na", NO_PROPS, None, None).unwrap();
    g
}

#[test]
fn is_true_keeps_only_true_nodes() {
    let g = build_bool_graph();
    let filter = NodeFilter.property("active").is_true();
    assert_eq!(filtered_names(filter, g), vec!["on"]);
}

#[test]
fn is_false_keeps_only_false_nodes() {
    let g = build_bool_graph();
    let filter = NodeFilter.property("active").is_false();
    assert_eq!(filtered_names(filter, g), vec!["off"]);
}

#[test]
fn is_true_excludes_absent_property() {
    // "na" has no "active" property — must not appear
    let g = build_bool_graph();
    let filter = NodeFilter.property("active").is_true();
    let names = filtered_names(filter, g);
    assert!(!names.contains(&"na".to_string()));
}

// ── Runtime validation via prop_type() ───────────────────────────────────

#[test]
fn string_op_on_numeric_prop_returns_error() {
    let g = build_temporal_graph();
    let filter = NodeFilter
        .property("score")
        .starts_with(Prop::Str("x".into()));
    let result = filter.create_filter(g.clone(), g);
    assert!(
        result.is_err(),
        "expected Err for string op on numeric property"
    );
}

#[test]
fn ordering_op_on_bool_prop_returns_error() {
    let g = Graph::new();
    g.add_node(0, "n", [("flag", true.into_prop())], None, None)
        .unwrap();
    // Use Prop::Bool as rhs so both sides share Output = Option<Prop>
    let filter = NodeFilter.property("flag").gt(Prop::Bool(false));
    let result = filter.create_filter(g.clone(), g);
    assert!(
        result.is_err(),
        "expected Err for ordering op on boolean property"
    );
}
