use raphtory::{
    db::{
        api::view::{filter_ops::Select, Filter},
        graph::views::filter::{
            model::{
                node_filter::{NodeFilter, NodeFilterFactory},
                EntityAggOps, PropertyExprFactory, ViewWrapOps,
            },
            CreateFilter,
        },
    },
    prelude::{AdditionOps, EntityExprFilterOps, Graph, GraphViewOps, NodeViewOps, NO_PROPS},
};
use raphtory_api::core::entities::properties::prop::{IntoProp, Prop};

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
    for<'graph> F::FilteredGraph<'graph, Graph>: GraphViewOps<'graph>,
{
    let mut names: Vec<String> = g
        .filter(filter)
        .unwrap()
        .nodes()
        .iter()
        .map(|n| n.name())
        .collect();
    names.sort();
    names
}

// ── expression-vs-expression: RHS can be another NodeExpr ────────────────

#[test]
fn total_gt_in_degree_selects_nodes_with_outgoing_edges() {
    // total=2, in-degrees: a=0, b=1, c=2 → total > in for a and b only
    let g = build_test_graph();
    assert_eq!(
        filtered_names(NodeFilter.degree().gt(NodeFilter.in_degree()), g),
        vec!["a", "b"]
    );
}

#[test]
fn test_id_filter_expr() {
    let g = Graph::new();
    g.add_node(0, 1, NO_PROPS, None, None).unwrap();
    g.add_node(0, 6, NO_PROPS, None, None).unwrap();
    let filter = NodeFilter.id().ge(5u64);

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
    for<'graph> F::FilteredGraph<'graph, Graph>: GraphViewOps<'graph>,
{
    let mut names: Vec<String> = g
        .filter(filter)
        .unwrap()
        .nodes()
        .iter()
        .map(|n| n.name())
        .collect();
    names.sort();
    names
}

// ── any() qualifier ─────────────────────────────────────────────────────

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

// ── all() qualifier ─────────────────────────────────────────────────────

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
    // Using EntityAggOps / EntityExprFilterOps on a temporal term directly
    let g = build_temporal_graph();
    let filter = NodeFilter.property("score").temporal().eq(10i64).any();
    assert_eq!(temporal_filtered_names(filter, g), vec!["alice"]);
}

// ── Windowed temporal filter ──────────────────────────────────────────────

/// Apply a windowed temporal filter directly (view is embedded in the expression).
fn windowed_filtered_names<F>(filter: F, g: Graph) -> Vec<String>
where
    F: CreateFilter,
    for<'graph> F::FilteredGraph<'graph, Graph>: GraphViewOps<'graph>,
{
    let mut names: Vec<String> = g
        .filter(filter)
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
    for<'graph> F::FilteredGraph<'graph, Graph>: GraphViewOps<'graph>,
{
    let mut names: Vec<String> = g
        .filter(filter)
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
        .layers("layer_a")
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
        .layers("layer_b")
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
        .layers("layer_a")
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
    let filter = NodeFilter.property("active").eq(Prop::Bool(true));
    assert_eq!(filtered_names(filter, g), vec!["on"]);
}

#[test]
fn is_false_keeps_only_false_nodes() {
    let g = build_bool_graph();
    let filter = NodeFilter.property("active").eq(Prop::Bool(false));
    assert_eq!(filtered_names(filter, g), vec!["off"]);
}

#[test]
fn is_true_excludes_absent_property() {
    // "na" has no "active" property — must not appear
    let g = build_bool_graph();
    let filter = NodeFilter.property("active").eq(Prop::Bool(true));
    let names = filtered_names(filter, g);
    assert!(!names.contains(&"na".to_string()));
}

// ── Runtime validation via prop_type() ───────────────────────────────────

#[test]
fn ordering_op_on_bool_prop_orders_false_before_true() {
    let g = Graph::new();
    g.add_node(0, "t", [("flag", true.into_prop())], None, None)
        .unwrap();
    g.add_node(0, "f", [("flag", false.into_prop())], None, None)
        .unwrap();
    // Use Prop::Bool as rhs so both sides share Output = Option<Prop>
    assert_eq!(
        filtered_names(NodeFilter.property("flag").gt(Prop::Bool(false)), g.clone()),
        vec!["t"]
    );
    assert_eq!(
        filtered_names(NodeFilter.property("flag").lt(Prop::Bool(true)), g),
        vec!["f"]
    );
}

// ── Sum widens past its element type ─────────────────────────────────────

/// A graph whose node `n` carries `prop` as a list, so a sum over it exceeds
/// the range of the element type.
fn graph_with_list(prop: &str, values: Vec<Prop>) -> Graph {
    let g = Graph::new();
    g.add_node(0, "n", [(prop, Prop::list(values).unwrap())], None, None)
        .unwrap();
    g
}

#[test]
fn sum_of_signed_elements_matches_a_wider_constant() {
    let g = graph_with_list("xs", vec![Prop::I32(i32::MAX), Prop::I32(i32::MAX)]);
    let filter = NodeFilter.property("xs").sum().eq(i64::from(i32::MAX) * 2);
    assert_eq!(filtered_names(filter, g), vec!["n"]);
}

#[test]
fn sum_of_float_elements_compares_in_f64() {
    let g = graph_with_list("xs", vec![Prop::F32(1.5), Prop::F32(2.25)]);
    let filter = NodeFilter.property("xs").sum().eq(3.75f64);
    assert_eq!(filtered_names(filter, g), vec!["n"]);
}

#[test]
fn sum_over_a_nested_list_widens_only_the_innermost_level() {
    // The outer level survives for a pending qualifier, so the comparison is
    // element-wise against the widened inner sums.
    let g = graph_with_list(
        "xs",
        vec![
            Prop::list([u8::MAX, u8::MAX]).unwrap(),
            Prop::list([1u8, 1]).unwrap(),
        ],
    );
    let filter = NodeFilter.property("xs").sum().eq(510u64).any();
    assert_eq!(filtered_names(filter, g), vec!["n"]);
}

// ── Numbers compare by value across widths and signedness ──────────────────

/// A constant that does not fit the property's own type still compares by
/// value: `u64::MAX` is above every `i64`, and a negative `i64` is below every
/// `u64`. Declaring them incomparable made `risk <= 2^63+5` match nothing.
#[test]
fn numeric_constants_beyond_the_property_width_compare_by_value() {
    let g = Graph::new();
    g.add_node(0, "low", [("risk", 3i64.into_prop())], None, None)
        .unwrap();
    g.add_node(0, "neg", [("risk", (-4i64).into_prop())], None, None)
        .unwrap();
    let all = vec!["low".to_string(), "neg".to_string()];
    assert_eq!(
        filtered_names(
            NodeFilter.property("risk").le(Prop::U64(u64::MAX)),
            g.clone()
        ),
        all
    );
    assert_eq!(
        filtered_names(
            NodeFilter.property("risk").le(Prop::U64(1u64 << 63 | 5)),
            g.clone()
        ),
        all
    );
    assert_eq!(
        filtered_names(
            NodeFilter.property("risk").gt(Prop::U64(1u64 << 63)),
            g.clone()
        ),
        Vec::<String>::new()
    );
    assert_eq!(
        filtered_names(NodeFilter.property("risk").lt(Prop::U64(0)), g.clone()),
        vec!["neg"]
    );
    assert_eq!(
        filtered_names(NodeFilter.property("risk").le(Prop::U64(3)), g),
        all
    );
}

// ── Terms through a view on a node the view does not show ─────────────────

/// early: p=1 @1, m=1 · late: p=1 @6, m=1 · no_meta: p=1 @1
///
/// ```text
/// time      0    1    2    3    4    5    6
/// early     ├────●───────────────────┤
/// late      ├────────────────────────┤    ●
/// no_meta   ├────●───────────────────┤
///           └─ window [0, 5) ────────┘
/// ```
fn build_view_membership_graph() -> Graph {
    let g = Graph::new();
    g.add_node(1, "early", [("p", 1i64)], None, None).unwrap();
    g.add_node(6, "late", [("p", 1i64)], None, None).unwrap();
    for name in ["early", "late"] {
        g.node(name).unwrap().add_metadata([("m", 1i64)]).unwrap();
    }
    g.add_node(1, "no_meta", [("p", 1i64)], None, None).unwrap();
    g
}

#[test]
fn windowed_property_term_skips_node_absent_from_window() {
    let g = build_view_membership_graph();
    let filter = NodeFilter.window(0, 5).property("p").eq(1i64);
    assert_eq!(filtered_names(filter, g), vec!["early", "no_meta"]);
}

#[test]
fn windowed_metadata_term_skips_node_absent_from_window() {
    let g = build_view_membership_graph();
    let filter = NodeFilter.window(0, 5).metadata("m").eq(1i64);
    assert_eq!(filtered_names(filter, g), vec!["early"]);
}

#[test]
fn windowed_temporal_term_skips_node_absent_from_window() {
    let g = build_view_membership_graph();
    let filter = NodeFilter
        .window(0, 5)
        .property("p")
        .temporal()
        .len()
        .eq(Prop::U64(0));
    assert_eq!(filtered_names(filter, g), vec!["late"]);
}
