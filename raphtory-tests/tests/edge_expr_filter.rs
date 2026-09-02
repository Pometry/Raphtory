use raphtory::{
    db::{
        api::view::Filter,
        graph::views::filter::model::{ComposableFilter, EdgeFilter, PropertyExprFactory},
    },
    prelude::*,
};

fn sorted_edges(g: impl GraphViewOps<'static>) -> Vec<String> {
    let mut edges: Vec<String> = g
        .edges()
        .iter()
        .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
        .collect();
    edges.sort();
    edges
}

#[test]
fn test_edge_temporal_len_gt() {
    let g = Graph::new();
    // A->B gets 3 temporal updates for "score"
    g.add_edge(1, "A", "B", [("score", Prop::I64(10))], None)
        .unwrap();
    g.add_edge(2, "A", "B", [("score", Prop::I64(20))], None)
        .unwrap();
    g.add_edge(3, "A", "B", [("score", Prop::I64(30))], None)
        .unwrap();
    // C->D gets 1 temporal update for "score"
    g.add_edge(1, "C", "D", [("score", Prop::I64(10))], None)
        .unwrap();
    // E->F gets no "score" update (zero temporal values)
    g.add_edge(1, "E", "F", [("other", Prop::I64(1))], None)
        .unwrap();

    let filter = EdgeFilter.property("score").temporal().len().gt(1usize);
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["A->B"]);
}

#[test]
fn test_edge_temporal_len_eq() {
    let g = Graph::new();
    g.add_edge(1, "A", "B", [("score", Prop::I64(10))], None)
        .unwrap();
    g.add_edge(2, "A", "B", [("score", Prop::I64(20))], None)
        .unwrap();
    g.add_edge(3, "A", "B", [("score", Prop::I64(30))], None)
        .unwrap();
    g.add_edge(1, "C", "D", [("score", Prop::I64(10))], None)
        .unwrap();

    // exactly 1 temporal update
    let filter = EdgeFilter.property("score").temporal().len().eq(1usize);
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["C->D"]);
}

#[test]
fn test_edge_temporal_len_lt() {
    let g = Graph::new();
    g.add_edge(1, "A", "B", [("score", Prop::I64(1))], None)
        .unwrap();
    g.add_edge(2, "A", "B", [("score", Prop::I64(2))], None)
        .unwrap();
    g.add_edge(3, "A", "B", [("score", Prop::I64(3))], None)
        .unwrap();
    g.add_edge(1, "C", "D", [("score", Prop::I64(1))], None)
        .unwrap();
    g.add_edge(2, "C", "D", [("score", Prop::I64(2))], None)
        .unwrap();

    // fewer than 3 updates
    let filter = EdgeFilter.property("score").temporal().len().lt(3usize);
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["C->D"]);
}

#[test]
fn test_edge_temporal_len_le() {
    let g = Graph::new();
    g.add_edge(1, "A", "B", [("score", Prop::I64(1))], None)
        .unwrap();
    g.add_edge(2, "A", "B", [("score", Prop::I64(2))], None)
        .unwrap();
    g.add_edge(3, "A", "B", [("score", Prop::I64(3))], None)
        .unwrap();
    g.add_edge(1, "C", "D", [("score", Prop::I64(1))], None)
        .unwrap();
    g.add_edge(2, "C", "D", [("score", Prop::I64(2))], None)
        .unwrap();

    // at most 2 updates
    let filter = EdgeFilter.property("score").temporal().len().le(2usize);
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["C->D"]);
}

#[test]
fn test_edge_temporal_len_ge() {
    let g = Graph::new();
    g.add_edge(1, "A", "B", [("score", Prop::I64(1))], None)
        .unwrap();
    g.add_edge(2, "A", "B", [("score", Prop::I64(2))], None)
        .unwrap();
    g.add_edge(3, "A", "B", [("score", Prop::I64(3))], None)
        .unwrap();
    g.add_edge(1, "C", "D", [("score", Prop::I64(1))], None)
        .unwrap();

    // at least 2 updates
    let filter = EdgeFilter.property("score").temporal().len().ge(2usize);
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["A->B"]);
}

#[test]
fn test_edge_temporal_len_ne() {
    let g = Graph::new();
    g.add_edge(1, "A", "B", [("score", Prop::I64(1))], None)
        .unwrap();
    g.add_edge(2, "A", "B", [("score", Prop::I64(2))], None)
        .unwrap();
    g.add_edge(3, "A", "B", [("score", Prop::I64(3))], None)
        .unwrap();
    g.add_edge(1, "C", "D", [("score", Prop::I64(1))], None)
        .unwrap();

    // not exactly 1 update
    let filter = EdgeFilter.property("score").temporal().len().ne(1usize);
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["A->B"]);
}

#[test]
fn test_edge_temporal_len_combined_with_and() {
    let g = Graph::new();
    g.add_edge(1, "A", "B", [("score", Prop::I64(1))], None)
        .unwrap();
    g.add_edge(2, "A", "B", [("score", Prop::I64(2))], None)
        .unwrap();
    g.add_edge(1, "C", "D", [("score", Prop::I64(1))], None)
        .unwrap();
    g.add_edge(2, "C", "D", [("score", Prop::I64(2))], None)
        .unwrap();
    g.add_edge(3, "C", "D", [("score", Prop::I64(3))], None)
        .unwrap();

    // both have >= 2 updates; only A->B has exactly 2
    let filter = EdgeFilter
        .property("score")
        .temporal()
        .len()
        .ge(2usize)
        .and(EdgeFilter.property("score").temporal().len().le(2usize));
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["A->B"]);
}

// ─────────────────────────────────────────────────────────────────────────────
// String ops via EdgeExprFilterOps (generic) and EdgeAggregated convenience
// ─────────────────────────────────────────────────────────────────────────────

fn band_graph() -> Graph {
    let g = Graph::new();
    g.add_edge(1, "Jimi", "John", [("band", Prop::str("Pink Floyd"))], None)
        .unwrap();
    g.add_edge(
        1,
        "John",
        "David",
        [("band", Prop::str("Led Zeppelin"))],
        None,
    )
    .unwrap();
    g.add_edge(
        1,
        "David",
        "Robert",
        [("band", Prop::str("Deep Purple"))],
        None,
    )
    .unwrap();
    g
}

#[test]
fn test_edge_property_contains_via_expr_filter_ops() {
    let g = band_graph();
    // generic form: PropertyExpr.contains(Prop::Str(...))
    let filter = EdgeFilter.property("band").contains(Prop::str("Floyd"));
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["Jimi->John"]);
}

#[test]
fn test_edge_property_not_contains_via_expr_filter_ops() {
    let g = band_graph();
    let filter = EdgeFilter.property("band").not_contains(Prop::str("Floyd"));
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["David->Robert", "John->David"]);
}

#[test]
fn test_edge_property_starts_with_via_expr_filter_ops() {
    let g = band_graph();
    let filter = EdgeFilter.property("band").starts_with(Prop::str("Pink"));
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["Jimi->John"]);
}

#[test]
fn test_edge_property_ends_with_via_expr_filter_ops() {
    let g = band_graph();
    let filter = EdgeFilter.property("band").ends_with(Prop::str("Zeppelin"));
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["John->David"]);
}

#[test]
fn test_edge_property_fuzzy_search_via_expr_filter_ops() {
    let g = Graph::new();
    // Use short values so whole-string Levenshtein is meaningful:
    // "Floyd" vs "Floid" = 1 substitution (y→i)
    // "Zeppelin" is much farther away
    g.add_edge(1, "A", "B", [("tag", Prop::str("Floyd"))], None)
        .unwrap();
    g.add_edge(1, "C", "D", [("tag", Prop::str("Zeppelin"))], None)
        .unwrap();

    let filter = EdgeFilter
        .property("tag")
        .fuzzy_search(Prop::str("Floid"), 1, false);
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["A->B"]);
}

#[test]
fn test_edge_aggregated_last_contains_str_convenience() {
    let g = Graph::new();
    g.add_edge(1, "A", "B", [("tag", Prop::str("rock"))], None)
        .unwrap();
    g.add_edge(2, "A", "B", [("tag", Prop::str("metal"))], None)
        .unwrap();
    g.add_edge(1, "C", "D", [("tag", Prop::str("jazz"))], None)
        .unwrap();

    // last temporal value of "tag": A->B = "metal", C->D = "jazz"
    let filter = EdgeFilter
        .property("tag")
        .temporal()
        .last()
        .contains("etal");
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["A->B"]);
}

#[test]
fn test_edge_aggregated_first_starts_with_str_convenience() {
    let g = Graph::new();
    g.add_edge(1, "A", "B", [("tag", Prop::str("rock"))], None)
        .unwrap();
    g.add_edge(2, "A", "B", [("tag", Prop::str("metal"))], None)
        .unwrap();
    g.add_edge(1, "C", "D", [("tag", Prop::str("jazz"))], None)
        .unwrap();

    // first temporal value: A->B = "rock", C->D = "jazz"
    let filter = EdgeFilter
        .property("tag")
        .temporal()
        .first()
        .starts_with("ro");
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["A->B"]);
}

// ─────────────────────────────────────────────────────────────────────────────
// Set ops — PropValueSetExpr (linear scan, Option<Prop>) and
//           SetEdgeFilter (HashSet, Option<I: Hash>)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_edge_property_is_in_prop_values() {
    // Path A: EdgePropertyExprOps::is_in — PropValueSetExpr
    let g = band_graph();
    let filter = EdgeFilter
        .property("band")
        .is_in([Prop::str("Pink Floyd"), Prop::str("Deep Purple")]);
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["David->Robert", "Jimi->John"]);
}

#[test]
fn test_edge_property_is_not_in_prop_values() {
    let g = band_graph();
    let filter = EdgeFilter
        .property("band")
        .is_not_in([Prop::str("Pink Floyd"), Prop::str("Deep Purple")]);
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["John->David"]);
}

#[test]
fn test_edge_aggregated_last_is_in_prop_values() {
    // Path A via EdgeAggregated convenience
    let g = Graph::new();
    g.add_edge(1, "A", "B", [("tag", Prop::str("rock"))], None)
        .unwrap();
    g.add_edge(2, "A", "B", [("tag", Prop::str("metal"))], None)
        .unwrap();
    g.add_edge(1, "C", "D", [("tag", Prop::str("jazz"))], None)
        .unwrap();

    // last value: A->B = "metal", C->D = "jazz"
    let filter = EdgeFilter
        .property("tag")
        .temporal()
        .last()
        .is_in([Prop::str("metal"), Prop::str("blues")]);
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["A->B"]);
}

#[test]
fn test_edge_aggregated_last_is_not_in_prop_values() {
    let g = Graph::new();
    g.add_edge(1, "A", "B", [("tag", Prop::str("rock"))], None)
        .unwrap();
    g.add_edge(2, "A", "B", [("tag", Prop::str("metal"))], None)
        .unwrap();
    g.add_edge(1, "C", "D", [("tag", Prop::str("jazz"))], None)
        .unwrap();

    let filter = EdgeFilter
        .property("tag")
        .temporal()
        .last()
        .is_not_in([Prop::str("metal"), Prop::str("blues")]);
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["C->D"]);
}

// ─────────────────────────────────────────────────────────────────────────────
// Step 4: EdgeQuantified string ops (any/all + contains/starts_with/ends_with)
// ─────────────────────────────────────────────────────────────────────────────

fn genre_graph() -> Graph {
    let g = Graph::new();
    // A->B has tags: "rock", "metal", "rock-n-roll" (3 updates)
    g.add_edge(1, "A", "B", [("tag", Prop::str("rock"))], None)
        .unwrap();
    g.add_edge(2, "A", "B", [("tag", Prop::str("metal"))], None)
        .unwrap();
    g.add_edge(3, "A", "B", [("tag", Prop::str("rock-n-roll"))], None)
        .unwrap();
    // C->D has tags: "jazz", "blues" (2 updates)
    g.add_edge(1, "C", "D", [("tag", Prop::str("jazz"))], None)
        .unwrap();
    g.add_edge(2, "C", "D", [("tag", Prop::str("blues"))], None)
        .unwrap();
    g
}

#[test]
fn test_edge_quantified_any_contains() {
    let g = genre_graph();
    // any temporal value of "tag" contains "rock"
    let filter = EdgeFilter.property("tag").temporal().contains("rock").any();
    let result = g.filter(filter).unwrap();
    // A->B has "rock" and "rock-n-roll" (contains "rock"), C->D has neither
    assert_eq!(sorted_edges(result), vec!["A->B"]);
}

#[test]
fn test_edge_quantified_any_starts_with() {
    let g = genre_graph();
    let filter = EdgeFilter
        .property("tag")
        .temporal()
        .starts_with("rock")
        .any();
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["A->B"]);
}

#[test]
fn test_edge_quantified_any_ends_with() {
    let g = genre_graph();
    let filter = EdgeFilter
        .property("tag")
        .temporal()
        .ends_with("roll")
        .any();
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["A->B"]);
}

#[test]
fn test_edge_quantified_any_not_contains() {
    let g = genre_graph();
    // any temporal value of "tag" does NOT contain "rock"
    // A->B: "metal" and "rock-n-roll" don't, but "rock" does → any not_contains is true for A->B
    // C->D: "jazz" and "blues" don't contain "rock" → any not_contains is true for C->D
    // Both edges pass (any value doesn't contain "rock")
    let filter = EdgeFilter
        .property("tag")
        .temporal()
        .not_contains("rock")
        .any();
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["A->B", "C->D"]);
}

#[test]
fn test_edge_quantified_all_contains() {
    let g = Graph::new();
    // A->B: all tags contain "rock"
    g.add_edge(1, "A", "B", [("tag", Prop::str("rock"))], None)
        .unwrap();
    g.add_edge(2, "A", "B", [("tag", Prop::str("rock-n-roll"))], None)
        .unwrap();
    // C->D: not all tags contain "rock"
    g.add_edge(1, "C", "D", [("tag", Prop::str("rock"))], None)
        .unwrap();
    g.add_edge(2, "C", "D", [("tag", Prop::str("jazz"))], None)
        .unwrap();

    let filter = EdgeFilter.property("tag").temporal().contains("rock").all();
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["A->B"]);
}

#[test]
fn test_edge_quantified_all_starts_with() {
    let g = Graph::new();
    g.add_edge(1, "A", "B", [("tag", Prop::str("rock"))], None)
        .unwrap();
    g.add_edge(2, "A", "B", [("tag", Prop::str("rock-n-roll"))], None)
        .unwrap();
    g.add_edge(1, "C", "D", [("tag", Prop::str("jazz"))], None)
        .unwrap();
    g.add_edge(2, "C", "D", [("tag", Prop::str("rock-steady"))], None)
        .unwrap();

    let filter = EdgeFilter
        .property("tag")
        .temporal()
        .starts_with("rock")
        .all();
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["A->B"]);
}

// ─────────────────────────────────────────────────────────────────────────────
// Step 4: EdgeQuantified set ops (any/all + is_in/is_not_in)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_edge_quantified_any_is_in() {
    let g = genre_graph();
    // any temporal value of "tag" is in {"metal", "blues"}
    let filter = EdgeFilter
        .property("tag")
        .temporal()
        .is_in([Prop::str("metal"), Prop::str("blues")])
        .any();
    let result = g.filter(filter).unwrap();
    // A->B has "metal" → passes; C->D has "blues" → passes
    assert_eq!(sorted_edges(result), vec!["A->B", "C->D"]);
}

#[test]
fn test_edge_quantified_any_is_not_in() {
    let g = genre_graph();
    // any temporal value of "tag" is NOT in {"metal", "blues"}
    let filter = EdgeFilter
        .property("tag")
        .temporal()
        .any()
        .is_not_in([Prop::str("metal"), Prop::str("blues")]);
    let result = g.filter(filter).unwrap();
    // A->B has "rock" and "rock-n-roll" not in set → passes
    // C->D has "jazz" not in set → passes
    assert_eq!(sorted_edges(result), vec!["A->B", "C->D"]);
}

#[test]
fn test_edge_quantified_all_is_in() {
    let g = Graph::new();
    g.add_edge(1, "A", "B", [("tag", Prop::str("rock"))], None)
        .unwrap();
    g.add_edge(2, "A", "B", [("tag", Prop::str("metal"))], None)
        .unwrap();
    g.add_edge(1, "C", "D", [("tag", Prop::str("jazz"))], None)
        .unwrap();
    g.add_edge(2, "C", "D", [("tag", Prop::str("metal"))], None)
        .unwrap();

    // all temporal values in {"rock", "metal"}
    let filter = EdgeFilter
        .property("tag")
        .temporal()
        .is_in([Prop::str("rock"), Prop::str("metal")])
        .all();
    let result = g.filter(filter).unwrap();
    // A->B: "rock" ✓, "metal" ✓ → passes; C->D: "jazz" ✗ → fails
    assert_eq!(sorted_edges(result), vec!["A->B"]);
}

#[test]
fn test_edge_quantified_all_is_not_in() {
    let g = Graph::new();
    g.add_edge(1, "A", "B", [("tag", Prop::str("rock"))], None)
        .unwrap();
    g.add_edge(2, "A", "B", [("tag", Prop::str("metal"))], None)
        .unwrap();
    g.add_edge(1, "C", "D", [("tag", Prop::str("jazz"))], None)
        .unwrap();
    g.add_edge(2, "C", "D", [("tag", Prop::str("blues"))], None)
        .unwrap();

    // all temporal values are NOT in {"rock", "metal"}
    let filter = EdgeFilter
        .property("tag")
        .temporal()
        .is_not_in([Prop::str("rock"), Prop::str("metal")])
        .all();
    let result = g.filter(filter).unwrap();
    // A->B: "rock" is in set → fails; C->D: "jazz" ✓, "blues" ✓ → passes
    assert_eq!(sorted_edges(result), vec!["C->D"]);
}

// ─────────────────────────────────────────────────────────────────────────────
// Step 5: Re-aggregation chains on EdgeAggregated
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_edge_aggregated_last_then_sum() {
    // Property is list-valued at each timestamp.
    // .last() picks the last snapshot's list, .sum() reduces it to a scalar.
    let g = Graph::new();
    // A->B: last snapshot = [3,4,5], sum = 12 (> 10)
    g.add_edge(
        1,
        "A",
        "B",
        [("score", Prop::List(vec![Prop::I64(1), Prop::I64(2)].into()))],
        None,
    )
    .unwrap();
    g.add_edge(
        2,
        "A",
        "B",
        [(
            "score",
            Prop::List(vec![Prop::I64(3), Prop::I64(4), Prop::I64(5)].into()),
        )],
        None,
    )
    .unwrap();
    // C->D: last (and only) snapshot = [1,2,3], sum = 6 (not > 10)
    g.add_edge(
        1,
        "C",
        "D",
        [(
            "score",
            Prop::List(vec![Prop::I64(1), Prop::I64(2), Prop::I64(3)].into()),
        )],
        None,
    )
    .unwrap();

    let filter = EdgeFilter
        .property("score")
        .temporal()
        .last()
        .sum()
        .gt(10i64);
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["A->B"]);
}

#[test]
fn test_edge_aggregated_last_then_contains() {
    let g = Graph::new();
    g.add_edge(1, "A", "B", [("tag", Prop::str("rock"))], None)
        .unwrap();
    g.add_edge(2, "A", "B", [("tag", Prop::str("metal"))], None)
        .unwrap();
    g.add_edge(1, "C", "D", [("tag", Prop::str("jazz"))], None)
        .unwrap();

    // last value: A->B = "metal", C->D = "jazz"
    // re-chain: last().contains("metal") → A->B passes
    let filter = EdgeFilter
        .property("tag")
        .temporal()
        .last()
        .contains("metal");
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["A->B"]);
}

#[test]
fn test_edge_aggregated_first_then_ends_with() {
    let g = Graph::new();
    g.add_edge(1, "A", "B", [("tag", Prop::str("rock-n-roll"))], None)
        .unwrap();
    g.add_edge(2, "A", "B", [("tag", Prop::str("jazz"))], None)
        .unwrap();
    g.add_edge(1, "C", "D", [("tag", Prop::str("blues"))], None)
        .unwrap();

    // first value: A->B = "rock-n-roll", C->D = "blues"
    let filter = EdgeFilter
        .property("tag")
        .temporal()
        .first()
        .ends_with("roll");
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["A->B"]);
}

#[test]
fn test_edge_aggregated_last_then_len() {
    // Property is list-valued at each timestamp.
    // .last() picks the last snapshot's list, .len() returns its length.
    let g = Graph::new();
    // A->B: last snapshot = [20, 30], len = 2
    g.add_edge(
        1,
        "A",
        "B",
        [("score", Prop::List(vec![Prop::I64(10)].into()))],
        None,
    )
    .unwrap();
    g.add_edge(
        2,
        "A",
        "B",
        [(
            "score",
            Prop::List(vec![Prop::I64(20), Prop::I64(30)].into()),
        )],
        None,
    )
    .unwrap();
    // C->D: last snapshot = [5, 10, 15], len = 3
    g.add_edge(
        1,
        "C",
        "D",
        [(
            "score",
            Prop::List(vec![Prop::I64(5), Prop::I64(10), Prop::I64(15)].into()),
        )],
        None,
    )
    .unwrap();

    let filter = EdgeFilter
        .property("score")
        .temporal()
        .last()
        .len()
        .eq(2usize);
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["A->B"]);
}

#[test]
fn test_edge_aggregated_last_then_any_is_in() {
    // Property is list-valued at each timestamp.
    // .last() picks the last snapshot's list, .is_in([...]).any() checks if any element is in the set.
    let g = Graph::new();
    // A->B: last snapshot = ["folk","metal"] — "metal" ∈ {"metal","blues"}
    g.add_edge(
        1,
        "A",
        "B",
        [(
            "tag",
            Prop::List(vec![Prop::str("rock"), Prop::str("pop")].into()),
        )],
        None,
    )
    .unwrap();
    g.add_edge(
        2,
        "A",
        "B",
        [(
            "tag",
            Prop::List(vec![Prop::str("folk"), Prop::str("metal")].into()),
        )],
        None,
    )
    .unwrap();
    // C->D: last (and only) snapshot = ["jazz","pop"] — neither in {"metal","blues"}
    g.add_edge(
        1,
        "C",
        "D",
        [(
            "tag",
            Prop::List(vec![Prop::str("jazz"), Prop::str("pop")].into()),
        )],
        None,
    )
    .unwrap();

    let filter = EdgeFilter
        .property("tag")
        .temporal()
        .last()
        .is_in([Prop::str("metal"), Prop::str("blues")])
        .any();
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["A->B"]);
}

#[test]
fn test_edge_aggregated_last_then_all_contains() {
    // Property is list-valued at each timestamp.
    // .last() picks the last snapshot's list, .contains("rock").all() checks all elements contain "rock".
    let g = Graph::new();
    // A->B: last snapshot = ["rock","rock-n-roll"] — all contain "rock"
    g.add_edge(
        1,
        "A",
        "B",
        [("tag", Prop::List(vec![Prop::str("jazz")].into()))],
        None,
    )
    .unwrap();
    g.add_edge(
        2,
        "A",
        "B",
        [(
            "tag",
            Prop::List(vec![Prop::str("rock"), Prop::str("rock-n-roll")].into()),
        )],
        None,
    )
    .unwrap();
    // C->D: last (and only) snapshot = ["rock","jazz"] — "jazz" doesn't contain "rock"
    g.add_edge(
        1,
        "C",
        "D",
        [(
            "tag",
            Prop::List(vec![Prop::str("rock"), Prop::str("jazz")].into()),
        )],
        None,
    )
    .unwrap();

    let filter = EdgeFilter
        .property("tag")
        .temporal()
        .last()
        .contains("rock")
        .all();
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["A->B"]);
}

#[test]
fn test_edge_aggregated_last_then_is_in() {
    let g = Graph::new();
    g.add_edge(1, "A", "B", [("tag", Prop::str("rock"))], None)
        .unwrap();
    g.add_edge(2, "A", "B", [("tag", Prop::str("metal"))], None)
        .unwrap();
    g.add_edge(1, "C", "D", [("tag", Prop::str("jazz"))], None)
        .unwrap();

    let filter = EdgeFilter
        .property("tag")
        .temporal()
        .last()
        .is_in([Prop::str("metal"), Prop::str("blues")]);
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["A->B"]);
}

// ─────────────────────────────────────────────────────────────────────────────
// Gap 1: EdgePropertyExprOps &str convenience methods (no Prop:: wrapper)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_edge_property_contains_str_literal() {
    let g = band_graph();
    let filter = EdgeFilter.property("band").contains("Floyd");
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["Jimi->John"]);
}

#[test]
fn test_edge_property_starts_with_str_literal() {
    let g = band_graph();
    let filter = EdgeFilter.property("band").starts_with("Pink");
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["Jimi->John"]);
}

#[test]
fn test_edge_property_ends_with_str_literal() {
    let g = band_graph();
    let filter = EdgeFilter.property("band").ends_with("Purple");
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["David->Robert"]);
}

#[test]
fn test_edge_property_not_contains_str_literal() {
    let g = band_graph();
    let filter = EdgeFilter.property("band").not_contains("Floyd");
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["David->Robert", "John->David"]);
}

// ─────────────────────────────────────────────────────────────────────────────
// Gap 2: is_true / is_false on EdgePropertyExprOps
// ─────────────────────────────────────────────────────────────────────────────

fn active_graph() -> Graph {
    let g = Graph::new();
    g.add_edge(1, "A", "B", [("active", Prop::Bool(true))], None)
        .unwrap();
    g.add_edge(1, "C", "D", [("active", Prop::Bool(false))], None)
        .unwrap();
    g.add_edge(1, "E", "F", [("active", Prop::Bool(true))], None)
        .unwrap();
    g
}

#[test]
fn test_edge_property_is_true() {
    let g = active_graph();
    let filter = EdgeFilter.property("active").is_true();
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["A->B", "E->F"]);
}

#[test]
fn test_edge_property_is_false() {
    let g = active_graph();
    let filter = EdgeFilter.property("active").is_false();
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["C->D"]);
}

// ─────────────────────────────────────────────────────────────────────────────
// Gap 3: EdgeQuantified re-aggregation chains (.any().sum(), .all().min(), etc.)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_edge_quantified_any_sum_gt() {
    let g = Graph::new();
    // A->B: t=1 has a list whose sum = 11 (> 5); t=2 has a list whose sum = 3
    g.add_edge(
        1,
        "A",
        "B",
        [("score", Prop::List(vec![Prop::I64(3), Prop::I64(8)].into()))],
        None,
    )
    .unwrap();
    g.add_edge(
        2,
        "A",
        "B",
        [("score", Prop::List(vec![Prop::I64(1), Prop::I64(2)].into()))],
        None,
    )
    .unwrap();
    // C->D: t=1 sum = 3, t=2 sum = 5 (neither > 5)
    g.add_edge(
        1,
        "C",
        "D",
        [("score", Prop::List(vec![Prop::I64(1), Prop::I64(2)].into()))],
        None,
    )
    .unwrap();
    g.add_edge(
        2,
        "C",
        "D",
        [("score", Prop::List(vec![Prop::I64(1), Prop::I64(4)].into()))],
        None,
    )
    .unwrap();

    // any temporal snapshot where sum of list > 5
    let filter = EdgeFilter.property("score").temporal().sum().gt(5i64).any();
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["A->B"]);
}

#[test]
fn test_edge_quantified_all_min_ge() {
    let g = Graph::new();
    // A->B: t=1 min = 3, t=2 min = 5 (all ≥ 3)
    g.add_edge(
        1,
        "A",
        "B",
        [("score", Prop::List(vec![Prop::I64(3), Prop::I64(8)].into()))],
        None,
    )
    .unwrap();
    g.add_edge(
        2,
        "A",
        "B",
        [("score", Prop::List(vec![Prop::I64(5), Prop::I64(9)].into()))],
        None,
    )
    .unwrap();
    // C->D: t=1 min = 1 (< 3), so not all snapshots pass
    g.add_edge(
        1,
        "C",
        "D",
        [("score", Prop::List(vec![Prop::I64(1), Prop::I64(9)].into()))],
        None,
    )
    .unwrap();
    g.add_edge(
        2,
        "C",
        "D",
        [("score", Prop::List(vec![Prop::I64(3), Prop::I64(5)].into()))],
        None,
    )
    .unwrap();

    // all temporal snapshots where min of list >= 3
    let filter = EdgeFilter.property("score").temporal().min().ge(3i64).all();
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["A->B"]);
}

#[test]
fn test_edge_quantified_any_any_contains() {
    let g = Graph::new();
    // A->B: t=1 list has "rock" (any element contains "rock")
    g.add_edge(
        1,
        "A",
        "B",
        [(
            "tag",
            Prop::List(vec![Prop::str("rock"), Prop::str("metal")].into()),
        )],
        None,
    )
    .unwrap();
    g.add_edge(
        2,
        "A",
        "B",
        [(
            "tag",
            Prop::List(vec![Prop::str("jazz"), Prop::str("blues")].into()),
        )],
        None,
    )
    .unwrap();
    // C->D: no list has any element containing "rock"
    g.add_edge(
        1,
        "C",
        "D",
        [(
            "tag",
            Prop::List(vec![Prop::str("jazz"), Prop::str("blues")].into()),
        )],
        None,
    )
    .unwrap();
    g.add_edge(
        2,
        "C",
        "D",
        [(
            "tag",
            Prop::List(vec![Prop::str("folk"), Prop::str("pop")].into()),
        )],
        None,
    )
    .unwrap();

    // any temporal snapshot where any list element contains "rock"
    let filter = EdgeFilter
        .property("tag")
        .temporal()
        .contains("rock")
        .any()
        .any();
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["A->B"]);
}

#[test]
fn test_edge_quantified_any_last_is_in() {
    let g = Graph::new();
    // A->B: last snapshot (t=2) has "metal" as an element
    g.add_edge(
        1,
        "A",
        "B",
        [(
            "tag",
            Prop::List(vec![Prop::str("rock"), Prop::str("folk")].into()),
        )],
        None,
    )
    .unwrap();
    g.add_edge(
        2,
        "A",
        "B",
        [(
            "tag",
            Prop::List(vec![Prop::str("pop"), Prop::str("metal")].into()),
        )],
        None,
    )
    .unwrap();
    // C->D: last (and only) snapshot has no element in {"metal"}
    g.add_edge(
        1,
        "C",
        "D",
        [(
            "tag",
            Prop::List(vec![Prop::str("jazz"), Prop::str("blues")].into()),
        )],
        None,
    )
    .unwrap();

    // last temporal snapshot's list — any element is in {"metal"}
    let filter = EdgeFilter
        .property("tag")
        .temporal()
        .last()
        .is_in([Prop::str("metal")])
        .any();
    let result = g.filter(filter).unwrap();
    assert_eq!(sorted_edges(result), vec!["A->B"]);
}
