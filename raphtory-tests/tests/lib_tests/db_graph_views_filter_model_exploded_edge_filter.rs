#[cfg(test)]
mod filter_tree_export_tests {
    use raphtory::db::graph::views::filter::model::{
        exploded_edge_filter::*, node_filter::NodeFilter, property_filter::ops::PropertyFilterOps,
        ComposableFilter, EdgeViewFilterOps, FilterTree, PropertyFilterFactory,
        TryAsCompositeFilter, ViewWrapOps,
    };

    // An exploded-edge property filter exports as the exploded-edge kind — the
    // transportable form the remote client ships.
    #[test]
    fn exploded_property_exports_as_exploded_edge_tree() {
        let f = ExplodedEdgeFilter.property("w").gt(1i64);
        let tree = f.try_as_filter_tree().unwrap();
        assert!(
            matches!(
                tree,
                FilterTree::ExplodedEdge(CompositeExplodedEdgeFilter::Property(_))
            ),
            "expected ExplodedEdge(Property), got {tree:?}"
        );
    }

    #[test]
    fn exploded_metadata_exports_as_exploded_edge_tree() {
        let f = ExplodedEdgeFilter.metadata("kind").eq("strong");
        let tree = f.try_as_filter_tree().unwrap();
        assert!(
            matches!(
                tree,
                FilterTree::ExplodedEdge(CompositeExplodedEdgeFilter::Property(_))
            ),
            "expected ExplodedEdge(Property), got {tree:?}"
        );
    }

    // A combinator of two exploded filters keeps its composite form — no
    // structural And wrapper.
    #[test]
    fn same_kind_combinators_export_as_a_composite() {
        let a = ExplodedEdgeFilter.property("w").gt(1i64);
        let b = ExplodedEdgeFilter.property("w").lt(9i64);
        let tree = a.clone().and(b.clone()).try_as_filter_tree().unwrap();
        assert!(
            matches!(
                tree,
                FilterTree::ExplodedEdge(CompositeExplodedEdgeFilter::And(_, _))
            ),
            "expected ExplodedEdge(And), got {tree:?}"
        );

        let tree = a.clone().or(b).try_as_filter_tree().unwrap();
        assert!(
            matches!(
                tree,
                FilterTree::ExplodedEdge(CompositeExplodedEdgeFilter::Or(_, _))
            ),
            "expected ExplodedEdge(Or), got {tree:?}"
        );

        let tree = a.not().try_as_filter_tree().unwrap();
        assert!(
            matches!(
                tree,
                FilterTree::ExplodedEdge(CompositeExplodedEdgeFilter::Not(_))
            ),
            "expected ExplodedEdge(Not), got {tree:?}"
        );
    }

    // A view wrapper over an exploded property keeps the composite form as
    // well (the wrapper becomes a Windowed composite variant).
    #[test]
    fn windowed_exploded_property_exports_as_a_composite() {
        let f = ExplodedEdgeFilter.window(2i64, 4i64).property("w").gt(1i64);
        let tree = f.try_as_filter_tree().unwrap();
        assert!(
            matches!(
                tree,
                FilterTree::ExplodedEdge(CompositeExplodedEdgeFilter::Windowed(_))
            ),
            "expected ExplodedEdge(Windowed), got {tree:?}"
        );
    }

    // The exploded predicates also export as plain edge filters; the
    // node → edge → exploded order must keep that export unchanged.
    #[test]
    fn exploded_predicates_still_export_as_plain_edge_filters() {
        let tree = ExplodedEdgeFilter.is_valid().try_as_filter_tree().unwrap();
        assert!(
            matches!(tree, FilterTree::Edge(_)),
            "expected Edge for is_valid, got {tree:?}"
        );
    }

    // A mixed node∧exploded combination exports structurally, with the
    // exploded leg tagged as its own kind.
    #[test]
    fn mixed_node_and_exploded_exports_structurally() {
        let n = NodeFilter.property("x").eq(1i64);
        let x = ExplodedEdgeFilter.property("w").gt(1i64);
        let tree = n.and(x).try_as_filter_tree().unwrap();
        let FilterTree::And(ref items) = tree else {
            panic!("expected structural And, got {tree:?}");
        };
        assert!(matches!(items[0], FilterTree::Node(_)));
        assert!(matches!(items[1], FilterTree::ExplodedEdge(_)));
    }
}
