use raphtory::{
    db::{
        api::view::StaticGraphViewOps,
        graph::views::{cached_view::CachedView, window_graph::WindowedGraph},
    },
    prelude::{GraphViewOps, TimeOps},
};
use raphtory_tests::assertions::GraphTransformer;
use std::ops::Range;

struct CachedGraphTransformer;

impl GraphTransformer for CachedGraphTransformer {
    type Return<G: StaticGraphViewOps> = CachedView<G>;
    fn apply<G: StaticGraphViewOps>(&self, graph: G) -> Self::Return<G> {
        graph.cache_view()
    }
}

struct WindowedCachedGraphTransformer(Range<i64>);

impl GraphTransformer for WindowedCachedGraphTransformer {
    type Return<G: StaticGraphViewOps> = WindowedGraph<CachedView<G>>;
    fn apply<G: StaticGraphViewOps>(&self, graph: G) -> Self::Return<G> {
        graph.cache_view().window(self.0.start, self.0.end)
    }
}

mod test_nodes_filters_cached_view_graph {
    use crate::filter_tests::{
        cached_view::{CachedGraphTransformer, WindowedCachedGraphTransformer},
        init_graph, Edges, Nodes,
    };
    use raphtory::{
        db::graph::views::filter::model::{node_filter::NodeFilter, PropertyExprFactory},
        prelude::EntityExprFilterOps,
    };
    use raphtory_tests::assertions::{
        assert_filter_nodes_results, TestGraphVariants, TestVariants,
    };

    #[test]
    fn test_nodes_filters() {
        let filter = NodeFilter.property("p1").eq(1u64);
        let expected_results = vec!["N1", "N3", "N4", "N6", "N7"];
        assert_filter_nodes_results(
            |graph| init_graph(graph, Nodes::Typed, Edges::None),
            CachedGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_nodes_filters_w() {
        // TODO: Enable event_disk_graph for filter_nodes once bug fixed: https://github.com/Pometry/Raphtory/issues/2098
        let filter = NodeFilter.property("p1").eq(1u64);
        let expected_results = vec!["N1", "N3", "N6"];
        assert_filter_nodes_results(
            |graph| init_graph(graph, Nodes::Typed, Edges::None),
            WindowedCachedGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            vec![TestGraphVariants::Graph],
        );
    }

    #[test]
    fn test_nodes_filters_pg_w() {
        let filter = NodeFilter.property("p1").ge(2u64);
        let expected_results = vec!["N2", "N5", "N8"];
        assert_filter_nodes_results(
            |graph| init_graph(graph, Nodes::Typed, Edges::None),
            WindowedCachedGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }
}

mod test_edges_filter_cached_view_graph {
    use raphtory::prelude::EdgeFilter;

    use raphtory_tests::assertions::{assert_filter_edges_results, TestVariants};

    use crate::filter_tests::cached_view::{
        CachedGraphTransformer, WindowedCachedGraphTransformer,
    };
    use raphtory::{
        db::graph::views::filter::model::PropertyExprFactory, prelude::EntityExprFilterOps,
    };

    use crate::filter_tests::{init_graph, Edges, Nodes};

    #[test]
    fn test_edges_filters() {
        let filter = EdgeFilter.property("p1").eq(1u64);
        let expected_results = vec!["N1->N2", "N3->N4", "N4->N5", "N6->N7", "N7->N8"];
        assert_filter_edges_results(
            |graph| init_graph(graph, Nodes::None, Edges::Unlayered),
            CachedGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_edges_filter_w() {
        let filter = EdgeFilter.property("p1").eq(1u64);
        let expected_results = vec!["N1->N2", "N3->N4", "N6->N7"];
        assert_filter_edges_results(
            |graph| init_graph(graph, Nodes::None, Edges::Unlayered),
            WindowedCachedGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_edges_filters_pg_w() {
        let filter = EdgeFilter.property("p1").ge(2u64);
        let expected_results = vec!["N2->N3", "N5->N6", "N8->N1"];
        assert_filter_edges_results(
            |graph| init_graph(graph, Nodes::None, Edges::Unlayered),
            WindowedCachedGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            vec![],
        );
    }
}
