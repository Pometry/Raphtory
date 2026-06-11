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
    use raphtory::{
        db::{
            api::view::StaticGraphViewOps,
            graph::views::filter::model::{
                node_filter::NodeFilter, property_filter::ops::PropertyFilterOps,
                PropertyFilterFactory,
            },
        },
        prelude::AdditionOps,
    };
    use raphtory_api::core::entities::properties::prop::Prop;
    use raphtory_tests::assertions::{
        assert_filter_nodes_results, assert_search_nodes_results, TestGraphVariants, TestVariants,
    };

    use crate::filter_tests::cached_view::{
        CachedGraphTransformer, WindowedCachedGraphTransformer,
    };

    fn init_graph<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
        let node_data = vec![
            (6, "N1", 2u64, "air_nomad"),
            (7, "N1", 1u64, "air_nomad"),
            (6, "N2", 1u64, "water_tribe"),
            (7, "N2", 2u64, "water_tribe"),
            (8, "N3", 1u64, "air_nomad"),
            (9, "N4", 1u64, "air_nomad"),
            (5, "N5", 1u64, "air_nomad"),
            (6, "N5", 2u64, "air_nomad"),
            (5, "N6", 1u64, "fire_nation"),
            (6, "N6", 1u64, "fire_nation"),
            (3, "N7", 1u64, "air_nomad"),
            (5, "N7", 1u64, "air_nomad"),
            (3, "N8", 1u64, "fire_nation"),
            (4, "N8", 2u64, "fire_nation"),
        ];

        for (ts, name, value, kind) in node_data {
            graph
                .add_node(ts, name, [("p1", Prop::U64(value))], Some(kind), None)
                .unwrap();
        }

        graph
    }

    #[test]
    fn test_nodes_filters() {
        let filter = NodeFilter.property("p1").eq(1u64);
        let expected_results = vec!["N1", "N3", "N4", "N6", "N7"];
        assert_filter_nodes_results(
            init_graph,
            CachedGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
        assert_search_nodes_results(
            init_graph,
            CachedGraphTransformer,
            filter,
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
            init_graph,
            WindowedCachedGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            vec![TestGraphVariants::Graph],
        );
        assert_search_nodes_results(
            init_graph,
            WindowedCachedGraphTransformer(6..9),
            filter,
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_nodes_filters_pg_w() {
        let filter = NodeFilter.property("p1").ge(2u64);
        let expected_results = vec!["N2", "N5", "N8"];
        assert_filter_nodes_results(
            init_graph,
            WindowedCachedGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
        assert_search_nodes_results(
            init_graph,
            WindowedCachedGraphTransformer(6..9),
            filter,
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }
}

mod test_edges_filter_cached_view_graph {
    use raphtory::{
        db::api::view::StaticGraphViewOps,
        prelude::{AdditionOps, EdgeFilter},
    };
    use raphtory_api::core::entities::properties::prop::Prop;
    use raphtory_tests::assertions::{
        assert_filter_edges_results, assert_search_edges_results, TestVariants,
    };

    use crate::filter_tests::cached_view::{
        CachedGraphTransformer, WindowedCachedGraphTransformer,
    };
    use raphtory::db::graph::views::filter::model::{
        property_filter::ops::PropertyFilterOps, PropertyFilterFactory,
    };

    fn init_graph<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
        let edge_data = vec![
            (6, "N1", "N2", 2u64),
            (7, "N1", "N2", 1u64),
            (6, "N2", "N3", 1u64),
            (7, "N2", "N3", 2u64),
            (8, "N3", "N4", 1u64),
            (9, "N4", "N5", 1u64),
            (5, "N5", "N6", 1u64),
            (6, "N5", "N6", 2u64),
            (5, "N6", "N7", 1u64),
            (6, "N6", "N7", 1u64),
            (3, "N7", "N8", 1u64),
            (5, "N7", "N8", 1u64),
            (3, "N8", "N1", 1u64),
            (4, "N8", "N1", 2u64),
        ];

        for (ts, src, dst, p1_val) in edge_data {
            graph
                .add_edge(ts, src, dst, [("p1", Prop::U64(p1_val))], None)
                .unwrap();
        }

        graph
    }

    #[test]
    fn test_edges_filters() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
        let filter = EdgeFilter.property("p1").eq(1u64);
        let expected_results = vec!["N1->N2", "N3->N4", "N4->N5", "N6->N7", "N7->N8"];
        assert_filter_edges_results(
            init_graph,
            CachedGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_graph,
            CachedGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_edges_filter_w() {
        let filter = EdgeFilter.property("p1").eq(1u64);
        let expected_results = vec!["N1->N2", "N3->N4", "N6->N7"];
        assert_filter_edges_results(
            init_graph,
            WindowedCachedGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_graph,
            WindowedCachedGraphTransformer(6..9),
            filter,
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_edges_filters_pg_w() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
        let filter = EdgeFilter.property("p1").ge(2u64);
        let expected_results = vec!["N2->N3", "N5->N6", "N8->N1"];
        assert_filter_edges_results(
            init_graph,
            WindowedCachedGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            vec![],
        );
        assert_search_edges_results(
            init_graph,
            WindowedCachedGraphTransformer(6..9),
            filter,
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }
}
