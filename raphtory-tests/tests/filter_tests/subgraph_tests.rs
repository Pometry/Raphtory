use raphtory::{
    db::{
        api::view::StaticGraphViewOps,
        graph::views::{node_subgraph::NodeSubgraph, window_graph::WindowedGraph},
    },
    prelude::{GraphViewOps, NodeViewOps, TimeOps},
};
use raphtory_tests::assertions::GraphTransformer;
use std::ops::Range;

struct NodeSubgraphTransformer(Option<Vec<String>>);

impl GraphTransformer for NodeSubgraphTransformer {
    type Return<G: StaticGraphViewOps> = NodeSubgraph<G>;
    fn apply<G: StaticGraphViewOps>(&self, graph: G) -> Self::Return<G> {
        let node_names: Vec<String> = self
            .0
            .clone()
            .unwrap_or_else(|| graph.nodes().name().collect::<Vec<String>>());
        graph.subgraph(node_names)
    }
}

struct WindowedNodeSubgraphTransformer(Option<Vec<String>>, Range<i64>);

impl GraphTransformer for WindowedNodeSubgraphTransformer {
    type Return<G: StaticGraphViewOps> = NodeSubgraph<WindowedGraph<G>>;
    fn apply<G: StaticGraphViewOps>(&self, graph: G) -> Self::Return<G> {
        let graph = graph.window(self.1.start, self.1.end);
        let node_names: Vec<String> = self
            .0
            .clone()
            .unwrap_or_else(|| graph.nodes().name().collect::<Vec<String>>());
        graph.subgraph(node_names)
    }
}

mod test_nodes_filters_node_subgraph {
    use crate::filter_tests::subgraph_tests::{
        NodeSubgraphTransformer, WindowedNodeSubgraphTransformer,
    };
    use raphtory::{
        db::{
            api::view::StaticGraphViewOps,
            graph::views::filter::model::{
                property_filter::ops::PropertyFilterOps, PropertyFilterFactory,
            },
        },
        prelude::{AdditionOps, NodeFilter},
    };
    use raphtory_api::core::entities::properties::prop::Prop;
    use raphtory_tests::assertions::{
        assert_filter_nodes_results, assert_search_nodes_results, TestGraphVariants, TestVariants,
    };

    fn init_graph<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
        let nodes = vec![
            (6, "N1", vec![("p1", Prop::U64(2u64))]),
            (7, "N1", vec![("p1", Prop::U64(1u64))]),
            (6, "N2", vec![("p1", Prop::U64(1u64))]),
            (7, "N2", vec![("p1", Prop::U64(2u64))]),
            (8, "N3", vec![("p1", Prop::U64(1u64))]),
            (9, "N4", vec![("p1", Prop::U64(1u64))]),
            (5, "N5", vec![("p1", Prop::U64(1u64))]),
            (6, "N5", vec![("p1", Prop::U64(2u64))]),
            (5, "N6", vec![("p1", Prop::U64(1u64))]),
            (6, "N6", vec![("p1", Prop::U64(1u64))]),
            (3, "N7", vec![("p1", Prop::U64(1u64))]),
            (5, "N7", vec![("p1", Prop::U64(1u64))]),
            (3, "N8", vec![("p1", Prop::U64(1u64))]),
            (4, "N8", vec![("p1", Prop::U64(2u64))]),
        ];

        for (id, name, props) in &nodes {
            graph
                .add_node(*id, name, props.clone(), None, None)
                .unwrap();
        }

        graph
    }

    #[test]
    fn test_search_nodes_subgraph() {
        let filter = NodeFilter.property("p1").eq(1u64);
        let expected_results = ["N1", "N3", "N4", "N6", "N7"];
        assert_filter_nodes_results(
            init_graph,
            NodeSubgraphTransformer(None),
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_graph,
            NodeSubgraphTransformer(None),
            filter,
            &expected_results,
            TestVariants::All,
        );

        let node_names: Option<Vec<String>> =
            Some(vec!["N2".into(), "N3".into(), "N4".into(), "N5".into()]);
        let filter = NodeFilter.property("p1").le(1u64);
        let expected_results = vec!["N3", "N4"];
        assert_filter_nodes_results(
            init_graph,
            NodeSubgraphTransformer(node_names.clone()),
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_graph,
            NodeSubgraphTransformer(node_names),
            filter,
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_search_nodes_subgraph_w() {
        // TODO: Enable event_disk_graph for filter_nodes once bug fixed: https://github.com/Pometry/Raphtory/issues/2098
        let filter = NodeFilter.property("p1").eq(1u64);
        let expected_results = vec!["N1", "N3", "N6"];
        assert_filter_nodes_results(
            init_graph,
            WindowedNodeSubgraphTransformer(None, 6..9),
            filter.clone(),
            &expected_results,
            vec![TestGraphVariants::Graph],
        );
        assert_search_nodes_results(
            init_graph,
            WindowedNodeSubgraphTransformer(None, 6..9),
            filter,
            &expected_results,
            vec![TestGraphVariants::Graph],
        );

        let node_names: Option<Vec<String>> = Some(vec!["N3".into()]);
        let filter = NodeFilter.property("p1").gt(0u64);
        let expected_results = vec!["N3"];
        assert_filter_nodes_results(
            init_graph,
            WindowedNodeSubgraphTransformer(node_names.clone(), 6..9),
            filter.clone(),
            &expected_results,
            vec![TestGraphVariants::Graph],
        );
        assert_search_nodes_results(
            init_graph,
            WindowedNodeSubgraphTransformer(node_names, 6..9),
            filter,
            &expected_results,
            vec![TestGraphVariants::Graph],
        );
    }

    #[test]
    fn test_search_nodes_pg_w() {
        let filter = NodeFilter.property("p1").eq(1u64);
        let expected_results = vec!["N1", "N3", "N6", "N7"];
        assert_filter_nodes_results(
            init_graph,
            WindowedNodeSubgraphTransformer(None, 6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
        assert_search_nodes_results(
            init_graph,
            WindowedNodeSubgraphTransformer(None, 6..9),
            filter,
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let node_names: Option<Vec<String>> =
            Some(vec!["N2".into(), "N3".into(), "N4".into(), "N5".into()]);
        let filter = NodeFilter.property("p1").ge(1u64);
        let expected_results = vec!["N2", "N3", "N5"];
        assert_filter_nodes_results(
            init_graph,
            WindowedNodeSubgraphTransformer(node_names.clone(), 6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
        assert_search_nodes_results(
            init_graph,
            WindowedNodeSubgraphTransformer(node_names, 6..9),
            filter,
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }
}

mod test_edges_filters_node_subgraph {
    use raphtory::{
        db::{
            api::view::StaticGraphViewOps,
            graph::views::filter::model::{
                property_filter::ops::PropertyFilterOps, PropertyFilterFactory,
            },
        },
        prelude::{AdditionOps, EdgeFilter},
    };
    use raphtory_api::core::entities::properties::prop::Prop;
    use raphtory_tests::assertions::{
        assert_filter_edges_results, assert_search_edges_results, TestVariants,
    };

    use crate::filter_tests::subgraph_tests::{
        NodeSubgraphTransformer, WindowedNodeSubgraphTransformer,
    };

    fn init_graph<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
        let edges = vec![
            (6, "N1", "N2", vec![("p1", Prop::U64(2u64))]),
            (7, "N1", "N2", vec![("p1", Prop::U64(1u64))]),
            (6, "N2", "N3", vec![("p1", Prop::U64(1u64))]),
            (7, "N2", "N3", vec![("p1", Prop::U64(2u64))]),
            (8, "N3", "N4", vec![("p1", Prop::U64(1u64))]),
            (9, "N4", "N5", vec![("p1", Prop::U64(1u64))]),
            (5, "N5", "N6", vec![("p1", Prop::U64(1u64))]),
            (6, "N5", "N6", vec![("p1", Prop::U64(2u64))]),
            (5, "N6", "N7", vec![("p1", Prop::U64(1u64))]),
            (6, "N6", "N7", vec![("p1", Prop::U64(1u64))]),
            (3, "N7", "N8", vec![("p1", Prop::U64(1u64))]),
            (5, "N7", "N8", vec![("p1", Prop::U64(1u64))]),
            (3, "N8", "N1", vec![("p1", Prop::U64(1u64))]),
            (4, "N8", "N1", vec![("p1", Prop::U64(2u64))]),
        ];

        for (id, src, tgt, props) in &edges {
            graph.add_edge(*id, src, tgt, props.clone(), None).unwrap();
        }

        graph
    }

    #[test]
    fn test_edges_filters() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph.
        let filter = EdgeFilter.property("p1").eq(1u64);
        let expected_results = vec!["N1->N2", "N3->N4", "N4->N5", "N6->N7", "N7->N8"];
        assert_filter_edges_results(
            init_graph,
            NodeSubgraphTransformer(None),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_graph,
            NodeSubgraphTransformer(None),
            filter,
            &expected_results,
            TestVariants::All,
        );

        let node_names: Option<Vec<String>> =
            Some(vec!["N2".into(), "N3".into(), "N4".into(), "N5".into()]);
        let filter = EdgeFilter.property("p1").le(1u64);
        let expected_results = vec!["N3->N4", "N4->N5"];
        assert_filter_edges_results(
            init_graph,
            NodeSubgraphTransformer(node_names.clone()),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_graph,
            NodeSubgraphTransformer(node_names),
            filter,
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_edges_filters_w() {
        let filter = EdgeFilter.property("p1").eq(1u64);
        let expected_results = vec!["N1->N2", "N3->N4", "N6->N7"];
        assert_filter_edges_results(
            init_graph,
            WindowedNodeSubgraphTransformer(None, 6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_graph,
            WindowedNodeSubgraphTransformer(None, 6..9),
            filter,
            &expected_results,
            TestVariants::EventOnly,
        );

        let node_names: Option<Vec<String>> =
            Some(vec!["N2".into(), "N3".into(), "N4".into(), "N5".into()]);
        let filter = EdgeFilter.property("p1").ge(1u64);
        let expected_results = vec!["N2->N3", "N3->N4"];
        assert_filter_edges_results(
            init_graph,
            WindowedNodeSubgraphTransformer(node_names.clone(), 6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_graph,
            WindowedNodeSubgraphTransformer(node_names, 6..9),
            filter,
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_edges_filters_pg_w() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph.
        let filter = EdgeFilter.property("p1").eq(1u64);
        let expected_results = vec!["N1->N2", "N3->N4", "N6->N7", "N7->N8"];
        assert_filter_edges_results(
            init_graph,
            WindowedNodeSubgraphTransformer(None, 6..9),
            filter.clone(),
            &expected_results,
            vec![],
        );
        assert_search_edges_results(
            init_graph,
            WindowedNodeSubgraphTransformer(None, 6..9),
            filter,
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let node_names: Option<Vec<String>> = Some(vec![
            "N2".into(),
            "N3".into(),
            "N4".into(),
            "N5".into(),
            "N6".into(),
        ]);
        let filter = EdgeFilter.property("p1").lt(2u64);
        let expected_results = vec!["N3->N4"];
        assert_filter_edges_results(
            init_graph,
            WindowedNodeSubgraphTransformer(node_names.clone(), 6..9),
            filter.clone(),
            &expected_results,
            vec![],
        );
        assert_search_edges_results(
            init_graph,
            WindowedNodeSubgraphTransformer(node_names, 6..9),
            filter,
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }
}
