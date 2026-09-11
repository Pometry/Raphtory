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
    use crate::filter_tests::{
        init_graph,
        subgraph_tests::{NodeSubgraphTransformer, WindowedNodeSubgraphTransformer},
        Edges, Nodes,
    };
    use raphtory::{
        db::graph::views::filter::model::PropertyExprFactory,
        prelude::{EntityExprFilterOps, NodeFilter},
    };
    use raphtory_tests::assertions::{
        assert_filter_nodes_results, TestGraphVariants, TestVariants,
    };

    #[test]
    fn test_search_nodes_subgraph() {
        let filter = NodeFilter.property("p1").eq(1u64);
        let expected_results = ["N1", "N3", "N4", "N6", "N7"];
        assert_filter_nodes_results(
            |graph| init_graph(graph, Nodes::Untyped, Edges::None),
            NodeSubgraphTransformer(None),
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let node_names: Option<Vec<String>> =
            Some(vec!["N2".into(), "N3".into(), "N4".into(), "N5".into()]);
        let filter = NodeFilter.property("p1").le(1u64);
        let expected_results = vec!["N3", "N4"];
        assert_filter_nodes_results(
            |graph| init_graph(graph, Nodes::Untyped, Edges::None),
            NodeSubgraphTransformer(node_names.clone()),
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_search_nodes_subgraph_w() {
        let filter = NodeFilter.property("p1").eq(1u64);
        let expected_results = vec!["N1", "N3", "N6"];
        assert_filter_nodes_results(
            |graph| init_graph(graph, Nodes::Untyped, Edges::None),
            WindowedNodeSubgraphTransformer(None, 6..9),
            filter.clone(),
            &expected_results,
            vec![TestGraphVariants::Graph],
        );

        let node_names: Option<Vec<String>> = Some(vec!["N3".into()]);
        let filter = NodeFilter.property("p1").gt(0u64);
        let expected_results = vec!["N3"];
        assert_filter_nodes_results(
            |graph| init_graph(graph, Nodes::Untyped, Edges::None),
            WindowedNodeSubgraphTransformer(node_names.clone(), 6..9),
            filter.clone(),
            &expected_results,
            vec![TestGraphVariants::Graph],
        );
    }

    #[test]
    fn test_search_nodes_pg_w() {
        let filter = NodeFilter.property("p1").eq(1u64);
        let expected_results = vec!["N1", "N3", "N6", "N7"];
        assert_filter_nodes_results(
            |graph| init_graph(graph, Nodes::Untyped, Edges::None),
            WindowedNodeSubgraphTransformer(None, 6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let node_names: Option<Vec<String>> =
            Some(vec!["N2".into(), "N3".into(), "N4".into(), "N5".into()]);
        let filter = NodeFilter.property("p1").ge(1u64);
        let expected_results = vec!["N2", "N3", "N5"];
        assert_filter_nodes_results(
            |graph| init_graph(graph, Nodes::Untyped, Edges::None),
            WindowedNodeSubgraphTransformer(node_names.clone(), 6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }
}

mod test_edges_filters_node_subgraph {
    use crate::filter_tests::{
        init_graph,
        subgraph_tests::{NodeSubgraphTransformer, WindowedNodeSubgraphTransformer},
        Edges, Nodes,
    };
    use raphtory::{
        db::graph::views::filter::model::PropertyExprFactory,
        prelude::{EdgeFilter, EntityExprFilterOps},
    };
    use raphtory_tests::assertions::{assert_filter_edges_results, TestVariants};

    #[test]
    fn test_edges_filters() {
        let filter = EdgeFilter.property("p1").eq(1u64);
        let expected_results = vec!["N1->N2", "N3->N4", "N4->N5", "N6->N7", "N7->N8"];
        assert_filter_edges_results(
            |graph| init_graph(graph, Nodes::None, Edges::Unlayered),
            NodeSubgraphTransformer(None),
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let node_names: Option<Vec<String>> =
            Some(vec!["N2".into(), "N3".into(), "N4".into(), "N5".into()]);
        let filter = EdgeFilter.property("p1").le(1u64);
        let expected_results = vec!["N3->N4", "N4->N5"];
        assert_filter_edges_results(
            |graph| init_graph(graph, Nodes::None, Edges::Unlayered),
            NodeSubgraphTransformer(node_names.clone()),
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_edges_filters_w() {
        let filter = EdgeFilter.property("p1").eq(1u64);
        let expected_results = vec!["N1->N2", "N3->N4", "N6->N7"];
        assert_filter_edges_results(
            |graph| init_graph(graph, Nodes::None, Edges::Unlayered),
            WindowedNodeSubgraphTransformer(None, 6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let node_names: Option<Vec<String>> =
            Some(vec!["N2".into(), "N3".into(), "N4".into(), "N5".into()]);
        let filter = EdgeFilter.property("p1").ge(1u64);
        let expected_results = vec!["N2->N3", "N3->N4"];
        assert_filter_edges_results(
            |graph| init_graph(graph, Nodes::None, Edges::Unlayered),
            WindowedNodeSubgraphTransformer(node_names.clone(), 6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_edges_filters_pg_w() {
        let filter = EdgeFilter.property("p1").eq(1u64);
        let expected_results = vec!["N1->N2", "N3->N4", "N6->N7", "N7->N8"];
        assert_filter_edges_results(
            |graph| init_graph(graph, Nodes::None, Edges::Unlayered),
            WindowedNodeSubgraphTransformer(None, 6..9),
            filter.clone(),
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
            |graph| init_graph(graph, Nodes::None, Edges::Unlayered),
            WindowedNodeSubgraphTransformer(node_names.clone(), 6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }
}
