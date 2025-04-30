use crate::{
    core::utils::errors::GraphError,
    db::{
        api::view::StaticGraphViewOps,
        graph::{
            edge::EdgeView,
            node::NodeView,
            views::filter::model::{AsEdgeFilter, AsNodeFilter},
        },
    },
    search::{
        edge_filter_executor::EdgeFilterExecutor, graph_index::GraphIndex,
        node_filter_executor::NodeFilterExecutor,
    },
};

#[derive(Copy, Clone)]
pub struct Searcher<'a> {
    node_filter_executor: NodeFilterExecutor<'a>,
    edge_filter_executor: EdgeFilterExecutor<'a>,
}

impl<'a> Searcher<'a> {
    pub(crate) fn new(index: &'a GraphIndex) -> Self {
        Self {
            node_filter_executor: NodeFilterExecutor::new(index),
            edge_filter_executor: EdgeFilterExecutor::new(index),
        }
    }

    pub fn search_nodes<G, F>(
        &self,
        graph: &G,
        filter: F,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<G>>, GraphError>
    where
        G: StaticGraphViewOps,
        F: AsNodeFilter,
    {
        let filter = filter.as_node_filter();
        self.node_filter_executor
            .filter_nodes(graph, &filter, limit, offset)
    }

    pub fn search_edges<G, F>(
        &self,
        graph: &G,
        filter: F,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<G>>, GraphError>
    where
        G: StaticGraphViewOps,
        F: AsEdgeFilter,
    {
        let filter = filter.as_edge_filter();
        self.edge_filter_executor
            .filter_edges(graph, &filter, limit, offset)
    }
}

// TODO: Fuzzy search tests are non exhaustive because the fuzzy search
//  semantics are still undecided. See Query Builder.
// TODO: All search tests in graph views (db/graph/views) should include
//  comparisons to filter apis results.
#[cfg(test)]
mod search_tests {
    use super::*;
    use crate::{db::graph::views::filter::model::NodeFilter, prelude::*};
    use raphtory_api::core::utils::logging::global_info_logger;
    use std::time::SystemTime;
    use tracing::info;

    #[cfg(test)]
    mod search_nodes {
        use crate::{
            core::IntoProp,
            db::{
                api::view::SearchableGraphOps,
                graph::views::filter::model::{
                    AsNodeFilter, NodeFilter, NodeFilterBuilderOps, PropertyFilterOps,
                },
            },
            prelude::{AdditionOps, Graph, NodeViewOps, PropertyFilter},
        };

        fn fuzzy_search_nodes(filter: impl AsNodeFilter) -> Vec<String> {
            let graph = Graph::new();
            graph
                .add_node(
                    1,
                    "pometry",
                    [("p1", "tango".into_prop())],
                    Some("fire_nation"),
                )
                .unwrap();
            graph
                .add_node(
                    1,
                    "shivam_kapoor",
                    [("p1", "charlie_bravo".into_prop())],
                    Some("fire_nation"),
                )
                .unwrap();

            graph.create_index().unwrap();

            let mut results = graph
                .search_nodes(filter, 10, 0)
                .expect("Failed to search for nodes")
                .into_iter()
                .map(|v| v.name())
                .collect::<Vec<_>>();
            results.sort();
            results
        }

        #[test]
        fn test_fuzzy_search() {
            let filter = NodeFilter::name().fuzzy_search("shivam_kapoor", 2, false);
            let results = fuzzy_search_nodes(filter);
            assert_eq!(results, vec!["shivam_kapoor"]);

            let filter = NodeFilter::name().fuzzy_search("pomet", 2, false);
            let results = fuzzy_search_nodes(filter);
            assert_eq!(results, vec!["pometry"]);
        }

        #[test]
        fn test_fuzzy_search_prefix_match() {
            let filter = NodeFilter::name().fuzzy_search("pome", 2, false);
            let results = fuzzy_search_nodes(filter);
            assert_eq!(results, Vec::<String>::new());

            let filter = NodeFilter::name().fuzzy_search("pome", 2, true);
            let results = fuzzy_search_nodes(filter);
            assert_eq!(results, vec!["pometry"]);
        }

        #[test]
        fn test_fuzzy_search_property() {
            let filter = PropertyFilter::property("p1").fuzzy_search("tano", 2, false);
            let results = fuzzy_search_nodes(filter);
            assert_eq!(results, vec!["pometry"]);
        }

        #[test]
        fn test_fuzzy_search_property_prefix_match() {
            let filter = PropertyFilter::property("p1").fuzzy_search("char", 2, false);
            let results = fuzzy_search_nodes(filter);
            assert_eq!(results, Vec::<String>::new());

            let filter = PropertyFilter::property("p1").fuzzy_search("char", 2, true);
            let results = fuzzy_search_nodes(filter);
            assert_eq!(results, vec!["shivam_kapoor"]);
        }
    }

    #[cfg(test)]
    mod search_edges {
        use crate::{
            core::IntoProp,
            db::{
                api::view::SearchableGraphOps,
                graph::views::filter::model::{
                    AsEdgeFilter, EdgeFilter, EdgeFilterOps, PropertyFilterOps,
                },
            },
            prelude::{AdditionOps, EdgeViewOps, Graph, NodeViewOps, PropertyFilter},
        };

        fn fuzzy_search_edges(filter: impl AsEdgeFilter) -> Vec<(String, String)> {
            let graph = Graph::new();
            graph
                .add_edge(
                    1,
                    "shivam",
                    "raphtory",
                    [("p1", "tango")],
                    Some("fire_nation"),
                )
                .unwrap();
            graph
                .add_edge(
                    2,
                    "raphtory",
                    "pometry",
                    [("p1", "charlie".into_prop()), ("p2", 2u64.into_prop())],
                    Some("air_nomads"),
                )
                .unwrap();
            graph
                .add_edge(
                    3,
                    "pometry",
                    "shivam",
                    [("p2", 6u64.into_prop()), ("p1", "classic".into_prop())],
                    Some("fire_nation"),
                )
                .unwrap();

            graph.create_index().unwrap();

            let mut results = graph
                .search_edges(filter, 5, 0)
                .expect("Failed to search for nodes")
                .into_iter()
                .map(|e| (e.src().name(), e.dst().name()))
                .collect::<Vec<_>>();
            results.sort();

            results
        }

        #[test]
        fn test_fuzzy_search() {
            let filter = EdgeFilter::src().name().fuzzy_search("shiva", 2, false);
            let results = fuzzy_search_edges(filter);
            assert_eq!(results, vec![("shivam".into(), "raphtory".into())]);

            let filter = EdgeFilter::dst().name().fuzzy_search("pomet", 2, false);
            let results = fuzzy_search_edges(filter);
            assert_eq!(results, vec![("raphtory".into(), "pometry".into())]);
        }

        #[test]
        fn test_fuzzy_search_prefix_match() {
            let filter = EdgeFilter::dst().name().fuzzy_search("pome", 2, false);
            let results = fuzzy_search_edges(filter);
            assert_eq!(results, Vec::<(String, String)>::new());

            let filter = EdgeFilter::dst().name().fuzzy_search("pome", 2, true);
            let results = fuzzy_search_edges(filter);
            assert_eq!(results, vec![("raphtory".into(), "pometry".into())]);
        }

        #[test]
        fn test_fuzzy_search_property() {
            let filter = PropertyFilter::property("p1").fuzzy_search("tano", 2, false);
            let results = fuzzy_search_edges(filter);
            assert_eq!(results, vec![("shivam".into(), "raphtory".into())]);
        }

        #[test]
        fn test_fuzzy_search_property_prefix_match() {
            let filter = PropertyFilter::property("p1").fuzzy_search("charl", 1, false);
            let results = fuzzy_search_edges(filter);
            assert_eq!(results, Vec::<(String, String)>::new());

            let filter = PropertyFilter::property("p1").fuzzy_search("charl", 1, true);
            let results = fuzzy_search_edges(filter);
            assert_eq!(results, vec![("raphtory".into(), "pometry".into())]);
        }
    }

    #[test]
    #[cfg(feature = "proto")]
    #[ignore = "this test is for experiments with the jira graph"]
    fn load_jira_graph() -> Result<(), GraphError> {
        use crate::db::graph::views::filter::model::NodeFilterBuilderOps;
        global_info_logger();
        let graph = Graph::decode("/tmp/graphs/jira").expect("failed to load graph");
        assert!(graph.count_nodes() > 0);

        let now = SystemTime::now();

        let elapsed = now.elapsed().unwrap().as_secs();
        info!("indexing took: {:?}", elapsed);
        graph.create_index().unwrap();

        let filter = NodeFilter::name().eq("DEV-1690");
        let issues = graph.search_nodes(filter, 5, 0)?;

        assert!(!issues.is_empty());

        let names = issues.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        info!("names: {:?}", names);

        Ok(())
    }
}
