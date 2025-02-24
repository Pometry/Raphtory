use crate::{
    core::{storage::timeindex::AsTime, utils::errors::GraphError},
    db::{
        api::{
            properties::internal::TemporalPropertiesOps,
            storage::graph::edges::edge_storage_ops::EdgeStorageOps,
            view::{
                internal::{core_ops::CoreGraphOps, InternalIndexSearch, NodeFilterOps},
                StaticGraphViewOps,
            },
        },
        graph::{
            edge::EdgeView,
            node::NodeView,
            views::property_filter::{CompositeEdgeFilter, CompositeNodeFilter},
        },
    },
    prelude::*,
    search::{
        edge_filter_executor::EdgeFilterExecutor, graph_index::GraphIndex,
        node_filter_executor::NodeFilterExecutor,
    },
};
use itertools::Itertools;
use rayon::{prelude::ParallelIterator, slice::ParallelSlice};
use std::{fmt::Debug, ops::Deref};
use tantivy::{query::Query, schema::Value, Document};

#[derive(Copy, Clone)]
pub struct Searcher<'a> {
    pub(crate) index: &'a GraphIndex,
    node_filter_executor: NodeFilterExecutor<'a>,
    edge_filter_executor: EdgeFilterExecutor<'a>,
}

impl<'a> Searcher<'a> {
    pub(crate) fn new(index: &'a GraphIndex) -> Self {
        Self {
            index,
            node_filter_executor: NodeFilterExecutor::new(index),
            edge_filter_executor: EdgeFilterExecutor::new(index),
        }
    }

    pub fn search_nodes<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &CompositeNodeFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<G>>, GraphError> {
        self.node_filter_executor
            .filter_nodes(graph, filter, limit, offset)
    }

    pub fn search_edges<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &CompositeEdgeFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<G>>, GraphError> {
        self.edge_filter_executor
            .filter_edges(graph, filter, limit, offset)
    }

    pub fn fuzzy_search_nodes<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &CompositeNodeFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<G>>, GraphError> {
        self.node_filter_executor
            .filter_nodes(graph, filter, limit, offset)
    }

    pub fn fuzzy_search_edges<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &CompositeEdgeFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<G>>, GraphError> {
        self.edge_filter_executor
            .filter_edges(graph, filter, limit, offset)
    }
}

// TODO: Fuzzy search tests are non exhaustive because the fuzzy search
//  semantics are still undecided. See Query Builder.
// TODO: All search tests in graph views (db/graph/views) should include
//  comparisons to filter apis results.
#[cfg(test)]
mod search_tests {
    use super::*;
    use crate::db::{
        api::{
            mutation::internal::DelegateDeletionOps,
            view::{internal::InternalIndexSearch, SearchableGraphOps},
        },
        graph::views::{deletion_graph::PersistentGraph, property_filter::Filter},
    };
    use proptest::collection::vec;
    use raphtory_api::core::utils::logging::global_info_logger;
    use std::time::SystemTime;
    use tantivy::{doc, query::AllQuery, schema::TEXT, DocAddress, Order};
    use tracing::info;

    #[cfg(test)]
    mod search_nodes {
        use crate::{
            core::{IntoProp, Prop},
            db::{
                api::{
                    mutation::internal::{
                        InheritMutationOps, InternalAdditionOps, InternalPropertyAdditionOps,
                    },
                    properties::internal::TemporalPropertiesOps,
                    view::{
                        internal::{CoreGraphOps, InternalIndexSearch, NodeHistoryFilter},
                        SearchableGraphOps, StaticGraphViewOps,
                    },
                },
                graph::views::property_filter::{
                    CompositeEdgeFilter, CompositeNodeFilter, Filter, PropertyRef, Temporal,
                },
            },
            prelude::{
                AdditionOps, EdgeViewOps, Graph, GraphViewOps, NodeStateOps, NodeViewOps,
                PropertyAdditionOps, PropertyFilter, TimeOps, NO_PROPS,
            },
            search::searcher::search_tests::PersistentGraph,
        };
        use raphtory_api::core::storage::timeindex::{AsTime, TimeIndexEntry};

        #[cfg(test)]
        mod test_nodes_latest_any_semantics {
            use crate::{
                core::Prop,
                db::{
                    api::{
                        mutation::internal::{InternalAdditionOps, InternalPropertyAdditionOps},
                        view::{internal::InternalIndexSearch, StaticGraphViewOps},
                    },
                    graph::views::property_filter::{CompositeNodeFilter, PropertyRef, Temporal},
                },
                prelude::{
                    AdditionOps, Graph, GraphViewOps, NodeViewOps, PropertyAdditionOps,
                    PropertyFilter, NO_PROPS,
                },
            };

            fn init_graph<
                G: StaticGraphViewOps
                    + AdditionOps
                    + InternalAdditionOps
                    + InternalPropertyAdditionOps
                    + PropertyAdditionOps,
            >(
                graph: G,
            ) -> G {
                graph
                    .add_node(6, "N1", [("p1", Prop::U64(2u64))], None)
                    .unwrap();
                graph
                    .add_node(7, "N1", [("p1", Prop::U64(1u64))], None)
                    .unwrap();
                graph
                    .node("N1")
                    .unwrap()
                    .add_constant_properties([("p1", Prop::U64(1u64))])
                    .unwrap();

                graph
                    .add_node(6, "N2", [("p1", Prop::U64(1u64))], None)
                    .unwrap();
                graph
                    .add_node(7, "N2", [("p1", Prop::U64(2u64))], None)
                    .unwrap();

                graph
                    .add_node(8, "N3", [("p1", Prop::U64(1u64))], None)
                    .unwrap();

                graph
                    .add_node(9, "N4", [("p1", Prop::U64(1u64))], None)
                    .unwrap();
                graph
                    .node("N4")
                    .unwrap()
                    .add_constant_properties([("p1", Prop::U64(2u64))])
                    .unwrap();

                graph
                    .add_node(5, "N5", [("p1", Prop::U64(1u64))], None)
                    .unwrap();
                graph
                    .add_node(6, "N5", [("p1", Prop::U64(2u64))], None)
                    .unwrap();

                graph
                    .add_node(5, "N6", [("p1", Prop::U64(1u64))], None)
                    .unwrap();
                graph
                    .add_node(6, "N6", [("p1", Prop::U64(1u64))], None)
                    .unwrap();

                graph
                    .add_node(3, "N7", [("p1", Prop::U64(1u64))], None)
                    .unwrap();
                graph
                    .add_node(5, "N7", [("p1", Prop::U64(1u64))], None)
                    .unwrap();

                graph
                    .add_node(3, "N8", [("p1", Prop::U64(1u64))], None)
                    .unwrap();
                graph
                    .add_node(4, "N8", [("p1", Prop::U64(2u64))], None)
                    .unwrap();

                graph
                    .add_node(2, "N9", [("p1", Prop::U64(2u64))], None)
                    .unwrap();
                graph
                    .node("N9")
                    .unwrap()
                    .add_constant_properties([("p1", Prop::U64(1u64))])
                    .unwrap();

                graph
                    .add_node(2, "N10", [("q1", Prop::U64(0u64))], None)
                    .unwrap();
                graph
                    .add_node(2, "N10", [("p1", Prop::U64(3u64))], None)
                    .unwrap();
                graph
                    .node("N10")
                    .unwrap()
                    .add_constant_properties([("p1", Prop::U64(1u64))])
                    .unwrap();

                graph
                    .add_node(2, "N11", [("p1", Prop::U64(3u64))], None)
                    .unwrap();
                graph
                    .add_node(2, "N11", [("q1", Prop::U64(0u64))], None)
                    .unwrap();
                graph
                    .node("N11")
                    .unwrap()
                    .add_constant_properties([("p1", Prop::U64(1u64))])
                    .unwrap();

                graph
                    .add_node(2, "N12", [("q1", Prop::U64(0u64))], None)
                    .unwrap();
                graph
                    .add_node(3, "N12", [("p1", Prop::U64(3u64))], None)
                    .unwrap();
                graph
                    .node("N12")
                    .unwrap()
                    .add_constant_properties([("p1", Prop::U64(1u64))])
                    .unwrap();

                graph
                    .add_node(2, "N13", [("q1", Prop::U64(0u64))], None)
                    .unwrap();
                graph
                    .add_node(3, "N13", [("p1", Prop::U64(3u64))], None)
                    .unwrap();
                graph
                    .node("N13")
                    .unwrap()
                    .add_constant_properties([("p1", Prop::U64(1u64))])
                    .unwrap();

                graph
                    .add_node(2, "N14", [("q1", Prop::U64(0u64))], None)
                    .unwrap();
                graph
                    .node("N14")
                    .unwrap()
                    .add_constant_properties([("p1", Prop::U64(1u64))])
                    .unwrap();

                graph.add_node(2, "N15", NO_PROPS, None).unwrap();
                graph
                    .node("N15")
                    .unwrap()
                    .add_constant_properties([("p1", Prop::U64(1u64))])
                    .unwrap();

                graph
            }

            #[test]
            fn test_temporal_any_semantics() {
                let g = Graph::new();
                let g = init_graph(g);
                let filter = CompositeNodeFilter::Property(PropertyFilter::eq(
                    PropertyRef::TemporalProperty("p1".to_string(), Temporal::Any),
                    1u64,
                ));
                let mut results = g
                    .searcher()
                    .unwrap()
                    .search_nodes(&g, &filter, 10, 0)
                    .unwrap()
                    .into_iter()
                    .map(|nv| nv.name())
                    .collect::<Vec<_>>();
                results.sort();
                assert_eq!(
                    results,
                    vec!["N1", "N2", "N3", "N4", "N5", "N6", "N7", "N8"]
                )
            }

            #[test]
            fn test_temporal_latest_semantics() {
                let g = Graph::new();
                let g = init_graph(g);
                let filter = CompositeNodeFilter::Property(PropertyFilter::eq(
                    PropertyRef::TemporalProperty("p1".to_string(), Temporal::Latest),
                    1u64,
                ));
                let mut results = g
                    .searcher()
                    .unwrap()
                    .search_nodes(&g, &filter, 10, 0)
                    .unwrap()
                    .into_iter()
                    .map(|nv| nv.name())
                    .collect::<Vec<_>>();
                results.sort();
                assert_eq!(results, vec!["N1", "N3", "N4", "N6", "N7"])
            }

            #[test]
            fn test_constant_semantics() {
                let g = Graph::new();
                let g = init_graph(g);
                let filter = CompositeNodeFilter::Property(PropertyFilter::eq(
                    PropertyRef::ConstantProperty("p1".to_string()),
                    1u64,
                ));
                let mut results = g
                    .searcher()
                    .unwrap()
                    .search_nodes(&g, &filter, 10, 0)
                    .unwrap()
                    .into_iter()
                    .map(|nv| nv.name())
                    .collect::<Vec<_>>();
                results.sort();
                assert_eq!(
                    results,
                    vec!["N1", "N10", "N11", "N12", "N13", "N14", "N15", "N9"]
                )
            }

            #[test]
            fn test_property_constant_semantics() {
                // For this graph there won't be any temporal property index for property name "p1".
                let graph = Graph::new();
                graph
                    .add_node(2, "N1", [("q1", Prop::U64(0u64))], None)
                    .unwrap();
                graph
                    .node("N1")
                    .unwrap()
                    .add_constant_properties([("p1", Prop::U64(1u64))])
                    .unwrap();

                graph.add_node(2, "N2", NO_PROPS, None).unwrap();
                graph
                    .node("N2")
                    .unwrap()
                    .add_constant_properties([("p1", Prop::U64(1u64))])
                    .unwrap();

                let filter = CompositeNodeFilter::Property(PropertyFilter::eq(
                    PropertyRef::Property("p1".to_string()),
                    1u64,
                ));
                let mut results = graph
                    .searcher()
                    .unwrap()
                    .search_nodes(&graph, &filter, 10, 0)
                    .unwrap()
                    .into_iter()
                    .map(|nv| nv.name())
                    .collect::<Vec<_>>();
                results.sort();
                assert_eq!(results, vec!["N1", "N2"])
            }

            #[test]
            fn test_property_temporal_semantics() {
                // For this graph there won't be any constant property index for property name "p1".
                let graph = Graph::new();
                graph
                    .add_node(1, "N1", [("p1", Prop::U64(1u64))], None)
                    .unwrap();
                graph
                    .node("N1")
                    .unwrap()
                    .add_constant_properties([("p2", Prop::U64(1u64))])
                    .unwrap();

                graph
                    .add_node(2, "N2", [("p1", Prop::U64(1u64))], None)
                    .unwrap();
                graph
                    .add_node(3, "N2", [("p1", Prop::U64(2u64))], None)
                    .unwrap();

                graph
                    .add_node(2, "N3", [("p1", Prop::U64(2u64))], None)
                    .unwrap();
                graph
                    .add_node(3, "N3", [("p1", Prop::U64(1u64))], None)
                    .unwrap();

                graph.add_node(2, "N4", NO_PROPS, None).unwrap();

                let filter = CompositeNodeFilter::Property(PropertyFilter::eq(
                    PropertyRef::Property("p1".to_string()),
                    1u64,
                ));
                let mut results = graph
                    .searcher()
                    .unwrap()
                    .search_nodes(&graph, &filter, 10, 0)
                    .unwrap()
                    .into_iter()
                    .map(|nv| nv.name())
                    .collect::<Vec<_>>();
                results.sort();
                assert_eq!(results, vec!["N1", "N3"])
            }

            #[test]
            fn test_property_semantics() {
                let g = Graph::new();
                let g = init_graph(g);
                let filter = CompositeNodeFilter::Property(PropertyFilter::eq(
                    PropertyRef::Property("p1".to_string()),
                    1u64,
                ));
                let mut results = g
                    .searcher()
                    .unwrap()
                    .search_nodes(&g, &filter, 10, 0)
                    .unwrap()
                    .into_iter()
                    .map(|nv| nv.name())
                    .collect::<Vec<_>>();
                results.sort();
                assert_eq!(results, vec!["N1", "N14", "N15", "N3", "N4", "N6", "N7"])
            }
        }

        #[cfg(test)]
        mod test_edges_latest_any_semantics {
            use crate::{
                core::Prop,
                db::{
                    api::{
                        mutation::internal::{InternalAdditionOps, InternalPropertyAdditionOps},
                        view::{internal::InternalIndexSearch, StaticGraphViewOps},
                    },
                    graph::views::property_filter::{CompositeEdgeFilter, PropertyRef, Temporal},
                },
                prelude::{
                    AdditionOps, EdgeViewOps, Graph, GraphViewOps, NodeViewOps,
                    PropertyAdditionOps, PropertyFilter, NO_PROPS,
                },
            };

            fn init_graph<
                G: StaticGraphViewOps
                    + AdditionOps
                    + InternalAdditionOps
                    + InternalPropertyAdditionOps
                    + PropertyAdditionOps,
            >(
                graph: G,
            ) -> G {
                graph
                    .add_edge(6, "N1", "N2", [("p1", Prop::U64(2u64))], None)
                    .unwrap();
                graph
                    .add_edge(7, "N1", "N2", [("p1", Prop::U64(1u64))], None)
                    .unwrap();
                graph
                    .edge("N1", "N2")
                    .unwrap()
                    .add_constant_properties([("p1", Prop::U64(1u64))], None)
                    .unwrap();

                graph
                    .add_edge(6, "N2", "N3", [("p1", Prop::U64(1u64))], None)
                    .unwrap();
                graph
                    .add_edge(7, "N2", "N3", [("p1", Prop::U64(2u64))], None)
                    .unwrap();

                graph
                    .add_edge(8, "N3", "N4", [("p1", Prop::U64(1u64))], None)
                    .unwrap();

                graph
                    .add_edge(9, "N4", "N5", [("p1", Prop::U64(1u64))], None)
                    .unwrap();
                graph
                    .edge("N4", "N5")
                    .unwrap()
                    .add_constant_properties([("p1", Prop::U64(2u64))], None)
                    .unwrap();

                graph
                    .add_edge(5, "N5", "N6", [("p1", Prop::U64(1u64))], None)
                    .unwrap();
                graph
                    .add_edge(6, "N5", "N6", [("p1", Prop::U64(2u64))], None)
                    .unwrap();

                graph
                    .add_edge(5, "N6", "N7", [("p1", Prop::U64(1u64))], None)
                    .unwrap();
                graph
                    .add_edge(6, "N6", "N7", [("p1", Prop::U64(1u64))], None)
                    .unwrap();

                graph
                    .add_edge(3, "N7", "N8", [("p1", Prop::U64(1u64))], None)
                    .unwrap();
                graph
                    .add_edge(5, "N7", "N8", [("p1", Prop::U64(1u64))], None)
                    .unwrap();

                graph
                    .add_edge(3, "N8", "N9", [("p1", Prop::U64(1u64))], None)
                    .unwrap();
                graph
                    .add_edge(4, "N8", "N9", [("p1", Prop::U64(2u64))], None)
                    .unwrap();

                graph
                    .add_edge(2, "N9", "N10", [("p1", Prop::U64(2u64))], None)
                    .unwrap();
                graph
                    .edge("N9", "N10")
                    .unwrap()
                    .add_constant_properties([("p1", Prop::U64(1u64))], None)
                    .unwrap();

                graph
                    .add_edge(2, "N10", "N11", [("q1", Prop::U64(0u64))], None)
                    .unwrap();
                graph
                    .add_edge(2, "N10", "N11", [("p1", Prop::U64(3u64))], None)
                    .unwrap();
                graph
                    .edge("N10", "N11")
                    .unwrap()
                    .add_constant_properties([("p1", Prop::U64(1u64))], None)
                    .unwrap();

                graph
                    .add_edge(2, "N11", "N12", [("p1", Prop::U64(3u64))], None)
                    .unwrap();
                graph
                    .add_edge(2, "N11", "N12", [("q1", Prop::U64(0u64))], None)
                    .unwrap();
                graph
                    .edge("N11", "N12")
                    .unwrap()
                    .add_constant_properties([("p1", Prop::U64(1u64))], None)
                    .unwrap();

                graph
                    .add_edge(2, "N12", "N13", [("q1", Prop::U64(0u64))], None)
                    .unwrap();
                graph
                    .add_edge(3, "N12", "N13", [("p1", Prop::U64(3u64))], None)
                    .unwrap();
                graph
                    .edge("N12", "N13")
                    .unwrap()
                    .add_constant_properties([("p1", Prop::U64(1u64))], None)
                    .unwrap();

                graph
                    .add_edge(2, "N13", "N14", [("q1", Prop::U64(0u64))], None)
                    .unwrap();
                graph
                    .add_edge(3, "N13", "N14", [("p1", Prop::U64(3u64))], None)
                    .unwrap();
                graph
                    .edge("N13", "N14")
                    .unwrap()
                    .add_constant_properties([("p1", Prop::U64(1u64))], None)
                    .unwrap();

                graph
                    .add_edge(2, "N14", "N15", [("q1", Prop::U64(0u64))], None)
                    .unwrap();
                graph
                    .edge("N14", "N15")
                    .unwrap()
                    .add_constant_properties([("p1", Prop::U64(1u64))], None)
                    .unwrap();

                graph.add_edge(2, "N15", "N1", NO_PROPS, None).unwrap();
                graph
                    .edge("N15", "N1")
                    .unwrap()
                    .add_constant_properties([("p1", Prop::U64(1u64))], None)
                    .unwrap();

                graph
            }

            #[test]
            fn test_temporal_any_semantics() {
                let g = Graph::new();
                let g = init_graph(g);
                let filter = CompositeEdgeFilter::Property(PropertyFilter::eq(
                    PropertyRef::TemporalProperty("p1".to_string(), Temporal::Any),
                    1u64,
                ));
                let mut results = g
                    .searcher()
                    .unwrap()
                    .search_edges(&g, &filter, 10, 0)
                    .unwrap()
                    .into_iter()
                    .map(|ev| format!("{}->{}", ev.src().name(), ev.dst().name()))
                    .collect::<Vec<_>>();
                results.sort();
                assert_eq!(
                    results,
                    vec![
                        "N1->N2", "N2->N3", "N3->N4", "N4->N5", "N5->N6", "N6->N7", "N7->N8",
                        "N8->N9"
                    ]
                )
            }

            #[test] // Wrong
            fn test_temporal_latest_semantics() {
                let g = Graph::new();
                let g = init_graph(g);
                let filter = CompositeEdgeFilter::Property(PropertyFilter::eq(
                    PropertyRef::TemporalProperty("p1".to_string(), Temporal::Latest),
                    1u64,
                ));
                let mut results = g
                    .searcher()
                    .unwrap()
                    .search_edges(&g, &filter, 10, 0)
                    .unwrap()
                    .into_iter()
                    .map(|ev| format!("{}->{}", ev.src().name(), ev.dst().name()))
                    .collect::<Vec<_>>();
                results.sort();
                assert_eq!(
                    results,
                    vec!["N1->N2", "N3->N4", "N4->N5", "N6->N7", "N7->N8"]
                )
            }

            #[test]
            fn test_constant_semantics() {
                let g = Graph::new();
                let g = init_graph(g);
                let filter = CompositeEdgeFilter::Property(PropertyFilter::eq(
                    PropertyRef::ConstantProperty("p1".to_string()),
                    1u64,
                ));
                let mut results = g
                    .searcher()
                    .unwrap()
                    .search_edges(&g, &filter, 10, 0)
                    .unwrap()
                    .into_iter()
                    .map(|ev| format!("{}->{}", ev.src().name(), ev.dst().name()))
                    .collect::<Vec<_>>();
                results.sort();
                assert_eq!(
                    results,
                    vec![
                        "N1->N2", "N10->N11", "N11->N12", "N12->N13", "N13->N14", "N14->N15",
                        "N15->N1", "N9->N10"
                    ]
                )
            }

            #[test] // Wrong
            fn test_property_constant_semantics() {
                // For this graph there won't be any temporal property index for property name "p1".
                let g = Graph::new();
                g.add_edge(2, "N1", "N2", [("q1", Prop::U64(0u64))], None)
                    .unwrap();
                g.edge("N1", "N2")
                    .unwrap()
                    .add_constant_properties([("p1", Prop::U64(1u64))], None)
                    .unwrap();

                g.add_edge(2, "N2", "N3", NO_PROPS, None).unwrap();
                g.edge("N2", "N3")
                    .unwrap()
                    .add_constant_properties([("p1", Prop::U64(1u64))], None)
                    .unwrap();

                let filter = CompositeEdgeFilter::Property(PropertyFilter::eq(
                    PropertyRef::Property("p1".to_string()),
                    1u64,
                ));
                let mut results = g
                    .searcher()
                    .unwrap()
                    .search_edges(&g, &filter, 10, 0)
                    .unwrap()
                    .into_iter()
                    .map(|ev| format!("{}->{}", ev.src().name(), ev.dst().name()))
                    .collect::<Vec<_>>();
                results.sort();
                assert_eq!(results, vec!["N1->N2", "N2->N3"])
            }

            #[test]
            fn test_property_temporal_semantics() {
                // For this graph there won't be any constant property index for property name "p1".
                let g = Graph::new();
                g.add_edge(1, "N1", "N2", [("p1", Prop::U64(1u64))], None)
                    .unwrap();
                g.edge("N1", "N2")
                    .unwrap()
                    .add_constant_properties([("p2", Prop::U64(1u64))], None)
                    .unwrap();

                g.add_edge(2, "N2", "N3", [("p1", Prop::U64(1u64))], None)
                    .unwrap();
                g.add_edge(3, "N2", "N3", [("p1", Prop::U64(2u64))], None)
                    .unwrap();

                g.add_edge(2, "N3", "N4", [("p1", Prop::U64(2u64))], None)
                    .unwrap();
                g.add_edge(3, "N3", "N4", [("p1", Prop::U64(1u64))], None)
                    .unwrap();

                g.add_edge(2, "N4", "N5", NO_PROPS, None).unwrap();

                let filter = CompositeEdgeFilter::Property(PropertyFilter::eq(
                    PropertyRef::Property("p1".to_string()),
                    1u64,
                ));
                let mut results = g
                    .searcher()
                    .unwrap()
                    .search_edges(&g, &filter, 10, 0)
                    .unwrap()
                    .into_iter()
                    .map(|ev| format!("{}->{}", ev.src().name(), ev.dst().name()))
                    .collect::<Vec<_>>();
                results.sort();
                assert_eq!(results, vec!["N1->N2", "N3->N4"])
            }

            #[test]
            fn test_property_semantics() {
                let g = Graph::new();
                let g = init_graph(g);
                let filter = CompositeEdgeFilter::Property(PropertyFilter::eq(
                    PropertyRef::Property("p1".to_string()),
                    1u64,
                ));
                let mut results = g
                    .searcher()
                    .unwrap()
                    .search_edges(&g, &filter, 10, 0)
                    .unwrap()
                    .into_iter()
                    .map(|ev| format!("{}->{}", ev.src().name(), ev.dst().name()))
                    .collect::<Vec<_>>();
                results.sort();
                assert_eq!(
                    results,
                    vec!["N1->N2", "N14->N15", "N15->N1", "N3->N4", "N4->N5", "N6->N7", "N7->N8"]
                )
            }
        }

        fn search_nodes_by_composite_filter(filter: &CompositeNodeFilter) -> Vec<String> {
            let graph = Graph::new();
            graph
                .add_node(
                    1,
                    1,
                    [
                        ("p1", "shivam_kapoor".into_prop()),
                        ("p9", 5u64.into_prop()),
                    ],
                    Some("fire_nation"),
                )
                .unwrap();
            graph
                .add_node(
                    2,
                    2,
                    [("p1", "prop12".into_prop()), ("p2", 2u64.into_prop())],
                    Some("air_nomads"),
                )
                .unwrap();
            graph
                .add_node(
                    3,
                    1,
                    [
                        ("p1", "shivam_kapoor".into_prop()),
                        ("p9", 5u64.into_prop()),
                    ],
                    Some("fire_nation"),
                )
                .unwrap();
            graph
                .add_node(3, 3, [("p2", 6u64), ("p3", 1u64)], Some("fire_nation"))
                .unwrap();
            graph
                .add_node(
                    4,
                    1,
                    [
                        ("p1", "shivam_kapoor".into_prop()),
                        ("p9", 5u64.into_prop()),
                    ],
                    Some("fire_nation"),
                )
                .unwrap();
            graph.add_node(3, 4, [("p4", "pometry")], None).unwrap();
            graph.add_node(4, 4, [("p5", 12u64)], None).unwrap();

            let mut results = graph
                .search_nodes(&filter, 10, 0)
                .expect("Failed to search for nodes")
                .into_iter()
                .map(|v| v.name())
                .collect::<Vec<_>>();
            results.sort();

            results
        }

        fn fuzzy_search_nodes_by_composite_filter(filter: &CompositeNodeFilter) -> Vec<String> {
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

            let mut results = graph
                .fuzzy_search_nodes(&filter, 10, 0)
                .expect("Failed to search for nodes")
                .into_iter()
                .map(|v| v.name())
                .collect::<Vec<_>>();
            results.sort();
            results
        }

        #[test]
        fn test_search_nodes_by_composite_filter() {
            let filter = CompositeNodeFilter::And(vec![
                CompositeNodeFilter::Property(PropertyFilter::eq(
                    PropertyRef::Property("p2".to_string()),
                    2u64,
                )),
                CompositeNodeFilter::Property(PropertyFilter::eq(
                    PropertyRef::Property("p1".to_string()),
                    3u64,
                )),
            ]);
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, Vec::<String>::new());

            let filter = CompositeNodeFilter::Or(vec![
                CompositeNodeFilter::Property(PropertyFilter::eq(
                    PropertyRef::Property("p2".to_string()),
                    2u64,
                )),
                CompositeNodeFilter::Property(PropertyFilter::eq(
                    PropertyRef::Property("p1".to_string()),
                    "shivam",
                )),
            ]);
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["1", "2"]);

            let filter = CompositeNodeFilter::Or(vec![
                CompositeNodeFilter::Property(PropertyFilter::eq(
                    PropertyRef::Property("p1".to_string()),
                    "pometry",
                )),
                CompositeNodeFilter::And(vec![
                    CompositeNodeFilter::Property(PropertyFilter::eq(
                        PropertyRef::Property("p2".to_string()),
                        6u64,
                    )),
                    CompositeNodeFilter::Property(PropertyFilter::eq(
                        PropertyRef::Property("p3".to_string()),
                        1u64,
                    )),
                ]),
            ]);
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["3"]);

            let filter = CompositeNodeFilter::And(vec![
                CompositeNodeFilter::Node(Filter::eq("node_type", "fire_nation")),
                CompositeNodeFilter::Property(PropertyFilter::eq(
                    PropertyRef::Property("p1".to_string()),
                    "prop1",
                )),
            ]);
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, Vec::<String>::new());
        }

        #[test]
        fn search_nodes_for_node_name_eq() {
            let filter = CompositeNodeFilter::Node(Filter::eq("node_name", "3"));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["3"]);
        }

        #[test]
        fn search_nodes_for_node_name_ne() {
            let filter = CompositeNodeFilter::Node(Filter::ne("node_name", "2"));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["1", "3", "4"]);
        }

        #[test]
        fn search_nodes_for_node_name_in() {
            let filter = CompositeNodeFilter::Node(Filter::includes("node_name", vec!["1".into()]));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["1"]);

            let filter = CompositeNodeFilter::Node(Filter::includes(
                "node_name",
                vec!["2".into(), "3".into()],
            ));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["2", "3"]);
        }

        #[test]
        fn search_nodes_for_node_name_not_in() {
            let filter = CompositeNodeFilter::Node(Filter::excludes("node_name", vec!["1".into()]));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["2", "3", "4"]);
        }

        #[test]
        fn search_nodes_for_node_type_eq() {
            let filter = CompositeNodeFilter::Node(Filter::eq("node_type", "fire_nation"));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["1", "3"]);
        }

        #[test]
        fn search_nodes_for_node_type_ne() {
            let filter = CompositeNodeFilter::Node(Filter::ne("node_type", "fire_nation"));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["2", "4"]);
        }

        #[test]
        fn search_nodes_for_node_type_in() {
            let filter = CompositeNodeFilter::Node(Filter::includes(
                "node_type",
                vec!["fire_nation".into()],
            ));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["1", "3"]);

            let filter = CompositeNodeFilter::Node(Filter::includes(
                "node_type",
                vec!["fire_nation".into(), "air_nomads".into()],
            ));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["1", "2", "3"]);
        }

        #[test]
        fn search_nodes_for_node_type_not_in() {
            let filter = CompositeNodeFilter::Node(Filter::excludes(
                "node_type",
                vec!["fire_nation".into()],
            ));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["2", "4"]);
        }

        #[test]
        fn search_nodes_for_property_eq() {
            let filter = CompositeNodeFilter::Property(PropertyFilter::eq(
                PropertyRef::Property("p2".to_string()),
                2u64,
            ));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["2"]);
        }

        #[test]
        fn search_nodes_for_property_ne() {
            let filter = CompositeNodeFilter::Property(PropertyFilter::ne(
                PropertyRef::Property("p2".to_string()),
                2u64,
            ));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["3"]);
        }

        #[test]
        fn search_nodes_for_property_lt() {
            let filter = CompositeNodeFilter::Property(PropertyFilter::lt(
                PropertyRef::Property("p2".to_string()),
                10u64,
            ));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["2", "3"]);
        }

        #[test]
        fn search_nodes_for_property_le() {
            let filter = CompositeNodeFilter::Property(PropertyFilter::le(
                PropertyRef::Property("p2".to_string()),
                6u64,
            ));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["2", "3"]);
        }

        #[test]
        fn search_nodes_for_property_gt() {
            let filter = CompositeNodeFilter::Property(PropertyFilter::gt(
                PropertyRef::Property("p2".to_string()),
                2u64,
            ));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["3"]);
        }

        #[test]
        fn search_nodes_for_property_ge() {
            let filter = CompositeNodeFilter::Property(PropertyFilter::ge(
                PropertyRef::Property("p2".to_string()),
                2u64,
            ));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["2", "3"]);
        }

        #[test]
        fn search_nodes_for_property_in() {
            let filter = CompositeNodeFilter::Property(PropertyFilter::includes(
                PropertyRef::Property("p2".to_string()),
                vec![Prop::U64(6)],
            ));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["3"]);

            let filter = CompositeNodeFilter::Property(PropertyFilter::includes(
                PropertyRef::Property("p2".to_string()),
                vec![Prop::U64(2), Prop::U64(6)],
            ));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["2", "3"]);
        }

        #[test]
        fn search_nodes_for_property_not_in() {
            let filter = CompositeNodeFilter::Property(PropertyFilter::excludes(
                PropertyRef::Property("p2".to_string()),
                vec![Prop::U64(6)],
            ));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["2"]);
        }

        #[test]
        fn search_nodes_for_property_is_some() {
            let filter = CompositeNodeFilter::Property(PropertyFilter::is_some(
                PropertyRef::Property("p2".to_string()),
            ));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["2", "3"]);
        }

        #[test]
        fn search_nodes_for_property_is_none() {
            let filter = CompositeNodeFilter::Property(PropertyFilter::is_none(
                PropertyRef::Property("p2".to_string()),
            ));
            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["1", "4"]);
        }

        #[test]
        fn test_search_nodes_by_props_added_at_different_times() {
            let filter = CompositeNodeFilter::And(vec![
                CompositeNodeFilter::Property(PropertyFilter::eq(
                    PropertyRef::Property("p4".to_string()),
                    "pometry",
                )),
                CompositeNodeFilter::Property(PropertyFilter::eq(
                    PropertyRef::Property("p5".to_string()),
                    12u64,
                )),
            ]);

            let results = search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["4"]);
        }

        #[test]
        fn test_fuzzy_search() {
            let filter = CompositeNodeFilter::Node(Filter::fuzzy_search(
                "node_name",
                "shivam_kapoor",
                2,
                false,
            ));
            let results = fuzzy_search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["shivam_kapoor"]);

            let filter =
                CompositeNodeFilter::Node(Filter::fuzzy_search("node_name", "pomet", 2, false));
            let results = fuzzy_search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["pometry"]);
        }

        #[test]
        fn test_fuzzy_search_prefix_match() {
            let filter =
                CompositeNodeFilter::Node(Filter::fuzzy_search("node_name", "pome", 2, false));
            let results = fuzzy_search_nodes_by_composite_filter(&filter);
            assert_eq!(results, Vec::<String>::new());

            let filter =
                CompositeNodeFilter::Node(Filter::fuzzy_search("node_name", "pome", 2, true));
            let results = fuzzy_search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["pometry"]);
        }

        #[test]
        fn test_fuzzy_search_property() {
            let filter = CompositeNodeFilter::Property(PropertyFilter::fuzzy_search(
                PropertyRef::Property("p1".to_string()),
                "tano",
                2,
                false,
            ));
            let results = fuzzy_search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["pometry"]);
        }

        #[test]
        fn test_fuzzy_search_property_prefix_match() {
            let filter = CompositeNodeFilter::Property(PropertyFilter::fuzzy_search(
                PropertyRef::Property("p1".to_string()),
                "char",
                2,
                false,
            ));
            let results = fuzzy_search_nodes_by_composite_filter(&filter);
            assert_eq!(results, Vec::<String>::new());

            let filter = CompositeNodeFilter::Property(PropertyFilter::fuzzy_search(
                PropertyRef::Property("p1".to_string()),
                "char",
                2,
                true,
            ));
            let results = fuzzy_search_nodes_by_composite_filter(&filter);
            assert_eq!(results, vec!["shivam_kapoor"]);
        }

        // More tests for windowed graph and other graph views can be found in their respective files
        // under src/db/graph/views
        #[test]
        fn test_search_nodes_windowed_graph() {
            let graph = PersistentGraph::new();
            graph
                .add_node(6, "N1", [("p1", Prop::U64(2u64))], None)
                .unwrap();
            graph
                .add_node(7, "N1", [("p1", Prop::U64(1u64))], None)
                .unwrap();

            graph
                .add_node(6, "N2", [("p1", Prop::U64(1u64))], None)
                .unwrap();
            graph
                .add_node(7, "N2", [("p1", Prop::U64(2u64))], None)
                .unwrap();

            graph
                .add_node(8, "N3", [("p1", Prop::U64(1u64))], None)
                .unwrap();

            graph
                .add_node(9, "N4", [("p1", Prop::U64(1u64))], None)
                .unwrap();

            graph
                .add_node(5, "N5", [("p1", Prop::U64(1u64))], None)
                .unwrap();
            graph
                .add_node(6, "N5", [("p1", Prop::U64(2u64))], None)
                .unwrap();

            graph
                .add_node(5, "N6", [("p1", Prop::U64(1u64))], None)
                .unwrap();
            graph
                .add_node(6, "N6", [("p1", Prop::U64(1u64))], None)
                .unwrap();

            graph
                .add_node(3, "N7", [("p1", Prop::U64(1u64))], None)
                .unwrap();
            graph
                .add_node(5, "N7", [("p1", Prop::U64(1u64))], None)
                .unwrap();

            graph
                .add_node(3, "N8", [("p1", Prop::U64(1u64))], None)
                .unwrap();
            graph
                .add_node(4, "N8", [("p1", Prop::U64(2u64))], None)
                .unwrap();

            let filter = CompositeNodeFilter::Property(PropertyFilter::eq(
                PropertyRef::Property("p1".to_string()),
                1u64,
            ));
            let mut results = graph
                .window(6, 9)
                .search_nodes(&filter, 10, 0)
                .expect("Failed to search for nodes")
                .into_iter()
                .map(|v| v.name())
                .collect::<Vec<_>>();
            results.sort();

            assert_eq!(results, vec!["N1", "N2", "N3", "N5", "N6", "N7"]);
        }
    }

    #[cfg(test)]
    mod search_edges {
        use crate::{
            core::{IntoProp, Prop},
            db::{
                api::view::SearchableGraphOps,
                graph::views::{
                    deletion_graph::PersistentGraph,
                    property_filter::{
                        CompositeEdgeFilter, CompositeNodeFilter, Filter, PropertyRef,
                    },
                },
            },
            prelude::{
                AdditionOps, EdgePropertyFilterOps, EdgeViewOps, Graph, GraphViewOps,
                NodePropertyFilterOps, NodeViewOps, PropertyFilter, TimeOps,
            },
        };
        use itertools::Itertools;

        fn search_edges_by_composite_filter(filter: &CompositeEdgeFilter) -> Vec<(String, String)> {
            let graph = Graph::new();

            graph
                .add_edge(1, 1, 2, [("p1", "shivam_kapoor")], Some("fire_nation"))
                .unwrap();
            graph
                .add_edge(
                    2,
                    1,
                    2,
                    [
                        ("p1", "shivam_kapoor".into_prop()),
                        ("p2", 4u64.into_prop()),
                    ],
                    Some("fire_nation"),
                )
                .unwrap();
            graph
                .add_edge(
                    2,
                    2,
                    3,
                    [("p1", "prop12".into_prop()), ("p2", 2u64.into_prop())],
                    Some("air_nomads"),
                )
                .unwrap();
            graph
                .add_edge(3, 3, 1, [("p2", 6u64), ("p3", 1u64)], Some("fire_nation"))
                .unwrap();
            graph
                .add_edge(3, 2, 1, [("p2", 6u64), ("p3", 1u64)], None)
                .unwrap();

            let mut results = graph
                .search_edges(&filter, 5, 0)
                .expect("Failed to search for nodes")
                .into_iter()
                .map(|e| (e.src().name(), e.dst().name()))
                .collect::<Vec<_>>();
            results.sort();

            results
        }

        fn fuzzy_search_edges_by_composite_filter(
            filter: &CompositeEdgeFilter,
        ) -> Vec<(String, String)> {
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

            let mut results = graph
                .fuzzy_search_edges(&filter, 5, 0)
                .expect("Failed to search for nodes")
                .into_iter()
                .map(|e| (e.src().name(), e.dst().name()))
                .collect::<Vec<_>>();
            results.sort();

            results
        }

        #[test]
        fn test_search_edges_by_composite_filter() {
            let filter = CompositeEdgeFilter::And(vec![
                CompositeEdgeFilter::Property(PropertyFilter::eq(
                    PropertyRef::Property("p2".to_string()),
                    2u64,
                )),
                CompositeEdgeFilter::Property(PropertyFilter::eq(
                    PropertyRef::Property("p1".to_string()),
                    3u64,
                )),
            ]);
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(results, Vec::<(String, String)>::new());

            let filter = CompositeEdgeFilter::Or(vec![
                CompositeEdgeFilter::Property(PropertyFilter::eq(
                    PropertyRef::Property("p2".to_string()),
                    2u64,
                )),
                CompositeEdgeFilter::Property(PropertyFilter::eq(
                    PropertyRef::Property("p1".to_string()),
                    "shivam",
                )),
            ]);
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(
                results,
                vec![("1".into(), "2".into()), ("2".into(), "3".into())]
            );

            let filter = CompositeEdgeFilter::Or(vec![
                CompositeEdgeFilter::Property(PropertyFilter::eq(
                    PropertyRef::Property("p1".to_string()),
                    "pometry",
                )),
                CompositeEdgeFilter::And(vec![
                    CompositeEdgeFilter::Property(PropertyFilter::eq(
                        PropertyRef::Property("p2".to_string()),
                        6u64,
                    )),
                    CompositeEdgeFilter::Property(PropertyFilter::eq(
                        PropertyRef::Property("p3".to_string()),
                        1u64,
                    )),
                ]),
            ]);
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(
                results,
                vec![("2".into(), "1".into()), ("3".into(), "1".into())]
            );

            let filter = CompositeEdgeFilter::And(vec![
                CompositeEdgeFilter::Edge(Filter::eq("from", "13")),
                CompositeEdgeFilter::Property(PropertyFilter::eq(
                    PropertyRef::Property("p1".to_string()),
                    "prop1",
                )),
            ]);
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(results, Vec::<(String, String)>::new());
        }

        #[test]
        fn search_edges_for_src_from_eq() {
            let filter = CompositeEdgeFilter::Edge(Filter::eq("from", "2"));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(
                results,
                vec![("2".into(), "1".into()), ("2".into(), "3".into())]
            );
        }

        #[test]
        fn search_edges_for_src_to_ne() {
            let filter = CompositeEdgeFilter::Edge(Filter::ne("to", "2"));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(
                results,
                vec![
                    ("2".into(), "1".into()),
                    ("2".into(), "3".into()),
                    ("3".into(), "1".into())
                ]
            );
        }

        #[test]
        fn search_edges_for_to_in() {
            let filter = CompositeEdgeFilter::Edge(Filter::includes("to", vec!["2".into()]));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(results, vec![("1".into(), "2".into())]);

            let filter =
                CompositeEdgeFilter::Edge(Filter::includes("to", vec!["2".into(), "3".into()]));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(
                results,
                vec![("1".into(), "2".into()), ("2".into(), "3".into())]
            );
        }

        #[test]
        fn search_edges_for_to_not_in() {
            let filter = CompositeEdgeFilter::Edge(Filter::excludes("to", vec!["1".into()]));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(
                results,
                vec![("1".into(), "2".into()), ("2".into(), "3".into())]
            );
        }

        #[test]
        fn search_edges_for_from_eq() {
            let filter = CompositeEdgeFilter::Edge(Filter::eq("from", "3"));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(results, vec![("3".into(), "1".into())]);
        }

        #[test]
        fn search_edges_for_from_ne() {
            let filter = CompositeEdgeFilter::Edge(Filter::ne("from", "1"));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(
                results,
                vec![
                    ("2".into(), "1".into()),
                    ("2".into(), "3".into()),
                    ("3".into(), "1".into())
                ]
            );
        }

        #[test]
        fn search_edges_for_from_in() {
            let filter = CompositeEdgeFilter::Edge(Filter::includes("from", vec!["1".into()]));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(results, vec![("1".into(), "2".into())]);

            let filter =
                CompositeEdgeFilter::Edge(Filter::includes("from", vec!["1".into(), "2".into()]));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(
                results,
                vec![
                    ("1".into(), "2".into()),
                    ("2".into(), "1".into()),
                    ("2".into(), "3".into())
                ]
            );
        }

        #[test]
        fn search_edges_for_from_not_in() {
            let filter = CompositeEdgeFilter::Edge(Filter::excludes("from", vec!["1".into()]));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(
                results,
                vec![
                    ("2".into(), "1".into()),
                    ("2".into(), "3".into()),
                    ("3".into(), "1".into())
                ]
            );
        }

        #[test]
        fn search_edges_for_property_eq() {
            let filter = CompositeEdgeFilter::Property(PropertyFilter::eq(
                PropertyRef::Property("p2".to_string()),
                2u64,
            ));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(results, vec![("2".into(), "3".into())]);
        }

        #[test]
        fn search_edges_for_property_ne() {
            let filter = CompositeEdgeFilter::Property(PropertyFilter::ne(
                PropertyRef::Property("p2".to_string()),
                2u64,
            ));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(
                results,
                vec![
                    ("1".into(), "2".into()),
                    ("2".into(), "1".into()),
                    ("3".into(), "1".into())
                ]
            );
        }

        #[test]
        fn search_edges_for_property_lt() {
            let filter = CompositeEdgeFilter::Property(PropertyFilter::lt(
                PropertyRef::Property("p2".to_string()),
                10u64,
            ));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(
                results,
                vec![
                    ("1".into(), "2".into()),
                    ("2".into(), "1".into()),
                    ("2".into(), "3".into()),
                    ("3".into(), "1".into())
                ]
            );
        }

        #[test]
        fn search_edges_for_property_le() {
            let filter = CompositeEdgeFilter::Property(PropertyFilter::le(
                PropertyRef::Property("p2".to_string()),
                6u64,
            ));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(
                results,
                vec![
                    ("1".into(), "2".into()),
                    ("2".into(), "1".into()),
                    ("2".into(), "3".into()),
                    ("3".into(), "1".into())
                ]
            );
        }

        #[test]
        fn search_edges_for_property_gt() {
            let filter = CompositeEdgeFilter::Property(PropertyFilter::gt(
                PropertyRef::Property("p2".to_string()),
                2u64,
            ));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(
                results,
                vec![
                    ("1".into(), "2".into()),
                    ("2".into(), "1".into()),
                    ("3".into(), "1".into())
                ]
            );
        }

        #[test]
        fn search_edges_for_property_ge() {
            let filter = CompositeEdgeFilter::Property(PropertyFilter::ge(
                PropertyRef::Property("p2".to_string()),
                2u64,
            ));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(
                results,
                vec![
                    ("1".into(), "2".into()),
                    ("2".into(), "1".into()),
                    ("2".into(), "3".into()),
                    ("3".into(), "1".into())
                ]
            );
        }

        #[test]
        fn search_edges_for_property_in() {
            let filter = CompositeEdgeFilter::Property(PropertyFilter::includes(
                PropertyRef::Property("p2".to_string()),
                vec![Prop::U64(6)],
            ));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(
                results,
                vec![("2".into(), "1".into()), ("3".into(), "1".into())]
            );

            let filter = CompositeEdgeFilter::Property(PropertyFilter::includes(
                PropertyRef::Property("p2".to_string()),
                vec![Prop::U64(2), Prop::U64(6)],
            ));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(
                results,
                vec![
                    ("2".into(), "1".into()),
                    ("2".into(), "3".into()),
                    ("3".into(), "1".into())
                ]
            );
        }

        #[test]
        fn search_edges_for_property_not_in() {
            let filter = CompositeEdgeFilter::Property(PropertyFilter::excludes(
                PropertyRef::Property("p2".to_string()),
                vec![Prop::U64(6)],
            ));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(
                results,
                vec![("1".into(), "2".into()), ("2".into(), "3".into())]
            );
        }

        #[test]
        fn search_edges_for_property_is_some() {
            let filter = CompositeEdgeFilter::Property(PropertyFilter::is_some(
                PropertyRef::Property("p2".to_string()),
            ));
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(
                results,
                vec![
                    ("1".into(), "2".into()),
                    ("2".into(), "1".into()),
                    ("2".into(), "3".into()),
                    ("3".into(), "1".into())
                ]
            );
        }

        // #[test]
        // fn search_edges_for_property_is_none() {
        //     let filter = CompositeNodeFilter::Property(PropertyFilter::is_none("p2"));
        //     let results = search_nodes_by_composite_filter(&filter);
        //     assert_eq!(results, vec!["1"]);
        // }

        #[test]
        fn search_edge_by_src_dst() {
            let filter = CompositeEdgeFilter::And(vec![
                CompositeEdgeFilter::Edge(Filter::eq("from", "3")),
                CompositeEdgeFilter::Edge(Filter::eq("to", "1")),
            ]);
            let results = search_edges_by_composite_filter(&filter);
            assert_eq!(results, vec![("3".into(), "1".into())]);
        }

        #[test]
        fn test_fuzzy_search() {
            let filter = CompositeEdgeFilter::Edge(Filter::fuzzy_search("from", "shiva", 2, false));
            let results = fuzzy_search_edges_by_composite_filter(&filter);
            assert_eq!(results, vec![("shivam".into(), "raphtory".into())]);

            let filter = CompositeEdgeFilter::Edge(Filter::fuzzy_search("to", "pomet", 2, false));
            let results = fuzzy_search_edges_by_composite_filter(&filter);
            assert_eq!(results, vec![("raphtory".into(), "pometry".into())]);
        }

        #[test]
        fn test_fuzzy_search_prefix_match() {
            let filter = CompositeEdgeFilter::Edge(Filter::fuzzy_search("to", "pome", 2, false));
            let results = fuzzy_search_edges_by_composite_filter(&filter);
            assert_eq!(results, Vec::<(String, String)>::new());

            let filter = CompositeEdgeFilter::Edge(Filter::fuzzy_search("to", "pome", 2, true));
            let results = fuzzy_search_edges_by_composite_filter(&filter);
            assert_eq!(results, vec![("raphtory".into(), "pometry".into())]);
        }

        #[test]
        fn test_fuzzy_search_property() {
            let filter = CompositeEdgeFilter::Property(PropertyFilter::fuzzy_search(
                PropertyRef::Property("p1".to_string()),
                "tano",
                2,
                false,
            ));
            let results = fuzzy_search_edges_by_composite_filter(&filter);
            assert_eq!(results, vec![("shivam".into(), "raphtory".into())]);
        }

        #[test]
        fn test_fuzzy_search_property_prefix_match() {
            let filter = CompositeEdgeFilter::Property(PropertyFilter::fuzzy_search(
                PropertyRef::Property("p1".to_string()),
                "charl",
                1,
                false,
            ));
            let results = fuzzy_search_edges_by_composite_filter(&filter);
            assert_eq!(results, Vec::<(String, String)>::new());

            let filter = CompositeEdgeFilter::Property(PropertyFilter::fuzzy_search(
                PropertyRef::Property("p1".to_string()),
                "charl",
                1,
                true,
            ));
            let results = fuzzy_search_edges_by_composite_filter(&filter);
            assert_eq!(results, vec![("raphtory".into(), "pometry".into())]);
        }
    }

    // #[test]
    // fn test_node_update_index() {
    //     let graph = Graph::new();
    //     graph
    //         .add_node(
    //             0,
    //             1,
    //             [("t_prop1", 1u64), ("t_prop2", 2u64)],
    //             Some("fire_nation"),
    //         )
    //         .unwrap();
    //     graph
    //         .add_node(1, 1, [("t_prop1", 5u64)], Some("fire_nation"))
    //         .unwrap();
    //     graph
    //         .add_node(0, 2, [("t_prop1", 2u64)], Some("air_nomads"))
    //         .unwrap();
    //     graph
    //         .add_node(0, 3, [("t_prop1", 3u64)], Some("water_tribe"))
    //         .unwrap();
    //     graph
    //         .add_node(0, 4, [("t_prop1", 4u64)], Some("earth_kingdom"))
    //         .unwrap();
    //     graph
    //         .node(1)
    //         .unwrap()
    //         .add_constant_properties([("c_prop1", Prop::Bool(true))])
    //         .unwrap();
    //
    //     // Create index from graph
    //     let _ = graph.searcher().unwrap();
    //
    //     // Delayed graph node update
    //     graph
    //         .add_node(0, 1, [("t_prop3", 3u64)], Some("fire_nation"))
    //         .unwrap();
    //     //
    //     // let query = AllQuery;
    //     //
    //     // let searcher = graph.searcher().unwrap().index.node_reader.searcher();
    //     // let top_docs = searcher
    //     //     .search(&AllQuery, &TopDocs::with_limit(100))
    //     //     .unwrap();
    //     //
    //     // println!("Total doc count: {}", top_docs.len());
    //     //
    //     // for (_score, doc_address) in top_docs {
    //     //     let doc: TantivyDocument = searcher.doc(doc_address).unwrap();
    //     //     println!("Document: {:?}", doc.to_json(searcher.schema()));
    //     // }
    //
    //     let filter = CompositeNodeFilter::And(vec![
    //         CompositeNodeFilter::Property(PropertyFilter::eq("t_prop1", 1u64)),
    //         CompositeNodeFilter::Property(PropertyFilter::eq("t_prop1", 1u64)),
    //     ]);
    //
    //     let mut results = graph
    //         .search_nodes(&filter, 5, 0)
    //         .expect("Failed to search for nodes")
    //         .into_iter()
    //         .map(|v| v.name())
    //         .collect::<Vec<_>>();
    //     results.sort();
    //
    //     assert_eq!(results, vec!["1"]);
    //
    //     let filter = CompositeNodeFilter::And(vec![
    //         CompositeNodeFilter::Property(PropertyFilter::eq("t_prop1", 5u64)),
    //         CompositeNodeFilter::Property(PropertyFilter::eq("c_prop1", true)),
    //     ]);
    //
    //     let mut results = graph
    //         .search_nodes(&filter, 5, 0)
    //         .expect("Failed to search for nodes")
    //         .into_iter()
    //         .map(|v| v.name())
    //         .collect::<Vec<_>>();
    //     results.sort();
    //
    //     assert_eq!(results, vec!["1"]);
    // }

    #[test]
    #[cfg(feature = "proto")]
    #[ignore = "this test is for experiments with the jira graph"]
    fn load_jira_graph() -> Result<(), GraphError> {
        global_info_logger();
        let graph = Graph::decode("/tmp/graphs/jira").expect("failed to load graph");
        assert!(graph.count_nodes() > 0);

        let now = SystemTime::now();

        let elapsed = now.elapsed().unwrap().as_secs();
        info!("indexing took: {:?}", elapsed);

        let filter = CompositeNodeFilter::Node(Filter::eq("name", "DEV-1690"));
        let issues = graph
            .searcher()
            .unwrap()
            .search_nodes(&graph, &filter, 5, 0)?;

        assert!(!issues.is_empty());

        let names = issues.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        info!("names: {:?}", names);

        Ok(())
    }

    // #[test]
    // fn test_search_windowed_graph() {
    //     let graph = Graph::new();
    //     for t in 0..10 {
    //         graph.add_node(t, 1, [("prop", t)], None).unwrap();
    //     }
    //     let wg = graph.window(8, 12);
    //     let results = wg
    //         .search_nodes("prop:1", 5, 0)
    //         .expect("Failed to search for node")
    //         .into_iter()
    //         .map(|v| v.name())
    //         .collect::<Vec<_>>();
    //
    //     println!("results = {:?}", results); // Should return "no results" as for window 8-12, node 1 has props 8-9
    // }
    //
    // #[test]
    // fn test_search_windowed_persistent_graph() {
    //     let graph = PersistentGraph::new();
    //     for t in 0..10 {
    //         graph.add_node(t, 1, [("test", t)], None).unwrap();
    //     }
    //     let wg = graph.window(8, 12);
    //     let results = wg
    //         .search_nodes("test:1", 5, 0)
    //         .expect("failed to search for node")
    //         .into_iter()
    //         .map(|v| v.name())
    //         .collect::<Vec<_>>();
    //
    //     println!("results = {:?}", results); // Should return "no results" as for window 8-12, node 1 has props 8-9
    //
    //     let wg = graph.window(10, 12);
    //     let results = wg
    //         .search_nodes("test:1", 5, 0)
    //         .expect("failed to search for node")
    //         .into_iter()
    //         .map(|v| v.name())
    //         .collect::<Vec<_>>();
    //
    //     println!("results = {:?}", results); // Should return "no results" as for window 10-12, node 1 has prop 9 as last prop update
    //
    //     let wg = graph.window(10, 12);
    //     let results = wg
    //         .search_nodes("test:9", 5, 0)
    //         .expect("failed to search for node")
    //         .into_iter()
    //         .map(|v| v.name())
    //         .collect::<Vec<_>>();
    //
    //     println!("results = {:?}", results); // Should return "node 1" as for window 10-12, node 9 has props 9 as last prop update
    //
    //     // Edge case:
    //     // let graph = PersistentGraph::new();
    //     // graph.add_node(0, 1, [("test1", 0)], None).unwrap(); // Creates doc which has both props
    //     // graph.add_node(0, 1, [("test2", 0)], None).unwrap(); // Creates doc which has both props
    //     // graph.add_node(2, 1, [("test2", 1)], None).unwrap(); // Creates doc which has only test2 prop
    //
    //     // Edge case:
    //     let graph = PersistentGraph::new();
    //     graph
    //         .add_node(0, 1, [("test1", 0), ("test2", 0)], None)
    //         .unwrap(); // Creates doc which has both props
    //     graph.add_node(2, 1, [("test2", 1)], None).unwrap(); // Creates doc which has only test2 prop
    //
    //     // Searching for test 2 in window 2-10 should return "no results"
    //     let wg = graph.window(2, 10);
    //     let results = wg
    //         .search_nodes("test2:0", 5, 0)
    //         .expect("failed to search for node")
    //         .into_iter()
    //         .map(|v| v.name())
    //         .collect::<Vec<_>>();
    //
    //     // Searching for test 2 in window 2-10 should return "node 1"
    //     let wg = graph.window(2, 10);
    //     let results = wg
    //         .search_nodes("test2:1", 5, 0)
    //         .expect("failed to search for node")
    //         .into_iter()
    //         .map(|v| v.name())
    //         .collect::<Vec<_>>();
    //
    //     // Searching for test 1 in window 2-10 should return "node 1"
    //     let wg = graph.window(2, 10);
    //     let results = wg
    //         .search_nodes("test1:0", 5, 0)
    //         .expect("failed to search for node")
    //         .into_iter()
    //         .map(|v| v.name())
    //         .collect::<Vec<_>>();
    //
    //     println!("results = {:?}", results); // Should return "node 1" as for window 10-12, node 9 has props 9 as last prop update
    // }
    //
    //
    // #[test]
    // fn test_search_nodes_by_props_added_at_different_times_range() {
    //     let graph = Graph::new();
    //     graph
    //         .add_node(1, 1, [("t_prop1", 1)], Some("fire_nation"))
    //         .unwrap();
    //     graph
    //         .add_node(3, 1, [("t_prop3", 3)], Some("fire_nation"))
    //         .unwrap();
    //
    //     let mut results = graph
    //         .search_nodes("t_prop1:1 AND t_prop3:3 AND time:[3 TO 3]", 5, 0)
    //         .expect("Failed to search for nodes")
    //         .into_iter()
    //         .map(|v| v.name())
    //         .collect::<Vec<_>>();
    //     results.sort();
    //
    //     assert_eq!(results, vec!["1"]);
    // }
    //
    // #[test]
    // fn test_search_nodes_range() {
    //     let graph = Graph::new();
    //     graph
    //         .add_node(2, 1, [("t_prop1", 1)], Some("fire_nation"))
    //         .unwrap();
    //     graph
    //         .add_node(4, 2, [("t_prop1", 1)], Some("air_nomads"))
    //         .unwrap();
    //     graph
    //         .add_node(6, 3, [("t_prop3", 3)], Some("fire_nation"))
    //         .unwrap();
    //
    //     let mut results = graph
    //         .search_nodes("node_type:fire_nation AND time:[1 TO 6]", 5, 0)
    //         .expect("Failed to search for nodes")
    //         .into_iter()
    //         .map(|v| v.name())
    //         .collect::<Vec<_>>();
    //     results.sort();
    //
    //     assert_eq!(results, vec!["1"]);
    // }
    //
    // // Discuss with lucas
    // #[test]
    // fn test1() {
    //     let graph = Graph::new();
    //     graph
    //         .add_node(0, 1, [("t_prop1", 1), ("t_prop2", 2)], Some("fire_nation"))
    //         .unwrap(); // doc 0
    //     graph
    //         .add_node(1, 1, [("t_prop1", 1), ("t_prop2", 2)], Some("fire_nation"))
    //         .unwrap(); // doc 1
    //     graph
    //         .add_node(
    //             2,
    //             2,
    //             [("t_prop1", 11), ("t_prop2", 12)],
    //             Some("fire_nation"),
    //         )
    //         .unwrap(); // doc 2
    //     graph
    //         .add_node(3, 3, [("t_prop1", 1), ("t_prop2", 2)], Some("water_tribe"))
    //         .unwrap(); // doc 3
    //     graph
    //         .add_node(
    //             4,
    //             4,
    //             [("t_prop1", 31), ("t_prop2", 32)],
    //             Some("fire_nation"),
    //         )
    //         .unwrap(); // doc 4
    //
    //     let mut results = graph
    //         .window(2, 5)
    //         .search_nodes("t_prop1:1 AND t_prop2:2", 5, 0)
    //         .expect("Failed to search for nodes")
    //         .into_iter()
    //         .map(|v| v.name())
    //         .collect::<Vec<_>>();
    //     results.sort();
    //
    //     assert_eq!(results, vec!["3"]);
    // }
    //
    // #[test]
    // fn test2() {
    //     let graph = Graph::new();
    //     graph
    //         .add_node(1, 1, [("p1", 1), ("p2", 2)], Some("fire_nation"))
    //         .unwrap();
    //     graph
    //         .add_node(2, 2, [("p4", 5)], Some("fire_nation"))
    //         .unwrap();
    //     graph
    //         .add_node(3, 3, [("p2", 4), ("p3", 3)], Some("water_tribe"))
    //         .unwrap();
    //
    //     let graph = graph.window(2, 5);
    //     let mut results = graph
    //         .searcher()
    //         .unwrap()
    //         .event_graph_search_nodes(&graph, "p2:4 AND p3:3", 5, 0)
    //         .expect("Failed to search for nodes")
    //         .into_iter()
    //         .map(|v| v.name())
    //         .collect::<Vec<_>>();
    //     results.sort();
    //
    //     // assert_eq!(results, vec!["3"]);
    // }
    //
    //
    // #[test]
    // fn add_node_search_by_description_and_time() {
    //     let graph = Graph::new();
    //     graph
    //         .add_node(
    //             1,
    //             "Gandalf",
    //             [("description", Prop::str("The wizard"))],
    //             None,
    //         )
    //         .expect("add node failed");
    //     graph
    //         .add_node(
    //             2,
    //             "Saruman",
    //             [("description", Prop::str("Another wizard"))],
    //             None,
    //         )
    //         .expect("add node failed");
    //
    //     // Find Saruman
    //     let nodes = graph
    //         .searcher()
    //         .unwrap()
    //         .search_nodes(&graph, r#"description:wizard AND time:[2 TO 5]"#, 10, 0)
    //         .expect("search failed");
    //     let actual = nodes.into_iter().map(|v| v.name()).collect::<Vec<_>>();
    //     let expected = vec!["Saruman"];
    //     assert_eq!(actual, expected);
    //
    //     // Find Gandalf
    //     let nodes = graph
    //         .searcher()
    //         .unwrap()
    //         .search_nodes(&graph, r#"description:'wizard' AND time:[1 TO 2}"#, 10, 0)
    //         .expect("search failed");
    //     let actual = nodes.into_iter().map(|v| v.name()).collect::<Vec<_>>();
    //     let expected = vec!["Gandalf"];
    //     assert_eq!(actual, expected);
    //
    //     // Find both wizards
    //     let nodes = graph
    //         .searcher()
    //         .unwrap()
    //         .search_nodes(&graph, r#"description:'wizard' AND time:[1 TO 100]"#, 10, 0)
    //         .expect("search failed");
    //     let mut actual = nodes.into_iter().map(|v| v.name()).collect::<Vec<_>>();
    //     let mut expected = vec!["Gandalf", "Saruman"];
    //
    //     // FIXME: this is not deterministic
    //     actual.sort();
    //     expected.sort();
    //
    //     assert_eq!(actual, expected);
    // }
}
