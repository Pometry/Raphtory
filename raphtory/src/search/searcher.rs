use crate::{
    core::utils::errors::GraphError,
    db::{
        api::view::StaticGraphViewOps,
        graph::{
            edge::EdgeView,
            node::NodeView,
            views::property_filter::{resolve_as_edge_filter, resolve_as_node_filter, FilterExpr},
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

    pub fn search_nodes<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: FilterExpr,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<G>>, GraphError> {
        let filter = resolve_as_node_filter(filter)?;
        self.node_filter_executor
            .filter_nodes(graph, &filter, limit, offset)
    }

    pub fn search_edges<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: FilterExpr,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<G>>, GraphError> {
        let filter = resolve_as_edge_filter(filter)?;
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
    use crate::{db::graph::views::property_filter::NodeFilter, prelude::*};
    use raphtory_api::core::utils::logging::global_info_logger;
    use std::time::SystemTime;
    use tracing::info;

    #[cfg(test)]
    mod search_nodes {
        use crate::{
            core::{IntoProp, Prop},
            db::{
                api::view::SearchableGraphOps,
                graph::views::property_filter::{
                    FilterExpr, NodeFilter, NodeFilterOps, PropertyFilterOps,
                },
            },
            prelude::{AdditionOps, Graph, NodeViewOps, PropertyFilter},
        };

        #[cfg(test)]
        mod test_nodes_latest_any_semantics {
            use crate::{
                core::Prop,
                db::{
                    api::{
                        mutation::internal::{InternalAdditionOps, InternalPropertyAdditionOps},
                        view::{SearchableGraphOps, StaticGraphViewOps},
                    },
                    graph::views::property_filter::{FilterExpr, PropertyFilterOps},
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
                let nodes = [
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
                    (2, "N9", vec![("p1", Prop::U64(2u64))]),
                    (2, "N10", vec![("q1", Prop::U64(0u64))]),
                    (2, "N10", vec![("p1", Prop::U64(3u64))]),
                    (2, "N11", vec![("p1", Prop::U64(3u64))]),
                    (2, "N11", vec![("q1", Prop::U64(0u64))]),
                    (2, "N12", vec![("q1", Prop::U64(0u64))]),
                    (3, "N12", vec![("p1", Prop::U64(3u64))]),
                    (2, "N13", vec![("q1", Prop::U64(0u64))]),
                    (3, "N13", vec![("p1", Prop::U64(3u64))]),
                    (2, "N14", vec![("q1", Prop::U64(0u64))]),
                    (2, "N15", vec![]),
                ];

                for (id, label, props) in nodes.iter() {
                    graph.add_node(*id, label, props.clone(), None).unwrap();
                }

                let constant_properties = [
                    ("N1", [("p1", Prop::U64(1u64))]),
                    ("N4", [("p1", Prop::U64(2u64))]),
                    ("N9", [("p1", Prop::U64(1u64))]),
                    ("N10", [("p1", Prop::U64(1u64))]),
                    ("N11", [("p1", Prop::U64(1u64))]),
                    ("N12", [("p1", Prop::U64(1u64))]),
                    ("N13", [("p1", Prop::U64(1u64))]),
                    ("N14", [("p1", Prop::U64(1u64))]),
                    ("N15", [("p1", Prop::U64(1u64))]),
                ];

                for (node, props) in constant_properties.iter() {
                    graph
                        .node(node)
                        .unwrap()
                        .add_constant_properties(props.clone())
                        .unwrap();
                }

                graph
            }

            fn init_graph_for_secondary_indexes<
                G: StaticGraphViewOps
                    + AdditionOps
                    + InternalAdditionOps
                    + InternalPropertyAdditionOps
                    + PropertyAdditionOps,
            >(
                graph: G,
            ) -> G {
                graph
                    .add_node(1, "N16", [("p1", Prop::U64(2u64))], None)
                    .unwrap();
                graph
                    .add_node(1, "N16", [("p1", Prop::U64(1u64))], None)
                    .unwrap();

                graph
                    .add_node(1, "N17", [("p1", Prop::U64(1u64))], None)
                    .unwrap();
                graph
                    .add_node(1, "N17", [("p1", Prop::U64(2u64))], None)
                    .unwrap();

                graph
            }

            #[test]
            fn test_temporal_any_semantics_for_secondary_indexes() {
                let g = Graph::new();
                let g = init_graph(g);
                // g.encode("/tmp/graphs/master").unwrap();
                let g = init_graph_for_secondary_indexes(g);
                g.create_index().unwrap();

                let filter: FilterExpr = PropertyFilter::temporal_property("p1").any().eq(1u64);
                let mut results = g
                    .search_nodes(filter, 10, 0)
                    .unwrap()
                    .into_iter()
                    .map(|nv| nv.name())
                    .collect::<Vec<_>>();
                results.sort();

                assert_eq!(
                    results,
                    vec!["N1", "N16", "N17", "N2", "N3", "N4", "N5", "N6", "N7", "N8"]
                )
            }

            #[test]
            fn test_temporal_latest_semantics_for_secondary_indexes() {
                let g = Graph::new();
                let g = init_graph(g);
                let g = init_graph_for_secondary_indexes(g);
                g.create_index().unwrap();

                let filter: FilterExpr = PropertyFilter::temporal_property("p1").latest().eq(1u64);
                let mut results = g
                    .search_nodes(filter, 10, 0)
                    .unwrap()
                    .into_iter()
                    .map(|nv| nv.name())
                    .collect::<Vec<_>>();
                results.sort();

                assert_eq!(results, vec!["N1", "N16", "N3", "N4", "N6", "N7"])
            }

            #[test]
            fn test_property_latest_semantics_for_secondary_indexes() {
                let g = Graph::new();
                let g = init_graph(g);
                let g = init_graph_for_secondary_indexes(g);
                g.create_index().unwrap();

                let filter: FilterExpr = PropertyFilter::property("p1").eq(1u64);
                let mut results = g
                    .search_nodes(filter, 10, 0)
                    .unwrap()
                    .into_iter()
                    .map(|nv| nv.name())
                    .collect::<Vec<_>>();
                results.sort();

                assert_eq!(
                    results,
                    vec!["N1", "N14", "N15", "N16", "N3", "N4", "N6", "N7"]
                )
            }

            #[test]
            fn test_temporal_any_semantics() {
                let g = Graph::new();
                let g = init_graph(g);
                g.create_index().unwrap();

                let filter: FilterExpr = PropertyFilter::temporal_property("p1").any().eq(1u64);
                let mut results = g
                    .search_nodes(filter, 10, 0)
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
                g.create_index().unwrap();

                let filter = PropertyFilter::temporal_property("p1").latest().eq(1u64);
                let mut results = g
                    .search_nodes(filter, 10, 0)
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
                g.create_index().unwrap();

                let filter = PropertyFilter::constant_property("p1").eq(1u64);
                let mut results = g
                    .search_nodes(filter, 10, 0)
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
                graph.create_index().unwrap();

                let filter = PropertyFilter::property("p1").eq(1u64);
                let mut results = graph
                    .search_nodes(filter, 10, 0)
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
                graph.create_index().unwrap();

                let filter = PropertyFilter::property("p1").eq(1u64);
                let mut results = graph
                    .search_nodes(filter, 10, 0)
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
                g.create_index().unwrap();

                let filter = PropertyFilter::property("p1").eq(1u64);
                let mut results = g
                    .search_nodes(filter, 10, 0)
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
                        view::{SearchableGraphOps, StaticGraphViewOps},
                    },
                    graph::views::property_filter::{FilterExpr, PropertyFilterOps},
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

            fn init_graph_for_secondary_indexes<
                G: StaticGraphViewOps
                    + AdditionOps
                    + InternalAdditionOps
                    + InternalPropertyAdditionOps
                    + PropertyAdditionOps,
            >(
                graph: G,
            ) -> G {
                graph
                    .add_edge(1, "N16", "N15", [("p1", Prop::U64(2u64))], None)
                    .unwrap();
                graph
                    .add_edge(1, "N16", "N15", [("p1", Prop::U64(1u64))], None)
                    .unwrap();

                graph
                    .add_edge(1, "N17", "N16", [("p1", Prop::U64(1u64))], None)
                    .unwrap();
                graph
                    .add_edge(1, "N17", "N16", [("p1", Prop::U64(2u64))], None)
                    .unwrap();

                graph
            }

            #[test]
            fn test_secondary_indexes_edges() {
                let g = Graph::new();
                let g = init_graph(g);
                g.create_index().unwrap();

                let filter: FilterExpr = PropertyFilter::temporal_property("p1").any().eq(1u64);
                g.search_edges(filter, 10, 0).unwrap();
            }

            #[test]
            fn test_temporal_any_semantics_for_secondary_indexes() {
                let g = Graph::new();
                let g = init_graph(g);
                let g = init_graph_for_secondary_indexes(g);
                g.create_index().unwrap();

                let filter: FilterExpr = PropertyFilter::temporal_property("p1").any().eq(1u64);
                let mut results = g
                    .search_edges(filter, 10, 0)
                    .unwrap()
                    .into_iter()
                    .map(|ev| format!("{}->{}", ev.src().name(), ev.dst().name()))
                    .collect::<Vec<_>>();
                results.sort();

                assert_eq!(
                    results,
                    vec![
                        "N1->N2", "N16->N15", "N17->N16", "N2->N3", "N3->N4", "N4->N5", "N5->N6",
                        "N6->N7", "N7->N8", "N8->N9"
                    ]
                )
            }

            #[test]
            fn test_temporal_latest_semantics_for_secondary_indexes() {
                let g = Graph::new();
                let g = init_graph(g);
                let g = init_graph_for_secondary_indexes(g);
                g.create_index().unwrap();

                let filter: FilterExpr = PropertyFilter::temporal_property("p1").latest().eq(1u64);
                let mut results = g
                    .search_edges(filter, 10, 0)
                    .unwrap()
                    .into_iter()
                    .map(|ev| format!("{}->{}", ev.src().name(), ev.dst().name()))
                    .collect::<Vec<_>>();
                results.sort();

                assert_eq!(
                    results,
                    vec!["N1->N2", "N16->N15", "N3->N4", "N4->N5", "N6->N7", "N7->N8"]
                )
            }

            #[test]
            fn test_property_latest_semantics_for_secondary_indexes() {
                let g = Graph::new();
                let g = init_graph(g);
                let g = init_graph_for_secondary_indexes(g);
                g.create_index().unwrap();

                let filter: FilterExpr = PropertyFilter::property("p1").eq(1u64);
                let mut results = g
                    .search_edges(filter, 10, 0)
                    .unwrap()
                    .into_iter()
                    .map(|ev| format!("{}->{}", ev.src().name(), ev.dst().name()))
                    .collect::<Vec<_>>();
                results.sort();

                assert_eq!(
                    results,
                    vec![
                        "N1->N2", "N14->N15", "N15->N1", "N16->N15", "N3->N4", "N4->N5", "N6->N7",
                        "N7->N8"
                    ]
                )
            }

            #[test]
            fn test_temporal_any_semantics() {
                let g = Graph::new();
                let g = init_graph(g);
                g.create_index().unwrap();

                let filter = PropertyFilter::temporal_property("p1").any().eq(1u64);
                let mut results = g
                    .search_edges(filter, 10, 0)
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
                g.create_index().unwrap();

                let filter = PropertyFilter::temporal_property("p1").latest().eq(1u64);
                let mut results = g
                    .search_edges(filter, 10, 0)
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
                g.create_index().unwrap();

                let filter = PropertyFilter::constant_property("p1").eq(1u64);
                let mut results = g
                    .search_edges(filter, 10, 0)
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
                g.create_index().unwrap();

                let filter = PropertyFilter::property("p1").eq(1u64);
                let mut results = g
                    .search_edges(filter, 10, 0)
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
                g.create_index().unwrap();

                let filter = PropertyFilter::property("p1").eq(1u64);
                let mut results = g
                    .search_edges(filter, 10, 0)
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
                g.create_index().unwrap();

                let filter = PropertyFilter::property("p1").eq(1u64);
                let mut results = g
                    .search_edges(filter, 10, 0)
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

        fn search_nodes_by_composite_filter(filter: FilterExpr) -> Vec<String> {
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

        fn fuzzy_search_nodes_by_composite_filter(filter: FilterExpr) -> Vec<String> {
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
        fn test_search_nodes_by_composite_filter() {
            let filter = PropertyFilter::property("p2")
                .eq(2u64)
                .and(PropertyFilter::property("p1").eq(3u64));
            let results = search_nodes_by_composite_filter(filter);
            assert_eq!(results, Vec::<String>::new());

            let filter = PropertyFilter::property("p2")
                .eq(2u64)
                .or(PropertyFilter::property("p1").eq("shivam"));
            let results = search_nodes_by_composite_filter(filter);
            assert_eq!(results, vec!["1", "2"]);

            let filter = PropertyFilter::property("p1")
                .eq("pometry")
                .or(PropertyFilter::property("p2")
                    .eq(6u64)
                    .and(PropertyFilter::property("p3").eq(1u64)));
            let results = search_nodes_by_composite_filter(filter);
            assert_eq!(results, vec!["3"]);

            let filter = NodeFilter::node_type()
                .eq("fire_nation")
                .and(PropertyFilter::property("p1").eq("prop1"));
            let results = search_nodes_by_composite_filter(filter);
            assert_eq!(results, Vec::<String>::new());
        }

        #[test]
        fn search_nodes_for_node_name_eq() {
            let filter = NodeFilter::node_name().eq("3");
            let results = search_nodes_by_composite_filter(filter);
            assert_eq!(results, vec!["3"]);
        }

        #[test]
        fn search_nodes_for_node_name_ne() {
            let filter = NodeFilter::node_name().ne("2");
            let results = search_nodes_by_composite_filter(filter);
            assert_eq!(results, vec!["1", "3", "4"]);
        }

        #[test]
        fn search_nodes_for_node_name_in() {
            let filter = NodeFilter::node_name().includes(vec!["1".into()]);
            let results = search_nodes_by_composite_filter(filter);
            assert_eq!(results, vec!["1"]);

            let filter = NodeFilter::node_name().includes(vec!["2".into(), "3".into()]);
            let results = search_nodes_by_composite_filter(filter);
            assert_eq!(results, vec!["2", "3"]);
        }

        #[test]
        fn search_nodes_for_node_name_not_in() {
            let filter = NodeFilter::node_name().excludes(vec!["1".into()]);
            let results = search_nodes_by_composite_filter(filter);
            assert_eq!(results, vec!["2", "3", "4"]);
        }

        #[test]
        fn search_nodes_for_node_type_eq() {
            let filter = NodeFilter::node_type().eq("fire_nation");
            let results = search_nodes_by_composite_filter(filter);
            assert_eq!(results, vec!["1", "3"]);
        }

        #[test]
        fn search_nodes_for_node_type_ne() {
            let filter = NodeFilter::node_type().ne("fire_nation");
            let results = search_nodes_by_composite_filter(filter);
            assert_eq!(results, vec!["2", "4"]);
        }

        #[test]
        fn search_nodes_for_node_type_in() {
            let filter = NodeFilter::node_type().includes(vec!["fire_nation".into()]);
            let results = search_nodes_by_composite_filter(filter);
            assert_eq!(results, vec!["1", "3"]);

            let filter =
                NodeFilter::node_type().includes(vec!["fire_nation".into(), "air_nomads".into()]);
            let results = search_nodes_by_composite_filter(filter);
            assert_eq!(results, vec!["1", "2", "3"]);
        }

        #[test]
        fn search_nodes_for_node_type_not_in() {
            let filter = NodeFilter::node_type().excludes(vec!["fire_nation".into()]);
            let results = search_nodes_by_composite_filter(filter);
            assert_eq!(results, vec!["2", "4"]);
        }

        #[test]
        fn search_nodes_for_property_eq() {
            let filter = PropertyFilter::property("p2").eq(2u64);
            let results = search_nodes_by_composite_filter(filter);
            assert_eq!(results, vec!["2"]);
        }

        #[test]
        fn search_nodes_for_property_ne() {
            let filter = PropertyFilter::property("p2").ne(2u64);
            let results = search_nodes_by_composite_filter(filter);
            assert_eq!(results, vec!["3"]);
        }

        #[test]
        fn search_nodes_for_property_lt() {
            let filter = PropertyFilter::property("p2").lt(10u64);
            let results = search_nodes_by_composite_filter(filter);
            assert_eq!(results, vec!["2", "3"]);
        }

        #[test]
        fn search_nodes_for_property_le() {
            let filter = PropertyFilter::property("p2").le(6u64);
            let results = search_nodes_by_composite_filter(filter);
            assert_eq!(results, vec!["2", "3"]);
        }

        #[test]
        fn search_nodes_for_property_gt() {
            let filter = PropertyFilter::property("p2").gt(2u64);
            let results = search_nodes_by_composite_filter(filter);
            assert_eq!(results, vec!["3"]);
        }

        #[test]
        fn search_nodes_for_property_ge() {
            let filter = PropertyFilter::property("p2").ge(2u64);
            let results = search_nodes_by_composite_filter(filter);
            assert_eq!(results, vec!["2", "3"]);
        }

        #[test]
        fn search_nodes_for_property_in() {
            let filter = PropertyFilter::property("p2").includes(vec![Prop::U64(6)]);
            let results = search_nodes_by_composite_filter(filter);
            assert_eq!(results, vec!["3"]);

            let filter = PropertyFilter::property("p2").includes(vec![Prop::U64(2), Prop::U64(6)]);
            let results = search_nodes_by_composite_filter(filter);
            assert_eq!(results, vec!["2", "3"]);
        }

        #[test]
        fn search_nodes_for_property_not_in() {
            let filter = PropertyFilter::property("p2").excludes(vec![Prop::U64(6)]);
            let results = search_nodes_by_composite_filter(filter);
            assert_eq!(results, vec!["2"]);
        }

        #[test]
        fn search_nodes_for_property_is_some() {
            let filter = PropertyFilter::property("p2").is_some();
            let results = search_nodes_by_composite_filter(filter);
            assert_eq!(results, vec!["2", "3"]);
        }

        #[test]
        fn search_nodes_for_property_is_none() {
            let filter = PropertyFilter::property("p2").is_none();
            let results = search_nodes_by_composite_filter(filter);
            assert_eq!(results, vec!["1", "4"]);
        }

        #[test]
        fn test_search_nodes_by_props_added_at_different_times() {
            let filter = PropertyFilter::property("p4")
                .eq("pometry")
                .and(PropertyFilter::property("p5").eq(12u64));

            let results = search_nodes_by_composite_filter(filter);
            assert_eq!(results, vec!["4"]);
        }

        #[test]
        fn test_fuzzy_search() {
            let filter = NodeFilter::node_name().fuzzy_search("shivam_kapoor", 2, false);
            let results = fuzzy_search_nodes_by_composite_filter(filter);
            assert_eq!(results, vec!["shivam_kapoor"]);

            let filter = NodeFilter::node_name().fuzzy_search("pomet", 2, false);
            let results = fuzzy_search_nodes_by_composite_filter(filter);
            assert_eq!(results, vec!["pometry"]);
        }

        #[test]
        fn test_fuzzy_search_prefix_match() {
            let filter = NodeFilter::node_name().fuzzy_search("pome", 2, false);
            let results = fuzzy_search_nodes_by_composite_filter(filter);
            assert_eq!(results, Vec::<String>::new());

            let filter = NodeFilter::node_name().fuzzy_search("pome", 2, true);
            let results = fuzzy_search_nodes_by_composite_filter(filter);
            assert_eq!(results, vec!["pometry"]);
        }

        #[test]
        fn test_fuzzy_search_property() {
            let filter = PropertyFilter::property("p1").fuzzy_search("tano", 2, false);
            let results = fuzzy_search_nodes_by_composite_filter(filter);
            assert_eq!(results, vec!["pometry"]);
        }

        #[test]
        fn test_fuzzy_search_property_prefix_match() {
            let filter = PropertyFilter::property("p1").fuzzy_search("char", 2, false);
            let results = fuzzy_search_nodes_by_composite_filter(filter);
            assert_eq!(results, Vec::<String>::new());

            let filter = PropertyFilter::property("p1").fuzzy_search("char", 2, true);
            let results = fuzzy_search_nodes_by_composite_filter(filter);
            assert_eq!(results, vec!["shivam_kapoor"]);
        }

        // More tests for windowed graph and other graph views can be found in their respective files
        // under src/db/graph/views
    }

    #[cfg(test)]
    mod search_edges {
        use crate::{
            core::{IntoProp, Prop},
            db::{
                api::view::SearchableGraphOps,
                graph::views::property_filter::{
                    EdgeFilter, EdgeFilterOps, FilterExpr, PropertyFilterOps,
                },
            },
            prelude::{AdditionOps, EdgeViewOps, Graph, NodeViewOps, PropertyFilter},
        };
        fn search_edges(filter: FilterExpr) -> Vec<(String, String)> {
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

        fn fuzzy_search_edges_by_composite_filter(filter: FilterExpr) -> Vec<(String, String)> {
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
        fn test_search_edges_by_composite_filter() {
            let filter = PropertyFilter::property("p2")
                .eq(2u64)
                .and(PropertyFilter::property("p1").eq(3u64));
            let results = search_edges(filter);
            assert_eq!(results, Vec::<(String, String)>::new());

            let filter = PropertyFilter::property("p2")
                .eq(2u64)
                .or(PropertyFilter::property("p1").eq("shivam"));
            let results = search_edges(filter);
            assert_eq!(
                results,
                vec![("1".into(), "2".into()), ("2".into(), "3".into())]
            );

            let filter = PropertyFilter::property("p1")
                .eq("pometry")
                .or(PropertyFilter::property("p2")
                    .eq(6u64)
                    .and(PropertyFilter::property("p3").eq(1u64)));
            let results = search_edges(filter);
            assert_eq!(
                results,
                vec![("2".into(), "1".into()), ("3".into(), "1".into())]
            );

            let filter = EdgeFilter::src()
                .eq("13")
                .and(PropertyFilter::property("p1").eq("prop1"));
            let results = search_edges(filter);
            assert_eq!(results, Vec::<(String, String)>::new());
        }

        #[test]
        fn search_edges_for_dst_eq() {
            let filter = EdgeFilter::dst().eq("2");
            let results = search_edges(filter);
            assert_eq!(
                results,
                vec![("1".into(), "2".into())]
            );
        }

        #[test]
        fn search_edges_for_dst_ne() {
            let filter = EdgeFilter::dst().ne("2");
            let results = search_edges(filter);
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
        fn search_edges_for_dst_in() {
            let filter = EdgeFilter::dst().includes(vec!["2".into()]);
            let results = search_edges(filter);
            assert_eq!(results, vec![("1".into(), "2".into())]);

            let filter = EdgeFilter::dst().includes(vec!["2".into(), "3".into()]);
            let results = search_edges(filter);
            assert_eq!(
                results,
                vec![("1".into(), "2".into()), ("2".into(), "3".into())]
            );
        }

        #[test]
        fn search_edges_for_dst_not_in() {
            let filter = EdgeFilter::dst().excludes(vec!["1".into()]);
            let results = search_edges(filter);
            assert_eq!(
                results,
                vec![("1".into(), "2".into()), ("2".into(), "3".into())]
            );
        }

        #[test]
        fn search_edges_for_src_eq() {
            let filter = EdgeFilter::src().eq("3");
            let results = search_edges(filter);
            assert_eq!(results, vec![("3".into(), "1".into())]);
        }

        #[test]
        fn search_edges_for_src_ne() {
            let filter = EdgeFilter::src().ne("1");
            let results = search_edges(filter);
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
        fn search_edges_for_src_in() {
            let filter = EdgeFilter::src().includes(vec!["1".into()]);
            let results = search_edges(filter);
            assert_eq!(results, vec![("1".into(), "2".into())]);

            let filter = EdgeFilter::src().includes(vec!["1".into(), "2".into()]);
            let results = search_edges(filter);
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
        fn search_edges_for_src_not_in() {
            let filter = EdgeFilter::src().excludes(vec!["1".into()]);
            let results = search_edges(filter);
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
            let filter = PropertyFilter::property("p2").eq(2u64);
            let results = search_edges(filter);
            assert_eq!(results, vec![("2".into(), "3".into())]);
        }

        #[test]
        fn search_edges_for_property_ne() {
            let filter = PropertyFilter::property("p2").ne(2u64);
            let results = search_edges(filter);
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
            let filter = PropertyFilter::property("p2").lt(10u64);
            let results = search_edges(filter);
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
            let filter = PropertyFilter::property("p2").le(6u64);
            let results = search_edges(filter);
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
            let filter = PropertyFilter::property("p2").gt(2u64);
            let results = search_edges(filter);
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
            let filter = PropertyFilter::property("p2").ge(2u64);
            let results = search_edges(filter);
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
            let filter = PropertyFilter::property("p2").includes(vec![Prop::U64(6)]);
            let results = search_edges(filter);
            assert_eq!(
                results,
                vec![("2".into(), "1".into()), ("3".into(), "1".into())]
            );

            let filter = PropertyFilter::property("p2").includes(vec![Prop::U64(2), Prop::U64(6)]);
            let results = search_edges(filter);
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
            let filter = PropertyFilter::property("p2").excludes(vec![Prop::U64(6)]);
            let results = search_edges(filter);
            assert_eq!(
                results,
                vec![("1".into(), "2".into()), ("2".into(), "3".into())]
            );
        }

        #[test]
        fn search_edges_for_property_is_some() {
            let filter = PropertyFilter::property("p2").is_some();
            let results = search_edges(filter);
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
            let filter = EdgeFilter::src().eq("3").and(EdgeFilter::dst().eq("1"));

            let results = search_edges(filter);
            assert_eq!(results, vec![("3".into(), "1".into())]);
        }

        #[test]
        fn test_fuzzy_search() {
            let filter = EdgeFilter::src().fuzzy_search("shiva", 2, false);
            let results = fuzzy_search_edges_by_composite_filter(filter);
            assert_eq!(results, vec![("shivam".into(), "raphtory".into())]);

            let filter = EdgeFilter::dst().fuzzy_search("pomet", 2, false);
            let results = fuzzy_search_edges_by_composite_filter(filter);
            assert_eq!(results, vec![("raphtory".into(), "pometry".into())]);
        }

        #[test]
        fn test_fuzzy_search_prefix_match() {
            let filter = EdgeFilter::dst().fuzzy_search("pome", 2, false);
            let results = fuzzy_search_edges_by_composite_filter(filter);
            assert_eq!(results, Vec::<(String, String)>::new());

            let filter = EdgeFilter::dst().fuzzy_search("pome", 2, true);
            let results = fuzzy_search_edges_by_composite_filter(filter);
            assert_eq!(results, vec![("raphtory".into(), "pometry".into())]);
        }

        #[test]
        fn test_fuzzy_search_property() {
            let filter = PropertyFilter::property("p1").fuzzy_search("tano", 2, false);
            let results = fuzzy_search_edges_by_composite_filter(filter);
            assert_eq!(results, vec![("shivam".into(), "raphtory".into())]);
        }

        #[test]
        fn test_fuzzy_search_property_prefix_match() {
            let filter = PropertyFilter::property("p1").fuzzy_search("charl", 1, false);
            let results = fuzzy_search_edges_by_composite_filter(filter);
            assert_eq!(results, Vec::<(String, String)>::new());

            let filter = PropertyFilter::property("p1").fuzzy_search("charl", 1, true);
            let results = fuzzy_search_edges_by_composite_filter(filter);
            assert_eq!(results, vec![("raphtory".into(), "pometry".into())]);
        }
    }

    #[test]
    #[cfg(feature = "proto")]
    #[ignore = "this test is for experiments with the jira graph"]
    fn load_jira_graph() -> Result<(), GraphError> {
        use crate::db::graph::views::property_filter::NodeFilterOps;
        global_info_logger();
        let graph = Graph::decode("/tmp/graphs/jira").expect("failed to load graph");
        assert!(graph.count_nodes() > 0);

        let now = SystemTime::now();

        let elapsed = now.elapsed().unwrap().as_secs();
        info!("indexing took: {:?}", elapsed);
        graph.create_index().unwrap();

        let filter = NodeFilter::node_name().eq("DEV-1690");
        let issues = graph.search_nodes(filter, 5, 0)?;

        assert!(!issues.is_empty());

        let names = issues.into_iter().map(|v| v.name()).collect::<Vec<_>>();
        info!("names: {:?}", names);

        Ok(())
    }
}
