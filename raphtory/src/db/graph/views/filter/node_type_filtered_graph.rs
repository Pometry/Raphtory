use crate::{
    core::{entities::LayerIds, utils::errors::GraphError},
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            storage::graph::nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
            view::internal::{
                Base, Immutable, InheritCoreOps, InheritEdgeFilterOps, InheritEdgeHistoryFilter,
                InheritLayerOps, InheritListOps, InheritMaterialize, InheritNodeHistoryFilter,
                InheritStorageOps, InheritTimeSemantics, NodeFilterOps, Static,
            },
        },
        graph::views::filter::{internal::InternalNodeFilterOps, NodeTypeFilter},
    },
    prelude::GraphViewOps,
};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct NodeTypeFilteredGraph<G> {
    pub(crate) graph: G,
    pub(crate) node_types_filter: Arc<[bool]>,
}

impl<G> Static for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> Base for NodeTypeFilteredGraph<G> {
    type Base = G;
    #[inline(always)]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<'graph, G: GraphViewOps<'graph>> NodeTypeFilteredGraph<G> {
    pub fn new(graph: G, node_types_filter: Arc<[bool]>) -> Self {
        Self {
            graph,
            node_types_filter,
        }
    }
}

impl InternalNodeFilterOps for NodeTypeFilter {
    type NodeFiltered<'graph, G: GraphViewOps<'graph>> = NodeTypeFilteredGraph<G>;

    fn create_node_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::NodeFiltered<'graph, G>, GraphError> {
        let node_types_filter = graph
            .node_meta()
            .node_type_meta()
            .get_keys()
            .iter()
            .map(|k| self.0.matches(Some(k))) // TODO: _default check
            .collect::<Vec<_>>();
        Ok(NodeTypeFilteredGraph::new(graph, node_types_filter.into()))
    }
}

impl<'graph, G: GraphViewOps<'graph>> Immutable for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreOps for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritEdgeFilterOps for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritListOps for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> NodeFilterOps for NodeTypeFilteredGraph<G> {
    #[inline]
    fn nodes_filtered(&self) -> bool {
        true
    }

    #[inline]
    fn node_list_trusted(&self) -> bool {
        false
    }

    #[inline]
    fn edge_filter_includes_node_filter(&self) -> bool {
        false
    }

    #[inline]
    fn filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.node_types_filter
            .get(node.node_type_id())
            .copied()
            .unwrap_or(false)
            && self.graph.filter_node(node, layer_ids)
    }
}

#[cfg(test)]
mod tests_node_type_filtered_subgraph {
    use crate::{db::graph::views::filter::PropertyRef, prelude::*};

    #[test]
    fn test_type_filtered_subgraph() {
        let graph = Graph::new();
        let edges = vec![
            (1, "A", "B", vec![("p1", 1u64)], None),
            (2, "B", "C", vec![("p1", 2u64)], None),
            (3, "C", "D", vec![("p1", 3u64)], None),
            (4, "D", "E", vec![("p1", 4u64)], None),
        ];

        for (id, src, dst, props, layer) in &edges {
            graph
                .add_edge(*id, src, dst, props.clone(), *layer)
                .unwrap();
        }

        let nodes = vec![
            (1, "A", vec![("p1", 1u64)], Some("water_tribe")),
            (2, "B", vec![("p1", 2u64)], Some("water_tribe")),
            (3, "C", vec![("p1", 1u64)], Some("fire_nation")),
            (4, "D", vec![("p1", 1u64)], Some("air_nomads")),
        ];

        for (id, name, props, layer) in &nodes {
            graph.add_node(*id, name, props.clone(), *layer).unwrap();
        }

        let type_filtered_subgraph = graph
            .subgraph_node_types(vec!["fire_nation", "air_nomads"])
            .window(1, 5);

        assert_eq!(type_filtered_subgraph.nodes(), vec!["C", "D"]);

        assert_eq!(
            type_filtered_subgraph
                .filter_nodes(PropertyFilter::eq(PropertyRef::Property("p1".into()), 1u64))
                .unwrap()
                .nodes(),
            vec!["C", "D"]
        );

        assert!(type_filtered_subgraph
            .filter_edges(PropertyFilter::eq(PropertyRef::Property("p1".into()), 1u64))
            .unwrap()
            .edges()
            .is_empty())
    }

    mod test_filters_node_type_filtered_subgraph {
        use crate::{
            db::api::view::StaticGraphViewOps,
            prelude::{AdditionOps, GraphViewOps, NodeViewOps},
        };

        macro_rules! assert_filter_results {
            ($filter_fn:ident, $filter:expr, $node_types:expr, $expected_results:expr) => {{
                let filter_results = $filter_fn($filter.clone(), $node_types.clone());
                assert_eq!($expected_results, filter_results);
            }};
        }

        macro_rules! assert_filter_results_w {
            ($filter_fn:ident, $filter:expr, $node_types:expr, $window:expr, $expected_results:expr) => {{
                let filter_results = $filter_fn($filter.clone(), $window, $node_types.clone());
                assert_eq!($expected_results, filter_results);
            }};
        }

        macro_rules! assert_filter_results_layers {
            ($filter_fn:ident, $filter:expr, $node_types:expr, $layers:expr, $expected_results:expr) => {{
                let filter_results =
                    $filter_fn($filter.clone(), $node_types.clone(), $layers.clone());
                assert_eq!($expected_results, filter_results);
            }};
        }

        macro_rules! assert_filter_results_layers_w {
            ($filter_fn:ident, $filter:expr, $node_types:expr, $window:expr, $layers:expr, $expected_results:expr) => {{
                let filter_results = $filter_fn(
                    $filter.clone(),
                    $node_types.clone(),
                    $layers.clone(),
                    $window,
                );
                assert_eq!($expected_results, filter_results);
            }};
        }

        #[cfg(feature = "search")]
        macro_rules! assert_search_results {
            ($search_fn:ident, $filter:expr, $node_types:expr, $expected_results:expr) => {{
                let search_results = $search_fn($filter.clone(), $node_types);
                assert_eq!($expected_results, search_results);
            }};
        }

        #[cfg(not(feature = "search"))]
        macro_rules! assert_search_results {
            ($search_fn:ident, $filter:expr, $node_types:expr, $expected_results:expr) => {};
        }

        #[cfg(feature = "search")]
        macro_rules! assert_search_results_layers {
            ($search_fn:ident, $filter:expr, $node_types:expr, $layers:expr, $expected_results:expr) => {{
                let search_results = $search_fn($filter.clone(), $node_types, $layers);
                assert_eq!($expected_results, search_results);
            }};
        }

        #[cfg(not(feature = "search"))]
        macro_rules! assert_search_results_layers {
            ($search_fn:ident, $filter:expr, $node_types:expr, $layers:expr, $expected_results:expr) => {};
        }

        #[cfg(feature = "search")]
        macro_rules! assert_search_results_w {
            ($search_fn:ident, $filter:expr, $node_types:expr, $window:expr, $expected_results:expr) => {{
                let search_results = $search_fn($filter.clone(), $window, $node_types);
                assert_eq!($expected_results, search_results);
            }};
        }

        #[cfg(not(feature = "search"))]
        macro_rules! assert_search_results_w {
            ($search_fn:ident, $filter:expr, $node_types:expr, $window:expr, $expected_results:expr) => {};
        }

        #[cfg(feature = "search")]
        macro_rules! assert_search_results_layers_w {
            ($search_fn:ident, $filter:expr, $node_types:expr, $layers:expr, $window:expr, $expected_results:expr) => {{
                let search_results = $search_fn($filter.clone(), $layers, $window, $node_types);
                assert_eq!($expected_results, search_results);
            }};
        }

        #[cfg(not(feature = "search"))]
        macro_rules! assert_search_results_layers_w {
            ($search_fn:ident, $filter:expr, $node_types:expr, $layers:expr, $window:expr, $expected_results:expr) => {};
        }

        fn get_all_node_types<G: StaticGraphViewOps + AdditionOps>(graph: &G) -> Vec<String> {
            graph
                .nodes()
                .node_type()
                .into_iter()
                .flat_map(|(_, node_type)| node_type)
                .map(|s| s.to_string())
                .collect()
        }

        mod test_nodes_filters_node_type_filtered_subgraph {
            use crate::{
                core::Prop,
                db::{
                    api::{
                        mutation::internal::{InternalAdditionOps, InternalPropertyAdditionOps},
                        view::StaticGraphViewOps,
                    },
                    graph::views::{
                        deletion_graph::PersistentGraph,
                        filter::{AsNodeFilter, PropertyFilterOps},
                    },
                },
                prelude::{
                    AdditionOps, Graph, GraphViewOps, NodeViewOps, PropertyAdditionOps,
                    PropertyFilter, TimeOps,
                },
            };
            use std::ops::Range;

            #[cfg(feature = "search")]
            pub use crate::db::api::view::SearchableGraphOps;
            use crate::{
                db::graph::views::filter::internal::InternalNodeFilterOps,
                prelude::NodePropertyFilterOps,
            };

            fn init_graph<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
                let nodes = vec![
                    (6, "N1", vec![("p1", Prop::U64(2u64))], Some("air_nomad")),
                    (7, "N1", vec![("p1", Prop::U64(1u64))], Some("air_nomad")),
                    (6, "N2", vec![("p1", Prop::U64(1u64))], Some("water_tribe")),
                    (7, "N2", vec![("p1", Prop::U64(2u64))], Some("water_tribe")),
                    (8, "N3", vec![("p1", Prop::U64(1u64))], Some("air_nomad")),
                    (9, "N4", vec![("p1", Prop::U64(1u64))], Some("air_nomad")),
                    (5, "N5", vec![("p1", Prop::U64(1u64))], Some("air_nomad")),
                    (6, "N5", vec![("p1", Prop::U64(2u64))], Some("air_nomad")),
                    (5, "N6", vec![("p1", Prop::U64(1u64))], Some("fire_nation")),
                    (6, "N6", vec![("p1", Prop::U64(1u64))], Some("fire_nation")),
                    (3, "N7", vec![("p1", Prop::U64(1u64))], Some("air_nomad")),
                    (5, "N7", vec![("p1", Prop::U64(1u64))], Some("air_nomad")),
                    (3, "N8", vec![("p1", Prop::U64(1u64))], Some("fire_nation")),
                    (4, "N8", vec![("p1", Prop::U64(2u64))], Some("fire_nation")),
                ];

                // Add nodes to the graph
                for (id, name, props, layer) in &nodes {
                    graph.add_node(*id, name, props.clone(), *layer).unwrap();
                }

                graph
            }

            fn filter_nodes<I: InternalNodeFilterOps>(
                filter: I,
                node_types: Option<Vec<String>>,
            ) -> Vec<String> {
                let graph = init_graph(Graph::new());
                let node_types: Vec<String> =
                    node_types.unwrap_or_else(|| get_all_node_types(&graph));
                filter_nodes_with(filter, graph.subgraph_node_types(node_types))
            }

            fn filter_nodes_w<I: InternalNodeFilterOps>(
                filter: I,
                w: Range<i64>,
                node_types: Option<Vec<String>>,
            ) -> Vec<String> {
                let graph = init_graph(Graph::new());
                let node_types: Vec<String> =
                    node_types.unwrap_or_else(|| get_all_node_types(&graph));
                filter_nodes_with(
                    filter,
                    graph.subgraph_node_types(node_types).window(w.start, w.end),
                )
            }

            fn filter_nodes_pg<I: InternalNodeFilterOps>(
                filter: I,
                node_types: Option<Vec<String>>,
            ) -> Vec<String> {
                let graph = init_graph(PersistentGraph::new());
                let node_types: Vec<String> =
                    node_types.unwrap_or_else(|| get_all_node_types(&graph));
                filter_nodes_with(filter, graph.subgraph_node_types(node_types))
            }

            fn filter_nodes_pg_w<I: InternalNodeFilterOps>(
                filter: I,
                w: Range<i64>,
                node_types: Option<Vec<String>>,
            ) -> Vec<String> {
                let graph = init_graph(PersistentGraph::new());
                let node_types: Vec<String> =
                    node_types.unwrap_or_else(|| get_all_node_types(&graph));
                filter_nodes_with(
                    filter,
                    graph.subgraph_node_types(node_types).window(w.start, w.end),
                )
            }

            #[cfg(feature = "search")]
            mod search_nodes {
                use crate::{
                    prelude::{
                        NodeViewOps, TimeOps,
                    },
                };
                use std::ops::Range;
                use crate::db::graph::views::deletion_graph::PersistentGraph;
                use crate::db::graph::views::filter::node_type_filtered_graph::tests_node_type_filtered_subgraph::test_filters_node_type_filtered_subgraph::test_nodes_filters_node_type_filtered_subgraph::{get_all_node_types, init_graph};
                use crate::db::graph::views::test_helpers::search_nodes_with;
                use crate::prelude::{Graph, GraphViewOps, PropertyFilter};

                pub fn search_nodes(
                    filter: PropertyFilter,
                    node_types: Option<Vec<String>>,
                ) -> Vec<String> {
                    let graph = init_graph(Graph::new());
                    let node_types: Vec<String> =
                        node_types.unwrap_or_else(|| get_all_node_types(&graph));
                    search_nodes_with(filter, graph.subgraph_node_types(node_types))
                }

                pub fn search_nodes_w(
                    filter: PropertyFilter,
                    w: Range<i64>,
                    node_types: Option<Vec<String>>,
                ) -> Vec<String> {
                    let graph = init_graph(Graph::new());
                    let node_types: Vec<String> =
                        node_types.unwrap_or_else(|| get_all_node_types(&graph));
                    search_nodes_with(
                        filter,
                        graph.subgraph_node_types(node_types).window(w.start, w.end),
                    )
                }

                pub fn search_nodes_pg(
                    filter: PropertyFilter,
                    node_types: Option<Vec<String>>,
                ) -> Vec<String> {
                    let graph = init_graph(PersistentGraph::new());
                    let node_types: Vec<String> =
                        node_types.unwrap_or_else(|| get_all_node_types(&graph));
                    search_nodes_with(filter, graph.subgraph_node_types(node_types))
                }

                pub fn search_nodes_pg_w(
                    filter: PropertyFilter,
                    w: Range<i64>,
                    node_types: Option<Vec<String>>,
                ) -> Vec<String> {
                    let graph = init_graph(PersistentGraph::new());
                    let node_types: Vec<String> =
                        node_types.unwrap_or_else(|| get_all_node_types(&graph));
                    search_nodes_with(
                        filter,
                        graph.subgraph_node_types(node_types).window(w.start, w.end),
                    )
                }
            }

            use crate::{db::graph::views::test_helpers::filter_nodes_with, prelude::LayerOps};
            #[cfg(feature = "search")]
            use search_nodes::*;
            use crate::db::graph::views::filter::node_type_filtered_graph::tests_node_type_filtered_subgraph::test_filters_node_type_filtered_subgraph::get_all_node_types;

            #[test]
            fn test_nodes_filters() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3", "N4", "N6", "N7"];
                assert_filter_results!(filter_nodes, filter, None, expected_results);
                assert_search_results!(search_nodes, filter, None, expected_results);

                let node_types: Option<Vec<String>> =
                    Some(vec!["air_nomad".into(), "water_tribe".into()]);
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3", "N4", "N7"];
                assert_filter_results!(filter_nodes, filter, node_types, expected_results);
                assert_search_results!(search_nodes, filter, node_types, expected_results);
            }

            #[test]
            fn test_nodes_filters_w() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3", "N6"];
                assert_filter_results_w!(filter_nodes_w, filter, None, 6..9, expected_results);
                assert_search_results_w!(search_nodes_w, filter, None, 6..9, expected_results);

                let node_types: Option<Vec<String>> =
                    Some(vec!["air_nomad".into(), "water_tribe".into()]);
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3"];
                assert_filter_results_w!(
                    filter_nodes_w,
                    filter,
                    node_types,
                    6..9,
                    expected_results
                );
                assert_search_results_w!(
                    search_nodes_w,
                    filter,
                    node_types,
                    6..9,
                    expected_results
                );
            }

            #[test]
            fn test_nodes_filters_pg() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3", "N4", "N6", "N7"];
                assert_filter_results!(filter_nodes_pg, filter, None, expected_results);
                assert_search_results!(search_nodes_pg, filter, None, expected_results);

                let node_types: Option<Vec<String>> =
                    Some(vec!["air_nomad".into(), "water_tribe".into()]);
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3", "N4", "N7"];
                // PropertyFilteringNotImplemented
                // assert_filter_results!(filter_nodes_pg, filter, node_types, expected_results);
                assert_search_results!(search_nodes_pg, filter, node_types, expected_results);
            }

            #[test]
            fn test_nodes_filters_pg_w() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3", "N6", "N7"];
                assert_filter_results_w!(filter_nodes_pg_w, filter, None, 6..9, expected_results);
                assert_search_results_w!(search_nodes_pg_w, filter, None, 6..9, expected_results);

                let node_types: Option<Vec<String>> =
                    Some(vec!["air_nomad".into(), "water_tribe".into()]);
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3", "N7"];
                // PropertyFilteringNotImplemented
                // assert_filter_results_w!(filter_nodes_pg_w, filter, node_types, 6..9, expected_results);
                assert_search_results_w!(
                    search_nodes_pg_w,
                    filter,
                    node_types,
                    6..9,
                    expected_results
                );
            }
        }

        mod test_edges_filters_node_type_filtered_subgraph {
            use crate::{
                core::Prop,
                db::{
                    api::{
                        mutation::internal::{InternalAdditionOps, InternalPropertyAdditionOps},
                        view::StaticGraphViewOps,
                    },
                    graph::views::{
                        deletion_graph::PersistentGraph,
                        filter::{
                            internal::InternalEdgeFilterOps, AsEdgeFilter, PropertyFilterOps,
                        },
                    },
                },
                prelude::{
                    AdditionOps, EdgePropertyFilterOps, EdgeViewOps, Graph, GraphViewOps,
                    NodeViewOps, PropertyAdditionOps, PropertyFilter, TimeOps, NO_PROPS,
                },
            };
            use std::ops::Range;

            #[cfg(feature = "search")]
            pub use crate::db::api::view::SearchableGraphOps;

            fn init_graph<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
                let edges = vec![
                    (
                        6,
                        "N1",
                        "N2",
                        vec![("p1", Prop::U64(2u64))],
                        Some("fire_nation"),
                    ),
                    (7, "N1", "N2", vec![("p1", Prop::U64(1u64))], None),
                    (
                        6,
                        "N2",
                        "N3",
                        vec![("p1", Prop::U64(1u64))],
                        Some("water_tribe"),
                    ),
                    (
                        7,
                        "N2",
                        "N3",
                        vec![("p1", Prop::U64(2u64))],
                        Some("water_tribe"),
                    ),
                    (
                        8,
                        "N3",
                        "N4",
                        vec![("p1", Prop::U64(1u64))],
                        Some("fire_nation"),
                    ),
                    (9, "N4", "N5", vec![("p1", Prop::U64(1u64))], None),
                    (
                        5,
                        "N5",
                        "N6",
                        vec![("p1", Prop::U64(1u64))],
                        Some("air_nomad"),
                    ),
                    (6, "N5", "N6", vec![("p1", Prop::U64(2u64))], None),
                    (
                        5,
                        "N6",
                        "N7",
                        vec![("p1", Prop::U64(1u64))],
                        Some("fire_nation"),
                    ),
                    (
                        6,
                        "N6",
                        "N7",
                        vec![("p1", Prop::U64(1u64))],
                        Some("fire_nation"),
                    ),
                    (
                        3,
                        "N7",
                        "N8",
                        vec![("p1", Prop::U64(1u64))],
                        Some("fire_nation"),
                    ),
                    (5, "N7", "N8", vec![("p1", Prop::U64(1u64))], None),
                    (
                        3,
                        "N8",
                        "N1",
                        vec![("p1", Prop::U64(1u64))],
                        Some("air_nomad"),
                    ),
                    (
                        4,
                        "N8",
                        "N1",
                        vec![("p1", Prop::U64(2u64))],
                        Some("water_tribe"),
                    ),
                ];

                for (id, src, dst, props, layer) in &edges {
                    graph
                        .add_edge(*id, src, dst, props.clone(), *layer)
                        .unwrap();
                }

                let nodes = vec![
                    (6, "N1", NO_PROPS, Some("air_nomad")),
                    (6, "N2", NO_PROPS, Some("water_tribe")),
                    (8, "N3", NO_PROPS, Some("air_nomad")),
                    (9, "N4", NO_PROPS, Some("air_nomad")),
                    (5, "N5", NO_PROPS, Some("air_nomad")),
                    (5, "N6", NO_PROPS, Some("fire_nation")),
                    (3, "N7", NO_PROPS, Some("air_nomad")),
                    (4, "N8", NO_PROPS, Some("fire_nation")),
                ];

                for (id, name, props, layer) in &nodes {
                    graph.add_node(*id, name, props.clone(), *layer).unwrap();
                }

                graph
            }

            fn filter_edges<I: InternalEdgeFilterOps>(
                filter: I,
                node_types: Option<Vec<String>>,
            ) -> Vec<String> {
                let graph = init_graph(Graph::new());
                let node_types: Vec<String> =
                    node_types.unwrap_or_else(|| get_all_node_types(&graph));
                filter_edges_with(filter, graph.subgraph_node_types(node_types))
            }

            fn filter_edges_layers<I: InternalEdgeFilterOps>(
                filter: I,
                node_types: Option<Vec<String>>,
                layers: Vec<&str>,
            ) -> Vec<String> {
                let graph = init_graph(Graph::new());
                let node_types: Vec<String> =
                    node_types.unwrap_or_else(|| get_all_node_types(&graph));
                let graph = graph
                    .subgraph_node_types(node_types)
                    .layers(layers)
                    .unwrap();
                filter_edges_with(filter, graph)
            }

            fn filter_edges_w<I: InternalEdgeFilterOps>(
                filter: I,
                w: Range<i64>,
                node_types: Option<Vec<String>>,
            ) -> Vec<String> {
                let graph = init_graph(Graph::new());
                let node_types: Vec<String> =
                    node_types.unwrap_or_else(|| get_all_node_types(&graph));
                filter_edges_with(
                    filter,
                    graph.subgraph_node_types(node_types).window(w.start, w.end),
                )
            }

            fn filter_edges_layers_w<I: InternalEdgeFilterOps>(
                filter: I,
                node_types: Option<Vec<String>>,
                layers: Vec<&str>,
                w: Range<i64>,
            ) -> Vec<String> {
                let graph = init_graph(Graph::new());
                let node_types: Vec<String> =
                    node_types.unwrap_or_else(|| get_all_node_types(&graph));
                let graph = graph
                    .subgraph_node_types(node_types)
                    .layers(layers)
                    .unwrap()
                    .window(w.start, w.end);
                filter_edges_with(filter, graph)
            }

            #[allow(dead_code)]
            fn filter_edges_pg<I: InternalEdgeFilterOps>(
                filter: I,
                node_types: Option<Vec<String>>,
            ) -> Vec<String> {
                let graph = init_graph(PersistentGraph::new());
                let node_types: Vec<String> =
                    node_types.unwrap_or_else(|| get_all_node_types(&graph));
                filter_edges_with(filter, graph.subgraph_node_types(node_types))
            }

            #[allow(dead_code)]
            fn filter_edges_pg_w<I: InternalEdgeFilterOps>(
                filter: I,
                w: Range<i64>,
                node_types: Option<Vec<String>>,
            ) -> Vec<String> {
                let graph = init_graph(PersistentGraph::new());
                let node_types: Vec<String> =
                    node_types.unwrap_or_else(|| get_all_node_types(&graph));
                filter_edges_with(
                    filter,
                    graph.subgraph_node_types(node_types).window(w.start, w.end),
                )
            }

            #[cfg(feature = "search")]
            mod search_edges {
                use std::ops::Range;
                use crate::db::graph::views::deletion_graph::PersistentGraph;
                use crate::db::graph::views::filter::node_type_filtered_graph::tests_node_type_filtered_subgraph::test_filters_node_type_filtered_subgraph::test_edges_filters_node_type_filtered_subgraph::{get_all_node_types, init_graph};
                use crate::db::graph::views::test_helpers::search_edges_with;
                use crate::prelude::{EdgeViewOps, Graph, GraphViewOps, LayerOps, NodeViewOps, PropertyFilter, TimeOps};

                pub fn search_edges(
                    filter: PropertyFilter,
                    node_types: Option<Vec<String>>,
                ) -> Vec<String> {
                    let graph = init_graph(Graph::new());
                    let node_types: Vec<String> =
                        node_types.unwrap_or_else(|| get_all_node_types(&graph));
                    search_edges_with(filter, graph.subgraph_node_types(node_types))
                }

                pub fn search_edges_w(
                    filter: PropertyFilter,
                    w: Range<i64>,
                    node_types: Option<Vec<String>>,
                ) -> Vec<String> {
                    let graph = init_graph(Graph::new());
                    let node_types: Vec<String> =
                        node_types.unwrap_or_else(|| get_all_node_types(&graph));
                    search_edges_with(
                        filter,
                        graph.subgraph_node_types(node_types).window(w.start, w.end),
                    )
                }

                pub fn search_edges_layers(
                    filter: PropertyFilter,
                    node_types: Option<Vec<String>>,
                    layers: Vec<&str>,
                ) -> Vec<String> {
                    let graph = init_graph(Graph::new());
                    let node_types: Vec<String> =
                        node_types.unwrap_or_else(|| get_all_node_types(&graph));
                    let graph = graph
                        .subgraph_node_types(node_types)
                        .layers(layers)
                        .unwrap();
                    search_edges_with(filter, graph)
                }

                pub fn search_edges_layers_w(
                    filter: PropertyFilter,
                    layers: Vec<&str>,
                    w: Range<i64>,
                    node_types: Option<Vec<String>>,
                ) -> Vec<String> {
                    let graph = init_graph(Graph::new());
                    let node_types: Vec<String> =
                        node_types.unwrap_or_else(|| get_all_node_types(&graph));
                    let graph = graph
                        .subgraph_node_types(node_types)
                        .layers(layers)
                        .unwrap()
                        .window(w.start, w.end);
                    search_edges_with(filter, graph)
                }

                pub fn search_edges_pg(
                    filter: PropertyFilter,
                    node_types: Option<Vec<String>>,
                ) -> Vec<String> {
                    let graph = init_graph(PersistentGraph::new());
                    let node_types: Vec<String> =
                        node_types.unwrap_or_else(|| get_all_node_types(&graph));
                    search_edges_with(filter, graph.subgraph_node_types(node_types))
                }

                pub fn search_edges_pg_w(
                    filter: PropertyFilter,
                    w: Range<i64>,
                    node_types: Option<Vec<String>>,
                ) -> Vec<String> {
                    let graph = init_graph(PersistentGraph::new());
                    let node_types: Vec<String> =
                        node_types.unwrap_or_else(|| get_all_node_types(&graph));
                    search_edges_with(
                        filter,
                        graph.subgraph_node_types(node_types).window(w.start, w.end),
                    )
                }

                pub fn search_edges_pg_layers(
                    filter: PropertyFilter,
                    node_types: Option<Vec<String>>,
                    layers: Vec<&str>,
                ) -> Vec<String> {
                    let graph = init_graph(PersistentGraph::new());
                    let node_types: Vec<String> =
                        node_types.unwrap_or_else(|| get_all_node_types(&graph));
                    let graph = graph
                        .subgraph_node_types(node_types)
                        .layers(layers)
                        .unwrap();
                    search_edges_with(filter, graph)
                }

                pub fn search_edges_pg_layers_w(
                    filter: PropertyFilter,
                    layers: Vec<&str>,
                    w: Range<i64>,
                    node_types: Option<Vec<String>>,
                ) -> Vec<String> {
                    let graph = init_graph(PersistentGraph::new());
                    let node_types: Vec<String> =
                        node_types.unwrap_or_else(|| get_all_node_types(&graph));
                    let graph = graph
                        .subgraph_node_types(node_types)
                        .layers(layers)
                        .unwrap()
                        .window(w.start, w.end);
                    search_edges_with(filter, graph)
                }
            }

            use crate::{db::graph::views::test_helpers::filter_edges_with, prelude::LayerOps};
            #[cfg(feature = "search")]
            use search_edges::*;
            use crate::db::graph::views::filter::node_type_filtered_graph::tests_node_type_filtered_subgraph::test_filters_node_type_filtered_subgraph::get_all_node_types;

            #[test]
            fn test_edges_filters() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4", "N4->N5", "N6->N7", "N7->N8"];
                assert_filter_results!(filter_edges, filter, None, expected_results);
                assert_search_results!(search_edges, filter, None, expected_results);

                let node_types: Option<Vec<String>> =
                    Some(vec!["air_nomad".into(), "water_tribe".into()]);
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4", "N4->N5"];
                assert_filter_results!(filter_edges, filter, node_types.clone(), expected_results);
                assert_search_results!(search_edges, filter, node_types.clone(), expected_results);

                let layers = vec!["fire_nation"];
                let expected_results = vec!["N3->N4"];
                assert_filter_results_layers!(
                    filter_edges_layers,
                    filter,
                    node_types,
                    layers,
                    expected_results
                );
                assert_search_results_layers!(
                    search_edges_layers,
                    filter,
                    node_types,
                    layers,
                    expected_results
                );
            }

            #[test]
            fn test_edges_filters_w() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4", "N6->N7"];
                assert_filter_results_w!(filter_edges_w, filter, None, 6..9, expected_results);
                assert_search_results_w!(search_edges_w, filter, None, 6..9, expected_results);

                let node_types: Option<Vec<String>> =
                    Some(vec!["air_nomad".into(), "water_tribe".into()]);
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4"];
                assert_filter_results_w!(
                    filter_edges_w,
                    filter,
                    node_types.clone(),
                    6..9,
                    expected_results
                );
                assert_search_results_w!(
                    search_edges_w,
                    filter,
                    node_types.clone(),
                    6..9,
                    expected_results
                );

                let layers = vec!["fire_nation"];
                let expected_results = vec!["N3->N4"];
                assert_filter_results_layers_w!(
                    filter_edges_layers_w,
                    filter,
                    node_types,
                    6..9,
                    layers,
                    expected_results
                );
                assert_search_results_layers_w!(
                    search_edges_layers_w,
                    filter,
                    node_types,
                    layers,
                    6..9,
                    expected_results
                )
            }

            #[test]
            fn test_edges_filters_pg() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4", "N4->N5", "N6->N7", "N7->N8"];
                // PropertyFilteringNotImplemented
                // assert_filter_results!(filter_edges_pg, filter, None, expected_results);
                assert_search_results!(search_edges_pg, filter, None, expected_results);

                let node_types: Option<Vec<String>> =
                    Some(vec!["air_nomad".into(), "water_tribe".into()]);
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4", "N4->N5"];
                // PropertyFilteringNotImplemented
                // assert_filter_results!(filter_edges_pg, filter, node_types, expected_results);
                assert_search_results!(
                    search_edges_pg,
                    filter,
                    node_types.clone(),
                    expected_results
                );

                let layers = vec!["fire_nation"];
                let expected_results = vec!["N3->N4"];
                assert_search_results_layers!(
                    search_edges_pg_layers,
                    filter,
                    node_types,
                    layers,
                    expected_results
                )
            }

            #[test]
            fn test_edges_filters_pg_w() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4", "N6->N7", "N7->N8"];
                // PropertyFilteringNotImplemented
                // assert_filter_results_w!(filter_edges_pg_w, filter, None, 6..9, expected_results);
                assert_search_results_w!(search_edges_pg_w, filter, None, 6..9, expected_results);

                let node_types: Option<Vec<String>> =
                    Some(vec!["air_nomad".into(), "water_tribe".into()]);
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4"];
                // PropertyFilteringNotImplemented
                // assert_filter_results_w!(filter_edges_pg_w, filter, node_types, 6..9, expected_results);
                assert_search_results_w!(
                    search_edges_pg_w,
                    filter,
                    node_types.clone(),
                    6..9,
                    expected_results
                );

                let layers = vec!["fire_nation"];
                let expected_results = vec!["N3->N4"];
                assert_search_results_layers_w!(
                    search_edges_pg_layers_w,
                    filter,
                    node_types,
                    layers,
                    6..9,
                    expected_results
                )
            }
        }
    }
}
