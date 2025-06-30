use crate::{
    core::entities::LayerIds,
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            view::internal::{
                Immutable, InheritAllEdgeFilterOps, InheritEdgeHistoryFilter, InheritLayerOps,
                InheritListOps, InheritMaterialize, InheritNodeHistoryFilter, InheritStorageOps,
                InheritTimeSemantics, InternalNodeFilterOps, Static,
            },
        },
        graph::views::filter::{internal::CreateNodeFilter, NodeTypeFilter},
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::inherit::Base;
use raphtory_storage::{
    core_ops::InheritCoreGraphOps,
    graph::nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
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

impl CreateNodeFilter for NodeTypeFilter {
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

impl<'graph, G: GraphViewOps<'graph>> InheritCoreGraphOps for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritAllEdgeFilterOps for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritListOps for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InternalNodeFilterOps for NodeTypeFilteredGraph<G> {
    #[inline]
    fn internal_nodes_filtered(&self) -> bool {
        true
    }

    #[inline]
    fn internal_node_list_trusted(&self) -> bool {
        false
    }

    #[inline]
    fn edge_filter_includes_node_filter(&self) -> bool {
        false
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.node_types_filter
            .get(node.node_type_id())
            .copied()
            .unwrap_or(false)
            && self.graph.internal_filter_node(node, layer_ids)
    }
}

#[cfg(test)]
mod tests_node_type_filtered_subgraph {
    use crate::{
        db::graph::{
            graph::assert_graph_equal,
            views::filter::model::property_filter::{PropertyFilter, PropertyRef},
        },
        prelude::*,
        test_utils::{build_graph, build_graph_strat, make_node_types},
    };
    use proptest::{arbitrary::any, proptest};
    use raphtory_storage::mutation::addition_ops::InternalAdditionOps;
    use std::ops::Range;

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

    #[test]
    fn materialize_prop_test() {
        proptest!(|(graph_f in build_graph_strat(10, 10, true), node_types in make_node_types())| {
            let g = Graph::from(build_graph(&graph_f)).subgraph_node_types(node_types);
            let gm = g.materialize().unwrap();
            assert_graph_equal(&g, &gm);
        })
    }

    #[test]
    fn materialize_type_window_prop_test() {
        proptest!(|(graph_f in build_graph_strat(10, 10, true), w in any::<Range<i64>>(), node_types in make_node_types())| {
            let g = Graph::from(build_graph(&graph_f)).subgraph_node_types(node_types);
            let gvw = g.window(w.start, w.end);
            let gmw = gvw.materialize().unwrap();
            assert_graph_equal(&gvw, &gmw);
        })
    }

    #[test]
    fn materialize_window_type_prop_test() {
        proptest!(|(graph_f in build_graph_strat(10, 10, true), w in any::<Range<i64>>(), node_types in make_node_types())| {
            let g = Graph::from(build_graph(&graph_f));
            let gvw = g.window(w.start, w.end).subgraph_node_types(node_types);
            let gmw = gvw.materialize().unwrap();
            assert_graph_equal(&gvw, &gmw);
        })
    }

    #[test]
    fn node_removed_via_edge_removal() {
        let g = Graph::new();
        g.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
        g.node(1).unwrap().set_node_type("test").unwrap();
        let expected = Graph::new();
        expected.resolve_layer(None).unwrap();
        assert_graph_equal(&g.subgraph_node_types(["test"]), &expected);
    }

    #[test]
    fn node_removed_via_edge_removal_window() {
        let g = Graph::new();
        g.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
        g.node(0).unwrap().set_node_type("two").unwrap();
        let gw = g.window(0, 1);
        let expected = Graph::new();
        expected.resolve_layer(None).unwrap();
        let sg = gw.subgraph_node_types(["_default"]);
        assert!(!sg.has_node(0));
        assert!(!sg.has_node(1));
        assert_graph_equal(&sg, &expected);
        assert_graph_equal(&sg, &sg.materialize().unwrap())
    }
    mod test_filters_node_type_filtered_subgraph {
        use crate::{
            db::{
                api::view::StaticGraphViewOps,
                graph::{
                    assertions::GraphTransformer,
                    views::{
                        filter::node_type_filtered_graph::NodeTypeFilteredGraph,
                        layer_graph::LayeredGraph, window_graph::WindowedGraph,
                    },
                },
            },
            prelude::{GraphViewOps, LayerOps, NodeViewOps, TimeOps},
        };
        use std::ops::Range;

        fn get_all_node_types<G: StaticGraphViewOps>(graph: &G) -> Vec<String> {
            graph
                .nodes()
                .node_type()
                .into_iter()
                .flat_map(|(_, node_type)| node_type)
                .map(|s| s.to_string())
                .collect()
        }

        struct NodeTypeGraphTransformer(Option<Vec<String>>);

        impl GraphTransformer for NodeTypeGraphTransformer {
            type Return<G: StaticGraphViewOps> = NodeTypeFilteredGraph<G>;
            fn apply<G: StaticGraphViewOps>(&self, graph: G) -> Self::Return<G> {
                let node_types: Vec<String> =
                    self.0.clone().unwrap_or_else(|| get_all_node_types(&graph));
                graph.subgraph_node_types(node_types)
            }
        }

        struct WindowedNodeTypeGraphTransformer(Option<Vec<String>>, Range<i64>);

        impl GraphTransformer for WindowedNodeTypeGraphTransformer {
            type Return<G: StaticGraphViewOps> = WindowedGraph<NodeTypeFilteredGraph<G>>;
            fn apply<G: StaticGraphViewOps>(&self, graph: G) -> Self::Return<G> {
                let node_types: Vec<String> =
                    self.0.clone().unwrap_or_else(|| get_all_node_types(&graph));
                graph
                    .subgraph_node_types(node_types)
                    .window(self.1.start, self.1.end)
            }
        }

        struct LayeredNodeTypeGraphTransformer(Option<Vec<String>>, Vec<String>);

        impl GraphTransformer for LayeredNodeTypeGraphTransformer {
            type Return<G: StaticGraphViewOps> = LayeredGraph<NodeTypeFilteredGraph<G>>;
            fn apply<G: StaticGraphViewOps>(&self, graph: G) -> Self::Return<G> {
                let node_types: Vec<String> =
                    self.0.clone().unwrap_or_else(|| get_all_node_types(&graph));
                graph
                    .subgraph_node_types(node_types)
                    .layers(self.1.clone())
                    .unwrap()
            }
        }

        struct LayeredWindowedNodeTypeGraphTransformer(
            Option<Vec<String>>,
            Range<i64>,
            Vec<String>,
        );

        impl GraphTransformer for LayeredWindowedNodeTypeGraphTransformer {
            type Return<G: StaticGraphViewOps> =
                WindowedGraph<LayeredGraph<NodeTypeFilteredGraph<G>>>;
            fn apply<G: StaticGraphViewOps>(&self, graph: G) -> Self::Return<G> {
                let node_types: Vec<String> =
                    self.0.clone().unwrap_or_else(|| get_all_node_types(&graph));
                graph
                    .subgraph_node_types(node_types)
                    .layers(self.2.clone())
                    .unwrap()
                    .window(self.1.start, self.1.end)
            }
        }

        mod test_nodes_filters_node_type_filtered_subgraph {
            use crate::{
                db::{
                    api::view::StaticGraphViewOps, graph::views::filter::model::PropertyFilterOps,
                },
                prelude::AdditionOps,
            };
            use raphtory_api::core::entities::properties::prop::Prop;

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

            use crate::db::graph::assertions::{
                assert_filter_nodes_results, assert_search_nodes_results, TestGraphVariants,
                TestVariants,
            };

            use crate::db::graph::views::filter::model::property_filter::PropertyFilter;
            use crate::db::graph::views::filter::node_type_filtered_graph::tests_node_type_filtered_subgraph::test_filters_node_type_filtered_subgraph::{NodeTypeGraphTransformer, WindowedNodeTypeGraphTransformer};

            #[test]
            fn test_nodes_filters() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3", "N4", "N6", "N7"];
                assert_filter_nodes_results(
                    init_graph,
                    NodeTypeGraphTransformer(None),
                    filter.clone(),
                    &expected_results,
                    TestVariants::All,
                );
                assert_search_nodes_results(
                    init_graph,
                    NodeTypeGraphTransformer(None),
                    filter,
                    &expected_results,
                    TestVariants::All,
                );

                let node_types: Option<Vec<String>> =
                    Some(vec!["air_nomad".into(), "water_tribe".into()]);
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3", "N4", "N7"];
                assert_filter_nodes_results(
                    init_graph,
                    NodeTypeGraphTransformer(node_types.clone()),
                    filter.clone(),
                    &expected_results,
                    TestVariants::All,
                );
                assert_search_nodes_results(
                    init_graph,
                    NodeTypeGraphTransformer(node_types),
                    filter,
                    &expected_results,
                    TestVariants::All,
                );
            }

            #[test]
            fn test_nodes_filters_w() {
                // TODO: Enable event_disk_graph for filter_nodes once bug fixed: https://github.com/Pometry/Raphtory/issues/2098
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3", "N6"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowedNodeTypeGraphTransformer(None, 6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowedNodeTypeGraphTransformer(None, 6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let node_types: Option<Vec<String>> =
                    Some(vec!["air_nomad".into(), "water_tribe".into()]);
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowedNodeTypeGraphTransformer(node_types.clone(), 6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowedNodeTypeGraphTransformer(node_types, 6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );
            }

            #[test]
            fn test_nodes_filters_pg_w() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3", "N6", "N7"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowedNodeTypeGraphTransformer(None, 6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowedNodeTypeGraphTransformer(None, 6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let node_types: Option<Vec<String>> =
                    Some(vec!["air_nomad".into(), "water_tribe".into()]);
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3", "N7"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowedNodeTypeGraphTransformer(node_types.clone(), 6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowedNodeTypeGraphTransformer(node_types, 6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
            }
        }

        mod test_edges_filters_node_type_filtered_subgraph {
            use crate::{
                db::{
                    api::view::StaticGraphViewOps, graph::views::filter::model::PropertyFilterOps,
                },
                prelude::{AdditionOps, NO_PROPS},
            };
            use raphtory_api::core::entities::properties::prop::Prop;

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

            use crate::db::graph::assertions::{assert_filter_edges_results, assert_search_edges_results, TestVariants};
            use crate::db::graph::views::filter::model::property_filter::PropertyFilter;
            use crate::db::graph::views::filter::node_type_filtered_graph::tests_node_type_filtered_subgraph::test_filters_node_type_filtered_subgraph::{ LayeredNodeTypeGraphTransformer, LayeredWindowedNodeTypeGraphTransformer, NodeTypeGraphTransformer, WindowedNodeTypeGraphTransformer};

            #[test]
            fn test_edges_filters() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4", "N4->N5", "N6->N7", "N7->N8"];
                assert_filter_edges_results(
                    init_graph,
                    NodeTypeGraphTransformer(None),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_edges_results(
                    init_graph,
                    NodeTypeGraphTransformer(None),
                    filter,
                    &expected_results,
                    TestVariants::All,
                );

                let node_types: Option<Vec<String>> =
                    Some(vec!["air_nomad".into(), "water_tribe".into()]);
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4", "N4->N5"];
                assert_filter_edges_results(
                    init_graph,
                    NodeTypeGraphTransformer(node_types.clone()),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_edges_results(
                    init_graph,
                    NodeTypeGraphTransformer(node_types.clone()),
                    filter.clone(),
                    &expected_results,
                    TestVariants::All,
                );

                let layers = vec!["fire_nation".to_string()];
                let expected_results = vec!["N3->N4"];
                assert_filter_edges_results(
                    init_graph,
                    LayeredNodeTypeGraphTransformer(node_types.clone(), layers.clone()),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_edges_results(
                    init_graph,
                    LayeredNodeTypeGraphTransformer(node_types.clone(), layers),
                    filter,
                    &expected_results,
                    TestVariants::All,
                );
            }

            #[test]
            fn test_edges_filters_w() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4", "N6->N7"];
                assert_filter_edges_results(
                    init_graph,
                    WindowedNodeTypeGraphTransformer(None, 6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_edges_results(
                    init_graph,
                    WindowedNodeTypeGraphTransformer(None, 6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let node_types: Option<Vec<String>> =
                    Some(vec!["air_nomad".into(), "water_tribe".into()]);
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4"];
                assert_filter_edges_results(
                    init_graph,
                    WindowedNodeTypeGraphTransformer(node_types.clone(), 6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_edges_results(
                    init_graph,
                    WindowedNodeTypeGraphTransformer(node_types.clone(), 6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let layers = vec!["fire_nation".to_string()];
                let expected_results = vec!["N3->N4"];
                assert_filter_edges_results(
                    init_graph,
                    LayeredWindowedNodeTypeGraphTransformer(
                        node_types.clone(),
                        6..9,
                        layers.clone(),
                    ),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_edges_results(
                    init_graph,
                    LayeredWindowedNodeTypeGraphTransformer(node_types.clone(), 6..9, layers),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
            }

            #[test]
            fn test_edges_filters_pg_w() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4", "N6->N7", "N7->N8"];
                assert_filter_edges_results(
                    init_graph,
                    WindowedNodeTypeGraphTransformer(None, 6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowedNodeTypeGraphTransformer(None, 6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let node_types: Option<Vec<String>> =
                    Some(vec!["air_nomad".into(), "water_tribe".into()]);
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4"];
                assert_filter_edges_results(
                    init_graph,
                    WindowedNodeTypeGraphTransformer(node_types.clone(), 6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    WindowedNodeTypeGraphTransformer(node_types.clone(), 6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let layers = vec!["fire_nation".to_string()];
                let expected_results = vec!["N3->N4"];
                assert_filter_edges_results(
                    init_graph,
                    LayeredWindowedNodeTypeGraphTransformer(
                        node_types.clone(),
                        6..9,
                        layers.clone(),
                    ),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    LayeredWindowedNodeTypeGraphTransformer(node_types.clone(), 6..9, layers),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
            }
        }
    }
}
