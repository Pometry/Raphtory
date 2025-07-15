use crate::{
    core::entities::{nodes::node_ref::AsNodeRef, LayerIds, VID},
    db::api::{
        properties::internal::InheritPropertiesOps,
        state::Index,
        view::internal::{
            EdgeList, GraphTimeSemanticsOps, Immutable, InheritEdgeHistoryFilter, InheritLayerOps,
            InheritMaterialize, InheritNodeHistoryFilter, InheritStorageOps, InheritTimeSemantics,
            InternalEdgeFilterOps, InternalEdgeLayerFilterOps, InternalExplodedEdgeFilterOps,
            InternalNodeFilterOps, ListOps, NodeList, NodeTimeSemanticsOps, Static,
        },
    },
    prelude::GraphViewOps,
};
use raphtory_api::{
    core::{entities::ELID, storage::timeindex::TimeIndexEntry},
    inherit::Base,
};
use raphtory_storage::{
    core_ops::{CoreGraphOps, InheritCoreGraphOps},
    graph::{
        edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
        nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
    },
};
use std::fmt::{Debug, Formatter};

#[derive(Clone)]
pub struct NodeSubgraph<G> {
    pub(crate) graph: G,
    pub(crate) nodes: Index<VID>,
}

impl<G> Static for NodeSubgraph<G> {}

impl<G: Debug> Debug for NodeSubgraph<G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeSubgraph")
            .field("graph", &self.graph as &dyn Debug)
            .field("nodes", &self.nodes)
            .finish()
    }
}

impl<'graph, G: GraphViewOps<'graph>> Base for NodeSubgraph<G> {
    type Base = G;
    #[inline(always)]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<'graph, G: GraphViewOps<'graph>> Immutable for NodeSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreGraphOps for NodeSubgraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for NodeSubgraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for NodeSubgraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for NodeSubgraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for NodeSubgraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for NodeSubgraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for NodeSubgraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for NodeSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> NodeSubgraph<G> {
    pub fn new(graph: G, nodes: impl IntoIterator<Item = impl AsNodeRef>) -> Self {
        let nodes: Index<_> = nodes
            .into_iter()
            .filter_map(|v| (&&graph).node(v).map(|n| n.node))
            .collect();
        let filter = NodeSubgraph {
            graph: &graph,
            nodes: nodes.clone(),
        };
        let time_semantics = filter.node_time_semantics();
        let nodes: Index<_> = nodes
            .into_iter()
            .filter(|vid| time_semantics.node_valid(filter.core_node(*vid).as_ref(), &filter)) // need to bypass higher-level optimisation as it assumes the node list is already filtered
            .collect();
        Self { graph, nodes }
    }
}

impl<'graph, G: GraphViewOps<'graph>> InternalExplodedEdgeFilterOps for NodeSubgraph<G> {
    fn internal_exploded_edge_filtered(&self) -> bool {
        self.graph.internal_exploded_edge_filtered()
    }

    fn internal_exploded_filter_edge_list_trusted(&self) -> bool {
        false
    }

    fn internal_filter_exploded_edge(
        &self,
        eid: ELID,
        t: TimeIndexEntry,
        layer_ids: &LayerIds,
    ) -> bool {
        self.graph.internal_filter_exploded_edge(eid, t, layer_ids)
    }

    fn node_filter_includes_exploded_edge_filter(&self) -> bool {
        true
    }

    fn edge_filter_includes_exploded_edge_filter(&self) -> bool {
        self.graph.edge_filter_includes_exploded_edge_filter()
    }

    fn edge_layer_filter_includes_exploded_edge_filter(&self) -> bool {
        self.graph.edge_layer_filter_includes_exploded_edge_filter()
    }
}

impl<'graph, G: GraphViewOps<'graph>> InternalEdgeLayerFilterOps for NodeSubgraph<G> {
    fn internal_edge_layer_filtered(&self) -> bool {
        self.graph.internal_edge_layer_filtered()
    }

    fn internal_layer_filter_edge_list_trusted(&self) -> bool {
        false
    }

    fn internal_filter_edge_layer(&self, edge: EdgeStorageRef, layer: usize) -> bool {
        self.graph.internal_filter_edge_layer(edge, layer)
    }

    fn node_filter_includes_edge_layer_filter(&self) -> bool {
        true
    }

    fn edge_filter_includes_edge_layer_filter(&self) -> bool {
        self.graph.edge_filter_includes_edge_layer_filter()
    }

    fn exploded_edge_filter_includes_edge_layer_filter(&self) -> bool {
        self.graph.exploded_edge_filter_includes_edge_layer_filter()
    }
}

impl<'graph, G: GraphViewOps<'graph>> InternalEdgeFilterOps for NodeSubgraph<G> {
    #[inline]
    fn internal_edge_filtered(&self) -> bool {
        self.graph.internal_edge_filtered()
    }

    #[inline]
    fn internal_edge_list_trusted(&self) -> bool {
        false
    }

    fn node_filter_includes_edge_filter(&self) -> bool {
        true
    }

    #[inline]
    fn internal_filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool {
        self.nodes.contains(&edge.src())
            && self.nodes.contains(&edge.dst())
            && self.graph.internal_filter_edge(edge, layer_ids)
    }
}

impl<'graph, G: GraphViewOps<'graph>> InternalNodeFilterOps for NodeSubgraph<G> {
    fn internal_nodes_filtered(&self) -> bool {
        true
    }
    fn internal_node_list_trusted(&self) -> bool {
        true
    }

    fn edge_layer_filter_includes_node_filter(&self) -> bool {
        self.graph.edge_layer_filter_includes_node_filter()
    }

    fn exploded_edge_filter_includes_node_filter(&self) -> bool {
        self.graph.exploded_edge_filter_includes_node_filter()
    }

    #[inline]
    fn edge_filter_includes_node_filter(&self) -> bool {
        true
    }
    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.graph.internal_filter_node(node, layer_ids) && self.nodes.contains(&node.vid())
    }
}

impl<'graph, G: GraphViewOps<'graph>> ListOps for NodeSubgraph<G> {
    fn node_list(&self) -> NodeList {
        NodeList::List {
            elems: self.nodes.clone(),
        }
    }

    fn edge_list(&self) -> EdgeList {
        self.graph.edge_list()
    }
}

#[cfg(test)]
mod subgraph_tests {
    use crate::{
        algorithms::{
            components::weakly_connected_components, motifs::triangle_count::triangle_count,
        },
        db::graph::{graph::assert_graph_equal, views::deletion_graph::PersistentGraph},
        prelude::*,
        test_storage,
        test_utils::{build_graph, build_graph_strat},
    };
    use ahash::HashSet;
    use itertools::Itertools;
    use proptest::{proptest, sample::subsequence};
    use raphtory_storage::mutation::addition_ops::InternalAdditionOps;
    use std::collections::BTreeSet;

    #[test]
    fn test_materialize_no_edges() {
        let graph = Graph::new();

        graph.add_node(1, 1, NO_PROPS, None).unwrap();
        graph.add_node(2, 2, NO_PROPS, None).unwrap();

        test_storage!(&graph, |graph| {
            let sg = graph.subgraph([1, 2, 1]); // <- duplicated nodes should have no effect

            let actual = sg.materialize().unwrap().into_events().unwrap();
            assert_graph_equal(&actual, &sg);
        });
    }

    #[test]
    fn test_remove_degree1_triangle_count() {
        let graph = Graph::new();
        let edges = vec![
            (1, 2, 1),
            (1, 3, 2),
            (1, 4, 3),
            (3, 1, 4),
            (3, 4, 5),
            (3, 5, 6),
            (4, 5, 7),
            (5, 6, 8),
            (5, 8, 9),
            (7, 5, 10),
            (8, 5, 11),
            (1, 9, 12),
            (9, 1, 13),
            (6, 3, 14),
            (4, 8, 15),
            (8, 3, 16),
            (5, 10, 17),
            (10, 5, 18),
            (10, 8, 19),
            (1, 11, 20),
            (11, 1, 21),
            (9, 11, 22),
            (11, 9, 23),
        ];
        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }
        test_storage!(&graph, |graph| {
            let subgraph = graph.subgraph(graph.nodes().into_iter().filter(|v| v.degree() > 1));
            let ts = triangle_count(&subgraph, None);
            let tg = triangle_count(graph, None);
            assert_eq!(ts, tg)
        });
    }

    #[test]
    fn layer_materialize() {
        let graph = Graph::new();
        graph.add_edge(0, 1, 2, NO_PROPS, Some("1")).unwrap();
        graph.add_edge(0, 3, 4, NO_PROPS, Some("2")).unwrap();

        test_storage!(&graph, |graph| {
            let sg = graph.subgraph([1, 2]);
            let sgm = sg.materialize().unwrap();
            assert_eq!(
                sg.unique_layers().collect_vec(),
                sgm.unique_layers().collect_vec()
            );
        });
    }

    #[test]
    fn test_cc() {
        let graph = Graph::new();
        graph.add_node(0, 0, NO_PROPS, None).unwrap();
        graph.add_node(0, 3, NO_PROPS, None).unwrap();
        graph.add_node(1, 2, NO_PROPS, None).unwrap();
        graph.add_node(1, 4, NO_PROPS, None).unwrap();
        graph.add_edge(0, 0, 1, NO_PROPS, Some("1")).unwrap();
        graph.add_edge(1, 3, 4, NO_PROPS, Some("1")).unwrap();
        let sg = graph.subgraph([0, 1, 3, 4]);
        let cc = weakly_connected_components(&sg, 100, None);
        let groups = cc.groups();
        let group_sets = groups
            .iter()
            .map(|(_, g)| g.id().into_iter_values().collect::<BTreeSet<_>>())
            .collect::<HashSet<_>>();
        assert_eq!(
            group_sets,
            HashSet::from_iter([
                BTreeSet::from([GID::U64(0), GID::U64(1)]),
                BTreeSet::from([GID::U64(3), GID::U64(4)])
            ])
        );
    }

    #[test]
    fn test_layer_edges() {
        let graph = Graph::new();
        graph.add_edge(0, 0, 1, NO_PROPS, Some("1")).unwrap();
        graph.add_edge(1, 0, 1, NO_PROPS, Some("2")).unwrap();

        assert_eq!(
            graph.subgraph([0, 1]).edges().id().collect_vec(),
            [(GID::U64(0), GID::U64(1))]
        );
        assert_eq!(
            graph
                .subgraph([0, 1])
                .valid_layers("1")
                .edges()
                .id()
                .collect_vec(),
            [(GID::U64(0), GID::U64(1))]
        );
    }

    mod test_filters_node_subgraph {
        use crate::{
            db::{
                api::view::StaticGraphViewOps,
                graph::{
                    assertions::{GraphTransformer, TestGraphVariants},
                    views::{node_subgraph::NodeSubgraph, window_graph::WindowedGraph},
                },
            },
            prelude::{GraphViewOps, NodeViewOps, TimeOps},
        };
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
            use crate::{
                db::{
                    api::view::StaticGraphViewOps,
                    graph::{
                        assertions::{
                            assert_filter_nodes_results, assert_search_nodes_results, TestVariants,
                        },
                        views::{
                            filter::model::{NodeFilter, PropertyFilterOps},
                            node_subgraph::subgraph_tests::test_filters_node_subgraph::{
                                NodeSubgraphTransformer, TestGraphVariants,
                                WindowedNodeSubgraphTransformer,
                            },
                        },
                    },
                },
                prelude::AdditionOps,
            };
            use raphtory_api::core::entities::properties::prop::Prop;

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
                    graph.add_node(*id, name, props.clone(), None).unwrap();
                }

                graph
            }

            #[test]
            fn test_search_nodes_subgraph() {
                let filter = NodeFilter::property("p1").eq(1u64);
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
                let filter = NodeFilter::property("p1").le(1u64);
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
                let filter = NodeFilter::property("p1").eq(1u64);
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
                let filter = NodeFilter::property("p1").gt(0u64);
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
                let filter = NodeFilter::property("p1").eq(1u64);
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
                let filter = NodeFilter::property("p1").ge(1u64);
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
            use crate::{
                db::{
                    api::view::StaticGraphViewOps,
                    graph::{
                        assertions::{
                            assert_filter_edges_results, assert_search_edges_results, TestVariants,
                        },
                        views::{
                            filter::model::{EdgeFilter, PropertyFilterOps},
                            node_subgraph::subgraph_tests::test_filters_node_subgraph::{
                                NodeSubgraphTransformer, WindowedNodeSubgraphTransformer,
                            },
                        },
                    },
                },
                prelude::AdditionOps,
            };
            use raphtory_api::core::entities::properties::prop::Prop;

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
                let filter = EdgeFilter::property("p1").eq(1u64);
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
                let filter = EdgeFilter::property("p1").le(1u64);
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
                let filter = EdgeFilter::property("p1").eq(1u64);
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
                let filter = EdgeFilter::property("p1").ge(1u64);
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
                let filter = EdgeFilter::property("p1").eq(1u64);
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
                let filter = EdgeFilter::property("p1").lt(2u64);
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
    }

    #[test]
    fn nodes_without_updates_are_filtered() {
        let g = Graph::new();
        g.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
        let expected = Graph::new();
        expected.resolve_layer(None).unwrap();
        let subgraph = g.subgraph([0]);
        assert_graph_equal(&subgraph, &expected);
    }

    #[test]
    fn materialize_proptest() {
        proptest!(|(graph in build_graph_strat(10, 10, false), nodes in subsequence((0..10).collect::<Vec<_>>(), 0..10))| {
            let graph = Graph::from(build_graph(&graph));
            let subgraph = graph.subgraph(nodes);
            assert_graph_equal(&subgraph, &subgraph.materialize().unwrap());
        })
    }

    #[test]
    fn materialize_persistent_proptest() {
        proptest!(|(graph in build_graph_strat(10, 10, true), nodes in subsequence((0..10).collect::<Vec<_>>(), 0..10))| {
            let graph = PersistentGraph::from(build_graph(&graph));
            let subgraph = graph.subgraph(nodes);
            assert_graph_equal(&subgraph, &subgraph.materialize().unwrap());
        })
    }

    #[test]
    fn test_subgraph_only_deletion() {
        let g = PersistentGraph::new();
        g.delete_edge(0, 0, 1, None).unwrap();
        let sg = g.subgraph([0]);
        let expected = PersistentGraph::new();
        expected.resolve_layer(None).unwrap();
        assert_graph_equal(&sg, &expected);
    }
}
