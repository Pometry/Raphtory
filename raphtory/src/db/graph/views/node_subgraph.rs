use crate::{
    core::entities::{nodes::node_ref::AsNodeRef, LayerIds, VID},
    db::api::{
        properties::internal::InheritPropertiesOps,
        state::Index,
        storage::graph::{
            edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
            nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
        },
        view::internal::{
            Base, EdgeFilterOps, EdgeList, Immutable, InheritCoreOps, InheritEdgeHistoryFilter,
            InheritLayerOps, InheritMaterialize, InheritNodeHistoryFilter, InheritTimeSemantics,
            ListOps, NodeFilterOps, NodeList, Static,
        },
    },
    prelude::GraphViewOps,
};
use std::fmt::{Debug, Formatter};

use crate::db::api::view::internal::InheritStorageOps;

#[derive(Clone)]
pub struct NodeSubgraph<G> {
    pub(crate) graph: G,
    pub(crate) nodes: Index<VID>,
}

impl<G> Static for NodeSubgraph<G> {}

impl<'graph, G: Debug + 'graph> Debug for NodeSubgraph<G> {
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

impl<'graph, G: GraphViewOps<'graph>> InheritCoreOps for NodeSubgraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for NodeSubgraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for NodeSubgraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for NodeSubgraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for NodeSubgraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for NodeSubgraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for NodeSubgraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for NodeSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> NodeSubgraph<G> {
    pub fn new(graph: G, nodes: impl IntoIterator<Item = impl AsNodeRef>) -> Self {
        let nodes = nodes
            .into_iter()
            .flat_map(|v| graph.internalise_node(v.as_node_ref()));
        let nodes = if graph.nodes_filtered() {
            Index::from_iter(nodes.filter(|n| graph.has_node(*n)))
        } else {
            Index::from_iter(nodes)
        };
        Self { graph, nodes }
    }
}

impl<'graph, G: GraphViewOps<'graph>> EdgeFilterOps for NodeSubgraph<G> {
    #[inline]
    fn edges_filtered(&self) -> bool {
        true
    }

    #[inline]
    fn edge_list_trusted(&self) -> bool {
        false
    }

    #[inline]
    fn filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool {
        self.graph.filter_edge(edge, layer_ids)
            && self.nodes.contains(&edge.src())
            && self.nodes.contains(&edge.dst())
    }
}

impl<'graph, G: GraphViewOps<'graph>> NodeFilterOps for NodeSubgraph<G> {
    fn nodes_filtered(&self) -> bool {
        true
    }
    // FIXME: should use list version and make this true
    fn node_list_trusted(&self) -> bool {
        true
    }

    #[inline]
    fn edge_filter_includes_node_filter(&self) -> bool {
        self.graph.edge_filter_includes_node_filter()
    }
    #[inline]
    fn filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.graph.filter_node(node, layer_ids) && self.nodes.contains(&node.vid())
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
        db::graph::graph::assert_graph_equal,
        prelude::*,
        test_storage,
    };
    use ahash::HashSet;
    use itertools::Itertools;
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
                    assertions::{ApplyFilter, GraphTransformer, TestGraphVariants},
                    views::{
                        filter::{
                            internal::{InternalEdgeFilterOps, InternalNodeFilterOps},
                            model::{AsEdgeFilter, AsNodeFilter},
                        },
                        node_subgraph::NodeSubgraph,
                        window_graph::WindowedGraph,
                    },
                },
            },
            prelude::{AdditionOps, GraphViewOps, NodeViewOps, TimeOps},
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
                core::Prop,
                db::{
                    api::view::StaticGraphViewOps,
                    graph::{
                        assertions::{
                            assert_filter_nodes_results, assert_search_nodes_results, TestVariants,
                        },
                        views::{
                            filter::model::PropertyFilterOps,
                            node_subgraph::subgraph_tests::test_filters_node_subgraph::{
                                NodeSubgraphTransformer, TestGraphVariants,
                                WindowedNodeSubgraphTransformer,
                            },
                        },
                    },
                },
                prelude::{AdditionOps, NodeViewOps, PropertyFilter},
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
                    graph.add_node(*id, name, props.clone(), None).unwrap();
                }

                graph
            }

            #[test]
            fn test_search_nodes_subgraph() {
                let filter = PropertyFilter::property("p1").eq(1u64);
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
                let filter = PropertyFilter::property("p1").le(1u64);
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
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3", "N6"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowedNodeSubgraphTransformer(None, 6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::Only(vec![TestGraphVariants::Graph]),
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowedNodeSubgraphTransformer(None, 6..9),
                    filter,
                    &expected_results,
                    TestVariants::Only(vec![TestGraphVariants::Graph]),
                );

                let node_names: Option<Vec<String>> = Some(vec!["N3".into()]);
                let filter = PropertyFilter::property("p1").gt(0u64);
                let expected_results = vec!["N3"];
                assert_filter_nodes_results(
                    init_graph,
                    WindowedNodeSubgraphTransformer(node_names.clone(), 6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::Only(vec![TestGraphVariants::Graph]),
                );
                assert_search_nodes_results(
                    init_graph,
                    WindowedNodeSubgraphTransformer(node_names, 6..9),
                    filter,
                    &expected_results,
                    TestVariants::Only(vec![TestGraphVariants::Graph]),
                );
            }

            #[test]
            fn test_search_nodes_pg_w() {
                let filter = PropertyFilter::property("p1").eq(1u64);
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
                let filter = PropertyFilter::property("p1").ge(1u64);
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
                core::Prop,
                db::{
                    api::view::StaticGraphViewOps,
                    graph::{
                        assertions::{
                            assert_filter_edges_results, assert_search_edges_results, TestVariants,
                        },
                        views::{
                            filter::model::PropertyFilterOps,
                            node_subgraph::subgraph_tests::test_filters_node_subgraph::{
                                NodeSubgraphTransformer, WindowedNodeSubgraphTransformer,
                            },
                        },
                    },
                },
                prelude::{AdditionOps, PropertyFilter},
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
                let filter = PropertyFilter::property("p1").eq(1u64);
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
                let filter = PropertyFilter::property("p1").le(1u64);
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
                let filter = PropertyFilter::property("p1").eq(1u64);
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
                let filter = PropertyFilter::property("p1").ge(1u64);
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
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4", "N6->N7", "N7->N8"];
                assert_filter_edges_results(
                    init_graph,
                    WindowedNodeSubgraphTransformer(None, 6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::Only(vec![]),
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
                let filter = PropertyFilter::property("p1").lt(2u64);
                let expected_results = vec!["N3->N4"];
                assert_filter_edges_results(
                    init_graph,
                    WindowedNodeSubgraphTransformer(node_names.clone(), 6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::Only(vec![]),
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
}
