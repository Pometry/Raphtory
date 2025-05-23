use crate::{
    core::entities::{nodes::node_ref::AsNodeRef, LayerIds, VID},
    db::api::{
        properties::internal::InheritPropertiesOps,
        state::Index,
        view::internal::{
            EdgeFilterOps, EdgeList, Immutable, InheritEdgeHistoryFilter, InheritLayerOps,
            InheritMaterialize, InheritNodeHistoryFilter, InheritStorageOps, InheritTimeSemantics,
            InternalNodeFilterOps, ListOps, NodeList, Static,
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
            .flat_map(|v| graph.internalise_node(v.as_node_ref()))
            .collect();
        let filter = NodeSubgraph {
            graph: &graph,
            nodes: nodes.clone(),
        };
        let nodes: Index<_> = nodes
            .into_iter()
            .filter(|vid| filter.has_node(*vid))
            .collect();
        Self { graph, nodes }
    }
}

impl<'graph, G: GraphViewOps<'graph>> EdgeFilterOps for NodeSubgraph<G> {
    #[inline]
    fn edges_filtered(&self) -> bool {
        true
    }

    #[inline]
    fn edge_history_filtered(&self) -> bool {
        self.graph.edge_history_filtered()
    }

    #[inline]
    fn edge_list_trusted(&self) -> bool {
        false
    }

    fn filter_edge_history(&self, eid: ELID, t: TimeIndexEntry, layer_ids: &LayerIds) -> bool {
        self.graph.filter_edge_history(eid, t, layer_ids) && {
            let core_edge = self.core_edge(eid.edge);
            self.nodes.contains(&core_edge.src()) && self.nodes.contains(&core_edge.dst())
        }
    }

    #[inline]
    fn filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool {
        self.graph.filter_edge(edge, layer_ids)
            && self.nodes.contains(&edge.src())
            && self.nodes.contains(&edge.dst())
    }
}

impl<'graph, G: GraphViewOps<'graph>> InternalNodeFilterOps for NodeSubgraph<G> {
    fn internal_nodes_filtered(&self) -> bool {
        true
    }
    fn internal_node_list_trusted(&self) -> bool {
        true
    }

    #[inline]
    fn edge_and_node_filter_independent(&self) -> bool {
        self.graph.edge_and_node_filter_independent()
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

        macro_rules! assert_filter_results {
            ($filter_fn:ident, $filter:expr, $node_names:expr, $expected_results:expr) => {{
                let filter_results = $filter_fn($filter.clone(), $node_names.clone());
                assert_eq!($expected_results, filter_results);
            }};
        }

        macro_rules! assert_filter_results_w {
            ($filter_fn:ident, $filter:expr, $node_names:expr, $window:expr, $expected_results:expr) => {{
                let filter_results = $filter_fn($filter.clone(), $window, $node_names.clone());
                assert_eq!($expected_results, filter_results);
            }};
        }

        #[cfg(feature = "search")]
        macro_rules! assert_search_results {
            ($search_fn:ident, $filter:expr, $node_names:expr, $expected_results:expr) => {{
                let search_results = $search_fn($filter.clone(), $node_names);
                assert_eq!($expected_results, search_results);
            }};
        }

        #[cfg(not(feature = "search"))]
        macro_rules! assert_search_results {
            ($search_fn:ident, $filter:expr, $node_names:expr, $expected_results:expr) => {};
        }

        #[cfg(feature = "search")]
        macro_rules! assert_search_results_w {
            ($search_fn:ident, $filter:expr, $node_names:expr, $window:expr, $expected_results:expr) => {{
                let search_results = $search_fn($filter.clone(), $window, $node_names);
                assert_eq!($expected_results, search_results);
            }};
        }

        #[cfg(not(feature = "search"))]
        macro_rules! assert_search_results_w {
            ($search_fn:ident, $filter:expr, $node_names:expr, $window:expr, $expected_results:expr) => {};
        }

        mod test_nodes_filters_node_subgraph {
            use crate::{
                db::{
                    api::view::StaticGraphViewOps,
                    graph::views::{
                        deletion_graph::PersistentGraph,
                        filter::{internal::CreateNodeFilter, model::PropertyFilterOps},
                        test_helpers::filter_nodes_with,
                    },
                },
                prelude::{AdditionOps, Graph, GraphViewOps, NodeViewOps, PropertyFilter, TimeOps},
            };
            use raphtory_api::core::entities::properties::prop::Prop;
            use std::ops::Range;

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

            fn filter_nodes<I: CreateNodeFilter>(
                filter: I,
                node_names: Option<Vec<String>>,
            ) -> Vec<String> {
                let graph = init_graph(Graph::new());
                let node_names: Vec<String> =
                    node_names.unwrap_or_else(|| graph.nodes().name().collect());
                filter_nodes_with(filter, graph.subgraph(node_names))
            }

            fn filter_nodes_w<I: CreateNodeFilter>(
                filter: I,
                w: Range<i64>,
                node_names: Option<Vec<String>>,
            ) -> Vec<String> {
                let graph = init_graph(Graph::new());
                let node_names: Vec<String> =
                    node_names.unwrap_or_else(|| graph.nodes().name().collect());
                filter_nodes_with(filter, graph.subgraph(node_names).window(w.start, w.end))
            }

            fn filter_nodes_pg<I: CreateNodeFilter>(
                filter: I,
                node_names: Option<Vec<String>>,
            ) -> Vec<String> {
                let graph = init_graph(PersistentGraph::new());
                let node_names: Vec<String> =
                    node_names.unwrap_or_else(|| graph.nodes().name().collect());
                filter_nodes_with(filter, graph.subgraph(node_names))
            }

            fn filter_nodes_pg_w<I: CreateNodeFilter>(
                filter: I,
                w: Range<i64>,
                node_names: Option<Vec<String>>,
            ) -> Vec<String> {
                let graph = init_graph(PersistentGraph::new());
                let node_names: Vec<String> =
                    node_names.unwrap_or_else(|| graph.nodes().name().collect());
                filter_nodes_with(filter, graph.subgraph(node_names).window(w.start, w.end))
            }

            #[cfg(feature = "search")]
            mod search_nodes {
                use std::ops::Range;
                use crate::db::graph::views::deletion_graph::PersistentGraph;
                use crate::db::graph::views::node_subgraph::subgraph_tests::test_filters_node_subgraph::test_nodes_filters_node_subgraph::init_graph;
                use crate::db::graph::views::test_helpers::search_nodes_with;
                use crate::prelude::{Graph, GraphViewOps, NodeViewOps, PropertyFilter, TimeOps};

                pub fn search_nodes(
                    filter: PropertyFilter,
                    node_names: Option<Vec<String>>,
                ) -> Vec<String> {
                    let graph = init_graph(Graph::new());
                    let node_names: Vec<String> =
                        node_names.unwrap_or_else(|| graph.nodes().name().collect());
                    search_nodes_with(filter, graph.subgraph(node_names))
                }

                pub fn search_nodes_w(
                    filter: PropertyFilter,
                    w: Range<i64>,
                    node_names: Option<Vec<String>>,
                ) -> Vec<String> {
                    let graph = init_graph(Graph::new());
                    let node_names: Vec<String> =
                        node_names.unwrap_or_else(|| graph.nodes().name().collect());
                    search_nodes_with(filter, graph.subgraph(node_names).window(w.start, w.end))
                }

                pub fn search_nodes_pg(
                    filter: PropertyFilter,
                    node_names: Option<Vec<String>>,
                ) -> Vec<String> {
                    let graph = init_graph(PersistentGraph::new());
                    let node_names: Vec<String> =
                        node_names.unwrap_or_else(|| graph.nodes().name().collect());
                    search_nodes_with(filter, graph.subgraph(node_names))
                }

                pub fn search_nodes_pg_w(
                    filter: PropertyFilter,
                    w: Range<i64>,
                    node_names: Option<Vec<String>>,
                ) -> Vec<String> {
                    let graph = init_graph(PersistentGraph::new());
                    let node_names: Vec<String> =
                        node_names.unwrap_or_else(|| graph.nodes().name().collect());
                    search_nodes_with(filter, graph.subgraph(node_names).window(w.start, w.end))
                }
            }

            #[cfg(feature = "search")]
            use search_nodes::*;

            #[test]
            fn test_search_nodes_subgraph() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3", "N4", "N6", "N7"];
                assert_filter_results!(filter_nodes, filter, None, expected_results);
                assert_search_results!(search_nodes, filter, None, expected_results);

                let node_names: Option<Vec<String>> =
                    Some(vec!["N2".into(), "N3".into(), "N4".into(), "N5".into()]);
                let filter = PropertyFilter::property("p1").le(1u64);
                let expected_results = vec!["N3", "N4"];
                assert_filter_results!(filter_nodes, filter, node_names, expected_results);
                assert_search_results!(search_nodes, filter, node_names, expected_results);
            }

            #[test]
            fn test_search_nodes_subgraph_w() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3", "N6"];
                assert_filter_results_w!(filter_nodes_w, filter, None, 6..9, expected_results);
                assert_search_results_w!(search_nodes_w, filter, None, 6..9, expected_results);

                let node_names: Option<Vec<String>> = Some(vec!["N3".into()]);
                let filter = PropertyFilter::property("p1").gt(0u64);
                let expected_results = vec!["N3"];
                assert_filter_results_w!(
                    filter_nodes_w,
                    filter,
                    node_names,
                    6..9,
                    expected_results
                );
                assert_search_results_w!(
                    search_nodes_w,
                    filter,
                    node_names,
                    6..9,
                    expected_results
                );
            }

            #[test]
            fn test_search_nodes_persistent_subgraph() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3", "N4", "N6", "N7"];
                assert_filter_results!(filter_nodes_pg, filter, None, expected_results);
                assert_search_results!(search_nodes_pg, filter, None, expected_results);

                let node_names: Option<Vec<String>> =
                    Some(vec!["N2".into(), "N3".into(), "N4".into(), "N5".into()]);
                let filter = PropertyFilter::property("p1").lt(2u64);
                let expected_results = vec!["N3", "N4"];
                assert_filter_results!(filter_nodes_pg, filter, node_names, expected_results);
                assert_search_results!(search_nodes_pg, filter, node_names, expected_results);
            }

            #[test]
            fn test_search_nodes_persistent_subgraph_w() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3", "N6", "N7"];
                assert_filter_results_w!(filter_nodes_pg_w, filter, None, 6..9, expected_results);
                assert_search_results_w!(search_nodes_pg_w, filter, None, 6..9, expected_results);

                let node_names: Option<Vec<String>> =
                    Some(vec!["N2".into(), "N3".into(), "N4".into(), "N5".into()]);
                let filter = PropertyFilter::property("p1").ge(1u64);
                let expected_results = vec!["N2", "N3", "N5"];
                assert_filter_results_w!(
                    filter_nodes_pg_w,
                    filter,
                    node_names,
                    6..9,
                    expected_results
                );
                assert_search_results_w!(
                    search_nodes_pg_w,
                    filter,
                    node_names,
                    6..9,
                    expected_results
                );
            }
        }

        mod test_edges_filters_node_subgraph {
            use crate::{
                db::{
                    api::view::StaticGraphViewOps,
                    graph::views::{
                        deletion_graph::PersistentGraph,
                        filter::{internal::InternalEdgeFilterOps, model::PropertyFilterOps},
                        test_helpers::filter_edges_with,
                    },
                },
                prelude::{AdditionOps, Graph, GraphViewOps, NodeViewOps, PropertyFilter, TimeOps},
            };
            use raphtory_api::core::entities::properties::prop::Prop;
            use std::ops::Range;

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

            fn filter_edges<I: InternalEdgeFilterOps>(
                filter: I,
                node_names: Option<Vec<String>>,
            ) -> Vec<String> {
                let graph = init_graph(Graph::new());
                let node_names: Vec<String> =
                    node_names.unwrap_or_else(|| graph.nodes().name().collect());
                filter_edges_with(filter, graph.subgraph(node_names))
            }

            fn filter_edges_w<I: InternalEdgeFilterOps>(
                filter: I,
                w: Range<i64>,
                node_names: Option<Vec<String>>,
            ) -> Vec<String> {
                let graph = init_graph(Graph::new());
                let node_names: Vec<String> =
                    node_names.unwrap_or_else(|| graph.nodes().name().collect());
                filter_edges_with(filter, graph.subgraph(node_names).window(w.start, w.end))
            }

            #[allow(dead_code)]
            fn filter_edges_pg<I: InternalEdgeFilterOps>(
                filter: I,
                node_names: Option<Vec<String>>,
            ) -> Vec<String> {
                let graph = init_graph(PersistentGraph::new());
                let node_names: Vec<String> =
                    node_names.unwrap_or_else(|| graph.nodes().name().collect());
                filter_edges_with(filter, graph.subgraph(node_names))
            }

            #[allow(dead_code)]
            fn filter_edges_pg_w<I: InternalEdgeFilterOps>(
                filter: I,
                w: Range<i64>,
                node_names: Option<Vec<String>>,
            ) -> Vec<String> {
                let graph = init_graph(PersistentGraph::new());
                let node_names: Vec<String> =
                    node_names.unwrap_or_else(|| graph.nodes().name().collect());
                filter_edges_with(filter, graph.subgraph(node_names).window(w.start, w.end))
            }

            #[cfg(feature = "search")]
            mod search_edges {
                use std::ops::Range;
                use crate::db::graph::views::deletion_graph::PersistentGraph;
                use crate::db::graph::views::node_subgraph::subgraph_tests::test_filters_node_subgraph::test_edges_filters_node_subgraph::init_graph;
                use crate::db::graph::views::test_helpers::search_edges_with;
                use crate::prelude::{Graph, GraphViewOps, NodeViewOps, PropertyFilter, TimeOps};

                pub fn search_edges(
                    filter: PropertyFilter,
                    node_names: Option<Vec<String>>,
                ) -> Vec<String> {
                    let graph = init_graph(Graph::new());
                    let node_names: Vec<String> =
                        node_names.unwrap_or_else(|| graph.nodes().name().collect());
                    search_edges_with(filter, graph.subgraph(node_names))
                }

                pub fn search_edges_w(
                    filter: PropertyFilter,
                    w: Range<i64>,
                    node_names: Option<Vec<String>>,
                ) -> Vec<String> {
                    let graph = init_graph(Graph::new());
                    let node_names: Vec<String> =
                        node_names.unwrap_or_else(|| graph.nodes().name().collect());
                    search_edges_with(filter, graph.subgraph(node_names).window(w.start, w.end))
                }

                pub fn search_edges_pg(
                    filter: PropertyFilter,
                    node_names: Option<Vec<String>>,
                ) -> Vec<String> {
                    let graph = init_graph(PersistentGraph::new());
                    let node_names: Vec<String> =
                        node_names.unwrap_or_else(|| graph.nodes().name().collect());
                    search_edges_with(filter, graph.subgraph(node_names))
                }

                pub fn search_edges_pg_w(
                    filter: PropertyFilter,
                    w: Range<i64>,
                    node_names: Option<Vec<String>>,
                ) -> Vec<String> {
                    let graph = init_graph(PersistentGraph::new());
                    let node_names: Vec<String> =
                        node_names.unwrap_or_else(|| graph.nodes().name().collect());
                    search_edges_with(filter, graph.subgraph(node_names).window(w.start, w.end))
                }
            }

            #[cfg(feature = "search")]
            use search_edges::*;

            #[test]
            fn test_edges_filters() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4", "N4->N5", "N6->N7", "N7->N8"];
                assert_filter_results!(filter_edges, filter, None, expected_results);
                assert_search_results!(search_edges, filter, None, expected_results);

                let node_names: Option<Vec<String>> =
                    Some(vec!["N2".into(), "N3".into(), "N4".into(), "N5".into()]);
                let filter = PropertyFilter::property("p1").le(1u64);
                let expected_results = vec!["N3->N4", "N4->N5"];
                assert_filter_results!(filter_edges, filter, node_names, expected_results);
                assert_search_results!(search_edges, filter, node_names, expected_results);
            }

            #[test]
            fn test_edges_filters_w() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4", "N6->N7"];
                assert_filter_results_w!(filter_edges_w, filter, None, 6..9, expected_results);
                assert_search_results_w!(search_edges_w, filter, None, 6..9, expected_results);

                let node_names: Option<Vec<String>> =
                    Some(vec!["N2".into(), "N3".into(), "N4".into(), "N5".into()]);
                let filter = PropertyFilter::property("p1").ge(1u64);
                let expected_results = vec!["N2->N3", "N3->N4"];
                assert_filter_results_w!(
                    filter_edges_w,
                    filter,
                    node_names,
                    6..9,
                    expected_results
                );
                assert_search_results_w!(
                    search_edges_w,
                    filter,
                    node_names,
                    6..9,
                    expected_results
                );
            }

            #[test]
            fn test_edges_filters_pg() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4", "N4->N5", "N6->N7", "N7->N8"];
                // TODO: PropertyFilteringNotImplemented
                // assert_filter_results!(filter_edges_pg, filter, None, expected_results);
                assert_search_results!(search_edges_pg, filter, None, expected_results);

                let node_names: Option<Vec<String>> =
                    Some(vec!["N2".into(), "N3".into(), "N4".into(), "N5".into()]);
                let filter = PropertyFilter::property("p1").le(1u64);
                let expected_results = vec!["N3->N4", "N4->N5"];
                // TODO: PropertyFilteringNotImplemented
                // assert_filter_results!(filter_edges_pg, filter, node_names, expected_results);
                assert_search_results!(search_edges_pg, filter, node_names, expected_results);
            }

            #[test]
            fn test_edges_filters_pg_w() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4", "N6->N7", "N7->N8"];
                // TODO: PropertyFilteringNotImplemented
                // assert_filter_results_w!(filter_edges_pg_w, filter, None, 6..9, expected_results);
                assert_search_results_w!(search_edges_pg_w, filter, None, 6..9, expected_results);

                let node_names: Option<Vec<String>> = Some(vec![
                    "N2".into(),
                    "N3".into(),
                    "N4".into(),
                    "N5".into(),
                    "N6".into(),
                ]);
                let filter = PropertyFilter::property("p1").lt(2u64);
                let expected_results = vec!["N3->N4"];
                // TODO: PropertyFilteringNotImplemented
                // assert_filter_results_w!(filter_edges_pg_w, filter, node_names, 6..9, expected_results);
                assert_search_results_w!(
                    search_edges_pg_w,
                    filter,
                    node_names,
                    6..9,
                    expected_results
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
}
