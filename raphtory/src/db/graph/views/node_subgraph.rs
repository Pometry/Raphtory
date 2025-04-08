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
            nodes: self.nodes.clone(),
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

    #[cfg(all(test, feature = "search"))]
    mod search_nodes_node_subgraph_tests {
        use crate::{
            core::Prop,
            db::{
                api::view::{SearchableGraphOps, StaticGraphViewOps},
                graph::views::{
                    deletion_graph::PersistentGraph,
                    property_filter::{FilterExpr, PropertyFilterOps},
                },
            },
            prelude::{AdditionOps, Graph, GraphViewOps, NodeViewOps, PropertyFilter, TimeOps},
        };
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

        fn search_nodes_by_composite_filter<G: StaticGraphViewOps + AdditionOps>(
            graph: &G,
            node_names: Vec<String>,
            filter: FilterExpr,
        ) -> Vec<String> {
            graph.create_index().unwrap();
            let mut results = graph
                .subgraph(node_names)
                .search_nodes(filter, 10, 0)
                .expect("Failed to search for nodes")
                .into_iter()
                .map(|v| v.name())
                .collect::<Vec<_>>();
            results.sort();
            results
        }

        fn search_nodes_by_composite_filter_w<G: StaticGraphViewOps + AdditionOps>(
            graph: &G,
            w: Range<i64>,
            node_names: Vec<String>,
            filter: FilterExpr,
        ) -> Vec<String> {
            graph.create_index().unwrap();
            let mut results = graph
                .subgraph(node_names)
                .window(w.start, w.end)
                .search_nodes(filter, 10, 0)
                .expect("Failed to search for nodes")
                .into_iter()
                .map(|v| v.name())
                .collect::<Vec<_>>();
            results.sort();
            results
        }

        #[test]
        fn test_search_nodes_subgraph() {
            let graph = Graph::new();
            let graph = init_graph(graph);

            let node_names = graph.nodes().name().collect_vec();
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_nodes_by_composite_filter(&graph, node_names, filter);
            assert_eq!(results, vec!["N1", "N3", "N4", "N6", "N7"]);

            let node_names: Vec<String> = vec!["N2".into(), "N3".into(), "N4".into(), "N5".into()];
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_nodes_by_composite_filter(&graph, node_names, filter);
            assert_eq!(results, vec!["N3", "N4"]);
        }

        #[test]
        fn test_search_nodes_subgraph_w() {
            let graph = Graph::new();
            let graph = init_graph(graph);
            let filter = PropertyFilter::property("p1").eq(1u64);

            let node_names = graph.nodes().name().collect_vec();
            let results = search_nodes_by_composite_filter_w(&graph, 6..9, node_names, filter);
            assert_eq!(results, vec!["N1", "N3", "N6"]);

            let node_names: Vec<String> = vec!["N2".into(), "N3".into(), "N4".into(), "N5".into()];

            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_nodes_by_composite_filter_w(&graph, 6..9, node_names, filter);
            assert_eq!(results, vec!["N3"]);
        }

        #[test]
        fn test_search_nodes_persistent_subgraph() {
            let graph = PersistentGraph::new();
            let graph = init_graph(graph);

            let node_names = graph.nodes().name().collect_vec();
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_nodes_by_composite_filter(&graph, node_names, filter);
            assert_eq!(results, vec!["N1", "N3", "N4", "N6", "N7"]);

            let node_names: Vec<String> = vec!["N2".into(), "N3".into(), "N4".into(), "N5".into()];
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_nodes_by_composite_filter(&graph, node_names, filter);
            assert_eq!(results, vec!["N3", "N4"]);
        }

        #[test]
        fn test_search_nodes_persistent_subgraph_w() {
            let graph = PersistentGraph::new();
            let graph = init_graph(graph);

            let node_names = graph.nodes().name().collect_vec();
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_nodes_by_composite_filter_w(&graph, 6..9, node_names, filter);
            assert_eq!(results, vec!["N1", "N3", "N6", "N7"]);

            let node_names: Vec<String> = vec!["N2".into(), "N3".into(), "N4".into(), "N5".into()];
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_nodes_by_composite_filter_w(&graph, 6..9, node_names, filter);
            assert_eq!(results, vec!["N3"]);
        }
    }

    #[cfg(all(test, feature = "search"))]
    mod search_edges_node_subgraph_tests {
        use crate::{
            core::Prop,
            db::{
                api::view::{SearchableGraphOps, StaticGraphViewOps},
                graph::views::{
                    deletion_graph::PersistentGraph,
                    property_filter::{FilterExpr, PropertyFilterOps},
                },
            },
            prelude::{
                AdditionOps, EdgeViewOps, Graph, GraphViewOps, NodeViewOps, PropertyFilter, TimeOps,
            },
        };
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

        fn search_edges_by_composite_filter<G: StaticGraphViewOps + AdditionOps>(
            graph: &G,
            node_names: Vec<String>,
            filter: FilterExpr,
        ) -> Vec<String> {
            graph.create_index().unwrap();
            let mut results = graph
                .subgraph(node_names)
                .search_edges(filter, 10, 0)
                .expect("Failed to search for nodes")
                .into_iter()
                .map(|v| format!("{}->{}", v.src().name(), v.dst().name()))
                .collect::<Vec<_>>();
            results.sort();
            results
        }

        fn search_edges_by_composite_filter_w<G: StaticGraphViewOps + AdditionOps>(
            graph: &G,
            w: Range<i64>,
            node_names: Vec<String>,
            filter: FilterExpr,
        ) -> Vec<String> {
            graph.create_index().unwrap();
            let mut results = graph
                .subgraph(node_names)
                .window(w.start, w.end)
                .search_edges(filter, 10, 0)
                .expect("Failed to search for nodes")
                .into_iter()
                .map(|v| format!("{}->{}", v.src().name(), v.dst().name()))
                .collect::<Vec<_>>();
            results.sort();
            results
        }

        #[test]
        fn test_search_edges_subgraph() {
            let graph = Graph::new();
            let graph = init_graph(graph);

            let node_names = graph.nodes().name().collect_vec();
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_edges_by_composite_filter(&graph, node_names, filter);
            assert_eq!(
                results,
                vec!["N1->N2", "N3->N4", "N4->N5", "N6->N7", "N7->N8"]
            );

            let node_names: Vec<String> = vec!["N2".into(), "N3".into(), "N4".into(), "N5".into()];
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_edges_by_composite_filter(&graph, node_names, filter);
            assert_eq!(results, vec!["N3->N4", "N4->N5"]);
        }

        #[test]
        fn test_search_edges_subgraph_w() {
            let graph = Graph::new();
            let graph = init_graph(graph);

            let node_names = graph.nodes().name().collect_vec();
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_edges_by_composite_filter_w(&graph, 6..9, node_names, filter);
            assert_eq!(results, vec!["N1->N2", "N3->N4", "N6->N7"]);

            let node_names: Vec<String> = vec!["N2".into(), "N3".into(), "N4".into(), "N5".into()];
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_edges_by_composite_filter_w(&graph, 6..9, node_names, filter);
            assert_eq!(results, vec!["N3->N4"]);
        }

        #[test]
        fn test_search_edges_persistent_subgraph() {
            let graph = PersistentGraph::new();
            let graph = init_graph(graph);

            let node_names = graph.nodes().name().collect_vec();
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_edges_by_composite_filter(&graph, node_names, filter);
            assert_eq!(
                results,
                vec!["N1->N2", "N3->N4", "N4->N5", "N6->N7", "N7->N8"]
            );

            let node_names: Vec<String> = vec!["N2".into(), "N3".into(), "N4".into(), "N5".into()];
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_edges_by_composite_filter(&graph, node_names, filter);
            assert_eq!(results, vec!["N3->N4", "N4->N5"]);
        }

        #[test]
        fn test_search_edges_persistent_subgraph_w() {
            let graph = PersistentGraph::new();
            let graph = init_graph(graph);

            let node_names = graph.nodes().name().collect_vec();
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_edges_by_composite_filter_w(&graph, 6..9, node_names, filter);
            assert_eq!(results, vec!["N1->N2", "N3->N4", "N6->N7", "N7->N8"]);

            let node_names: Vec<String> = vec![
                "N2".into(),
                "N3".into(),
                "N4".into(),
                "N5".into(),
                "N6".into(),
            ];
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_edges_by_composite_filter_w(&graph, 6..9, node_names, filter);
            assert_eq!(results, vec!["N3->N4"]);
        }
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
}
