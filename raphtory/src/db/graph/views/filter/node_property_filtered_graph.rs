use crate::{
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            view::internal::{
                Immutable, InheritAllEdgeFilterOps, InheritEdgeHistoryFilter, InheritLayerOps,
                InheritListOps, InheritMaterialize, InheritNodeHistoryFilter, InheritStorageOps,
                InheritTimeSemantics, InternalNodeFilterOps, Static,
            },
        },
        graph::views::filter::{
            internal::CreateFilter,
            model::{property_filter::PropertyFilter, NodeFilter},
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::{core::entities::LayerIds, inherit::Base};
use raphtory_storage::{core_ops::InheritCoreGraphOps, graph::nodes::node_ref::NodeStorageRef};

#[derive(Debug, Clone)]
pub struct NodePropertyFilteredGraph<G> {
    graph: G,
    t_prop_id: Option<usize>,
    c_prop_id: Option<usize>,
    filter: PropertyFilter<NodeFilter>,
}

impl<G> NodePropertyFilteredGraph<G> {
    pub(crate) fn new(
        graph: G,
        t_prop_id: Option<usize>,
        c_prop_id: Option<usize>,
        filter: PropertyFilter<NodeFilter>,
    ) -> Self {
        Self {
            graph,
            t_prop_id,
            c_prop_id,
            filter,
        }
    }
}

impl CreateFilter for PropertyFilter<NodeFilter> {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = NodePropertyFilteredGraph<G>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let t_prop_id = self.resolve_temporal_prop_id(graph.node_meta())?;
        let c_prop_id = self.resolve_constant_prop_id(graph.node_meta(), false)?;
        Ok(NodePropertyFilteredGraph::new(
            graph, t_prop_id, c_prop_id, self,
        ))
    }
}

impl<G> Base for NodePropertyFilteredGraph<G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G> Static for NodePropertyFilteredGraph<G> {}
impl<G> Immutable for NodePropertyFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreGraphOps for NodePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for NodePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for NodePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for NodePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for NodePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritAllEdgeFilterOps for NodePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for NodePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for NodePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for NodePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for NodePropertyFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InternalNodeFilterOps for NodePropertyFilteredGraph<G> {
    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        if self.graph.internal_filter_node(node, layer_ids) {
            self.filter
                .matches_node(&self.graph, self.t_prop_id, self.c_prop_id, node)
        } else {
            false
        }
    }
}

#[cfg(test)]
mod test_node_property_filtered_graph {
    use crate::{
        db::{
            api::view::{filter_ops::BaseFilterOps, IterFilterOps},
            graph::{
                graph::assert_edges_equal,
                views::filter::model::{
                    ComposableFilter, NodeFilter, NodeFilterBuilderOps, PropertyFilterOps,
                },
            },
        },
        prelude::*,
        test_utils::{
            add_node_props, build_edge_list, build_graph_from_edge_list, build_node_props,
            node_filtered_graph,
        },
    };
    use itertools::Itertools;
    use proptest::{arbitrary::any, proptest};

    #[test]
    fn test_node_filter_on_nodes() {
        let g = Graph::new();
        g.add_node(0, "Jimi", [("band", "JH Experience")], None)
            .unwrap();
        g.add_node(1, "John", [("band", "Dead & Company")], None)
            .unwrap();
        g.add_node(2, "David", [("band", "Pink Floyd")], None)
            .unwrap();

        let filter_expr = NodeFilter::name()
            .eq("John")
            .and(NodeFilter::property("band").eq("Dead & Company"));
        let filtered_nodes = g.nodes().filter_iter(filter_expr).unwrap();

        // filter_nodes doesn't filter the iterator, it only filters the view of the nodes which includes history, edges, etc.
        assert_eq!(
            filtered_nodes.name().collect::<Vec<_>>(),
            vec!["Jimi", "John", "David"]
        );

        // TODO: Bug! History isn't getting filtered
        let res = filtered_nodes
            .iter()
            .map(|n| n.history())
            .collect::<Vec<_>>();
        assert_eq!(res, vec![vec![], vec![1], vec![]]);

        // TODO: Bug! Properties aren't getting filtered
        let res = filtered_nodes
            .iter()
            .map(|n| n.properties().get("band"))
            .collect::<Vec<_>>();
        assert_eq!(res, vec![None, Some(Prop::str("Dead & Company")), None]);

        g.add_edge(3, "John", "Jimi", NO_PROPS, None).unwrap();

        let res = filtered_nodes
            .iter()
            .map(|n| n.out_neighbours().name().collect_vec())
            .collect::<Vec<_>>();
        assert_eq!(res, vec![Vec::<String>::new(), vec![], vec![]]);
    }

    #[test]
    fn test_node_property_filter_on_nodes() {
        let g = Graph::new();
        g.add_node(0, 1, [("test", 1i64)], None).unwrap();
        g.add_node(0, 2, [("test", 2i64)], None).unwrap();
        g.add_node(1, 3, [("test", 3i64)], None).unwrap();
        g.add_node(1, 4, [("test", 4i64)], None).unwrap();

        g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        g.add_edge(1, 2, 3, NO_PROPS, None).unwrap();
        g.add_edge(1, 2, 1, NO_PROPS, None).unwrap();
        g.add_edge(2, 3, 4, NO_PROPS, None).unwrap();
        g.add_edge(3, 4, 1, NO_PROPS, None).unwrap();

        let n1 = g.node(1).unwrap();

        assert_eq!(
            n1.filter(NodeFilter::property("test").eq(1i64))
                .unwrap()
                .edges()
                .id()
                .collect_vec(),
            vec![]
        );
        assert_eq!(
            n1.filter(NodeFilter::property("test").eq(2i64))
                .unwrap()
                .out_neighbours()
                .id()
                .collect_vec(),
            vec![GID::U64(2)]
        );

        let n2 = g.node(2).unwrap();

        assert_eq!(
            n2.filter(NodeFilter::property("test").gt(1i64))
                .unwrap()
                .neighbours()
                .id()
                .collect_vec(),
            vec![GID::U64(3)]
        );

        assert_eq!(
            n2.filter(NodeFilter::property("test").gt(0i64))
                .unwrap()
                .neighbours()
                .id()
                .collect_vec(),
            vec![GID::U64(1), GID::U64(3)]
        );

        let gp = g.persistent_graph();
        let n1p = gp.node(1).unwrap();

        assert_eq!(
            n1p.filter(NodeFilter::property("test").eq(1i64))
                .unwrap()
                .edges()
                .id()
                .collect_vec(),
            vec![]
        );
        assert_eq!(
            n1p.filter(NodeFilter::property("test").eq(2i64))
                .unwrap()
                .out_neighbours()
                .id()
                .collect_vec(),
            vec![GID::U64(2)]
        );

        let n2p = gp.node(2).unwrap();

        assert_eq!(
            n2p.filter(NodeFilter::property("test").gt(1i64))
                .unwrap()
                .neighbours()
                .id()
                .collect_vec(),
            vec![GID::U64(3)]
        );

        assert_eq!(
            n2p.filter(NodeFilter::property("test").gt(0i64))
                .unwrap()
                .neighbours()
                .id()
                .collect_vec(),
            vec![GID::U64(1), GID::U64(3)]
        );
    }

    #[test]
    fn test_node_property_filter_path() {
        let g = Graph::new();
        g.add_node(0, 1, [("test", 1i64)], None).unwrap();
        g.add_node(1, 2, [("test", 2i64)], None).unwrap();
        g.add_node(1, 3, [("test", 3i64)], None).unwrap();
        g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        g.add_edge(1, 2, 3, NO_PROPS, None).unwrap();
        g.add_edge(1, 2, 1, NO_PROPS, None).unwrap();
        g.add_edge(1, 1, 3, NO_PROPS, None).unwrap();

        let filtered_nodes = g
            .nodes()
            .filter_iter(NodeFilter::property("test").gt(1i64))
            .unwrap();
        assert_eq!(
            filtered_nodes
                .out_neighbours()
                .id()
                .map(|i| i.collect_vec())
                .collect_vec(),
            vec![vec![GID::U64(2), GID::U64(3)], vec![GID::U64(3)], vec![]]
        );

        assert_eq!(
            filtered_nodes
                .out_neighbours()
                .degree()
                .map(|i| i.collect_vec())
                .collect_vec(),
            vec![vec![1, 1], vec![1], vec![]]
        );

        let filtered_nodes_p = g
            .persistent_graph()
            .nodes()
            .filter_iter(NodeFilter::property("test").gt(1i64))
            .unwrap();
        assert_eq!(
            filtered_nodes_p
                .out_neighbours()
                .id()
                .map(|i| i.collect_vec())
                .collect_vec(),
            vec![vec![GID::U64(2), GID::U64(3)], vec![GID::U64(3)], vec![]]
        );
    }

    #[test]
    fn test_node_property_filter_on_graph() {
        let g = Graph::new();
        g.add_node(0, 1, [("test", 1i64)], None).unwrap();
        g.add_node(1, 2, [("test", 2i64)], None).unwrap();
        g.add_node(1, 3, [("test", 3i64)], None).unwrap();
        g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        g.add_edge(1, 2, 3, NO_PROPS, None).unwrap();
        g.add_edge(1, 2, 1, NO_PROPS, None).unwrap();
        g.add_edge(1, 1, 3, NO_PROPS, None).unwrap();

        let gf = g.filter(NodeFilter::property("test").eq(1i64)).unwrap();
        assert_eq!(gf.edges().id().collect_vec(), vec![]);

        let gf = g.filter(NodeFilter::property("test").gt(1i64)).unwrap();
        assert_eq!(
            gf.edges().id().collect_vec(),
            vec![(GID::U64(2), GID::U64(3))]
        );

        let gf = g.filter(NodeFilter::property("test").lt(3i64)).unwrap();
        assert_eq!(
            gf.edges().id().collect_vec(),
            vec![(GID::U64(1), GID::U64(2)), (GID::U64(2), GID::U64(1))]
        );

        let gp = g.persistent_graph();
        let gf = gp.filter(NodeFilter::property("test").eq(1i64)).unwrap();
        assert_eq!(gf.edges().id().collect_vec(), vec![]);

        let gf = gp.filter(NodeFilter::property("test").gt(1i64)).unwrap();
        assert_eq!(
            gf.edges().id().collect_vec(),
            vec![(GID::U64(2), GID::U64(3))]
        );

        let gf = gp.filter(NodeFilter::property("test").lt(3i64)).unwrap();
        assert_eq!(
            gf.edges().id().collect_vec(),
            vec![(GID::U64(1), GID::U64(2)), (GID::U64(2), GID::U64(1))]
        );
    }

    #[test]
    fn test_filter_gt() {
        proptest!(|(
            edges in build_edge_list(100, 100), nodes in build_node_props(100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            add_node_props(&g, &nodes);
            let filtered = g.filter(
                NodeFilter::property("int_prop").gt(v)
            ).unwrap();
            let expected_g = node_filtered_graph(&edges, &nodes, |_, int_v| {int_v.filter(|&vv| *vv > v ).is_some()});
            assert_edges_equal(&filtered.edges(), &expected_g.edges());
            let filtered_p = g.persistent_graph().filter(
                NodeFilter::property("int_prop").gt(v)
            ).unwrap();
            assert_edges_equal(&filtered_p.edges(), &expected_g.persistent_graph().edges());
            // FIXME: history filtering not working properly
            // assert_graph_equal(&filtered, &expected_g);
        })
    }

    #[test]
    fn test_filter_ge() {
        proptest!(|(
            edges in build_edge_list(100, 100), nodes in build_node_props(100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            add_node_props(&g, &nodes);
            let filtered = g.filter(
                NodeFilter::property("int_prop").ge(v)
            ).unwrap();
            let expected_g = node_filtered_graph(&edges, &nodes, |_, int_v| {int_v.filter(|&vv| *vv >= v ).is_some()});
            assert_edges_equal(&filtered.edges(), &expected_g.edges());
                        let filtered_p = g.persistent_graph().filter(
                NodeFilter::property("int_prop").ge(v)
            ).unwrap();
            assert_edges_equal(&filtered_p.edges(), &expected_g.persistent_graph().edges());
            // FIXME: history filtering not working properly
            // assert_graph_equal(&filtered, &expected_g);
        })
    }

    #[test]
    fn test_filter_lt() {
        proptest!(|(
            edges in build_edge_list(100, 100), nodes in build_node_props(100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            add_node_props(&g, &nodes);
            let filtered = g.filter(
                NodeFilter::property("int_prop").lt(v)
            ).unwrap();
            let expected_g = node_filtered_graph(&edges, &nodes, |_, int_v| {int_v.filter(|&vv| *vv < v ).is_some()});
            assert_edges_equal(&filtered.edges(), &expected_g.edges());
                        let filtered_p = g.persistent_graph().filter(
                NodeFilter::property("int_prop").lt(v)
            ).unwrap();
            assert_edges_equal(&filtered_p.edges(), &expected_g.persistent_graph().edges());
            // FIXME: history filtering not working properly
            // assert_graph_equal(&filtered, &expected_g);
        })
    }

    #[test]
    fn test_filter_le() {
        proptest!(|(
            edges in build_edge_list(100, 100), nodes in build_node_props(100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            add_node_props(&g, &nodes);
            let filtered = g.filter(
                NodeFilter::property("int_prop").le(v)
            ).unwrap();
            let expected_g = node_filtered_graph(&edges, &nodes, |_, int_v| {int_v.filter(|&vv| *vv <= v ).is_some()});
            assert_edges_equal(&filtered.edges(), &expected_g.edges());
                        let filtered_p = g.persistent_graph().filter(
                NodeFilter::property("int_prop").le(v)
            ).unwrap();
            assert_edges_equal(&filtered_p.edges(), &expected_g.persistent_graph().edges());
            // FIXME: history filtering not working properly
            // assert_graph_equal(&filtered, &expected_g);
        })
    }

    #[test]
    fn test_filter_eq() {
        proptest!(|(
            edges in build_edge_list(100, 100), nodes in build_node_props(100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            add_node_props(&g, &nodes);
            let filtered = g.filter(
                NodeFilter::property("int_prop").eq(v)
            ).unwrap();
            let expected_g = node_filtered_graph(&edges, &nodes, |_, int_v| {int_v.filter(|&vv| *vv == v ).is_some()});
            assert_edges_equal(&filtered.edges(), &expected_g.edges());
                        let filtered_p = g.persistent_graph().filter(
                NodeFilter::property("int_prop").eq(v)
            ).unwrap();
            assert_edges_equal(&filtered_p.edges(), &expected_g.persistent_graph().edges());
            // FIXME: history filtering not working properly
            // assert_graph_equal(&filtered, &expected_g);
        })
    }

    #[test]
    fn test_filter_ne() {
        proptest!(|(
            edges in build_edge_list(100, 100), nodes in build_node_props(100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            add_node_props(&g, &nodes);
            let filtered = g.filter(
                NodeFilter::property("int_prop").ne(v)
            ).unwrap();
            let expected_g = node_filtered_graph(&edges, &nodes, |_, int_v| {int_v.filter(|&vv| *vv != v ).is_some()});
            assert_edges_equal(&filtered.edges(), &expected_g.edges());
                        let filtered_p = g.persistent_graph().filter(
                NodeFilter::property("int_prop").ne(v)
            ).unwrap();
            assert_edges_equal(&filtered_p.edges(), &expected_g.persistent_graph().edges());
            // FIXME: history filtering not working properly
            // assert_graph_equal(&filtered, &expected_g);
        })
    }

    #[test]
    fn test_filter_is_some() {
        proptest!(|(
            edges in build_edge_list(100, 100), nodes in build_node_props(100),
        )| {
            let g = build_graph_from_edge_list(&edges);
            add_node_props(&g, &nodes);
            let filtered = g.filter(
                NodeFilter::property("int_prop").is_some()
            ).unwrap();
            let expected_g = node_filtered_graph(&edges, &nodes, |_, int_v| {int_v.is_some()});
            assert_edges_equal(&filtered.edges(), &expected_g.edges());
                        let filtered_p = g.persistent_graph().filter(
                NodeFilter::property("int_prop").is_some()
            ).unwrap();
            assert_edges_equal(&filtered_p.edges(), &expected_g.persistent_graph().edges());
            // FIXME: history filtering not working properly
            // assert_graph_equal(&filtered, &expected_g);
        })
    }

    #[test]
    fn test_filter_is_none() {
        proptest!(|(
            edges in build_edge_list(100, 100), nodes in build_node_props(100)
        )| {
            let g = build_graph_from_edge_list(&edges);
            add_node_props(&g, &nodes);
            let filtered = g.filter(
                NodeFilter::property("int_prop").is_none()
            ).unwrap();
            let expected_g = node_filtered_graph(&edges, &nodes, |_, int_v| {int_v.is_none()});
            assert_edges_equal(&filtered.edges(), &expected_g.edges());
                        let filtered_p = g.persistent_graph().filter(
                NodeFilter::property("int_prop").is_none()
            ).unwrap();
            assert_edges_equal(&filtered_p.edges(), &expected_g.persistent_graph().edges());
            // FIXME: history filtering not working properly
            // assert_graph_equal(&filtered, &expected_g);
        })
    }

    #[test]
    fn test_filter_is_none_simple_graph() {
        let graph = Graph::new();
        graph
            .add_node(1, 1, [("p1", 1), ("p2", 2)], Some("fire_nation"))
            .unwrap();
        graph
            .add_node(2, 1, [("p6", 6)], Some("fire_nation"))
            .unwrap();
        graph
            .add_node(2, 2, [("p4", 5)], Some("fire_nation"))
            .unwrap();
        graph
            .add_node(3, 3, [("p2", 4), ("p3", 3)], Some("water_tribe"))
            .unwrap();

        assert_eq!(graph.count_nodes(), 3);

        let filtered = graph.filter(NodeFilter::property("p2").is_none()).unwrap();
        let ids = filtered.nodes().name().collect_vec();

        assert_eq!(ids, vec!["2"]);
    }
}
