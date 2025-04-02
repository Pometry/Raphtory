use crate::{
    core::utils::errors::GraphError,
    db::{
        api::view::internal::OneHopFilter, graph::views::filter::internal::InternalNodeFilterOps,
    },
    prelude::GraphViewOps,
};

pub trait NodePropertyFilterOps<'graph>: OneHopFilter<'graph> {
    fn filter_nodes<F: InternalNodeFilterOps>(
        &self,
        filter: F,
    ) -> Result<Self::Filtered<F::NodeFiltered<'graph, Self::FilteredGraph>>, GraphError> {
        Ok(self.one_hop_filtered(filter.create_node_filter(self.current_filter().clone())?))
    }
}

impl<'graph, G: GraphViewOps<'graph>> NodePropertyFilterOps<'graph> for G {}

#[cfg(test)]
mod test {
    use crate::{
        db::{
            api::view::BaseNodeViewOps,
            graph::{
                graph::assert_edges_equal,
                views::filter::{
                    ComposableFilter, CompositeNodeFilter, Filter, NodeFilter, NodeFilterOps,
                    PropertyFilter, PropertyFilterOps, PropertyRef,
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

        let filter_expr = NodeFilter::node_name().eq("Jimi");
        let filter_expr = NodeFilter::node_name()
            .eq("John")
            // .and(PropertyFilter::property("band").eq("Dead & Company"))
            .and(PropertyFilter::property("band").eq("Dead & Company"));
        // let filter_expr = CompositeNodeFilter::Node(Filter::eq("node_name", "Jimi"));
        let filtered_nodes = g.nodes().filter_nodes(filter_expr).unwrap();

        assert_eq!(
            filtered_nodes.iter().map(|n| n.name()).collect::<Vec<_>>(),
            // vec!["Jimi"]
            vec!["John"]
        );
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
            n1.filter_nodes(PropertyFilter::eq(
                PropertyRef::Property("test".to_string()),
                1i64
            ))
            .unwrap()
            .edges()
            .id()
            .collect_vec(),
            vec![]
        );
        assert_eq!(
            n1.filter_nodes(PropertyFilter::eq(
                PropertyRef::Property("test".to_string()),
                2i64
            ))
            .unwrap()
            .out_neighbours()
            .id()
            .collect_vec(),
            vec![GID::U64(2)]
        );

        let n2 = g.node(2).unwrap();

        assert_eq!(
            n2.filter_nodes(PropertyFilter::gt(
                PropertyRef::Property("test".to_string()),
                1i64
            ))
            .unwrap()
            .neighbours()
            .id()
            .collect_vec(),
            vec![GID::U64(3)]
        );

        assert_eq!(
            n2.filter_nodes(PropertyFilter::gt(
                PropertyRef::Property("test".to_string()),
                0i64
            ))
            .unwrap()
            .neighbours()
            .id()
            .collect_vec(),
            vec![GID::U64(1), GID::U64(3)]
        );

        let gp = g.persistent_graph();
        let n1p = gp.node(1).unwrap();

        assert_eq!(
            n1p.filter_nodes(PropertyFilter::eq(
                PropertyRef::Property("test".to_string()),
                1i64
            ))
            .unwrap()
            .edges()
            .id()
            .collect_vec(),
            vec![]
        );
        assert_eq!(
            n1p.filter_nodes(PropertyFilter::eq(
                PropertyRef::Property("test".to_string()),
                2i64
            ))
            .unwrap()
            .out_neighbours()
            .id()
            .collect_vec(),
            vec![GID::U64(2)]
        );

        let n2p = gp.node(2).unwrap();

        assert_eq!(
            n2p.filter_nodes(PropertyFilter::gt(
                PropertyRef::Property("test".to_string()),
                1i64
            ))
            .unwrap()
            .neighbours()
            .id()
            .collect_vec(),
            vec![GID::U64(3)]
        );

        assert_eq!(
            n2p.filter_nodes(PropertyFilter::gt(
                PropertyRef::Property("test".to_string()),
                0i64
            ))
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
            .filter_nodes(PropertyFilter::gt(
                PropertyRef::Property("test".to_string()),
                1i64,
            ))
            .unwrap();
        assert_eq!(
            filtered_nodes.id().collect_vec(),
            vec![GID::U64(2), GID::U64(3)]
        );
        assert_eq!(
            filtered_nodes
                .out_neighbours()
                .id()
                .map(|n| n.collect_vec())
                .collect_vec(),
            vec![vec![GID::U64(3)], vec![]]
        );

        let filtered_nodes_p = g
            .persistent_graph()
            .nodes()
            .filter_nodes(PropertyFilter::gt(
                PropertyRef::Property("test".to_string()),
                1i64,
            ))
            .unwrap();
        assert_eq!(
            filtered_nodes_p.id().collect_vec(),
            vec![GID::U64(2), GID::U64(3)]
        );
        assert_eq!(
            filtered_nodes_p
                .out_neighbours()
                .id()
                .map(|n| n.collect_vec())
                .collect_vec(),
            vec![vec![GID::U64(3)], vec![]]
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

        let gf = g
            .filter_nodes(PropertyFilter::eq(
                PropertyRef::Property("test".to_string()),
                1i64,
            ))
            .unwrap();
        assert_eq!(gf.edges().id().collect_vec(), vec![]);

        let gf = g
            .filter_nodes(PropertyFilter::gt(
                PropertyRef::Property("test".to_string()),
                1i64,
            ))
            .unwrap();
        assert_eq!(
            gf.edges().id().collect_vec(),
            vec![(GID::U64(2), GID::U64(3))]
        );

        let gf = g
            .filter_nodes(PropertyFilter::lt(
                PropertyRef::Property("test".to_string()),
                3i64,
            ))
            .unwrap();
        assert_eq!(
            gf.edges().id().collect_vec(),
            vec![(GID::U64(1), GID::U64(2)), (GID::U64(2), GID::U64(1))]
        );

        let gp = g.persistent_graph();
        let gf = gp
            .filter_nodes(PropertyFilter::eq(
                PropertyRef::Property("test".to_string()),
                1i64,
            ))
            .unwrap();
        assert_eq!(gf.edges().id().collect_vec(), vec![]);

        let gf = gp
            .filter_nodes(PropertyFilter::gt(
                PropertyRef::Property("test".to_string()),
                1i64,
            ))
            .unwrap();
        assert_eq!(
            gf.edges().id().collect_vec(),
            vec![(GID::U64(2), GID::U64(3))]
        );

        let gf = gp
            .filter_nodes(PropertyFilter::lt(
                PropertyRef::Property("test".to_string()),
                3i64,
            ))
            .unwrap();
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
            let filtered = g.filter_nodes(PropertyFilter::gt(PropertyRef::Property("int_prop".to_string()), v)).unwrap();
            let expected_g = node_filtered_graph(&edges, &nodes, |_, int_v| {int_v.filter(|&vv| *vv > v ).is_some()});
            assert_edges_equal(&filtered.edges(), &expected_g.edges());
            let filtered_p = g.persistent_graph().filter_nodes(PropertyFilter::gt(PropertyRef::Property("int_prop".to_string()), v)).unwrap();
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
            let filtered = g.filter_nodes(PropertyFilter::ge(PropertyRef::Property("int_prop".to_string()), v)).unwrap();
            let expected_g = node_filtered_graph(&edges, &nodes, |_, int_v| {int_v.filter(|&vv| *vv >= v ).is_some()});
            assert_edges_equal(&filtered.edges(), &expected_g.edges());
                        let filtered_p = g.persistent_graph().filter_nodes(PropertyFilter::ge(PropertyRef::Property("int_prop".to_string()), v)).unwrap();
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
            let filtered = g.filter_nodes(PropertyFilter::lt(PropertyRef::Property("int_prop".to_string()), v)).unwrap();
            let expected_g = node_filtered_graph(&edges, &nodes, |_, int_v| {int_v.filter(|&vv| *vv < v ).is_some()});
            assert_edges_equal(&filtered.edges(), &expected_g.edges());
                        let filtered_p = g.persistent_graph().filter_nodes(PropertyFilter::lt(PropertyRef::Property("int_prop".to_string()), v)).unwrap();
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
            let filtered = g.filter_nodes(PropertyFilter::le(PropertyRef::Property("int_prop".to_string()), v)).unwrap();
            let expected_g = node_filtered_graph(&edges, &nodes, |_, int_v| {int_v.filter(|&vv| *vv <= v ).is_some()});
            assert_edges_equal(&filtered.edges(), &expected_g.edges());
                        let filtered_p = g.persistent_graph().filter_nodes(PropertyFilter::le(PropertyRef::Property("int_prop".to_string()), v)).unwrap();
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
            let filtered = g.filter_nodes(PropertyFilter::eq(PropertyRef::Property("int_prop".to_string()), v)).unwrap();
            let expected_g = node_filtered_graph(&edges, &nodes, |_, int_v| {int_v.filter(|&vv| *vv == v ).is_some()});
            assert_edges_equal(&filtered.edges(), &expected_g.edges());
                        let filtered_p = g.persistent_graph().filter_nodes(PropertyFilter::eq(PropertyRef::Property("int_prop".to_string()), v)).unwrap();
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
            let filtered = g.filter_nodes(PropertyFilter::ne(PropertyRef::Property("int_prop".to_string()), v)).unwrap();
            let expected_g = node_filtered_graph(&edges, &nodes, |_, int_v| {int_v.filter(|&vv| *vv != v ).is_some()});
            assert_edges_equal(&filtered.edges(), &expected_g.edges());
                        let filtered_p = g.persistent_graph().filter_nodes(PropertyFilter::ne(PropertyRef::Property("int_prop".to_string()), v)).unwrap();
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
            let filtered = g.filter_nodes(PropertyFilter::is_some(PropertyRef::Property("int_prop".to_string()))).unwrap();
            let expected_g = node_filtered_graph(&edges, &nodes, |_, int_v| {int_v.is_some()});
            assert_edges_equal(&filtered.edges(), &expected_g.edges());
                        let filtered_p = g.persistent_graph().filter_nodes(PropertyFilter::is_some(PropertyRef::Property("int_prop".to_string()))).unwrap();
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
            let filtered = g.filter_nodes(PropertyFilter::is_none(PropertyRef::Property("int_prop".to_string()))).unwrap();
            let expected_g = node_filtered_graph(&edges, &nodes, |_, int_v| {int_v.is_none()});
            assert_edges_equal(&filtered.edges(), &expected_g.edges());
                        let filtered_p = g.persistent_graph().filter_nodes(PropertyFilter::is_none(PropertyRef::Property("int_prop".to_string()))).unwrap();
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

        let filtered = graph
            .filter_nodes(PropertyFilter::is_none(PropertyRef::Property(
                "p2".to_string(),
            )))
            .unwrap();
        let ids = filtered.nodes().name().collect_vec();

        assert_eq!(ids, vec!["2"]);
    }
}
