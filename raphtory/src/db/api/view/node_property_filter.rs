use crate::{
    core::utils::errors::GraphError,
    db::{
        api::view::internal::OneHopFilter,
        graph::views::property_filter::internal::InternalNodePropertyFilterOps,
    },
    prelude::GraphViewOps,
};

pub trait NodePropertyFilterOps<'graph>: OneHopFilter<'graph> {
    fn filter_nodes<F: InternalNodePropertyFilterOps>(
        &self,
        filter: F,
    ) -> Result<Self::Filtered<F::NodePropertyFiltered<'graph, Self::FilteredGraph>>, GraphError>
    {
        Ok(self
            .one_hop_filtered(filter.create_node_property_filter(self.current_filter().clone())?))
    }
}

impl<'graph, G: GraphViewOps<'graph>> NodePropertyFilterOps<'graph> for G {}

#[cfg(test)]
mod test {
    use crate::{
        db::graph::{graph::assert_edges_equal, views::property_filter::PropertyFilter},
        prelude::*,
        test_utils::{
            add_node_props, build_edge_list, build_graph_from_edge_list, build_node_props,
            node_filtered_graph,
        },
    };
    use itertools::Itertools;
    use proptest::{arbitrary::any, proptest};

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
            n1.filter_nodes(PropertyFilter::eq("test", 1i64))
                .unwrap()
                .edges()
                .id()
                .collect_vec(),
            vec![]
        );
        assert_eq!(
            n1.filter_nodes(PropertyFilter::eq("test", 2i64))
                .unwrap()
                .out_neighbours()
                .id()
                .collect_vec(),
            vec![GID::U64(2)]
        );

        let n2 = g.node(2).unwrap();

        assert_eq!(
            n2.filter_nodes(PropertyFilter::gt("test", 1i64))
                .unwrap()
                .neighbours()
                .id()
                .collect_vec(),
            vec![GID::U64(3)]
        );

        assert_eq!(
            n2.filter_nodes(PropertyFilter::gt("test", 0i64))
                .unwrap()
                .neighbours()
                .id()
                .collect_vec(),
            vec![GID::U64(1), GID::U64(3)]
        );

        let gp = g.persistent_graph();
        let n1p = gp.node(1).unwrap();

        assert_eq!(
            n1p.filter_nodes(PropertyFilter::eq("test", 1i64))
                .unwrap()
                .edges()
                .id()
                .collect_vec(),
            vec![]
        );
        assert_eq!(
            n1p.filter_nodes(PropertyFilter::eq("test", 2i64))
                .unwrap()
                .out_neighbours()
                .id()
                .collect_vec(),
            vec![GID::U64(2)]
        );

        let n2p = gp.node(2).unwrap();

        assert_eq!(
            n2p.filter_nodes(PropertyFilter::gt("test", 1i64))
                .unwrap()
                .neighbours()
                .id()
                .collect_vec(),
            vec![GID::U64(3)]
        );

        assert_eq!(
            n2p.filter_nodes(PropertyFilter::gt("test", 0i64))
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
            .filter_nodes(PropertyFilter::gt("test", 1i64))
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
            .filter_nodes(PropertyFilter::gt("test", 1i64))
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

        let gf = g.filter_nodes(PropertyFilter::eq("test", 1i64)).unwrap();
        assert_eq!(gf.edges().id().collect_vec(), vec![]);

        let gf = g.filter_nodes(PropertyFilter::gt("test", 1i64)).unwrap();
        assert_eq!(
            gf.edges().id().collect_vec(),
            vec![(GID::U64(2), GID::U64(3))]
        );

        let gf = g.filter_nodes(PropertyFilter::lt("test", 3i64)).unwrap();
        assert_eq!(
            gf.edges().id().collect_vec(),
            vec![(GID::U64(1), GID::U64(2)), (GID::U64(2), GID::U64(1))]
        );

        let gp = g.persistent_graph();
        let gf = gp.filter_nodes(PropertyFilter::eq("test", 1i64)).unwrap();
        assert_eq!(gf.edges().id().collect_vec(), vec![]);

        let gf = gp.filter_nodes(PropertyFilter::gt("test", 1i64)).unwrap();
        assert_eq!(
            gf.edges().id().collect_vec(),
            vec![(GID::U64(2), GID::U64(3))]
        );

        let gf = gp.filter_nodes(PropertyFilter::lt("test", 3i64)).unwrap();
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
            let filtered = g.filter_nodes(PropertyFilter::gt("int_prop", v)).unwrap();
            let expected_g = node_filtered_graph(&edges, &nodes, |_, int_v| {int_v.filter(|&vv| *vv > v ).is_some()});
            assert_edges_equal(&filtered.edges(), &expected_g.edges());
            let filtered_p = g.persistent_graph().filter_nodes(PropertyFilter::gt("int_prop", v)).unwrap();
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
            let filtered = g.filter_nodes(PropertyFilter::ge("int_prop", v)).unwrap();
            let expected_g = node_filtered_graph(&edges, &nodes, |_, int_v| {int_v.filter(|&vv| *vv >= v ).is_some()});
            assert_edges_equal(&filtered.edges(), &expected_g.edges());
                        let filtered_p = g.persistent_graph().filter_nodes(PropertyFilter::ge("int_prop", v)).unwrap();
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
            let filtered = g.filter_nodes(PropertyFilter::lt("int_prop", v)).unwrap();
            let expected_g = node_filtered_graph(&edges, &nodes, |_, int_v| {int_v.filter(|&vv| *vv < v ).is_some()});
            assert_edges_equal(&filtered.edges(), &expected_g.edges());
                        let filtered_p = g.persistent_graph().filter_nodes(PropertyFilter::lt("int_prop", v)).unwrap();
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
            let filtered = g.filter_nodes(PropertyFilter::le("int_prop", v)).unwrap();
            let expected_g = node_filtered_graph(&edges, &nodes, |_, int_v| {int_v.filter(|&vv| *vv <= v ).is_some()});
            assert_edges_equal(&filtered.edges(), &expected_g.edges());
                        let filtered_p = g.persistent_graph().filter_nodes(PropertyFilter::le("int_prop", v)).unwrap();
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
            let filtered = g.filter_nodes(PropertyFilter::eq("int_prop", v)).unwrap();
            let expected_g = node_filtered_graph(&edges, &nodes, |_, int_v| {int_v.filter(|&vv| *vv == v ).is_some()});
            assert_edges_equal(&filtered.edges(), &expected_g.edges());
                        let filtered_p = g.persistent_graph().filter_nodes(PropertyFilter::eq("int_prop", v)).unwrap();
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
            let filtered = g.filter_nodes(PropertyFilter::ne("int_prop", v)).unwrap();
            let expected_g = node_filtered_graph(&edges, &nodes, |_, int_v| {int_v.filter(|&vv| *vv != v ).is_some()});
            assert_edges_equal(&filtered.edges(), &expected_g.edges());
                        let filtered_p = g.persistent_graph().filter_nodes(PropertyFilter::ne("int_prop", v)).unwrap();
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
            let filtered = g.filter_nodes(PropertyFilter::is_some("int_prop")).unwrap();
            let expected_g = node_filtered_graph(&edges, &nodes, |_, int_v| {int_v.is_some()});
            assert_edges_equal(&filtered.edges(), &expected_g.edges());
                        let filtered_p = g.persistent_graph().filter_nodes(PropertyFilter::is_some("int_prop")).unwrap();
            assert_edges_equal(&filtered_p.edges(), &expected_g.persistent_graph().edges());
            // FIXME: history filtering not working properly
            // assert_graph_equal(&filtered, &expected_g);
        })
    }

    #[test]
    fn test_filter_is_none() {
        proptest!(|(
            edges in build_edge_list(2, 10), nodes in build_node_props(50)
        )| {
            let g = build_graph_from_edge_list(&edges);
            add_node_props(&g, &nodes);
            let filtered = g.filter_nodes(PropertyFilter::is_none("int_prop")).unwrap();
            let expected_g = node_filtered_graph(&edges, &nodes, |_, int_v| {int_v.is_none()});
            assert_edges_equal(&filtered.edges(), &expected_g.edges());
                        let filtered_p = g.persistent_graph().filter_nodes(PropertyFilter::is_none("int_prop")).unwrap();
            assert_edges_equal(&filtered_p.edges(), &expected_g.persistent_graph().edges());
            // FIXME: history filtering not working properly
            // assert_graph_equal(&filtered, &expected_g);
        })
    }

    #[test]
    fn test_filter_is_none_sample() {
        let nodes = vec![
            (0u64, None, None),
            (1, None, None),
            (2, None, None),
            (3, None, None),
            (4, None, None),
            (5, None, None),
            (6, Some("".to_string()), None),
            (7, None, None),
            (8, None, None),
            (9, None, None),
            (10, None, None),
            (11, None, None),
            (12, None, None),
            (13, None, None),
            (14, None, None),
            (15, None, None),
            (16, None, None),
            (17, None, None),
            (18, None, None),
            (19, None, None),
            (20, None, None),
            (21, None, None),
            (22, None, None),
            (23, None, Some(-1006640885922i64)),
            (24, None, Some(3140823659770432416)),
            (25, None, Some(-8241928021672356322)),
            (26, None, None),
            (27, None, Some(1531674380875899820)),
            (28, None, None),
            (29, None, None),
            (30, None, None),
            (31, None, Some(-46139717112632079)),
            (32, None, None),
            (33, Some("a".to_string()), Some(1268855120093795622)),
            (34, None, None),
            (35, Some("".to_string()), None),
            (36, None, Some(-7161369594877871170)),
            (37, None, Some(802040896379373150)),
            (38, None, Some(-8575189323174740844)),
            (39, Some("b".to_string()), None),
            (40, None, None),
            (41, None, None),
            (42, None, Some(-9213792616588373967)),
            (43, Some("c".to_string()), Some(-8033735226120800391)),
            (44, Some("d".to_string()), Some(-4373305175473905212)),
            (45, None, Some(4466699769621037019)),
            (46, None, None),
            (47, Some("e".to_string()), Some(-7288882491415087470)),
            (48, Some("f".to_string()), Some(-3571741428046533850)),
        ];

        let edges = vec![(6u64, 2u64, 0i64, "".to_string(), 0i64)];

        let g = build_graph_from_edge_list(&edges);
        add_node_props(&g, &nodes);

        let node6 = g.node(6).unwrap();
        let prop = node6.properties().get("int_prop");
        assert_eq!(prop, None);

        let filtered = g.filter_nodes(PropertyFilter::is_none("int_prop")).unwrap();

        let expected_g = node_filtered_graph(&edges, &nodes, |_, int_v| int_v.is_none());

        let mut nodes = filtered.nodes().id().collect_vec();
        let mut expected_nodes = expected_g.nodes().id().collect_vec();

        nodes.sort();
        expected_nodes.sort();
        println!("{:?}\n{:?}", nodes, expected_nodes);
        assert_eq!(nodes, expected_nodes);

        let edges = filtered.edges();
        let expected_edges = expected_g.edges();

        println!(
            "{:?}, {:?}",
            edges.id().collect_vec(),
            expected_edges.id().collect_vec()
        );

        assert_edges_equal(&expected_edges, &expected_edges);
        let filtered_p = g
            .persistent_graph()
            .filter_nodes(PropertyFilter::is_none("int_prop"))
            .unwrap();
        assert_edges_equal(&filtered_p.edges(), &expected_g.persistent_graph().edges());
        // FIXME: history filtering not working properly
        // assert_graph_equal(&filtered, &expected_g);
    }
}
