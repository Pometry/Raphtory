use crate::{
    core::utils::errors::GraphError,
    db::{
        api::view::internal::OneHopFilter,
        graph::views::property_filter::internal::InternalNodePropertyFilterOps,
    },
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

impl<'graph, G: OneHopFilter<'graph>> NodePropertyFilterOps<'graph> for G {}

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
            let filtered = g.filter_nodes(PropertyFilter::is_none("int_prop")).unwrap();
            let expected_g = node_filtered_graph(&edges, &nodes, |_, int_v| {int_v.is_none()});
            assert_edges_equal(&filtered.edges(), &expected_g.edges());
            // FIXME: history filtering not working properly
            // assert_graph_equal(&filtered, &expected_g);
        })
    }
}
