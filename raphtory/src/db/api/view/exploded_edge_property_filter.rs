use crate::{
    core::utils::errors::GraphError,
    db::{
        api::view::internal::OneHopFilter,
        graph::views::property_filter::internal::InternalExplodedEdgeFilterOps,
    },
};

pub trait ExplodedEdgePropertyFilterOps<'graph>: OneHopFilter<'graph> {
    fn filter_exploded_edges<F: InternalExplodedEdgeFilterOps>(
        &self,
        filter: F,
    ) -> Result<Self::Filtered<F::ExplodedEdgeFiltered<'graph, Self::FilteredGraph>>, GraphError>;
}

impl<'graph, G: OneHopFilter<'graph> + 'graph> ExplodedEdgePropertyFilterOps<'graph> for G {
    fn filter_exploded_edges<F: InternalExplodedEdgeFilterOps>(
        &self,
        filter: F,
    ) -> Result<Self::Filtered<F::ExplodedEdgeFiltered<'graph, Self::FilteredGraph>>, GraphError>
    {
        let graph = filter.create_exploded_edge_filter(self.current_filter().clone())?;
        Ok(self.one_hop_filtered(graph))
    }
}

#[cfg(test)]
mod test {
    use crate::{
        db::{
            api::view::{
                exploded_edge_property_filter::ExplodedEdgePropertyFilterOps, node::NodeViewOps,
            },
            graph::{graph::assert_graph_equal, views::deletion_graph::PersistentGraph},
        },
        prelude::*,
        test_utils::{build_edge_list, build_graph_from_edge_list},
    };
    use itertools::Itertools;
    use proptest::{arbitrary::any, proptest};

    fn build_filtered_graph(
        edges: &[(u64, u64, i64, String, i64)],
        filter: impl Fn(i64) -> bool,
    ) -> Graph {
        let g = Graph::new();
        for (src, dst, t, str_prop, int_prop) in edges {
            g.add_node(*t, *src, NO_PROPS, None).unwrap();
            g.add_node(*t, *dst, NO_PROPS, None).unwrap();
            if filter(*int_prop) {
                g.add_edge(
                    *t,
                    *src,
                    *dst,
                    [
                        ("str_prop", str_prop.into()),
                        ("int_prop", Prop::I64(*int_prop)),
                    ],
                    None,
                )
                .unwrap();
            }
        }
        g
    }

    #[test]
    fn test_filter_gt() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter_exploded_edges(PropertyFilter::gt("int_prop", v)).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv > v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_one_edge() {
        let g = Graph::new();
        g.add_edge(0, 1, 2, [("int_prop", 0i64)], None).unwrap();
        let filtered = g
            .filter_exploded_edges(PropertyFilter::gt("int_prop", 1i64))
            .unwrap();
        let edges = filtered
            .edges()
            .explode()
            .iter()
            .map(|e| (e.src().id(), e.dst().id()))
            .collect_vec();
        let gf = Graph::new();
        gf.add_node(0, 1, NO_PROPS, None).unwrap();
        gf.add_node(0, 2, NO_PROPS, None).unwrap();

        println!("{:?}", edges);
        assert_graph_equal(&filtered, &gf);
    }

    #[test]
    fn test_filter_ge() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter_exploded_edges(PropertyFilter::ge("int_prop", v)).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv >= v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_filter_lt() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter_exploded_edges( PropertyFilter::lt("int_prop", v)).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv < v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_filter_le() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter_exploded_edges(PropertyFilter::le("int_prop", v)).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv <= v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_filter_eq() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter_exploded_edges(PropertyFilter::eq("int_prop", v)).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv == v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_filter_ne() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter_exploded_edges( PropertyFilter::ne("int_prop", v)).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv != v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_persistent_graph() {
        let g = PersistentGraph::new();
        g.add_edge(0, 1, 2, [("int_prop", 0i64)], None).unwrap();
        g.delete_edge(2, 1, 2, None).unwrap();
        g.add_edge(5, 1, 2, [("int_prop", 5i64)], None).unwrap();

        let edges = g
            .node(1)
            .unwrap()
            .filter_exploded_edges(PropertyFilter::gt("int_prop", 1i64))
            .unwrap()
            .edges()
            .explode()
            .collect();
        println!("{:?}", edges);

        assert_eq!(edges.len(), 1);
    }
}
