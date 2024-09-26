use crate::{
    core::{entities::properties::props::Meta, utils::errors::GraphError, PropType},
    db::{
        api::view::internal::{CoreGraphOps, OneHopFilter},
        graph::views::property_filter::{
            exploded_edge_property_filter::ExplodedEdgePropertyFilteredGraph,
            PropertyFilter,
        },
    },
};

pub trait ExplodedEdgePropertyFilterOps<'graph> {
    type FilteredViewType;

    fn filter_exploded_edges(
        &self,
        property: &str,
        filter: PropertyFilter,
    ) -> Result<Self::FilteredViewType, GraphError>;
}

fn get_id_and_check_type(
    meta: &Meta,
    property: &str,
    dtype: PropType,
) -> Result<Option<usize>, GraphError> {
    let t_prop_id = meta
        .temporal_prop_meta()
        .get_and_validate(property, dtype)?;
    Ok(t_prop_id)
}

fn get_id(meta: &Meta, property: &str) -> Option<usize> {
    let t_prop_id = meta.temporal_prop_meta().get_id(property);
    t_prop_id
}

impl<'graph, G: OneHopFilter<'graph> + 'graph> ExplodedEdgePropertyFilterOps<'graph> for G {
    type FilteredViewType = G::Filtered<ExplodedEdgePropertyFilteredGraph<G::FilteredGraph>>;

    fn filter_exploded_edges(
        &self,
        property: &str,
        filter: PropertyFilter,
    ) -> Result<Self::FilteredViewType, GraphError> {
        let t_prop_id = match &filter {
            PropertyFilter::ByValue(filter) => {
                get_id_and_check_type(self.current_filter().edge_meta(), property, filter.dtype())?
            }
            _ => get_id(self.current_filter().edge_meta(), property),
        };
        Ok(
            self.one_hop_filtered(ExplodedEdgePropertyFilteredGraph::new(
                self.current_filter().clone(),
                t_prop_id,
                filter,
            )),
        )
    }
}

#[cfg(test)]
mod test {
    use crate::{
        db::{
            api::view::{
                exploded_edge_property_filter::ExplodedEdgePropertyFilterOps, node::NodeViewOps,
            },
            graph::graph::assert_graph_equal,
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
            let filtered = g.filter_exploded_edges("int_prop", PropertyFilter::gt(v)).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv > v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_one_edge() {
        let g = Graph::new();
        g.add_edge(0, 1, 2, [("int_prop", 0i64)], None).unwrap();
        let filtered = g
            .filter_exploded_edges("int_prop", PropertyFilter::gt(1i64))
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
            let filtered = g.filter_exploded_edges("int_prop", PropertyFilter::ge(v)).unwrap();
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
            let filtered = g.filter_exploded_edges("int_prop", PropertyFilter::lt(v)).unwrap();
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
            let filtered = g.filter_exploded_edges("int_prop", PropertyFilter::le(v)).unwrap();
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
            let filtered = g.filter_exploded_edges("int_prop", PropertyFilter::eq(v)).unwrap();
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
            let filtered = g.filter_exploded_edges("int_prop", PropertyFilter::ne(v)).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv != v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }
}
