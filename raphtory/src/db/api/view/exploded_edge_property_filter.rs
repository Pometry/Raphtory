use crate::{
    core::{entities::properties::props::Meta, utils::errors::GraphError, Prop, PropType},
    db::{
        api::view::internal::{CoreGraphOps, OneHopFilter},
        graph::views::property_filter::{
            exploded_edge_property_filter::ExplodedEdgePropertyFilteredGraph, PropFilter,
            PropValueFilter,
        },
    },
};
use std::{collections::HashSet, sync::Arc};

pub trait ExplodedEdgePropertyFilterOps<'graph> {
    type FilteredViewType;
    fn filter_exploded_edges_eq(
        &self,
        property: &str,
        value: Prop,
    ) -> Result<Self::FilteredViewType, GraphError>;

    fn filter_exploded_edges_lt(
        &self,
        property: &str,
        value: Prop,
    ) -> Result<Self::FilteredViewType, GraphError>;

    fn filter_exploded_edges_gt(
        &self,
        property: &str,
        value: Prop,
    ) -> Result<Self::FilteredViewType, GraphError>;
    fn filter_exploded_edges_le(
        &self,
        property: &str,
        value: Prop,
    ) -> Result<Self::FilteredViewType, GraphError>;
    fn filter_exploded_edges_ne(
        &self,
        property: &str,
        value: Prop,
    ) -> Result<Self::FilteredViewType, GraphError>;
    fn filter_exploded_edges_ge(
        &self,
        property: &str,
        value: Prop,
    ) -> Result<Self::FilteredViewType, GraphError>;
    fn filter_exploded_edges_has(&self, property: &str) -> Self::FilteredViewType;
    fn filter_exploded_edges_has_not(&self, property: &str) -> Self::FilteredViewType;
    fn filter_exploded_edges_in(
        &self,
        property: &str,
        set: HashSet<Prop>,
    ) -> Self::FilteredViewType;
    fn filter_exploded_edges_not_in(
        &self,
        property: &str,
        set: HashSet<Prop>,
    ) -> Self::FilteredViewType;
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

    fn filter_exploded_edges_lt(
        &self,
        property: &str,
        value: Prop,
    ) -> Result<Self::FilteredViewType, GraphError> {
        let t_prop_id =
            get_id_and_check_type(self.current_filter().edge_meta(), property, value.dtype())?;
        Ok(
            self.one_hop_filtered(ExplodedEdgePropertyFilteredGraph::new(
                self.current_filter().clone(),
                t_prop_id,
                PropValueFilter::new(value, |left, right| left < right),
            )),
        )
    }

    fn filter_exploded_edges_le(
        &self,
        property: &str,
        value: Prop,
    ) -> Result<Self::FilteredViewType, GraphError> {
        let t_prop_id =
            get_id_and_check_type(self.current_filter().edge_meta(), property, value.dtype())?;
        Ok(
            self.one_hop_filtered(ExplodedEdgePropertyFilteredGraph::new(
                self.current_filter().clone(),
                t_prop_id,
                PropValueFilter::new(value, |left, right| left <= right),
            )),
        )
    }

    fn filter_exploded_edges_eq(
        &self,
        property: &str,
        value: Prop,
    ) -> Result<Self::FilteredViewType, GraphError> {
        let t_prop_id =
            get_id_and_check_type(self.current_filter().edge_meta(), property, value.dtype())?;

        Ok(
            self.one_hop_filtered(ExplodedEdgePropertyFilteredGraph::new(
                self.current_filter().clone(),
                t_prop_id,
                PropValueFilter::new(value, |left, right| left == right),
            )),
        )
    }

    fn filter_exploded_edges_ne(
        &self,
        property: &str,
        value: Prop,
    ) -> Result<Self::FilteredViewType, GraphError> {
        let t_prop_id =
            get_id_and_check_type(self.current_filter().edge_meta(), property, value.dtype())?;

        Ok(
            self.one_hop_filtered(ExplodedEdgePropertyFilteredGraph::new(
                self.current_filter().clone(),
                t_prop_id,
                PropValueFilter::new(value, |left, right| left != right),
            )),
        )
    }

    fn filter_exploded_edges_gt(
        &self,
        property: &str,
        value: Prop,
    ) -> Result<Self::FilteredViewType, GraphError> {
        let t_prop_id =
            get_id_and_check_type(self.current_filter().edge_meta(), property, value.dtype())?;

        Ok(
            self.one_hop_filtered(ExplodedEdgePropertyFilteredGraph::new(
                self.current_filter().clone(),
                t_prop_id,
                PropValueFilter::new(value, |left, right| left > right),
            )),
        )
    }

    fn filter_exploded_edges_ge(
        &self,
        property: &str,
        value: Prop,
    ) -> Result<Self::FilteredViewType, GraphError> {
        let t_prop_id =
            get_id_and_check_type(self.current_filter().edge_meta(), property, value.dtype())?;

        Ok(
            self.one_hop_filtered(ExplodedEdgePropertyFilteredGraph::new(
                self.current_filter().clone(),
                t_prop_id,
                PropValueFilter::new(value, |left, right| left >= right),
            )),
        )
    }

    fn filter_exploded_edges_has(&self, property: &str) -> Self::FilteredViewType {
        let t_prop_id = get_id(self.current_filter().edge_meta(), property);
        self.one_hop_filtered(ExplodedEdgePropertyFilteredGraph::new(
            self.current_filter().clone(),
            t_prop_id,
            PropFilter::Has,
        ))
    }

    fn filter_exploded_edges_has_not(&self, property: &str) -> Self::FilteredViewType {
        let t_prop_id = get_id(self.current_filter().edge_meta(), property);
        self.one_hop_filtered(ExplodedEdgePropertyFilteredGraph::new(
            self.current_filter().clone(),
            t_prop_id,
            PropFilter::HasNot,
        ))
    }

    fn filter_exploded_edges_in(
        &self,
        property: &str,
        set: HashSet<Prop>,
    ) -> Self::FilteredViewType {
        let t_prop_id = get_id(self.current_filter().edge_meta(), property);
        self.one_hop_filtered(ExplodedEdgePropertyFilteredGraph::new(
            self.current_filter().clone(),
            t_prop_id,
            PropFilter::In(Arc::new(set)),
        ))
    }

    fn filter_exploded_edges_not_in(
        &self,
        property: &str,
        set: HashSet<Prop>,
    ) -> Self::FilteredViewType {
        let t_prop_id = get_id(self.current_filter().edge_meta(), property);
        self.one_hop_filtered(ExplodedEdgePropertyFilteredGraph::new(
            self.current_filter().clone(),
            t_prop_id,
            PropFilter::NotIn(Arc::new(set)),
        ))
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
            let filtered = g.filter_exploded_edges_gt("int_prop", v.into_prop()).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv > v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_one_edge() {
        let g = Graph::new();
        g.add_edge(0, 1, 2, [("int_prop", 0i64)], None).unwrap();
        let filtered = g.filter_exploded_edges_gt("int_prop", 1i64.into()).unwrap();
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
            let filtered = g.filter_exploded_edges_ge("int_prop", v.into_prop()).unwrap();
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
            let filtered = g.filter_exploded_edges_lt("int_prop", v.into_prop()).unwrap();
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
            let filtered = g.filter_exploded_edges_le("int_prop", v.into_prop()).unwrap();
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
            let filtered = g.filter_exploded_edges_eq("int_prop", v.into_prop()).unwrap();
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
            let filtered = g.filter_exploded_edges_ne("int_prop", v.into_prop()).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv != v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }
}
