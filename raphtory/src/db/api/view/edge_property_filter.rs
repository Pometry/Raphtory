use crate::{
    core::{entities::properties::props::Meta, utils::errors::GraphError, Prop, PropType},
    db::{
        api::view::internal::{CoreGraphOps, OneHopFilter},
        graph::views::property_filter::{
            edge_property_filter::EdgePropertyFilteredGraph, PropFilter, PropValueFilter,
        },
    },
};
use std::{collections::HashSet, sync::Arc};

pub trait EdgePropertyFilterOps<'graph> {
    type FilteredViewType;

    fn filter_edges_eq(
        &self,
        property: &str,
        value: Prop,
    ) -> Result<Self::FilteredViewType, GraphError>;

    fn filter_edges_lt(
        &self,
        property: &str,
        value: Prop,
    ) -> Result<Self::FilteredViewType, GraphError>;

    fn filter_edges_gt(
        &self,
        property: &str,
        value: Prop,
    ) -> Result<Self::FilteredViewType, GraphError>;
    fn filter_edges_le(
        &self,
        property: &str,
        value: Prop,
    ) -> Result<Self::FilteredViewType, GraphError>;
    fn filter_edges_ne(
        &self,
        property: &str,
        value: Prop,
    ) -> Result<Self::FilteredViewType, GraphError>;
    fn filter_edges_ge(
        &self,
        property: &str,
        value: Prop,
    ) -> Result<Self::FilteredViewType, GraphError>;
    fn filter_edges_has(&self, property: &str) -> Self::FilteredViewType;
    fn filter_edges_has_not(&self, property: &str) -> Self::FilteredViewType;
    fn filter_edges_in(&self, property: &str, set: HashSet<Prop>) -> Self::FilteredViewType;
    fn filter_edges_not_in(&self, property: &str, set: HashSet<Prop>) -> Self::FilteredViewType;
}

fn get_ids_and_check_type(
    meta: &Meta,
    property: &str,
    dtype: PropType,
) -> Result<(Option<usize>, Option<usize>), GraphError> {
    let t_prop_id = meta
        .temporal_prop_meta()
        .get_and_validate(property, dtype)?;
    let c_prop_id = meta.const_prop_meta().get_and_validate(property, dtype)?;
    Ok((t_prop_id, c_prop_id))
}

fn get_ids(meta: &Meta, property: &str) -> (Option<usize>, Option<usize>) {
    let t_prop_id = meta.temporal_prop_meta().get_id(property);
    let c_prop_id = meta.const_prop_meta().get_id(property);
    (t_prop_id, c_prop_id)
}

impl<'graph, G: OneHopFilter<'graph>> EdgePropertyFilterOps<'graph> for G {
    type FilteredViewType = G::Filtered<EdgePropertyFilteredGraph<G::FilteredGraph>>;

    fn filter_edges_lt(
        &self,
        property: &str,
        value: Prop,
    ) -> Result<Self::FilteredViewType, GraphError> {
        let (t_prop_id, c_prop_id) =
            get_ids_and_check_type(self.current_filter().edge_meta(), property, value.dtype())?;
        Ok(self.one_hop_filtered(EdgePropertyFilteredGraph::new(
            self.current_filter().clone(),
            t_prop_id,
            c_prop_id,
            PropValueFilter::new(value, |left, right| left < right),
        )))
    }

    fn filter_edges_le(
        &self,
        property: &str,
        value: Prop,
    ) -> Result<Self::FilteredViewType, GraphError> {
        let (t_prop_id, c_prop_id) =
            get_ids_and_check_type(self.current_filter().edge_meta(), property, value.dtype())?;
        Ok(self.one_hop_filtered(EdgePropertyFilteredGraph::new(
            self.current_filter().clone(),
            t_prop_id,
            c_prop_id,
            PropValueFilter::new(value, |left, right| left <= right),
        )))
    }

    fn filter_edges_eq(
        &self,
        property: &str,
        value: Prop,
    ) -> Result<Self::FilteredViewType, GraphError> {
        let (t_prop_id, c_prop_id) =
            get_ids_and_check_type(self.current_filter().edge_meta(), property, value.dtype())?;

        Ok(self.one_hop_filtered(EdgePropertyFilteredGraph::new(
            self.current_filter().clone(),
            t_prop_id,
            c_prop_id,
            PropValueFilter::new(value, |left, right| left == right),
        )))
    }

    fn filter_edges_ne(
        &self,
        property: &str,
        value: Prop,
    ) -> Result<Self::FilteredViewType, GraphError> {
        let (t_prop_id, c_prop_id) =
            get_ids_and_check_type(self.current_filter().edge_meta(), property, value.dtype())?;

        Ok(self.one_hop_filtered(EdgePropertyFilteredGraph::new(
            self.current_filter().clone(),
            t_prop_id,
            c_prop_id,
            PropValueFilter::new(value, |left, right| left != right),
        )))
    }

    fn filter_edges_gt(
        &self,
        property: &str,
        value: Prop,
    ) -> Result<Self::FilteredViewType, GraphError> {
        let (t_prop_id, c_prop_id) =
            get_ids_and_check_type(self.current_filter().edge_meta(), property, value.dtype())?;

        Ok(self.one_hop_filtered(EdgePropertyFilteredGraph::new(
            self.current_filter().clone(),
            t_prop_id,
            c_prop_id,
            PropValueFilter::new(value, |left, right| left > right),
        )))
    }

    fn filter_edges_ge(
        &self,
        property: &str,
        value: Prop,
    ) -> Result<Self::FilteredViewType, GraphError> {
        let (t_prop_id, c_prop_id) =
            get_ids_and_check_type(self.current_filter().edge_meta(), property, value.dtype())?;

        Ok(self.one_hop_filtered(EdgePropertyFilteredGraph::new(
            self.current_filter().clone(),
            t_prop_id,
            c_prop_id,
            PropValueFilter::new(value, |left, right| left >= right),
        )))
    }

    fn filter_edges_has(&self, property: &str) -> Self::FilteredViewType {
        let (t_prop_id, c_prop_id) = get_ids(self.current_filter().edge_meta(), property);
        self.one_hop_filtered(EdgePropertyFilteredGraph::new(
            self.current_filter().clone(),
            t_prop_id,
            c_prop_id,
            PropFilter::Has,
        ))
    }

    fn filter_edges_has_not(&self, property: &str) -> Self::FilteredViewType {
        let (t_prop_id, c_prop_id) = get_ids(self.current_filter().edge_meta(), property);
        self.one_hop_filtered(EdgePropertyFilteredGraph::new(
            self.current_filter().clone(),
            t_prop_id,
            c_prop_id,
            PropFilter::HasNot,
        ))
    }

    fn filter_edges_in(&self, property: &str, set: HashSet<Prop>) -> Self::FilteredViewType {
        let (t_prop_id, c_prop_id) = get_ids(self.current_filter().edge_meta(), property);
        self.one_hop_filtered(EdgePropertyFilteredGraph::new(
            self.current_filter().clone(),
            t_prop_id,
            c_prop_id,
            PropFilter::In(Arc::new(set)),
        ))
    }

    fn filter_edges_not_in(&self, property: &str, set: HashSet<Prop>) -> Self::FilteredViewType {
        let (t_prop_id, c_prop_id) = get_ids(self.current_filter().edge_meta(), property);
        self.one_hop_filtered(EdgePropertyFilteredGraph::new(
            self.current_filter().clone(),
            t_prop_id,
            c_prop_id,
            PropFilter::NotIn(Arc::new(set)),
        ))
    }
}

#[cfg(test)]
mod test {
    use crate::{
        prelude::*,
        test_utils::{build_edge_list, build_graph_from_edge_list},
    };
    use itertools::Itertools;
    use proptest::{arbitrary::any, proptest};

    #[test]
    fn test_edge_property_filter_on_nodes() {
        let g = Graph::new();
        g.add_edge(0, 1, 2, [("test", 1i64)], None).unwrap();
        g.add_edge(0, 1, 3, [("test", 3i64)], None).unwrap();
        g.add_edge(1, 2, 3, [("test", 2i64)], None).unwrap();
        g.add_edge(1, 2, 4, [("test", 0i64)], None).unwrap();

        let n1 = g
            .node(1)
            .unwrap()
            .filter_edges_eq("test", Prop::I64(1))
            .unwrap();
        assert_eq!(
            n1.edges().id().collect_vec(),
            vec![(GID::U64(1), GID::U64(2))]
        );
        let n2 = g
            .node(2)
            .unwrap()
            .filter_edges_gt("test", Prop::I64(1))
            .unwrap();
        assert_eq!(
            n2.edges().id().collect_vec(),
            vec![(GID::U64(2), GID::U64(3))]
        );
    }

    #[test]
    fn test_filter() {
        let g = Graph::new();
        g.add_edge(0, 1, 2, [("test", 1i64)], None).unwrap();
        g.add_edge(1, 2, 3, [("test", 2i64)], None).unwrap();

        let gf = g.filter_edges_eq("test", Prop::I64(1)).unwrap();
        assert_eq!(
            gf.edges().id().collect_vec(),
            vec![(GID::U64(1), GID::U64(2))]
        );
        let gf = g.filter_edges_gt("test", Prop::I64(1)).unwrap();
        assert_eq!(
            gf.edges().id().collect_vec(),
            vec![(GID::U64(2), GID::U64(3))]
        );
    }

    #[test]
    fn test_filter_gt() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter_edges_gt("int_prop", v.into_prop()).unwrap();
            for e in g.edges().iter() {
                if e.properties().get("int_prop").unwrap_i64() > v {
                    assert!(filtered.has_edge(e.src(), e.dst()));
                } else {
                    assert!(!filtered.has_edge(e.src(), e.dst()));
                }
            }
        })
    }

    #[test]
    fn test_filter_ge() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter_edges_ge("int_prop", v.into_prop()).unwrap();
            for e in g.edges().iter() {
                if e.properties().get("int_prop").unwrap_i64() >= v {
                    assert!(filtered.has_edge(e.src(), e.dst()));
                } else {
                    assert!(!filtered.has_edge(e.src(), e.dst()));
                }
            }
        })
    }

    #[test]
    fn test_filter_lt() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter_edges_lt("int_prop", v.into_prop()).unwrap();
            for e in g.edges().iter() {
                if e.properties().get("int_prop").unwrap_i64() < v {
                    assert!(filtered.has_edge(e.src(), e.dst()));
                } else {
                    assert!(!filtered.has_edge(e.src(), e.dst()));
                }
            }
        })
    }

    #[test]
    fn test_filter_le() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter_edges_le("int_prop", v.into_prop()).unwrap();
            for e in g.edges().iter() {
                if e.properties().get("int_prop").unwrap_i64() <= v {
                    assert!(filtered.has_edge(e.src(), e.dst()));
                } else {
                    assert!(!filtered.has_edge(e.src(), e.dst()));
                }
            }
        })
    }

    #[test]
    fn test_filter_eq() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter_edges_eq("int_prop", v.into_prop()).unwrap();
            for e in g.edges().iter() {
                if e.properties().get("int_prop").unwrap_i64() == v {
                    assert!(filtered.has_edge(e.src(), e.dst()));
                } else {
                    assert!(!filtered.has_edge(e.src(), e.dst()));
                }
            }
        })
    }

    #[test]
    fn test_filter_ne() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter_edges_ne("int_prop", v.into_prop()).unwrap();
            for e in g.edges().iter() {
                if e.properties().get("int_prop").unwrap_i64() != v {
                    assert!(filtered.has_edge(e.src(), e.dst()));
                } else {
                    assert!(!filtered.has_edge(e.src(), e.dst()));
                }
            }
        })
    }
}
