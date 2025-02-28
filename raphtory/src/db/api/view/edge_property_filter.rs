use crate::{
    core::utils::errors::GraphError,
    db::{
        api::view::internal::{InternalMaterialize, OneHopFilter},
        graph::views::property_filter::internal::InternalEdgeFilterOps,
    },
    prelude::GraphViewOps,
};
use raphtory_api::GraphType;

pub trait EdgePropertyFilterOps<'graph>: OneHopFilter<'graph> {
    fn filter_edges<F: InternalEdgeFilterOps>(
        &self,
        filter: F,
    ) -> Result<Self::Filtered<F::EdgeFiltered<'graph, Self::FilteredGraph>>, GraphError> {
        if matches!(
            self.current_filter().graph_type(),
            GraphType::PersistentGraph
        ) {
            return Err(GraphError::PropertyFilteringNotImplemented);
        }
        Ok(self.one_hop_filtered(filter.create_edge_filter(self.current_filter().clone())?))
    }
}

impl<'graph, G: GraphViewOps<'graph>> EdgePropertyFilterOps<'graph> for G {}

#[cfg(test)]
mod test {
    use crate::{
        db::graph::views::property_filter::{PropertyFilter, PropertyFilterOps, PropertyRef},
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

        let filter = PropertyFilter::eq(PropertyRef::Property("test".to_string()), 1i64);
        let n1 = g.node(1).unwrap().filter_edges(filter).unwrap();
        assert_eq!(
            n1.edges().id().collect_vec(),
            vec![(GID::U64(1), GID::U64(2))]
        );
        let n2 = g
            .node(2)
            .unwrap()
            .filter_edges(PropertyFilter::gt(
                PropertyRef::Property("test".to_string()),
                1i64,
            ))
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

        let filter = PropertyFilter::eq(PropertyRef::Property("test".to_string()), 1i64);
        let gf = g.filter_edges(filter).unwrap();
        assert_eq!(
            gf.edges().id().collect_vec(),
            vec![(GID::U64(1), GID::U64(2))]
        );
        let gf = g
            .filter_edges(PropertyFilter::gt(
                PropertyRef::Property("test".to_string()),
                1i64,
            ))
            .unwrap();
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
            let filter = PropertyFilter::gt(PropertyRef::Property("int_prop".to_string()), v);
            let filtered = g.filter_edges(
                filter
            ).unwrap();
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
            let filter = PropertyFilter::ge(PropertyRef::Property("int_prop".to_string()), v);
            let filtered = g.filter_edges(
                filter
            ).unwrap();
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
            let filter = PropertyFilter::lt(PropertyRef::Property("int_prop".to_string()), v);
            let filtered = g.filter_edges(
                filter
            ).unwrap();
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
            let filter = PropertyFilter::le(PropertyRef::Property("int_prop".to_string()), v);
            let filtered = g.filter_edges(
                filter
            ).unwrap();
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
            let filter = PropertyFilter::eq(PropertyRef::Property("int_prop".to_string()), v);
            let filtered = g.filter_edges(
                filter
            ).unwrap();
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
            let filter = PropertyFilter::ne(PropertyRef::Property("int_prop".to_string()), v);
            let filtered = g.filter_edges(
                filter
            ).unwrap();
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
