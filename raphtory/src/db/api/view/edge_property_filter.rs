use crate::{
    db::{
        api::view::internal::{InternalMaterialize, OneHopFilter},
        graph::views::filter::internal::CreateEdgeFilter,
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::GraphType;

pub trait EdgePropertyFilterOps<'graph>: OneHopFilter<'graph> {
    fn filter_edges<F: CreateEdgeFilter>(
        &self,
        filter: F,
    ) -> Result<Self::Filtered<F::EdgeFiltered<'graph, Self::FilteredGraph>>, GraphError> {
        Ok(self.one_hop_filtered(filter.create_edge_filter(self.current_filter().clone())?))
    }
}

impl<'graph, G: GraphViewOps<'graph>> EdgePropertyFilterOps<'graph> for G {}

#[cfg(test)]
mod test {
    use crate::{
        db::graph::{
            graph::{assert_edges_equal, assert_graph_equal},
            views::{
                deletion_graph::PersistentGraph,
                filter::model::{
                    property_filter::{PropertyFilter, PropertyRef},
                    ComposableFilter, EdgeFilter, EdgeFilterOps,
                },
            },
        },
        prelude::*,
        test_utils::{build_edge_list, build_graph_from_edge_list},
    };
    use itertools::Itertools;
    use proptest::{arbitrary::any, proptest};

    #[test]
    fn test_edge_filter() {
        use crate::db::graph::views::filter::model::PropertyFilterOps;

        let g = Graph::new();
        g.add_edge(0, "Jimi", "John", [("band", "JH Experience")], None)
            .unwrap();
        g.add_edge(1, "John", "David", [("band", "Dead & Company")], None)
            .unwrap();
        g.add_edge(2, "David", "Jimi", [("band", "Pink Floyd")], None)
            .unwrap();

        let filter_expr = EdgeFilter::dst()
            .name()
            .eq("David")
            .and(PropertyFilter::property("band").eq("Dead & Company"));
        let filtered_edges = g.filter_edges(filter_expr).unwrap();

        assert_eq!(
            filtered_edges
                .edges()
                .iter()
                .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
                .collect::<Vec<_>>(),
            vec!["John->David"]
        );

        let g_expected = Graph::new();
        g_expected
            .add_edge(1, "John", "David", [("band", "Dead & Company")], None)
            .unwrap();

        assert_eq!(
            filtered_edges
                .edges()
                .iter()
                .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
                .collect::<Vec<_>>(),
            vec!["John->David"]
        );
        assert_graph_equal(&filtered_edges, &g_expected);
    }

    #[test]
    fn test_edge_filter_persistent() {
        use crate::db::graph::views::filter::model::PropertyFilterOps;

        let g = PersistentGraph::new();
        g.add_edge(0, "Jimi", "John", [("band", "JH Experience")], None)
            .unwrap();
        g.add_edge(1, "John", "David", [("band", "Dead & Company")], None)
            .unwrap();
        g.add_edge(2, "David", "Jimi", [("band", "Pink Floyd")], None)
            .unwrap();

        let filter_expr = EdgeFilter::dst()
            .name()
            .eq("David")
            .and(PropertyFilter::property("band").eq("Dead & Company"));
        let filtered_edges = g.filter_edges(filter_expr).unwrap();

        let g_expected = PersistentGraph::new();
        g_expected
            .add_edge(1, "John", "David", [("band", "Dead & Company")], None)
            .unwrap();

        assert_eq!(
            filtered_edges
                .edges()
                .iter()
                .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
                .collect::<Vec<_>>(),
            vec!["John->David"]
        );
        assert_graph_equal(&filtered_edges, &g_expected);
    }

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
