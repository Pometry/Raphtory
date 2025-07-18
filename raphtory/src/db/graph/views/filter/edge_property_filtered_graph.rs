use crate::{
    core::entities::LayerIds,
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            view::internal::{
                Immutable, InheritEdgeHistoryFilter, InheritEdgeLayerFilterOps,
                InheritExplodedEdgeFilterOps, InheritLayerOps, InheritListOps, InheritMaterialize,
                InheritNodeFilterOps, InheritNodeHistoryFilter, InheritStorageOps,
                InheritTimeSemantics, InternalEdgeFilterOps, Static,
            },
        },
        graph::views::filter::{
            internal::CreateFilter, model::edge_filter::EdgeFilter, PropertyFilter,
        },
    },
    errors::GraphError,
    prelude::{GraphViewOps, LayerOps},
};
use raphtory_api::inherit::Base;
use raphtory_storage::{core_ops::InheritCoreGraphOps, graph::edges::edge_ref::EdgeStorageRef};

#[derive(Debug, Clone)]
pub struct EdgePropertyFilteredGraph<G> {
    graph: G,
    t_prop_id: Option<usize>,
    c_prop_id: Option<usize>,
    filter: PropertyFilter<EdgeFilter>,
}

impl<G> EdgePropertyFilteredGraph<G> {
    pub(crate) fn new(
        graph: G,
        t_prop_id: Option<usize>,
        c_prop_id: Option<usize>,
        filter: PropertyFilter<EdgeFilter>,
    ) -> Self {
        Self {
            graph,
            t_prop_id,
            c_prop_id,
            filter,
        }
    }
}

impl CreateFilter for PropertyFilter<EdgeFilter> {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = EdgePropertyFilteredGraph<G>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let t_prop_id = self.resolve_temporal_prop_id(graph.edge_meta())?;
        let resolve_to_map = graph.num_layers() > 1;
        let c_prop_id = self.resolve_constant_prop_id(graph.edge_meta(), resolve_to_map)?;
        Ok(EdgePropertyFilteredGraph::new(
            graph, t_prop_id, c_prop_id, self,
        ))
    }
}

impl<G> Base for EdgePropertyFilteredGraph<G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G> Static for EdgePropertyFilteredGraph<G> {}
impl<G> Immutable for EdgePropertyFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreGraphOps for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeFilterOps for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for EdgePropertyFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritExplodedEdgeFilterOps
    for EdgePropertyFilteredGraph<G>
{
}

impl<'graph, G: GraphViewOps<'graph>> InheritEdgeLayerFilterOps for EdgePropertyFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InternalEdgeFilterOps for EdgePropertyFilteredGraph<G> {
    #[inline]
    fn internal_edge_filtered(&self) -> bool {
        true
    }

    #[inline]
    fn internal_edge_list_trusted(&self) -> bool {
        false
    }

    #[inline]
    fn internal_filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool {
        if self.graph.internal_filter_edge(edge, layer_ids) {
            self.filter
                .matches_edge(&self.graph, self.t_prop_id, self.c_prop_id, edge)
        } else {
            false
        }
    }
}

#[cfg(test)]
mod test_edge_property_filtered_graph {
    use crate::{
        db::{
            api::view::filter_ops::BaseFilterOps,
            graph::{
                graph::{assert_graph_equal, assert_persistent_materialize_graph_equal},
                views::{
                    deletion_graph::PersistentGraph,
                    filter::model::{
                        edge_filter::{EdgeFilter, EdgeFilterOps},
                        property_filter::PropertyFilterOps,
                        ComposableFilter, PropertyFilterFactory,
                    },
                },
            },
        },
        prelude::*,
        test_utils::{
            build_edge_deletions, build_edge_list, build_graph_from_edge_list, build_window,
        },
    };
    use itertools::Itertools;
    use proptest::{arbitrary::any, proptest};
    use raphtory_api::core::entities::properties::prop::PropType;
    use raphtory_storage::mutation::addition_ops::InternalAdditionOps;

    #[test]
    fn test_edge_filter() {
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
            .and(EdgeFilter::property("band").eq("Dead & Company"));
        let filtered_edges = g.filter(filter_expr).unwrap();

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
            .and(EdgeFilter::property("band").eq("Dead & Company"));
        let filtered_edges = g.filter(filter_expr).unwrap();

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

        let filter = EdgeFilter::property("test").eq(1i64);
        let n1 = g.node(1).unwrap().filter(filter).unwrap();
        assert_eq!(
            n1.edges().id().collect_vec(),
            vec![(GID::U64(1), GID::U64(2))]
        );
        let n2 = g
            .node(2)
            .unwrap()
            .filter(EdgeFilter::property("test").gt(1i64))
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

        let filter = EdgeFilter::property("test").eq(1i64);
        let gf = g.filter(filter).unwrap();
        assert_eq!(
            gf.edges().id().collect_vec(),
            vec![(GID::U64(1), GID::U64(2))]
        );
        let gf = g.filter(EdgeFilter::property("test").gt(1i64)).unwrap();
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
            let filter = EdgeFilter::property("int_prop").gt(v);
            let filtered = g.filter(
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
            let filter = EdgeFilter::property("int_prop").ge(v);
            let filtered = g.filter(
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
            let filter = EdgeFilter::property("int_prop").lt(v);
            let filtered = g.filter(
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
            let filter = EdgeFilter::property("int_prop").le(v);
            let filtered = g.filter(
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
            let filter = EdgeFilter::property("int_prop").eq(v);
            let filtered = g.filter(
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
            let filter = EdgeFilter::property("int_prop").ne(v);
            let filtered = g.filter(
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

    #[test]
    fn test_graph_materialise_window() {
        proptest!(|(edges in build_edge_list(100, 100), edge_deletions in build_edge_deletions(100, 100), v in any::<i64>(), (start, end) in build_window())| {
            let g = build_graph_from_edge_list(&edges);
            for (src, dst, t) in edge_deletions {
                g.delete_edge(t, src, dst, None).unwrap();
            }
            let gwf = g.window(start, end)
                .filter(
                EdgeFilter::property("int_prop").gt(v)
            )
                .unwrap();
            let gwfm = gwf.materialize().unwrap();
            assert_graph_equal(&gwf, &gwfm);

            let gfw = g
                .filter(
                EdgeFilter::property("int_prop").gt(v)
            ).unwrap()
                .window(start, end);
            let gfwm = gfw.materialize().unwrap();
            assert_graph_equal(&gfw, &gfwm);
        })
    }

    #[test]
    fn test_persistent_graph_materialise_window() {
        proptest!(|(edges in build_edge_list(100, 100), edge_deletions in build_edge_deletions(100, 100), v in any::<i64>(), (start, end) in build_window())| {
            let g = build_graph_from_edge_list(&edges);
            let g = g.persistent_graph();
            for (src, dst, t) in edge_deletions {
                g.delete_edge(t, src, dst, None).unwrap();
            }
            let gwf = g.window(start, end)
                .filter(
                EdgeFilter::property("int_prop").gt(v)
            )
                .unwrap();
            let gwfm = gwf.materialize().unwrap();
            assert_persistent_materialize_graph_equal(&gwf, &gwfm);

            let gfw = g
                .filter(
                EdgeFilter::property("int_prop").gt(v)
            ).unwrap()
                .window(start, end);
            let gfwm = gfw.materialize().unwrap();
            assert_persistent_materialize_graph_equal(&gfw, &gfwm);
        })
    }

    #[test]
    fn test_single_unfiltered_edge_empty_window_persistent() {
        let g = PersistentGraph::new();
        g.add_edge(0, 0, 1, [("test", 1i64)], None).unwrap();
        g.delete_edge(10, 0, 1, None).unwrap();
        let gw = g
            .filter(EdgeFilter::property("test").gt(0i64))
            .unwrap()
            .window(0, 0);

        assert_eq!(gw.count_edges(), 0);
        let expected = PersistentGraph::new();
        expected
            .resolve_edge_property("test", PropType::I64, false)
            .unwrap();
        expected.resolve_layer(None).unwrap();
        assert_graph_equal(&gw, &expected)
    }

    #[test]
    fn test_single_deleted_edge_window_persistent() {
        let g = PersistentGraph::new();
        g.add_edge(0, 0, 1, [("test", 1i64)], None).unwrap();
        g.delete_edge(1, 0, 1, None).unwrap();
        let gw = g
            .filter(EdgeFilter::property("test").gt(0i64))
            .unwrap()
            .window(0, 2);
        let gm = gw.materialize().unwrap();

        assert_eq!(gw.count_edges(), 1);
        assert_eq!(gw.count_temporal_edges(), 1);

        assert_eq!(gw.node(0).unwrap().edge_history_count(), 2);
        assert_eq!(gw.node(0).unwrap().after(0).edge_history_count(), 1);

        assert_persistent_materialize_graph_equal(&gw, &gm)
    }

    #[test]
    fn test_single_unfiltered_edge_window_persistent_2() {
        let g = PersistentGraph::new();
        g.add_edge(1, 0, 1, [("test", 1i64)], None).unwrap();
        g.delete_edge(0, 0, 0, None).unwrap();

        let gwf = g
            .window(-1, 2)
            .filter(EdgeFilter::property("test").gt(0i64))
            .unwrap();
        assert!(gwf.has_edge(0, 1));
        assert!(!gwf.has_edge(0, 0));
        assert_eq!(gwf.node(0).unwrap().earliest_time(), Some(1));
        assert_persistent_materialize_graph_equal(&gwf, &gwf.materialize().unwrap());

        let gfw = g
            .filter(EdgeFilter::property("test").gt(0i64))
            .unwrap()
            .window(-1, 2);
        let gm = gfw.materialize().unwrap();

        assert_eq!(gfw.count_edges(), 1);
        assert_eq!(gfw.count_temporal_edges(), 1);

        assert_eq!(gfw.node(0).unwrap().edge_history_count(), 1);
        assert_eq!(gfw.node(0).unwrap().after(0).edge_history_count(), 1);

        assert_persistent_materialize_graph_equal(&gfw, &gm)
    }
}
