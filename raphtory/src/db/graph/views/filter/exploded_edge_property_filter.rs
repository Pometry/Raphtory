use crate::{
    core::entities::LayerIds,
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            view::internal::{
                EdgeTimeSemanticsOps, Immutable, InheritEdgeFilterOps, InheritEdgeHistoryFilter,
                InheritEdgeLayerFilterOps, InheritLayerOps, InheritListOps, InheritMaterialize,
                InheritNodeFilterOps, InheritNodeHistoryFilter, InheritStorageOps,
                InheritTimeSemantics, InternalExplodedEdgeFilterOps, Static,
            },
        },
        graph::views::filter::{internal::CreateFilter, model::edge_filter::ExplodedEdgeFilter},
    },
    errors::GraphError,
    prelude::{GraphViewOps, PropertyFilter},
};
use raphtory_api::{
    core::{
        entities::{EID, ELID},
        storage::timeindex::TimeIndexEntry,
    },
    inherit::Base,
};
use raphtory_storage::core_ops::InheritCoreGraphOps;

#[derive(Debug, Clone)]
pub struct ExplodedEdgePropertyFilteredGraph<G> {
    graph: G,
    prop_id: Option<usize>,
    filter: PropertyFilter<ExplodedEdgeFilter>,
}

impl<G> Static for ExplodedEdgePropertyFilteredGraph<G> {}
impl<G> Immutable for ExplodedEdgePropertyFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> ExplodedEdgePropertyFilteredGraph<G> {
    pub(crate) fn new(
        graph: G,
        prop_id: Option<usize>,
        filter: PropertyFilter<ExplodedEdgeFilter>,
    ) -> Self {
        Self {
            graph,
            prop_id,
            filter,
        }
    }

    fn filter(&self, e: EID, t: TimeIndexEntry, layer: usize) -> bool {
        self.filter.matches(
            self.prop_id
                .and_then(|prop_id| {
                    let time_semantics = self.graph.edge_time_semantics();
                    let edge = self.graph.core_edge(e);
                    time_semantics.temporal_edge_prop_exploded(
                        edge.as_ref(),
                        &self.graph,
                        prop_id,
                        t,
                        layer,
                    )
                })
                .as_ref(),
        )
    }
}

impl CreateFilter for PropertyFilter<ExplodedEdgeFilter> {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = ExplodedEdgePropertyFilteredGraph<G>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let t_prop_id = self.resolve_temporal_prop_id(graph.edge_meta())?;
        Ok(ExplodedEdgePropertyFilteredGraph::new(
            graph.clone(),
            t_prop_id,
            self,
        ))
    }
}

impl<G> Base for ExplodedEdgePropertyFilteredGraph<G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreGraphOps for ExplodedEdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter
    for ExplodedEdgePropertyFilteredGraph<G>
{
}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter
    for ExplodedEdgePropertyFilteredGraph<G>
{
}

impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for ExplodedEdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for ExplodedEdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for ExplodedEdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for ExplodedEdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps
    for ExplodedEdgePropertyFilteredGraph<G>
{
}

impl<'graph, G: GraphViewOps<'graph>> InheritNodeFilterOps
    for ExplodedEdgePropertyFilteredGraph<G>
{
}
impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics
    for ExplodedEdgePropertyFilteredGraph<G>
{
}

impl<'graph, G: GraphViewOps<'graph>> InheritEdgeFilterOps
    for ExplodedEdgePropertyFilteredGraph<G>
{
}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeLayerFilterOps
    for ExplodedEdgePropertyFilteredGraph<G>
{
}
impl<'graph, G: GraphViewOps<'graph>> InternalExplodedEdgeFilterOps
    for ExplodedEdgePropertyFilteredGraph<G>
{
    fn internal_exploded_edge_filtered(&self) -> bool {
        true
    }

    fn internal_exploded_filter_edge_list_trusted(&self) -> bool {
        false
    }

    fn internal_filter_exploded_edge(
        &self,
        eid: ELID,
        t: TimeIndexEntry,
        layer_ids: &LayerIds,
    ) -> bool {
        self.graph.internal_filter_exploded_edge(eid, t, layer_ids) && {
            if eid.is_deletion() {
                true
            } else {
                self.filter(eid.edge, t, eid.layer())
            }
        }
    }
}

#[cfg(test)]
mod test_exploded_edge_property_filtered_graph {
    use crate::{
        db::{
            api::view::filter_ops::BaseFilterOps,
            graph::{
                graph::{
                    assert_graph_equal, assert_node_equal, assert_nodes_equal,
                    assert_persistent_materialize_graph_equal,
                },
                views::{
                    deletion_graph::PersistentGraph,
                    filter::model::{
                        edge_filter::ExplodedEdgeFilter, property_filter::PropertyFilterOps,
                        PropertyFilterFactory,
                    },
                },
            },
        },
        prelude::*,
        test_utils::{
            build_edge_deletions, build_edge_list, build_edge_list_with_deletions,
            build_graph_from_edge_list, build_window, Update,
        },
    };
    use itertools::Itertools;
    use proptest::{arbitrary::any, proptest};
    use raphtory_api::core::entities::properties::prop::PropType;
    use raphtory_core::entities::nodes::node_ref::AsNodeRef;
    use raphtory_storage::mutation::addition_ops::InternalAdditionOps;
    use std::collections::HashMap;

    fn build_filtered_graph(
        edges: &[(u64, u64, i64, String, i64)],
        filter: impl Fn(i64) -> bool,
    ) -> Graph {
        let g = Graph::new();
        for (src, dst, t, str_prop, int_prop) in edges {
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
        if !edges.is_empty() {
            g.resolve_layer(None).unwrap();
        }
        g
    }

    fn build_filtered_nodes_graph(
        edges: &[(u64, u64, i64, String, i64)],
        filter: impl Fn(i64) -> bool,
    ) -> Graph {
        let g = Graph::new();
        for (src, dst, t, str_prop, int_prop) in edges {
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
            g.resolve_node(src.as_node_ref()).unwrap();
            g.resolve_node(dst.as_node_ref()).unwrap();
        }
        if !edges.is_empty() {
            g.resolve_layer(None).unwrap();
        }
        g
    }

    fn build_filtered_persistent_graph(
        edges: HashMap<(u64, u64), Vec<(i64, Update)>>,
        filter: impl Fn(i64) -> bool,
    ) -> (PersistentGraph, PersistentGraph) {
        let g = PersistentGraph::new();
        let g_filtered = PersistentGraph::new();
        if !edges.iter().all(|(_, v)| v.is_empty()) {
            g_filtered.resolve_layer(None).unwrap();
        }
        for ((src, dst), updates) in edges {
            for (t, update) in updates {
                match update {
                    Update::Deletion => {
                        g.delete_edge(t, src, dst, None).unwrap();
                        g_filtered.delete_edge(t, src, dst, None).unwrap();
                    }
                    Update::Addition(str_prop, int_prop) => {
                        g.add_edge(
                            t,
                            src,
                            dst,
                            [
                                ("str_prop", str_prop.clone().into()),
                                ("int_prop", Prop::I64(int_prop)),
                            ],
                            None,
                        )
                        .unwrap();
                        if filter(int_prop) {
                            g_filtered
                                .add_edge(
                                    t,
                                    src,
                                    dst,
                                    [
                                        ("str_prop", str_prop.into()),
                                        ("int_prop", Prop::I64(int_prop)),
                                    ],
                                    None,
                                )
                                .unwrap();
                        } else {
                            g_filtered.delete_edge(t, src, dst, None).unwrap();
                            // properties still exist after filtering
                            g_filtered
                                .resolve_edge_property("str_prop", PropType::Str, false)
                                .unwrap();
                            g_filtered
                                .resolve_edge_property("int_prop", PropType::I64, false)
                                .unwrap();
                        }
                    }
                }
            }
        }
        (g, g_filtered)
    }

    #[test]
    fn test_filter_gt() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter(
                ExplodedEdgeFilter::property("int_prop").gt(v)
            ).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv > v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_filter_gt_persistent() {
        proptest!(|(
            edges in build_edge_list_with_deletions(100, 100), v in any::<i64>()
        )| {
            let (g, expected_filtered_g) = build_filtered_persistent_graph(edges, |vv| vv > v);
            let filtered = g.filter(
                ExplodedEdgeFilter::property("int_prop").gt(v)
            ).unwrap();
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_one_edge() {
        let g = Graph::new();
        g.add_edge(0, 1, 2, [("int_prop", 0i64)], None).unwrap();
        let filtered = g
            .filter(ExplodedEdgeFilter::property("int_prop").gt(1i64))
            .unwrap();
        let gf = Graph::new();
        gf.resolve_layer(None).unwrap();
        assert_eq!(filtered.count_nodes(), 0);
        assert_eq!(filtered.count_edges(), 0);
        assert_graph_equal(&filtered, &gf);
    }

    #[test]
    fn test_filter_ge() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter(
                ExplodedEdgeFilter::property("int_prop").ge(v)
            ).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv >= v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_filter_ge_persistent() {
        proptest!(|(
            edges in build_edge_list_with_deletions(100, 100), v in any::<i64>()
        )| {
            let (g, expected_filtered_g) = build_filtered_persistent_graph(edges, |vv| vv >= v);
            let filtered = g.filter(
                ExplodedEdgeFilter::property("int_prop").ge(v)
            ).unwrap();
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_filter_persistent_single_filtered_edge() {
        let g = PersistentGraph::new();
        g.add_edge(0, 0, 0, [("test", 1i64)], None).unwrap();
        let gf = g
            .filter(ExplodedEdgeFilter::property("test").gt(1i64))
            .unwrap();

        assert_eq!(gf.count_edges(), 1);
        assert_eq!(gf.count_temporal_edges(), 0);

        let expected = PersistentGraph::new();
        expected.delete_edge(0, 0, 0, None).unwrap();
        //the property still exists!
        expected
            .resolve_edge_property("test", PropType::I64, false)
            .unwrap();

        assert_graph_equal(&gf, &expected);
    }

    #[test]
    fn test_filter_lt() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter(
                ExplodedEdgeFilter::property("int_prop").lt(v)
            ).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv < v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_filter_lt_persistent() {
        proptest!(|(
            edges in build_edge_list_with_deletions(100, 100), v in any::<i64>()
        )| {
            let (g, expected_filtered_g) = build_filtered_persistent_graph(edges, |vv| vv < v);
            let filtered = g.filter(
                ExplodedEdgeFilter::property("int_prop").lt(v)
            ).unwrap();
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_filter_le() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter(
                ExplodedEdgeFilter::property("int_prop").le(v)
            ).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv <= v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_filter_le_persistent() {
        proptest!(|(
            edges in build_edge_list_with_deletions(100, 100), v in any::<i64>()
        )| {
            let (g, expected_filtered_g) = build_filtered_persistent_graph(edges, |vv| vv <= v);
            let filtered = g.filter(
                ExplodedEdgeFilter::property("int_prop").le(v)
            ).unwrap();
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_filter_eq() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter(
                ExplodedEdgeFilter::property("int_prop").eq(v)
            ).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv == v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_filter_eq_persistent() {
        proptest!(|(
            edges in build_edge_list_with_deletions(100, 100), v in any::<i64>()
        )| {
            let (g, expected_filtered_g) = build_filtered_persistent_graph(edges, |vv| vv == v);
            let filtered = g.filter(
                ExplodedEdgeFilter::property("int_prop").eq(v)
            ).unwrap();
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_filter_ne() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter(
                ExplodedEdgeFilter::property("int_prop").ne(v)
            ).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv != v);
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_filter_ne_persistent() {
        proptest!(|(
            edges in build_edge_list_with_deletions(100, 100), v in any::<i64>()
        )| {
            let (g, expected_filtered_g) = build_filtered_persistent_graph(edges, |vv| vv != v);
            let filtered = g.filter(
                ExplodedEdgeFilter::property("int_prop").ne(v)
            ).unwrap();
            assert_graph_equal(&filtered, &expected_filtered_g);
        })
    }

    #[test]
    fn test_filter_window() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>(), (start, end) in build_window()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter(
                ExplodedEdgeFilter::property("int_prop").eq(v)
            ).unwrap();
            let expected_filtered_g = build_filtered_graph(&edges, |vv| vv == v);
            assert_graph_equal(&filtered.window(start, end), &expected_filtered_g.window(start, end));
        })
    }

    #[test]
    fn test_filter_window_persistent() {
        proptest!(|(
            edges in build_edge_list_with_deletions(100, 100), v in any::<i64>(), (start, end) in build_window()
        )| {
            let (g, expected_filtered_g) = build_filtered_persistent_graph(edges, |vv| vv >= v);
            let filtered = g.filter(
                ExplodedEdgeFilter::property("int_prop").ge(v)
            ).unwrap();
            assert_graph_equal(&filtered.window(start, end), &expected_filtered_g.window(start, end));
        })
    }

    #[test]
    fn test_filter_materialise_is_consistent() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter(
                ExplodedEdgeFilter::property("int_prop").eq(v)
            ).unwrap();
            let mat = filtered.materialize().unwrap();
            assert_graph_equal(&filtered, &mat);
        })
    }

    #[test]
    fn test_filter_persistent_materialise_is_consistent() {
        proptest!(|(
            edges in build_edge_list_with_deletions(100, 100), v in any::<i64>()
        )| {
            let (g, expected) = build_filtered_persistent_graph(edges, |vv| vv >= v);
            let filtered = g.filter(
                ExplodedEdgeFilter::property("int_prop").ge(v)
            ).unwrap();
            let mat = filtered.materialize().unwrap();
            assert_graph_equal(&filtered, &mat);
            assert_graph_equal(&expected, &mat);
        })
    }

    #[test]
    fn test_filter_on_nodes() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered_nodes =
                g.nodes().filter(
                    ExplodedEdgeFilter::property("int_prop").eq(v)
                ).unwrap();
            let expected_filtered_g = build_filtered_nodes_graph(&edges, |vv| vv == v);
            assert_nodes_equal(&filtered_nodes, &expected_filtered_g.nodes());
        })
    }

    #[test]
    fn test_filter_on_node() {
        proptest!(|(
            edges in build_edge_list(100, 2), v in any::<i64>()
        )| {
            let g = build_graph_from_edge_list(&edges);
            if let Some(node) = g.node(0) {
                let filtered_node = node.filter(
                    ExplodedEdgeFilter::property("int_prop").eq(v)
                ).unwrap();
                let expected_filtered_g = build_filtered_graph(&edges, |vv| vv == v);
                if filtered_node.degree() == 0 {
                    // should be filtered out
                    assert!(expected_filtered_g.node(0).is_none())
                } else {
                    assert_node_equal(filtered_node, expected_filtered_g.node(0).unwrap())
                }
            }
        })
    }

    #[test]
    fn test_filter_materialise_window_is_consistent() {
        proptest!(|(
            edges in build_edge_list(100, 100), v in any::<i64>(), (start, end) in build_window()
        )| {
            let g = build_graph_from_edge_list(&edges);
            let filtered = g.filter(
                ExplodedEdgeFilter::property("int_prop").eq(v)
            ).unwrap();
            let left = filtered.window(start, end);
            let right = filtered.window(start, end).materialize().unwrap();
            assert_graph_equal(&left, &right);
        })
    }

    #[test]
    fn test_persistent_graph() {
        let g = PersistentGraph::new();
        g.add_edge(0, 1, 2, [("int_prop", 0i64)], None).unwrap();
        g.delete_edge(2, 1, 2, None).unwrap();
        g.add_edge(5, 1, 2, [("int_prop", 5i64)], None).unwrap();
        g.delete_edge(7, 1, 2, None).unwrap();

        let edges = g
            .node(1)
            .unwrap()
            .filter(ExplodedEdgeFilter::property("int_prop").gt(1i64))
            .unwrap()
            .edges()
            .explode()
            .collect();
        println!("{:?}", edges);

        assert_eq!(edges.len(), 1);
        let gf = g
            .filter(ExplodedEdgeFilter::property("int_prop").gt(1i64))
            .unwrap();
        let gfm = gf.materialize().unwrap();

        assert_graph_equal(&gf, &gfm); // check materialise is consistent
    }

    #[test]
    fn test_persistent_graph_explode_semantics() {
        let g = PersistentGraph::new();
        g.add_edge(0, 0, 1, [("test", 0i64)], None).unwrap();
        g.add_edge(2, 0, 1, [("test", 2i64)], None).unwrap();
        g.add_edge(5, 0, 1, [("test", 5i64)], None).unwrap();
        g.delete_edge(10, 0, 1, None).unwrap();

        let gf = g
            .filter(ExplodedEdgeFilter::property("test").ne(2i64))
            .unwrap();
        assert_eq!(
            gf.edges().explode().earliest_time().collect_vec(),
            [Some(0i64), Some(5i64)]
        );
        assert_eq!(
            gf.edges().explode().latest_time().collect_vec(),
            [Some(2i64), Some(10i64)]
        );
    }

    #[test]
    fn test_persistent_graph_materialise() {
        proptest!(|(edges in build_edge_list(100, 100), edge_deletions in build_edge_deletions(100, 100), v in any::<i64>())| {
            let g = build_graph_from_edge_list(&edges);
            let g = g.persistent_graph();
            for (src, dst, t) in edge_deletions {
                g.delete_edge(t, src, dst, None).unwrap();
            }
            let gf = g
                .filter(
                ExplodedEdgeFilter::property("int_prop").gt(v)
            )
                .unwrap();
            let gfm = gf.materialize().unwrap();
            assert_graph_equal(&gf, &gfm)
        })
    }

    #[test]
    fn test_single_filtered_edge_persistent() {
        let g = PersistentGraph::new();
        g.add_edge(0, 0, 0, [("test", 0i64)], None).unwrap();
        let gf = g
            .filter(ExplodedEdgeFilter::property("test").gt(0i64))
            .unwrap();
        let gfm = gf.materialize().unwrap();
        dbg!(&gfm);
        assert_eq!(gf.node(0).unwrap().out_degree(), 1);
        assert_eq!(gfm.node(0).unwrap().out_degree(), 1);
        assert_graph_equal(&gf, &gfm);
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
                ExplodedEdgeFilter::property("int_prop").gt(v)
            )
                .unwrap();
            let gwfm = gwf.materialize().unwrap();
            assert_persistent_materialize_graph_equal(&gwf, &gwfm);

            let gfw = g
                .filter(
                ExplodedEdgeFilter::property("int_prop").gt(v)
            ).unwrap()
                .window(start, end);
            let gfwm = gfw.materialize().unwrap();
            assert_persistent_materialize_graph_equal(&gfw, &gfwm);
        })
    }

    #[test]
    fn test_persistent_failure() {
        let g = PersistentGraph::new();
        g.add_edge(-1, 0, 1, [("test", Prop::I32(-1))], None)
            .unwrap();
        g.add_edge(0, 0, 1, [("test", Prop::I32(1))], None).unwrap();

        let gwf = g
            .filter(ExplodedEdgeFilter::property("test").gt(0))
            .unwrap()
            .window(-1, 0);
        assert_eq!(gwf.count_nodes(), 0);
        assert_eq!(gwf.count_edges(), 0);
        let gm = gwf.materialize().unwrap();

        assert_persistent_materialize_graph_equal(&gwf, &gm);

        let gfw = g
            .window(-1, 0)
            .filter(ExplodedEdgeFilter::property("test").gt(0))
            .unwrap();
        assert_eq!(gfw.count_edges(), 0);
        let gm = gfw.materialize().unwrap();
        assert_persistent_materialize_graph_equal(&gfw, &gm);
    }

    #[test]
    fn test_persistent_graph_only_deletion() {
        let g = PersistentGraph::new();
        g.delete_edge(0, 0, 0, None).unwrap();
        let gfw = g
            .filter(ExplodedEdgeFilter::property("int_prop").gt(1i64))
            .unwrap()
            .window(-1, 1);
        let gfwm = gfw.materialize().unwrap();

        // deletions are not filtered
        assert_graph_equal(&g, &gfw);
        assert_graph_equal(&gfw, &gfwm);
    }
}
