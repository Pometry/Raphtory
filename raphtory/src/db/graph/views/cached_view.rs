use crate::{
    core::entities::LayerIds,
    db::api::{
        properties::internal::InheritPropertiesOps,
        storage::graph::{
            edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
            nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
        },
        view::internal::{
            Base, CoreGraphOps, EdgeFilterOps, Immutable, InheritCoreOps, InheritEdgeHistoryFilter,
            InheritLayerOps, InheritListOps, InheritMaterialize, InheritNodeHistoryFilter,
            InheritTimeSemantics, InternalLayerOps, NodeFilterOps, Static,
        },
    },
    prelude::{GraphViewOps, LayerOps},
};
use rayon::prelude::*;
use roaring::RoaringTreemap;
use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
};

use crate::db::api::view::internal::InheritStorageOps;

#[derive(Clone)]
pub struct CachedView<G> {
    pub(crate) graph: G,
    pub(crate) layered_mask: Arc<[(RoaringTreemap, RoaringTreemap)]>,
}

impl<G> Static for CachedView<G> {}

impl<'graph, G: Debug + 'graph> Debug for CachedView<G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaskedGraph")
            .field("graph", &self.graph as &dyn Debug)
            .finish()
    }
}

impl<'graph, G: GraphViewOps<'graph>> Base for CachedView<G> {
    type Base = G;
    #[inline(always)]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<'graph, G: GraphViewOps<'graph>> Immutable for CachedView<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreOps for CachedView<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for CachedView<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for CachedView<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for CachedView<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for CachedView<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for CachedView<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for CachedView<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for CachedView<G> {}

impl<'graph, G: GraphViewOps<'graph>> CachedView<G> {
    pub fn new(graph: G) -> Self {
        let mut layered_masks = vec![];
        for l_name in graph.unique_layers() {
            let l_id = graph.get_layer_id(&l_name).unwrap();
            let layer_g = graph.layers(l_name).unwrap();

            let nodes = layer_g
                .nodes()
                .par_iter()
                .map(|node| node.node.as_u64())
                .collect::<Vec<_>>();

            let nodes: RoaringTreemap = nodes.into_iter().collect();

            let edges = layer_g.core_edges();

            let edges = edges
                .as_ref()
                .par_iter(&LayerIds::All)
                .filter(|edge| {
                    graph.filter_edge(edge.as_ref(), layer_g.layer_ids())
                        && nodes.contains(edge.src().as_u64())
                        && nodes.contains(edge.dst().as_u64())
                })
                .map(|edge| edge.eid().as_u64())
                .collect::<Vec<_>>();
            let edges: RoaringTreemap = edges.into_iter().collect();

            if layered_masks.len() < l_id + 1 {
                layered_masks.resize(l_id + 1, (RoaringTreemap::new(), RoaringTreemap::new()));
            }

            layered_masks[l_id] = (nodes, edges);
        }

        Self {
            graph,
            layered_mask: layered_masks.into(),
        }
    }
}

// FIXME: this should use the list version ideally
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for CachedView<G> {}
impl<'graph, G: GraphViewOps<'graph>> EdgeFilterOps for CachedView<G> {
    #[inline]
    fn edges_filtered(&self) -> bool {
        self.graph.edges_filtered()
    }

    #[inline]
    fn edge_list_trusted(&self) -> bool {
        self.graph.edge_list_trusted()
    }

    #[inline]
    fn filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool {
        let filter_fn =
            |(_, edges): &(RoaringTreemap, RoaringTreemap)| edges.contains(edge.eid().as_u64());
        match layer_ids {
            LayerIds::None => false,
            LayerIds::All => self.layered_mask.iter().any(filter_fn),
            LayerIds::One(id) => self.layered_mask.get(*id).map(filter_fn).unwrap_or(false),
            LayerIds::Multiple(multiple) => multiple
                .iter()
                .any(|id| self.layered_mask.get(id).map(filter_fn).unwrap_or(false)),
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>> NodeFilterOps for CachedView<G> {
    fn nodes_filtered(&self) -> bool {
        self.graph.nodes_filtered()
    }
    fn node_list_trusted(&self) -> bool {
        self.graph.node_list_trusted()
    }

    fn edge_filter_includes_node_filter(&self) -> bool {
        true
    }

    #[inline]
    fn filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        match layer_ids {
            LayerIds::None => false,
            LayerIds::All => self
                .layered_mask
                .iter()
                .any(|(nodes, _)| nodes.contains(node.vid().as_u64())),
            LayerIds::One(id) => self
                .layered_mask
                .get(*id)
                .map(|(nodes, _)| nodes.contains(node.vid().as_u64()))
                .unwrap_or(false),
            LayerIds::Multiple(multiple) => multiple.iter().any(|id| {
                self.layered_mask
                    .get(id)
                    .map(|(nodes, _)| nodes.contains(node.vid().as_u64()))
                    .unwrap_or(false)
            }),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        algorithms::motifs::triangle_count::triangle_count, db::graph::graph::assert_graph_equal,
        prelude::*, test_storage,
    };
    use itertools::Itertools;
    use proptest::prelude::*;

    #[test]
    fn empty_graph() {
        let graph = Graph::new();
        test_storage!(&graph, |graph| {
            let sg = graph.cache_view();
            assert_graph_equal(&sg, &graph);
        });
    }

    #[test]
    fn empty_window() {
        let graph = Graph::new();
        graph.add_edge(1, 1, 1, NO_PROPS, None).unwrap();
        test_storage!(&graph, |graph| {
            let window = graph.window(2, 3);
            let sg = window.cache_view();
            assert_graph_equal(&window, &sg);
        });
    }

    #[test]
    fn test_materialize_no_edges() {
        let graph = Graph::new();

        graph.add_node(1, 1, NO_PROPS, None).unwrap();
        graph.add_node(2, 2, NO_PROPS, None).unwrap();

        test_storage!(&graph, |graph| {
            let sg = graph.cache_view();

            let actual = sg.materialize().unwrap().into_events().unwrap();
            assert_graph_equal(&actual, &sg);
        });
    }

    #[test]
    fn test_mask_the_window_50pc() {
        let graph = Graph::new();
        let edges = vec![
            (1, 2, 1),
            (1, 3, 2),
            (1, 4, 3),
            (3, 1, 4),
            (3, 4, 5),
            (3, 5, 6),
            (4, 5, 7),
            (5, 6, 8),
            (5, 8, 9),
            (7, 5, 10),
            (8, 5, 11),
            (1, 9, 12),
            (9, 1, 13),
            (6, 3, 14),
            (4, 8, 15),
            (8, 3, 16),
            (5, 10, 17),
            (10, 5, 18),
            (10, 8, 19),
            (1, 11, 20),
            (11, 1, 21),
            (9, 11, 22),
            (11, 9, 23),
        ];
        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }
        test_storage!(&graph, |graph| {
            let window = graph.window(12, 24);
            let mask = window.cache_view();
            let ts = triangle_count(&mask, None);
            let tg = triangle_count(&window, None);
            assert_eq!(ts, tg);
        });
    }

    #[test]
    fn masked_always_equals() {
        fn check(edge_list: &[(u8, u8, i16, u8)]) {
            let graph = Graph::new();
            for (src, dst, ts, layer) in edge_list {
                graph
                    .add_edge(
                        *ts as i64,
                        *src as u64,
                        *dst as u64,
                        NO_PROPS,
                        Some(&layer.to_string()),
                    )
                    .unwrap();
            }

            test_storage!(&graph, |graph| {
                let layers = graph
                    .unique_layers()
                    .take(graph.unique_layers().count() / 2)
                    .collect_vec();

                let earliest = graph.earliest_time().unwrap();
                let latest = graph.latest_time().unwrap();
                let middle = earliest + (latest - earliest) / 2;

                if !layers.is_empty() && earliest < middle && middle < latest {
                    let subgraph = graph.layers(layers).unwrap().window(earliest, middle);
                    let masked = subgraph.cache_view();
                    assert_graph_equal(&subgraph, &masked);
                }
            });
        }

        proptest!(|(edge_list in any::<Vec<(u8, u8, i16, u8)>>().prop_filter("greater than 3",|v| v.len() > 0 ))| {
            check(&edge_list);
        })
    }

    #[cfg(test)]
    mod test_filters_cached_view {
        mod test_nodes_filters_cached_view_graph {
            use crate::{
                assert_filter_results, assert_filter_results_w, assert_search_results,
                assert_search_results_w,
                core::Prop,
                db::{
                    api::view::StaticGraphViewOps,
                    graph::views::{
                        deletion_graph::PersistentGraph, filter::model::PropertyFilterOps,
                    },
                },
                prelude::{AdditionOps, Graph, NodeViewOps, PropertyFilter, TimeOps},
            };
            use std::ops::Range;

            #[cfg(feature = "search")]
            pub use crate::db::api::view::SearchableGraphOps;
            use crate::db::graph::views::{
                filter::internal::InternalNodeFilterOps, test_helpers::filter_nodes_with,
            };

            fn init_graph<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
                let node_data = vec![
                    (6, "N1", 2u64, "air_nomad"),
                    (7, "N1", 1u64, "air_nomad"),
                    (6, "N2", 1u64, "water_tribe"),
                    (7, "N2", 2u64, "water_tribe"),
                    (8, "N3", 1u64, "air_nomad"),
                    (9, "N4", 1u64, "air_nomad"),
                    (5, "N5", 1u64, "air_nomad"),
                    (6, "N5", 2u64, "air_nomad"),
                    (5, "N6", 1u64, "fire_nation"),
                    (6, "N6", 1u64, "fire_nation"),
                    (3, "N7", 1u64, "air_nomad"),
                    (5, "N7", 1u64, "air_nomad"),
                    (3, "N8", 1u64, "fire_nation"),
                    (4, "N8", 2u64, "fire_nation"),
                ];

                for (ts, name, value, kind) in node_data {
                    graph
                        .add_node(ts, name, [("p1", Prop::U64(value))], Some(kind))
                        .unwrap();
                }

                graph
            }

            fn filter_nodes<I: InternalNodeFilterOps>(filter: I) -> Vec<String> {
                filter_nodes_with(filter, init_graph(Graph::new()))
            }

            fn filter_nodes_w<I: InternalNodeFilterOps>(filter: I, w: Range<i64>) -> Vec<String> {
                filter_nodes_with(filter, init_graph(Graph::new()).window(w.start, w.end))
            }

            fn filter_nodes_pg<I: InternalNodeFilterOps>(filter: I) -> Vec<String> {
                filter_nodes_with(filter, init_graph(PersistentGraph::new()))
            }

            fn filter_nodes_pg_w<I: InternalNodeFilterOps>(
                filter: I,
                w: Range<i64>,
            ) -> Vec<String> {
                filter_nodes_with(
                    filter,
                    init_graph(PersistentGraph::new()).window(w.start, w.end),
                )
            }

            #[cfg(feature = "search")]
            mod search_nodes {
                use std::ops::Range;
                use crate::db::graph::views::cached_view::test::test_filters_cached_view::test_nodes_filters_cached_view_graph::init_graph;
                use crate::db::graph::views::deletion_graph::PersistentGraph;
                use crate::db::graph::views::test_helpers::search_nodes_with;
                use crate::prelude::{Graph, PropertyFilter, TimeOps};

                pub fn search_nodes(filter: PropertyFilter) -> Vec<String> {
                    search_nodes_with(filter, init_graph(Graph::new()))
                }

                pub fn search_nodes_w(filter: PropertyFilter, w: Range<i64>) -> Vec<String> {
                    search_nodes_with(filter, init_graph(Graph::new()).window(w.start, w.end))
                }

                pub fn search_nodes_pg(filter: PropertyFilter) -> Vec<String> {
                    search_nodes_with(filter, init_graph(PersistentGraph::new()))
                }

                pub fn search_nodes_pg_w(filter: PropertyFilter, w: Range<i64>) -> Vec<String> {
                    search_nodes_with(
                        filter,
                        init_graph(PersistentGraph::new()).window(w.start, w.end),
                    )
                }
            }

            #[cfg(feature = "search")]
            use search_nodes::*;

            #[test]
            fn test_nodes_filters() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3", "N4", "N6", "N7"];
                assert_filter_results!(filter_nodes, filter, expected_results);
                assert_search_results!(search_nodes, filter, expected_results);
            }

            #[test]
            fn test_nodes_filters_w() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3", "N6"];
                assert_filter_results_w!(filter_nodes_w, filter, 6..9, expected_results);
                assert_search_results_w!(search_nodes_w, filter, 6..9, expected_results);
            }

            #[test]
            fn test_nodes_filters_pg() {
                let filter = PropertyFilter::property("p1").le(1u64);
                let expected_results = vec!["N1", "N3", "N4", "N6", "N7"];
                assert_filter_results!(filter_nodes_pg, filter, expected_results);
                assert_search_results!(search_nodes_pg, filter, expected_results);
            }

            #[test]
            fn test_nodes_filters_pg_w() {
                let filter = PropertyFilter::property("p1").ge(2u64);
                let expected_results = vec!["N2", "N5", "N8"];
                assert_filter_results_w!(filter_nodes_pg_w, filter, 6..9, expected_results);
                assert_search_results_w!(search_nodes_pg_w, filter, 6..9, expected_results);
            }
        }

        mod test_edges_filter_cached_view_graph {
            use crate::{
                assert_filter_results, assert_filter_results_w, assert_search_results,
                assert_search_results_w,
            };

            #[cfg(feature = "search")]
            pub use crate::db::api::view::SearchableGraphOps;
            use crate::{
                core::Prop,
                db::{
                    api::view::StaticGraphViewOps,
                    graph::views::{
                        deletion_graph::PersistentGraph,
                        filter::{internal::InternalEdgeFilterOps, model::PropertyFilterOps},
                        test_helpers::filter_edges_with,
                    },
                },
                prelude::{AdditionOps, EdgeViewOps, Graph, NodeViewOps, PropertyFilter, TimeOps},
            };
            use std::ops::Range;

            fn init_graph<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
                let edge_data = vec![
                    (6, "N1", "N2", 2u64),
                    (7, "N1", "N2", 1u64),
                    (6, "N2", "N3", 1u64),
                    (7, "N2", "N3", 2u64),
                    (8, "N3", "N4", 1u64),
                    (9, "N4", "N5", 1u64),
                    (5, "N5", "N6", 1u64),
                    (6, "N5", "N6", 2u64),
                    (5, "N6", "N7", 1u64),
                    (6, "N6", "N7", 1u64),
                    (3, "N7", "N8", 1u64),
                    (5, "N7", "N8", 1u64),
                    (3, "N8", "N1", 1u64),
                    (4, "N8", "N1", 2u64),
                ];

                for (ts, src, dst, p1_val) in edge_data {
                    graph
                        .add_edge(ts, src, dst, [("p1", Prop::U64(p1_val))], None)
                        .unwrap();
                }

                graph
            }

            fn filter_edges<I: InternalEdgeFilterOps>(filter: I) -> Vec<String> {
                filter_edges_with(filter, init_graph(Graph::new()))
            }

            fn filter_edges_w<I: InternalEdgeFilterOps>(filter: I, w: Range<i64>) -> Vec<String> {
                filter_edges_with(filter, init_graph(Graph::new()).window(w.start, w.end))
            }

            #[allow(dead_code)]
            fn filter_edges_pg<I: InternalEdgeFilterOps>(filter: I) -> Vec<String> {
                filter_edges_with(filter, init_graph(PersistentGraph::new()))
            }

            #[allow(dead_code)]
            fn filter_edges_pg_w<I: InternalEdgeFilterOps>(
                filter: I,
                w: Range<i64>,
            ) -> Vec<String> {
                filter_edges_with(
                    filter,
                    init_graph(PersistentGraph::new()).window(w.start, w.end),
                )
            }

            #[cfg(feature = "search")]
            mod search_edges {
                use crate::db::graph::views::cached_view::test::test_filters_cached_view::test_edges_filter_cached_view_graph::init_graph;
                use crate::db::graph::views::test_helpers::search_edges_with;
                use crate::prelude::{Graph, PropertyFilter, TimeOps};
                use std::ops::Range;
                use crate::db::graph::views::deletion_graph::PersistentGraph;

                pub fn search_edges(filter: PropertyFilter) -> Vec<String> {
                    search_edges_with(filter, init_graph(Graph::new()))
                }

                pub fn search_edges_w(filter: PropertyFilter, w: Range<i64>) -> Vec<String> {
                    search_edges_with(filter, init_graph(Graph::new()).window(w.start, w.end))
                }

                pub fn search_edges_pg(filter: PropertyFilter) -> Vec<String> {
                    search_edges_with(filter, init_graph(PersistentGraph::new()))
                }

                pub fn search_edges_pg_w(filter: PropertyFilter, w: Range<i64>) -> Vec<String> {
                    search_edges_with(
                        filter,
                        init_graph(PersistentGraph::new()).window(w.start, w.end),
                    )
                }
            }

            #[cfg(feature = "search")]
            use search_edges::*;

            #[test]
            fn test_edges_filters() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4", "N4->N5", "N6->N7", "N7->N8"];
                assert_filter_results!(filter_edges, filter, expected_results);
                assert_search_results!(search_edges, filter, expected_results);
            }

            #[test]
            fn test_edges_filter_w() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4", "N6->N7"];
                assert_filter_results_w!(filter_edges_w, filter, 6..9, expected_results);
                assert_search_results_w!(search_edges_w, filter, 6..9, expected_results);
            }

            #[test]
            fn test_edges_filters_pg() {
                let filter = PropertyFilter::property("p1").le(1u64);
                let expected_results = vec!["N1->N2", "N3->N4", "N4->N5", "N6->N7", "N7->N8"];
                // TODO: PropertyFilteringNotImplemented
                // assert_filter_results!(filter_edges_pg, filter, expected_results);
                assert_search_results!(search_edges_pg, filter, expected_results);
            }

            #[test]
            fn test_edges_filters_pg_w() {
                let filter = PropertyFilter::property("p1").ge(2u64);
                let expected_results = vec!["N2->N3", "N5->N6", "N8->N1"];
                // TODO: PropertyFilteringNotImplemented
                // assert_filter_results_w!(filter_edges_pg_w, filter, 6..9, expected_results);
                assert_search_results_w!(search_edges_pg_w, filter, 6..9, expected_results);
            }
        }
    }
}
