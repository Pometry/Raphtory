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

    #[cfg(all(test, feature = "search"))]
    mod search_nodes_cached_view_graph_tests {
        use crate::{
            core::Prop,
            db::{
                api::view::{SearchableGraphOps, StaticGraphViewOps},
                graph::views::{
                    deletion_graph::PersistentGraph,
                    property_filter::{FilterExpr, PropertyFilterOps},
                },
            },
            prelude::{AdditionOps, Graph, NodeViewOps, PropertyFilter, TimeOps},
        };
        use std::ops::Range;

        fn init_graph<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
            graph
                .add_node(6, "N1", [("p1", Prop::U64(2u64))], Some("air_nomad"))
                .unwrap();
            graph
                .add_node(7, "N1", [("p1", Prop::U64(1u64))], Some("air_nomad"))
                .unwrap();

            graph
                .add_node(6, "N2", [("p1", Prop::U64(1u64))], Some("water_tribe"))
                .unwrap();
            graph
                .add_node(7, "N2", [("p1", Prop::U64(2u64))], Some("water_tribe"))
                .unwrap();

            graph
                .add_node(8, "N3", [("p1", Prop::U64(1u64))], Some("air_nomad"))
                .unwrap();

            graph
                .add_node(9, "N4", [("p1", Prop::U64(1u64))], Some("air_nomad"))
                .unwrap();

            graph
                .add_node(5, "N5", [("p1", Prop::U64(1u64))], Some("air_nomad"))
                .unwrap();
            graph
                .add_node(6, "N5", [("p1", Prop::U64(2u64))], Some("air_nomad"))
                .unwrap();

            graph
                .add_node(5, "N6", [("p1", Prop::U64(1u64))], Some("fire_nation"))
                .unwrap();
            graph
                .add_node(6, "N6", [("p1", Prop::U64(1u64))], Some("fire_nation"))
                .unwrap();

            graph
                .add_node(3, "N7", [("p1", Prop::U64(1u64))], Some("air_nomad"))
                .unwrap();
            graph
                .add_node(5, "N7", [("p1", Prop::U64(1u64))], Some("air_nomad"))
                .unwrap();

            graph
                .add_node(3, "N8", [("p1", Prop::U64(1u64))], Some("fire_nation"))
                .unwrap();
            graph
                .add_node(4, "N8", [("p1", Prop::U64(2u64))], Some("fire_nation"))
                .unwrap();

            graph
        }

        fn search_nodes_by_composite_filter<G: StaticGraphViewOps + AdditionOps>(
            graph: &G,
            filter: FilterExpr,
        ) -> Vec<String> {
            graph.create_index().unwrap();
            let cv = graph.cache_view();
            let mut results = cv
                .search_nodes(filter, 10, 0)
                .expect("Failed to search for nodes")
                .into_iter()
                .map(|v| v.name())
                .collect::<Vec<_>>();
            results.sort();
            results
        }

        fn search_nodes_by_composite_filter_w<G: StaticGraphViewOps + AdditionOps>(
            graph: &G,
            w: Range<i64>,
            filter: FilterExpr,
        ) -> Vec<String> {
            graph.create_index().unwrap();
            let cv = graph.cache_view();
            let mut results = cv
                .window(w.start, w.end)
                .search_nodes(filter, 10, 0)
                .expect("Failed to search for nodes")
                .into_iter()
                .map(|v| v.name())
                .collect::<Vec<_>>();
            results.sort();
            results
        }

        #[test]
        fn test_search_nodes_cached_view_graph() {
            let graph = Graph::new();
            let graph = init_graph(graph);

            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_nodes_by_composite_filter(&graph, filter);
            assert_eq!(results, vec!["N1", "N3", "N4", "N6", "N7"]);
        }

        #[test]
        fn test_search_nodes_cached_view_graph_w() {
            let graph = Graph::new();
            let graph = init_graph(graph);

            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_nodes_by_composite_filter_w(&graph, 6..9, filter);
            assert_eq!(results, vec!["N1", "N3", "N6"]);
        }

        #[test]
        fn test_search_nodes_persistent_cached_view_graph() {
            let graph = PersistentGraph::new();
            let graph = init_graph(graph);

            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_nodes_by_composite_filter(&graph, filter);
            assert_eq!(results, vec!["N1", "N3", "N4", "N6", "N7"]);
        }

        #[test]
        fn test_search_nodes_persistent_cached_view_graph_w() {
            let graph = PersistentGraph::new();
            let graph = init_graph(graph);

            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_nodes_by_composite_filter_w(&graph, 6..9, filter);
            assert_eq!(results, vec!["N1", "N3", "N6", "N7"]);
        }
    }

    #[cfg(all(test, feature = "search"))]
    mod search_edges_cached_view_graph_tests {
        use crate::{
            core::Prop,
            db::{
                api::view::{SearchableGraphOps, StaticGraphViewOps},
                graph::views::{
                    deletion_graph::PersistentGraph,
                    property_filter::{FilterExpr, PropertyFilterOps},
                },
            },
            prelude::{AdditionOps, EdgeViewOps, Graph, NodeViewOps, PropertyFilter, TimeOps},
        };
        use std::ops::Range;

        fn init_graph<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
            graph
                .add_edge(6, "N1", "N2", [("p1", Prop::U64(2u64))], None)
                .unwrap();
            graph
                .add_edge(7, "N1", "N2", [("p1", Prop::U64(1u64))], None)
                .unwrap();

            graph
                .add_edge(6, "N2", "N3", [("p1", Prop::U64(1u64))], None)
                .unwrap();
            graph
                .add_edge(7, "N2", "N3", [("p1", Prop::U64(2u64))], None)
                .unwrap();

            graph
                .add_edge(8, "N3", "N4", [("p1", Prop::U64(1u64))], None)
                .unwrap();

            graph
                .add_edge(9, "N4", "N5", [("p1", Prop::U64(1u64))], None)
                .unwrap();

            graph
                .add_edge(5, "N5", "N6", [("p1", Prop::U64(1u64))], None)
                .unwrap();
            graph
                .add_edge(6, "N5", "N6", [("p1", Prop::U64(2u64))], None)
                .unwrap();

            graph
                .add_edge(5, "N6", "N7", [("p1", Prop::U64(1u64))], None)
                .unwrap();
            graph
                .add_edge(6, "N6", "N7", [("p1", Prop::U64(1u64))], None)
                .unwrap();

            graph
                .add_edge(3, "N7", "N8", [("p1", Prop::U64(1u64))], None)
                .unwrap();
            graph
                .add_edge(5, "N7", "N8", [("p1", Prop::U64(1u64))], None)
                .unwrap();

            graph
                .add_edge(3, "N8", "N1", [("p1", Prop::U64(1u64))], None)
                .unwrap();
            graph
                .add_edge(4, "N8", "N1", [("p1", Prop::U64(2u64))], None)
                .unwrap();

            graph
        }

        fn search_edges_by_composite_filter<G: StaticGraphViewOps + AdditionOps>(
            graph: &G,
            filter: FilterExpr,
        ) -> Vec<String> {
            graph.create_index().unwrap();
            let cv = graph.cache_view();
            let mut results = cv
                .search_edges(filter, 10, 0)
                .expect("Failed to search for nodes")
                .into_iter()
                .map(|v| format!("{}->{}", v.src().name(), v.dst().name()))
                .collect::<Vec<_>>();
            results.sort();
            results
        }

        fn search_edges_by_composite_filter_w<G: StaticGraphViewOps + AdditionOps>(
            graph: &G,
            w: Range<i64>,
            filter: FilterExpr,
        ) -> Vec<String> {
            graph.create_index().unwrap();
            let cv = graph.cache_view();
            let mut results = cv
                .window(w.start, w.end)
                .search_edges(filter, 10, 0)
                .expect("Failed to search for nodes")
                .into_iter()
                .map(|v| format!("{}->{}", v.src().name(), v.dst().name()))
                .collect::<Vec<_>>();
            results.sort();
            results
        }

        #[test]
        fn test_search_edges_cached_view_graph() {
            let graph = Graph::new();
            let graph = init_graph(graph);

            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_edges_by_composite_filter(&graph, filter);
            assert_eq!(
                results,
                vec!["N1->N2", "N3->N4", "N4->N5", "N6->N7", "N7->N8"]
            );
        }

        #[test]
        fn test_search_edges_cached_view_graph_w() {
            let graph = Graph::new();
            let graph = init_graph(graph);

            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_edges_by_composite_filter_w(&graph, 6..9, filter);
            assert_eq!(results, vec!["N1->N2", "N3->N4", "N6->N7"]);
        }

        #[test]
        fn test_search_edges_persistent_cached_view_graph() {
            let graph = PersistentGraph::new();
            let graph = init_graph(graph);

            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_edges_by_composite_filter(&graph, filter);
            assert_eq!(
                results,
                vec!["N1->N2", "N3->N4", "N4->N5", "N6->N7", "N7->N8"]
            );
        }

        #[test]
        fn test_search_edges_persistent_cached_view_graph_w() {
            let graph = PersistentGraph::new();
            let graph = init_graph(graph);

            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_edges_by_composite_filter_w(&graph, 6..9, filter);
            assert_eq!(results, vec!["N1->N2", "N3->N4", "N6->N7", "N7->N8"]);
        }
    }
}
