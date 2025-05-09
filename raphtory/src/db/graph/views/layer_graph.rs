use crate::{
    core::{
        entities::{LayerIds, Multiple},
        utils::errors::GraphError,
    },
    db::api::{
        properties::internal::InheritPropertiesOps,
        storage::graph::edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
        view::{
            internal::{
                Base, EdgeFilterOps, Immutable, InheritCoreOps, InheritEdgeHistoryFilter,
                InheritListOps, InheritMaterialize, InheritNodeFilterOps, InheritNodeHistoryFilter,
                InheritStorageOps, InheritTimeSemantics, InternalLayerOps, Static,
            },
            Layer,
        },
    },
    prelude::GraphViewOps,
};
use raphtory_api::core::{entities::ELID, storage::timeindex::TimeIndexEntry};
use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
};

#[derive(Clone)]
pub struct LayeredGraph<G> {
    /// The underlying `Graph` object.
    pub graph: G,
    /// The layer this graphs points to.
    pub layers: LayerIds,
}

impl<'graph, G: GraphViewOps<'graph>> Immutable for LayeredGraph<G> {}

impl<G> Static for LayeredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph> + Debug> Debug for LayeredGraph<G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LayeredGraph")
            .field("graph", &self.graph as &dyn Debug)
            .field("layers", &self.layers)
            .finish()
    }
}

impl<'graph, G: GraphViewOps<'graph>> Base for LayeredGraph<G> {
    type Base = G;
    #[inline(always)]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for LayeredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritListOps for LayeredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreOps for LayeredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for LayeredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for LayeredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for LayeredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for LayeredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for LayeredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeFilterOps for LayeredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> LayeredGraph<G> {
    pub fn new(graph: G, layers: LayerIds) -> Self {
        Self { graph, layers }
    }

    /// Get the intersection between the previously requested layers and the layers of
    /// this view
    fn constrain(&self, layers: LayerIds) -> LayerIds {
        match layers {
            LayerIds::None => LayerIds::None,
            LayerIds::All => self.layers.clone(),
            _ => match &self.layers {
                LayerIds::All => layers,
                LayerIds::One(id) => match layers.find(*id) {
                    Some(layer) => LayerIds::One(layer),
                    None => LayerIds::None,
                },
                LayerIds::Multiple(ids) => {
                    // intersect the layers
                    let new_layers: Arc<[usize]> =
                        ids.iter().filter_map(|id| layers.find(id)).collect();
                    match new_layers.len() {
                        0 => LayerIds::None,
                        1 => LayerIds::One(new_layers[0]),
                        _ => LayerIds::Multiple(Multiple(new_layers)),
                    }
                }
                LayerIds::None => LayerIds::None,
            },
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>> InternalLayerOps for LayeredGraph<G> {
    fn layer_ids(&self) -> &LayerIds {
        &self.layers
    }

    fn layer_ids_from_names(&self, key: Layer) -> Result<LayerIds, GraphError> {
        Ok(self.constrain(self.graph.layer_ids_from_names(key)?))
    }

    fn valid_layer_ids_from_names(&self, key: Layer) -> LayerIds {
        self.constrain(self.graph.valid_layer_ids_from_names(key))
    }
}

impl<'graph, G: GraphViewOps<'graph>> EdgeFilterOps for LayeredGraph<G> {
    fn edges_filtered(&self) -> bool {
        !matches!(self.layers, LayerIds::All) || self.graph.edges_filtered()
    }

    fn edge_history_filtered(&self) -> bool {
        !matches!(self.layers, LayerIds::All) || self.graph.edge_history_filtered()
    }

    fn edge_list_trusted(&self) -> bool {
        matches!(self.layers, LayerIds::All) && self.graph.edge_list_trusted()
    }

    fn filter_edge_history(&self, eid: ELID, t: TimeIndexEntry, layer_ids: &LayerIds) -> bool {
        layer_ids.contains(&eid.layer()) && self.graph.filter_edge_history(eid, t, layer_ids)
    }

    fn filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool {
        edge.has_layer(layer_ids) && self.graph.filter_edge(edge, &layer_ids)
    }
}

#[cfg(test)]
mod test_layers {
    use crate::{
        db::graph::{graph::assert_graph_equal, views::deletion_graph::PersistentGraph},
        prelude::*,
        test_storage,
        test_utils::{build_graph, build_graph_layer, build_graph_strat},
    };
    use itertools::Itertools;
    use proptest::proptest;
    use raphtory_api::core::entities::GID;

    #[test]
    fn prop_test_layering() {
        proptest!(|(graph_f in build_graph_strat(10, 10, false), layer in proptest::sample::subsequence(&["_default", "a", "b"], 0..3))| {
            let g_layer_expected = Graph::from(build_graph_layer(&graph_f, layer.clone()));
            let g = Graph::from(build_graph(&graph_f));
                let g_layer = g.valid_layers(layer.clone());
                assert_graph_equal(&g_layer, &g_layer_expected);
        })
    }

    #[test]
    fn prop_test_layering_persistent_graph() {
        proptest!(|(graph_f in build_graph_strat(10, 10, true), layer in proptest::sample::subsequence(&["_default", "a", "b"], 0..3))| {
            let g_layer_expected = PersistentGraph::from(build_graph_layer(&graph_f, layer.clone()));
            let g = PersistentGraph::from(build_graph(&graph_f));
            let g_layer = g.valid_layers(layer.clone());
            assert_graph_equal(&g_layer, &g_layer_expected);
        })
    }

    #[test]
    fn test_layer_node() {
        let graph = Graph::new();

        graph.add_edge(0, 1, 2, NO_PROPS, Some("layer1")).unwrap();
        graph.add_edge(0, 2, 3, NO_PROPS, Some("layer2")).unwrap();
        graph.add_edge(3, 2, 4, NO_PROPS, Some("layer1")).unwrap();
        graph.add_edge(1, 4, 1, NO_PROPS, Some("layer3")).unwrap();

        test_storage!(&graph, |graph| {
            let neighbours = graph
                .layers(vec!["layer1", "layer2"])
                .unwrap()
                .node(1)
                .unwrap()
                .neighbours()
                .into_iter()
                .collect_vec();
            assert_eq!(
                neighbours[0]
                    .layers("layer2")
                    .unwrap()
                    .edges()
                    .id()
                    .collect_vec(),
                vec![(GID::U64(2), GID::U64(3))]
            );
            assert_eq!(
                graph
                    .layers("layer2")
                    .unwrap()
                    .node(neighbours[0].name())
                    .unwrap()
                    .edges()
                    .id()
                    .collect_vec(),
                vec![(GID::U64(2), GID::U64(3))]
            );
            let mut edges = graph
                .layers("layer1")
                .unwrap()
                .node(neighbours[0].name())
                .unwrap()
                .edges()
                .id()
                .filter_map(|(a, b)| a.to_u64().zip(b.to_u64()))
                .collect_vec();
            edges.sort();
            assert_eq!(edges, vec![(1, 2), (2, 4)]);
            let mut edges = graph
                .layers("layer1")
                .unwrap()
                .edges()
                .id()
                .filter_map(|(a, b)| a.to_u64().zip(b.to_u64()))
                .collect_vec();
            edges.sort();
            assert_eq!(edges, vec![(1, 2), (2, 4)]);
            let mut edges = graph
                .layers(vec!["layer1", "layer2"])
                .unwrap()
                .edges()
                .id()
                .filter_map(|(a, b)| a.to_u64().zip(b.to_u64()))
                .collect_vec();
            edges.sort();
            assert_eq!(edges, vec![(1, 2), (2, 3), (2, 4)]);

            let mut edges = graph
                .layers(["layer1", "layer3"])
                .unwrap()
                .window(0, 2)
                .edges()
                .id()
                .filter_map(|(a, b)| a.to_u64().zip(b.to_u64()))
                .collect_vec();
            edges.sort();
            assert_eq!(edges, vec![(1, 2), (4, 1)]);
        });
    }

    #[test]
    fn layering_tests() {
        let graph = Graph::new();
        let e1 = graph.add_edge(0, 1, 2, NO_PROPS, Some("1")).unwrap();
        graph.add_edge(1, 1, 2, NO_PROPS, Some("2")).unwrap();

        println!("edge: {e1:?}");
        // FIXME: this is weird, see issue #1458
        assert!(e1.has_layer("2"));
        let history = e1.layers("2").unwrap().history();
        println!("history: {:?}", history);
        assert!(e1.layers("2").unwrap().history().is_empty());

        test_storage!(&graph, |graph| {
            let e = graph.edge(1, 2).unwrap();
            // layers with non-existing layers errors
            assert!(e.layers(["1", "3"]).is_err());
            // valid_layers ignores non-existing layers
            assert_eq!(e.valid_layers(["1", "3"]).layer_names(), ["1"]);
            assert!(e.has_layer("1"));
            assert!(e.has_layer("2"));
            assert!(!e.has_layer("3"));
            assert!(e.valid_layers("1").has_layer("1"));
            assert!(!e.valid_layers("1").has_layer("2"));
        });
    }

    #[cfg(all(test, feature = "search"))]
    mod search_nodes_layer_graph_tests {
        use crate::{
            core::Prop,
            db::{
                api::view::{SearchableGraphOps, StaticGraphViewOps},
                graph::views::{
                    deletion_graph::PersistentGraph,
                    property_filter::{FilterExpr, PropertyFilterOps},
                },
            },
            prelude::{AdditionOps, Graph, LayerOps, NodeViewOps, PropertyFilter, TimeOps},
        };
        use std::ops::Range;

        fn init_graph<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
            let edges = vec![
                (6, "N1", "N2", vec![("p1", Prop::U64(2u64))], Some("layer1")),
                (7, "N1", "N2", vec![("p1", Prop::U64(1u64))], Some("layer2")),
                (6, "N2", "N3", vec![("p1", Prop::U64(1u64))], Some("layer1")),
                (7, "N2", "N3", vec![("p1", Prop::U64(2u64))], Some("layer2")),
                (8, "N3", "N4", vec![("p1", Prop::U64(1u64))], Some("layer1")),
                (9, "N4", "N5", vec![("p1", Prop::U64(1u64))], Some("layer1")),
                (5, "N5", "N6", vec![("p1", Prop::U64(1u64))], Some("layer1")),
                (6, "N5", "N6", vec![("p1", Prop::U64(2u64))], Some("layer2")),
                (5, "N6", "N7", vec![("p1", Prop::U64(1u64))], Some("layer1")),
                (6, "N6", "N7", vec![("p1", Prop::U64(1u64))], Some("layer2")),
                (3, "N7", "N8", vec![("p1", Prop::U64(1u64))], Some("layer1")),
                (5, "N7", "N8", vec![("p1", Prop::U64(1u64))], Some("layer2")),
                (3, "N8", "N1", vec![("p1", Prop::U64(1u64))], Some("layer1")),
                (4, "N8", "N1", vec![("p1", Prop::U64(2u64))], Some("layer2")),
            ];

            for (id, src, tgt, props, layer) in &edges {
                graph
                    .add_edge(*id, src, tgt, props.clone(), *layer)
                    .unwrap();
            }

            let nodes = vec![
                (6, "N1", vec![("p1", Prop::U64(2u64))], Some("air_nomad")),
                (7, "N1", vec![("p1", Prop::U64(1u64))], Some("air_nomad")),
                (6, "N2", vec![("p1", Prop::U64(1u64))], Some("water_tribe")),
                (7, "N2", vec![("p1", Prop::U64(2u64))], Some("water_tribe")),
                (8, "N3", vec![("p1", Prop::U64(1u64))], Some("air_nomad")),
                (9, "N4", vec![("p1", Prop::U64(1u64))], Some("air_nomad")),
                (5, "N5", vec![("p1", Prop::U64(1u64))], Some("air_nomad")),
                (6, "N5", vec![("p1", Prop::U64(2u64))], Some("air_nomad")),
                (5, "N6", vec![("p1", Prop::U64(1u64))], Some("fire_nation")),
                (6, "N6", vec![("p1", Prop::U64(1u64))], Some("fire_nation")),
                (3, "N7", vec![("p1", Prop::U64(1u64))], Some("air_nomad")),
                (5, "N7", vec![("p1", Prop::U64(1u64))], Some("air_nomad")),
                (3, "N8", vec![("p1", Prop::U64(1u64))], Some("fire_nation")),
                (4, "N8", vec![("p1", Prop::U64(2u64))], Some("fire_nation")),
            ];

            for (id, name, props, label) in &nodes {
                graph.add_node(*id, name, props.clone(), *label).unwrap();
            }

            graph
        }

        fn search_nodes_by_composite_filter<G: StaticGraphViewOps + AdditionOps>(
            graph: &G,
            filter: FilterExpr,
            layers: Vec<String>,
        ) -> Vec<String> {
            graph.create_index().unwrap();
            let lgv = graph
                .layers(layers.clone())
                .expect("Failed to get graph for layers");
            let mut results = lgv
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
            layers: Vec<String>,
        ) -> Vec<String> {
            graph.create_index().unwrap();
            let lgv = graph
                .layers(layers.clone())
                .expect("Failed to get graph for layers");

            let mut results = lgv
                .window(w.start, w.end)
                .search_nodes(filter, 10, 0)
                .expect("Failed to search for nodes")
                .into_iter()
                .map(|v| v.name())
                .collect::<Vec<_>>();
            results.sort();
            results
        }

        // Layers don't have any effect on the number of nodes in a graph.
        // In other words, it is as good as applying no layer filters.
        #[test]
        fn test_search_nodes_layer_graph() {
            let graph = Graph::new();
            let graph = init_graph(graph);

            let layers = vec!["layer1".into(), "layer2".into()];
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_nodes_by_composite_filter(&graph, filter, layers);
            assert_eq!(results, vec!["N1", "N3", "N4", "N6", "N7"]);

            let layers = vec!["layer1".into()];
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_nodes_by_composite_filter(&graph, filter, layers);
            assert_eq!(results, vec!["N1", "N3", "N4", "N6", "N7"]);

            let layers = vec!["layer2".into()];
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_nodes_by_composite_filter(&graph, filter, layers);
            assert_eq!(results, vec!["N1", "N3", "N4", "N6", "N7"]);
        }

        #[test]
        fn test_search_nodes_layer_graph_w() {
            let graph = Graph::new();
            let graph = init_graph(graph);

            let layers = vec!["layer1".into(), "layer2".into()];
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_nodes_by_composite_filter_w(&graph, 6..9, filter, layers);
            assert_eq!(results, vec!["N1", "N3", "N6"]);

            let layers = vec!["layer1".into()];
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_nodes_by_composite_filter_w(&graph, 6..9, filter, layers);
            assert_eq!(results, vec!["N1", "N3", "N6"]);

            let layers = vec!["layer2".into()];
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_nodes_by_composite_filter_w(&graph, 6..9, filter, layers);
            assert_eq!(results, vec!["N1", "N3", "N6"]);
        }

        #[test]
        fn test_search_nodes_persistent_layer_graph() {
            let graph = PersistentGraph::new();
            let graph = init_graph(graph);

            let layers = vec!["layer1".into(), "layer2".into()];
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_nodes_by_composite_filter(&graph, filter, layers);
            assert_eq!(results, vec!["N1", "N3", "N4", "N6", "N7"]);

            let layers = vec!["layer1".into()];
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_nodes_by_composite_filter(&graph, filter, layers);
            assert_eq!(results, vec!["N1", "N3", "N4", "N6", "N7"]);

            let layers = vec!["layer2".into()];
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_nodes_by_composite_filter(&graph, filter, layers);
            assert_eq!(results, vec!["N1", "N3", "N4", "N6", "N7"]);
        }

        #[test]
        fn test_search_nodes_persistent_layer_graph_w() {
            let graph = PersistentGraph::new();
            let graph = init_graph(graph);

            let layers = vec!["layer1".into(), "layer2".into()];
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_nodes_by_composite_filter_w(&graph, 6..9, filter, layers);
            assert_eq!(results, vec!["N1", "N3", "N6", "N7"]);

            let layers = vec!["layer1".into()];
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_nodes_by_composite_filter_w(&graph, 6..9, filter, layers);
            assert_eq!(results, vec!["N1", "N3", "N6", "N7"]);

            let layers = vec!["layer2".into()];
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_nodes_by_composite_filter_w(&graph, 6..9, filter, layers);
            assert_eq!(results, vec!["N1", "N3", "N6", "N7"]);
        }
    }

    #[cfg(all(test, feature = "search"))]
    mod search_edges_layer_graph_tests {
        use crate::{
            core::Prop,
            db::{
                api::view::{SearchableGraphOps, StaticGraphViewOps},
                graph::views::{
                    deletion_graph::PersistentGraph,
                    property_filter::{FilterExpr, PropertyFilterOps},
                },
            },
            prelude::{
                AdditionOps, EdgeViewOps, Graph, LayerOps, NodeViewOps, PropertyFilter, TimeOps,
            },
        };
        use std::ops::Range;

        fn init_graph<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
            graph
                .add_edge(6, "N1", "N2", [("p1", Prop::U64(2u64))], Some("layer1"))
                .unwrap();
            graph
                .add_edge(7, "N1", "N2", [("p1", Prop::U64(1u64))], Some("layer2"))
                .unwrap();

            graph
                .add_edge(6, "N2", "N3", [("p1", Prop::U64(1u64))], Some("layer1"))
                .unwrap();
            graph
                .add_edge(7, "N2", "N3", [("p1", Prop::U64(2u64))], Some("layer2"))
                .unwrap();

            graph
                .add_edge(8, "N3", "N4", [("p1", Prop::U64(1u64))], Some("layer1"))
                .unwrap();

            graph
                .add_edge(9, "N4", "N5", [("p1", Prop::U64(1u64))], Some("layer1"))
                .unwrap();

            graph
                .add_edge(5, "N5", "N6", [("p1", Prop::U64(1u64))], Some("layer1"))
                .unwrap();
            graph
                .add_edge(6, "N5", "N6", [("p1", Prop::U64(2u64))], Some("layer2"))
                .unwrap();

            graph
                .add_edge(5, "N6", "N7", [("p1", Prop::U64(1u64))], Some("layer1"))
                .unwrap();
            graph
                .add_edge(6, "N6", "N7", [("p1", Prop::U64(1u64))], Some("layer2"))
                .unwrap();

            graph
                .add_edge(3, "N7", "N8", [("p1", Prop::U64(1u64))], Some("layer1"))
                .unwrap();
            graph
                .add_edge(5, "N7", "N8", [("p1", Prop::U64(1u64))], Some("layer2"))
                .unwrap();

            graph
                .add_edge(3, "N8", "N1", [("p1", Prop::U64(1u64))], Some("layer1"))
                .unwrap();
            graph
                .add_edge(4, "N8", "N1", [("p1", Prop::U64(2u64))], Some("layer2"))
                .unwrap();

            graph
        }

        fn search_edges_by_composite_filter<G: StaticGraphViewOps + AdditionOps>(
            graph: &G,
            filter: FilterExpr,
            layers: Vec<String>,
        ) -> Vec<String> {
            graph.create_index().unwrap();
            let lgv = graph
                .layers(layers.clone())
                .expect("Failed to get graph for layers");
            let mut results = lgv
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
            layers: Vec<String>,
        ) -> Vec<String> {
            graph.create_index().unwrap();
            let lgv = graph
                .layers(layers.clone())
                .expect("Failed to get graph for layers");
            let mut results = lgv
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
        fn test_search_edges_layer_graph() {
            let graph = Graph::new();
            let graph = init_graph(graph);

            let layers = vec!["layer1".into(), "layer2".into()];
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_edges_by_composite_filter(&graph, filter, layers);
            assert_eq!(
                results,
                vec!["N1->N2", "N3->N4", "N4->N5", "N6->N7", "N7->N8"]
            );

            let layers = vec!["layer1".into()];
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_edges_by_composite_filter(&graph, filter, layers);
            assert_eq!(
                results,
                vec!["N2->N3", "N3->N4", "N4->N5", "N5->N6", "N6->N7", "N7->N8", "N8->N1"]
            );

            let layers = vec!["layer2".into()];
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_edges_by_composite_filter(&graph, filter, layers);
            assert_eq!(results, vec!["N1->N2", "N6->N7", "N7->N8"]);
        }

        #[test]
        fn test_search_edges_layer_graph_w() {
            let graph = Graph::new();
            let graph = init_graph(graph);

            // Edge Property Semantics:
            // 1. All property updates to an edge belong to a layer (or _default if no layer specified)
            // 2. However, when asked for a value of a particular property for an edge, the latest update
            // across all specified layers (or all layers if no layers specified) is returned!
            let layers = vec!["layer1".into(), "layer2".into()];
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_edges_by_composite_filter_w(&graph, 6..9, filter, layers);
            assert_eq!(results, vec!["N1->N2", "N3->N4", "N6->N7"]);

            // Edge Property Semantics:
            // When filtering by specific layer, filter criteria (p1==1) and latest semantics is applicable
            // only to that specific layer.
            let layers = vec!["layer1".into()];
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_edges_by_composite_filter_w(&graph, 6..9, filter, layers);
            assert_eq!(results, vec!["N2->N3", "N3->N4"]);

            let layers = vec!["layer2".into()];
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_edges_by_composite_filter_w(&graph, 6..9, filter, layers);
            assert_eq!(results, vec!["N1->N2", "N6->N7"]);
        }

        #[test]
        fn test_search_edges_persistent_layer_graph() {
            let graph = PersistentGraph::new();
            let graph = init_graph(graph);

            let layers = vec!["layer1".into(), "layer2".into()];
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_edges_by_composite_filter(&graph, filter, layers);
            assert_eq!(
                results,
                vec!["N1->N2", "N3->N4", "N4->N5", "N6->N7", "N7->N8"]
            );

            let layers = vec!["layer1".into()];
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_edges_by_composite_filter(&graph, filter, layers);
            assert_eq!(
                results,
                vec!["N2->N3", "N3->N4", "N4->N5", "N5->N6", "N6->N7", "N7->N8", "N8->N1"]
            );

            let layers = vec!["layer2".into()];
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_edges_by_composite_filter(&graph, filter, layers);
            assert_eq!(results, vec!["N1->N2", "N6->N7", "N7->N8"]);
        }

        #[test]
        fn test_search_edges_persistent_layer_graph_w() {
            let graph = PersistentGraph::new();
            let graph = init_graph(graph);

            let layers = vec!["layer1".into(), "layer2".into()];
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_edges_by_composite_filter_w(&graph, 6..9, filter, layers);

            // Why is the edge N8 -> N1 included in the results?
            // The reason edge N8 -> N1 is included as part of the results because of following two semantic reasons:
            //     .add_edge(3, "N8", "N1", [("p1", Prop::U64(1u64))], Some("layer1"))
            //     .add_edge(4, "N8", "N1", [("p1", Prop::U64(2u64))], Some("layer2"))
            // 1. As per layer graph semantics, every edge update belongs to a particular layer (or '_default' if no layer specified).
            //     This means the last_before is computed per layer and not across layers. In other words, when computing
            //     last_before for (N8->N1, layer1) and window(6, 9), t = 3 is the correct last before edge update timestamp and not t = 4
            //     because t=4 edge update is in layer2.
            // 2. Since the search is conducted across both the layers i.e., layer1 and layer2, the results are union of
            //     results from both layer1 and layer2.
            assert_eq!(results, vec!["N1->N2", "N3->N4", "N6->N7", "N7->N8"]);

            let layers = vec!["layer1".into()];
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_edges_by_composite_filter_w(&graph, 6..9, filter, layers);
            assert_eq!(
                results,
                vec!["N2->N3", "N3->N4", "N5->N6", "N6->N7", "N7->N8", "N8->N1"]
            );

            let layers = vec!["layer2".into()];
            let filter = PropertyFilter::property("p1").eq(1u64);
            let results = search_edges_by_composite_filter_w(&graph, 6..9, filter, layers);
            assert_eq!(results, vec!["N1->N2", "N6->N7", "N7->N8"]);
        }
    }
}
