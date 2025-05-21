use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
};

use crate::{
    core::{
        entities::{LayerIds, Multiple},
        utils::errors::GraphError,
    },
    db::api::{
        properties::internal::InheritPropertiesOps,
        view::{
            internal::{
                Base, Immutable, InheritCoreOps, InheritEdgeFilterOps, InheritEdgeHistoryFilter,
                InheritListOps, InheritMaterialize, InheritNodeFilterOps, InheritNodeHistoryFilter,
                InheritTimeSemantics, InternalLayerOps, Static,
            },
            Layer,
        },
    },
    prelude::GraphViewOps,
};

use crate::db::api::view::internal::InheritStorageOps;

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

impl<'graph, G: GraphViewOps<'graph>> InheritNodeFilterOps for LayeredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreOps for LayeredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for LayeredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for LayeredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritEdgeFilterOps for LayeredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for LayeredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for LayeredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for LayeredGraph<G> {}

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

#[cfg(test)]
mod test_layers {
    use crate::{prelude::*, test_storage};
    use itertools::Itertools;
    use raphtory_api::core::entities::GID;

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

        // FIXME: this is weird, see issue #1458
        assert!(e1.has_layer("2"));
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

    mod test_filters_layer_graph {
        use crate::{
            db::{
                api::view::StaticGraphViewOps,
                graph::{
                    assertions::GraphTransformer,
                    views::{layer_graph::LayeredGraph, window_graph::WindowedGraph},
                },
            },
            prelude::{GraphViewOps, LayerOps, NodeViewOps, TimeOps},
        };
        use std::ops::Range;

        struct LayeredGraphTransformer(Vec<String>);

        impl GraphTransformer for LayeredGraphTransformer {
            type Return<G: StaticGraphViewOps> = LayeredGraph<G>;
            fn apply<G: StaticGraphViewOps>(&self, graph: G) -> Self::Return<G> {
                graph.layers(self.0.clone()).unwrap()
            }
        }

        struct LayeredGraphWindowTransformer(Vec<String>, Range<i64>);

        impl GraphTransformer for LayeredGraphWindowTransformer {
            type Return<G: StaticGraphViewOps> = WindowedGraph<LayeredGraph<G>>;
            fn apply<G: StaticGraphViewOps>(&self, graph: G) -> Self::Return<G> {
                graph
                    .layers(self.0.clone())
                    .unwrap()
                    .window(self.1.start, self.1.end)
            }
        }

        mod test_nodes_filters_layer_graph {
            use crate::{
                core::Prop,
                db::{
                    api::view::StaticGraphViewOps, graph::views::filter::model::PropertyFilterOps,
                },
                prelude::{AdditionOps, PropertyFilter},
            };

            use crate::db::graph::{
                assertions::{
                    assert_filter_nodes_results, assert_search_nodes_results, TestGraphVariants,
                    TestVariants,
                },
                views::layer_graph::test_layers::test_filters_layer_graph::{
                    LayeredGraphTransformer, LayeredGraphWindowTransformer,
                },
            };

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

            // Layers don't have any effect on the number of nodes in a graph.
            // In other words, it is as good as applying no layer filters.
            #[test]
            fn test_nodes_filters() {
                let layers: Vec<String> = vec!["layer1".into(), "layer2".into()];
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3", "N4", "N6", "N7"];
                assert_filter_nodes_results(
                    init_graph,
                    LayeredGraphTransformer(layers.clone()),
                    filter.clone(),
                    &expected_results,
                    TestVariants::All,
                );
                assert_search_nodes_results(
                    init_graph,
                    LayeredGraphTransformer(layers),
                    filter,
                    &expected_results,
                    TestVariants::All,
                );

                let layers: Vec<String> = vec!["layer1".into()];
                let filter = PropertyFilter::property("p1").ge(2u64);
                let expected_results = vec!["N2", "N5", "N8"];
                assert_filter_nodes_results(
                    init_graph,
                    LayeredGraphTransformer(layers.clone()),
                    filter.clone(),
                    &expected_results,
                    TestVariants::All,
                );
                assert_search_nodes_results(
                    init_graph,
                    LayeredGraphTransformer(layers),
                    filter,
                    &expected_results,
                    TestVariants::All,
                );

                let layers: Vec<String> = vec!["layer2".into()];
                let filter = PropertyFilter::property("p1").le(1u64);
                let expected_results = vec!["N1", "N3", "N4", "N6", "N7"];
                assert_filter_nodes_results(
                    init_graph,
                    LayeredGraphTransformer(layers.clone()),
                    filter.clone(),
                    &expected_results,
                    TestVariants::All,
                );
                assert_search_nodes_results(
                    init_graph,
                    LayeredGraphTransformer(layers),
                    filter,
                    &expected_results,
                    TestVariants::All,
                );

                let layers: Vec<String> = vec!["layer1".into()];
                let filter = PropertyFilter::property("p1").lt(2u64);
                let expected_results = vec!["N1", "N3", "N4", "N6", "N7"];
                assert_filter_nodes_results(
                    init_graph,
                    LayeredGraphTransformer(layers.clone()),
                    filter.clone(),
                    &expected_results,
                    TestVariants::All,
                );
                assert_search_nodes_results(
                    init_graph,
                    LayeredGraphTransformer(layers),
                    filter,
                    &expected_results,
                    TestVariants::All,
                );

                let layers: Vec<String> = vec!["layer2".into()];
                let filter = PropertyFilter::property("p1").gt(1u64);
                let expected_results = vec!["N2", "N5", "N8"];
                assert_filter_nodes_results(
                    init_graph,
                    LayeredGraphTransformer(layers.clone()),
                    filter.clone(),
                    &expected_results,
                    TestVariants::All,
                );
                assert_search_nodes_results(
                    init_graph,
                    LayeredGraphTransformer(layers),
                    filter,
                    &expected_results,
                    TestVariants::All,
                );
            }

            #[test]
            fn test_nodes_filters_w() {
                // TODO: Enable event_disk_graph for filter_nodes once bug fixed: https://github.com/Pometry/Raphtory/issues/2098
                let layers: Vec<String> = vec!["layer1".into(), "layer2".into()];
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3", "N6"];
                assert_filter_nodes_results(
                    init_graph,
                    LayeredGraphWindowTransformer(layers.clone(), 6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    LayeredGraphWindowTransformer(layers.clone(), 6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let layers: Vec<String> = vec!["layer1".into()];
                let filter = PropertyFilter::property("p1").ge(2u64);
                let expected_results = vec!["N2", "N5"];
                assert_filter_nodes_results(
                    init_graph,
                    LayeredGraphWindowTransformer(layers.clone(), 6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    LayeredGraphWindowTransformer(layers.clone(), 6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let layers: Vec<String> = vec!["layer2".into()];
                let filter = PropertyFilter::property("p1").lt(2u64);
                let expected_results = vec!["N1", "N3", "N6"];
                assert_filter_nodes_results(
                    init_graph,
                    LayeredGraphWindowTransformer(layers.clone(), 6..9),
                    filter.clone(),
                    &expected_results,
                    vec![TestGraphVariants::Graph],
                );
                assert_search_nodes_results(
                    init_graph,
                    LayeredGraphWindowTransformer(layers.clone(), 6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );
            }

            #[test]
            fn test_nodes_filters_pg_w() {
                let layers: Vec<String> = vec!["layer1".into(), "layer2".into()];
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3", "N6", "N7"];
                assert_filter_nodes_results(
                    init_graph,
                    LayeredGraphWindowTransformer(layers.clone(), 6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    LayeredGraphWindowTransformer(layers.clone(), 6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let layers: Vec<String> = vec!["layer1".into()];
                let filter = PropertyFilter::property("p1").lt(2u64);
                let expected_results = vec!["N1", "N3", "N6", "N7"];
                assert_filter_nodes_results(
                    init_graph,
                    LayeredGraphWindowTransformer(layers.clone(), 6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    LayeredGraphWindowTransformer(layers.clone(), 6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let layers: Vec<String> = vec!["layer2".into()];
                let filter = PropertyFilter::property("p1").gt(1u64);
                let expected_results = vec!["N2", "N5", "N8"];
                assert_filter_nodes_results(
                    init_graph,
                    LayeredGraphWindowTransformer(layers.clone(), 6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
                assert_search_nodes_results(
                    init_graph,
                    LayeredGraphWindowTransformer(layers.clone(), 6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
            }
        }

        mod test_edges_filters_layer_graph {
            use crate::{
                core::Prop,
                db::{
                    api::view::StaticGraphViewOps,
                    graph::{
                        assertions::{
                            assert_filter_edges_results, assert_search_edges_results, TestVariants,
                        },
                        views::{
                            filter::model::PropertyFilterOps,
                            layer_graph::test_layers::test_filters_layer_graph::{
                                LayeredGraphTransformer, LayeredGraphWindowTransformer,
                            },
                        },
                    },
                },
                prelude::{AdditionOps, PropertyFilter},
            };

            fn init_graph<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
                let edges = vec![
                    (6, "N1", "N2", 2u64, "layer1"),
                    (7, "N1", "N2", 1u64, "layer2"),
                    (6, "N2", "N3", 1u64, "layer1"),
                    (7, "N2", "N3", 2u64, "layer2"),
                    (8, "N3", "N4", 1u64, "layer1"),
                    (9, "N4", "N5", 1u64, "layer1"),
                    (5, "N5", "N6", 1u64, "layer1"),
                    (6, "N5", "N6", 2u64, "layer2"),
                    (5, "N6", "N7", 1u64, "layer1"),
                    (6, "N6", "N7", 1u64, "layer2"),
                    (3, "N7", "N8", 1u64, "layer1"),
                    (5, "N7", "N8", 1u64, "layer2"),
                    (3, "N8", "N1", 1u64, "layer1"),
                    (4, "N8", "N1", 2u64, "layer2"),
                ];

                for (ts, src, dst, p1_val, layer) in edges {
                    graph
                        .add_edge(ts, src, dst, [("p1", Prop::U64(p1_val))], Some(layer))
                        .unwrap();
                }

                graph
            }

            #[test]
            fn test_edges_filters() {
                // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph.
                let layers: Vec<String> = vec!["layer1".into(), "layer2".into()];
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4", "N4->N5", "N6->N7", "N7->N8"];
                assert_filter_edges_results(
                    init_graph,
                    LayeredGraphTransformer(layers.clone()),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_edges_results(
                    init_graph,
                    LayeredGraphTransformer(layers),
                    filter,
                    &expected_results,
                    TestVariants::All,
                );

                let layers: Vec<String> = vec!["layer1".into()];
                let filter = PropertyFilter::property("p1").le(1u64);
                let expected_results = vec![
                    "N2->N3", "N3->N4", "N4->N5", "N5->N6", "N6->N7", "N7->N8", "N8->N1",
                ];
                assert_filter_edges_results(
                    init_graph,
                    LayeredGraphTransformer(layers.clone()),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_edges_results(
                    init_graph,
                    LayeredGraphTransformer(layers),
                    filter,
                    &expected_results,
                    TestVariants::All,
                );

                let layers: Vec<String> = vec!["layer2".into()];
                let filter = PropertyFilter::property("p1").ge(2u64);
                let expected_results = vec!["N2->N3", "N5->N6", "N8->N1"];
                assert_filter_edges_results(
                    init_graph,
                    LayeredGraphTransformer(layers.clone()),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_edges_results(
                    init_graph,
                    LayeredGraphTransformer(layers),
                    filter,
                    &expected_results,
                    TestVariants::All,
                );

                let layers: Vec<String> = vec!["layer1".into()];
                let filter = PropertyFilter::property("p1").lt(2u64);
                let expected_results = vec![
                    "N2->N3", "N3->N4", "N4->N5", "N5->N6", "N6->N7", "N7->N8", "N8->N1",
                ];
                assert_filter_edges_results(
                    init_graph,
                    LayeredGraphTransformer(layers.clone()),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_edges_results(
                    init_graph,
                    LayeredGraphTransformer(layers),
                    filter,
                    &expected_results,
                    TestVariants::All,
                );

                let layers: Vec<String> = vec!["layer2".into()];
                let filter = PropertyFilter::property("p1").gt(1u64);
                let expected_results = vec!["N2->N3", "N5->N6", "N8->N1"];
                assert_filter_edges_results(
                    init_graph,
                    LayeredGraphTransformer(layers.clone()),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_edges_results(
                    init_graph,
                    LayeredGraphTransformer(layers),
                    filter,
                    &expected_results,
                    TestVariants::All,
                );
            }

            #[test]
            fn test_edges_filter_w() {
                // Edge Property Semantics:
                // 1. All property updates to an edge belong to a layer (or _default if no layer specified)
                // 2. However, when asked for a value of a particular property for an edge, the latest update
                // across all specified layers (or all layers if no layers specified) is returned!
                let layers: Vec<String> = vec!["layer1".into(), "layer2".into()];
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4", "N6->N7"];
                assert_filter_edges_results(
                    init_graph,
                    LayeredGraphWindowTransformer(layers.clone(), 6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_edges_results(
                    init_graph,
                    LayeredGraphWindowTransformer(layers, 6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                // Edge Property Semantics:
                // When filtering by specific layer, filter criteria (p1==1) and latest semantics is applicable
                // only to that specific layer.
                let layers: Vec<String> = vec!["layer1".into()];
                let filter = PropertyFilter::property("p1").lt(2u64);
                let expected_results = vec!["N2->N3", "N3->N4"];
                assert_filter_edges_results(
                    init_graph,
                    LayeredGraphWindowTransformer(layers.clone(), 6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_edges_results(
                    init_graph,
                    LayeredGraphWindowTransformer(layers, 6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );

                let layers: Vec<String> = vec!["layer2".into()];
                let filter = PropertyFilter::property("p1").gt(1u64);
                let expected_results = vec!["N2->N3", "N5->N6"];
                assert_filter_edges_results(
                    init_graph,
                    LayeredGraphWindowTransformer(layers.clone(), 6..9),
                    filter.clone(),
                    &expected_results,
                    TestVariants::EventOnly,
                );
                assert_search_edges_results(
                    init_graph,
                    LayeredGraphWindowTransformer(layers, 6..9),
                    filter,
                    &expected_results,
                    TestVariants::EventOnly,
                );
            }

            #[test]
            fn test_edges_filters_pg_w() {
                // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph.
                let layers: Vec<String> = vec!["layer1".into(), "layer2".into()];
                let filter = PropertyFilter::property("p1").eq(1u64);

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
                let expected_results = vec!["N1->N2", "N3->N4", "N6->N7", "N7->N8"];
                assert_filter_edges_results(
                    init_graph,
                    LayeredGraphWindowTransformer(layers.clone(), 6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    LayeredGraphWindowTransformer(layers, 6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let layers: Vec<String> = vec!["layer1".into()];
                let filter = PropertyFilter::property("p1").le(1u64);
                let expected_results =
                    vec!["N2->N3", "N3->N4", "N5->N6", "N6->N7", "N7->N8", "N8->N1"];
                assert_filter_edges_results(
                    init_graph,
                    LayeredGraphWindowTransformer(layers.clone(), 6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    LayeredGraphWindowTransformer(layers, 6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );

                let layers: Vec<String> = vec!["layer2".into()];
                let filter = PropertyFilter::property("p1").ge(2u64);
                let expected_results = vec!["N2->N3", "N5->N6", "N8->N1"];
                assert_filter_edges_results(
                    init_graph,
                    LayeredGraphWindowTransformer(layers.clone(), 6..9),
                    filter.clone(),
                    &expected_results,
                    vec![],
                );
                assert_search_edges_results(
                    init_graph,
                    LayeredGraphWindowTransformer(layers, 6..9),
                    filter,
                    &expected_results,
                    TestVariants::PersistentOnly,
                );
            }
        }
    }
}
