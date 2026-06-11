use raphtory::{
    db::{
        api::view::StaticGraphViewOps,
        graph::views::{layer_graph::LayeredGraph, window_graph::WindowedGraph},
    },
    prelude::{LayerOps, TimeOps},
};
use raphtory_tests::assertions::GraphTransformer;
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

pub mod test_nodes_filters_layer_graph {
    use raphtory::{
        db::{
            api::view::StaticGraphViewOps,
            graph::views::filter::model::property_filter::ops::PropertyFilterOps,
        },
        prelude::AdditionOps,
    };
    use raphtory_api::core::entities::properties::prop::Prop;

    use crate::filter_tests::test_layers::{
        LayeredGraphTransformer, LayeredGraphWindowTransformer,
    };
    use raphtory::{db::graph::views::filter::model::PropertyFilterFactory, prelude::NodeFilter};

    use raphtory_tests::assertions::{
        assert_filter_nodes_results, assert_search_nodes_results, TestGraphVariants, TestVariants,
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
            graph
                .add_node(*id, name, props.clone(), *label, None)
                .unwrap();
        }

        graph
    }

    // Layers don't have any effect on the number of nodes in a graph.
    // In other words, it is as good as applying no layer filters.
    #[test]
    fn test_nodes_filters() {
        let layers: Vec<String> = vec!["layer1".into(), "layer2".into()];
        let filter = NodeFilter.property("p1").eq(1u64);
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
        let filter = NodeFilter.property("p1").ge(2u64);
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
        let filter = NodeFilter.property("p1").le(1u64);
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
        let filter = NodeFilter.property("p1").lt(2u64);
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
        let filter = NodeFilter.property("p1").gt(1u64);
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
        let filter = NodeFilter.property("p1").eq(1u64);
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
        let filter = NodeFilter.property("p1").ge(2u64);
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
        let filter = NodeFilter.property("p1").lt(2u64);
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
        let filter = NodeFilter.property("p1").eq(1u64);
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
        let filter = NodeFilter.property("p1").lt(2u64);
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
        let filter = NodeFilter.property("p1").gt(1u64);
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
    use raphtory::{
        db::{
            api::view::StaticGraphViewOps,
            graph::views::filter::model::{
                property_filter::ops::PropertyFilterOps, PropertyFilterFactory,
            },
        },
        prelude::{AdditionOps, EdgeFilter},
    };
    use raphtory_api::core::entities::properties::prop::Prop;
    use raphtory_tests::assertions::{
        assert_filter_edges_results, assert_search_edges_results, TestGraphVariants, TestVariants,
    };

    use crate::filter_tests::test_layers::{
        LayeredGraphTransformer, LayeredGraphWindowTransformer,
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
        let filter = EdgeFilter.property("p1").eq(1u64);
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
        let filter = EdgeFilter.property("p1").le(1u64);
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
        let filter = EdgeFilter.property("p1").ge(2u64);
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
        let filter = EdgeFilter.property("p1").lt(2u64);
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
        let filter = EdgeFilter.property("p1").gt(1u64);
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
        let filter = EdgeFilter.property("p1").eq(1u64);
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
        let filter = EdgeFilter.property("p1").lt(2u64);
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
        let filter = EdgeFilter.property("p1").gt(1u64);
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
        let filter = EdgeFilter.property("p1").eq(1u64);

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
            vec![TestGraphVariants::PersistentGraph],
        );
        assert_search_edges_results(
            init_graph,
            LayeredGraphWindowTransformer(layers, 6..9),
            filter,
            &expected_results,
            vec![TestGraphVariants::PersistentGraph],
        );

        let layers: Vec<String> = vec!["layer1".into()];
        let filter = EdgeFilter.property("p1").le(1u64);
        let expected_results = vec!["N2->N3", "N3->N4", "N5->N6", "N6->N7", "N7->N8", "N8->N1"];
        assert_filter_edges_results(
            init_graph,
            LayeredGraphWindowTransformer(layers.clone(), 6..9),
            filter.clone(),
            &expected_results,
            vec![TestGraphVariants::PersistentGraph],
        );
        assert_search_edges_results(
            init_graph,
            LayeredGraphWindowTransformer(layers, 6..9),
            filter,
            &expected_results,
            vec![TestGraphVariants::PersistentGraph],
        );

        let layers: Vec<String> = vec!["layer2".into()];
        let filter = EdgeFilter.property("p1").ge(2u64);
        let expected_results = vec!["N2->N3", "N5->N6", "N8->N1"];
        assert_filter_edges_results(
            init_graph,
            LayeredGraphWindowTransformer(layers.clone(), 6..9),
            filter.clone(),
            &expected_results,
            vec![TestGraphVariants::PersistentGraph],
        );
        assert_search_edges_results(
            init_graph,
            LayeredGraphWindowTransformer(layers, 6..9),
            filter,
            &expected_results,
            vec![TestGraphVariants::PersistentGraph],
        );
    }
}
