#[cfg(test)]
mod test_edge_history_filter_persistent_graph {
    use raphtory::{
        db::{
            api::view::{
                internal::{EdgeHistoryFilter, InternalLayerOps},
                StaticGraphViewOps,
            },
            graph::views::deletion_graph::PersistentGraph,
        },
        prelude::{AdditionOps, GraphViewOps},
    };
    use raphtory_api::core::{entities::properties::prop::Prop, storage::timeindex::EventTime};
    use raphtory_storage::core_ops::CoreGraphOps;

    fn init_graph<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
        let edges = [
            (6, "N1", "N2", Prop::U64(2), Some("layer1")),
            (7, "N1", "N2", Prop::U64(1), Some("layer2")),
            (6, "N2", "N3", Prop::U64(1), Some("layer1")),
            (7, "N2", "N3", Prop::U64(2), Some("layer2")),
            (8, "N3", "N4", Prop::U64(1), Some("layer1")),
            (9, "N4", "N5", Prop::U64(1), Some("layer1")),
            (5, "N5", "N6", Prop::U64(1), Some("layer1")),
            (6, "N5", "N6", Prop::U64(2), Some("layer2")),
            (5, "N6", "N7", Prop::U64(1), Some("layer1")),
            (6, "N6", "N7", Prop::U64(1), Some("layer2")),
            (3, "N7", "N8", Prop::U64(1), Some("layer1")),
            (5, "N7", "N8", Prop::U64(1), Some("layer2")),
            (3, "N8", "N1", Prop::U64(1), Some("layer1")),
            (4, "N8", "N1", Prop::U64(2), Some("layer2")),
            (3, "N9", "N2", Prop::U64(1), Some("layer1")),
            (3, "N9", "N2", Prop::U64(2), Some("layer2")),
        ];

        for (time, src, dst, prop, layer) in edges {
            graph
                .add_edge(time, src, dst, [("p1", prop)], layer)
                .unwrap();
        }

        graph
    }

    #[test]
    fn test_is_edge_prop_update_latest() {
        let g = PersistentGraph::new();
        let g = init_graph(g);

        let prop_id = g.edge_meta().temporal_prop_mapper().get_id("p1").unwrap();

        let edge_id = g.edge("N1", "N2").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest(
            g.layer_ids(),
            g.get_layer_id("layer2").unwrap(),
            prop_id,
            edge_id,
            EventTime::end(7),
        );
        assert!(bool);

        let edge_id = g.edge("N2", "N3").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest(
            g.layer_ids(),
            g.get_layer_id("layer1").unwrap(),
            prop_id,
            edge_id,
            EventTime::end(6),
        );
        assert!(!bool);

        let edge_id = g.edge("N3", "N4").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest(
            g.layer_ids(),
            g.get_layer_id("layer1").unwrap(),
            prop_id,
            edge_id,
            EventTime::end(8),
        );
        assert!(bool);

        let edge_id = g.edge("N4", "N5").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest(
            g.layer_ids(),
            g.get_layer_id("layer1").unwrap(),
            prop_id,
            edge_id,
            EventTime::end(9),
        );
        assert!(bool);

        let edge_id = g.edge("N5", "N6").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest(
            g.layer_ids(),
            g.get_layer_id("layer1").unwrap(),
            prop_id,
            edge_id,
            EventTime::end(5),
        );
        assert!(!bool);

        let edge_id = g.edge("N6", "N7").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest(
            g.layer_ids(),
            g.get_layer_id("layer1").unwrap(),
            prop_id,
            edge_id,
            EventTime::end(5),
        );
        assert!(!bool);
        let edge_id = g.edge("N6", "N7").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest(
            g.layer_ids(),
            g.get_layer_id("layer2").unwrap(),
            prop_id,
            edge_id,
            EventTime::end(6),
        );
        assert!(bool);

        let edge_id = g.edge("N7", "N8").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest(
            g.layer_ids(),
            g.get_layer_id("layer1").unwrap(),
            prop_id,
            edge_id,
            EventTime::end(3),
        );
        assert!(!bool);
        let edge_id = g.edge("N7", "N8").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest(
            g.layer_ids(),
            g.get_layer_id("layer2").unwrap(),
            prop_id,
            edge_id,
            EventTime::end(5),
        );
        assert!(bool);

        let edge_id = g.edge("N8", "N1").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest(
            g.layer_ids(),
            g.get_layer_id("layer1").unwrap(),
            prop_id,
            edge_id,
            EventTime::end(3),
        );
        assert!(!bool);

        // TODO: Revisit this test
        // let edge_id = g.edge("N9", "N2").unwrap().edge.pid();
        // let bool = g.is_edge_prop_update_latest(prop_id, edge_id, EventTime::end(3));
        // assert!(!bool);
    }

    #[test]
    fn test_is_edge_prop_update_latest_w() {
        let g = PersistentGraph::new();
        let g = init_graph(g);

        let prop_id = g.edge_meta().temporal_prop_mapper().get_id("p1").unwrap();
        let w = EventTime::start(6)..EventTime::start(9);

        let edge_id = g.edge("N1", "N2").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest_window(
            g.layer_ids(),
            g.get_layer_id("layer1").unwrap(),
            prop_id,
            edge_id,
            EventTime::end(7),
            w.clone(),
        );
        assert!(bool);

        let edge_id = g.edge("N2", "N3").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest_window(
            g.layer_ids(),
            g.get_layer_id("layer1").unwrap(),
            prop_id,
            edge_id,
            EventTime::end(6),
            w.clone(),
        );
        assert!(!bool);

        let edge_id = g.edge("N3", "N4").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest_window(
            g.layer_ids(),
            g.get_layer_id("layer1").unwrap(),
            prop_id,
            edge_id,
            EventTime::end(8),
            w.clone(),
        );
        assert!(bool);

        let edge_id = g.edge("N4", "N5").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest_window(
            g.layer_ids(),
            g.get_layer_id("layer1").unwrap(),
            prop_id,
            edge_id,
            EventTime::end(9),
            w.clone(),
        );
        assert!(!bool);

        let edge_id = g.edge("N5", "N6").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest_window(
            g.layer_ids(),
            g.get_layer_id("layer1").unwrap(),
            prop_id,
            edge_id,
            EventTime::end(5),
            w.clone(),
        );
        assert!(!bool);

        let edge_id = g.edge("N6", "N7").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest_window(
            g.layer_ids(),
            g.get_layer_id("layer1").unwrap(),
            prop_id,
            edge_id,
            EventTime::end(5),
            w.clone(),
        );
        assert!(!bool);
        let edge_id = g.edge("N6", "N7").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest_window(
            g.layer_ids(),
            g.get_layer_id("layer2").unwrap(),
            prop_id,
            edge_id,
            EventTime::end(6),
            w.clone(),
        );
        assert!(bool);

        let edge_id = g.edge("N7", "N8").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest_window(
            g.layer_ids(),
            g.get_layer_id("layer1").unwrap(),
            prop_id,
            edge_id,
            EventTime::end(3),
            w.clone(),
        );
        assert!(!bool);
        let edge_id = g.edge("N7", "N8").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest_window(
            g.layer_ids(),
            g.get_layer_id("layer2").unwrap(),
            prop_id,
            edge_id,
            EventTime::end(5),
            w.clone(),
        );
        assert!(bool);

        let edge_id = g.edge("N8", "N1").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest_window(
            g.layer_ids(),
            g.get_layer_id("layer1").unwrap(),
            prop_id,
            edge_id,
            EventTime::end(3),
            w.clone(),
        );
        assert!(!bool);

        // TODO: Revisit this test
        // let edge_id = g.edge("N9", "N2").unwrap().edge.pid();
        // let bool = g.is_edge_prop_update_latest_window(prop_id, edge_id, EventTime::end(3), w.clone());
        // assert!(!bool);
    }
}
