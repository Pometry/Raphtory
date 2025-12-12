use raphtory::{
    db::{
        api::view::{internal::NodeHistoryFilter, StaticGraphViewOps},
        graph::views::deletion_graph::PersistentGraph,
    },
    prelude::{AdditionOps, GraphViewOps},
};
use raphtory_api::core::{entities::properties::prop::Prop, storage::timeindex::EventTime};
use raphtory_storage::core_ops::CoreGraphOps;

fn init_graph<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
    let nodes = [
        (6, "N1", Prop::U64(2)),
        (7, "N1", Prop::U64(1)),
        (6, "N2", Prop::U64(1)),
        (7, "N2", Prop::U64(2)),
        (8, "N3", Prop::U64(1)),
        (9, "N4", Prop::U64(1)),
        (5, "N5", Prop::U64(1)),
        (6, "N5", Prop::U64(2)),
        (5, "N6", Prop::U64(1)),
        (6, "N6", Prop::U64(1)),
        (3, "N7", Prop::U64(1)),
        (5, "N7", Prop::U64(1)),
        (3, "N8", Prop::U64(1)),
        (4, "N8", Prop::U64(2)),
    ];

    for (time, id, prop) in nodes {
        graph.add_node(time, id, [("p1", prop)], None).unwrap();
    }

    graph
}

#[test]
fn test_is_prop_update_latest() {
    let g = PersistentGraph::new();
    let g = init_graph(g);

    let prop_id = g.node_meta().temporal_prop_mapper().get_id("p1").unwrap();

    let node_id = g.node("N1").unwrap().node;
    let bool = g.is_node_prop_update_latest(prop_id, node_id, EventTime::end(7));
    assert!(bool);

    let node_id = g.node("N2").unwrap().node;
    let bool = g.is_node_prop_update_latest(prop_id, node_id, EventTime::end(6));
    assert!(!bool);

    let node_id = g.node("N3").unwrap().node;
    let bool = g.is_node_prop_update_latest(prop_id, node_id, EventTime::end(8));
    assert!(bool);

    let node_id = g.node("N4").unwrap().node;
    let bool = g.is_node_prop_update_latest(prop_id, node_id, EventTime::end(9));
    assert!(bool);

    let node_id = g.node("N5").unwrap().node;
    let bool = g.is_node_prop_update_latest(prop_id, node_id, EventTime::end(5));
    assert!(!bool);

    let node_id = g.node("N6").unwrap().node;
    let bool = g.is_node_prop_update_latest(prop_id, node_id, EventTime::end(5));
    assert!(!bool);
    let node_id = g.node("N6").unwrap().node;
    let bool = g.is_node_prop_update_latest(prop_id, node_id, EventTime::end(6));
    assert!(bool);

    let node_id = g.node("N7").unwrap().node;
    let bool = g.is_node_prop_update_latest(prop_id, node_id, EventTime::end(3));
    assert!(!bool);
    let node_id = g.node("N7").unwrap().node;
    let bool = g.is_node_prop_update_latest(prop_id, node_id, EventTime::end(5));
    assert!(bool);

    let node_id = g.node("N8").unwrap().node;
    let bool = g.is_node_prop_update_latest(prop_id, node_id, EventTime::end(3));
    assert!(!bool);
}

#[test]
fn test_is_prop_update_latest_w() {
    let g = PersistentGraph::new();
    let g = init_graph(g);

    let prop_id = g.node_meta().temporal_prop_mapper().get_id("p1").unwrap();
    let w = EventTime::start(6)..EventTime::start(9);

    let node_id = g.node("N1").unwrap().node;
    let bool = g.is_node_prop_update_latest_window(prop_id, node_id, EventTime::end(7), w.clone());
    assert!(bool);

    let node_id = g.node("N2").unwrap().node;
    let bool = g.is_node_prop_update_latest_window(prop_id, node_id, EventTime::end(6), w.clone());
    assert!(!bool);

    let node_id = g.node("N3").unwrap().node;
    let bool = g.is_node_prop_update_latest_window(prop_id, node_id, EventTime::end(8), w.clone());
    assert!(bool);

    let node_id = g.node("N4").unwrap().node;
    let bool = g.is_node_prop_update_latest_window(prop_id, node_id, EventTime::end(9), w.clone());
    assert!(!bool);

    let node_id = g.node("N5").unwrap().node;
    let bool = g.is_node_prop_update_latest_window(prop_id, node_id, EventTime::end(5), w.clone());
    assert!(!bool);

    let node_id = g.node("N6").unwrap().node;
    let bool = g.is_node_prop_update_latest_window(prop_id, node_id, EventTime::end(5), w.clone());
    assert!(!bool);
    let node_id = g.node("N6").unwrap().node;
    let bool = g.is_node_prop_update_latest_window(prop_id, node_id, EventTime::end(6), w.clone());
    assert!(bool);

    let node_id = g.node("N7").unwrap().node;
    let bool = g.is_node_prop_update_latest_window(prop_id, node_id, EventTime::end(3), w.clone());
    assert!(!bool);
    let node_id = g.node("N7").unwrap().node;
    let bool = g.is_node_prop_update_latest_window(prop_id, node_id, EventTime::end(5), w.clone());
    assert!(bool);

    let node_id = g.node("N8").unwrap().node;
    let bool = g.is_node_prop_update_latest_window(prop_id, node_id, EventTime::end(3), w.clone());
    assert!(!bool);
}
