use itertools::Itertools;
use raphtory::{
    db::api::view::history::{compose_history_from_items, History, InternalHistoryOps, Intervals},
    prelude::*,
};
use raphtory_api::core::{
    entities::properties::prop::Prop,
    storage::timeindex::{AsTime, EventTime},
};
use std::error::Error;

#[test]
fn test_neighbours_history() -> Result<(), Box<dyn Error>> {
    let graph = Graph::new();
    let node = graph.add_node(1, "node", NO_PROPS, None, None).unwrap();
    let node2 = graph.add_node(2, "node2", NO_PROPS, None, None).unwrap();
    let node3 = graph.add_node(3, "node3", NO_PROPS, None, None).unwrap();
    let _edge = graph.add_edge(4, &node, &node2, NO_PROPS, None).unwrap();
    let _edge2 = graph.add_edge(5, &node, &node3, NO_PROPS, None).unwrap();
    let node4 = graph.add_node(6, "node4", NO_PROPS, None, None).unwrap();
    let _edge3 = graph.add_edge(7, &node2, &node4, NO_PROPS, None).unwrap();

    let history = graph.node("node").unwrap().neighbours().combined_history();
    assert_eq!(history.earliest_time().unwrap().t(), 2);
    assert_eq!(history.t().collect(), [2, 3, 4, 5, 7]);

    let history2 = graph.nodes().neighbours().combined_history();
    assert_eq!(history2.earliest_time().unwrap().t(), 1);
    assert_eq!(history2.latest_time().unwrap().t(), 7);
    let mut history2_collected = history2.t().collect();
    history2_collected.dedup();
    assert_eq!(history2_collected, [1, 2, 3, 4, 5, 6, 7]);

    Ok(())
}

#[test]
fn test_intervals() -> Result<(), Box<dyn Error>> {
    let graph = Graph::new();
    let node = graph.add_node(1, "node", NO_PROPS, None, None).unwrap();
    graph.add_node(4, "node", NO_PROPS, None, None).unwrap();
    graph.add_node(10, "node", NO_PROPS, None, None).unwrap();
    graph.add_node(30, "node", NO_PROPS, None, None).unwrap();
    let interval = Intervals(&node);
    assert_eq!(interval.collect(), &[3, 6, 20]);

    // make sure there are no intervals (1 time entry)
    let node2 = graph.add_node(1, "node2", NO_PROPS, None, None).unwrap();
    let interval2 = Intervals(&node2);
    assert_eq!(interval2.collect(), Vec::<i64>::new());
    Ok(())
}

#[test]
fn test_intervals_same_timestamp() -> Result<(), Box<dyn Error>> {
    let graph = Graph::new();
    let node = graph.add_node(1, "node", NO_PROPS, None, None).unwrap();
    graph.add_node(1, "node", NO_PROPS, None, None).unwrap();
    let interval = Intervals(&node);
    assert_eq!(interval.collect(), &[0]);

    graph.add_node(2, "node", NO_PROPS, None, None).unwrap();
    assert_eq!(interval.collect(), &[0, 1]);
    Ok(())
}

#[test]
fn test_intervals_mean() -> Result<(), Box<dyn Error>> {
    let graph = Graph::new();
    let node = graph.add_node(1, "node", NO_PROPS, None, None).unwrap();
    graph.add_node(4, "node", NO_PROPS, None, None).unwrap();
    graph.add_node(10, "node", NO_PROPS, None, None).unwrap();
    graph.add_node(30, "node", NO_PROPS, None, None).unwrap();
    let interval = Intervals(&node);
    assert_eq!(interval.mean(), Some(29f64 / 3f64));

    // make sure mean is None if there is no interval to be calculated (1 time entry)
    let node2 = graph.add_node(1, "node2", NO_PROPS, None, None).unwrap();
    let interval2 = Intervals(&node2);
    assert_eq!(interval2.mean(), None);
    Ok(())
}

#[test]
fn test_intervals_median() -> Result<(), Box<dyn Error>> {
    let graph = Graph::new();
    let node = graph.add_node(1, "node", NO_PROPS, None, None).unwrap();
    graph.add_node(30, "node", NO_PROPS, None, None).unwrap();
    graph.add_node(31, "node", NO_PROPS, None, None).unwrap();
    graph.add_node(40, "node", NO_PROPS, None, None).unwrap(); // intervals are 29, 1, 9
    let interval = Intervals(&node);
    assert_eq!(interval.median(), Some(9));

    // make sure median is None if there is no interval to be calculated (1 time entry)
    let node2 = graph.add_node(1, "node2", NO_PROPS, None, None).unwrap();
    let interval2 = Intervals(&node2);
    assert_eq!(interval2.median(), None);
    Ok(())
}

#[test]
fn test_intervals_max() -> Result<(), Box<dyn Error>> {
    let graph = Graph::new();
    let node = graph.add_node(1, "node", NO_PROPS, None, None).unwrap();
    graph.add_node(30, "node", NO_PROPS, None, None).unwrap();
    graph.add_node(31, "node", NO_PROPS, None, None).unwrap();
    graph.add_node(40, "node", NO_PROPS, None, None).unwrap(); // intervals are 29, 1, 9
    let interval = Intervals(&node);
    assert_eq!(interval.max(), Some(29));

    // make sure max is None if there is no interval to be calculated (1 time entry)
    let node2 = graph.add_node(1, "node2", NO_PROPS, None, None).unwrap();
    let interval2 = Intervals(&node2);
    assert_eq!(interval2.max(), None);
    Ok(())
}

// test nodes and edges
#[test]
fn test_basic() -> Result<(), Box<dyn Error>> {
    let graph = Graph::new();
    let dumbledore_node = graph
        .add_node(
            1,
            "Dumbledore",
            [("type", Prop::str("Character"))],
            None,
            None,
        )
        .unwrap();

    let harry_node = graph
        .add_node(2, "Harry", [("type", Prop::str("Character"))], None, None)
        .unwrap();

    let character_edge = graph
        .add_edge(
            3,
            "Dumbledore",
            "Harry",
            [("meeting", Prop::str("Character Co-occurrence"))],
            None,
        )
        .unwrap();

    let dumbledore_node_history_object = History::new(dumbledore_node.clone());
    assert_eq!(
        dumbledore_node_history_object.iter().collect_vec(),
        vec![EventTime::new(1, 0), EventTime::new(3, 2)]
    );

    let harry_node_history_object = History::new(harry_node.clone());
    assert_eq!(
        harry_node_history_object.iter().collect_vec(),
        vec![EventTime::new(2, 1), EventTime::new(3, 2)]
    );

    let edge_history_object = History::new(character_edge.clone());
    assert_eq!(
        edge_history_object.iter().collect_vec(),
        vec![EventTime::new(3, 2)]
    );

    let tmp_vector: Vec<Box<dyn InternalHistoryOps>> = vec![
        Box::new(dumbledore_node),
        Box::new(harry_node),
        Box::new(character_edge),
    ];
    let composite_history_object = compose_history_from_items(tmp_vector);
    assert_eq!(
        composite_history_object.iter().collect_vec(),
        vec![
            EventTime::new(1, 0),
            EventTime::new(2, 1),
            EventTime::new(3, 2),
            EventTime::new(3, 2),
            EventTime::new(3, 2)
        ]
    );

    Ok(())
}

// test a layer
#[test]
fn test_single_layer() -> Result<(), Box<dyn Error>> {
    let graph = Graph::new();
    let dumbledore_node = graph
        .add_node(
            1,
            "Dumbledore",
            [("type", Prop::str("Character"))],
            None,
            None,
        )
        .unwrap();
    let dumbledore_node_id = dumbledore_node.id();

    let harry_node = graph
        .add_node(2, "Harry", [("type", Prop::str("Character"))], None, None)
        .unwrap();
    let harry_node_id = harry_node.id();

    let character_edge = graph
        .add_edge(
            3,
            "Dumbledore",
            "Harry",
            [("meeting", Prop::str("Character Co-occurrence"))],
            None,
        )
        .unwrap();

    let broom_node = graph
        .add_node(
            4,
            "Broom",
            [("type", Prop::str("Magical Object"))],
            None,
            None,
        )
        .unwrap();
    let broom_node_id = broom_node.id();

    let broom_harry_magical_edge = graph
        .add_edge(
            4,
            "Broom",
            "Harry",
            [("use", Prop::str("Flying on broom"))],
            Some("Magical Object Uses"),
        )
        .unwrap();

    let _broom_dumbledore_magical_edge = graph
        .add_edge(
            4,
            "Broom",
            "Dumbledore",
            [("use", Prop::str("Flying on broom"))],
            Some("Magical Object Uses"),
        )
        .unwrap();

    let broom_harry_normal_edge = graph
        .add_edge(
            5,
            "Broom",
            "Harry",
            [("use", Prop::str("Cleaning with broom"))],
            None,
        )
        .unwrap();

    let _broom_dumbledore_normal_edge = graph
        .add_edge(
            5,
            "Broom",
            "Dumbledore",
            [("use", Prop::str("Cleaning with broom"))],
            None,
        )
        .unwrap();

    let dumbledore_history = History::new(dumbledore_node);
    assert_eq!(
        dumbledore_history.iter().collect_vec(),
        vec![
            EventTime::new(1, 0),
            EventTime::new(3, 2),
            EventTime::new(4, 5),
            EventTime::new(5, 7)
        ]
    );

    let harry_history = History::new(harry_node);
    assert_eq!(
        harry_history.iter().collect_vec(),
        vec![
            EventTime::new(2, 1),
            EventTime::new(3, 2),
            EventTime::new(4, 4),
            EventTime::new(5, 6)
        ]
    );

    let broom_history = History::new(broom_node);
    assert_eq!(
        broom_history.iter().collect_vec(),
        vec![
            EventTime::new(4, 3),
            EventTime::new(4, 4),
            EventTime::new(4, 5),
            EventTime::new(5, 6),
            EventTime::new(5, 7)
        ]
    );

    let character_edge_history = History::new(character_edge);
    assert_eq!(character_edge_history.collect(), [EventTime::new(3, 2)]);

    // normal history differs from "Magical Object Uses" history
    let broom_harry_normal_history = History::new(broom_harry_normal_edge);
    assert_eq!(broom_harry_normal_history.collect(), [EventTime::new(5, 6)]);

    let broom_harry_magical_history = History::new(broom_harry_magical_edge);
    assert_eq!(
        broom_harry_magical_history.collect(),
        [EventTime::new(4, 4)]
    );

    // make graphview using layer
    let magical_graph_view = graph.layers("Magical Object Uses").unwrap();
    let dumbledore_node_magical_view = magical_graph_view.node(dumbledore_node_id.clone()).unwrap();
    let harry_node_magical_view = magical_graph_view.node(harry_node_id.clone()).unwrap();
    let broom_node_magical_view = magical_graph_view.node(broom_node_id.clone()).unwrap();

    // history of nodes are different when only applied to the "Magical Object Uses" layer
    let dumbledore_magical_history = History::new(dumbledore_node_magical_view);
    assert_eq!(
        dumbledore_magical_history.collect(),
        [EventTime::new(1, 0), EventTime::new(4, 5)]
    );

    let harry_magical_history = History::new(harry_node_magical_view);
    assert_eq!(
        harry_magical_history.collect(),
        [EventTime::new(2, 1), EventTime::new(4, 4)]
    );

    let broom_magical_history = History::new(broom_node_magical_view);
    assert_eq!(
        broom_magical_history.collect(),
        [
            EventTime::new(4, 3),
            EventTime::new(4, 4),
            EventTime::new(4, 5)
        ]
    );

    // edge retrieved from layered graph view is from the layer
    let broom_dumbledore_magical_edge_retrieved = magical_graph_view
        .edge(broom_node_id, dumbledore_node_id)
        .unwrap();

    let broom_dumbledore_magical_history =
        History::new(broom_dumbledore_magical_edge_retrieved.clone());
    assert_eq!(
        broom_dumbledore_magical_history.collect(),
        [EventTime::new(4, 5)]
    );

    Ok(())
}

#[test]
fn test_lazy_node_state() -> Result<(), Box<dyn Error>> {
    let graph = Graph::new();
    let _dumbledore_node = graph
        .add_node(
            1,
            "Dumbledore",
            [("type", Prop::str("Character"))],
            None,
            None,
        )
        .unwrap();

    let _harry_node = graph
        .add_node(2, "Harry", [("type", Prop::str("Character"))], None, None)
        .unwrap();

    let _character_edge = graph
        .add_edge(
            3,
            "Dumbledore",
            "Harry",
            [("meeting", Prop::str("Character Co-occurrence"))],
            None,
        )
        .unwrap();

    let _broom_node = graph
        .add_node(
            4,
            "Broom",
            [("type", Prop::str("Magical Object"))],
            None,
            None,
        )
        .unwrap();

    let _broom_harry_magical_edge = graph
        .add_edge(
            4,
            "Broom",
            "Harry",
            [("use", Prop::str("Flying on broom"))],
            Some("Magical Object Uses"),
        )
        .unwrap();

    let _broom_dumbledore_magical_edge = graph
        .add_edge(
            4,
            "Broom",
            "Dumbledore",
            [("use", Prop::str("Flying on broom"))],
            Some("Magical Object Uses"),
        )
        .unwrap();

    let _broom_harry_normal_edge = graph
        .add_edge(
            5,
            "Broom",
            "Harry",
            [("use", Prop::str("Cleaning with broom"))],
            None,
        )
        .unwrap();

    let _broom_dumbledore_normal_edge = graph
        .add_edge(
            5,
            "Broom",
            "Dumbledore",
            [("use", Prop::str("Cleaning with broom"))],
            None,
        )
        .unwrap();

    // Test basic LazyNodeState history operations
    let all_nodes_history = graph.nodes().history();
    let nodes_history_as_history = History::new(&all_nodes_history);

    // history object orders them automatically bc of kmerge
    let expected_history_all_ordered = [
        EventTime::new(1, 0),
        EventTime::new(2, 1),
        EventTime::new(3, 2),
        EventTime::new(3, 2),
        EventTime::new(4, 3),
        EventTime::new(4, 4),
        EventTime::new(4, 4),
        EventTime::new(4, 5),
        EventTime::new(4, 5),
        EventTime::new(5, 6),
        EventTime::new(5, 6),
        EventTime::new(5, 7),
        EventTime::new(5, 7),
    ];

    // lazy_node_state returns an iterator of history objects, not ordered
    let expected_history_all_unordered = [
        EventTime::new(1, 0),
        EventTime::new(3, 2),
        EventTime::new(4, 5),
        EventTime::new(5, 7),
        EventTime::new(2, 1),
        EventTime::new(3, 2),
        EventTime::new(4, 4),
        EventTime::new(5, 6),
        EventTime::new(4, 3),
        EventTime::new(4, 4),
        EventTime::new(4, 5),
        EventTime::new(5, 6),
        EventTime::new(5, 7),
    ];

    // Test that the merged history contains all timestamps from all nodes
    // Each operation adds a timestamp, so we should have timestamps from node additions and edge additions
    assert!(!nodes_history_as_history.is_empty());
    assert_eq!(
        nodes_history_as_history.earliest_time().unwrap(),
        EventTime::new(1, 0)
    );

    assert_eq!(nodes_history_as_history, expected_history_all_ordered);
    assert_eq!(
        nodes_history_as_history.latest_time().unwrap(),
        EventTime::new(5, 7)
    );

    // Test collect_time_entries method on LazyNodeState<HistoryOp>
    let full_collected = all_nodes_history.collect_time_entries();
    assert!(!full_collected.is_empty());
    assert_eq!(full_collected, expected_history_all_ordered);

    // Test individual node history access via flatten()
    let individual_histories = all_nodes_history.flatten().collect();
    assert_eq!(individual_histories, nodes_history_as_history.collect());

    // Test timestamp conversion
    let timestamps: Vec<_> = all_nodes_history
        .t()
        .iter_values()
        .flat_map(|ts| ts.collect())
        .collect();
    assert!(!timestamps.is_empty());
    assert_eq!(timestamps, expected_history_all_unordered.map(|t| t.t()));

    // Test intervals
    let intervals: Vec<_> = all_nodes_history.intervals().collect();
    assert_eq!(intervals.len(), 3); // One per node
    assert_eq!(
        intervals.iter().map(|i| i.collect()).collect::<Vec<_>>(),
        vec!(vec![2, 1, 1], vec![1, 1, 1], vec![0, 0, 1, 0])
    );

    // Test windowed operations
    let windowed_graph = graph.window(2, 4);
    let windowed_nodes_history = windowed_graph.nodes().history();
    let windowed_history_as_history = History::new(&windowed_nodes_history);

    // Window should filter the timestamps
    let windowed_collected = windowed_nodes_history.collect_time_entries();

    // Windowed should have fewer or equal timestamps
    assert!(windowed_collected.len() <= full_collected.len());
    assert_eq!(
        windowed_collected,
        [
            EventTime::new(2, 1),
            EventTime::new(3, 2),
            EventTime::new(3, 2)
        ]
    ); // unordered
    assert_eq!(
        windowed_history_as_history,
        [
            EventTime::new(2, 1),
            EventTime::new(3, 2),
            EventTime::new(3, 2)
        ]
    ); // ordered

    // Test layer-specific operations
    let magical_layer_graph = graph.layers("Magical Object Uses").unwrap();
    let magical_nodes_history = magical_layer_graph.nodes().history();
    let magical_history_as_history = History::new(&magical_nodes_history);

    // Should have different history than the full graph
    let magical_collected = magical_nodes_history.collect_time_entries();
    assert_eq!(
        magical_collected,
        [
            EventTime::new(1, 0),
            EventTime::new(2, 1),
            EventTime::new(4, 3),
            EventTime::new(4, 4),
            EventTime::new(4, 4),
            EventTime::new(4, 5),
            EventTime::new(4, 5),
        ]
    ); // unordered
    assert_eq!(
        magical_history_as_history,
        [
            EventTime::new(1, 0),
            EventTime::new(2, 1),
            EventTime::new(4, 3),
            EventTime::new(4, 4),
            EventTime::new(4, 4),
            EventTime::new(4, 5),
            EventTime::new(4, 5),
        ]
    ); // ordered

    // Test earliest and latest time operations on LazyNodeState
    let earliest_times = all_nodes_history.earliest_time();
    let latest_times = all_nodes_history.latest_time();

    // These return LazyNodeState with different operations
    assert_eq!(
        earliest_times
            .iter_values()
            .map(|t| t.unwrap())
            .collect_vec(),
        [
            EventTime::new(1, 0),
            EventTime::new(2, 1),
            EventTime::new(4, 3)
        ]
    );

    assert_eq!(
        latest_times.iter_values().map(|t| t.unwrap()).collect_vec(),
        [
            EventTime::new(5, 7),
            EventTime::new(5, 6),
            EventTime::new(5, 7)
        ]
    );

    // Test that History trait methods work on LazyNodeState
    let history_rev = nodes_history_as_history.iter_rev().collect::<Vec<_>>();

    // Reverse should be the reverse of forward iteration
    assert_eq!(
        nodes_history_as_history,
        history_rev.into_iter().rev().collect::<Vec<_>>()
    );

    // Test event id access
    let event_ids_lazy: Vec<_> = all_nodes_history
        .event_id()
        .iter_values()
        .flat_map(|s| s.collect())
        .collect();
    let event_ids_normal: Vec<_> = nodes_history_as_history.event_id().collect();
    assert_eq!(event_ids_lazy, [0, 2, 5, 7, 1, 2, 4, 6, 3, 4, 5, 6, 7]); // unordered
    assert_eq!(event_ids_normal, [0, 1, 2, 2, 3, 4, 4, 5, 5, 6, 6, 7, 7]); // ordered

    // Test combined window and layer filtering
    let windowed_layered_graph = graph.window(3, 6).layers("Magical Object Uses").unwrap();
    let windowed_layered_history = windowed_layered_graph.nodes().history();
    let windowed_layered_history_as_history = History::new(&windowed_layered_history);
    let windowed_layered_collected = windowed_layered_history.collect_time_entries();

    // Should be even more filtered
    assert_eq!(
        windowed_layered_collected,
        [
            EventTime::new(4, 3),
            EventTime::new(4, 4),
            EventTime::new(4, 4),
            EventTime::new(4, 5),
            EventTime::new(4, 5)
        ]
    ); // unordered
    assert_eq!(
        windowed_layered_history_as_history,
        [
            EventTime::new(4, 3),
            EventTime::new(4, 4),
            EventTime::new(4, 4),
            EventTime::new(4, 5),
            EventTime::new(4, 5)
        ]
    ); // ordered

    // Test iter and iter_rev on LazyNodeState directly (through InternalHistoryOps)
    let direct_iter: Vec<EventTime> = InternalHistoryOps::iter(&all_nodes_history).collect();
    let direct_iter_rev: Vec<_> = all_nodes_history.iter_rev().collect();
    assert_eq!(
        direct_iter,
        direct_iter_rev.into_iter().rev().collect::<Vec<_>>()
    );

    Ok(())
}
