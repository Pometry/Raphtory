use itertools::Itertools;
use raphtory::{
    core::utils::time::TryIntoTime,
    db::{
        api::{mutation::AdditionOps, view::WindowSet},
        graph::{
            graph::{assert_graph_equal, Graph},
            views::deletion_graph::PersistentGraph,
        },
    },
    prelude::{DeletionOps, GraphViewOps, TimeOps, NO_PROPS},
};
use raphtory_core::utils::time::ParseTimeError;

pub mod test_utils;

// start inclusive, end exclusive
fn graph_with_timeline(start: i64, end: i64) -> Graph {
    let g = Graph::new();
    g.add_edge(start, 0, 1, NO_PROPS, None).unwrap();
    g.add_edge(end - 1, 0, 1, NO_PROPS, None).unwrap();
    g
}

fn assert_bounds<'graph, G>(
    windows: WindowSet<'graph, G>,
    expected: Vec<(Option<i64>, Option<i64>)>,
) where
    G: GraphViewOps<'graph>,
{
    let window_bounds = windows.map(|w| (w.start(), w.end())).collect_vec();
    assert_eq!(window_bounds, expected)
}

#[test]
fn snapshot() {
    let graph = PersistentGraph::new();
    graph.add_edge(3, 0, 1, [("a", "a")], None).unwrap();
    graph.add_edge(4, 0, 2, [("b", "b")], None).unwrap();
    graph.delete_edge(5, 0, 1, None).unwrap();

    for time in 2..7 {
        assert_graph_equal(&graph.at(time), &graph.snapshot_at(time));
    }
    assert_graph_equal(&graph.latest(), &graph.snapshot_latest());

    let graph = graph.event_graph();

    for time in 2..7 {
        assert_graph_equal(&graph.before(time + 1), &graph.snapshot_at(time));
    }
    assert_graph_equal(&graph, &graph.snapshot_latest());
}

#[test]
fn rolling() {
    let graph = graph_with_timeline(1, 7);
    test_storage!(&graph, |graph| {
        let windows = graph.rolling(2, None).unwrap();
        let expected = vec![(Some(1), Some(3)), (Some(3), Some(5)), (Some(5), Some(7))];
        assert_bounds(windows, expected);
    });

    let graph = graph_with_timeline(1, 6);
    test_storage!(&graph, |graph| {
        let windows = graph.rolling(3, Some(2)).unwrap();
        let expected = vec![(Some(0), Some(3)), (Some(2), Some(5)), (Some(4), Some(7))];
        assert_bounds(windows, expected.clone());
    });

    let graph = graph_with_timeline(0, 9);
    test_storage!(&graph, |graph| {
        let windows = graph.window(1, 6).rolling(3, Some(2)).unwrap();
        assert_bounds(
            windows,
            vec![(Some(1), Some(3)), (Some(2), Some(5)), (Some(4), Some(6))],
        );
    });
}

#[test]
fn expanding() {
    let graph = graph_with_timeline(1, 7);
    test_storage!(&graph, |graph| {
        let windows = graph.expanding(2).unwrap();
        let expected = vec![(None, Some(3)), (None, Some(5)), (None, Some(7))];
        assert_bounds(windows, expected);
    });

    let graph = graph_with_timeline(1, 6);
    test_storage!(&graph, |graph| {
        let windows = graph.expanding(2).unwrap();
        let expected = vec![(None, Some(3)), (None, Some(5)), (None, Some(7))];
        assert_bounds(windows, expected.clone());
    });

    let graph = graph_with_timeline(0, 9);
    test_storage!(&graph, |graph| {
        let windows = graph.window(1, 6).expanding(2).unwrap();
        assert_bounds(
            windows,
            vec![(Some(1), Some(3)), (Some(1), Some(5)), (Some(1), Some(6))],
        );
    });
}

#[test]
fn rolling_dates() {
    let start = "2020-06-06 00:00:00".try_into_time().unwrap();
    let end = "2020-06-07 23:59:59.999".try_into_time().unwrap();
    let graph = graph_with_timeline(start, end);
    test_storage!(&graph, |graph| {
        let windows = graph.rolling("1 day", None).unwrap();
        let expected = vec![
            (
                "2020-06-06 00:00:00".try_into_time().ok(), // entire 2020-06-06
                "2020-06-07 00:00:00".try_into_time().ok(),
            ),
            (
                "2020-06-07 00:00:00".try_into_time().ok(), // entire 2020-06-06
                "2020-06-08 00:00:00".try_into_time().ok(),
            ),
        ];
        assert_bounds(windows, expected);
    });

    let start = "2020-06-06 00:00:00".try_into_time().unwrap();
    let end = "2020-06-08 00:00:00".try_into_time().unwrap();
    let graph = graph_with_timeline(start, end);
    test_storage!(&graph, |graph| {
        let windows = graph.rolling("1 day", None).unwrap();
        let expected = vec![
            (
                "2020-06-06 00:00:00".try_into_time().ok(), // entire 2020-06-06
                "2020-06-07 00:00:00".try_into_time().ok(),
            ),
            (
                "2020-06-07 00:00:00".try_into_time().ok(), // entire 2020-06-07
                "2020-06-08 00:00:00".try_into_time().ok(),
            ),
        ];
        assert_bounds(windows, expected);
    });

    // TODO: turn this back on if we bring bach epoch alignment for unwindowed graphs
    // let start = "2020-06-05 23:59:59.999".into_time().unwrap();
    // let end = "2020-06-07 00:00:00.000".into_time().unwrap();
    // let g = graph_with_timeline(start, end);
    // let windows = g.rolling("1 day", None).unwrap();
    // let expected = vec![
    //     (
    //         "2020-06-05 00:00:00".into_time().unwrap(), // entire 2020-06-06
    //         "2020-06-06 00:00:00".into_time().unwrap(),
    //     ),
    //     (
    //         "2020-06-06 00:00:00".into_time().unwrap(), // entire 2020-06-07
    //         "2020-06-07 00:00:00".into_time().unwrap(),
    //     ),
    // ];
    // assert_bounds(windows, expected);
}

#[test]
fn test_errors() {
    let start = "2020-06-06 00:00:00".try_into_time().unwrap();
    let end = "2020-06-07 23:59:59.999".try_into_time().unwrap();
    let graph = graph_with_timeline(start, end);
    match graph.rolling("1 day", Some("0 days")) {
        Ok(_) => {
            panic!("Expected error, but got Ok")
        }
        Err(e) => {
            assert_eq!(e, ParseTimeError::ZeroSizeStep)
        }
    }
    match graph.rolling(1, Some(0)) {
        Ok(_) => {
            panic!("Expected error, but got Ok")
        }
        Err(e) => {
            assert_eq!(e, ParseTimeError::ZeroSizeStep)
        }
    }
    match graph.expanding("0 day") {
        Ok(_) => {
            panic!("Expected error, but got Ok")
        }
        Err(e) => {
            assert_eq!(e, ParseTimeError::ZeroSizeStep)
        }
    }
    match graph.expanding(0) {
        Ok(_) => {
            panic!("Expected error, but got Ok")
        }
        Err(e) => {
            assert_eq!(e, ParseTimeError::ZeroSizeStep)
        }
    }

    match graph.expanding("0fead day") {
        Ok(_) => {
            panic!("Expected error, but got Ok")
        }
        Err(e) => {
            assert!(matches!(e, ParseTimeError::ParseInt { .. }))
        }
    }

    match graph.expanding("0 dadasasdy") {
        Ok(_) => {
            panic!("Expected error, but got Ok")
        }
        Err(e) => {
            assert!(matches!(e, ParseTimeError::InvalidUnit { .. }))
        }
    }

    assert_eq!(
        graph.rolling("1 day", Some("1000 days")).unwrap().count(),
        0
    )
}

#[test]
fn expanding_dates() {
    let start = "2020-06-06 00:00:00".try_into_time().unwrap();
    let end = "2020-06-07 23:59:59.999".try_into_time().unwrap();
    let graph = graph_with_timeline(start, end);
    test_storage!(&graph, |graph| {
        let windows = graph.expanding("1 day").unwrap();
        let expected = vec![
            (None, "2020-06-07 00:00:00".try_into_time().ok()),
            (None, "2020-06-08 00:00:00".try_into_time().ok()),
        ];
        assert_bounds(windows, expected);
    });

    let start = "2020-06-06 00:00:00".try_into_time().unwrap();
    let end = "2020-06-08 00:00:00".try_into_time().unwrap();
    let graph = graph_with_timeline(start, end);
    test_storage!(&graph, |graph| {
        let windows = graph.expanding("1 day").unwrap();
        let expected = vec![
            (None, "2020-06-07 00:00:00".try_into_time().ok()),
            (None, "2020-06-08 00:00:00".try_into_time().ok()),
        ];
        assert_bounds(windows, expected);
    });
}
