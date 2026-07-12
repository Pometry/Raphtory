use itertools::Itertools;
use proptest::prelude::*;
use raphtory::{
    algorithms::motifs::triangle_count::triangle_count, db::graph::graph::assert_graph_equal,
    prelude::*,
};
use raphtory_api::core::storage::timeindex::AsTime;
use raphtory_tests::test_storage;

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

    graph.add_node(1, 1, NO_PROPS, None, None).unwrap();
    graph.add_node(2, 2, NO_PROPS, None, None).unwrap();

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
fn masked_always_equals_proptest() {
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

            let earliest = graph.earliest_time().unwrap().t();
            let latest = graph.latest_time().unwrap().t();
            let middle = earliest + (latest - earliest) / 2;

            if !layers.is_empty() && earliest < middle && middle < latest {
                let subgraph = graph.layers(layers).unwrap().window(earliest, middle);
                let masked = subgraph.cache_view();
                assert_graph_equal(&subgraph, &masked);
            }
        });
    }

    proptest!(|(edge_list in any::<Vec<(u8, u8, i16, u8)>>().prop_filter("greater than 3",|v| !v.is_empty() ))| {
        check(&edge_list);
    })
}
