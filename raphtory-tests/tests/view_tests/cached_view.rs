use itertools::Itertools;
use proptest::prelude::*;
use raphtory::{
    algorithms::motifs::triangle_count::triangle_count,
    db::{
        api::view::Filter,
        graph::{
            assertions::assert_graph_equal,
            views::filter::model::{ExplodedEdgeFilter, PropertyFilterFactory},
        },
    },
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

fn fixture() -> Graph {
    let graph = Graph::new();
    // in the static layer, so every view shows it
    graph
        .add_node(0, "unlayered", NO_PROPS, None, None)
        .unwrap();
    graph
        .add_edge(0, "a", "b", NO_PROPS, Some("layer_a"))
        .unwrap();
    graph
        .add_edge(0, "c", "d", NO_PROPS, Some("layer_b"))
        .unwrap();
    // in the default layer, an ordinary layer for edges
    graph.add_edge(0, "e", "f", NO_PROPS, None).unwrap();
    graph
}

fn names<'a, G: GraphViewOps<'a>>(graph: &G) -> Vec<String> {
    let mut names: Vec<String> = graph.nodes().name().into_iter().map(|(_, n)| n).collect();
    names.sort();
    names
}

/// Every edge formatted as `src-dst@layer`, sorted.
fn edges<'a, G: GraphViewOps<'a>>(graph: &G) -> Vec<String> {
    let mut edges: Vec<String> = graph
        .edges()
        .iter()
        .flat_map(|edge| {
            let (src, dst) = (edge.src().name(), edge.dst().name());
            edge.layer_names()
                .into_iter()
                .map(|layer| format!("{src}-{dst}@{layer}"))
                .collect::<Vec<_>>()
        })
        .collect();
    edges.sort();
    edges
}

fn assert_caching_doesnt_change_graph<'a, G: GraphViewOps<'a> + Clone>(view: &G, what: &str) {
    let cached = view.cache_view();
    assert_eq!(names(&cached), names(view), "nodes disagree for {what}");
    assert_eq!(edges(&cached), edges(view), "edges disagree for {what}");
    assert_eq!(
        cached.count_edges(),
        view.count_edges(),
        "edge count disagrees for {what}"
    );
}

#[test]
fn caching_a_no_layer_view_keeps_exactly_its_unlayered_nodes() {
    let graph = fixture();
    let no_layers = graph
        .exclude_layers(["layer_a", "layer_b", "_default"])
        .unwrap();

    let direct = names(&no_layers);
    assert_eq!(
        direct,
        vec!["unlayered".to_string()],
        "the engine's own answer"
    );
    assert_eq!(names(&no_layers.cache_view()), direct);
    assert_eq!(
        no_layers.cache_view().edges().len(),
        no_layers.edges().len()
    );
}

#[test]
fn excluding_every_layer_after_caching_also_keeps_only_unlayered_nodes() {
    let graph = fixture();
    let cached = graph.cache_view();

    let excluded = ["layer_a", "layer_b", "_default"];
    let direct = names(&graph.exclude_layers(excluded).unwrap());
    let after = names(&cached.exclude_layers(excluded).unwrap());
    assert_eq!(
        after, direct,
        "caching must not change what excluding layers shows"
    );
}

#[test]
fn caching_agrees_on_all_layers_and_on_one() {
    let graph = fixture();
    assert_eq!(names(&graph.cache_view()), names(&graph));

    let one = graph.layers("layer_a").unwrap();
    assert_eq!(names(&one.cache_view()), names(&one));
    assert_eq!(one.cache_view().edges().len(), one.edges().len());
}

/// Unlike nodes, edges have no layer visible in every view, so selecting no layers means no edges.
#[test]
fn caching_agrees_on_edges_for_every_layer_selection() {
    let graph = fixture();

    assert_caching_doesnt_change_graph(&graph, "the whole graph");
    assert_caching_doesnt_change_graph(&graph.layers("layer_a").unwrap(), "one named layer");
    assert_caching_doesnt_change_graph(&graph.layers("_default").unwrap(), "the default layer");
    assert_caching_doesnt_change_graph(
        &graph.layers(vec!["layer_a", "layer_b"]).unwrap(),
        "several named layers",
    );
    assert_caching_doesnt_change_graph(
        &graph.exclude_layers("layer_a").unwrap(),
        "one layer excluded",
    );

    let none = graph
        .exclude_layers(vec!["layer_a", "layer_b", "_default"])
        .unwrap();
    assert!(edges(&none).is_empty(), "no layer selected means no edges");
    assert_caching_doesnt_change_graph(&none, "every layer excluded");
}

#[test]
fn the_default_edge_layer_is_not_exempt_the_way_the_static_node_layer_is() {
    let graph = fixture();
    let without_default = graph.exclude_layers("_default").unwrap();

    assert!(
        !edges(&without_default)
            .iter()
            .any(|e| e.contains("@_default")),
        "excluding the default layer must hide its edges, got {:?}",
        edges(&without_default)
    );
    assert!(names(&without_default).contains(&"unlayered".to_string()));
    assert_caching_doesnt_change_graph(&without_default, "the default layer excluded");
}

/// Properties for filters to cut on, along with edges in two layers.
fn filtered_fixture() -> Graph {
    let graph = Graph::new();
    for (name, x) in [
        ("a", 1i64),
        ("b", 2),
        ("c", 3),
        ("d", 1),
        ("e", 2),
        ("f", 5),
    ] {
        graph.add_node(0, name, [("x", x)], None, None).unwrap();
    }
    graph
        .add_node(0, "unlayered", [("x", 1i64)], None, None)
        .unwrap();
    graph
        .add_edge(0, "a", "b", [("w", 1i64)], Some("layer_a"))
        .unwrap();
    graph
        .add_edge(1, "a", "b", [("w", 5i64)], Some("layer_b"))
        .unwrap();
    graph
        .add_edge(0, "b", "d", [("w", 2i64)], Some("layer_b"))
        .unwrap();
    graph
        .add_edge(5, "b", "d", [("w", 7i64)], Some("layer_b"))
        .unwrap();
    graph
        .add_edge(2, "c", "f", [("w", 4i64)], Some("layer_a"))
        .unwrap();
    graph.add_edge(10, "e", "a", [("w", 3i64)], None).unwrap();
    graph
}

/// Caching must change nothing in any layer, down to the exploded edges.
fn assert_caching_changes_nothing_in_any_layer<'a, G: GraphViewOps<'a> + Clone>(
    view: &G,
    what: &str,
) {
    assert_caching_doesnt_change_graph(view, what);
    let cached = view.cache_view();
    assert_eq!(
        cached.count_temporal_edges(),
        view.count_temporal_edges(),
        "exploded edges disagree for {what}"
    );
    for layer in ["layer_a", "layer_b", "_default"] {
        let direct = view.layers(layer).unwrap();
        let after = cached.layers(layer).unwrap();
        assert_eq!(names(&after), names(&direct), "nodes in {layer} for {what}");
        assert_eq!(edges(&after), edges(&direct), "edges in {layer} for {what}");
        assert_eq!(
            after.count_temporal_edges(),
            direct.count_temporal_edges(),
            "exploded edges in {layer} for {what}"
        );
        assert_caching_doesnt_change_graph(&direct, &format!("{what}, only {layer}"));
    }
    assert_caching_doesnt_change_graph(
        &view.exclude_layers("layer_a").unwrap(),
        &format!("{what}, without layer_a"),
    );
}

#[test]
fn caching_agrees_with_node_window_and_exploded_filters() {
    let graph = filtered_fixture();
    let nodes = graph.filter(NodeFilter.property("x").lt(3i64)).unwrap();
    assert!(
        names(&nodes).len() < names(&graph).len(),
        "the node filter must hide something"
    );
    assert_caching_changes_nothing_in_any_layer(&nodes, "a node filter");
    assert_caching_changes_nothing_in_any_layer(&graph.window(0, 6), "a window");
    let exploded = graph
        .filter(ExplodedEdgeFilter.property("w").gt(2i64))
        .unwrap();
    assert!(
        exploded.count_temporal_edges() < graph.count_temporal_edges(),
        "the exploded filter must hide something"
    );
    assert_caching_changes_nothing_in_any_layer(&exploded, "an exploded edge filter");
    assert_caching_changes_nothing_in_any_layer(
        &nodes.window(0, 6),
        "a node filter inside a window",
    );
}
