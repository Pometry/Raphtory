use chrono::Local;
use itertools::Itertools;
use proptest::{arbitrary::any, proptest};
#[cfg(feature = "io")]
use raphtory::db::api::storage::storage::PersistenceStrategy;
#[cfg(feature = "io")]
use raphtory::db::api::view::materialize_using_recordbatches;
use raphtory::{
    db::graph::graph::{assert_graph_equal, assert_graph_equal_timestamps},
    prelude::*,
    test_storage,
    test_utils::{build_edge_list, build_graph_from_edge_list},
};
use raphtory_api::core::storage::arc_str::OptionAsStr;
use raphtory_storage::core_ops::CoreGraphOps;
use std::{
    fs, io,
    ops::Range,
    path::{Path, PathBuf},
    time::Instant,
};

#[cfg(feature = "io")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct GraphSummary {
    nodes: usize,
    edges: usize,
    temporal_edges: usize,
    earliest_time: Option<raphtory_api::core::storage::timeindex::EventTime>,
    latest_time: Option<raphtory_api::core::storage::timeindex::EventTime>,
    layers: Vec<String>,
}

#[cfg(feature = "io")]
fn summarize_graph<'graph, G: GraphViewOps<'graph>>(graph: &'graph G) -> GraphSummary {
    GraphSummary {
        nodes: graph.count_nodes(),
        edges: graph.count_edges(),
        temporal_edges: graph.count_temporal_edges(),
        earliest_time: graph.earliest_time(),
        latest_time: graph.latest_time(),
        layers: graph
            .unique_layers()
            .map(|layer| layer.to_string())
            .sorted()
            .collect(),
    }
}

#[cfg(feature = "io")]
fn default_sf10_graph_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../ldbc/data/social_network-sf10-CsvComposite-LongDateFormatter/graph")
}

#[cfg(feature = "io")]
fn default_sf3_graph_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../ldbc/data/social_network-sf3-CsvComposite-LongDateFormatter/graph")
}

#[cfg(feature = "io")]
fn default_sf1_graph_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../ldbc/data/social_network-sf1-CsvComposite-LongDateFormatter/graph")
}

#[cfg(feature = "io")]
fn default_materialized_graphs_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../ldbc/data/materialized-graphs")
}

#[cfg(feature = "io")]
fn remove_dir_all_ignore_not_found(path: impl AsRef<Path>) -> io::Result<()> {
    match fs::remove_dir_all(path.as_ref()) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

#[test]
fn test_materialize() {
    let g = Graph::new();
    g.add_edge(0, 1, 2, [("layer1", "1")], Some("1")).unwrap();
    g.add_edge(0, 1, 2, [("layer2", "2")], Some("2")).unwrap();

    let gm = g.materialize().unwrap();

    assert_graph_equal(&g, &gm);
    assert_eq!(
        gm.nodes().name().iter_values().collect::<Vec<String>>(),
        vec!["1", "2"]
    );

    assert!(g
        .layers("2")
        .unwrap()
        .edge(1, 2)
        .unwrap()
        .properties()
        .temporal()
        .get("layer1")
        .and_then(|prop| prop.latest())
        .is_none());

    assert!(gm
        .into_events()
        .unwrap()
        .layers("2")
        .unwrap()
        .edge(1, 2)
        .unwrap()
        .properties()
        .temporal()
        .get("layer1")
        .and_then(|prop| prop.latest())
        .is_none());
}

#[test]
fn test_graph_properties() {
    let g = Graph::new();
    g.add_properties(1, [("test", "test")]).unwrap();
    g.add_metadata([("test_metadata", "test2")]).unwrap();

    test_storage!(&g, |g| {
        let gm = g.materialize().unwrap();
        assert_graph_equal(&g, &gm);
    });
}

#[test]
fn materialize_prop_test() {
    proptest!(|(edges in build_edge_list(100, 100), w in any::<Range<i64>>())| {
        let g = build_graph_from_edge_list(&edges);
        test_storage!(&g, |g| {
            let gm = g.materialize().unwrap();
            assert_graph_equal_timestamps(&g, &gm);
            let gw = g.window(w.start, w.end);
            let gmw = gw.materialize().unwrap();
            assert_graph_equal_timestamps(&gw, &gmw);
        });
    })
}

#[cfg(feature = "io")]
#[test]
fn test_materialize_using_recordbatches_matches_materialize() {
    let g = Graph::new();
    g.add_node(0, "A", [("node_meta", "alpha")], Some("TypeA"))
        .unwrap();
    g.add_node(1, "B", [("node_meta", "beta")], None).unwrap();
    g.add_edge(2, "A", "B", [("weight", 1)], Some("layer1"))
        .unwrap();
    g.add_edge(3, "A", "B", [("weight", 2)], Some("layer2"))
        .unwrap();
    g.delete_edge(4, "A", "B", Some("layer1")).unwrap();
    g.add_properties(5, [("graph_prop", "present")]).unwrap();
    g.add_metadata([("graph_meta", "constant")]).unwrap();

    let expected = g.materialize().unwrap();
    let actual =
        materialize_using_recordbatches(&g, None, g.core_graph().extension().config().clone())
            .unwrap();

    assert_graph_equal_timestamps(&expected, &actual);
}

#[cfg(feature = "io")]
#[test]
#[ignore = "requires a locally persisted SNB SF10 graph produced by ldbc/load_snb_sf10.py"]
fn test_materialize_snb_sf10_timings() {
    let graph_path = default_sf1_graph_path();
    let old_materialize_graph_path = default_materialized_graphs_path().join("old_materialize");
    let rb_materialize_graph_path = default_materialized_graphs_path().join("rb_materialize");
    // clear out the directories in case they had previous files in them
    remove_dir_all_ignore_not_found(&old_materialize_graph_path).unwrap();
    remove_dir_all_ignore_not_found(&rb_materialize_graph_path).unwrap();
    fs::create_dir_all(&old_materialize_graph_path).unwrap();
    fs::create_dir_all(&rb_materialize_graph_path).unwrap();

    if !graph_path.exists() {
        eprintln!("SNB graph not found at {}", graph_path.display());
        return;
    }

    println!("Loading SNB graph from {}...", graph_path.display());
    let load_start = Instant::now();
    let g = Graph::load(&graph_path).unwrap();
    let load_elapsed = load_start.elapsed();
    let source_summary = summarize_graph(&g);
    println!(
        "Loaded source graph in {:?}: {} nodes, {} edges, {} temporal edges",
        load_elapsed, source_summary.nodes, source_summary.edges, source_summary.temporal_edges
    );

    println!(
        "Starting materialize using RecordBatches at {}",
        Local::now()
    );
    let recordbatch_start = Instant::now();
    let recordbatch_graph = materialize_using_recordbatches(
        &g,
        Some(&rb_materialize_graph_path),
        g.core_graph().extension().config().clone(),
    )
    .unwrap();
    let recordbatch_elapsed = recordbatch_start.elapsed();
    println!(
        "Finished materialize using RecordBatches at {}\nTook {recordbatch_elapsed:?}",
        Local::now()
    );
    let recordbatch_summary = summarize_graph(&recordbatch_graph);
    // drop(recordbatch_graph);

    println!("Starting materialize impl (old) at {}", Local::now());
    let impl_start = Instant::now();
    let materialize_impl_graph = g.materialize_at(&old_materialize_graph_path).unwrap();
    let impl_elapsed = impl_start.elapsed();
    println!(
        "Finished materialize impl (old) at {}\nTook {impl_elapsed:?}",
        Local::now()
    );
    let impl_summary = summarize_graph(&materialize_impl_graph);
    // drop(materialize_impl_graph);

    assert_eq!(impl_summary, source_summary);
    assert_eq!(recordbatch_summary, source_summary);
    let assert_equal_start = Instant::now();
    assert_graph_equal_timestamps(&recordbatch_graph, &materialize_impl_graph);
    let assert_eq_elapsed = assert_equal_start.elapsed();
    println!("Graph equals assertion took {assert_eq_elapsed:?}");

    let impl_secs = impl_elapsed.as_secs_f64();
    let recordbatch_secs = recordbatch_elapsed.as_secs_f64();
    let faster = if impl_secs < recordbatch_secs {
        "materialize_impl"
    } else if recordbatch_secs < impl_secs {
        "materialize_using_recordbatches"
    } else {
        "tie"
    };
    let ratio = if impl_secs > 0.0 && recordbatch_secs > 0.0 {
        if impl_secs > recordbatch_secs {
            impl_secs / recordbatch_secs
        } else {
            recordbatch_secs / impl_secs
        }
    } else {
        1.0
    };

    println!("materialize_impl via g.materialize(): {:?}", impl_elapsed);
    println!(
        "materialize_using_recordbatches(...): {:?}",
        recordbatch_elapsed
    );
    println!("Faster path: {faster} ({ratio:.2}x)");
}

#[test]
fn test_subgraph() {
    let g = Graph::new();
    g.add_node(0, 1, NO_PROPS, None).unwrap();
    g.add_node(0, 2, NO_PROPS, None).unwrap();
    g.add_node(0, 3, NO_PROPS, None).unwrap();
    g.add_node(0, 4, NO_PROPS, None).unwrap();
    g.add_node(0, 5, NO_PROPS, None).unwrap();

    let nodes_subgraph = g.subgraph(vec![4, 5]);
    assert_eq!(
        nodes_subgraph
            .nodes()
            .name()
            .iter_values()
            .collect::<Vec<String>>(),
        vec!["4", "5"]
    );
    let gm = nodes_subgraph.materialize().unwrap();
    assert_graph_equal(&nodes_subgraph, &gm);
}

#[test]
fn test_exclude_nodes() {
    let g = Graph::new();
    g.add_node(0, 1, NO_PROPS, None).unwrap();
    g.add_node(0, 2, NO_PROPS, None).unwrap();
    g.add_node(0, 3, NO_PROPS, None).unwrap();
    g.add_node(0, 4, NO_PROPS, None).unwrap();
    g.add_node(0, 5, NO_PROPS, None).unwrap();

    let exclude_nodes_subgraph = g.exclude_nodes(vec![4, 5]);
    assert_eq!(
        exclude_nodes_subgraph
            .nodes()
            .name()
            .iter_values()
            .sorted()
            .collect::<Vec<String>>(),
        vec!["1", "2", "3"]
    );
    let gm = exclude_nodes_subgraph.materialize().unwrap();
    assert_graph_equal(&exclude_nodes_subgraph, &gm);
}

#[test]
fn testing_node_types() {
    let graph = Graph::new();
    graph.add_node(0, "A", NO_PROPS, None).unwrap();
    graph.add_node(1, "B", NO_PROPS, Some("H")).unwrap();

    test_storage!(&graph, |graph| {
        let node_a = graph.node("A").unwrap();
        let node_b = graph.node("B").unwrap();
        let node_a_type = node_a.node_type();
        let node_a_type_str = node_a_type.as_str();

        assert_eq!(node_a_type_str, None);
        assert_eq!(node_b.node_type().as_str(), Some("H"));
    });

    // Nodes with No type can be overwritten
    let node_a = graph.add_node(1, "A", NO_PROPS, Some("TYPEA")).unwrap();
    assert_eq!(node_a.node_type().as_str(), Some("TYPEA"));

    // Check that overwriting a node type returns an error
    assert!(graph.add_node(2, "A", NO_PROPS, Some("TYPEB")).is_err());
    // Double check that the type did not actually change
    assert_eq!(graph.node("A").unwrap().node_type().as_str(), Some("TYPEA"));
    // Check that the update is not added to the graph
    let all_node_types = graph.get_all_node_types();
    assert_eq!(all_node_types.len(), 2);
}

#[test]
fn changing_property_type_errors() {
    let g = Graph::new();
    let props_0 = [("test", Prop::U64(1))];
    let props_1 = [("test", Prop::F64(0.1))];
    g.add_properties(0, props_0.clone()).unwrap();
    assert!(g.add_properties(1, props_1.clone()).is_err());

    g.add_node(0, 1, props_0.clone(), None).unwrap();
    assert!(g.add_node(1, 1, props_1.clone(), None).is_err());

    g.add_edge(0, 1, 2, props_0.clone(), None).unwrap();
    assert!(g.add_edge(1, 1, 2, props_1.clone(), None).is_err());
}

#[test]
fn test_edge_layer_properties() {
    let g = Graph::new();
    g.add_edge(1, "A", "B", [("greeting", "howdy")], Some("layer 1"))
        .unwrap();
    g.add_edge(2, "A", "B", [("greeting", "ola")], Some("layer 2"))
        .unwrap();
    g.add_edge(2, "A", "B", [("greeting", "hello")], Some("layer 2"))
        .unwrap();
    g.add_edge(3, "A", "B", [("greeting", "namaste")], Some("layer 3"))
        .unwrap();

    let edge_ab = g.edge("A", "B").unwrap();
    let props = edge_ab
        .properties()
        .iter()
        .filter_map(|(k, v)| v.map(move |v| (k.to_string(), v.to_string())))
        .collect::<Vec<_>>();
    assert_eq!(props, vec![("greeting".to_string(), "namaste".to_string())]);
}
