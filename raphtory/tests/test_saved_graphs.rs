#![cfg(feature = "io")]

use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use raphtory::{
    arrow_loader::df_loaders::edges::ColumnNames,
    db::graph::graph::{assert_graph_equal, assert_graph_equal_timestamps},
    io::parquet_loaders::{
        get_parquet_file_paths, load_edge_metadata_from_parquet, load_edges_from_parquet,
        load_graph_props_from_parquet, load_node_metadata_from_parquet, load_nodes_from_parquet,
    },
    prelude::*,
    serialise::{StableDecode, StableEncode},
};
use std::{
    fs, io,
    path::{Path, PathBuf},
};

// These mirror the (currently `pub(crate)`) column-name constants in
// `raphtory::parquet_encoder`. They MUST stay in sync with the encoder.
const NODE_GID_COL: &str = "rap_node_gid";
const NODE_VID_COL: &str = "rap_node_vid";
const TYPE_COL: &str = "rap_node_type";
const TYPE_ID_COL: &str = "rap_node_type_id";
const TIME_COL: &str = "rap_time";
const SECONDARY_INDEX_COL: &str = "rap_secondary_index";
const SRC_VID_COL: &str = "rap_src_vid";
const SRC_GID_COL: &str = "rap_src_gid";
const DST_VID_COL: &str = "rap_dst_vid";
const DST_GID_COL: &str = "rap_dst_gid";
const LAYER_COL: &str = "rap_layer";
const LAYER_ID_COL: &str = "rap_layer_id";
const EDGE_ID_COL: &str = "rap_edge_id";

const GRAPH_C_DIR: &str = "graph_c";
const GRAPH_T_DIR: &str = "graph_t";
const NODES_C_DIR: &str = "nodes_c";
const NODES_T_DIR: &str = "nodes_t";
const EDGES_C_DIR: &str = "edges_c";
const EDGES_T_DIR: &str = "edges_t";

fn bench_data_dir() -> PathBuf {
    // raphtory/Cargo.toml -> raphtory dir -> Raphtory root -> graphql-bench/data/apache
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../graphql-bench/data/apache")
}

fn master_parquet_files_dir() -> PathBuf {
    bench_data_dir().join("master/data0/graph0")
}

fn master_dir() -> PathBuf {
    bench_data_dir().join("master")
}

fn master_new_dir() -> PathBuf {
    bench_data_dir().join("master_new")
}

fn event_graph_disk_storage_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("resources/test/disk_graphs/event_graph")
}

fn persistent_graph_disk_storage_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("resources/test/disk_graphs/persistent_graph")
}

fn parquet_prop_columns(path: &Path, exclude: &[&str]) -> Vec<String> {
    get_parquet_file_paths(path)
        .unwrap()
        .into_iter()
        .next()
        .map(|file| {
            ArrowReaderMetadata::load(&fs::File::open(file).unwrap(), Default::default())
                .unwrap()
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .filter(|name| !exclude.iter().any(|x| x == name))
                .collect()
        })
        .unwrap_or_default()
}

fn remove_dir_all_ignore_not_found(path: impl AsRef<Path>) -> io::Result<()> {
    match fs::remove_dir_all(path.as_ref()) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

/// Load the graphql-bench master graph from its raw parquet directories by calling each
/// of the per-section loaders directly (load_*_from_parquet functions).
/// This builds an in-memory Graph from scratch so VIDs get reassigned contiguously and segments grow naturally.
fn load_graphql_master_from_parquet_loaders(parquet_dir: &Path) -> Graph {
    let graph = Graph::new();

    // ---- graph_c ----
    let c_graph_path = parquet_dir.join(GRAPH_C_DIR);
    if c_graph_path.exists() {
        let metadata_cols = parquet_prop_columns(&c_graph_path, &[TIME_COL]);
        let metadata_cols: Vec<&str> = metadata_cols.iter().map(String::as_str).collect();
        load_graph_props_from_parquet(
            &graph,
            &c_graph_path,
            TIME_COL,
            None,
            &[],
            &metadata_cols,
            None,
            None,
        )
        .unwrap();
    }

    // ---- graph_t ----
    let t_graph_path = parquet_dir.join(GRAPH_T_DIR);
    if t_graph_path.exists() {
        let prop_cols = parquet_prop_columns(&t_graph_path, &[TIME_COL, SECONDARY_INDEX_COL]);
        let prop_cols: Vec<&str> = prop_cols.iter().map(String::as_str).collect();
        if !prop_cols.is_empty() {
            load_graph_props_from_parquet(
                &graph,
                &t_graph_path,
                TIME_COL,
                Some(SECONDARY_INDEX_COL),
                &prop_cols,
                &[],
                None,
                None,
            )
            .unwrap();
        }
    }

    // ---- nodes_c ----
    // Resolve by GID (not by the parquet's original VID column) so the new
    // graph gets dense, contiguous VIDs. Passing `node_id_col`/`node_type_id_col`
    // = None forces GID-based resolution inside the loader.
    let c_node_path = parquet_dir.join(NODES_C_DIR);
    if c_node_path.exists() {
        let metadata_cols = parquet_prop_columns(
            &c_node_path,
            &[NODE_GID_COL, NODE_VID_COL, TYPE_COL, TYPE_ID_COL],
        );
        let metadata_cols: Vec<&str> = metadata_cols.iter().map(String::as_str).collect();
        load_node_metadata_from_parquet(
            &graph,
            &c_node_path,
            NODE_GID_COL,
            None,
            Some(TYPE_COL),
            None,
            None,
            &metadata_cols,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
    }

    // ---- nodes_t ----
    let t_node_path = parquet_dir.join(NODES_T_DIR);
    if t_node_path.exists() {
        // exclude rap_layer{,_id} as well as the obvious meta columns, they're currently null/0 values.
        let prop_cols = parquet_prop_columns(
            &t_node_path,
            &[
                NODE_GID_COL,
                NODE_VID_COL,
                TYPE_COL,
                TIME_COL,
                SECONDARY_INDEX_COL,
                LAYER_COL,
                LAYER_ID_COL,
            ],
        );
        let prop_cols: Vec<&str> = prop_cols.iter().map(String::as_str).collect();
        load_nodes_from_parquet(
            &graph,
            &t_node_path,
            TIME_COL,
            Some(SECONDARY_INDEX_COL),
            NODE_GID_COL,
            None,
            None,
            &prop_cols,
            &[],
            None,
            None,
            None,
            None,
            None,
            true,
            None,
        )
        .unwrap();
    }

    // ---- edges_t ----
    // Reference src/dst by GID so the loader looks the nodes up by name.
    let t_edge_path = parquet_dir.join(EDGES_T_DIR);
    if t_edge_path.exists() {
        let prop_cols = parquet_prop_columns(
            &t_edge_path,
            &[
                TIME_COL,
                SECONDARY_INDEX_COL,
                SRC_VID_COL,
                SRC_GID_COL,
                DST_VID_COL,
                DST_GID_COL,
                LAYER_COL,
                LAYER_ID_COL,
                EDGE_ID_COL,
            ],
        );
        let prop_cols: Vec<&str> = prop_cols.iter().map(String::as_str).collect();
        load_edges_from_parquet(
            &graph,
            &t_edge_path,
            ColumnNames::new(
                TIME_COL,
                Some(SECONDARY_INDEX_COL),
                SRC_GID_COL,
                DST_GID_COL,
                Some(LAYER_COL),
            ),
            true,
            &prop_cols,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
    }

    // ---- edges_d ----
    // skipped: master is an event graph and has no deletions.

    // ---- edges_c ----
    let c_edge_path = parquet_dir.join(EDGES_C_DIR);
    if c_edge_path.exists() {
        let metadata_cols = parquet_prop_columns(
            &c_edge_path,
            &[
                SRC_VID_COL,
                SRC_GID_COL,
                DST_VID_COL,
                DST_GID_COL,
                LAYER_COL,
                EDGE_ID_COL,
            ],
        );
        let metadata_cols: Vec<&str> = metadata_cols.iter().map(String::as_str).collect();
        load_edge_metadata_from_parquet(
            &graph,
            &c_edge_path,
            SRC_GID_COL,
            DST_GID_COL,
            &metadata_cols,
            None,
            None,
            Some(LAYER_COL),
            None,
            None,
            true,
        )
        .unwrap();
    }

    graph
}

#[test]
#[ignore = "we don't always want to rebuild the graph"]
fn rebuild_apache_master_with_contiguous_vids() {
    let parquet_dir = master_parquet_files_dir();
    if !parquet_dir.exists() {
        panic!(
            "expected parquet graph at {} — make sure graphql-bench/data/apache/master is present",
            parquet_dir.display()
        );
    }

    println!("Loading master from raw parquet via section loaders...");
    let loaded = load_graphql_master_from_parquet_loaders(&parquet_dir);
    println!(
        "Loaded: nodes={}, edges={}, temporal_edges={}",
        loaded.count_nodes(),
        loaded.count_edges(),
        loaded.count_temporal_edges(),
    );

    let out_dir = master_new_dir();
    remove_dir_all_ignore_not_found(&out_dir).unwrap();
    println!("Writing rebuilt graph to {}", out_dir.display());
    loaded.encode(&out_dir).unwrap();

    println!("Re-loading {} via Graph::decode...", out_dir.display());
    let reloaded = Graph::decode(&out_dir).unwrap();
    println!(
        "Reloaded: nodes={}, edges={}, temporal_edges={}",
        reloaded.count_nodes(),
        reloaded.count_edges(),
        reloaded.count_temporal_edges(),
    );

    assert_graph_equal_timestamps(&loaded, &reloaded);
    println!("OK: rebuilt graph matches single-call decode.");
}

#[test]
fn test_graphql_bench_graph() {
    let parquet_dir = master_parquet_files_dir();
    let master_dir = master_dir();
    if !parquet_dir.exists() {
        panic!(
            "expected parquet graph at {} — make sure graphql-bench/data/apache/master is present",
            parquet_dir.display()
        );
    }

    println!("Loading master from raw parquet via section loaders...");
    let loaded = load_graphql_master_from_parquet_loaders(&parquet_dir);
    println!(
        "Loaded: nodes={}, edges={}, temporal_edges={}",
        loaded.count_nodes(),
        loaded.count_edges(),
        loaded.count_temporal_edges(),
    );
    // these are here to make sure the graph was loaded properly, but they can change if the graph changes
    assert_eq!(
        loaded.count_nodes(),
        52151,
        "node count has changed from 52151 to {}",
        loaded.count_nodes()
    );
    assert_eq!(
        loaded.count_edges(),
        44045,
        "edge count has changed from 44045 to {}",
        loaded.count_edges()
    );
    assert_eq!(
        loaded.count_temporal_edges(),
        44715,
        "temporal edge count has changed from 44715 to {}",
        loaded.count_temporal_edges()
    );

    println!("Re-loading {} via Graph::decode...", parquet_dir.display());
    let reloaded = Graph::decode(&master_dir).unwrap();
    println!(
        "Reloaded: nodes={}, edges={}, temporal_edges={}",
        reloaded.count_nodes(),
        reloaded.count_edges(),
        reloaded.count_temporal_edges(),
    );

    assert_graph_equal(&loaded, &reloaded);
    println!("OK: rebuilt graph matches single-call decode.");
}

fn populate_graph<G: AdditionOps + PropertyAdditionOps + DeletionOps>(graph: &G) {
    // graph_c
    graph
        .add_metadata([
            ("dataset", Prop::str("v4_test")),
            ("schema_version", Prop::U64(1)),
            ("public", Prop::Bool(true)),
        ])
        .unwrap();

    // graph_t
    graph
        .add_properties(1, [("status", Prop::str("init")), ("count", Prop::I64(0))])
        .unwrap();
    graph
        .add_properties(
            5,
            [("status", Prop::str("active")), ("count", Prop::I64(3))],
        )
        .unwrap();
    graph
        .add_properties(10, [("status", Prop::str("done")), ("count", Prop::I64(5))])
        .unwrap();

    // nodes_c + nodes_t + node types
    // Two node types so we test the type column
    let alice = graph
        .add_node(
            1,
            "alice",
            [("score", Prop::I64(10)), ("active", Prop::Bool(true))],
            Some("Person"),
            None,
        )
        .unwrap();
    alice
        .add_metadata([("dept", Prop::str("eng")), ("hired", Prop::I64(2020))])
        .unwrap();

    let bob = graph
        .add_node(1, "bob", [("score", Prop::I64(7))], Some("Person"), None)
        .unwrap();
    bob.add_metadata([("dept", Prop::str("sales"))]).unwrap();

    let server = graph
        .add_node(
            1,
            "server-1",
            [("cpu", Prop::F64(0.1))],
            Some("Server"),
            None,
        )
        .unwrap();
    server
        .add_metadata([("region", Prop::str("us-west-2"))])
        .unwrap();

    // additional temporal updates to nodes with no node type
    graph
        .add_node(3, "alice", [("score", Prop::I64(15))], None, None)
        .unwrap();
    graph
        .add_node(5, "bob", [("score", Prop::I64(9))], None, None)
        .unwrap();
    graph
        .add_node(7, "server-1", [("cpu", Prop::F64(0.6))], None, None)
        .unwrap();

    // edges_t + edges_c
    let knows = graph
        .add_edge(
            2,
            "alice",
            "bob",
            [("weight", Prop::F64(1.0))],
            Some("knows"),
        )
        .unwrap();
    knows
        .add_metadata([("since", Prop::I64(2019))], Some("knows"))
        .unwrap();
    // second update on the same edge and layer
    graph
        .add_edge(
            6,
            "alice",
            "bob",
            [("weight", Prop::F64(2.5))],
            Some("knows"),
        )
        .unwrap();

    let uses = graph
        .add_edge(
            3,
            "alice",
            "server-1",
            [("requests", Prop::I64(3))],
            Some("uses"),
        )
        .unwrap();
    uses.add_metadata([("permission", Prop::str("admin"))], Some("uses"))
        .unwrap();
    graph
        .add_edge(
            4,
            "bob",
            "server-1",
            [("requests", Prop::I64(1))],
            Some("uses"),
        )
        .unwrap();

    // edge on the default layer
    graph
        .add_edge(5, "alice", "bob", [("msg", Prop::str("hi"))], None)
        .unwrap();

    // edges_d
    graph
        .delete_edge(8, "bob", "server-1", Some("uses"))
        .unwrap();
}

#[test]
#[ignore = "we don't always want to rebuild the graphs"]
fn build_v4_saved_disk_graphs() {
    // event graph on disk
    let event_graph_path = event_graph_disk_storage_dir();
    remove_dir_all_ignore_not_found(&event_graph_path).unwrap();
    let disk_event_graph =
        Graph::new_at_path(&event_graph_path).expect("event graph couldn't be created");
    populate_graph(&disk_event_graph);
    drop(disk_event_graph);

    // persistent graph on disk
    let persistent_graph_path = persistent_graph_disk_storage_dir();
    remove_dir_all_ignore_not_found(&persistent_graph_path).unwrap();
    let disk_persistent_graph = PersistentGraph::new_at_path(&persistent_graph_path)
        .expect("persistent graph couldn't be created");
    populate_graph(&disk_persistent_graph);
    drop(disk_persistent_graph);
}

// this should fail when the disk graphs are unreadable or some data is loaded incorrectly
// Fixtures live under `raphtory/resources/test/disk_graphs/{event,persistent}_graph`.
// If they don't exist, run `cargo test ... -- --ignored build_v4_saved_disk_graphs`
// first to generate them.
#[test]
fn validate_v4_disk_graphs() {
    // event graph
    let event_graph_path = event_graph_disk_storage_dir();
    assert!(
        event_graph_path.exists(),
        "missing fixture at {} - run `build_v4_saved_disk_graphs` first",
        event_graph_path.display(),
    );
    let loaded_event_graph =
        Graph::load(&event_graph_path).expect("event graph couldn't be loaded");
    let populated_event_graph = Graph::new();
    populate_graph(&populated_event_graph);
    assert_graph_equal(&loaded_event_graph, &populated_event_graph);

    // persistent graph
    let persistent_graph_path = persistent_graph_disk_storage_dir();
    assert!(
        persistent_graph_path.exists(),
        "missing fixture at {} - run `build_v4_saved_disk_graphs` first",
        persistent_graph_path.display(),
    );
    let loaded_persistent_graph =
        PersistentGraph::load(&persistent_graph_path).expect("persistent graph couldn't be loaded");
    let populated_persistent_graph = PersistentGraph::new();
    populate_graph(&populated_persistent_graph);
    assert_graph_equal_timestamps(&loaded_persistent_graph, &populated_persistent_graph);
}
