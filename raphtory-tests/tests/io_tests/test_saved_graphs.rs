use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use raphtory::{
    arrow_loader::df_loaders::edges::ColumnNames,
    db::graph::graph::{assert_graph_equal, assert_graph_equal_timestamps},
    io::parquet_loaders::{
        get_parquet_file_paths, load_edge_metadata_from_parquet, load_edges_from_parquet,
        load_graph_props_from_parquet, load_node_metadata_from_parquet, load_nodes_from_parquet,
    },
    parquet_encoder::{
        DST_GID_COL, DST_VID_COL, EDGE_COL_ID, LAYER_COL, LAYER_ID_COL, NODE_GID_COL, NODE_VID_COL,
        SECONDARY_INDEX_COL, SRC_GID_COL, SRC_VID_COL, TIME_COL, TYPE_COL, TYPE_ID_COL,
    },
    prelude::*,
    serialise::{
        parquet::{
            EDGES_C_PATH, EDGES_T_PATH, GRAPH_C_PATH, GRAPH_T_PATH, NODES_C_PATH, NODES_T_PATH,
        },
        StableDecode, StableEncode,
    },
};
use std::{
    fs, io,
    path::{Path, PathBuf},
};

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
    let c_graph_path = parquet_dir.join(GRAPH_C_PATH);
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
    let t_graph_path = parquet_dir.join(GRAPH_T_PATH);
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
    let c_node_path = parquet_dir.join(NODES_C_PATH);
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
    let t_node_path = parquet_dir.join(NODES_T_PATH);
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
    let t_edge_path = parquet_dir.join(EDGES_T_PATH);
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
                EDGE_COL_ID,
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
    let c_edge_path = parquet_dir.join(EDGES_C_PATH);
    if c_edge_path.exists() {
        let metadata_cols = parquet_prop_columns(
            &c_edge_path,
            &[
                SRC_VID_COL,
                SRC_GID_COL,
                DST_VID_COL,
                DST_GID_COL,
                LAYER_COL,
                EDGE_COL_ID,
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
fn rebuild_graphql_master_with_contiguous_vids() {
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
