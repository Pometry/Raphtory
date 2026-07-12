use chrono::Local;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use raphtory::{
    arrow_loader::df_loaders::edges::ColumnNames,
    db::{
        api::view::{materialize_impl, MaterializedGraph},
        graph::graph::assert_graph_equal_timestamps,
    },
    io::parquet_loaders::{
        get_parquet_file_paths, load_edge_deletions_from_parquet, load_edge_metadata_from_parquet,
        load_edges_from_parquet, load_graph_props_from_parquet, load_node_metadata_from_parquet,
        load_nodes_from_parquet,
    },
    parquet_encoder::{
        DST_GID_COL, DST_VID_COL, EDGE_COL_ID, LAYER_COL, LAYER_ID_COL, NODE_GID_COL, NODE_VID_COL,
        SECONDARY_INDEX_COL, SRC_GID_COL, SRC_VID_COL, TIME_COL, TYPE_COL, TYPE_ID_COL,
    },
    prelude::{
        AdditionOps, DeletionOps, Graph, GraphViewOps, LayerOps, ParquetDecoder, ParquetEncoder,
        PropertyAdditionOps,
    },
    serialise::parquet::{
        EDGES_C_PATH, EDGES_D_PATH, EDGES_T_PATH, GRAPH_C_PATH, GRAPH_T_PATH, NODES_C_PATH,
        NODES_T_PATH,
    },
};
use raphtory_storage::core_ops::CoreGraphOps;
use std::{
    fs, io,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use storage::persist::strategy::PersistenceStrategy;

fn default_sf10_graph_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../ldbc/data/social_network-sf10-CsvComposite-LongDateFormatter/graph")
}

fn default_sf10_parquet_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        "../../ldbc/data/social_network-sf10-CsvComposite-LongDateFormatter/parquet/data0/graph0",
    )
}

fn default_sf1_graph_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../ldbc/data/social_network-sf1-CsvComposite-LongDateFormatter/graph")
}

fn default_sf1_parquet_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        "../../ldbc/data/social_network-sf1-CsvComposite-LongDateFormatter/parquet/data0/graph0",
    )
}

fn default_materialized_graphs_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../ldbc/data/materialized-graphs")
}

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
    g.add_node(0, "A", [("node_meta", "alpha")], Some("TypeA"), None)
        .unwrap();
    g.add_node(1, "B", [("node_meta", "beta")], None, None)
        .unwrap();
    g.add_edge(2, "A", "B", [("weight", 1)], Some("layer1"))
        .unwrap();
    g.add_edge(3, "A", "B", [("weight", 2)], Some("layer2"))
        .unwrap();
    g.delete_edge(4, "A", "B", Some("layer1")).unwrap();
    g.add_properties(5, [("graph_prop", "present")]).unwrap();
    g.add_metadata([("graph_meta", "constant")]).unwrap();

    let expected = g.materialize().unwrap();

    assert_graph_equal_timestamps(&expected, &g);
}

#[test]
#[ignore = "requires a locally persisted SNB SF1 graph produced by ldbc/load_snb_sf10.py"]
fn test_get_materialize_snb_sf1_time() {
    let graph_path = default_sf1_graph_path();
    let rb_materialize_graph_path = default_materialized_graphs_path().join("rb_materialize_sf1");
    // clear out the directories in case they had previous files in them
    remove_dir_all_ignore_not_found(&rb_materialize_graph_path).unwrap();
    fs::create_dir_all(&rb_materialize_graph_path).unwrap();

    if !graph_path.exists() {
        eprintln!("SNB graph not found at {}", graph_path.display());
        return;
    }

    println!("Loading SNB graph from {}", graph_path.display());
    let g = Graph::load(&graph_path).unwrap();
    println!(
        "Loaded source graph: {} nodes, {} edges, {} temporal edges",
        g.count_nodes(),
        g.count_edges(),
        g.count_temporal_edges()
    );

    println!("Starting materialize at {}", Local::now());
    let materialize_start = Instant::now();
    let materialized_graph = materialize_impl(
        &g,
        Some(&rb_materialize_graph_path),
        g.core_graph().extension().config().clone(),
    )
    .unwrap();
    let materialize_elapsed = materialize_start.elapsed();
    println!(
        "Finished materialize at {}\nTook {materialize_elapsed:?}",
        Local::now()
    );

    println!("Checking materialized graph...");
    assert_graph_equal_timestamps(&g, &materialized_graph);
    println!("Passed!");
    remove_dir_all_ignore_not_found(&rb_materialize_graph_path).unwrap();
}

#[test]
#[ignore = "requires a locally persisted SNB SF1 graph produced by ldbc/load_snb_sf10.py"]
fn test_materialize_filtered_sf1_matches() {
    let graph_path = default_sf1_graph_path();
    let rb_materialize_graph_path =
        default_materialized_graphs_path().join("sf1_filtered_materialize_rb");

    remove_dir_all_ignore_not_found(&rb_materialize_graph_path).unwrap();
    fs::create_dir_all(&rb_materialize_graph_path).unwrap();

    if !graph_path.exists() {
        eprintln!("SNB graph not found at {}", graph_path.display());
        return;
    }

    let selected_node_types = ["Person", "Forum", "Post", "Comment"];
    let selected_layers = [
        "KNOWS",
        "LIKES",
        "HAS_MEMBER",
        "HAS_CREATOR",
        "HAS_MODERATOR",
        "CONTAINER_OF",
        "REPLY_OF",
    ];

    println!(
        "Loading filtered-view SF1 source graph from {}",
        graph_path.display()
    );
    let g = Graph::load(&graph_path).unwrap();

    let total_nodes = g.count_nodes();
    let total_edges = g.count_edges();
    let total_temporal_edges = g.count_temporal_edges();

    let filtered = g
        .subgraph_node_types(selected_node_types)
        .layers(selected_layers)
        .unwrap();

    let selected_nodes = filtered.count_nodes();
    let selected_edges = filtered.count_edges();
    let selected_temporal_edges = filtered.count_temporal_edges();

    println!(
        "Filtered SF1 view uses node types {:?} and layers {:?}",
        selected_node_types, selected_layers
    );
    let nodes_percent = (selected_nodes * 100).checked_div(total_nodes).unwrap_or(0);
    let edges_percent = (selected_edges * 100).checked_div(total_edges).unwrap_or(0);
    let temporal_edges_percent = (selected_temporal_edges * 100)
        .checked_div(total_temporal_edges)
        .unwrap_or(0);

    println!(
        "Selected {selected_nodes}/{total_nodes} nodes ({nodes_percent}%), \
        {selected_edges}/{total_edges} edges ({edges_percent}%), \
        {selected_temporal_edges}/{total_temporal_edges} temporal edges ({temporal_edges_percent}%)"
    );

    println!("Starting filtered SF1 materialize at {}", Local::now());
    let materialize_start = Instant::now();
    let materialized_graph = materialize_impl(
        &filtered,
        Some(&rb_materialize_graph_path),
        g.core_graph().extension().config().clone(),
    )
    .unwrap();
    let materialize_elapsed = materialize_start.elapsed();
    println!(
        "Finished filtered SF1 materialize at {}\nTook {materialize_elapsed:?}",
        Local::now()
    );

    println!("Checking filtered materialized graph");
    assert_graph_equal_timestamps(&filtered, &materialized_graph);
    println!("Passed!");

    remove_dir_all_ignore_not_found(&rb_materialize_graph_path).unwrap();
}

fn get_materialize_time(graph_path: &Path, materialize_graph_path: &Path) -> Duration {
    remove_dir_all_ignore_not_found(&materialize_graph_path).unwrap();
    fs::create_dir_all(&materialize_graph_path).unwrap();

    if !graph_path.exists() {
        panic!("SNB graph not found at {}", graph_path.display());
    }

    println!("Loading SF10 SNB graph from {}", graph_path.display());
    let sf10_graph = Graph::load(&graph_path).unwrap();
    println!(
        "Loaded SF10 source graph: {} nodes, {} edges, {} temporal edges",
        sf10_graph.count_nodes(),
        sf10_graph.count_edges(),
        sf10_graph.count_temporal_edges()
    );

    println!("Starting SF10 materialize at {}", Local::now());
    let materialize_start = Instant::now();
    let _materialized_graph = materialize_impl(
        &sf10_graph,
        Some(&materialize_graph_path),
        sf10_graph.core_graph().extension().config().clone(),
    )
    .unwrap();
    let materialize_elapsed = materialize_start.elapsed();
    println!(
        "Finished SF10 materialize at {}\nTook {materialize_elapsed:?}",
        Local::now()
    );
    drop(_materialized_graph);
    drop(sf10_graph);
    // free up disk space for next test
    remove_dir_all_ignore_not_found(&materialize_graph_path).unwrap();
    materialize_elapsed
}

fn get_parquet_decode_time(
    graph_path: &Path,
    parquet_path: &Path,
    decode_graph_path: &Path,
) -> Duration {
    remove_dir_all_ignore_not_found(&decode_graph_path).unwrap();
    fs::create_dir_all(&decode_graph_path).unwrap();

    if !graph_path.exists() {
        panic!("SNB graph not found at {}", graph_path.display());
    }
    if !parquet_path.exists() {
        panic!(
            "SNB parquet directory not found at {}",
            parquet_path.display()
        );
    }

    println!("Loading SF10 SNB graph from {}", graph_path.display());
    let sf10_graph = Graph::load(&graph_path).unwrap();
    println!(
        "Loaded SF10 source graph in: {} nodes, {} edges, {} temporal edges",
        sf10_graph.count_nodes(),
        sf10_graph.count_edges(),
        sf10_graph.count_temporal_edges()
    );
    let sf10_extension_config = sf10_graph.core_graph().extension().config().clone();
    drop(sf10_graph);

    println!("Starting SF10 decode_parquet at {}", Local::now());
    let parquet_decode_start = Instant::now();
    let _parquet_graph = MaterializedGraph::decode_parquet(
        &parquet_path,
        Some(&decode_graph_path),
        sf10_extension_config,
    )
    .unwrap();
    let parquet_decode_elapsed = parquet_decode_start.elapsed();
    println!(
        "Finished SF10 decode_parquet at {}\nTook {parquet_decode_elapsed:?}",
        Local::now()
    );
    drop(_parquet_graph);
    // free up disk space for next test
    remove_dir_all_ignore_not_found(&decode_graph_path).unwrap();
    parquet_decode_elapsed
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
                .map(|field| field.name().to_string())
                .filter(|name| !exclude.iter().any(|excluded| excluded == name))
                .collect()
        })
        .unwrap_or_default()
}

fn get_parquet_encode_time(graph_path: &Path, parquet_graph_path: &Path) -> Duration {
    remove_dir_all_ignore_not_found(&parquet_graph_path).unwrap();
    fs::create_dir_all(&parquet_graph_path).unwrap();

    if !graph_path.exists() {
        panic!("SNB graph not found at {}", graph_path.display());
    }

    println!("Loading SF10 SNB graph from {}", graph_path.display());
    let sf10_graph = Graph::load(&graph_path).unwrap();
    println!(
        "Loaded SF10 source graph: {} nodes, {} edges, {} temporal edges",
        sf10_graph.count_nodes(),
        sf10_graph.count_edges(),
        sf10_graph.count_temporal_edges()
    );

    println!("Starting SF10 encode_parquet at {}", Local::now());
    let parquet_dump_start = Instant::now();
    sf10_graph.encode_parquet(parquet_graph_path).unwrap();
    let parquet_dump_elapsed = parquet_dump_start.elapsed();
    println!(
        "Finished SF10 encode_parquet at {}\nTook {parquet_dump_elapsed:?}",
        Local::now()
    );

    parquet_dump_elapsed
}

fn get_parquet_df_loader_time(
    graph_path: &Path,
    parquet_path: &Path,
    load_graph_path: &Path,
) -> Duration {
    remove_dir_all_ignore_not_found(&load_graph_path).unwrap();
    fs::create_dir_all(&load_graph_path).unwrap();

    if !graph_path.exists() {
        panic!("SNB graph not found at {}", graph_path.display());
    }
    if !parquet_path.exists() {
        panic!(
            "SNB parquet graph directory not found at {}",
            parquet_path.display()
        );
    }

    println!("Loading SF10 SNB graph from {}", graph_path.display());
    let sf10_graph = Graph::load(&graph_path).unwrap();
    println!(
        "Loaded SF10 source graph: {} nodes, {} edges, {} temporal edges",
        sf10_graph.count_nodes(),
        sf10_graph.count_edges(),
        sf10_graph.count_temporal_edges()
    );
    let sf10_extension_config = sf10_graph.core_graph().extension().config().clone();
    drop(sf10_graph);

    let replay_graph =
        Graph::new_at_path_with_config(load_graph_path, sf10_extension_config).unwrap();
    println!("Starting SF10 parquet loader replay at {}", Local::now());
    let parquet_load_start = Instant::now();

    let c_graph_path = parquet_path.join(GRAPH_C_PATH);
    if c_graph_path.exists() {
        let graph_c_metadata = parquet_prop_columns(&c_graph_path, &[TIME_COL]);
        let graph_c_metadata = graph_c_metadata
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let graph_c_start = Instant::now();
        load_graph_props_from_parquet(
            &replay_graph,
            &c_graph_path,
            TIME_COL,
            None,
            &[],
            &graph_c_metadata,
            None,
            None,
        )
        .unwrap();
        println!(
            "GraphC loaded at {}\nTook {:?}",
            Local::now(),
            graph_c_start.elapsed()
        );
    }

    let t_graph_path = parquet_path.join(GRAPH_T_PATH);
    if t_graph_path.exists() {
        let graph_t_props = parquet_prop_columns(&t_graph_path, &[TIME_COL, SECONDARY_INDEX_COL]);
        let graph_t_props = graph_t_props.iter().map(String::as_str).collect::<Vec<_>>();
        let graph_t_start = Instant::now();
        load_graph_props_from_parquet(
            &replay_graph,
            &t_graph_path,
            TIME_COL,
            Some(SECONDARY_INDEX_COL),
            &graph_t_props,
            &[],
            None,
            None,
        )
        .unwrap();
        println!(
            "GraphT loaded at {}\nTook {:?}",
            Local::now(),
            graph_t_start.elapsed()
        );
    }

    let c_node_path = parquet_path.join(NODES_C_PATH);
    if c_node_path.exists() {
        let node_c_metadata = parquet_prop_columns(
            &c_node_path,
            &[NODE_GID_COL, NODE_VID_COL, TYPE_COL, TYPE_ID_COL, LAYER_COL],
        );
        let node_c_metadata = node_c_metadata
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let nodes_c_start = Instant::now();
        load_node_metadata_from_parquet(
            &replay_graph,
            &c_node_path,
            NODE_GID_COL,
            None,
            Some(TYPE_COL),
            Some(NODE_VID_COL),
            Some(TYPE_ID_COL),
            &node_c_metadata,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        println!(
            "NodesC loaded at {}\nTook {:?}",
            Local::now(),
            nodes_c_start.elapsed()
        );
    }

    let t_node_path = parquet_path.join(NODES_T_PATH);
    if t_node_path.exists() {
        let node_t_props = parquet_prop_columns(
            &t_node_path,
            &[
                NODE_GID_COL,
                NODE_VID_COL,
                TYPE_COL,
                TIME_COL,
                SECONDARY_INDEX_COL,
            ],
        );
        let node_t_props = node_t_props.iter().map(String::as_str).collect::<Vec<_>>();
        let nodes_t_start = Instant::now();
        load_nodes_from_parquet(
            &replay_graph,
            &t_node_path,
            TIME_COL,
            Some(SECONDARY_INDEX_COL),
            NODE_VID_COL,
            None,
            None,
            &node_t_props,
            &[],
            None,
            None,
            None,
            None,
            None,
            false,
            None,
        )
        .unwrap();
        println!(
            "NodesT loaded at {}\nTook {:?}",
            Local::now(),
            nodes_t_start.elapsed()
        );
    }

    let t_edge_path = parquet_path.join(EDGES_T_PATH);
    if t_edge_path.exists() {
        let edge_t_props = parquet_prop_columns(
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
        let edge_t_props = edge_t_props.iter().map(String::as_str).collect::<Vec<_>>();
        let edges_t_start = Instant::now();
        load_edges_from_parquet(
            &replay_graph,
            &t_edge_path,
            ColumnNames::new(
                TIME_COL,
                Some(SECONDARY_INDEX_COL),
                SRC_VID_COL,
                DST_VID_COL,
                Some(LAYER_COL),
            )
            .with_layer_id_col(LAYER_ID_COL)
            .with_edge_id_col(EDGE_COL_ID),
            false,
            &edge_t_props,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        println!(
            "EdgesT loaded at {}\nTook {:?}",
            Local::now(),
            edges_t_start.elapsed()
        );
    }

    let d_edge_path = parquet_path.join(EDGES_D_PATH);
    if d_edge_path.exists() {
        let edges_d_start = Instant::now();
        load_edge_deletions_from_parquet(
            &replay_graph,
            &d_edge_path,
            ColumnNames::new(
                TIME_COL,
                Some(SECONDARY_INDEX_COL),
                SRC_VID_COL,
                DST_VID_COL,
                Some(LAYER_COL),
            )
            .with_layer_id_col(LAYER_ID_COL)
            .with_edge_id_col(EDGE_COL_ID),
            None,
            false,
            None,
            None,
        )
        .unwrap();
        println!(
            "EdgesD loaded at {}\nTook {:?}",
            Local::now(),
            edges_d_start.elapsed()
        );
    }

    let c_edge_path = parquet_path.join(EDGES_C_PATH);
    if c_edge_path.exists() {
        let edge_c_metadata = parquet_prop_columns(
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
        let edge_c_metadata = edge_c_metadata
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let edges_c_start = Instant::now();
        load_edge_metadata_from_parquet(
            &replay_graph,
            &c_edge_path,
            SRC_VID_COL,
            DST_VID_COL,
            &edge_c_metadata,
            None,
            None,
            Some(LAYER_COL),
            None,
            None,
            false,
        )
        .unwrap();
        println!(
            "EdgesC loaded at {}\nTook {:?}",
            Local::now(),
            edges_c_start.elapsed()
        );
    }

    let parquet_load_elapsed = parquet_load_start.elapsed();
    println!(
        "Finished SF10 parquet loader replay at {}\nLoaded graph: {} nodes, {} edges, {} temporal edges\nTook {parquet_load_elapsed:?}",
        Local::now(),
        replay_graph.count_nodes(),
        replay_graph.count_edges(),
        replay_graph.count_temporal_edges(),
    );
    drop(replay_graph);
    remove_dir_all_ignore_not_found(&load_graph_path).unwrap();

    parquet_load_elapsed
}

#[test]
#[ignore = "requires locally persisted SNB SF10 graphs and parquet export"]
fn test_all() {
    let graph_path = default_sf10_graph_path();
    let parquet_path = default_sf10_parquet_path();
    let parquet_loader_graph_path = default_materialized_graphs_path().join("parquet_loader_sf10");
    let parquet_decode_graph_path = default_materialized_graphs_path().join("parquet_decode_sf10");
    let materialize_graph_path = default_materialized_graphs_path().join("rb_materialize_sf10");

    let materialize_duration = get_materialize_time(&graph_path, &materialize_graph_path);

    let parquet_dump_duration = get_parquet_encode_time(&graph_path, &parquet_path);

    let parquet_loader_duration =
        get_parquet_df_loader_time(&graph_path, &parquet_path, &parquet_loader_graph_path);

    let parquet_decode_duration =
        get_parquet_decode_time(&graph_path, &parquet_path, &parquet_decode_graph_path);

    println!(
        "Summary:\n  encode_parquet: {:?}\n  parquet loaders replay: {:?}\n  decode_parquet: {:?}\n  materialize: {:?}",
        parquet_dump_duration,
        parquet_loader_duration,
        parquet_decode_duration,
        materialize_duration
    );
}
