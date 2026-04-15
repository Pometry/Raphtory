use chrono::Local;
use itertools::Itertools;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use proptest::{arbitrary::any, proptest};
#[cfg(feature = "io")]
use raphtory::io::parquet_loaders::{
    get_parquet_file_paths, load_edge_deletions_from_parquet, load_edge_metadata_from_parquet,
    load_edges_from_parquet, load_graph_props_from_parquet, load_node_metadata_from_parquet,
    load_nodes_from_parquet,
};
use raphtory::{
    arrow_loader::df_loaders::edges::ColumnNames,
    db::{
        api::{
            storage::storage::PersistenceStrategy,
            view::{materialize_using_recordbatches, MaterializedGraph},
        },
        graph::graph::{assert_graph_equal, assert_graph_equal_timestamps, graph_equal},
    },
    prelude::*,
    serialise::parquet::ParquetEncoder,
    test_storage,
    test_utils::{build_edge_list, build_graph_from_edge_list},
};
use raphtory_api::core::storage::arc_str::OptionAsStr;
use raphtory_storage::core_ops::CoreGraphOps;
use std::{
    fs, io,
    ops::Range,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

fn default_sf10_graph_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../ldbc/data/social_network-sf10-CsvComposite-LongDateFormatter/graph")
}

#[cfg(feature = "io")]
fn default_sf10_parquet_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        "../../ldbc/data/social_network-sf10-CsvComposite-LongDateFormatter/parquet/data0/graph0",
    )
}

fn default_sf3_graph_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../ldbc/data/social_network-sf3-CsvComposite-LongDateFormatter/graph")
}

#[cfg(feature = "io")]
fn default_sf3_parquet_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        "../../ldbc/data/social_network-sf3-CsvComposite-LongDateFormatter/parquet/data0/graph0",
    )
}

fn default_sf1_graph_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../ldbc/data/social_network-sf1-CsvComposite-LongDateFormatter/graph")
}

#[cfg(feature = "io")]
fn default_sf1_parquet_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        "../../ldbc/data/social_network-sf1-CsvComposite-LongDateFormatter/parquet/data0/graph0",
    )
}

fn default_materialized_graphs_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../ldbc/data/materialized-graphs")
}

fn set_snb_env_vars() {
    unsafe {
        std::env::set_var("RAPHTORY_FSYNC_ON_FLUSH", "false");
        std::env::set_var("RAPHTORY_MAX_NODE_PAGE_LEN", "500000");
        std::env::set_var("RAPHTORY_MAX_EDGE_PAGE_LEN", "5000000");
        std::env::set_var("RAPHTORY_MAX_MEMORY_BYTES", "12000000000");
        std::env::set_var("RAPHTORY_MIN_FLUSH_BYTES", "128000000");
    }
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

    println!("Loading SNB graph from {}", graph_path.display());
    let g = Graph::load(&graph_path).unwrap();
    println!(
        "Loaded source graph: {} nodes, {} edges, {} temporal edges",
        g.count_nodes(),
        g.count_edges(),
        g.count_temporal_edges()
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

    println!("Starting materialize impl (old) at {}", Local::now());
    let impl_start = Instant::now();
    let materialize_impl_graph = g.materialize_at(&old_materialize_graph_path).unwrap();
    let impl_elapsed = impl_start.elapsed();
    println!(
        "Finished materialize impl (old) at {}\nTook {impl_elapsed:?}",
        Local::now()
    );

    assert!(graph_equal(&g, &materialize_impl_graph));
    assert!(graph_equal(&g, &recordbatch_graph));

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

    println!("Faster path: {faster} ({ratio:.2}x)");
}

fn get_new_materialize_time(graph_path: &Path, materialize_graph_path: &Path) -> Duration {
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

    println!(
        "Starting SF10 materialize using RecordBatches at {}",
        Local::now()
    );
    let recordbatch_start = Instant::now();
    let _recordbatch_graph = materialize_using_recordbatches(
        &sf10_graph,
        Some(&materialize_graph_path),
        sf10_graph.core_graph().extension().config().clone(),
    )
    .unwrap();
    let recordbatch_elapsed = recordbatch_start.elapsed();
    println!(
        "Finished SF10 materialize using RecordBatches at {}\nTook {recordbatch_elapsed:?}",
        Local::now()
    );
    drop(_recordbatch_graph);
    drop(sf10_graph);
    // free up disk space for next test
    remove_dir_all_ignore_not_found(&materialize_graph_path).unwrap();
    recordbatch_elapsed
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

// FIXME: Is there a way to safely import these from parquet/mod.rs?
const RAP_NODE_ID_COL: &str = "rap_node_id";
const RAP_NODE_VID_COL: &str = "rap_node_vid";
const RAP_NODE_TYPE_COL: &str = "rap_node_type";
const RAP_NODE_TYPE_ID_COL: &str = "rap_node_type_id";
const RAP_TIME_COL: &str = "rap_time";
const RAP_SECONDARY_INDEX_COL: &str = "rap_secondary_index";
const RAP_SRC_ID_COL: &str = "rap_src_id";
const RAP_DST_ID_COL: &str = "rap_dst_id";
const RAP_EDGE_ID_COL: &str = "rap_edge_id";
const RAP_LAYER_COL: &str = "rap_layer";
const RAP_LAYER_ID_COL: &str = "rap_layer_id";
#[cfg(feature = "io")]
const GRAPH_C_PARQUET_DIR: &str = "graph_c";
#[cfg(feature = "io")]
const GRAPH_T_PARQUET_DIR: &str = "graph_t";
#[cfg(feature = "io")]
const NODES_C_PARQUET_DIR: &str = "nodes_c";
#[cfg(feature = "io")]
const NODES_T_PARQUET_DIR: &str = "nodes_t";
#[cfg(feature = "io")]
const EDGES_T_PARQUET_DIR: &str = "edges_t";
#[cfg(feature = "io")]
const EDGES_D_PARQUET_DIR: &str = "edges_d";
#[cfg(feature = "io")]
const EDGES_C_PARQUET_DIR: &str = "edges_c";

#[cfg(feature = "io")]
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

#[cfg(feature = "io")]
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

#[cfg(feature = "io")]
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

    let c_graph_path = parquet_path.join(GRAPH_C_PARQUET_DIR);
    if c_graph_path.exists() {
        let graph_c_metadata = parquet_prop_columns(&c_graph_path, &[RAP_TIME_COL]);
        let graph_c_metadata = graph_c_metadata
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let graph_c_start = Instant::now();
        load_graph_props_from_parquet(
            &replay_graph,
            &c_graph_path,
            RAP_TIME_COL,
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

    let t_graph_path = parquet_path.join(GRAPH_T_PARQUET_DIR);
    if t_graph_path.exists() {
        let graph_t_props =
            parquet_prop_columns(&t_graph_path, &[RAP_TIME_COL, RAP_SECONDARY_INDEX_COL]);
        let graph_t_props = graph_t_props.iter().map(String::as_str).collect::<Vec<_>>();
        let graph_t_start = Instant::now();
        load_graph_props_from_parquet(
            &replay_graph,
            &t_graph_path,
            RAP_TIME_COL,
            Some(RAP_SECONDARY_INDEX_COL),
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

    let c_node_path = parquet_path.join(NODES_C_PARQUET_DIR);
    if c_node_path.exists() {
        let node_c_metadata = parquet_prop_columns(
            &c_node_path,
            &[
                RAP_NODE_ID_COL,
                RAP_NODE_VID_COL,
                RAP_NODE_TYPE_COL,
                RAP_NODE_TYPE_ID_COL,
            ],
        );
        let node_c_metadata = node_c_metadata
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let nodes_c_start = Instant::now();
        load_node_metadata_from_parquet(
            &replay_graph,
            &c_node_path,
            RAP_NODE_ID_COL,
            None,
            Some(RAP_NODE_TYPE_COL),
            Some(RAP_NODE_VID_COL),
            Some(RAP_NODE_TYPE_ID_COL),
            &node_c_metadata,
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

    let t_node_path = parquet_path.join(NODES_T_PARQUET_DIR);
    if t_node_path.exists() {
        let node_t_props = parquet_prop_columns(
            &t_node_path,
            &[RAP_NODE_VID_COL, RAP_TIME_COL, RAP_SECONDARY_INDEX_COL],
        );
        let node_t_props = node_t_props.iter().map(String::as_str).collect::<Vec<_>>();
        let nodes_t_start = Instant::now();
        load_nodes_from_parquet(
            &replay_graph,
            &t_node_path,
            RAP_TIME_COL,
            Some(RAP_SECONDARY_INDEX_COL),
            RAP_NODE_VID_COL,
            None,
            None,
            &node_t_props,
            &[],
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

    let t_edge_path = parquet_path.join(EDGES_T_PARQUET_DIR);
    if t_edge_path.exists() {
        let edge_t_props = parquet_prop_columns(
            &t_edge_path,
            &[
                RAP_TIME_COL,
                RAP_SECONDARY_INDEX_COL,
                RAP_SRC_ID_COL,
                RAP_DST_ID_COL,
                RAP_LAYER_COL,
                RAP_LAYER_ID_COL,
                RAP_EDGE_ID_COL,
            ],
        );
        let edge_t_props = edge_t_props.iter().map(String::as_str).collect::<Vec<_>>();
        let edges_t_start = Instant::now();
        load_edges_from_parquet(
            &replay_graph,
            &t_edge_path,
            ColumnNames::new(
                RAP_TIME_COL,
                Some(RAP_SECONDARY_INDEX_COL),
                RAP_SRC_ID_COL,
                RAP_DST_ID_COL,
                Some(RAP_LAYER_COL),
            )
            .with_layer_id_col(RAP_LAYER_ID_COL)
            .with_edge_id_col(RAP_EDGE_ID_COL),
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

    let d_edge_path = parquet_path.join(EDGES_D_PARQUET_DIR);
    if d_edge_path.exists() {
        let edges_d_start = Instant::now();
        load_edge_deletions_from_parquet(
            &replay_graph,
            &d_edge_path,
            ColumnNames::new(
                RAP_TIME_COL,
                Some(RAP_SECONDARY_INDEX_COL),
                RAP_SRC_ID_COL,
                RAP_DST_ID_COL,
                Some(RAP_LAYER_COL),
            )
            .with_layer_id_col(RAP_LAYER_ID_COL)
            .with_edge_id_col(RAP_EDGE_ID_COL),
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

    let c_edge_path = parquet_path.join(EDGES_C_PARQUET_DIR);
    if c_edge_path.exists() {
        let edge_c_metadata = parquet_prop_columns(
            &c_edge_path,
            &[
                RAP_SRC_ID_COL,
                RAP_DST_ID_COL,
                RAP_LAYER_COL,
                RAP_EDGE_ID_COL,
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
            RAP_SRC_ID_COL,
            RAP_DST_ID_COL,
            &edge_c_metadata,
            None,
            None,
            Some(RAP_LAYER_COL),
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

#[cfg(feature = "io")]
#[test]
#[ignore = "requires locally persisted SNB SF10 graphs and parquet export"]
fn test_current() {
    set_snb_env_vars();
    let graph_path = default_sf10_graph_path();
    let parquet_path = default_sf10_parquet_path();
    let parquet_loader_graph_path = default_materialized_graphs_path().join("parquet_loader_sf10");
    let parquet_decode_graph_path = default_materialized_graphs_path().join("parquet_decode_sf10");
    let materialize_graph_path = default_materialized_graphs_path().join("rb_materialize_sf10");

    let parquet_dump_duration = get_parquet_encode_time(&graph_path, &parquet_path);

    let parquet_loader_duration =
        get_parquet_df_loader_time(&graph_path, &parquet_path, &parquet_loader_graph_path);

    let parquet_decode_duration =
        get_parquet_decode_time(&graph_path, &parquet_path, &parquet_decode_graph_path);

    let materialize_duration = get_new_materialize_time(&graph_path, &materialize_graph_path);

    println!(
        "Summary:\n  encode_parquet: {:?}\n  parquet loaders replay: {:?}\n  decode_parquet: {:?}\n  materialize_using_recordbatches: {:?}",
        parquet_dump_duration,
        parquet_loader_duration,
        parquet_decode_duration,
        materialize_duration
    );

    // let materialize_secs = materialize_duration.as_secs_f64();
    // let parquet_decode_secs = parquet_decode_duration.as_secs_f64();
    // let faster = if materialize_secs < parquet_decode_secs {
    //     "materialize_using_recordbatches"
    // } else if parquet_decode_secs < materialize_secs {
    //     "decode_parquet"
    // } else {
    //     "tie"
    // };
    // let ratio = if materialize_secs > 0.0 && parquet_decode_secs > 0.0 {
    //     if materialize_secs > parquet_decode_secs {
    //         materialize_secs / parquet_decode_secs
    //     } else {
    //         parquet_decode_secs / materialize_secs
    //     }
    // } else {
    //     1.0
    // };
    //
    // println!("Faster path: {faster} ({ratio:.2}x)");
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
