use raphtory::{
    arrow_loader::df_loaders::edges::ColumnNames, errors::GraphError,
    io::parquet_loaders::load_edges_from_parquet, prelude::*,
};
use std::path::{Path, PathBuf};

#[cfg(target_os = "macos")]
use tikv_jemallocator::Jemalloc;

#[cfg(target_os = "macos")]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const TIME_COL: &str = "epoch_time";
const SRC_COL: &str = "src";
const DST_COL: &str = "dst";

/// One LANL edge layer: a directory of Parquet files and the columns to load as properties.
struct LayerInput {
    layer: &'static str,
    dir: &'static str,
    property_cols: &'static [&'static str],
}

const LAYERS: &[LayerInput] = &[
    LayerInput {
        layer: "nft",
        dir: "nft_sorted",
        property_cols: &[
            "duration",
            "protocol",
            "src_port",
            "dst_port",
            "src_packets",
            "dst_packets",
            "src_bytes",
            "dst_bytes",
        ],
    },
    LayerInput {
        layer: "v1",
        dir: "v1_sorted",
        property_cols: &[
            "event_id",
            "user_name",
            "domain_name",
            "logon_id",
            "process_name",
            "process_id",
            "parent_process_name",
            "parent_process_id",
        ],
    },
    LayerInput {
        layer: "v2",
        dir: "v2_sorted",
        property_cols: &[
            "event_id",
            "log_host",
            "logon_type",
            "logon_type_description",
            "user_name",
            "domain_name",
            "logon_id",
            "subject_user_name",
            "subject_domain_name",
            "subject_logon_id",
            "status",
            "service_name",
            "authentication_package",
            "failure_reason",
            "process_name",
            "process_id",
            "parent_process_name",
            "parent_process_id",
        ],
    },
];

/// Load the LANL edge layers from `data_dir` into `graph`.
///
/// `layer_filter`, when given, keeps only the layers whose name is in the list.
fn load_lanl_graph(
    data_dir: &Path,
    layer_filter: Option<&[String]>,
    graph: &Graph,
) -> Result<(), GraphError> {
    let layers = LAYERS.iter().filter(|l| {
        layer_filter
            .map(|f| f.iter().any(|name| name == l.layer))
            .unwrap_or(true)
    });

    for input in layers {
        let path = data_dir.join(input.dir);
        println!(
            "Loading layer '{}' from {:?} with property columns {:?}...",
            input.layer, path, input.property_cols
        );
        load_edges_from_parquet(
            graph,
            &path,
            ColumnNames::new(TIME_COL, None, SRC_COL, DST_COL, None),
            true,
            input.property_cols,
            &[],
            None,
            Some(input.layer),
            None,
            None,
        )?;
        println!(" ✓ Finished loading layer '{}'", input.layer);
    }

    println!(
        "\n✅ Graph loaded: {} nodes, {} edges",
        graph.count_nodes(),
        graph.count_edges()
    );
    Ok(())
}

/// Usage: `lanl_loader <data_dir> [layers_json] [graph_path]`
///
/// `<data_dir>` must contain `nft_sorted/`, `v1_sorted/` and `v2_sorted/`.
///
/// Load all three layers into `<data_dir>/../graph`:
/// ```sh
/// cargo run --release --example lanl_loader --features io -- /data/lanl
/// ```
///
/// Load only the `nft` and `v2` layers into a chosen graph directory:
/// ```sh
/// cargo run --release --example lanl_loader --features io -- /data/lanl '["nft","v2"]' /data/lanl_graph
/// ```
fn main() {
    let data_dir = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| panic!("Usage: lanl_loader <data_dir> [layers_json] [graph_path]"));
    // Optional JSON list of layers to load, e.g. '["nft","v2"]'.
    let layer_filter = std::env::args()
        .nth(2)
        .map(|s| serde_json::from_str::<Vec<String>>(&s))
        .transpose()
        .unwrap();

    let graph_path = std::env::args()
        .nth(3)
        .map(PathBuf::from)
        .unwrap_or_else(|| data_dir.join("..").join("lanl_graph"));
    let graph = if !graph_path.exists() {
        Graph::new_at_path(&graph_path).unwrap()
    } else {
        Graph::load(&graph_path).unwrap()
    };
    load_lanl_graph(&data_dir, layer_filter.as_deref(), &graph).unwrap()
}
