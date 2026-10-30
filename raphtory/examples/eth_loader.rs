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
/// Load ETH data from Parquet files into a Raphtory Graph.
fn load_eth_graph(parquet_path: &Path, graph: &Graph) -> Result<(), GraphError> {
    // ── Static Nodes ──────────────────────────────────────────────────────

    // ┌────────────────────────────────────────────────────────────────────────────────┐
    // │                                   *.parquet                                    │
    // │                                                                                │
    // │ gas                         bigint       hash                        varchar   │
    // │ input                       varchar      nonce                       bigint    │
    // │ value                       double       block_number                bigint    │
    // │ block_hash                  varchar      transaction_index           bigint    │
    // │ from_address                varchar      to_address                  varchar   │
    // │ gas_price                   bigint       receipt_cumulative_gas_used bigint    │
    // │ receipt_gas_used            bigint       receipt_contract_address    varchar   │
    // │ receipt_status              bigint       receipt_effective_gas_price bigint    │
    // │ transaction_type            bigint       max_fee_per_gas             bigint    │
    // │ max_priority_fee_per_gas    bigint       block_timestamp             timestamp │
    // │ date                        varchar      last_modified               timestamp │
    // └────────────────────────────────────────────────────────────────────────────────┘
    // ETH_NO_PROPS loads the edges alone; ETH_BATCH_SIZE sets the rows per chunk
    let properties: &[&str] = if std::env::var_os("ETH_NO_PROPS").is_some() {
        &[]
    } else {
        &["gas", "value", "block_hash", "transaction_type", "hash"]
    };
    let batch_size = std::env::var("ETH_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse().ok());
    load_edges_from_parquet(
        graph,
        parquet_path,
        ColumnNames::new("block_timestamp", None, "from_address", "to_address", None),
        true,
        properties,
        &[],
        None,
        None,
        batch_size,
        None,
    )?;
    graph.flush()?;
    println!("  ✓ ETH edges");

    println!(
        "\n✅ Graph loaded: {} nodes, {} edges",
        graph.count_nodes(),
        graph.count_edges()
    );
    Ok(())
}

fn main() {
    let parquet_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| panic!("Usage: snb_loader <data_dir>"));
    let graph_path = std::env::args()
        .nth(2)
        .map(PathBuf::from)
        .unwrap_or_else(|| parquet_path.join("..").join("graph"));
    if graph_path.exists() {
        let now = std::time::Instant::now();
        let graph = Graph::load(&graph_path).unwrap();
        println!(
            "Graph loaded from {graph_path:?} num nodes: {}, num edges: {}",
            graph.count_nodes(),
            graph.count_edges()
        );
        println!("Time taken: {:?}", now.elapsed());
    } else {
        let graph = Graph::new_at_path(&graph_path).unwrap();
        load_eth_graph(&parquet_path, &graph).unwrap()
    }
}

