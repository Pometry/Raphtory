use raphtory::arrow_loader::df_loaders::edges::ColumnNames;
#[cfg(feature = "io")]
use raphtory::io::parquet_loaders::load_edges_from_parquet;
use raphtory::{errors::GraphError, prelude::*};
use std::path::{Path, PathBuf};

/// Load ETH data from Parquet files into a Raphtory Graph.
#[cfg(feature = "io")]
fn load_eth_graph(parquet_path: &Path, graph: &Graph) -> Result<(), GraphError> {
    // ── Static Nodes ──────────────────────────────────────────────────────

    load_edges_from_parquet(
        graph,
        parquet_path,
        ColumnNames::new("block_timestamp", None, "from_address", "to_address", None),
        true,
        &["transaction_index", "value", "gas", "gas_price", "hash"],
        &[],
        None,
        Some("ETH"),
        None,
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

#[cfg(feature = "io")]
fn main() {
    let parquet_path = std::env::args()
        .nth(1)
        .map(|dir| PathBuf::from(dir))
        .unwrap_or_else(|| panic!("Usage: snb_loader <data_dir>"));
    let graph_path = std::env::args()
        .nth(2)
        .map(|graph| PathBuf::from(graph))
        .unwrap_or_else(|| parquet_path.join("..").join("graph"));
    let graph = Graph::new_at_path(&graph_path).unwrap();
    load_eth_graph(&parquet_path, &graph).unwrap()
}

#[cfg(not(feature = "io"))]
fn main() {}
