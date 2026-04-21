#[cfg(feature = "io")]
use raphtory::io::{
    arrow::df_loaders::edges::ColumnNames, parquet_loaders::load_edges_from_parquet,
};
use raphtory::{errors::GraphError, prelude::*};
#[cfg(feature = "io")]
use raphtory_storage::core_ops::CoreGraphOps;
use std::path::{Path, PathBuf};

/// Load ETH data from Parquet files into a Raphtory Graph.
#[cfg(feature = "io")]
fn load_eth_graph(parquet_path: &Path, graph: &Graph) -> Result<(), GraphError> {
    // ── Static Nodes ──────────────────────────────────────────────────────

    let props = (1..=20).map(|i| format!("prop_{i}")).collect::<Vec<_>>();
    let props = props.iter().map(|s| s.as_ref()).collect::<Vec<_>>();
    load_edges_from_parquet(
        graph,
        parquet_path,
        ColumnNames::new("time", None, "source", "target", None),
        true,
        &props,
        &[],
        None,
        Some("G500"),
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

#[cfg(not(feature = "io"))]
fn main() {}
