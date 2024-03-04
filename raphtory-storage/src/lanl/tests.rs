#[cfg(test)]
mod tests {
    use crate::lanl::*;
    use raphtory::{
        algorithms::{
            centrality::pagerank::unweighted_page_rank, components::weakly_connected_components,
        },
        arrow::{
            algorithms::connected_components::connected_components,
            graph_impl::{ArrowGraph, ParquetLayerCols},
        },
        prelude::GraphViewOps,
    };
    use std::num::NonZeroUsize;

    #[test]
    fn test_query1() {
        let graph_dir = "target";
        let layer_parquet_cols: Vec<ParquetLayerCols> = vec![
            ParquetLayerCols {
                parquet_dir: "data/netflowsorted/nft_sorted",
                layer: "netflow",
                src_col: "src",
                src_hash_col: "src_hash",
                dst_col: "dst",
                dst_hash_col: "dst_hash",
                time_col: "epoch_time",
            },
            ParquetLayerCols {
                parquet_dir: "data/netflowsorted/v1_sorted",
                layer: "events_1v",
                src_col: "src",
                src_hash_col: "src_hash",
                dst_col: "dst",
                dst_hash_col: "dst_hash",
                time_col: "epoch_time",
            },
            ParquetLayerCols {
                parquet_dir: "data/netflowsorted/v2_sorted",
                layer: "events_2v",
                src_col: "src",
                src_hash_col: "src_hash",
                dst_col: "dst",
                dst_hash_col: "dst_hash",
                time_col: "epoch_time",
            },
        ];

        let graph = match ArrowGraph::load_from_dir(graph_dir) {
            Ok(g) => g,
            Err(e) => {
                println!("Failed to load the graph from the directory. Attempting to load from parquet files: {}", e);

                let num_threads = std::thread::available_parallelism()
                    .unwrap_or(NonZeroUsize::new(1).unwrap())
                    .into();
                let chunk_size = 268_435_456;
                let t_props_chunk_size = chunk_size / 8;
                let read_chunk_size = Some(4_000_000);
                let concurrent_files = Some(1);

                ArrowGraph::load_from_parquets(
                    graph_dir,
                    layer_parquet_cols,
                    chunk_size,
                    t_props_chunk_size,
                    read_chunk_size,
                    concurrent_files,
                    num_threads,
                )
                .expect("Failed to load the graph from parquet files")
            }
        };

        println!("Node count {}", graph.count_nodes());
        println!("Edge count {}", graph.count_edges());
        println!("Earliest time {}", graph.earliest());
        println!("Latest time {}", graph.latest());

        measure("Query 1", || query1::run(&graph).unwrap(), true);
        measure("Query 2", || query2::run(&graph).unwrap(), true);
        measure("Query 3", || query3::run(&graph).unwrap(), true);
        measure("Query 3b", || query3b::run(&graph).unwrap(), true);
        // # measure("Query 3c", || query3c::run(&graph).unwrap(), true);
        measure("Query 4", || query4::run2(&graph).unwrap(), true);

        measure("CC", || connected_components(&graph.layer(0)), false);
        measure(
            "Weakly CC",
            || weakly_connected_components(&graph, 20, None),
            false,
        );
        measure(
            "Page Rank",
            || unweighted_page_rank(&graph, 100, None, None, true),
            false,
        );
        measure(
            "Exfilteration Query 1",
            || exfiltration::query1::run(&graph),
            false,
        );
        measure(
            "Exfilteration Count Query Total",
            || exfiltration::count::query_total(&graph, 30),
            false,
        );
        measure(
            "Exfilteration List Query Count",
            || exfiltration::list::query_count(&graph, 30),
            false,
        );
    }
}
