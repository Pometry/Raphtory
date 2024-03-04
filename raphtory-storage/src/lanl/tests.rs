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
    use std::{collections::HashMap, env, num::NonZeroUsize};

    #[test]
    fn test_query1() {
        let executable_path = env::current_exe().expect("Failed to get executable path");
        let rsc_dir = executable_path
            .parent()
            .expect("Executable has no parent directory")
            .join("../../../resource");

        let graph_dir = rsc_dir.join("target");
        let rsc_dir = rsc_dir.canonicalize().unwrap();
        let parquet_dirs = vec![
            rsc_dir
                .join("netflowsorted/nft_sorted")
                .to_str()
                .unwrap()
                .to_string(),
            rsc_dir
                .join("netflowsorted/v1_sorted")
                .to_str()
                .unwrap()
                .to_string(),
            rsc_dir
                .join("netflowsorted/v2_sorted")
                .to_str()
                .unwrap()
                .to_string(),
        ];

        let layer_parquet_cols: Vec<ParquetLayerCols> = vec![
            ParquetLayerCols {
                parquet_dir: &parquet_dirs[0],
                layer: "netflow",
                src_col: "src",
                src_hash_col: "src_hash",
                dst_col: "dst",
                dst_hash_col: "dst_hash",
                time_col: "epoch_time",
            },
            ParquetLayerCols {
                parquet_dir: &parquet_dirs[1],
                layer: "events_1v",
                src_col: "src",
                src_hash_col: "src_hash",
                dst_col: "dst",
                dst_hash_col: "dst_hash",
                time_col: "epoch_time",
            },
            ParquetLayerCols {
                parquet_dir: &parquet_dirs[2],
                layer: "events_2v",
                src_col: "src",
                src_hash_col: "src_hash",
                dst_col: "dst",
                dst_hash_col: "dst_hash",
                time_col: "epoch_time",
            },
        ];

        let graph = match ArrowGraph::load_from_dir(graph_dir.clone()) {
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

        assert!(graph.count_nodes() == 2);
        assert!(graph.count_edges() == 1);
        assert!(graph.earliest() == 7257605);
        assert!(graph.latest() == 7281409);

        assert!(measure("Query 1", || query1::run(&graph).unwrap(), true) == 0);
        assert!(measure("Query 2", || query2::run(&graph).unwrap(), true) == 0);
        assert!(measure("Query 3", || query3::run(&graph).unwrap(), true) == 0);
        assert!(measure("Query 3b", || query3b::run(&graph).unwrap(), true) == 0);
        // assert!(measure("Query 3c", || query3c::run(&graph).unwrap(), true) == 0);
        assert!(measure("Query 4", || query4::run2(&graph).unwrap(), true) == 0);

        assert!(measure("CC", || connected_components(&graph.layer(0)), false) == vec![0, 1]);

        let actual = measure(
            "Weakly CC",
            || weakly_connected_components(&graph, 20, None),
            false,
        )
        .get_all_with_names();
        let expected: HashMap<String, u64> = HashMap::from_iter(vec![
            ("Comp156925".to_string(), 14843814336300980724),
            ("Comp523733".to_string(), 11548323944331110206),
        ]);
        assert!(actual == expected);

        let actual = measure(
            "Page Rank",
            || unweighted_page_rank(&graph, 100, None, None, true),
            false,
        )
        .get_all_with_names();
        let expected: HashMap<String, f64> = HashMap::from_iter(vec![
            ("Comp156925".to_string(), 0.8695642321898587),
            ("Comp523733".to_string(), 0.13043576781014135),
        ]);
        assert!(actual == expected);

        assert!(
            measure(
                "Exfilteration Query 1",
                || exfiltration::query1::run(&graph),
                false,
            ) == Some(0)
        );
        assert!(
            measure(
                "Exfilteration Count Query Total",
                || exfiltration::count::query_total(&graph, 30),
                false,
            ) == 0
        );
        assert!(
            measure(
                "Exfilteration List Query Count",
                || exfiltration::list::query_count(&graph, 30),
                false,
            ) == 0
        );
    }
}
