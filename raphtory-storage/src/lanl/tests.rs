#[cfg(test)]
mod tests {
    use crate::lanl::*;
    use ahash::HashMapExt;
    use itertools::Itertools;
    use raphtory::{
        algorithms::{
            centrality::pagerank::unweighted_page_rank, components::weakly_connected_components,
        },
        arrow::graph_impl::{ArrowGraph, ParquetLayerCols},
        prelude::{GraphViewOps, *},
    };
    use raphtory_arrow::algorithms::connected_components::connected_components;
    use std::{cmp::Reverse, collections::HashMap, env, num::NonZeroUsize, path::Path};
    use tempfile::tempdir;

    #[test]
    fn test_query1() {
        let rsc_dir = Path::new(&env::var_os("CARGO_MANIFEST_DIR").unwrap())
            .parent()
            .unwrap()
            .join("resource");
        println!("{rsc_dir:?}");

        let graph_dir = tempdir().unwrap();
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
                dst_col: "dst",
                time_col: "epoch_time",
            },
            ParquetLayerCols {
                parquet_dir: &parquet_dirs[1],
                layer: "events_1v",
                src_col: "src",
                dst_col: "dst",
                time_col: "epoch_time",
            },
            ParquetLayerCols {
                parquet_dir: &parquet_dirs[2],
                layer: "events_2v",
                src_col: "src",
                dst_col: "dst",
                time_col: "epoch_time",
            },
        ];

        let graph = match measure_without_print_results("Graph load from dir", || {
            ArrowGraph::load_from_dir(graph_dir.as_ref())
        }) {
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

                measure_without_print_results("Graph load from parquets", || {
                    ArrowGraph::load_from_parquets(
                        graph_dir,
                        layer_parquet_cols,
                        None,
                        chunk_size,
                        t_props_chunk_size,
                        read_chunk_size,
                        concurrent_files,
                        num_threads,
                    )
                })
                .expect("Failed to load the graph from parquet files")
            }
        };

        assert_eq!(graph.count_nodes(), 1624);
        assert_eq!(graph.valid_layers("netflow").count_edges(), 2018);
        assert_eq!(graph.earliest_time(), Some(7257601));
        assert_eq!(graph.latest_time(), Some(7343985));

        assert_eq!(
            measure_with_print_results("Query 1", || query1::run(graph.as_ref()).unwrap()),
            0
        );
        assert_eq!(
            measure_with_print_results("Query 2", || query2::run(graph.as_ref()).unwrap()),
            0
        );
        assert_eq!(
            measure_with_print_results("Query 3", || query3::run(graph.as_ref()).unwrap()),
            0
        );
        assert_eq!(
            measure_with_print_results("Query 3b", || query3b::run(graph.as_ref()).unwrap()),
            0
        );
        // assert!(measure_with_print_results("Query 3c", || query3c::run(&graph).unwrap()) == 0);
        assert_eq!(
            measure_with_print_results("Query 4", || query4::run2(graph.as_ref()).unwrap()),
            0
        );

        let get_all_with_names = &measure_without_print_results("Weakly CC", || {
            weakly_connected_components(&graph, 10_000_000, None)
        })
        .get_all_with_names();

        let ccs1 = get_all_with_names
            .iter()
            .fold(HashMap::new(), |mut map, (_, c)| {
                map.entry(c).and_modify(|e| *e += 1).or_insert(1usize);
                map
            })
            .into_iter()
            .sorted_by_key(|(_, count)| Reverse(*count))
            .take(10)
            .map(|(_, count)| count)
            .collect::<Vec<_>>();

        let ccs2 = measure_without_print_results("CC", || connected_components(graph.as_ref()))
            .into_iter()
            .enumerate()
            .fold(HashMap::new(), |mut map, (_, c)| {
                map.entry(c).and_modify(|e| *e += 1).or_insert(1usize);
                map
            })
            .into_iter()
            .sorted_by_key(|(_, count)| Reverse(*count))
            .take(10)
            .map(|(_, count)| count)
            .collect::<Vec<_>>();

        // FIXME: this needs to be fixed, the arrow CC is unstable and gives us a different result
        assert_eq!(ccs1, ccs2);

        let actual = get_all_with_names.len();
        assert_eq!(actual, 1624);

        let actual = measure_without_print_results("Page Rank", || {
            unweighted_page_rank(
                &graph.valid_layers("netflow"),
                Some(100),
                None,
                None,
                true,
                None,
            )
        })
        .get_all_with_names()
        .len();
        assert_eq!(actual, 1624);

        assert_eq!(
            measure_with_print_results("Exfilteration Query 1", || exfiltration::query1::run(
                graph.as_ref()
            ),),
            Some(0)
        );
        assert_eq!(
            measure_with_print_results("Exfilteration Count Query Total", || {
                exfiltration::count::query_total(graph.as_ref(), 30)
            },),
            0
        );
        assert_eq!(
            measure_with_print_results("Exfilteration List Query Count", || {
                exfiltration::list::query_count(graph.as_ref(), 30)
            },),
            0
        );
    }
}
