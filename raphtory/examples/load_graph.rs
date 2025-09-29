use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[cfg(all(feature = "io", feature = "arrow"))]
fn main() {
    use std::path::PathBuf;

    use raphtory::{io::parquet_loaders::load_edges_from_parquet, prelude::*};
    use raphtory_storage::core_ops::CoreGraphOps;
    let layer_map = [
        (
            "eip155:8217:5c13e303a62fc5dedf5b52d66873f2e59fedadc2",
            "usd₮0_opt_edge_list",
        ),
        (
            "eip155:8217:e2053bcf56d2030d2470fb454574237cf9ee3d4b",
            "usdc_e_opt_edge_list",
        ),
        (
            "eip155:8217:608792deb376cce1c9fa4d0e6b7b44f507cffa6a",
            "usdc_opt_edge_list",
        ),
        (
            "eip155:8217:9025095263d1e548dc890a7589a4c78038ac40ab",
            "usdt_opt_edge_list",
        ),
        (
            "eip155:8217:9025095263d1e548dc890a7589a4c78038ac40ab",
            "usdt6_opt_edge_list",
        ),
        (
            "eip155:8217:d077a400968890eacc75cdc901f0356c943e4fdb",
            "usdt7_opt_edge_list",
        ),
    ]
    .into_iter()
    .collect::<std::collections::HashMap<_, _>>();

    let graph_path = PathBuf::from("/Volumes/pometry/work/graphs/kaia_1");
    let parquet_root = "/Volumes/pometry/work/assets/kaia";
    if graph_path.exists() {
        let now = std::time::Instant::now();
        let g = Graph::load_from_path(&graph_path);

        println!(
            "num_edges: {} num_nodes: {}, temporal_edges: {}, layers: {:?}, earliest:{:?}, latest: {:?}, time to load: {:?}",
            g.unfiltered_num_edges(),
            g.unfiltered_num_nodes(),
            g.count_temporal_edges(),
            g.unique_layers().collect::<Vec<_>>(),
            g.earliest_time(),
            g.latest_time(),
            now.elapsed()
        );
    } else {
        let g = Graph::new_at_path(&graph_path);

        let print_stats_fn = |g: &Graph| {
            println!(
                "num_edges: {} num_nodes: {}, temporal_edges: {}, earliest: {:?}, latest: {:?}, layers: {:?}",
                g.unfiltered_num_edges(),
                g.unfiltered_num_nodes(),
                g.count_temporal_edges(),
                g.earliest_time(),
                g.latest_time(),
                g.unique_layers().collect::<Vec<_>>()
            );
        };
        let now = std::time::Instant::now();

        for entry in std::fs::read_dir(parquet_root).expect("Failed to read parquet root") {
            let entry = entry.expect("Failed to read entry");
            let path = entry.path();
            if path.is_dir() {
                let dir_name = path.file_name().unwrap().to_str().unwrap();
                let layer_name = layer_map
                    .get(dir_name.strip_prefix("asset_id=").unwrap())
                    .unwrap_or_else(|| panic!("{dir_name} not in layer_map")); //.map_or(dir_name, |n| *n);

                println!("Loading layer: {layer_name} from {path:?}");

                load_edges_from_parquet(
                    &g,
                    &path,
                    "transaction_timestamp",
                    "transfer_sender_cluster_id",
                    "transfer_receiver_cluster_id",
                    &[
                        "transaction_hash",
                        "transfer_index",
                        "transfer_amount_asset",
                        "transfer_amount_usd",
                        "transfer_sender_name",
                        "transfer_receiver_name",
                        "receiver_address",
                        "transfer_sender_category",
                        "transfer_receiver_category",
                    ],
                    &[],
                    None,
                    Some(layer_name),
                    None,
                    Some(2_000_000),
                )
                .expect("Failed to load edges from parquet");

                print_stats_fn(&g);
            }
        }

        println!("Total time: {:?}", now.elapsed());
    }
}

#[cfg(not(all(feature = "io", feature = "arrow")))]
fn main() {}
