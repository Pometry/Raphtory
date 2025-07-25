use std::path::PathBuf;

use raphtory::{io::parquet_loaders::load_edges_from_parquet, prelude::*};
use raphtory_storage::core_ops::CoreGraphOps;

fn main() {
    let graph_path = PathBuf::from("/Volumes/Work/tether/graphs/raphtory_graph");
    let layers = [
        "dai_ava_edge_list",
        "usp_ava_edge_list",
        "usdc_e_ava_edge_list",
        "usdc_ava_edge_list",
        "usdt_ava_edge_list",
    ];
    let parquet_root = "/Volumes/Work/tether/avalance_table";

    for layer in layers {
        let parquet_path = format!("{parquet_root}/{layer}");
        println!("Loading layer: {layer} from {parquet_path}");
        let g = if graph_path.exists() {
            Graph::load_from_path(&graph_path)
        } else {
            Graph::new_at_path(&graph_path)
        };

        load_edges_from_parquet(
            &g,
            &parquet_path,
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
            Some(layer),
            None,
        )
        .expect("Failed to load edges from parquet");

        println!(
            "num_edges: {} num_nodes: {}, layers: {:?}",
            g.unfiltered_num_edges(),
            g.unfiltered_num_nodes(),
            g.unique_layers().collect::<Vec<_>>()
        );

        let mut all_edges_count = 0;
        let mut all_nodes_count = 0;

        for n in g.nodes() {
            all_nodes_count += 1usize;
            for _ in n.out_edges() {
                all_edges_count += 1usize;
            }
        }
        println!("Total edges in graph: {all_edges_count}, total nodes: {all_nodes_count}");
    }
}
