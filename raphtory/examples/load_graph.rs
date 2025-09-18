use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[cfg(all(feature = "io", feature = "arrow"))]
fn main() {
    use std::path::PathBuf;
    #[derive(serde::Serialize, PartialEq, PartialOrd, Eq, Ord)]
    struct Edge {
        src: String,
        dst: String,
    }

    use raphtory::{io::parquet_loaders::load_edges_from_parquet, prelude::*};
    use raphtory_storage::core_ops::CoreGraphOps;

    let graph_path = PathBuf::from("/Volumes/Work/graphs/raphtory_graph_1");
    let layers = [
        "usd₮0_opt_edge_list",
        "dai_opt_edge_list",
        "dola_opt_edge_list",
        "susd_opt_edge_list",
        "usdc_e_opt_edge_list",
        "usdc_opt_edge_list",
        "usdt_opt_edge_list",
    ];
    let layers = [
        "dai_arb_edge_list",
        // "usdc_arb_edge_list",
        // "usdc_e_arb_edge_list",
        // "usde_arb_edge_list",
        // "usdt_arb_edge_list",
        "usdx_arb_edge_list",
    ];
    let parquet_root = "/Volumes/Work/assets/optimism";
    let parquet_root = "/Volumes/Work/assets/arbitrum_one";
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

            // let mut all_edges_count = 0;
            // let mut all_nodes_count = 0;

            // let mut wtr = csv::WriterBuilder::new()
            //     .has_headers(true)
            //     .from_path(format!(
            //         "/Volumes/Work/assets/usd_0_opt_edge_list_sampl_actual.csv"
            //     ))
            //     .expect("Failed to create CSV writer");

            // let mut edges = vec![];
            // for n in g.nodes() {
            //     all_nodes_count += 1usize;
            //     for e in n.out_edges() {
            //         all_edges_count += 1usize;
            //         // edges.push(Edge {
            //         //     src: e.src().name().to_string(),
            //         //     dst: e.dst().name().to_string(),
            //         // });
            //     }
            // }
            // edges.sort();

            // for edge in edges {
            //     wtr.serialize(edge).expect("Failed to write edge to CSV");
            // }

            // println!("Total edges in graph: {all_edges_count}, total nodes: {all_nodes_count}");
        };
        let now = std::time::Instant::now();
        for layer in layers {
            let parquet_path = format!("{parquet_root}/{layer}");
            println!("Loading layer: {layer} from {parquet_path}");

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
                Some(2_000_000),
            )
            .expect("Failed to load edges from parquet");

            print_stats_fn(&g);
        }
        println!("Total time: {:?}", now.elapsed());

        // let g = Graph::load_from_path(&graph_path);
        // print_stats_fn(&g);
    }
}

#[cfg(not(all(feature = "io", feature = "arrow")))]
fn main() {}
