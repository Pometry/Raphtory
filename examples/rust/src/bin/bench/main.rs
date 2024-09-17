use raphtory::{
    algorithms::centrality::pagerank::unweighted_page_rank, io::csv_loader::CsvLoader,
    logging::global_info_logger, prelude::*,
};
use serde::Deserialize;
use std::{
    env,
    path::{Path, PathBuf},
    time::Instant,
};
use tracing::info;

#[derive(Deserialize, std::fmt::Debug)]
pub struct Benchr {
    src_id: String,
    dst_id: String,
}

fn main() {
    global_info_logger();
    info!("Hello, world!");
    let args: Vec<String> = env::args().collect();

    let default_data_dir: PathBuf = [env!("CARGO_MANIFEST_DIR"), "src/bin/bench/data"]
        .iter()
        .collect();

    let data_dir = if args.len() < 2 {
        &default_data_dir
    } else {
        Path::new(args.get(1).unwrap())
    };

    if !data_dir.exists() {
        panic!("Missing data dir = {}", data_dir.to_str().unwrap())
    }

    let encoded_data_dir = data_dir.join("graphdb.bincode");
    info!("Loading data");
    let graph = if encoded_data_dir.exists() {
        let now = Instant::now();
        let g = Graph::decode(encoded_data_dir.as_path())
            .expect("Failed to load graph from encoded data files");

        info!(
            "Loaded graph from encoded data files {} with {} nodes, {} edges which took {} seconds",
            encoded_data_dir.to_str().unwrap(),
            g.count_nodes(),
            g.count_edges(),
            now.elapsed().as_secs()
        );

        g
    } else {
        let g = Graph::new();
        let now = Instant::now();

        CsvLoader::new(data_dir)
            .set_delimiter("\t")
            .load_into_graph(&g, |lotr: Benchr, g: &Graph| {
                g.add_node(1, lotr.src_id.clone(), NO_PROPS, None)
                    .expect("Failed to add node");

                g.add_node(1, lotr.dst_id.clone(), NO_PROPS, None)
                    .expect("Failed to add node");

                g.add_edge(1, lotr.src_id.clone(), lotr.dst_id.clone(), NO_PROPS, None)
                    .expect("Failed to add edge");
            })
            .expect("Failed to load graph from CSV data files");

        info!(
            "Loaded graph from CSV data files {} with {} nodes, {} edges which took {} seconds",
            encoded_data_dir.to_str().unwrap(),
            g.count_nodes(),
            g.count_edges(),
            now.elapsed().as_secs()
        );

        g.encode(encoded_data_dir).expect("Failed to save graph");

        g
    };
    info!("Data loaded\nPageRanking");
    unweighted_page_rank(&graph, Some(25), Some(8), None, true, None);
    info!("Done PR");
}
