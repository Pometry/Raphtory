use raphtory::{
    algorithms::centrality::pagerank::unweighted_page_rank,
    graph_loader::source::csv_loader::CsvLoader, prelude::*,
};
use serde::Deserialize;
use std::{
    env,
    path::{Path, PathBuf},
    time::Instant,
};

#[derive(Deserialize, std::fmt::Debug)]
pub struct Benchr {
    src_id: String,
    dst_id: String,
}

fn main() {
    println!("Hello, world!");
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
    println!("Loading data");
    let graph = if encoded_data_dir.exists() {
        let now = Instant::now();
        let g = Graph::load_from_file(encoded_data_dir.as_path())
            .expect("Failed to load graph from encoded data files");

        println!(
            "Loaded graph from encoded data files {} with {} vertices, {} edges which took {} seconds",
            encoded_data_dir.to_str().unwrap(),
            g.count_vertices(),
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
                g.add_vertex(1, lotr.src_id.clone(), NO_PROPS)
                    .expect("Failed to add vertex");

                g.add_vertex(1, lotr.dst_id.clone(), NO_PROPS)
                    .expect("Failed to add vertex");

                g.add_edge(1, lotr.src_id.clone(), lotr.dst_id.clone(), NO_PROPS, None)
                    .expect("Failed to add edge");
            })
            .expect("Failed to load graph from CSV data files");

        println!(
            "Loaded graph from CSV data files {} with {} vertices, {} edges which took {} seconds",
            encoded_data_dir.to_str().unwrap(),
            g.count_vertices(),
            g.count_edges(),
            now.elapsed().as_secs()
        );

        g.save_to_file(encoded_data_dir)
            .expect("Failed to save graph");

        g
    };
    println!("Data loaded\nPageRanking");
    unweighted_page_rank(&graph, 25, Some(8), None, true);
    println!("Done PR");
}
