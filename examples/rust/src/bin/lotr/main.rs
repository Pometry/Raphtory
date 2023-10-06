use itertools::Itertools;
use raphtory::{
    algorithms::pathing::temporal_reachability::temporally_reachable_nodes, core::utils::hashing,
    graph_loader::source::csv_loader::CsvLoader, prelude::*,
};
use serde::Deserialize;
use std::{
    env,
    path::{Path, PathBuf},
    time::Instant,
};

#[derive(Deserialize, std::fmt::Debug)]
pub struct Lotr {
    src_id: String,
    dst_id: String,
    time: i64,
}

fn main() {
    let args: Vec<String> = env::args().collect();

    let default_data_dir: PathBuf = [env!("CARGO_MANIFEST_DIR"), "src/bin/lotr/data"]
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
            .load_into_graph(&g, |lotr: Lotr, g: &Graph| {
                g.add_vertex(
                    lotr.time,
                    lotr.src_id.clone(),
                    [("type", Prop::str("Character"))],
                )
                .expect("Failed to add vertex");

                g.add_vertex(
                    lotr.time,
                    lotr.dst_id.clone(),
                    [("type", Prop::str("Character"))],
                )
                .expect("Failed to add vertex");

                g.add_edge(
                    lotr.time,
                    lotr.src_id.clone(),
                    lotr.dst_id.clone(),
                    [("type", Prop::str("Character Co-occurrence"))],
                    None,
                )
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

    assert_eq!(graph.count_vertices(), 139);
    assert_eq!(graph.count_edges(), 701);

    let gandalf = hashing::calculate_hash(&"Gandalf");

    assert_eq!(gandalf, 2760374808085341115);
    assert!(graph.has_vertex(gandalf));
    assert_eq!(graph.vertex(gandalf).unwrap().name(), "Gandalf");

    let r = temporally_reachable_nodes(&graph, None, 20, 31930, vec!["Gandalf"], None);
    assert_eq!(
        r.result.keys().sorted().collect_vec(),
        vec!["Gandalf", "Saruman", "Wormtongue"]
    )
}
